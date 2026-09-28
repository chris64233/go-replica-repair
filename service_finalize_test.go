package goreplicarepair

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 并发完成最后几个数据块：目标副本只在全部完成后暴露一次，完成通知唯一。
func TestConcurrentFinalizeSwitchesOnce(t *testing.T) {
	const n = 8
	data := make([][]byte, n)
	for i := range data {
		data[i] = []byte(fmt.Sprintf("chunk-body-%d", i))
	}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	// dst 在修复完成前从未发布过该对象清单：并发观察者在整个修复期间
	// 反复读取；若读到清单，它必须是 RepairedFrom 非空的修复清单，
	// 绝不能是半成品（这只是并发扰动，完成态在下方确定性断言）。
	stop := make(chan struct{})
	var watchWG sync.WaitGroup
	watchWG.Add(1)
	go func() {
		defer watchWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if m, err := e.svc.CurrentManifest("dst", "obj"); err == nil && m.RepairedFrom == "" {
				t.Errorf("target exposed non-repaired manifest: %+v", m)
			}
			time.Sleep(time.Microsecond)
		}
	}()

	var completions int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// 每个工作者循环领取，直到领到自己的数据块（序号不确定）。
			for {
				l, err := e.svc.ClaimChunk(sessID, fmt.Sprintf("w%d", idx))
				if errors.Is(err, ErrNoAvailableChunk) || errors.Is(err, ErrSessionTerminal) {
					return
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				d := data[l.ChunkIndex]
				key := e.putBlob(t, d)
				if err := e.svc.StageBlob(sessID, l.WorkerID, key); err != nil {
					t.Errorf("stage: %v", err)
					return
				}
				_, rec, err := e.svc.CompleteChunk(ChunkReceipt{
					SessionID: sessID, ChunkIndex: l.ChunkIndex,
					LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: l.WorkerID, BlobKey: key,
				})
				if err != nil {
					t.Errorf("complete: %v", err)
					return
				}
				if rec != nil {
					atomic.AddInt64(&completions, 1)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	if completions != 1 {
		t.Fatalf("completion notifications = %d, want exactly 1", completions)
	}
	close(stop)
	watchWG.Wait()

	// 确定性断言（不依赖观察者是否恰好观察到切换瞬间）：
	// 完成后目标副本必然已暴露，且暴露的只能是修复清单。
	cur, err := e.svc.CurrentManifest("dst", "obj")
	must(t, err)
	sess, err := e.svc.GetSession(sessID)
	must(t, err)
	if sess.Status != SessionCompleted || !sess.CompletionNotified {
		t.Fatalf("session not completed: %+v", sess)
	}
	if cur.ID != sess.RepairedManifestID || cur.RepairedFrom != sess.FrozenManifestID {
		t.Fatalf("target current = %+v, want switch to repaired manifest", cur)
	}
	if len(cur.Chunks) != n {
		t.Fatalf("repaired manifest chunks = %d, want %d", len(cur.Chunks), n)
	}
	// 源副本仍指向冻结的源清单，未被波及。
	src, err := e.svc.CurrentManifest("src", "obj")
	must(t, err)
	if src.ID != sess.FrozenManifestID {
		t.Fatalf("source manifest changed: %s", src.ID)
	}
	// 完成结果重复读取始终是同一条凭证。
	r1, err := e.svc.GetCompletion(sessID)
	must(t, err)
	r2, err := e.svc.GetCompletion(sessID)
	must(t, err)
	if r1.NotificationID != r2.NotificationID || r1.RepairedManifestID != r2.RepairedManifestID {
		t.Fatal("completion record is not stable across reads")
	}
}

// 全部完成之前，任何中途状态都不能从目标副本读到。
func TestTargetHiddenUntilAllChunksDone(t *testing.T) {
	data := [][]byte{[]byte("a1"), []byte("b2"), []byte("c3")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	for i := 0; i < len(data)-1; i++ {
		l, key := e.claimAndStage(t, sessID, fmt.Sprintf("w%d", i), data[i])
		_, rec, err := e.svc.CompleteChunk(ChunkReceipt{
			SessionID: sessID, ChunkIndex: i,
			LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: l.WorkerID, BlobKey: key,
		})
		must(t, err)
		if rec != nil {
			t.Fatal("completion surfaced before all chunks done")
		}
		if _, err := e.svc.CurrentManifest("dst", "obj"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("target exposed early, got err=%v", err)
		}
	}
	l, key := e.claimAndStage(t, sessID, "wlast", data[2])
	_, rec, err := e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 2,
		LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "wlast", BlobKey: key,
	})
	must(t, err)
	if rec == nil {
		t.Fatal("final completion did not produce notification")
	}
	cur, err := e.svc.CurrentManifest("dst", "obj")
	must(t, err)
	if cur.ID != rec.RepairedManifestID {
		t.Fatal("target not switched exactly to completion manifest")
	}
}
