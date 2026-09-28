package goreplicarepair

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func startSession(t *testing.T, e *testEnv, data [][]byte) string {
	t.Helper()
	e.registerAndPublish(t, "man-v1", data)
	sess, err := e.svc.CreateRepair(CreateRepairInput{
		SourceReplica: "src", TargetReplica: "dst", ObjectID: "obj",
	})
	must(t, err)
	return sess.ID
}

// 每次领取生成新 lease 与 epoch；旧工作者迟到回执不能覆盖接管者结果。
func TestClaimEpochAndLateReceiptRejected(t *testing.T) {
	data := [][]byte{[]byte("chunk-0-data")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	l1, err := e.svc.ClaimChunk(sessID, "worker-A")
	must(t, err)
	if l1.Epoch != 1 {
		t.Fatalf("first epoch = %d, want 1", l1.Epoch)
	}

	// A 迟迟不交；租约到期后 B 接管。
	e.clk.Advance(11 * time.Second)
	l2, err := e.svc.ClaimChunk(sessID, "worker-B")
	must(t, err)
	if l2.ChunkIndex != 0 || l2.Epoch != 2 {
		t.Fatalf("takeover lease = %+v, want chunk 0 epoch 2", l2)
	}
	if l1.LeaseID == l2.LeaseID {
		t.Fatal("takeover must produce a new lease id")
	}

	// A 的迟到回执（旧 lease / 旧 epoch）必须被拒绝，不能覆盖 B。
	keyA := e.putBlob(t, data[0])
	must(t, e.svc.StageBlob(sessID, "worker-A", keyA))
	_, _, err = e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l1.LeaseID, Epoch: l1.Epoch, WorkerID: "worker-A", BlobKey: keyA,
	})
	assertErrIs(t, err, ErrLeaseMismatch)

	// 篡改 worker / epoch / lease 任一字段同样拒绝。
	keyB := e.putBlob(t, data[0])
	must(t, e.svc.StageBlob(sessID, "worker-B", keyB))
	base := ChunkReceipt{SessionID: sessID, ChunkIndex: 0,
		LeaseID: l2.LeaseID, Epoch: l2.Epoch, WorkerID: "worker-B", BlobKey: keyB}
	for _, mutate := range []func(*ChunkReceipt){
		func(r *ChunkReceipt) { r.Epoch = 99 },
		func(r *ChunkReceipt) { r.LeaseID = "lease-000099" },
		func(r *ChunkReceipt) { r.WorkerID = "worker-A" },
		func(r *ChunkReceipt) { r.SessionID = "sess-other" },
	} {
		r := base
		mutate(&r)
		if _, _, err := e.svc.CompleteChunk(r); !errors.Is(err, ErrLeaseMismatch) &&
			!errors.Is(err, ErrNotFound) {
			t.Fatalf("forged receipt %+v: got %v", r, err)
		}
	}

	// B 的正确回执完成数据块；A 此后再补回执也不能改写。
	_, rec, err := e.svc.CompleteChunk(base)
	must(t, err)
	if rec == nil || rec.RepairedManifestID == "" {
		t.Fatalf("completion record missing: %+v", rec)
	}
	_, _, err = e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l1.LeaseID, Epoch: l1.Epoch, WorkerID: "worker-A", BlobKey: keyA,
	})
	// 会话已完成：旧回执无论按租约不匹配还是会话终态被拒，都不能覆盖结果。
	if !errors.Is(err, ErrLeaseMismatch) && !errors.Is(err, ErrSessionTerminal) {
		t.Fatalf("late receipt after completion: got %v, want lease mismatch or terminal", err)
	}

	// 完成结果未被旧工作者覆盖。
	sess, err := e.svc.GetSession(sessID)
	must(t, err)
	if sess.Chunks[0].WorkerID != "" || sess.Chunks[0].Status != ChunkCompleted {
		t.Fatalf("late receipt overwrote takeover result: %+v", sess.Chunks[0])
	}
}

// 租约未到期时，同一数据块不能被第二个工作者重复领取；到期后可被接管。
func TestClaimRespectsActiveLease(t *testing.T) {
	data := [][]byte{[]byte("only-chunk")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	l1, err := e.svc.ClaimChunk(sessID, "A")
	must(t, err)
	_, err = e.svc.ClaimChunk(sessID, "B")
	assertErrIs(t, err, ErrNoAvailableChunk)

	// 未到期推进：仍不可领。
	e.clk.Advance(5 * time.Second)
	_, err = e.svc.ClaimChunk(sessID, "B")
	assertErrIs(t, err, ErrNoAvailableChunk)

	// 超过租约期限：可接管（epoch 递增），接管后再无块可领。
	e.clk.Advance(6 * time.Second)
	l2, err := e.svc.ClaimChunk(sessID, "B")
	must(t, err)
	if l2.Epoch != l1.Epoch+1 {
		t.Fatalf("takeover epoch = %d, want %d", l2.Epoch, l1.Epoch+1)
	}
	_, err = e.svc.ClaimChunk(sessID, "C")
	assertErrIs(t, err, ErrNoAvailableChunk)
}

// 摘要不符不能标记成功，租约作废，数据块可被重新领取并用正确内容修复。
func TestDigestMismatchRejectsAndReclaimable(t *testing.T) {
	data := [][]byte{[]byte("correct-content")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	l, err := e.svc.ClaimChunk(sessID, "A")
	must(t, err)
	wrongKey := mustStage(t, e, sessID, "A", []byte("WRONG"))
	_, _, err = e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "A",
		BlobKey: wrongKey,
	})
	assertErrIs(t, err, ErrDigestMismatch)

	got, err := e.svc.GetSession(sessID)
	must(t, err)
	if got.Chunks[0].Status != ChunkPending {
		t.Fatalf("chunk status = %s, want pending after mismatch", got.Chunks[0].Status)
	}

	// 重新领取（新 epoch）并用正确内容完成。
	l2, key := e.claimAndStage(t, sessID, "B", data[0])
	if l2.Epoch != 2 {
		t.Fatalf("reclaim epoch = %d, want 2", l2.Epoch)
	}
	_, rec, err := e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l2.LeaseID, Epoch: l2.Epoch, WorkerID: "B", BlobKey: key,
	})
	must(t, err)
	if rec == nil {
		t.Fatal("expected completion")
	}
}

// blob 必须先 StageBlob 进入会话暂存，否则即使摘要正确也拒绝回执。
func TestUnstagedBlobRejected(t *testing.T) {
	data := [][]byte{[]byte("abc")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)
	l, err := e.svc.ClaimChunk(sessID, "A")
	must(t, err)
	key := e.putBlob(t, data[0]) // 只上传，不 StageBlob
	_, _, err = e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "A", BlobKey: key,
	})
	assertErrIs(t, err, ErrBlobNotStaged)
}

// 租约到期后的迟到回执也必须被拒绝。
func TestExpiredLeaseReceiptRejected(t *testing.T) {
	data := [][]byte{[]byte("abc"), []byte("def")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)
	l0, key0 := e.claimAndStage(t, sessID, "A", data[0])
	must(t, e.svc.StageBlob(sessID, "A", key0))
	e.clk.Advance(11 * time.Second)
	_, _, err := e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l0.LeaseID, Epoch: l0.Epoch, WorkerID: "A", BlobKey: key0,
	})
	assertErrIs(t, err, ErrLeaseMismatch)
}

func mustStage(t *testing.T, e *testEnv, sessID, worker string, data []byte) string {
	t.Helper()
	key := e.putBlob(t, data)
	must(t, e.svc.StageBlob(sessID, worker, key))
	return key
}

// 进度查询随领取/回执变化。
func TestProgress(t *testing.T) {
	data := [][]byte{[]byte("aaa"), []byte("bbbb"), []byte("cc")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	p, err := e.svc.GetProgress(sessID)
	must(t, err)
	if p.Total != 3 || p.Pending != 3 || p.Completed != 0 || p.TotalSize != 9 {
		t.Fatalf("initial progress = %+v", p)
	}

	l, key := e.claimAndStage(t, sessID, "w", data[0])
	p, err = e.svc.GetProgress(sessID)
	must(t, err)
	if p.Pending != 2 || p.Leased != 1 {
		t.Fatalf("after claim progress = %+v", p)
	}
	_, _, err = e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "w", BlobKey: key,
	})
	must(t, err)
	p, err = e.svc.GetProgress(sessID)
	must(t, err)
	if p.Completed != 1 || p.DoneSize != 3 {
		t.Fatalf("after complete progress = %+v", p)
	}
	if p.Status != SessionRunning {
		t.Fatalf("status = %s", p.Status)
	}
}

// 无块可领时错误哨兵稳定。
func TestClaimUnknownSession(t *testing.T) {
	e := newTestEnv(t)
	if _, err := e.svc.ClaimChunk("nope", "w"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	if err := e.svc.StageBlob("nope", "w", "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

// 完成通知 ID 在会话生命周期内唯一且可重复查询。
func TestCompletionRecordStable(t *testing.T) {
	data := [][]byte{[]byte("x"), []byte("y")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)
	var first *CompletionRecord
	for i, d := range data {
		l, key := e.claimAndStage(t, sessID, fmt.Sprintf("w%d", i), d)
		_, rec, err := e.svc.CompleteChunk(ChunkReceipt{
			SessionID: sessID, ChunkIndex: i,
			LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: l.WorkerID, BlobKey: key,
		})
		must(t, err)
		if i == len(data)-1 {
			first = rec
		}
	}
	again, err := e.svc.GetCompletion(sessID)
	must(t, err)
	if again.NotificationID != first.NotificationID ||
		again.RepairedManifestID != first.RepairedManifestID {
		t.Fatalf("completion record not stable: %+v vs %+v", first, again)
	}
}
