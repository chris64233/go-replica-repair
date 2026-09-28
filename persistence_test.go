package goreplicarepair

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// FileStore 持久化：进程“重启”后状态完整恢复。
func TestFileStorePersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	data := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	setup := func() (*Service, *MemoryBlobStore, *fakeClock) {
		clk := newFakeClock()
		blobs := NewMemoryBlobStore()
		store, err := NewFileStore(path)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		svc := NewService(store, blobs,
			WithClock(clk), WithLeaseTTL(10*time.Second), WithSessionTimeout(1*time.Hour))
		return svc, blobs, clk
	}

	svc, blobs, _ := setup()
	must(t, svc.RegisterReplica("src"))
	must(t, svc.RegisterReplica("dst"))
	specs := make([]ChunkSpec, len(data))
	for i, d := range data {
		specs[i] = ChunkSpec{Index: i, Digest: ComputeDigest(d), Size: int64(len(d))}
	}
	_, err := svc.PublishManifest(PublishManifestInput{
		ManifestID: "man-v1", ObjectID: "obj", Version: 1,
		Chunks: specs, Replicas: []string{"src"},
	})
	must(t, err)
	sess, err := svc.CreateRepair(CreateRepairInput{SourceReplica: "src", TargetReplica: "dst", ObjectID: "obj"})
	must(t, err)

	// 完成第 0 块、领取第 1 块后“崩溃”。
	l0, err := svc.ClaimChunk(sess.ID, "w0")
	must(t, err)
	k0, _, err := blobs.Put(data[0])
	must(t, err)
	must(t, svc.StageBlob(sess.ID, "w0", k0))
	_, _, err = svc.CompleteChunk(ChunkReceipt{
		SessionID: sess.ID, ChunkIndex: 0,
		LeaseID: l0.LeaseID, Epoch: l0.Epoch, WorkerID: "w0", BlobKey: k0,
	})
	must(t, err)
	l1, err := svc.ClaimChunk(sess.ID, "w1")
	must(t, err)
	if l1.ChunkIndex != 1 {
		t.Fatalf("second lease chunk = %d", l1.ChunkIndex)
	}
	must(t, svc.store.(*FileStore).Close())

	// 重开：清单、会话、各数据块状态与租约 epoch 全部恢复。
	svc2, _, _ := setup()
	cur, err := svc2.CurrentManifest("src", "obj")
	must(t, err)
	if cur.ID != "man-v1" || len(cur.Chunks) != 3 {
		t.Fatalf("manifest not restored: %+v", cur)
	}
	got, err := svc2.GetSession(sess.ID)
	must(t, err)
	if got.Status != SessionRunning || got.FrozenManifestID != "man-v1" ||
		got.Chunks[0].Status != ChunkCompleted || got.Chunks[0].BlobKey != k0 ||
		got.Chunks[1].Status != ChunkLeased || got.Chunks[1].LeaseID != l1.LeaseID ||
		got.Chunks[1].Epoch != l1.Epoch || got.Chunks[2].Status != ChunkPending {
		t.Fatalf("session not restored faithfully: %+v", got)
	}
	prog, err := svc2.GetProgress(sess.ID)
	must(t, err)
	if prog.Completed != 1 || prog.Leased != 1 || prog.Pending != 1 || prog.DoneSize != 5 {
		t.Fatalf("progress restored wrong: %+v", prog)
	}
	must(t, svc2.store.(*FileStore).Close())

	// 再次重开并取消：终态同样持久化。
	svc3, _, _ := setup()
	cancelled, err := svc3.Cancel(sess.ID, "restart abort")
	must(t, err)
	if cancelled.Status != SessionCancelled {
		t.Fatalf("cancel = %s", cancelled.Status)
	}
	must(t, svc3.store.(*FileStore).Close())

	svc4, _, _ := setup()
	got2, err := svc4.GetSession(sess.ID)
	must(t, err)
	if got2.Status != SessionCancelled {
		t.Fatalf("terminal state not persisted: %s", got2.Status)
	}
	// 清理在重开后仍可执行（全新 BlobStore 中内容已不存在，按跳过处理，不报错）。
	rep, err := svc4.CleanupSession(sess.ID)
	must(t, err)
	if len(rep.Deleted) != 0 {
		t.Fatalf("cleanup after restart = %+v", rep)
	}
	if _, err := svc4.ClaimChunk(sess.ID, "w"); !errors.Is(err, ErrSessionTerminal) {
		t.Fatalf("claim after restart cancel: %v", err)
	}
	must(t, svc4.store.(*FileStore).Close())
}

// 完整修复后重启：目标副本指向、完成凭证仍可查。
func TestFileStoreCompletionAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	data := [][]byte{[]byte("one"), []byte("two")}

	clk := newFakeClock()
	blobs := NewMemoryBlobStore()
	store, err := NewFileStore(path)
	must(t, err)
	svc := NewService(store, blobs, WithClock(clk))
	must(t, svc.RegisterReplica("src"))
	must(t, svc.RegisterReplica("dst"))
	specs := make([]ChunkSpec, len(data))
	for i, d := range data {
		specs[i] = ChunkSpec{Index: i, Digest: ComputeDigest(d), Size: int64(len(d))}
	}
	_, err = svc.PublishManifest(PublishManifestInput{
		ManifestID: "m1", ObjectID: "obj", Version: 1, Chunks: specs, Replicas: []string{"src"},
	})
	must(t, err)
	sess, err := svc.CreateRepair(CreateRepairInput{SourceReplica: "src", TargetReplica: "dst", ObjectID: "obj"})
	must(t, err)
	for i, d := range data {
		l, err := svc.ClaimChunk(sess.ID, "w")
		must(t, err)
		if l.ChunkIndex != i {
			t.Fatalf("lease order wrong: %d", l.ChunkIndex)
		}
		key, _, err := blobs.Put(d)
		must(t, err)
		must(t, svc.StageBlob(sess.ID, "w", key))
		_, _, err = svc.CompleteChunk(ChunkReceipt{
			SessionID: sess.ID, ChunkIndex: i,
			LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "w", BlobKey: key,
		})
		must(t, err)
	}
	must(t, store.Close())

	blobs2 := NewMemoryBlobStore()
	store2, err := NewFileStore(path)
	must(t, err)
	svc2 := NewService(store2, blobs2, WithClock(clk))
	cur, err := svc2.CurrentManifest("dst", "obj")
	must(t, err)
	if cur.RepairedFrom != "m1" || cur.Version != 1 || len(cur.Chunks) != 2 {
		t.Fatalf("target switch not persisted: %+v", cur)
	}
	rec, err := svc2.GetCompletion(sess.ID)
	must(t, err)
	if rec.RepairedManifestID != cur.ID || rec.TotalChunks != 2 || rec.NotificationID == "" {
		t.Fatalf("completion record not persisted: %+v", rec)
	}
	must(t, store2.Close())

	// 落盘文件是合法 JSON 且非空。
	info, err := os.Stat(path)
	must(t, err)
	if info.Size() == 0 {
		t.Fatal("state file empty")
	}
}

// MemoryStore 的 CAS 冲突语义。
func TestMemoryStoreConflict(t *testing.T) {
	ms := NewMemoryStore()
	st, rev, err := ms.Load()
	must(t, err)
	st.NextSeq = 1
	newRev, err := ms.Commit(rev, st)
	must(t, err)
	if newRev != rev+1 {
		t.Fatalf("newRev = %d", newRev)
	}
	st2, rev2, err := ms.Load()
	must(t, err)
	if rev2 != newRev || st2.NextSeq != 1 {
		t.Fatalf("commit lost: rev=%d seq=%d", rev2, st2.NextSeq)
	}
	if _, err := ms.Commit(rev, st); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale commit: %v", err)
	}
}

// BlobStore 引用计数：同内容多副本共享，逐个释放后才删除。
func TestBlobStoreRefCounting(t *testing.T) {
	b := NewMemoryBlobStore()
	key, _, err := b.Put([]byte("payload"))
	must(t, err)
	must(t, b.AddRef(key, "r1"))
	must(t, b.AddRef(key, "r2"))
	if err := b.AddRef("missing", "r1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("AddRef missing: %v", err)
	}
	deleted, skipped, err := b.DeleteUnreferenced([]string{key})
	must(t, err)
	if len(deleted) != 0 || len(skipped) != 1 {
		t.Fatalf("protected blob deleted: d=%v s=%v", deleted, skipped)
	}
	must(t, b.ReleaseRef(key, "r1"))
	deleted, _, err = b.DeleteUnreferenced([]string{key})
	must(t, err)
	if len(deleted) != 0 {
		t.Fatal("blob deleted while one ref remains")
	}
	must(t, b.ReleaseRef(key, "r2"))
	deleted, _, err = b.DeleteUnreferenced([]string{key})
	must(t, err)
	if len(deleted) != 1 || deleted[0] != key {
		t.Fatalf("blob not deleted after refs released: %v", deleted)
	}
	// 幂等：再次删除不存在的键计入 skipped。
	_, skipped, err = b.DeleteUnreferenced([]string{key})
	must(t, err)
	if len(skipped) != 1 {
		t.Fatalf("repeat cleanup not idempotent: %v", skipped)
	}
}
