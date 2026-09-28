package goreplicarepair

import (
	"errors"
	"testing"
	"time"
)

// 取消后停止发放新租约、拒绝新暂存与回执，已上传垃圾进入可清理状态。
func TestCancelStopsLeasesAndCleansStaged(t *testing.T) {
	data := [][]byte{[]byte("chunk-0"), []byte("chunk-1")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	l0, key0 := e.claimAndStage(t, sessID, "A", data[0])
	l1, key1 := e.claimAndStage(t, sessID, "B", data[1])

	// 模拟一次错误上传：内容与任何有效副本清单都不符，但确实占用了存储。
	keyBad := e.putBlob(t, []byte("wrong-upload-body"))
	must(t, e.svc.StageBlob(sessID, "A", keyBad))

	sess, err := e.svc.Cancel(sessID, "operator abort")
	must(t, err)
	if sess.Status != SessionCancelled || sess.CancelReason != "operator abort" {
		t.Fatalf("cancel = %+v", sess)
	}

	if _, err := e.svc.ClaimChunk(sessID, "C"); !errors.Is(err, ErrSessionTerminal) {
		t.Fatalf("claim after cancel: got %v", err)
	}
	if err := e.svc.StageBlob(sessID, "C", key0); !errors.Is(err, ErrSessionTerminal) {
		t.Fatalf("stage after cancel: got %v", err)
	}
	_, _, err = e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l0.LeaseID, Epoch: l0.Epoch, WorkerID: "A", BlobKey: key0,
	})
	if !errors.Is(err, ErrSessionTerminal) {
		t.Fatalf("receipt after cancel: got %v", err)
	}
	_ = l1

	// 取消幂等。
	sess2, err := e.svc.Cancel(sessID, "again")
	must(t, err)
	if sess2.Status != SessionCancelled {
		t.Fatalf("second cancel changed status: %s", sess2.Status)
	}

	// 清理：错误上传无任何有效副本引用，必须删除；与源清单摘要一致的
	// 正确内容仍被源副本引用，必须保留。
	rep, err := e.svc.CleanupSession(sessID)
	must(t, err)
	if len(rep.Deleted) != 1 || rep.Deleted[0] != keyBad {
		t.Fatalf("deleted = %v, want only %s (skipped=%v)", rep.Deleted, keyBad, rep.Skipped)
	}
	if _, err := e.blobs.Stat(keyBad); !errors.Is(err, ErrNotFound) {
		t.Fatal("wrong-upload blob survived cleanup")
	}
	if _, err := e.blobs.Stat(key0); err != nil {
		t.Fatalf("blob still referenced by source replica was deleted: %v", err)
	}
	if _, err := e.blobs.Stat(key1); err != nil {
		t.Fatalf("blob still referenced by source replica was deleted: %v", err)
	}

	// 重复清理安全幂等。
	rep2, err := e.svc.CleanupSession(sessID)
	must(t, err)
	if len(rep2.Deleted) != 0 {
		t.Fatalf("second cleanup deleted again: %v", rep2.Deleted)
	}

	// 运行中的会话不允许清理。
	other := e.freshObject(t, "obj2", [][]byte{[]byte("z")})
	if _, err := e.svc.CleanupSession(other); !errors.Is(err, ErrSessionNotTerminal) {
		t.Fatalf("cleanup running session: got %v", err)
	}
}

// 清理不能误删其他有效副本仍在引用的内容：暂存 blob 恰好被另一个副本的
// 当前清单按摘要引用时，即使本会话取消也必须保留。
func TestCleanupProtectsBlobsReferencedByOtherReplicas(t *testing.T) {
	data := [][]byte{[]byte("chunk-0")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	// 暂存两块内容：一块被副本 other 的当前清单引用，一块无任何引用。
	shared := []byte("shared-by-other-replica")
	garbage := []byte("nobody-references-this")
	_, _ = e.svc.ClaimChunk(sessID, "A")
	keyShared := e.putBlob(t, shared)
	keyGarbage := e.putBlob(t, garbage)
	must(t, e.svc.StageBlob(sessID, "A", keyShared))
	must(t, e.svc.StageBlob(sessID, "A", keyGarbage))

	must(t, e.svc.RegisterReplica("other"))
	_, err := e.svc.PublishManifest(PublishManifestInput{
		ManifestID: "man-other", ObjectID: "obj-other", Version: 1,
		Chunks:   []ChunkSpec{{Index: 0, Digest: ComputeDigest(shared), Size: int64(len(shared))}},
		Replicas: []string{"other"},
	})
	must(t, err)

	_, err = e.svc.Cancel(sessID, "abort")
	must(t, err)
	rep, err := e.svc.CleanupSession(sessID)
	must(t, err)

	if len(rep.Deleted) != 1 || rep.Deleted[0] != keyGarbage {
		t.Fatalf("deleted = %v, want only %s", rep.Deleted, keyGarbage)
	}
	if !contains(rep.Skipped, keyShared) {
		t.Fatalf("shared key not skipped: %v", rep.Skipped)
	}
	if _, err := e.blobs.Stat(keyShared); err != nil {
		t.Fatalf("blob referenced by other replica was deleted: %v", err)
	}
}

// 清理不能误删其他仍在运行的修复会话暂存的内容；待该会话也终止后才可删。
func TestCleanupProtectsBlobsStagedByRunningSession(t *testing.T) {
	e := newTestEnv(t)
	must(t, e.svc.RegisterReplica("src"))
	must(t, e.svc.RegisterReplica("dst"))
	specA := []ChunkSpec{{Index: 0, Digest: ComputeDigest([]byte("real-a")), Size: 6}}
	specB := []ChunkSpec{{Index: 0, Digest: ComputeDigest([]byte("real-b")), Size: 6}}
	_, err := e.svc.PublishManifest(PublishManifestInput{
		ManifestID: "man-a", ObjectID: "objA", Version: 1, Chunks: specA, Replicas: []string{"src"},
	})
	must(t, err)
	_, err = e.svc.PublishManifest(PublishManifestInput{
		ManifestID: "man-b", ObjectID: "objB", Version: 1, Chunks: specB, Replicas: []string{"src"},
	})
	must(t, err)

	a, err := e.svc.CreateRepair(CreateRepairInput{SourceReplica: "src", TargetReplica: "dst", ObjectID: "objA"})
	must(t, err)
	b, err := e.svc.CreateRepair(CreateRepairInput{SourceReplica: "src", TargetReplica: "dst", ObjectID: "objB"})
	must(t, err)

	// 两个工作者都错误上传了同一块内容（与各自冻结清单都不符）。
	bad := []byte("duplicated-wrong-upload")
	keyBad := e.putBlob(t, bad)
	must(t, e.svc.StageBlob(a.ID, "wa", keyBad))
	must(t, e.svc.StageBlob(b.ID, "wb", keyBad))

	// A 先取消并清理：B 仍在运行且暂存同一 blob，必须保留。
	_, err = e.svc.Cancel(a.ID, "abort")
	must(t, err)
	rep, err := e.svc.CleanupSession(a.ID)
	must(t, err)
	if len(rep.Deleted) != 0 || !contains(rep.Skipped, keyBad) {
		t.Fatalf("blob staged by running session not protected: %+v", rep)
	}
	if _, err := e.blobs.Stat(keyBad); err != nil {
		t.Fatalf("blob deleted while session B running: %v", err)
	}

	// B 也取消后，无任何引用，清理方可删除。
	_, err = e.svc.Cancel(b.ID, "abort")
	must(t, err)
	repB, err := e.svc.CleanupSession(b.ID)
	must(t, err)
	if len(repB.Deleted) != 1 || repB.Deleted[0] != keyBad {
		t.Fatalf("blob not deleted after all sessions cancelled: %+v", repB)
	}
	if _, err := e.blobs.Stat(keyBad); !errors.Is(err, ErrNotFound) {
		t.Fatal("blob still present after final cleanup")
	}
}

// 完成会话清理：成功清单引用的 blob 必须保留。
func TestCleanupAfterCompletionKeepsManifestBlobs(t *testing.T) {
	data := [][]byte{[]byte("good-chunk")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)
	l, key := e.claimAndStage(t, sessID, "A", data[0])
	_, rec, err := e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "A", BlobKey: key,
	})
	must(t, err)
	if rec == nil {
		t.Fatal("expected completion")
	}
	rep, err := e.svc.CleanupSession(sessID)
	must(t, err)
	if len(rep.Deleted) != 0 || !contains(rep.Skipped, key) {
		t.Fatalf("completed-session blob must survive: %+v", rep)
	}
	if _, err := e.blobs.Stat(key); err != nil {
		t.Fatalf("manifest blob missing: %v", err)
	}
}

// 超时推进：过期会话转 timed_out 并停止发租约；未过期会话的过期租约复位。
func TestSweepTimeouts(t *testing.T) {
	data := [][]byte{[]byte("c0"), []byte("c1")}
	e := newTestEnv(t)
	aID := startSession(t, e, data)
	la, ka := e.claimAndStage(t, aID, "A", data[0])
	_ = la

	bID := e.freshObject(t, "obj2", [][]byte{[]byte("z0")})
	lb, kb := e.claimAndStage(t, bID, "B", []byte("z0"))
	_ = kb

	// A 会话期间还产生过一块错误上传，用于验证超时后可清理。
	keyGarbage := e.putBlob(t, []byte("garbage-after-timeout"))
	must(t, e.svc.StageBlob(aID, "g", keyGarbage))

	// 推进 30 秒：租约（10s）过期，两个会话截止时间都未到。
	e.clk.Advance(30 * time.Second)
	res, err := e.svc.SweepTimeouts()
	must(t, err)
	if len(res.TimedOutSessions) != 0 || res.ExpiredLeases != 2 {
		t.Fatalf("sweep after 30s = %+v, want 2 expired leases, 0 timeouts", res)
	}
	// B 的过期租约已复位：可重新领取，epoch 递增。
	lb2, err := e.svc.ClaimChunk(bID, "B2")
	must(t, err)
	if lb2.ChunkIndex != 0 || lb2.Epoch != lb.Epoch+1 {
		t.Fatalf("reclaim after lease expiry = %+v (old epoch %d)", lb2, lb.Epoch)
	}

	// 再推进 1 小时：A（截止 1h）超时，B（截止 24h）仍存活。
	e.clk.Advance(1 * time.Hour)
	res, err = e.svc.SweepTimeouts()
	must(t, err)
	if len(res.TimedOutSessions) != 1 || res.TimedOutSessions[0] != aID {
		t.Fatalf("sweep after deadline = %+v, want only %s timed out", res, aID)
	}
	if _, err := e.svc.ClaimChunk(aID, "X"); !errors.Is(err, ErrSessionTerminal) {
		t.Fatalf("claim on timed out session: got %v", err)
	}
	got, err := e.svc.GetSession(aID)
	must(t, err)
	if got.Status != SessionTimedOut {
		t.Fatalf("A status = %s", got.Status)
	}

	// 超时会话的无引用错误上传可清理；正确内容仍被源副本保护。
	rep, err := e.svc.CleanupSession(aID)
	must(t, err)
	if len(rep.Deleted) != 1 || rep.Deleted[0] != keyGarbage {
		t.Fatalf("timed-out cleanup = %+v, want garbage deleted", rep)
	}
	if !contains(rep.Skipped, ka) {
		t.Fatalf("source-referenced blob not protected: %+v", rep)
	}

	// B 未受影响，仍可继续完成修复。
	prog, err := e.svc.GetProgress(bID)
	must(t, err)
	if prog.Status != SessionRunning {
		t.Fatalf("B status = %s, want running", prog.Status)
	}
}

// 部分数据块已校验通过、会话随后取消：这些内容未进入成功清单。
// 若仍被某有效副本按摘要引用则保留；若源端已升级、再无任何副本引用，则可清理。
func TestCancelAfterPartialCompletionOrphanCleanup(t *testing.T) {
	e := newTestEnv(t)
	must(t, e.svc.RegisterReplica("src"))
	must(t, e.svc.RegisterReplica("dst"))
	must(t, e.svc.RegisterReplica("tmp"))
	orphan := []byte("only-in-repair-session")
	keep := []byte("still-referenced-body")
	specs := []ChunkSpec{
		{Index: 0, Digest: ComputeDigest(orphan), Size: int64(len(orphan))},
		{Index: 1, Digest: ComputeDigest(keep), Size: int64(len(keep))},
	}
	_, err := e.svc.PublishManifest(PublishManifestInput{
		ManifestID: "m1", ObjectID: "obj", Version: 1, Chunks: specs,
		Replicas: []string{"src", "tmp"},
	})
	must(t, err)
	// 以 src 为源创建修复（冻结 m1）。
	sess, err := e.svc.CreateRepair(CreateRepairInput{
		SourceReplica: "src", TargetReplica: "dst", ObjectID: "obj",
	})
	must(t, err)
	l, key := e.claimAndStage(t, sess.ID, "A", orphan)
	ch, _, err := e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sess.ID, ChunkIndex: 0,
		LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "A", BlobKey: key,
	})
	must(t, err)
	if ch.Status != ChunkCompleted {
		t.Fatalf("chunk status = %s, want completed (session still running on chunk 1)", ch.Status)
	}

	// 源端发布 v2：两块摘要都换掉，并让 src、tmp 全部切走；旧会话仍冻结在 m1。
	v2 := [][]byte{[]byte("newer-body"), []byte("totally-different")}
	v2specs := make([]ChunkSpec, 2)
	for i, d := range v2 {
		v2specs[i] = ChunkSpec{Index: i, Digest: ComputeDigest(d), Size: int64(len(d))}
	}
	_, err = e.svc.PublishManifest(PublishManifestInput{
		ManifestID: "m2", ObjectID: "obj", Version: 2,
		Chunks: v2specs, Replicas: []string{"src", "tmp"},
	})
	must(t, err)

	_, err = e.svc.Cancel(sess.ID, "source moved on")
	must(t, err)
	rep2, err := e.svc.CleanupSession(sess.ID)
	must(t, err)
	if len(rep2.Deleted) != 1 || rep2.Deleted[0] != key {
		t.Fatalf("orphan completed chunk should be cleaned: %+v", rep2)
	}
	if _, err := e.blobs.Stat(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan blob survived cleanup: %v", err)
	}
}

// 已完成会话不可取消。
func TestCancelCompletedRejected(t *testing.T) {
	data := [][]byte{[]byte("x")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)
	l, key := e.claimAndStage(t, sessID, "A", data[0])
	_, _, err := e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 0,
		LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "A", BlobKey: key,
	})
	must(t, err)
	if _, err := e.svc.Cancel(sessID, "late"); !errors.Is(err, ErrSessionTerminal) {
		t.Fatalf("cancel completed: got %v", err)
	}
}

// 未知会话的结果/进度/清理返回 NotFound。
func TestUnknownSessionErrors(t *testing.T) {
	e := newTestEnv(t)
	if _, err := e.svc.GetSession("x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetSession: %v", err)
	}
	if _, err := e.svc.GetProgress("x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetProgress: %v", err)
	}
	if _, err := e.svc.GetCompletion("x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetCompletion: %v", err)
	}
	if _, err := e.svc.Cancel("x", "r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := e.svc.CleanupSession("x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Cleanup: %v", err)
	}
}

// freshObject 在已登记 src/dst 的环境中发布一个新对象并创建修复会话。
func (e *testEnv) freshObject(t *testing.T, objectID string, data [][]byte) string {
	t.Helper()
	specs := make([]ChunkSpec, len(data))
	for i, d := range data {
		specs[i] = ChunkSpec{Index: i, Digest: ComputeDigest(d), Size: int64(len(d))}
	}
	_, err := e.svc.PublishManifest(PublishManifestInput{
		ManifestID: "man-" + objectID, ObjectID: objectID, Version: 1,
		Chunks: specs, Replicas: []string{"src"},
	})
	must(t, err)
	sess, err := e.svc.CreateRepair(CreateRepairInput{
		SourceReplica: "src", TargetReplica: "dst", ObjectID: objectID,
		Timeout: 24 * time.Hour,
	})
	must(t, err)
	return sess.ID
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
