package goreplicarepair

import (
	"errors"
	"testing"
)

func TestRegisterReplicaValidation(t *testing.T) {
	svc := NewService(NewMemoryStore(), NewMemoryBlobStore())
	if err := svc.RegisterReplica(""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty replica: %v", err)
	}
	if err := svc.RegisterReplica("r1"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := svc.RegisterReplica("r1"); err != nil {
		t.Fatalf("idempotent register should succeed: %v", err)
	}
}

func TestCreateRepairValidation(t *testing.T) {
	e := newTestEnv(t)
	cases := []CreateRepairInput{
		{SourceReplica: "src", TargetReplica: "dst"},
		{ObjectID: "o", TargetReplica: "dst"},
		{ObjectID: "o", SourceReplica: "src"},
		{ObjectID: "o", SourceReplica: "same", TargetReplica: "same"},
	}
	for i, in := range cases {
		if _, err := e.svc.CreateRepair(in); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("case %d: got %v", i, err)
		}
	}

	// 副本未登记 / 对象在源副本不存在。
	e.registerAndPublish(t, "m1", [][]byte{[]byte("a")})
	if _, err := e.svc.CreateRepair(CreateRepairInput{
		SourceReplica: "src", TargetReplica: "ghost", ObjectID: "obj",
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unregistered target: %v", err)
	}
	if _, err := e.svc.CreateRepair(CreateRepairInput{
		SourceReplica: "src", TargetReplica: "dst", ObjectID: "missing-obj",
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing object: %v", err)
	}
}

func TestClaimAndReceiptValidation(t *testing.T) {
	data := [][]byte{[]byte("a")}
	e := newTestEnv(t)
	sessID := startSession(t, e, data)

	if _, err := e.svc.ClaimChunk("", "w"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty session: %v", err)
	}
	if _, err := e.svc.ClaimChunk(sessID, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty worker: %v", err)
	}

	l, err := e.svc.ClaimChunk(sessID, "w")
	must(t, err)
	for _, bad := range []ChunkReceipt{
		{SessionID: sessID, ChunkIndex: 0, LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "w"}, // 无 blob
		{SessionID: sessID, ChunkIndex: 0, LeaseID: l.LeaseID, Epoch: l.Epoch, BlobKey: "k"},  // 无 worker
		{SessionID: sessID, ChunkIndex: 0, Epoch: l.Epoch, WorkerID: "w", BlobKey: "k"},       // 无 lease
		{ChunkIndex: 0, LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "w", BlobKey: "k"},      // 无 session
	} {
		if _, _, err := e.svc.CompleteChunk(bad); !errors.Is(err, ErrInvalidArgument) &&
			!errors.Is(err, ErrNotFound) {
			t.Fatalf("receipt %+v: got %v", bad, err)
		}
	}

	// 越界数据块序号。
	key := mustStage(t, e, sessID, "w", data[0])
	_, _, err = e.svc.CompleteChunk(ChunkReceipt{
		SessionID: sessID, ChunkIndex: 42,
		LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: "w", BlobKey: key,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("out-of-range index: %v", err)
	}
}

func TestGetManifestAndCompletionErrors(t *testing.T) {
	e := newTestEnv(t)
	if _, err := e.svc.GetManifest("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetManifest: %v", err)
	}
	if _, err := e.svc.CurrentManifest("ghost", "o"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CurrentManifest replica: %v", err)
	}

	sessID := startSession(t, e, [][]byte{[]byte("a")})
	// 未完成会话取完成凭证：返回“尚未终态”类错误。
	if _, err := e.svc.GetCompletion(sessID); !errors.Is(err, ErrSessionNotTerminal) {
		t.Fatalf("GetCompletion on running session: %v", err)
	}
	if _, err := e.svc.CurrentManifest("dst", "obj"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CurrentManifest object: %v", err)
	}
}
