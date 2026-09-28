package goreplicarepair

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock 是可控时钟。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// testEnv 封装一套内存服务。
type testEnv struct {
	svc   *Service
	blobs *MemoryBlobStore
	clk   *fakeClock
}

func newTestEnv(t *testing.T, opts ...Option) *testEnv {
	t.Helper()
	clk := newFakeClock()
	blobs := NewMemoryBlobStore()
	allOpts := append([]Option{
		WithClock(clk),
		WithLeaseTTL(10 * time.Second),
		WithSessionTimeout(1 * time.Hour),
	}, opts...)
	return &testEnv{
		svc:   NewService(NewMemoryStore(), blobs, allOpts...),
		blobs: blobs,
		clk:   clk,
	}
}

// registerAndPublish 登记源/目标副本并发布 v1 清单，返回清单数据。
func (e *testEnv) registerAndPublish(t *testing.T, manifestID string, data [][]byte) *Manifest {
	t.Helper()
	must(t, e.svc.RegisterReplica("src"))
	must(t, e.svc.RegisterReplica("dst"))
	specs := make([]ChunkSpec, len(data))
	for i, d := range data {
		specs[i] = ChunkSpec{Index: i, Digest: ComputeDigest(d), Size: int64(len(d))}
	}
	m, err := e.svc.PublishManifest(PublishManifestInput{
		ManifestID: manifestID,
		ObjectID:   "obj",
		Version:    1,
		Chunks:     specs,
		Replicas:   []string{"src"},
	})
	must(t, err)
	return m
}

// putBlob 把数据写入 BlobStore。
func (e *testEnv) putBlob(t *testing.T, data []byte) string {
	t.Helper()
	key, _, err := e.blobs.Put(data)
	must(t, err)
	return key
}

// claimAndStage 领取数据块并暂存对应 blob，返回租约与 blob 键。
func (e *testEnv) claimAndStage(t *testing.T, sessID, worker string, data []byte) (*ChunkLease, string) {
	t.Helper()
	l, err := e.svc.ClaimChunk(sessID, worker)
	must(t, err)
	key := e.putBlob(t, data)
	must(t, e.svc.StageBlob(sessID, worker, key))
	return l, key
}

func complete(t *testing.T, e *testEnv, l *ChunkLease, worker, key string) (*ChunkState, *CompletionRecord) {
	t.Helper()
	ch, rec, err := e.svc.CompleteChunk(ChunkReceipt{
		SessionID:  l.SessionID,
		ChunkIndex: l.ChunkIndex,
		LeaseID:    l.LeaseID,
		Epoch:      l.Epoch,
		WorkerID:   worker,
		BlobKey:    key,
	})
	must(t, err)
	return ch, rec
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertErrIs(t *testing.T, err error, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("expected error %v, got %v", target, err)
	}
}

// ---------------------------------------------------------------------------
// 需求 1：创建时冻结源清单快照，新版本不得混入旧会话
// ---------------------------------------------------------------------------

func TestCreateRepairFreezesManifestSnapshot(t *testing.T) {
	e := newTestEnv(t)
	v1chunks := [][]byte{[]byte("aaa"), []byte("bbb")}
	m1 := e.registerAndPublish(t, "man-v1", v1chunks)

	sess, err := e.svc.CreateRepair(CreateRepairInput{
		SourceReplica: "src", TargetReplica: "dst", ObjectID: "obj",
	})
	must(t, err)

	if sess.FrozenManifestID != m1.ID || sess.FrozenVersion != 1 {
		t.Fatalf("session not frozen to v1: %+v", sess)
	}
	if sess.TotalSize != 6 {
		t.Fatalf("total size = %d, want 6", sess.TotalSize)
	}
	if len(sess.Chunks) != 2 ||
		sess.Chunks[0].Digest != ComputeDigest(v1chunks[0]) ||
		sess.Chunks[1].Digest != ComputeDigest(v1chunks[1]) {
		t.Fatalf("session chunks not copied from v1 snapshot: %+v", sess.Chunks)
	}

	// 源端发布 v2 新清单（内容、块数、总大小均变化），并把源副本切到 v2。
	v2chunks := [][]byte{[]byte("AAAA"), []byte("bb"), []byte("cc")}
	specs2 := make([]ChunkSpec, len(v2chunks))
	var total2 int64
	for i, d := range v2chunks {
		specs2[i] = ChunkSpec{Index: i, Digest: ComputeDigest(d), Size: int64(len(d))}
		total2 += int64(len(d))
	}
	m2, err := e.svc.PublishManifest(PublishManifestInput{
		ManifestID: "man-v2", ObjectID: "obj", Version: 2,
		Chunks: specs2, Replicas: []string{"src"},
	})
	must(t, err)

	cur, err := e.svc.CurrentManifest("src", "obj")
	must(t, err)
	if cur.ID != m2.ID {
		t.Fatalf("source current = %s, want %s", cur.ID, m2.ID)
	}

	// 完成旧会话：必须按 v1 快照执行，且修复清单与 v1 一致（2 块、总大小 6）。
	for i, d := range v1chunks {
		l, err := e.svc.ClaimChunk(sess.ID, fmt.Sprintf("w%d", i))
		must(t, err)
		if l.ChunkIndex != i || l.Digest != ComputeDigest(d) {
			t.Fatalf("lease handed out non-v1 chunk spec: %+v", l)
		}
		key := e.putBlob(t, d)
		must(t, e.svc.StageBlob(sess.ID, l.WorkerID, key))
		_, rec, err := e.svc.CompleteChunk(ChunkReceipt{
			SessionID: sess.ID, ChunkIndex: l.ChunkIndex,
			LeaseID: l.LeaseID, Epoch: l.Epoch, WorkerID: l.WorkerID, BlobKey: key,
		})
		must(t, err)
		if i == len(v1chunks)-1 {
			if rec == nil {
				t.Fatal("expected completion record on final chunk")
			}
		}
	}

	got, err := e.svc.GetSession(sess.ID)
	must(t, err)
	if got.Status != SessionCompleted || got.FrozenVersion != 1 {
		t.Fatalf("session = %+v, want completed against frozen v1", got)
	}
	repaired, err := e.svc.GetManifest(got.RepairedManifestID)
	must(t, err)
	if repaired.Version != 1 || repaired.TotalSize != 6 || len(repaired.Chunks) != 2 {
		t.Fatalf("repaired manifest mixes versions: %+v", repaired)
	}
	if repaired.RepairedFrom != m1.ID {
		t.Fatalf("RepairedFrom = %q, want %q", repaired.RepairedFrom, m1.ID)
	}
	if total2 == 6 {
		t.Fatal("test setup invalid")
	}
}

func TestPublishManifestValidation(t *testing.T) {
	e := newTestEnv(t)
	must(t, e.svc.RegisterReplica("src"))

	cases := []PublishManifestInput{
		{ObjectID: "o", Version: 1, Chunks: []ChunkSpec{{Index: 0, Digest: "x", Size: 1}}},
		{ManifestID: "m", Version: 1, Chunks: []ChunkSpec{{Index: 0, Digest: "x", Size: 1}}},
		{ManifestID: "m", ObjectID: "o", Version: 0, Chunks: []ChunkSpec{{Index: 0, Digest: "x", Size: 1}}},
		{ManifestID: "m", ObjectID: "o", Version: 1},
		{ManifestID: "m", ObjectID: "o", Version: 1, Chunks: []ChunkSpec{{Index: 1, Digest: "x", Size: 1}}},
		{ManifestID: "m", ObjectID: "o", Version: 1, Chunks: []ChunkSpec{{Index: 0, Digest: "", Size: 1}}},
	}
	for i, in := range cases {
		if _, err := e.svc.PublishManifest(in); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("case %d: expected ErrInvalidArgument, got %v", i, err)
		}
	}

	good := PublishManifestInput{ManifestID: "m", ObjectID: "o", Version: 1,
		Chunks: []ChunkSpec{{Index: 0, Digest: "x", Size: 1}}}
	if _, err := e.svc.PublishManifest(good); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	if _, err := e.svc.PublishManifest(good); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate manifest id: got %v", err)
	}
	dupVer := good
	dupVer.ManifestID = "m2"
	if _, err := e.svc.PublishManifest(dupVer); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate object version: got %v", err)
	}
}
