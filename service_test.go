package goreplicarepair

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time          { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

func newTestService(t *testing.T, opts ...Option) (*Service, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)}
	all := append([]Option{WithClock(clk), WithSessionTTL(time.Minute), WithLeaseTTL(10 * time.Second)}, opts...)
	svc, err := New(all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc, clk
}

func testManifest(source, version string, payloads [][]byte, at time.Time) Manifest {
	chunks := make([]Chunk, len(payloads))
	var offset, total int64
	for i, p := range payloads {
		chunks[i] = Chunk{
			ID:     fmt.Sprintf("c%d", i+1),
			Offset: offset,
			Size:   int64(len(p)),
			Digest: DigestOf(p),
		}
		offset += int64(len(p))
		total += int64(len(p))
	}
	return Manifest{
		Source:      source,
		Version:     version,
		ObjectID:    "obj-1",
		TotalSize:   total,
		Chunks:      chunks,
		PublishedAt: at,
	}
}

func payloads() [][]byte {
	return [][]byte{[]byte("chunk-one"), []byte("chunk-two"), []byte("chunk-three")}
}

// upload/claim/verify one chunk end to end.
func repairChunk(t *testing.T, svc *Service, sessID, worker string, c Chunk, payload []byte) {
	t.Helper()
	ctx := context.Background()
	key, err := svc.UploadBlob(ctx, sessID, payload)
	if err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}
	if key != c.Digest {
		t.Fatalf("blob key %s != manifest digest %s", key, c.Digest)
	}
	lease, err := claimUntil(t, svc, sessID, worker, c.ID)
	if err != nil {
		t.Fatalf("ClaimChunk: %v", err)
	}
	_, err = svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: c.ID, LeaseID: lease.ID,
		Epoch: lease.Epoch, BlobKey: key, Digest: c.Digest,
	})
	if err != nil {
		t.Fatalf("SubmitReceipt: %v", err)
	}
}

// claimUntil claims until the lease is for the wanted chunk id.
func claimUntil(t *testing.T, svc *Service, sessID, worker, want string) (*Lease, error) {
	ctx := context.Background()
	for {
		l, err := svc.ClaimChunk(ctx, sessID, worker)
		if err != nil {
			return nil, err
		}
		if l.ChunkID == want {
			return l, nil
		}
	}
}

// ---- 1. Snapshot freeze ----

func TestCreateRepairFreezesManifest(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	v1 := testManifest("src", "v1", data, clk.Now())
	v2data := [][]byte{[]byte("new-chunk-one"), []byte("chunk-two"), []byte("chunk-three"), []byte("c4")}
	v2 := testManifest("src", "v2", v2data, clk.Now())
	if err := svc.RegisterManifest(ctx, v1); err != nil {
		t.Fatal(err)
	}
	sessID, err := svc.CreateRepair(ctx, "src", "v1", "tgt-A")
	if err != nil {
		t.Fatal(err)
	}
	// Source publishes a new manifest after the session started.
	if err := svc.RegisterManifest(ctx, v2); err != nil {
		t.Fatal(err)
	}

	p, err := svc.GetProgress(ctx, sessID)
	if err != nil {
		t.Fatal(err)
	}
	if p.ManifestVer != "v1" || p.TotalChunks != 3 {
		t.Fatalf("session not frozen on v1: %+v", p)
	}
	if p.TotalSize != v1.TotalSize {
		t.Fatalf("total size %d != frozen %d", p.TotalSize, v1.TotalSize)
	}

	for _, c := range v1.Chunks {
		repairChunk(t, svc, sessID, "w", c, data[chunkIndex(v1, c.ID)])
	}

	note, err := svc.GetResult(ctx, sessID)
	if err != nil {
		t.Fatal(err)
	}
	if note == nil || note.ManifestVer != "v1" {
		t.Fatalf("completion should reference frozen v1, got %+v", note)
	}
	cur, err := svc.GetReplicaCurrentManifest(ctx, "tgt-A")
	if err != nil {
		t.Fatal(err)
	}
	if cur != "src@v1" {
		t.Fatalf("target switched to %q, want src@v1", cur)
	}
}

func chunkIndex(m Manifest, id string) int {
	for i, c := range m.Chunks {
		if c.ID == id {
			return i
		}
	}
	return -1
}

func TestSessionCannotMixChunksAcrossManifestVersions(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	v1 := testManifest("src", "v1", data, clk.Now())
	// v2 keeps c2/c3 but replaces c1 with different bytes (and digest).
	v2data := [][]byte{[]byte("replaced-chunk-one"), data[1], data[2]}
	v2 := testManifest("src", "v2", v2data, clk.Now())
	if err := svc.RegisterManifest(ctx, v1); err != nil {
		t.Fatal(err)
	}
	sessID, err := svc.CreateRepair(ctx, "src", "v1", "tgt")
	if err != nil {
		t.Fatal(err)
	}
	// Source publishes v2 after the session froze v1.
	if err := svc.RegisterManifest(ctx, v2); err != nil {
		t.Fatal(err)
	}

	// A worker that (incorrectly) fetched c1 from the new v2 manifest uploads
	// those bytes; the receipt must be rejected because the session works
	// against the frozen v1 snapshot only.
	v2key, err := svc.UploadBlob(ctx, sessID, v2data[0])
	if err != nil {
		t.Fatal(err)
	}
	l, err := claimUntil(t, svc, sessID, "w1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l.ID,
		Epoch: l.Epoch, BlobKey: v2key, Digest: v2.Chunks[0].Digest,
	})
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("v2 chunk in v1 session: want ErrDigestMismatch, got %v", err)
	}

	// The rejected receipt did not consume the lease: the same worker can
	// still complete c1 with the correct v1 bytes under the same lease.
	key, err := svc.UploadBlob(ctx, sessID, data[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l.ID,
		Epoch: l.Epoch, BlobKey: key, Digest: v1.Chunks[0].Digest,
	}); err != nil {
		t.Fatalf("v1 receipt after mismatch: %v", err)
	}

	// The session completes against v1 content only, and the target switches
	// to v1 — never to a v1/v2 mix.
	for _, c := range v1.Chunks[1:] {
		repairChunk(t, svc, sessID, "w", c, data[chunkIndex(v1, c.ID)])
	}
	note, err := svc.GetResult(ctx, sessID)
	if err != nil {
		t.Fatal(err)
	}
	if note == nil || note.ManifestVer != "v1" || note.ChunkCount != len(v1.Chunks) {
		t.Fatalf("completion = %+v, want frozen v1", note)
	}
	if cur, _ := svc.GetReplicaCurrentManifest(ctx, "tgt"); cur != "src@v1" {
		t.Fatalf("target = %q, want src@v1", cur)
	}
}

func TestCannotCreateSessionForUnknownManifest(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	if _, err := svc.CreateRepair(ctx, "src", "nope", "t"); !errors.Is(err, ErrManifestNotFound) {
		t.Fatalf("want ErrManifestNotFound, got %v", err)
	}
}

func TestDuplicateManifestRejected(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	m := testManifest("src", "v1", payloads(), clk.Now())
	if err := svc.RegisterManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterManifest(ctx, m); !errors.Is(err, ErrManifestExists) {
		t.Fatalf("want ErrManifestExists, got %v", err)
	}
}

func TestManifestValidation(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	bad := testManifest("src", "v1", payloads(), clk.Now())
	bad.TotalSize++ // offsets no longer sum to total size
	if err := svc.RegisterManifest(ctx, bad); !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("want ErrManifestInvalid, got %v", err)
	}
}

// ---- 2. Leases, epochs, stale receipts ----

func TestLeaseExpiryReclaimBumpsEpoch(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	m := testManifest("src", "v1", payloads(), clk.Now())
	if err := svc.RegisterManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")

	l1, err := svc.ClaimChunk(ctx, sessID, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if l1.Epoch != 1 {
		t.Fatalf("first epoch = %d, want 1", l1.Epoch)
	}
	// Nothing else available while the single... there are 3 chunks, so claim
	// twice more to exhaust, then expiry must make l1 reclaimable.
	if _, err := svc.ClaimChunk(ctx, sessID, "w1"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ClaimChunk(ctx, sessID, "w1"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ClaimChunk(ctx, sessID, "w1"); !errors.Is(err, ErrNoChunkAvailable) {
		t.Fatalf("want ErrNoChunkAvailable, got %v", err)
	}
	clk.advance(11 * time.Second)
	l2, err := svc.ClaimChunk(ctx, sessID, "w2")
	if err != nil {
		t.Fatal(err)
	}
	if l2.ChunkID != l1.ChunkID {
		t.Fatalf("expected reclaim of %s, got %s", l1.ChunkID, l2.ChunkID)
	}
	if l2.ID == l1.ID || l2.Epoch != 2 {
		t.Fatalf("reclaim must produce new lease/epoch: %+v vs %+v", l1, l2)
	}
}

func TestStaleReceiptCannotOverwrite(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	if err := svc.RegisterManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")

	key, err := svc.UploadBlob(ctx, sessID, data[0])
	if err != nil {
		t.Fatal(err)
	}
	l1, err := claimUntil(t, svc, sessID, "w1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	// w1 goes silent; its lease expires and w2 takes over with epoch 2.
	clk.advance(11 * time.Second)
	l2, err := claimUntil(t, svc, sessID, "w2", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if l2.Epoch != 2 {
		t.Fatalf("takeover epoch = %d, want 2", l2.Epoch)
	}

	// w1's late receipt (epoch 1) must be rejected.
	_, err = svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l1.ID,
		Epoch: l1.Epoch, BlobKey: key, Digest: m.Chunks[0].Digest,
	})
	if !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale receipt: want ErrLeaseMismatch, got %v", err)
	}

	// w2's valid receipt wins and verifies the chunk.
	if _, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l2.ID,
		Epoch: l2.Epoch, BlobKey: key, Digest: m.Chunks[0].Digest,
	}); err != nil {
		t.Fatalf("takeover receipt: %v", err)
	}

	// w1 retrying yet again still cannot overwrite the verified result.
	_, err = svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l1.ID,
		Epoch: l1.Epoch, BlobKey: key, Digest: m.Chunks[0].Digest,
	})
	if !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("post-verify stale receipt: want ErrLeaseMismatch, got %v", err)
	}

	p, _ := svc.GetProgress(ctx, sessID)
	if p.Verified != 1 {
		t.Fatalf("verified = %d, want 1", p.Verified)
	}
}

func TestExpiredLeaseReceiptRejected(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	key, _ := svc.UploadBlob(ctx, sessID, data[0])
	l, _ := claimUntil(t, svc, sessID, "w1", "c1")
	clk.advance(11 * time.Second) // lease expires; nobody reclaims yet
	_, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l.ID,
		Epoch: l.Epoch, BlobKey: key, Digest: m.Chunks[0].Digest,
	})
	if !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("expired lease receipt: want ErrLeaseMismatch, got %v", err)
	}
}

func TestDigestMismatchRejected(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")

	// Upload bytes whose digest differs from the frozen manifest.
	wrong := []byte("this-is-not-chunk-one")
	key, err := svc.UploadBlob(ctx, sessID, wrong)
	if err != nil {
		t.Fatal(err)
	}
	l, _ := claimUntil(t, svc, sessID, "w1", "c1")
	_, err = svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l.ID,
		Epoch: l.Epoch, BlobKey: key, Digest: DigestOf(wrong),
	})
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("want ErrDigestMismatch, got %v", err)
	}

	// Claiming the manifest digest but lying in the reported digest is also
	// rejected.
	_, err = svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l.ID,
		Epoch: l.Epoch, BlobKey: m.Chunks[0].Digest, Digest: key,
	})
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("want ErrDigestMismatch, got %v", err)
	}

	p, _ := svc.GetProgress(ctx, sessID)
	if p.Verified != 0 || p.Leased != 1 {
		t.Fatalf("chunk must remain leased, got %+v", p)
	}
}

func TestMissingBlobRejected(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	m := testManifest("src", "v1", payloads(), clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	l, _ := claimUntil(t, svc, sessID, "w1", "c1")
	_, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: l.ID,
		Epoch: l.Epoch, BlobKey: m.Chunks[0].Digest, Digest: m.Chunks[0].Digest,
	})
	if !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("want ErrBlobNotFound, got %v", err)
	}
}

func TestDuplicateWinningReceiptIdempotent(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	key, _ := svc.UploadBlob(ctx, sessID, data[0])
	l, _ := claimUntil(t, svc, sessID, "w1", "c1")
	r := Receipt{SessionID: sessID, ChunkID: "c1", LeaseID: l.ID,
		Epoch: l.Epoch, BlobKey: key, Digest: m.Chunks[0].Digest}
	if _, err := svc.SubmitReceipt(ctx, r); err != nil {
		t.Fatal(err)
	}
	res, err := svc.SubmitReceipt(ctx, r) // at-least-once retry
	if err != nil || !res.Accepted {
		t.Fatalf("duplicate receipt must be idempotent, got %v %+v", err, res)
	}
}

// ---- 3. Atomic completion, single switch/notification, no early exposure ----

func TestTargetNotExposedUntilAllChunksVerified(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")

	repairChunk(t, svc, sessID, "w", m.Chunks[0], data[0])
	cur, _ := svc.GetReplicaCurrentManifest(ctx, "tgt")
	if cur != "" {
		t.Fatalf("target exposed early: %q", cur)
	}
	if note, _ := svc.GetResult(ctx, sessID); note != nil {
		t.Fatalf("notification emitted early: %+v", note)
	}
	repairChunk(t, svc, sessID, "w", m.Chunks[1], data[1])
	cur, _ = svc.GetReplicaCurrentManifest(ctx, "tgt")
	if cur != "" {
		t.Fatalf("target exposed before last chunk: %q", cur)
	}
	repairChunk(t, svc, sessID, "w", m.Chunks[2], data[2])
	cur, _ = svc.GetReplicaCurrentManifest(ctx, "tgt")
	if cur != "src@v1" {
		t.Fatalf("target not switched after completion: %q", cur)
	}
}

func TestConcurrentCompletionSingleSwitch(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	for _, p := range data {
		if _, err := svc.UploadBlob(ctx, sessID, p); err != nil {
			t.Fatal(err)
		}
	}

	// Hand out the three leases serially, then have all three workers submit
	// their receipts concurrently. The last finisher performs the only switch
	// inside the serialized critical section.
	leases := make([]*Lease, len(m.Chunks))
	for i := range m.Chunks {
		l, err := claimUntil(t, svc, sessID, fmt.Sprintf("w%d", i), m.Chunks[i].ID)
		if err != nil {
			t.Fatal(err)
		}
		leases[i] = l
	}
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := range m.Chunks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := m.Chunks[i]
			_, err := svc.SubmitReceipt(ctx, Receipt{
				SessionID: sessID, ChunkID: c.ID, LeaseID: leases[i].ID,
				Epoch: leases[i].Epoch, BlobKey: c.Digest, Digest: c.Digest,
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	p, err := svc.GetProgress(ctx, sessID)
	if err != nil {
		t.Fatal(err)
	}
	if p.State != SessionSucceeded || p.Verified != 3 {
		t.Fatalf("session = %+v", p)
	}
	n1, _ := svc.GetResult(ctx, sessID)
	n2, _ := svc.GetResult(ctx, sessID)
	if n1 == nil || n1.EmittedAt != n2.EmittedAt {
		t.Fatalf("notification must be unique and stable: %+v %+v", n1, n2)
	}
	// No new leases after the switch.
	if _, err := svc.ClaimChunk(ctx, sessID, "w"); !errors.Is(err, ErrSessionNotRunning) {
		t.Fatalf("post-completion claim: want ErrSessionNotRunning, got %v", err)
	}
}

func TestConcurrentClaimsAssignEachChunkOnce(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	m := testManifest("src", "v1", payloads(), clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")

	var mu sync.Mutex
	got := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			for {
				l, err := svc.ClaimChunk(ctx, sessID, worker)
				if errors.Is(err, ErrNoChunkAvailable) {
					return
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				mu.Lock()
				got[l.ChunkID]++
				mu.Unlock()
			}
		}(fmt.Sprintf("w%d", w))
	}
	wg.Wait()
	if len(got) != 3 {
		t.Fatalf("got %v, want 3 distinct chunks", got)
	}
	for id, n := range got {
		if n != 1 {
			t.Fatalf("chunk %s claimed %d times concurrently", id, n)
		}
	}
}

// ---- 4. Cancel / timeout / cleanup with reference protection ----

func TestCancelStopsLeasesAndExposesCleanup(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	m := testManifest("src", "v1", payloads(), clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	if _, err := svc.UploadBlob(ctx, sessID, data0()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Cancel(ctx, sessID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ClaimChunk(ctx, sessID, "w"); !errors.Is(err, ErrSessionNotRunning) {
		t.Fatalf("claim after cancel: want ErrSessionNotRunning, got %v", err)
	}
	if _, err := svc.UploadBlob(ctx, sessID, []byte("x")); !errors.Is(err, ErrSessionNotRunning) {
		t.Fatalf("upload after cancel: want ErrSessionNotRunning, got %v", err)
	}
	p, _ := svc.GetProgress(ctx, sessID)
	if p.State != SessionCancelled {
		t.Fatalf("state = %s", p.State)
	}
	keys, err := svc.ListCleanable(ctx, sessID)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != m.Chunks[0].Digest {
		t.Fatalf("cleanable = %v", keys)
	}
}

func data0() []byte { return payloads()[0] }

func TestTimeoutStopsLeases(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t, WithSessionTTL(30*time.Second))
	m := testManifest("src", "v1", payloads(), clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")

	clk.advance(31 * time.Second)
	if err := svc.AdvanceTimeouts(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ClaimChunk(ctx, sessID, "w"); !errors.Is(err, ErrSessionNotRunning) {
		t.Fatalf("claim after timeout: want ErrSessionNotRunning, got %v", err)
	}
	p, _ := svc.GetProgress(ctx, sessID)
	if p.State != SessionTimedOut || p.FinishedAt == nil {
		t.Fatalf("progress = %+v", p)
	}
}

func TestTimeoutOrphansBecomeCleanable(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t, WithSessionTTL(30*time.Second))
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	key, err := svc.UploadBlob(ctx, sessID, data[0])
	if err != nil {
		t.Fatal(err)
	}

	clk.advance(31 * time.Second)
	if err := svc.AdvanceTimeouts(ctx); err != nil {
		t.Fatal(err)
	}
	p, _ := svc.GetProgress(ctx, sessID)
	if p.State != SessionTimedOut {
		t.Fatalf("state = %s, want timed_out", p.State)
	}

	// The uploaded chunk was never referenced by a successful manifest, so it
	// is cleanable; cleanup removes exactly that blob.
	keys, err := svc.ListCleanable(ctx, sessID)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("cleanable = %v, want [%s]", keys, key)
	}
	n, err := svc.CleanupSession(ctx, sessID)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("cleanup removed %d, want 1", n)
	}
	if _, exists := svc.k.blobs[key]; exists {
		t.Fatal("orphan blob still present after cleanup")
	}
	if _, err := svc.CleanupSession(ctx, sessID); !errors.Is(err, ErrNothingToClean) {
		t.Fatalf("second cleanup: want ErrNothingToClean, got %v", err)
	}
}

func TestCleanupDoesNotDeleteReferencedBlobs(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)

	// Session A: cancelled after uploading c1 (shared digest with the object).
	aID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt-A")
	sharedKey, _ := svc.UploadBlob(ctx, aID, data[0])
	// Session A also uploads a private abandoned attempt for c2.
	privateKey, _ := svc.UploadBlob(ctx, aID, []byte("abandoned-private-bytes"))
	if err := svc.Cancel(ctx, aID); err != nil {
		t.Fatal(err)
	}

	// Session B: completes successfully on another target, its replica now
	// references the shared c1 digest.
	bID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt-B")
	for _, c := range m.Chunks {
		repairChunk(t, svc, bID, "w", c, data[chunkIndex(m, c.ID)])
	}

	keys, err := svc.ListCleanable(ctx, aID)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if k == sharedKey {
			t.Fatal("cleanup would delete blob still referenced by valid replica tgt-B")
		}
	}
	n, err := svc.CleanupSession(ctx, aID)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("cleanup removed %d blobs, want 1", n)
	}
	// The private blob is gone after cleanup.
	if _, exists := svc.k.blobs[privateKey]; exists {
		t.Fatal("private blob still present after cleanup")
	}
	if _, exists := svc.k.blobs[sharedKey]; !exists {
		t.Fatal("shared blob wrongly deleted")
	}

	// Successful session B has nothing orphaned: all its uploads are pinned
	// by tgt-B's current manifest.
	if _, err := svc.CleanupSession(ctx, bID); !errors.Is(err, ErrNothingToClean) {
		t.Fatalf("successful session cleanup: want ErrNothingToClean, got %v", err)
	}
}

func TestCleanupRunningSessionRejected(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	m := testManifest("src", "v1", payloads(), clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	if _, err := svc.ListCleanable(ctx, sessID); !errors.Is(err, ErrSessionTerminal) {
		t.Fatalf("want ErrSessionTerminal, got %v", err)
	}
}

// ---- 5. Persistence across restart ----

func TestStateSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	clk := &fakeClock{t: time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)}
	svc, err := New(WithClock(clk), WithPersistence(FilePersistence{Path: path}),
		WithSessionTTL(time.Minute), WithLeaseTTL(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	repairChunk(t, svc, sessID, "w", m.Chunks[0], data[0])
	// Upload c2's blob and take its lease before the restart.
	if _, err := svc.UploadBlob(ctx, sessID, data[1]); err != nil {
		t.Fatal(err)
	}
	lease2, err := svc.ClaimChunk(ctx, sessID, "w")
	if err != nil {
		t.Fatal(err)
	}
	if lease2.ChunkID != "c2" {
		t.Fatalf("expected c2 lease, got %s", lease2.ChunkID)
	}

	// Restart with the same clock value: all leases and progress survive.
	svc2, err := New(WithClock(clk), WithPersistence(FilePersistence{Path: path}),
		WithSessionTTL(time.Minute), WithLeaseTTL(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := svc2.GetProgress(ctx, sessID)
	if p.Verified != 1 || p.Leased != 1 || p.ManifestVer != "v1" {
		t.Fatalf("progress lost across restart: %+v", p)
	}
	// The pre-restart lease still validates and finishes c2.
	if _, err := svc2.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c2", LeaseID: lease2.ID,
		Epoch: lease2.Epoch, BlobKey: m.Chunks[1].Digest, Digest: m.Chunks[1].Digest,
	}); err != nil {
		t.Fatalf("receipt after restart: %v", err)
	}
	repairChunk(t, svc2, sessID, "w", m.Chunks[2], data[2])
	note, err := svc2.GetResult(ctx, sessID)
	if err != nil || note == nil || note.ManifestVer != "v1" {
		t.Fatalf("completion after restart = %v %+v", err, note)
	}
	cur, _ := svc2.GetReplicaCurrentManifest(ctx, "tgt")
	if cur != "src@v1" {
		t.Fatalf("target manifest after restart = %q", cur)
	}
}

func TestProgressTracksSize(t *testing.T) {
	ctx := context.Background()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	svc.RegisterManifest(ctx, m)
	sessID, _ := svc.CreateRepair(ctx, "src", "v1", "tgt")
	repairChunk(t, svc, sessID, "w", m.Chunks[0], data[0])
	p, _ := svc.GetProgress(ctx, sessID)
	if p.VerifiedSize != m.Chunks[0].Size || p.Pending != 2 {
		t.Fatalf("progress = %+v", p)
	}
}
