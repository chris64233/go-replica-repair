package goreplicarepair

import (
	"context"
	"errors"
	"testing"
	"time"
)

// msRequest builds a valid multi-source request: two candidates, src-a owns
// c1+c2, src-b owns c2+c3, all digests matching the target manifest.
func msRequest(m Manifest) MultiSourceRequest {
	return MultiSourceRequest{
		ID:       "ms-1",
		TargetID: "replica-t",
		Manifest: m,
		Candidates: []SourceCandidate{
			{SourceID: "src-a", Version: "v1", Chunks: []ChunkDeclaration{
				{ChunkID: "c1", Digest: m.Chunks[0].Digest},
				{ChunkID: "c2", Digest: m.Chunks[1].Digest},
			}},
			{SourceID: "src-b", Version: "v1", Chunks: []ChunkDeclaration{
				{ChunkID: "c2", Digest: m.Chunks[1].Digest},
				{ChunkID: "c3", Digest: m.Chunks[2].Digest},
			}},
		},
	}
}

func TestMultiSourceFreezeAndStableSelection(t *testing.T) {
	svc, clk := newTestService(t)
	ctx := context.Background()
	m := testManifest("origin", "v9", payloads(), clk.Now())

	sessID, err := svc.CreateMultiSourceRepair(ctx, msRequest(m))
	if err != nil {
		t.Fatalf("CreateMultiSourceRepair: %v", err)
	}

	// Source updates after creation must not change the session basis.
	_ = svc.RegisterManifest(ctx, testManifest("src-a", "v2", payloads(), clk.Now()))

	view, err := svc.GetMultiSourceView(ctx, sessID)
	if err != nil {
		t.Fatalf("GetMultiSourceView: %v", err)
	}
	if view.State != SessionRunning || view.ManifestVer != "v9" {
		t.Fatalf("unexpected view: %+v", view)
	}
	// Deterministic rule: smallest (SourceID, Version) among owners.
	want := map[string]string{"c1": "src-a", "c2": "src-a", "c3": "src-b"}
	for _, cv := range view.Chunks {
		if cv.SourceID != want[cv.ChunkID] {
			t.Fatalf("chunk %s source = %q, want %q", cv.ChunkID, cv.SourceID, want[cv.ChunkID])
		}
		mc := findManifestChunk(m, cv.ChunkID)
		if cv.Digest != mc.Digest {
			t.Fatalf("chunk %s digest mismatch", cv.ChunkID)
		}
	}
	// Re-querying is stable.
	view2, _ := svc.GetMultiSourceView(ctx, sessID)
	for i := range view.Chunks {
		if view.Chunks[i] != view2.Chunks[i] {
			t.Fatalf("selection not stable across queries")
		}
	}
}

func TestMultiSourceDigestConflictBlocks(t *testing.T) {
	svc, clk := newTestService(t)
	ctx := context.Background()
	m := testManifest("origin", "v9", payloads(), clk.Now())
	req := msRequest(m)
	req.Candidates[1].Chunks[0].Digest = DigestOf([]byte("forged-c2"))

	sessID, err := svc.CreateMultiSourceRepair(ctx, req)
	if err != nil {
		t.Fatalf("CreateMultiSourceRepair: %v", err)
	}
	view, _ := svc.GetMultiSourceView(ctx, sessID)
	if view.State != SessionBlocked {
		t.Fatalf("state = %s, want blocked", view.State)
	}
	if len(view.Divergences) != 1 || view.Divergences[0].ChunkID != "c2" ||
		view.Divergences[0].Kind != DivergenceDigestConflict {
		t.Fatalf("divergences = %+v", view.Divergences)
	}
	if _, err := svc.ClaimChunk(ctx, sessID, "w1"); !errors.Is(err, ErrSessionBlocked) {
		t.Fatalf("ClaimChunk err = %v, want ErrSessionBlocked", err)
	}
	// A blocked session can be cancelled to converge.
	if err := svc.Cancel(ctx, sessID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}

func TestMultiSourceNoSourceBlocks(t *testing.T) {
	svc, clk := newTestService(t)
	ctx := context.Background()
	m := testManifest("origin", "v9", payloads(), clk.Now())
	req := msRequest(m)
	req.Candidates[1].Chunks = req.Candidates[1].Chunks[:1] // src-b drops c3

	sessID, err := svc.CreateMultiSourceRepair(ctx, req)
	if err != nil {
		t.Fatalf("CreateMultiSourceRepair: %v", err)
	}
	view, _ := svc.GetMultiSourceView(ctx, sessID)
	if view.State != SessionBlocked {
		t.Fatalf("state = %s, want blocked", view.State)
	}
	if len(view.Divergences) != 1 || view.Divergences[0].Kind != DivergenceNoSource ||
		view.Divergences[0].ChunkID != "c3" {
		t.Fatalf("divergences = %+v", view.Divergences)
	}
}

func TestMultiSourceIdempotencyAndConflict(t *testing.T) {
	svc, clk := newTestService(t)
	ctx := context.Background()
	m := testManifest("origin", "v9", payloads(), clk.Now())
	req := msRequest(m)

	id1, err := svc.CreateMultiSourceRepair(ctx, req)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Same session number + same candidate set (even reordered) → original.
	reordered := msRequest(m)
	reordered.Candidates[0], reordered.Candidates[1] = reordered.Candidates[1], reordered.Candidates[0]
	id2, err := svc.CreateMultiSourceRepair(ctx, reordered)
	if err != nil || id2 != id1 {
		t.Fatalf("idempotent create = %q, %v; want %q", id2, err, id1)
	}
	// Changed candidate version → conflict.
	c1 := msRequest(m)
	c1.Candidates[0].Version = "v2"
	if _, err := svc.CreateMultiSourceRepair(ctx, c1); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("candidate version change err = %v", err)
	}
	// Changed declared digest → conflict.
	c2 := msRequest(m)
	c2.Candidates[0].Chunks[0].Digest = DigestOf([]byte("other"))
	if _, err := svc.CreateMultiSourceRepair(ctx, c2); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("digest change err = %v", err)
	}
	// Changed target manifest → conflict.
	c3 := msRequest(m)
	c3.Manifest.Version = "v10"
	if _, err := svc.CreateMultiSourceRepair(ctx, c3); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("manifest change err = %v", err)
	}
}

// claimFor claims until the lease covers chunkID and returns it.
func claimFor(t *testing.T, svc *Service, sessID, worker, chunkID string) *Lease {
	t.Helper()
	for {
		l, err := svc.ClaimChunk(context.Background(), sessID, worker)
		if err != nil {
			t.Fatalf("ClaimChunk: %v", err)
		}
		if l.ChunkID == chunkID {
			return l
		}
	}
}

func TestMultiSourceLeaseReceiptAndTakeover(t *testing.T) {
	svc, clk := newTestService(t)
	ctx := context.Background()
	m := testManifest("origin", "v9", payloads(), clk.Now())
	sessID, err := svc.CreateMultiSourceRepair(ctx, msRequest(m))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	lease := claimFor(t, svc, sessID, "w1", "c1")
	if lease.SourceID != "src-a" || lease.SessionVersion != 1 {
		t.Fatalf("lease = %+v", lease)
	}
	key, err := svc.UploadBlob(ctx, sessID, []byte("chunk-one"))
	if err != nil {
		t.Fatalf("UploadBlob: %v", err)
	}

	// Wrong source on the receipt → rejected.
	if _, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: lease.ID, Epoch: lease.Epoch,
		SourceID: "src-b", SessionVersion: 1, BlobKey: key, Digest: key,
	}); !errors.Is(err, ErrSourceMismatch) {
		t.Fatalf("wrong source err = %v", err)
	}
	// Wrong session version → rejected.
	if _, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: lease.ID, Epoch: lease.Epoch,
		SourceID: "src-a", SessionVersion: 2, BlobKey: key, Digest: key,
	}); !errors.Is(err, ErrSourceMismatch) {
		t.Fatalf("wrong version err = %v", err)
	}

	// Lease expires and is taken over by another worker.
	clk.advance(11 * time.Second)
	lease2 := claimFor(t, svc, sessID, "w2", "c1")
	if lease2.ID == lease.ID || lease2.Epoch == lease.Epoch {
		t.Fatalf("takeover did not rotate lease: %+v", lease2)
	}
	// The stale worker's late receipt must not win.
	if _, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: lease.ID, Epoch: lease.Epoch,
		SourceID: "src-a", SessionVersion: 1, BlobKey: key, Digest: key,
	}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale receipt err = %v", err)
	}
	// The takeover worker's receipt verifies the chunk.
	res, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: lease2.ID, Epoch: lease2.Epoch,
		SourceID: "src-a", SessionVersion: 1, BlobKey: key, Digest: key,
	})
	if err != nil || !res.Accepted {
		t.Fatalf("takeover receipt: %v %+v", err, res)
	}
	// Idempotent retry by the winning lease is accepted...
	if _, err := svc.SubmitReceipt(ctx, Receipt{
		SessionID: sessID, ChunkID: "c1", LeaseID: lease2.ID, Epoch: lease2.Epoch,
		SourceID: "src-a", SessionVersion: 1, BlobKey: key, Digest: key,
	}); err != nil {
		t.Fatalf("idempotent retry should succeed: %v", err)
	}
	// ...but a verified chunk cannot be claimed or rewritten by anyone else.
	if _, err := svc.ClaimChunk(ctx, sessID, "w3"); err != nil {
		t.Fatalf("claim next: %v", err)
	}
	view, _ := svc.GetMultiSourceView(ctx, sessID)
	for _, cv := range view.Chunks {
		if cv.ChunkID == "c1" {
			if cv.State != ChunkVerified || cv.FinalLease != lease2.ID || cv.FinalEpoch != lease2.Epoch {
				t.Fatalf("c1 view = %+v", cv)
			}
		}
	}
}

func TestMultiSourcePublishAndResult(t *testing.T) {
	svc, clk := newTestService(t)
	ctx := context.Background()
	m := testManifest("origin", "v9", payloads(), clk.Now())
	sessID, err := svc.CreateMultiSourceRepair(ctx, msRequest(m))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Before all chunks verified, no result and no publish.
	if note, _ := svc.GetResult(ctx, sessID); note != nil {
		t.Fatalf("premature notification: %+v", note)
	}
	for i, p := range payloads() {
		chunkID := m.Chunks[i].ID
		key, err := svc.UploadBlob(ctx, sessID, p)
		if err != nil {
			t.Fatalf("UploadBlob: %v", err)
		}
		l := claimFor(t, svc, sessID, "w1", chunkID)
		if _, err := svc.SubmitReceipt(ctx, Receipt{
			SessionID: sessID, ChunkID: chunkID, LeaseID: l.ID, Epoch: l.Epoch,
			SourceID: l.SourceID, SessionVersion: l.SessionVersion,
			BlobKey: key, Digest: key,
		}); err != nil {
			t.Fatalf("SubmitReceipt %s: %v", chunkID, err)
		}
	}
	note, err := svc.GetResult(ctx, sessID)
	if err != nil || note == nil {
		t.Fatalf("GetResult: %v %+v", err, note)
	}
	if note.ManifestVer != "v9" || note.ChunkCount != 3 {
		t.Fatalf("note = %+v", note)
	}
	cur, _ := svc.GetReplicaCurrentManifest(ctx, "replica-t")
	if cur != "origin@v9" {
		t.Fatalf("current manifest = %q", cur)
	}
	// Same session number + same candidates returns the original result.
	id2, err := svc.CreateMultiSourceRepair(ctx, msRequest(m))
	if err != nil || id2 != sessID {
		t.Fatalf("idempotent create = %q, %v", id2, err)
	}
	view, _ := svc.GetMultiSourceView(ctx, sessID)
	if view.State != SessionSucceeded {
		t.Fatalf("state = %s", view.State)
	}
	for _, cv := range view.Chunks {
		if cv.State != ChunkVerified || cv.VerifiedAt == nil {
			t.Fatalf("chunk view = %+v", cv)
		}
	}
}

func TestMultiSourcePersistenceRoundTrip(t *testing.T) {
	store := &memoryPersistence{}
	clk := &fakeClock{t: time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)}
	newSvc := func() *Service {
		svc, err := New(WithClock(clk), WithPersistence(store),
			WithSessionTTL(time.Minute), WithLeaseTTL(10*time.Second))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return svc
	}
	ctx := context.Background()
	m := testManifest("origin", "v9", payloads(), clk.Now())
	svc := newSvc()
	sessID, err := svc.CreateMultiSourceRepair(ctx, msRequest(m))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Restart: frozen selection survives.
	svc2 := newSvc()
	view, err := svc2.GetMultiSourceView(ctx, sessID)
	if err != nil {
		t.Fatalf("view after restart: %v", err)
	}
	if view.State != SessionRunning || len(view.Candidates) != 2 {
		t.Fatalf("view = %+v", view)
	}
	if view.Chunks[0].SourceID != "src-a" || view.Chunks[2].SourceID != "src-b" {
		t.Fatalf("selection lost after restart: %+v", view.Chunks)
	}
	// Idempotent create still returns the original session after restart.
	id2, err := svc2.CreateMultiSourceRepair(ctx, msRequest(m))
	if err != nil || id2 != sessID {
		t.Fatalf("idempotent after restart = %q, %v", id2, err)
	}
}
