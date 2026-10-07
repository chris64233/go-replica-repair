package goreplicarepair

import (
	"context"
	"errors"
	"testing"
	"time"
)

func multiPayloads() [][]byte {
	return [][]byte{[]byte("alpha-one"), []byte("beta-two"), []byte("gamma-three")}
}

// candidate declares chunks[indices] of payloads for one source.
func candidate(sourceID, version string, payloads [][]byte, indices ...int) SourceCandidate {
	c := SourceCandidate{SourceID: sourceID, ManifestVersion: version}
	for _, i := range indices {
		c.Chunks = append(c.Chunks, SourceChunk{
			ID:     chunkID(i),
			Size:   int64(len(payloads[i])),
			Digest: DigestOf(payloads[i]),
		})
	}
	return c
}

func chunkID(i int) string {
	return []string{"c1", "c2", "c3"}[i]
}

func multiRequest(sessionID string, candidates ...SourceCandidate) MultiRepairRequest {
	return MultiRepairRequest{
		SessionID:  sessionID,
		TargetID:   "replica-t",
		ObjectID:   "obj-1",
		Candidates: candidates,
	}
}

func TestMultiSourceStableAssignmentAndFreeze(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	p := multiPayloads()

	// src-a holds c1+c2, src-b holds c2+c3: no single source covers all.
	cands := []SourceCandidate{
		candidate("src-a", "v1", p, 0, 1),
		candidate("src-b", "v1", p, 1, 2),
	}
	plan, err := svc.CreateMultiSourceRepair(ctx, multiRequest("ms-1", cands...))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if plan.Blocked || len(plan.Divergences) != 0 {
		t.Fatalf("unexpected block: %+v", plan)
	}
	// c2 is offered by both; the stable rule picks the smaller source id.
	want := map[string]string{"c1": "src-a", "c2": "src-a", "c3": "src-b"}
	for _, ch := range plan.Chunks {
		if ch.SourceID != want[ch.ChunkID] {
			t.Fatalf("chunk %s assigned to %q, want %q", ch.ChunkID, ch.SourceID, want[ch.ChunkID])
		}
		if ch.Digest != DigestOf(p[int(ch.ChunkID[1]-'1')]) {
			t.Fatalf("chunk %s digest mismatch", ch.ChunkID)
		}
	}

	// Mutating the caller's candidate afterwards must not change the frozen
	// session: the selection basis is fixed at creation.
	cands[0].Chunks[0].Digest = "tampered"
	cands[1].ManifestVersion = "v2"
	again, err := svc.GetRepairPlan(ctx, "ms-1")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	for _, ch := range again.Chunks {
		if ch.Digest == "tampered" {
			t.Fatal("frozen digest changed after caller mutation")
		}
	}
	for _, c := range again.Candidates {
		if c.SourceID == "src-b" && c.ManifestVersion != "v1" {
			t.Fatal("frozen candidate version changed after caller mutation")
		}
	}
}

func TestMultiSourceIdempotencyAndConflict(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	p := multiPayloads()
	req := multiRequest("ms-1",
		candidate("src-a", "v1", p, 0, 1),
		candidate("src-b", "v1", p, 2),
	)
	first, err := svc.CreateMultiSourceRepair(ctx, req)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Same session number + same candidate set returns the original result.
	second, err := svc.CreateMultiSourceRepair(ctx, req)
	if err != nil {
		t.Fatalf("idempotent re-create: %v", err)
	}
	if second.SessionID != first.SessionID || second.ManifestVer != first.ManifestVer {
		t.Fatalf("idempotent re-create returned different session: %+v", second)
	}

	// Changed candidate manifest version conflicts.
	changedVersion := multiRequest("ms-1",
		candidate("src-a", "v2", p, 0, 1),
		candidate("src-b", "v1", p, 2),
	)
	if _, err := svc.CreateMultiSourceRepair(ctx, changedVersion); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("version change: got %v, want ErrSessionConflict", err)
	}
	// Changed digest conflicts.
	badDigest := multiRequest("ms-1",
		candidate("src-a", "v1", p, 0, 1),
		candidate("src-b", "v1", p, 2),
	)
	badDigest.Candidates[1].Chunks[0].Digest = "different"
	if _, err := svc.CreateMultiSourceRepair(ctx, badDigest); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("digest change: got %v, want ErrSessionConflict", err)
	}
	// Changed target conflicts.
	otherTarget := multiRequest("ms-1",
		candidate("src-a", "v1", p, 0, 1),
		candidate("src-b", "v1", p, 2),
	)
	otherTarget.TargetID = "replica-other"
	if _, err := svc.CreateMultiSourceRepair(ctx, otherTarget); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("target change: got %v, want ErrSessionConflict", err)
	}
	// The original session is untouched by all conflicting attempts.
	plan, err := svc.GetRepairPlan(ctx, "ms-1")
	if err != nil || plan.State != SessionRunning {
		t.Fatalf("original session damaged: %v %+v", err, plan)
	}
}

func TestMultiSourceDivergenceBlocksSession(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	p := multiPayloads()

	a := candidate("src-a", "v1", p, 0, 1)
	b := candidate("src-b", "v1", p, 1, 2)
	// The sources disagree on c2's digest.
	b.Chunks[0].Digest = DigestOf([]byte("forged-two"))

	plan, err := svc.CreateMultiSourceRepair(ctx, multiRequest("ms-1", a, b))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !plan.Blocked {
		t.Fatal("session with diverging digests must be blocked")
	}
	if len(plan.Divergences) != 1 || plan.Divergences[0].ChunkID != "c2" {
		t.Fatalf("divergence not recorded: %+v", plan.Divergences)
	}
	if len(plan.Divergences[0].Declarations) != 2 {
		t.Fatalf("divergence must keep both declarations: %+v", plan.Divergences[0])
	}

	// A blocked session refuses to execute: no leases, no uploads, no receipts.
	if _, err := svc.ClaimChunk(ctx, "ms-1", "w1"); !errors.Is(err, ErrSessionBlocked) {
		t.Fatalf("claim on blocked session: got %v", err)
	}
	if _, err := svc.UploadBlob(ctx, "ms-1", p[0]); !errors.Is(err, ErrSessionBlocked) {
		t.Fatalf("upload on blocked session: got %v", err)
	}
	if _, err := svc.SubmitReceipt(ctx, Receipt{SessionID: "ms-1", ChunkID: "c1"}); !errors.Is(err, ErrSessionBlocked) {
		t.Fatalf("receipt on blocked session: got %v", err)
	}
	if note, _ := svc.GetResult(ctx, "ms-1"); note != nil {
		t.Fatal("blocked session must never publish")
	}
}

func TestMultiSourceReceiptTripleAndTakeover(t *testing.T) {
	svc, clk := newTestService(t)
	ctx := context.Background()
	p := multiPayloads()

	if _, err := svc.CreateMultiSourceRepair(ctx, multiRequest("ms-1",
		candidate("src-a", "v1", p, 0),
		candidate("src-b", "v1", p, 1),
	)); err != nil {
		t.Fatalf("create: %v", err)
	}

	lease, err := svc.ClaimChunk(ctx, "ms-1", "w1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if lease.SourceID == "" || lease.SessionVersion == 0 {
		t.Fatalf("lease must carry source and session version: %+v", lease)
	}
	key, err := svc.UploadBlob(ctx, "ms-1", p[0])
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	receipt := Receipt{
		SessionID: "ms-1", ChunkID: lease.ChunkID, LeaseID: lease.ID,
		Epoch: lease.Epoch, BlobKey: key, Digest: key,
		SourceID: lease.SourceID, SessionVersion: lease.SessionVersion,
	}

	// Wrong session version is rejected.
	bad := receipt
	bad.SessionVersion++
	if _, err := svc.SubmitReceipt(ctx, bad); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("wrong session version: got %v", err)
	}
	// Wrong source is rejected.
	bad = receipt
	bad.SourceID = "src-b"
	if _, err := svc.SubmitReceipt(ctx, bad); !errors.Is(err, ErrSourceMismatch) {
		t.Fatalf("wrong source: got %v", err)
	}
	// The full matching triple is accepted.
	res, err := svc.SubmitReceipt(ctx, receipt)
	if err != nil || !res.Accepted {
		t.Fatalf("valid receipt: %v %+v", err, res)
	}
	// Idempotent retry of the winning receipt is fine...
	if res, err := svc.SubmitReceipt(ctx, receipt); err != nil || !res.Accepted {
		t.Fatalf("idempotent retry: %v %+v", err, res)
	}
	// ...but the verified chunk can never be rewritten by another lease.
	clk.advance(11 * time.Second)
	other, err := svc.ClaimChunk(ctx, "ms-1", "w2")
	if err != nil {
		t.Fatalf("claim c2: %v", err)
	}
	if other.ChunkID == receipt.ChunkID {
		t.Fatal("verified chunk was re-leased")
	}
	rewrite := Receipt{
		SessionID: "ms-1", ChunkID: receipt.ChunkID, LeaseID: other.ID,
		Epoch: other.Epoch, BlobKey: key, Digest: key,
		SourceID: receipt.SourceID, SessionVersion: receipt.SessionVersion,
	}
	if _, err := svc.SubmitReceipt(ctx, rewrite); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("rewrite of verified chunk: got %v", err)
	}

	// Lease takeover: w2's lease on c2 expires, w3 takes over; w2's late
	// receipt must not overwrite w3's result.
	key2, err := svc.UploadBlob(ctx, "ms-1", p[1])
	if err != nil {
		t.Fatalf("upload c2: %v", err)
	}
	clk.advance(11 * time.Second)
	taken, err := svc.ClaimChunk(ctx, "ms-1", "w3")
	if err != nil {
		t.Fatalf("takeover claim: %v", err)
	}
	if taken.ChunkID != other.ChunkID || taken.Epoch <= other.Epoch {
		t.Fatalf("takeover must bump epoch: %+v vs %+v", taken, other)
	}
	late := Receipt{
		SessionID: "ms-1", ChunkID: other.ChunkID, LeaseID: other.ID,
		Epoch: other.Epoch, BlobKey: key2, Digest: key2,
		SourceID: other.SourceID, SessionVersion: other.SessionVersion,
	}
	if _, err := svc.SubmitReceipt(ctx, late); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("late receipt from old worker: got %v", err)
	}
	fresh := Receipt{
		SessionID: "ms-1", ChunkID: taken.ChunkID, LeaseID: taken.ID,
		Epoch: taken.Epoch, BlobKey: key2, Digest: key2,
		SourceID: taken.SourceID, SessionVersion: taken.SessionVersion,
	}
	if res, err := svc.SubmitReceipt(ctx, fresh); err != nil || !res.Accepted {
		t.Fatalf("taking-over worker receipt: %v %+v", err, res)
	}

	// All chunks verified: the target is published exactly once.
	note, err := svc.GetResult(ctx, "ms-1")
	if err != nil || note == nil {
		t.Fatalf("completion notification: %v %+v", err, note)
	}
	cur, err := svc.GetReplicaCurrentManifest(ctx, "replica-t")
	if err != nil || cur == "" {
		t.Fatalf("target not published: %v %q", err, cur)
	}

	// The plan exposes the chosen source, digest and lease outcome per chunk.
	plan, err := svc.GetRepairPlan(ctx, "ms-1")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.State != SessionSucceeded || plan.NotificationID == "" {
		t.Fatalf("plan after success: %+v", plan)
	}
	for _, ch := range plan.Chunks {
		if ch.State != ChunkVerified || ch.FinalLease == "" || ch.VerifiedAt == nil {
			t.Fatalf("chunk %s missing lease result: %+v", ch.ChunkID, ch)
		}
		if ch.SourceID == "" || ch.Digest == "" {
			t.Fatalf("chunk %s missing source/digest: %+v", ch.ChunkID, ch)
		}
	}
}

func TestMultiSourceSurvivesRestart(t *testing.T) {
	store := &memoryPersistence{}
	clk := &fakeClock{t: time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)}
	svc, err := New(WithClock(clk), WithPersistence(store), WithSessionTTL(time.Minute))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	p := multiPayloads()
	if _, err := svc.CreateMultiSourceRepair(ctx, multiRequest("ms-1",
		candidate("src-a", "v1", p, 0),
		candidate("src-b", "v1", p, 1),
	)); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Restart: the frozen candidates, assignment and version must be restored.
	svc2, err := New(WithClock(clk), WithPersistence(store), WithSessionTTL(time.Minute))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	plan, err := svc2.GetRepairPlan(ctx, "ms-1")
	if err != nil {
		t.Fatalf("plan after restart: %v", err)
	}
	if len(plan.Candidates) != 2 || len(plan.Chunks) != 2 || plan.Version != 1 {
		t.Fatalf("plan not restored: %+v", plan)
	}
	// Idempotent re-create still returns the original session after restart.
	if _, err := svc2.CreateMultiSourceRepair(ctx, multiRequest("ms-1",
		candidate("src-a", "v1", p, 0),
		candidate("src-b", "v1", p, 1),
	)); err != nil {
		t.Fatalf("idempotent re-create after restart: %v", err)
	}
}
