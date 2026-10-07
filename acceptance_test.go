package goreplicarepair

import (
	"context"
	"errors"
	"testing"
)

// runFullRepair registers the manifest, repairs every chunk and returns the
// (succeeded) session id plus the manifest.
func runFullRepair(t *testing.T, svc *Service, source, version, target string, data [][]byte) (string, Manifest) {
	t.Helper()
	ctx := context.Background()
	m := testManifest(source, version, data, svc.k.clock.Now())
	if err := svc.RegisterManifest(ctx, m); err != nil {
		t.Fatalf("RegisterManifest: %v", err)
	}
	sessID, err := svc.CreateRepair(ctx, source, version, target)
	if err != nil {
		t.Fatalf("CreateRepair: %v", err)
	}
	for i, c := range m.Chunks {
		repairChunk(t, svc, sessID, "w", c, data[i])
	}
	return sessID, m
}

func sampleAll(m Manifest) []string {
	ids := make([]string, len(m.Chunks))
	for i, c := range m.Chunks {
		ids[i] = c.ID
	}
	return ids
}

func passReports(m Manifest, ids ...string) []SampleReport {
	var out []SampleReport
	for _, id := range ids {
		out = append(out, SampleReport{ChunkID: id, Digest: findManifestChunk(m, id).Digest})
	}
	return out
}

func TestAcceptanceApprovesAndMarksUsable(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	data := payloads()
	_, m := runFullRepair(t, svc, "src", "v1", "tgt", data)

	// Repaired but not yet accepted: must not serve as a new source.
	if usable, _ := svc.GetReplicaUsable(ctx, "tgt"); usable {
		t.Fatal("replica usable before acceptance")
	}

	acc, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-1", TargetID: "tgt", SourceID: "src", Version: "v1",
		ChunkIDs:  []string{"c1", "c3"},
		Rule:      AcceptanceRule{Algorithm: DigestAlgorithmSHA256},
		Inspector: "inspector-7",
	})
	if err != nil {
		t.Fatalf("CreateAcceptance: %v", err)
	}
	if acc.Decision != AcceptancePending || acc.ManifestVer != "v1" || len(acc.Samples) != 2 {
		t.Fatalf("unexpected order: %+v", acc)
	}

	// Batched submission: one chunk now, one later.
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c1")); err != nil {
		t.Fatalf("SubmitSamples batch 1: %v", err)
	}
	view, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c3"))
	if err != nil {
		t.Fatalf("SubmitSamples batch 2: %v", err)
	}
	if view.Decision != AcceptanceApproved {
		t.Fatalf("decision = %s, want approved", view.Decision)
	}
	if usable, _ := svc.GetReplicaUsable(ctx, "tgt"); !usable {
		t.Fatal("replica not usable after approval")
	}
}

func TestAcceptancePartialFailureKeepsFailedChunks(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	data := payloads()
	_, m := runFullRepair(t, svc, "src", "v1", "tgt", data)

	if _, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-1", TargetID: "tgt", SourceID: "src", Version: "v1",
		ChunkIDs:  []string{"c1", "c2"},
		Rule:      AcceptanceRule{Algorithm: DigestAlgorithmSHA256},
		Inspector: "insp",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c1")); err != nil {
		t.Fatal(err)
	}
	_, err := svc.SubmitSamples(ctx, "acc-1", []SampleReport{
		{ChunkID: "c2", Digest: DigestOf([]byte("tampered")), Reason: "digest mismatch on re-read"},
	})
	if err != nil {
		t.Fatalf("SubmitSamples failing chunk: %v", err)
	}

	view, err := svc.GetAcceptance(ctx, "acc-1")
	if err != nil {
		t.Fatal(err)
	}
	if view.Decision != AcceptanceRejected {
		t.Fatalf("decision = %s, want rejected", view.Decision)
	}
	var failed *SampleView
	for i := range view.Samples {
		if view.Samples[i].State == SampleFailed {
			failed = &view.Samples[i]
		}
	}
	if failed == nil || failed.ChunkID != "c2" || failed.Reason != "digest mismatch on re-read" {
		t.Fatalf("failed chunk not retained: %+v", view.Samples)
	}
	if usable, _ := svc.GetReplicaUsable(ctx, "tgt"); usable {
		t.Fatal("rejected acceptance marked replica usable")
	}
	// A decided order accepts no further samples.
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c2")); !errors.Is(err, ErrAcceptanceClosed) {
		t.Fatalf("submit after decision: %v", err)
	}
}

func TestAcceptanceConflictingSecondResult(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	data := payloads()
	_, m := runFullRepair(t, svc, "src", "v1", "tgt", data)

	if _, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-1", TargetID: "tgt", SourceID: "src", Version: "v1",
		ChunkIDs:  []string{"c1", "c2"},
		Rule:      AcceptanceRule{Algorithm: DigestAlgorithmSHA256},
		Inspector: "insp",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c1")); err != nil {
		t.Fatal(err)
	}
	// Identical resubmission is an idempotent no-op.
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c1")); err != nil {
		t.Fatalf("identical resubmit: %v", err)
	}
	// A disagreeing second result conflicts instead of overwriting.
	_, err := svc.SubmitSamples(ctx, "acc-1", []SampleReport{
		{ChunkID: "c1", Digest: DigestOf([]byte("other"))},
	})
	if !errors.Is(err, ErrSampleConflict) {
		t.Fatalf("conflicting resubmit: %v", err)
	}
	view, _ := svc.GetAcceptance(ctx, "acc-1")
	if view.Samples[0].State != SampleConflict {
		t.Fatalf("chunk state = %s, want conflict", view.Samples[0].State)
	}
	if view.Samples[0].ObservedDigest != m.Chunks[0].Digest {
		t.Fatal("first result was overwritten")
	}
	// Finish the rest: the conflicted chunk can never pass, so the order
	// rejects and the replica stays unusable.
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c2")); err != nil {
		t.Fatal(err)
	}
	view, _ = svc.GetAcceptance(ctx, "acc-1")
	if view.Decision != AcceptanceRejected {
		t.Fatalf("decision = %s, want rejected", view.Decision)
	}
	if usable, _ := svc.GetReplicaUsable(ctx, "tgt"); usable {
		t.Fatal("conflicted acceptance marked replica usable")
	}
}

func TestAcceptanceIdempotentCreateAndConflict(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	data := payloads()
	runFullRepair(t, svc, "src", "v1", "tgt", data)

	req := AcceptanceRequest{
		ID: "acc-1", TargetID: "tgt", SourceID: "src", Version: "v1",
		ChunkIDs:  []string{"c1"},
		Rule:      AcceptanceRule{Algorithm: DigestAlgorithmSHA256},
		Inspector: "insp",
	}
	first, err := svc.CreateAcceptance(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.CreateAcceptance(ctx, req)
	if err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if again.ID != first.ID || again.Decision != first.Decision {
		t.Fatalf("retry returned different order: %+v", again)
	}

	// Same id, different sample set or rule: conflict.
	changed := req
	changed.ChunkIDs = []string{"c1", "c2"}
	if _, err := svc.CreateAcceptance(ctx, changed); !errors.Is(err, ErrAcceptanceConflict) {
		t.Fatalf("changed sample set: %v", err)
	}
	changed = req
	changed.Inspector = "someone-else"
	if _, err := svc.CreateAcceptance(ctx, changed); !errors.Is(err, ErrAcceptanceConflict) {
		t.Fatalf("changed inspector: %v", err)
	}
}

func TestAcceptanceStaleWhenVersionMovesOn(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	data := payloads()
	_, m := runFullRepair(t, svc, "src", "v1", "tgt", data)

	if _, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-1", TargetID: "tgt", SourceID: "src", Version: "v1",
		ChunkIDs:  sampleAll(m),
		Rule:      AcceptanceRule{Algorithm: DigestAlgorithmSHA256},
		Inspector: "insp",
	}); err != nil {
		t.Fatal(err)
	}
	// Sample all but the last chunk, then a re-repair starts for the target.
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c1", "c2")); err != nil {
		t.Fatal(err)
	}
	v2data := [][]byte{[]byte("chunk-one"), []byte("chunk-two"), []byte("chunk-three-v2")}
	v2 := testManifest("src", "v2", v2data, svc.k.clock.Now())
	if err := svc.RegisterManifest(ctx, v2); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateRepair(ctx, "src", "v2", "tgt"); err != nil {
		t.Fatal(err)
	}

	// The old acceptance must not conclude nor mark the version usable.
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, "c3")); !errors.Is(err, ErrAcceptanceStale) {
		t.Fatalf("submit during re-repair: %v", err)
	}
	view, _ := svc.GetAcceptance(ctx, "acc-1")
	if view.Decision != AcceptanceStale {
		t.Fatalf("decision = %s, want stale", view.Decision)
	}
	if usable, _ := svc.GetReplicaUsable(ctx, "tgt"); usable {
		t.Fatal("stale acceptance marked replica usable")
	}
}

func TestAcceptanceRevokeKeepsReason(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	data := payloads()
	_, m := runFullRepair(t, svc, "src", "v1", "tgt", data)

	if _, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-1", TargetID: "tgt", SourceID: "src", Version: "v1",
		ChunkIDs:  sampleAll(m),
		Rule:      AcceptanceRule{Algorithm: DigestAlgorithmSHA256},
		Inspector: "insp",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitSamples(ctx, "acc-1", passReports(m, sampleAll(m)...)); err != nil {
		t.Fatal(err)
	}
	if usable, _ := svc.GetReplicaUsable(ctx, "tgt"); !usable {
		t.Fatal("not usable after approval")
	}

	if _, err := svc.RevokeAcceptance(ctx, "acc-1", ""); !errors.Is(err, ErrReasonRequired) {
		t.Fatalf("revoke without reason: %v", err)
	}
	view, err := svc.RevokeAcceptance(ctx, "acc-1", "sampled blob re-read flaked")
	if err != nil {
		t.Fatal(err)
	}
	if view.Decision != AcceptanceRevoked || view.RevokeReason != "sampled blob re-read flaked" {
		t.Fatalf("revoke not recorded: %+v", view)
	}
	if usable, _ := svc.GetReplicaUsable(ctx, "tgt"); usable {
		t.Fatal("replica still usable after revoke")
	}
	// Revoking again with the same reason is idempotent; a different reason
	// conflicts with the recorded one.
	if _, err := svc.RevokeAcceptance(ctx, "acc-1", "sampled blob re-read flaked"); err != nil {
		t.Fatalf("idempotent revoke: %v", err)
	}
	if _, err := svc.RevokeAcceptance(ctx, "acc-1", "another reason"); !errors.Is(err, ErrAcceptanceConflict) {
		t.Fatalf("revoke with new reason: %v", err)
	}
}

func TestAcceptanceUnknownChunkRejected(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	data := payloads()
	runFullRepair(t, svc, "src", "v1", "tgt", data)

	if _, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		TargetID: "tgt", SourceID: "src", Version: "v1",
		ChunkIDs:  []string{"nope"},
		Rule:      AcceptanceRule{Algorithm: DigestAlgorithmSHA256},
		Inspector: "insp",
	}); !errors.Is(err, ErrAcceptanceInvalid) {
		t.Fatalf("unknown chunk: %v", err)
	}
}
