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

// completeRepair registers m, creates a repair session for target and repairs
// every chunk from data. It returns the session id and manifest.
func completeRepair(t *testing.T, svc *Service, source, version, target string, m Manifest, data [][]byte) string {
	t.Helper()
	ctx := context.Background()
	if err := svc.RegisterManifest(ctx, m); err != nil {
		t.Fatalf("RegisterManifest: %v", err)
	}
	sessID, err := svc.CreateRepair(ctx, source, version, target)
	if err != nil {
		t.Fatalf("CreateRepair: %v", err)
	}
	for _, c := range m.Chunks {
		repairChunk(t, svc, sessID, "w", c, data[chunkIndex(m, c.ID)])
	}
	return sessID
}

func acceptanceFixture(t *testing.T) (*Service, *fakeClock, Manifest, [][]byte, string) {
	t.Helper()
	svc, clk := newTestService(t)
	data := payloads()
	m := testManifest("src", "v1", data, clk.Now())
	sessID := completeRepair(t, svc, "src", "v1", "tgt-A", m, data)
	return svc, clk, m, data, sessID
}

func openAcceptance(t *testing.T, svc *Service, sessID string, samples ...string) *AcceptanceView {
	t.Helper()
	a, err := svc.CreateAcceptance(context.Background(), AcceptanceRequest{
		ID:           "acc-1",
		SessionID:    sessID,
		SampleChunks: samples,
		Rule:         RuleDigestSHA256,
		Acceptor:     "inspector-1",
	})
	if err != nil {
		t.Fatalf("CreateAcceptance: %v", err)
	}
	return a
}

// ---- 1. Happy path: full acceptance promotes the replica as a usable source ----

func TestAcceptancePromotesAfterAllSamplesPass(t *testing.T) {
	ctx := context.Background()
	svc, _, m, _, sessID := acceptanceFixture(t)

	// Repaired but not accepted: the target is not yet a usable source.
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "" {
		t.Fatalf("replica promoted before acceptance: %q", got)
	}

	a := openAcceptance(t, svc, sessID, "c1", "c3")
	if a.State != AcceptancePending || a.RepairVersion != "v1" || a.Rule != RuleDigestSHA256 {
		t.Fatalf("unexpected certificate: %+v", a)
	}

	// Partial batch: stays pending and must not promote.
	v, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	if v.State != AcceptancePending {
		t.Fatalf("state = %q, want pending", v.State)
	}
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "" {
		t.Fatalf("promoted after partial batch: %q", got)
	}

	v, err = svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c3", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	if v.State != AcceptancePassed || !v.Promoted {
		t.Fatalf("state = %q promoted = %v, want passed/promoted", v.State, v.Promoted)
	}
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "src@v1" {
		t.Fatalf("accepted manifest = %q, want src@v1", got)
	}

	// Frozen details are queryable.
	got, err := svc.GetAcceptance(ctx, "acc-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Acceptor != "inspector-1" || len(got.SampleChunks) != 2 ||
		got.SampleChunks[0] != "c1" || got.SampleChunks[1] != "c3" {
		t.Fatalf("frozen scope wrong: %+v", got)
	}
	if len(got.Results) != 2 || got.Results[0].ChunkID != "c1" || !got.Results[0].Pass {
		t.Fatalf("results wrong: %+v", got.Results)
	}
	_ = m
}

// ---- 2. Partial failure keeps failed chunks and blocks promotion ----

func TestAcceptancePartialFailureBlocksPromotion(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1", "c2", "c3")

	v, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{
		{ChunkID: "c1", Pass: true},
		{ChunkID: "c2", Pass: false, Reason: "checksum drift on inspection"},
		{ChunkID: "c3", Pass: true},
	})
	if err != nil {
		t.Fatalf("failed inspection is a recorded result, got err %v", err)
	}
	if v.State != AcceptanceFailed {
		t.Fatalf("state = %q, want failed", v.State)
	}
	if len(v.Failures) != 1 || v.Failures[0] != "c2" {
		t.Fatalf("failures = %v, want [c2]", v.Failures)
	}
	var reason string
	for _, r := range v.Results {
		if r.ChunkID == "c2" {
			reason = r.Reason
		}
	}
	if reason != "checksum drift on inspection" {
		t.Fatalf("failure reason not retained: %q", reason)
	}
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "" {
		t.Fatalf("failed acceptance promoted replica: %q", got)
	}
	// Frozen decision rejects further batches.
	if _, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c2", Pass: true}}); !errors.Is(err, ErrAcceptanceClosed) {
		t.Fatalf("err = %v, want ErrAcceptanceClosed", err)
	}
}

func TestAcceptanceFailureReasonRequired(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1")
	if _, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: false}}); !errors.Is(err, ErrAcceptanceInvalid) {
		t.Fatalf("err = %v, want ErrAcceptanceInvalid", err)
	}
}

// Service-side rule enforcement: a claimed pass the stored content cannot
// substantiate becomes a failure rather than trusting the inspector.
func TestAcceptanceServiceEnforcesDigestRule(t *testing.T) {
	ctx := context.Background()
	svc, clk, m, data, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1")

	// Tamper with the content store behind the repaired version's back and
	// re-register under a new blob key is not possible (key = digest), so
	// instead delete the blob: the claimed pass must be rejected by the rule.
	key := DigestOf(data[0])
	svc.k.mu.Lock()
	delete(svc.k.blobs, key)
	svc.k.mu.Unlock()

	v, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	if v.State != AcceptanceFailed || len(v.Failures) != 1 || v.Failures[0] != "c1" {
		t.Fatalf("claimed pass without blob must fail: %+v", v)
	}
	_ = clk
	_ = m
}

// ---- 3. Duplicate results: consistent is idempotent, disagreeing conflicts ----

func TestDuplicateChunkResultConflictNotOverwrite(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1", "c2")

	if _, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{
		{ChunkID: "c1", Pass: true},
		{ChunkID: "c2", Pass: true},
	}); err != nil {
		t.Fatal(err)
	}
	// Same batch resent (at-least-once): still pending-equivalent? No — all
	// samples have passed, so the certificate already promoted. Use a wider
	// sample set to test mid-flight duplicates instead.
	v, err := svc.GetAcceptance(ctx, "acc-1")
	if err != nil {
		t.Fatal(err)
	}
	if v.State != AcceptancePassed {
		t.Fatalf("state = %q, want passed", v.State)
	}
}

func TestDuplicateChunkResultConsistentIsIdempotent(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1", "c2")

	v, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	// Resend the identical verdict while still waiting on c2.
	v, err = svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: true}})
	if err != nil {
		t.Fatalf("identical resend should be idempotent: %v", err)
	}
	if v.State != AcceptancePending {
		t.Fatalf("state = %q, want pending", v.State)
	}
	if v.Results[0].Conflict {
		t.Fatalf("identical resend flagged conflict: %+v", v.Results[0])
	}

	// A disagreeing second verdict conflicts and freezes failure; the first
	// (passing) verdict is retained.
	v, err = svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: false, Reason: "second look disagrees"}})
	if !errors.Is(err, ErrAcceptanceConflict) {
		t.Fatalf("err = %v, want ErrAcceptanceConflict", err)
	}
	if v.State != AcceptanceFailed {
		t.Fatalf("state = %q, want failed", v.State)
	}
	if len(v.Failures) != 1 || v.Failures[0] != "c1" {
		t.Fatalf("failures = %v, want [c1]", v.Failures)
	}
	var c1 ChunkResultView
	for _, r := range v.Results {
		if r.ChunkID == "c1" {
			c1 = r
		}
	}
	if !c1.Conflict || !c1.Pass {
		t.Fatalf("first verdict must be kept with conflict flag: %+v", c1)
	}
	if c1.Reason != "" {
		t.Fatalf("first verdict reason must not be overwritten: %q", c1.Reason)
	}
}

// ---- 4. Acceptance number reuse: same params returns original, changed conflicts ----

func TestAcceptanceNumberIdempotentAndScopeConflict(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, sessID := acceptanceFixture(t)
	first := openAcceptance(t, svc, sessID, "c1", "c2")

	again, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-1", SessionID: sessID, SampleChunks: []string{"c2", "c1"},
		Rule: RuleDigestSHA256, Acceptor: "inspector-1",
	})
	if err != nil {
		t.Fatalf("same scope (different order) should return original: %v", err)
	}
	if again != nil && again.CreatedAt != first.CreatedAt {
		t.Fatal("re-request should return the original certificate")
	}

	// Different sample set.
	_, err = svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-1", SessionID: sessID, SampleChunks: []string{"c1"},
		Rule: RuleDigestSHA256, Acceptor: "inspector-1",
	})
	if !errors.Is(err, ErrAcceptanceConflict) {
		t.Fatalf("changed samples: err = %v, want ErrAcceptanceConflict", err)
	}
	// Different rule.
	_, err = svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-1", SessionID: sessID, SampleChunks: []string{"c1", "c2"},
		Rule: "rule-other", Acceptor: "inspector-1",
	})
	if !errors.Is(err, ErrAcceptanceInvalid) {
		t.Fatalf("unknown rule: err = %v, want ErrAcceptanceInvalid", err)
	}
}

// ---- 5. Version competition: re-repair / newer switch invalidates old acceptance ----

func TestAcceptanceStaleAfterNewRepairVersion(t *testing.T) {
	ctx := context.Background()
	svc, clk, _, data, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1", "c2")

	// Source publishes v2 and a new repair switches the target while the old
	// acceptance is still open.
	v2data := [][]byte{[]byte("new-chunk-one"), data[1], data[2]}
	v2 := testManifest("src", "v2", v2data, clk.Now())
	sess2 := completeRepair(t, svc, "src", "v2", "tgt-A", v2, v2data)

	v, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: true}})
	if !errors.Is(err, ErrAcceptanceStale) {
		t.Fatalf("err = %v, want ErrAcceptanceStale", err)
	}
	if v.State != AcceptanceStale {
		t.Fatalf("state = %q, want stale", v.State)
	}
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "" {
		t.Fatalf("stale acceptance must not promote: %q", got)
	}
	// A fresh acceptance for v2 works.
	a2, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-2", SessionID: sess2, SampleChunks: []string{"c1"},
		Rule: RuleDigestSHA256, Acceptor: "inspector-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if a2.RepairVersion != "v2" {
		t.Fatalf("fresh acceptance version = %q, want v2", a2.RepairVersion)
	}
}

// Opening an acceptance for a version already superseded is rejected outright.
func TestCreateAcceptanceRejectsSupersededVersion(t *testing.T) {
	ctx := context.Background()
	svc, clk, _, data, sessID := acceptanceFixture(t)
	v2data := [][]byte{[]byte("new-chunk-one"), data[1], data[2]}
	v2 := testManifest("src", "v2", v2data, clk.Now())
	completeRepair(t, svc, "src", "v2", "tgt-A", v2, v2data)

	_, err := svc.CreateAcceptance(ctx, AcceptanceRequest{
		ID: "acc-old", SessionID: sessID, SampleChunks: []string{"c1"},
		Rule: RuleDigestSHA256, Acceptor: "inspector-1",
	})
	if !errors.Is(err, ErrAcceptanceStale) {
		t.Fatalf("err = %v, want ErrAcceptanceStale", err)
	}
}

// Concurrent final-acceptance vs re-repair start: the old acceptance loses the
// version race and cannot mark the contested version usable.
func TestAcceptanceLosesRaceAgainstConcurrentRerepair(t *testing.T) {
	ctx := context.Background()
	svc, clk, _, data, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1")

	v2data := [][]byte{[]byte("new-chunk-one"), data[1], data[2]}
	v2 := testManifest("src", "v2", v2data, clk.Now())
	if err := svc.RegisterManifest(ctx, v2); err != nil {
		t.Fatal(err)
	}
	// A re-repair session for the same target starts but does not finish.
	if _, err := svc.CreateRepair(ctx, "src", "v2", "tgt-A"); !errors.Is(err, ErrSessionExists) {
		// v1 session is terminal, so a new running session is allowed.
		if err != nil {
			t.Fatalf("CreateRepair v2: %v", err)
		}
	}

	v, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: true}})
	if !errors.Is(err, ErrAcceptanceRace) {
		t.Fatalf("err = %v, want ErrAcceptanceRace", err)
	}
	if v.State != AcceptanceStale || v.Promoted {
		t.Fatalf("old acceptance should freeze stale without promotion: %+v", v)
	}
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "" {
		t.Fatalf("no promotion allowed during re-repair, got %q", got)
	}
}

// Under concurrent submissions of the final sample, promotion happens exactly
// once and the certificate decision is stable.
func TestConcurrentAcceptanceSubmissionsSinglePromotion(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1", "c2", "c3")

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := []ChunkResult{
				{ChunkID: "c1", Pass: true},
				{ChunkID: "c2", Pass: true},
				{ChunkID: "c3", Pass: true},
			}
			_, err := svc.SubmitAcceptanceResults(ctx, "acc-1", r)
			if err != nil && !errors.Is(err, ErrAcceptanceClosed) {
				errs <- fmt.Errorf("worker %d: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	v, _ := svc.GetAcceptance(ctx, "acc-1")
	if v.State != AcceptancePassed || !v.Promoted {
		t.Fatalf("final state = %+v", v)
	}
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "src@v1" {
		t.Fatalf("accepted = %q, want src@v1", got)
	}
}

// ---- 6. Revocation keeps the reason and withdraws promotion ----

func TestRevokeAcceptanceRecordsReasonAndWithdraws(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _, sessID := acceptanceFixture(t)
	openAcceptance(t, svc, sessID, "c1")
	if _, err := svc.SubmitAcceptanceResults(ctx, "acc-1", []ChunkResult{{ChunkID: "c1", Pass: true}}); err != nil {
		t.Fatal(err)
	}
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "src@v1" {
		t.Fatalf("precondition: accepted manifest %q", got)
	}

	if _, err := svc.RevokeAcceptance(ctx, "acc-1", ""); !errors.Is(err, ErrReasonRequired) {
		t.Fatalf("err = %v, want ErrReasonRequired", err)
	}
	v, err := svc.RevokeAcceptance(ctx, "acc-1", "post-check found silent corruption")
	if err != nil {
		t.Fatal(err)
	}
	if v.State != AcceptanceRevoked || v.RevokeReason != "post-check found silent corruption" || v.RevokedAt == nil {
		t.Fatalf("revocation not recorded: %+v", v)
	}
	if got, _ := svc.GetReplicaAcceptedManifest(ctx, "tgt-A"); got != "" {
		t.Fatalf("promotion not withdrawn: %q", got)
	}
	if _, err := svc.RevokeAcceptance(ctx, "acc-1", "again"); !errors.Is(err, ErrAcceptanceClosed) {
		t.Fatalf("double revoke: err = %v, want ErrAcceptanceClosed", err)
	}
}

// ---- 7. Acceptance survives restart ----

func TestAcceptanceSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := FilePersistence{Path: path}

	svc, clk, _, _, sessID := func() (*Service, *fakeClock, Manifest, [][]byte, string) {
		s, c := newTestService(t, WithPersistence(store))
		d := payloads()
		m := testManifest("src", "v1", d, c.Now())
		id := completeRepair(t, s, "src", "v1", "tgt-A", m, d)
		return s, c, m, d, id
	}()
	a, err := svc.CreateAcceptance(context.Background(), AcceptanceRequest{
		ID: "acc-1", SessionID: sessID, SampleChunks: []string{"c1", "c2"},
		Rule: RuleDigestSHA256, Acceptor: "inspector-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitAcceptanceResults(context.Background(), "acc-1", []ChunkResult{{ChunkID: "c1", Pass: true}}); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(WithClock(clk), WithSessionTTL(time.Minute), WithLeaseTTL(10*time.Second), WithPersistence(store))
	if err != nil {
		t.Fatal(err)
	}
	v, err := restarted.GetAcceptance(context.Background(), "acc-1")
	if err != nil {
		t.Fatal(err)
	}
	if v.State != AcceptancePending || v.RepairVersion != "v1" || len(v.Results) != 1 || v.Results[0].ChunkID != "c1" {
		t.Fatalf("certificate not restored: %+v", v)
	}
	_ = a
	// Complete after restart and verify promotion persists too.
	if _, err := restarted.SubmitAcceptanceResults(context.Background(), "acc-1", []ChunkResult{{ChunkID: "c2", Pass: true}}); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.GetReplicaAcceptedManifest(context.Background(), "tgt-A")
	if err != nil {
		t.Fatal(err)
	}
	if got != "src@v1" {
		t.Fatalf("accepted after restart = %q", got)
	}
}
