package goreplicarepair

import (
	"context"
	"sort"
	"time"
)

// CreateAcceptance opens a repair acceptance certificate (修复验收单).
//
// The certificate freezes, once and for all:
//   - the exact repair version (the successful session's frozen manifest),
//   - the sampled chunk set,
//   - the validation rule, and
//   - the acceptor responsible for the decision.
//
// A certificate can only be opened against a completed repair whose version is
// still the one the target currently serves: once a newer repair has switched
// the target, the old version is stale and ErrAcceptanceStale is returned.
//
// Reusing an acceptance number is idempotent: the same parameters return the
// original certificate, while a changed session/version, sample set or rule
// returns ErrAcceptanceConflict without touching the original.
func (s *Service) CreateAcceptance(ctx context.Context, req AcceptanceRequest) (*AcceptanceView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateAcceptanceRequest(req); err != nil {
		return nil, err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()

	if existing, ok := k.acceptances[req.ID]; ok {
		if !sameFrozenScope(existing, req) {
			return s.acceptanceView(existing), ErrAcceptanceConflict
		}
		return s.acceptanceView(existing), nil
	}

	sess, ok := k.sessions[req.SessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}
	if sess.State != SessionSucceeded {
		return nil, ErrSessionNotRunning
	}
	samples, err := normalizedSamples(sess, req.SampleChunks)
	if err != nil {
		return nil, err
	}

	boundKey := manifestKey(sess.SourceID, sess.Frozen.Version)
	if rep, ok := k.replicas[sess.TargetID]; !ok || rep.CurrentManifest != boundKey {
		// The repaired version has already been superseded: no certificate
		// may be opened for it.
		return nil, ErrAcceptanceStale
	}

	now := k.clock.Now()
	a := &acceptance{
		ID:            req.ID,
		State:         AcceptancePending,
		SessionID:     req.SessionID,
		TargetID:      sess.TargetID,
		SourceID:      sess.SourceID,
		RepairVersion: sess.Frozen.Version,
		ManifestKey:   boundKey,
		SampleChunks:  samples,
		Rule:          req.Rule,
		Acceptor:      req.Acceptor,
		CreatedAt:     now,
		Results:       map[string]*acceptanceChunk{},
	}
	k.acceptances[a.ID] = a
	if err := k.persist(); err != nil {
		delete(k.acceptances, a.ID)
		return nil, err
	}
	return s.acceptanceView(a), nil
}

// SubmitAcceptanceResults appends a batch of per-chunk inspection verdicts.
//
// Batches are allowed: unseen samples are recorded and the certificate stays
// pending until every sample has a passing verdict. The second verdict for the
// same chunk must equal the first one (pass flag and reason); a disagreement
// marks the chunk as conflicting — the original verdict is never overwritten —
// and freezes the certificate as failed.
//
// The frozen validation rule is always re-checked by the service: a chunk
// reported as passing whose stored blob fails the rule is recorded as a
// failure. Only when every sample passes, the version still matches the target
// and no re-repair session is running does the certificate promote the replica
// to a usable source replica.
func (s *Service) SubmitAcceptanceResults(ctx context.Context, acceptanceID string, results []ChunkResult) (*AcceptanceView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, ErrAcceptanceInvalid
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()

	a, ok := k.acceptances[acceptanceID]
	if !ok {
		return nil, ErrAcceptanceNotFound
	}
	switch a.State {
	case AcceptancePending:
	case AcceptanceStale:
		return s.acceptanceView(a), ErrAcceptanceStale
	default:
		return s.acceptanceView(a), ErrAcceptanceClosed
	}
	now := k.clock.Now()

	if err := s.guardAcceptanceVersionLocked(a, now); err != nil {
		if perr := k.persist(); perr != nil {
			return nil, perr
		}
		return s.acceptanceView(a), err
	}

	sess := k.sessions[a.SessionID]
	batchConflict := false
	for _, r := range results {
		if !contains(a.SampleChunks, r.ChunkID) {
			return nil, ErrChunkNotFound
		}
		if !r.Pass && r.Reason == "" {
			return nil, ErrAcceptanceInvalid
		}
		// The service enforces the frozen rule itself; a claimed pass that
		// cannot be substantiated by the stored content is a failure.
		if r.Pass {
			if reason, ok := s.evaluateSampleLocked(sess, a, r.ChunkID); !ok {
				r.Pass = false
				r.Reason = reason
			}
		}

		prev, exists := a.Results[r.ChunkID]
		if !exists {
			ts := now
			a.Results[r.ChunkID] = &acceptanceChunk{
				ChunkID:   r.ChunkID,
				Pass:      r.Pass,
				Reason:    r.Reason,
				UpdatedAt: &ts,
			}
			continue
		}
		if prev.Pass == r.Pass && prev.Reason == r.Reason {
			// Consistent re-submission (at-least-once retry): idempotent.
			continue
		}
		// Disagreeing second verdict: flag conflict, keep the first result.
		if !prev.Conflict {
			prev.Conflict = true
			ts := now
			prev.UpdatedAt = &ts
		}
		batchConflict = true
	}

	switch {
	case batchConflict || hasFailingSample(a):
		s.freezeAcceptanceLocked(a, AcceptanceFailed, now)
		if err := k.persist(); err != nil {
			return nil, err
		}
		if batchConflict {
			return s.acceptanceView(a), ErrAcceptanceConflict
		}
		return s.acceptanceView(a), nil
	case allSamplesPassed(a):
		// Final critical section: re-check version and re-repair race under
		// the same lock that serializes session creation and target
		// switching, so an old acceptance can never promote a contested or
		// superseded version.
		if err := s.guardAcceptanceVersionLocked(a, now); err != nil {
			if perr := k.persist(); perr != nil {
				return nil, perr
			}
			return s.acceptanceView(a), err
		}
		rep := k.replicas[a.TargetID]
		rep.AcceptedManifest = a.ManifestKey
		acceptedAt := now
		rep.AcceptedAt = &acceptedAt
		a.Promoted = true
		s.freezeAcceptanceLocked(a, AcceptancePassed, now)
	}
	if err := k.persist(); err != nil {
		return nil, err
	}
	return s.acceptanceView(a), nil
}

// RevokeAcceptance cancels an acceptance certificate and records the reason.
// Revoking a certificate that had promoted the replica withdraws that
// promotion (as long as the replica still points at the certificate's
// version), so the target ceases to serve as a usable source replica.
func (s *Service) RevokeAcceptance(ctx context.Context, acceptanceID, reason string) (*AcceptanceView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reason == "" {
		return nil, ErrReasonRequired
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	a, ok := k.acceptances[acceptanceID]
	if !ok {
		return nil, ErrAcceptanceNotFound
	}
	if a.State == AcceptanceRevoked {
		return s.acceptanceView(a), ErrAcceptanceClosed
	}
	now := k.clock.Now()
	if a.Promoted {
		if rep, ok := k.replicas[a.TargetID]; ok && rep.AcceptedManifest == a.ManifestKey {
			rep.AcceptedManifest = ""
			rep.AcceptedAt = nil
		}
		a.Promoted = false
	}
	a.State = AcceptanceRevoked
	a.RevokedAt = &now
	a.RevokeReason = reason
	if err := k.persist(); err != nil {
		return nil, err
	}
	return s.acceptanceView(a), nil
}

// GetAcceptance returns the certificate projection: bound repair version,
// sampled chunks, per-chunk verdicts with failure reasons and the frozen
// decision.
func (s *Service) GetAcceptance(ctx context.Context, acceptanceID string) (*AcceptanceView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.k.mu.Lock()
	defer s.k.mu.Unlock()
	a, ok := s.k.acceptances[acceptanceID]
	if !ok {
		return nil, ErrAcceptanceNotFound
	}
	return s.acceptanceView(a), nil
}

// GetReplicaAcceptedManifest reports the manifest version the target is
// allowed to serve as a usable source replica. Empty means "repaired or not,
// nothing passed acceptance yet".
func (s *Service) GetReplicaAcceptedManifest(ctx context.Context, targetID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.k.mu.Lock()
	defer s.k.mu.Unlock()
	if rep, ok := s.k.replicas[targetID]; ok {
		return rep.AcceptedManifest, nil
	}
	return "", nil
}

// guardAcceptanceVersionLocked fails an open certificate when its bound repair
// version can no longer be accepted. It becomes stale when the target serves
// another version, and loses the version race when a re-repair session for
// the target is running. Caller holds the lock.
func (s *Service) guardAcceptanceVersionLocked(a *acceptance, now time.Time) error {
	rep, ok := s.k.replicas[a.TargetID]
	if !ok || rep.CurrentManifest != a.ManifestKey {
		s.freezeAcceptanceLocked(a, AcceptanceStale, now)
		return ErrAcceptanceStale
	}
	for _, other := range s.k.sessions {
		if other.TargetID == a.TargetID && other.State == SessionRunning {
			s.freezeAcceptanceLocked(a, AcceptanceStale, now)
			return ErrAcceptanceRace
		}
	}
	return nil
}

// freezeAcceptanceLocked records the one-way decision. Caller holds the lock.
func (s *Service) freezeAcceptanceLocked(a *acceptance, state string, now time.Time) {
	if a.State != AcceptancePending {
		return
	}
	a.State = state
	a.DecidedAt = &now
}

// invalidateAcceptancesLocked marks every still-open certificate for target
// whose bound version is no longer the current one as stale. Called while
// holding the lock when the target switches to a new repair version.
func (s *Service) invalidateAcceptancesLocked(target, currentKey string, now time.Time) {
	for _, a := range s.k.acceptances {
		if a.State != AcceptancePending || a.TargetID != target {
			continue
		}
		if a.ManifestKey != currentKey {
			s.freezeAcceptanceLocked(a, AcceptanceStale, now)
		}
	}
}

// evaluateSampleLocked runs the frozen validation rule for one sampled chunk.
// Returns ""/true when the stored repaired content satisfies the rule.
func (s *Service) evaluateSampleLocked(sess *session, a *acceptance, chunkID string) (string, bool) {
	switch a.Rule {
	case RuleDigestSHA256:
	default:
		return "unknown validation rule: " + a.Rule, false
	}
	mc := findManifestChunk(sess.Frozen, chunkID)
	if mc == nil {
		return "chunk absent from frozen repair manifest", false
	}
	c := findChunk(sess, chunkID)
	if c == nil || c.State != ChunkVerified || c.BlobKey != mc.Digest {
		return "chunk was not verified for the bound repair version", false
	}
	blob, ok := s.k.blobs[mc.Digest]
	if !ok {
		return "repaired blob missing from content store", false
	}
	if digestBytes(blob) != mc.Digest {
		return "stored blob digest does not match frozen manifest digest", false
	}
	return "", true
}

func validateAcceptanceRequest(req AcceptanceRequest) error {
	if req.ID == "" || req.SessionID == "" || req.Rule == "" || req.Acceptor == "" {
		return ErrAcceptanceInvalid
	}
	if req.Rule != RuleDigestSHA256 {
		return ErrAcceptanceInvalid
	}
	if len(req.SampleChunks) == 0 {
		return ErrAcceptanceInvalid
	}
	return nil
}

func normalizedSamples(sess *session, samples []string) ([]string, error) {
	seen := map[string]bool{}
	out := make([]string, 0, len(samples))
	for _, id := range samples {
		if id == "" || seen[id] {
			return nil, ErrAcceptanceInvalid
		}
		if findManifestChunk(sess.Frozen, id) == nil {
			return nil, ErrChunkNotFound
		}
		seen[id] = true
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// sameFrozenScope compares the immutable parts of an existing certificate
// against a repeated request. The sample list is compared as a set.
func sameFrozenScope(a *acceptance, req AcceptanceRequest) bool {
	if a.SessionID != req.SessionID || a.Rule != req.Rule {
		return false
	}
	if len(a.SampleChunks) != len(req.SampleChunks) {
		return false
	}
	seen := map[string]bool{}
	for _, id := range a.SampleChunks {
		seen[id] = true
	}
	for _, id := range req.SampleChunks {
		if !seen[id] {
			return false
		}
	}
	return true
}

func hasFailingSample(a *acceptance) bool {
	for _, r := range a.Results {
		if !r.Pass || r.Conflict {
			return true
		}
	}
	return false
}

func allSamplesPassed(a *acceptance) bool {
	if len(a.Results) != len(a.SampleChunks) {
		return false
	}
	for _, r := range a.Results {
		if !r.Pass || r.Conflict {
			return false
		}
	}
	return true
}

func (s *Service) acceptanceView(a *acceptance) *AcceptanceView {
	idx := map[string]int{}
	for i, id := range a.SampleChunks {
		idx[id] = i
	}
	results := make([]ChunkResultView, 0, len(a.Results))
	failures := []string{}
	for _, r := range a.Results {
		results = append(results, ChunkResultView{
			ChunkID:   r.ChunkID,
			Pass:      r.Pass,
			Reason:    r.Reason,
			Conflict:  r.Conflict,
			UpdatedAt: r.UpdatedAt,
		})
		if !r.Pass || r.Conflict {
			failures = append(failures, r.ChunkID)
		}
	}
	sort.Slice(results, func(i, j int) bool { return idx[results[i].ChunkID] < idx[results[j].ChunkID] })
	sort.Slice(failures, func(i, j int) bool { return idx[failures[i]] < idx[failures[j]] })
	return &AcceptanceView{
		ID:            a.ID,
		State:         a.State,
		SessionID:     a.SessionID,
		TargetID:      a.TargetID,
		SourceID:      a.SourceID,
		RepairVersion: a.RepairVersion,
		SampleChunks:  append([]string{}, a.SampleChunks...),
		Rule:          a.Rule,
		Acceptor:      a.Acceptor,
		Results:       results,
		Failures:      failures,
		CreatedAt:     a.CreatedAt,
		DecidedAt:     a.DecidedAt,
		RevokedAt:     a.RevokedAt,
		RevokeReason:  a.RevokeReason,
		Promoted:      a.Promoted,
	}
}
