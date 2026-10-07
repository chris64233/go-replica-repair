package goreplicarepair

import "context"

// CreateAcceptance opens an acceptance order for the repaired version a
// target replica now serves. The order freezes the manifest version, the
// exact sample set, the verification rule and the inspector: once the repair
// version moves on, this order can no longer conclude (ErrAcceptanceStale).
//
// The request is idempotent per acceptance id: repeating the identical
// request returns the recorded order unchanged, while reusing the id with a
// different version, sample set, rule or inspector fails with
// ErrAcceptanceConflict.
func (s *Service) CreateAcceptance(ctx context.Context, req AcceptanceRequest) (*AcceptanceView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()

	m, ok := k.manifests[manifestKey(req.SourceID, req.Version)]
	if !ok {
		return nil, ErrManifestNotFound
	}
	if err := validateAcceptanceRequest(req, m); err != nil {
		return nil, err
	}
	if req.ID != "" {
		if existing, ok := k.acceptances[req.ID]; ok {
			if !acceptanceMatches(existing, req) {
				return nil, ErrAcceptanceConflict
			}
			return acceptanceView(existing), nil
		}
	}

	now := k.clock.Now()
	acc := &acceptance{
		ID:          req.ID,
		TargetID:    req.TargetID,
		SourceID:    req.SourceID,
		ManifestVer: req.Version,
		Rule:        req.Rule,
		Inspector:   req.Inspector,
		Decision:    AcceptancePending,
		CreatedAt:   now,
	}
	if acc.ID == "" {
		acc.ID = k.newID("acc")
	}
	for _, id := range req.ChunkIDs {
		mc := findManifestChunk(m, id)
		acc.Samples = append(acc.Samples, sampleState{
			ChunkID:        id,
			State:          SamplePending,
			ExpectedDigest: mc.Digest,
		})
	}
	k.acceptances[acc.ID] = acc
	if err := k.persist(); err != nil {
		delete(k.acceptances, acc.ID)
		return nil, err
	}
	return acceptanceView(acc), nil
}

// SubmitSamples records a batch of inspector results. Batches may arrive in
// any number of calls. A second result for the same chunk must be identical
// to the first; a disagreement marks the chunk as conflicted (the first
// result is kept, never overwritten) and the batch fails with
// ErrSampleConflict.
//
// When the last pending sample is recorded the order concludes in the same
// critical section: all passed approves the order and marks the replica
// usable — but only if the replica still serves the accepted version and no
// new repair is running for the target; otherwise the order goes stale.
// Any failed or conflicted chunk rejects the order, keeps the failure
// records and leaves the replica unusable as a new source.
func (s *Service) SubmitSamples(ctx context.Context, acceptanceID string, reports []SampleReport) (*AcceptanceView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(reports) == 0 {
		return nil, ErrAcceptanceInvalid
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	acc, ok := k.acceptances[acceptanceID]
	if !ok {
		return nil, ErrAcceptanceNotFound
	}
	if acc.Decision != AcceptancePending {
		return nil, ErrAcceptanceClosed
	}
	if !s.acceptanceLiveLocked(acc) {
		s.staleLocked(acc)
		if err := k.persist(); err != nil {
			return nil, err
		}
		return nil, ErrAcceptanceStale
	}

	conflict := false
	for _, rep := range reports {
		sample := findSample(acc, rep.ChunkID)
		if sample == nil {
			return nil, ErrChunkNotSampled
		}
		if sample.State != SamplePending {
			// Idempotent resubmission is fine; a disagreeing second result
			// marks a conflict and never overwrites the first record.
			if sample.ObservedDigest == rep.Digest && sample.State != SampleConflict {
				continue
			}
			sample.State = SampleConflict
			conflict = true
			continue
		}
		sample.ObservedDigest = rep.Digest
		if sample.ExpectedDigest == rep.Digest {
			sample.State = SamplePassed
		} else {
			sample.State = SampleFailed
			sample.Reason = rep.Reason
			if sample.Reason == "" {
				sample.Reason = "digest mismatch under " + acc.Rule.Algorithm
			}
		}
	}
	if conflict {
		if err := k.persist(); err != nil {
			return nil, err
		}
		return nil, ErrSampleConflict
	}

	if allSamplesDecided(acc) {
		s.concludeAcceptanceLocked(acc)
	}
	if err := k.persist(); err != nil {
		return nil, err
	}
	return acceptanceView(acc), nil
}

// RevokeAcceptance withdraws an order. A reason is mandatory and is retained
// in the query view. If the order had marked the replica usable, that mark is
// lifted unless the replica has since moved to another version.
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
	acc, ok := k.acceptances[acceptanceID]
	if !ok {
		return nil, ErrAcceptanceNotFound
	}
	if acc.Decision == AcceptanceRevoked {
		if acc.RevokeReason != reason {
			return nil, ErrAcceptanceConflict
		}
		return acceptanceView(acc), nil
	}
	wasApproved := acc.Decision == AcceptanceApproved
	now := k.clock.Now()
	acc.Decision = AcceptanceRevoked
	acc.RevokeReason = reason
	acc.DecidedAt = &now
	if wasApproved {
		if rep, ok := k.replicas[acc.TargetID]; ok &&
			rep.UsableManifest == manifestKey(acc.SourceID, acc.ManifestVer) {
			rep.Usable = false
			rep.UsableManifest = ""
		}
	}
	if err := k.persist(); err != nil {
		return nil, err
	}
	return acceptanceView(acc), nil
}

// GetAcceptance returns the query view: repair version, sampled chunks with
// failure reasons and the acceptance decision.
func (s *Service) GetAcceptance(ctx context.Context, acceptanceID string) (*AcceptanceView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	acc, ok := k.acceptances[acceptanceID]
	if !ok {
		return nil, ErrAcceptanceNotFound
	}
	// A still-pending order whose version was superseded is reported stale.
	if acc.Decision == AcceptancePending && !s.acceptanceLiveLocked(acc) {
		s.staleLocked(acc)
		if err := k.persist(); err != nil {
			return nil, err
		}
	}
	return acceptanceView(acc), nil
}

// GetReplicaUsable reports whether the replica currently serves a version
// that passed acceptance (and may therefore act as a new source).
func (s *Service) GetReplicaUsable(ctx context.Context, targetID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.k.mu.Lock()
	defer s.k.mu.Unlock()
	if rep, ok := s.k.replicas[targetID]; ok {
		return rep.Usable && rep.UsableManifest == rep.CurrentManifest, nil
	}
	return false, nil
}

// concludeAcceptanceLocked evaluates a fully sampled order. Caller holds the
// lock and has already verified liveness.
func (s *Service) concludeAcceptanceLocked(acc *acceptance) {
	now := s.k.clock.Now()
	acc.DecidedAt = &now
	for i := range acc.Samples {
		if acc.Samples[i].State != SamplePassed {
			// Partial failure: failed chunks stay on record and the replica
			// is forbidden from becoming a new source.
			acc.Decision = AcceptanceRejected
			return
		}
	}
	if !s.acceptanceLiveLocked(acc) {
		acc.Decision = AcceptanceStale
		return
	}
	acc.Decision = AcceptanceApproved
	rep, ok := s.k.replicas[acc.TargetID]
	if !ok {
		// Unreachable: liveness requires the replica to serve this version.
		acc.Decision = AcceptanceStale
		return
	}
	rep.Usable = true
	rep.UsableManifest = manifestKey(acc.SourceID, acc.ManifestVer)
	rep.UpdatedAt = now
}

// acceptanceLiveLocked reports whether the accepted version is the one the
// replica currently serves and no new repair session is running for the
// target. An old acceptance must never mark a version that is being
// re-repaired, nor one the replica has not switched to yet.
func (s *Service) acceptanceLiveLocked(acc *acceptance) bool {
	key := manifestKey(acc.SourceID, acc.ManifestVer)
	rep, ok := s.k.replicas[acc.TargetID]
	if !ok || rep.CurrentManifest != key {
		return false
	}
	for _, sess := range s.k.sessions {
		if sess.TargetID == acc.TargetID && sess.State == SessionRunning {
			return false
		}
	}
	return true
}

// staleLocked freezes a pending order as stale.
func (s *Service) staleLocked(acc *acceptance) {
	now := s.k.clock.Now()
	acc.Decision = AcceptanceStale
	acc.DecidedAt = &now
}

func validateAcceptanceRequest(req AcceptanceRequest, m Manifest) error {
	if req.TargetID == "" || req.Inspector == "" || len(req.ChunkIDs) == 0 {
		return ErrAcceptanceInvalid
	}
	if req.Rule.Algorithm != DigestAlgorithmSHA256 {
		return ErrAcceptanceInvalid
	}
	seen := map[string]bool{}
	for _, id := range req.ChunkIDs {
		if seen[id] || findManifestChunk(m, id) == nil {
			return ErrAcceptanceInvalid
		}
		seen[id] = true
	}
	return nil
}

// acceptanceMatches reports whether a repeated request is identical to the
// recorded order (idempotent retry) or conflicts with it.
func acceptanceMatches(acc *acceptance, req AcceptanceRequest) bool {
	if acc.TargetID != req.TargetID || acc.SourceID != req.SourceID ||
		acc.ManifestVer != req.Version || acc.Inspector != req.Inspector ||
		acc.Rule != req.Rule || len(acc.Samples) != len(req.ChunkIDs) {
		return false
	}
	for i, id := range req.ChunkIDs {
		if acc.Samples[i].ChunkID != id {
			return false
		}
	}
	return true
}

func findSample(acc *acceptance, chunkID string) *sampleState {
	for i := range acc.Samples {
		if acc.Samples[i].ChunkID == chunkID {
			return &acc.Samples[i]
		}
	}
	return nil
}

func allSamplesDecided(acc *acceptance) bool {
	for i := range acc.Samples {
		if acc.Samples[i].State == SamplePending {
			return false
		}
	}
	return true
}

func acceptanceView(acc *acceptance) *AcceptanceView {
	v := &AcceptanceView{
		ID:           acc.ID,
		TargetID:     acc.TargetID,
		SourceID:     acc.SourceID,
		ManifestVer:  acc.ManifestVer,
		Rule:         acc.Rule,
		Inspector:    acc.Inspector,
		Decision:     acc.Decision,
		RevokeReason: acc.RevokeReason,
		CreatedAt:    acc.CreatedAt,
		DecidedAt:    acc.DecidedAt,
		Samples:      make([]SampleView, len(acc.Samples)),
	}
	for i, sm := range acc.Samples {
		v.Samples[i] = SampleView{
			ChunkID:        sm.ChunkID,
			State:          sm.State,
			ExpectedDigest: sm.ExpectedDigest,
			ObservedDigest: sm.ObservedDigest,
			Reason:         sm.Reason,
		}
	}
	return v
}
