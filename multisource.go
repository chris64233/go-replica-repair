package goreplicarepair

import (
	"context"
	"sort"
)

// CreateMultiSourceRepair opens a repair session that may draw chunks from
// several healthy source replicas. The target manifest, the candidate set and
// every candidate's manifest version and chunk digests are frozen into the
// session at creation time; a source publishing a newer manifest afterwards
// never changes this session's selection basis.
//
// For every chunk of the target manifest the candidates that declared it must
// agree on a single digest (which must also equal the target manifest's
// digest). The source is then chosen by a stable rule — the lexicographically
// smallest (SourceID, Version) among the owners — so the assignment is
// verifiable and identical for the whole session. Any chunk whose candidates
// disagree, or that no candidate owns, is recorded as a divergence and the
// session is created in the blocked state: it never hands out leases.
//
// Creation is idempotent on the caller-chosen session number: the same number
// with the same target manifest and candidate set returns the original
// session id; a changed candidate version, target manifest or digest returns
// ErrSessionConflict.
func (s *Service) CreateMultiSourceRepair(ctx context.Context, req MultiSourceRequest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateMultiSourceRequest(req); err != nil {
		return "", err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, sess := range k.sessions {
		if sess.SessionNo == req.ID {
			if !sameSessionBasis(sess, req) {
				return "", ErrSessionConflict
			}
			return sess.ID, nil
		}
	}
	for _, sess := range k.sessions {
		if sess.TargetID == req.TargetID && (sess.State == SessionRunning || sess.State == SessionBlocked) {
			return "", ErrSessionExists
		}
	}

	frozen := cloneManifest(req.Manifest)
	candidates := freezeCandidates(req.Candidates)
	chunks, divergences := assignSources(frozen, candidates)

	now := k.clock.Now()
	state := SessionRunning
	if len(divergences) > 0 {
		state = SessionBlocked
	}
	sess := &session{
		ID:          k.newID("sess"),
		SessionNo:   req.ID,
		Version:     1,
		Multi:       true,
		SourceID:    frozen.Source,
		TargetID:    req.TargetID,
		ObjectID:    frozen.ObjectID,
		State:       state,
		Candidates:  candidates,
		Divergences: divergences,
		Frozen:      frozen,
		Chunks:      chunks,
		Deadline:    now.Add(s.sessionTTL),
		CreatedAt:   now,
	}
	k.sessions[sess.ID] = sess
	// Pin the frozen target manifest so cleanup never deletes a blob the
	// published replica still references.
	key := manifestKey(frozen.Source, frozen.Version)
	if _, ok := k.manifests[key]; !ok {
		k.manifests[key] = cloneManifest(frozen)
	}
	if err := k.persist(); err != nil {
		delete(k.sessions, sess.ID)
		return "", err
	}
	return sess.ID, nil
}

// GetMultiSourceView returns the per-chunk selection, digest, lease outcome
// and any blocking divergences of a multi-source repair session.
func (s *Service) GetMultiSourceView(ctx context.Context, sessionID string) (*MultiSourceView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	sess, ok := k.sessions[sessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}
	s.sweepLocked(sess)
	v := &MultiSourceView{
		SessionID:   sess.ID,
		SessionNo:   sess.SessionNo,
		State:       sess.State,
		Version:     sess.Version,
		TargetID:    sess.TargetID,
		ManifestVer: sess.Frozen.Version,
		Candidates:  freezeCandidates(sess.Candidates),
		Divergences: append([]Divergence(nil), sess.Divergences...),
		Deadline:    sess.Deadline,
		CreatedAt:   sess.CreatedAt,
		FinishedAt:  sess.FinishedAt,
	}
	for i := range sess.Chunks {
		c := &sess.Chunks[i]
		v.Chunks = append(v.Chunks, ChunkView{
			ChunkID:    c.ID,
			SourceID:   c.SourceID,
			Digest:     c.Digest,
			State:      c.State,
			Epoch:      c.Epoch,
			LeaseID:    c.LeaseID,
			ExpiresAt:  c.ExpiresAt,
			FinalLease: c.FinalLease,
			FinalEpoch: c.FinalEpoch,
			VerifiedAt: c.VerifiedAt,
		})
	}
	return v, nil
}

// assignSources picks one source per target chunk using the frozen candidate
// declarations. The rule is deterministic: among the candidates that own the
// chunk and agree on its digest, the lexicographically smallest
// (SourceID, Version) wins. Chunks with no owner or with conflicting digests
// produce divergences instead of an arbitrary choice.
func assignSources(m Manifest, candidates []SourceCandidate) ([]chunkState, []Divergence) {
	chunks := make([]chunkState, 0, len(m.Chunks))
	var divergences []Divergence
	for _, mc := range m.Chunks {
		cs := chunkState{ID: mc.ID, State: ChunkPending, Epoch: 0, Digest: mc.Digest}
		digests := map[string]string{}
		for _, cand := range candidates {
			for _, decl := range cand.Chunks {
				if decl.ChunkID == mc.ID {
					digests[cand.SourceID+"@"+cand.Version] = decl.Digest
				}
			}
		}
		if len(digests) == 0 {
			divergences = append(divergences, Divergence{ChunkID: mc.ID, Kind: DivergenceNoSource})
			chunks = append(chunks, cs)
			continue
		}
		conflict := false
		for _, d := range digests {
			if d != mc.Digest {
				conflict = true
				break
			}
		}
		if conflict {
			divergences = append(divergences, Divergence{
				ChunkID: mc.ID,
				Kind:    DivergenceDigestConflict,
				Digests: digests,
			})
			chunks = append(chunks, cs)
			continue
		}
		owners := make([]string, 0, len(digests))
		for owner := range digests {
			owners = append(owners, owner)
		}
		sort.Strings(owners)
		cs.SourceID = ownerSource(owners[0], candidates)
		chunks = append(chunks, cs)
	}
	return chunks, divergences
}

// ownerSource extracts the source id from a "source@version" owner key.
func ownerSource(owner string, candidates []SourceCandidate) string {
	for _, cand := range candidates {
		if cand.SourceID+"@"+cand.Version == owner {
			return cand.SourceID
		}
	}
	return owner
}

// freezeCandidates deep-copies and sorts the candidate declarations so the
// frozen basis is canonical regardless of caller ordering.
func freezeCandidates(in []SourceCandidate) []SourceCandidate {
	out := make([]SourceCandidate, len(in))
	for i, cand := range in {
		out[i] = SourceCandidate{
			SourceID: cand.SourceID,
			Version:  cand.Version,
			Chunks:   append([]ChunkDeclaration(nil), cand.Chunks...),
		}
		sort.Slice(out[i].Chunks, func(a, b int) bool {
			return out[i].Chunks[a].ChunkID < out[i].Chunks[b].ChunkID
		})
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].SourceID != out[b].SourceID {
			return out[a].SourceID < out[b].SourceID
		}
		return out[a].Version < out[b].Version
	})
	return out
}

// sameSessionBasis reports whether an existing session was created from
// exactly this request basis: target, frozen manifest and candidate set
// (including versions and declared digests).
func sameSessionBasis(sess *session, req MultiSourceRequest) bool {
	if !sess.Multi || sess.TargetID != req.TargetID {
		return false
	}
	if !sameManifest(sess.Frozen, req.Manifest) {
		return false
	}
	frozen := freezeCandidates(req.Candidates)
	if len(sess.Candidates) != len(frozen) {
		return false
	}
	for i := range frozen {
		a, b := sess.Candidates[i], frozen[i]
		if a.SourceID != b.SourceID || a.Version != b.Version || len(a.Chunks) != len(b.Chunks) {
			return false
		}
		for j := range a.Chunks {
			if a.Chunks[j] != b.Chunks[j] {
				return false
			}
		}
	}
	return true
}

func sameManifest(a, b Manifest) bool {
	if a.Source != b.Source || a.Version != b.Version || a.ObjectID != b.ObjectID ||
		a.TotalSize != b.TotalSize || len(a.Chunks) != len(b.Chunks) {
		return false
	}
	for i := range a.Chunks {
		if a.Chunks[i] != b.Chunks[i] {
			return false
		}
	}
	return true
}

func validateMultiSourceRequest(req MultiSourceRequest) error {
	if req.ID == "" || req.TargetID == "" {
		return ErrCandidateInvalid
	}
	if err := validateManifest(req.Manifest); err != nil {
		return err
	}
	if len(req.Candidates) == 0 {
		return ErrCandidateInvalid
	}
	chunkIDs := map[string]bool{}
	for _, c := range req.Manifest.Chunks {
		chunkIDs[c.ID] = true
	}
	seen := map[string]bool{}
	for _, cand := range req.Candidates {
		if cand.SourceID == "" || cand.Version == "" || len(cand.Chunks) == 0 {
			return ErrCandidateInvalid
		}
		key := cand.SourceID + "@" + cand.Version
		if seen[key] {
			return ErrCandidateInvalid
		}
		seen[key] = true
		for _, decl := range cand.Chunks {
			if decl.ChunkID == "" || decl.Digest == "" || !chunkIDs[decl.ChunkID] {
				return ErrCandidateInvalid
			}
		}
	}
	return nil
}
