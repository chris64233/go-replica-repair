package goreplicarepair

import (
	"context"
	"sort"
	"time"
)

// multiSourceName is the synthetic source id under which the merged manifest
// of a multi-source session is registered.
const multiSourceName = "multi"

// SourceChunk is one chunk a candidate source replica declares to own.
type SourceChunk struct {
	ID     string `json:"id"`
	Size   int64  `json:"size"`
	Digest string `json:"digest"`
}

// SourceCandidate describes one healthy source replica offered for a
// multi-source repair: which manifest version it serves and which chunks
// (with digests) it can provide. The whole declaration is frozen into the
// session at creation time; later changes on the source never alter an
// existing session's selection basis.
type SourceCandidate struct {
	SourceID        string        `json:"source_id"`
	ManifestVersion string        `json:"manifest_version"`
	Chunks          []SourceChunk `json:"chunks"`
}

// MultiRepairRequest opens (or idempotently re-requests) a multi-source
// repair session.
type MultiRepairRequest struct {
	// SessionID is the caller-chosen session number. Reusing it with an
	// identical target and candidate set returns the original session;
	// reusing it with any changed candidate version, chunk digest or target
	// returns ErrSessionConflict.
	SessionID string
	TargetID  string
	ObjectID  string
	// Candidates is the frozen set of source replicas. The repairable chunk
	// set is the union of all declared chunks.
	Candidates []SourceCandidate
}

// SourceDigest records what one candidate declared for a diverging chunk.
type SourceDigest struct {
	SourceID string `json:"source_id"`
	Digest   string `json:"digest"`
	Size     int64  `json:"size"`
}

// ChunkDivergence records a chunk whose candidates declared conflicting
// digests. Any divergence blocks the whole session.
type ChunkDivergence struct {
	ChunkID      string         `json:"chunk_id"`
	Declarations []SourceDigest `json:"declarations"`
}

// ChunkAssignmentView is the query projection of one chunk's frozen source
// choice plus its current lease/verification state.
type ChunkAssignmentView struct {
	ChunkID  string `json:"chunk_id"`
	SourceID string `json:"source_id"`
	Digest   string `json:"digest"`
	State    string `json:"state"`
	LeaseID  string `json:"lease_id,omitempty"`
	Epoch    int    `json:"epoch"`
	// FinalLease/FinalEpoch identify the lease whose receipt verified the
	// chunk; set only once verified.
	FinalLease string     `json:"final_lease_id,omitempty"`
	FinalEpoch int        `json:"final_epoch,omitempty"`
	VerifiedAt *time.Time `json:"verified_at,omitempty"`
}

// CandidateView is the query projection of one frozen candidate.
type CandidateView struct {
	SourceID        string `json:"source_id"`
	ManifestVersion string `json:"manifest_version"`
	ChunkCount      int    `json:"chunk_count"`
}

// RepairPlanView is the query projection of a multi-source session: the
// frozen candidates, the per-chunk source assignment with digest and lease
// outcome, and every blocking divergence.
type RepairPlanView struct {
	SessionID   string                `json:"session_id"`
	Version     int                   `json:"version"`
	State       string                `json:"state"`
	Blocked     bool                  `json:"blocked"`
	TargetID    string                `json:"target_id"`
	ObjectID    string                `json:"object_id"`
	ManifestVer string                `json:"manifest_version"`
	Candidates  []CandidateView       `json:"candidates"`
	Chunks      []ChunkAssignmentView `json:"chunks"`
	Divergences []ChunkDivergence     `json:"divergences,omitempty"`
	// NotificationID is set once the session succeeded.
	NotificationID string `json:"notification_id,omitempty"`
}

// CreateMultiSourceRepair freezes the candidate set into a new repair session
// and deterministically assigns every repairable chunk to one source.
//
// The assignment rule is stable: for each chunk, every candidate must agree
// on the digest — conflicting declarations are recorded as divergences and
// block the session (ErrSessionBlocked on any execution attempt) rather than
// letting the service pick one arbitrarily. Among the agreeing candidates the
// lexicographically smallest source id wins, so the choice is reproducible
// and cannot change while the session lives.
//
// The session number is caller-chosen: repeating the call with the same
// number and an identical target/candidate set returns the original session
// unchanged; any change to candidate versions, chunk digests or target
// returns ErrSessionConflict.
func (s *Service) CreateMultiSourceRepair(ctx context.Context, req MultiRepairRequest) (*RepairPlanView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	candidates, err := normalizeCandidates(req.Candidates)
	if err != nil {
		return nil, err
	}
	if req.SessionID == "" || req.TargetID == "" || req.ObjectID == "" {
		return nil, ErrCandidateInvalid
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()

	if existing, ok := k.sessions[req.SessionID]; ok {
		if existing.Multi && existing.TargetID == req.TargetID &&
			sameCandidateSet(existing.Sources, candidates) {
			return s.repairPlanViewLocked(existing), nil
		}
		return s.repairPlanViewLocked(existing), ErrSessionConflict
	}
	for _, sess := range k.sessions {
		if sess.TargetID == req.TargetID && sess.State == SessionRunning {
			return nil, ErrSessionExists
		}
	}

	assignments, divergences := assignChunks(candidates)

	// Merge the union of chunks into one frozen manifest so digest
	// verification, progress accounting, the atomic publish and cleanup
	// pinning all reuse the existing single-manifest machinery.
	merged := Manifest{
		Source:      multiSourceName,
		Version:     "multi-" + req.SessionID,
		ObjectID:    req.ObjectID,
		PublishedAt: k.clock.Now(),
	}
	chunkIDs := make([]string, 0, len(assignments))
	for id := range assignments {
		chunkIDs = append(chunkIDs, id)
	}
	sort.Strings(chunkIDs)
	var offset int64
	for _, id := range chunkIDs {
		a := assignments[id]
		merged.Chunks = append(merged.Chunks, Chunk{
			ID:     id,
			Offset: offset,
			Size:   a.Size,
			Digest: a.Digest,
		})
		offset += a.Size
	}
	merged.TotalSize = offset
	if len(merged.Chunks) == 0 {
		return nil, ErrCandidateInvalid
	}

	now := k.clock.Now()
	sess := &session{
		ID:          req.SessionID,
		SourceID:    multiSourceName,
		TargetID:    req.TargetID,
		ObjectID:    req.ObjectID,
		State:       SessionRunning,
		Frozen:      merged,
		Deadline:    now.Add(s.sessionTTL),
		CreatedAt:   now,
		Multi:       true,
		Version:     1,
		Sources:     candidates,
		Blocked:     len(divergences) > 0,
		Divergences: divergences,
	}
	for _, id := range chunkIDs {
		sess.Chunks = append(sess.Chunks, chunkState{
			ID:       id,
			State:    ChunkPending,
			SourceID: assignments[id].SourceID,
		})
	}
	// Register the merged manifest so cleanup pinning and the replica
	// pointer resolve it like any other manifest.
	k.manifests[manifestKey(merged.Source, merged.Version)] = cloneManifest(merged)
	k.sessions[sess.ID] = sess
	if err := k.persist(); err != nil {
		delete(k.sessions, sess.ID)
		delete(k.manifests, manifestKey(merged.Source, merged.Version))
		return nil, err
	}
	return s.repairPlanViewLocked(sess), nil
}

// GetRepairPlan returns the frozen multi-source plan for a session: chosen
// source and digest per chunk, lease/verification outcome, frozen candidates
// and any blocking divergences. Single-source sessions are projected as a
// one-candidate plan.
func (s *Service) GetRepairPlan(ctx context.Context, sessionID string) (*RepairPlanView, error) {
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
	return s.repairPlanViewLocked(sess), nil
}

// repairPlanViewLocked builds the query projection. Caller holds the lock.
func (s *Service) repairPlanViewLocked(sess *session) *RepairPlanView {
	v := &RepairPlanView{
		SessionID:   sess.ID,
		Version:     sess.Version,
		State:       sess.State,
		Blocked:     sess.Blocked,
		TargetID:    sess.TargetID,
		ObjectID:    sess.ObjectID,
		ManifestVer: sess.Frozen.Version,
		Divergences: append([]ChunkDivergence(nil), sess.Divergences...),
	}
	if sess.Notification != nil {
		v.NotificationID = sess.ID
	}
	if sess.Multi {
		for _, c := range sess.Sources {
			v.Candidates = append(v.Candidates, CandidateView{
				SourceID:        c.SourceID,
				ManifestVersion: c.ManifestVersion,
				ChunkCount:      len(c.Chunks),
			})
		}
	} else {
		v.Candidates = append(v.Candidates, CandidateView{
			SourceID:        sess.SourceID,
			ManifestVersion: sess.Frozen.Version,
			ChunkCount:      len(sess.Frozen.Chunks),
		})
	}
	for i := range sess.Chunks {
		c := &sess.Chunks[i]
		view := ChunkAssignmentView{
			ChunkID:    c.ID,
			SourceID:   c.SourceID,
			State:      c.State,
			LeaseID:    c.LeaseID,
			Epoch:      c.Epoch,
			FinalLease: c.FinalLease,
			FinalEpoch: c.FinalEpoch,
			VerifiedAt: c.VerifiedAt,
		}
		if !sess.Multi {
			view.SourceID = sess.SourceID
		}
		if mc := findManifestChunk(sess.Frozen, c.ID); mc != nil {
			view.Digest = mc.Digest
		}
		v.Chunks = append(v.Chunks, view)
	}
	return v
}

// chunkAssignment is the frozen per-chunk choice.
type chunkAssignment struct {
	SourceID string
	Digest   string
	Size     int64
}

// assignChunks applies the stable selection rule: a chunk is assigned to the
// lexicographically smallest source declaring it, but only when every
// declaring candidate agrees on digest and size; disagreement produces a
// divergence instead of an arbitrary pick. Candidates must already be
// normalized (sorted by source id).
func assignChunks(candidates []SourceCandidate) (map[string]chunkAssignment, []ChunkDivergence) {
	decls := map[string][]SourceDigest{}
	order := []string{}
	for _, cand := range candidates {
		for _, ch := range cand.Chunks {
			if _, seen := decls[ch.ID]; !seen {
				order = append(order, ch.ID)
			}
			decls[ch.ID] = append(decls[ch.ID], SourceDigest{
				SourceID: cand.SourceID,
				Digest:   ch.Digest,
				Size:     ch.Size,
			})
		}
	}
	assignments := map[string]chunkAssignment{}
	var divergences []ChunkDivergence
	for _, id := range order {
		ds := decls[id]
		agree := true
		for _, d := range ds[1:] {
			if d.Digest != ds[0].Digest || d.Size != ds[0].Size {
				agree = false
				break
			}
		}
		if !agree {
			divergences = append(divergences, ChunkDivergence{ChunkID: id, Declarations: ds})
			continue
		}
		// Candidates are sorted by source id, so ds[0] is the stable winner.
		assignments[id] = chunkAssignment{
			SourceID: ds[0].SourceID,
			Digest:   ds[0].Digest,
			Size:     ds[0].Size,
		}
	}
	return assignments, divergences
}

// normalizeCandidates validates, deep-copies and deterministically orders the
// candidate set: candidates sorted by source id, each chunk list sorted by
// chunk id. The frozen copy is what the session — and idempotency
// comparisons — rely on for the rest of its life.
func normalizeCandidates(in []SourceCandidate) ([]SourceCandidate, error) {
	if len(in) == 0 {
		return nil, ErrCandidateInvalid
	}
	seen := map[string]bool{}
	out := make([]SourceCandidate, 0, len(in))
	for _, c := range in {
		if c.SourceID == "" || c.ManifestVersion == "" || len(c.Chunks) == 0 {
			return nil, ErrCandidateInvalid
		}
		if seen[c.SourceID] {
			return nil, ErrCandidateInvalid
		}
		seen[c.SourceID] = true
		cp := SourceCandidate{
			SourceID:        c.SourceID,
			ManifestVersion: c.ManifestVersion,
			Chunks:          append([]SourceChunk(nil), c.Chunks...),
		}
		chunkIDs := map[string]bool{}
		for _, ch := range cp.Chunks {
			if ch.ID == "" || ch.Digest == "" || ch.Size <= 0 || chunkIDs[ch.ID] {
				return nil, ErrCandidateInvalid
			}
			chunkIDs[ch.ID] = true
		}
		sort.Slice(cp.Chunks, func(i, j int) bool { return cp.Chunks[i].ID < cp.Chunks[j].ID })
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SourceID < out[j].SourceID })
	return out, nil
}

// sameCandidateSet reports whether two normalized candidate sets are
// identical in source ids, manifest versions and declared chunk digests.
func sameCandidateSet(a, b []SourceCandidate) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].SourceID != b[i].SourceID || a[i].ManifestVersion != b[i].ManifestVersion ||
			len(a[i].Chunks) != len(b[i].Chunks) {
			return false
		}
		for j := range a[i].Chunks {
			if a[i].Chunks[j] != b[i].Chunks[j] {
				return false
			}
		}
	}
	return true
}
