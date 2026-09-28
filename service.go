package goreplicarepair

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Default time budgets.
const (
	DefaultSessionTTL = time.Hour
	DefaultLeaseTTL   = 30 * time.Second
)

// Service is the replica-repair facade. It is safe for concurrent use.
type Service struct {
	k          *kernel
	sessionTTL time.Duration
	leaseTTL   time.Duration
}

// Option configures a Service at construction.
type Option func(*Service)

// WithClock injects a clock (mainly for tests).
func WithClock(c Clock) Option {
	return func(s *Service) { s.k.clock = c }
}

// WithPersistence injects a persistence backend.
func WithPersistence(p Persistence) Option {
	return func(s *Service) { s.k.store = p }
}

// WithSessionTTL overrides the session deadline.
func WithSessionTTL(d time.Duration) Option {
	return func(s *Service) { s.sessionTTL = d }
}

// WithLeaseTTL overrides the lease lifetime.
func WithLeaseTTL(d time.Duration) Option {
	return func(s *Service) { s.leaseTTL = d }
}

// New constructs a Service and restores state from its persistence backend.
func New(opts ...Option) (*Service, error) {
	s := &Service{
		k:          newKernel(nil, nil),
		sessionTTL: DefaultSessionTTL,
		leaseTTL:   DefaultLeaseTTL,
	}
	for _, o := range opts {
		o(s)
	}
	if err := s.k.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// manifestKey identifies a manifest by its source and version.
func manifestKey(source, version string) string { return source + "@" + version }

// RegisterManifest registers an immutable source manifest. Re-registering the
// same source+version is rejected so callers cannot silently replace a frozen
// snapshot other sessions may depend on.
func (s *Service) RegisterManifest(ctx context.Context, m Manifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateManifest(m); err != nil {
		return err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	key := manifestKey(m.Source, m.Version)
	if _, ok := k.manifests[key]; ok {
		return ErrManifestExists
	}
	k.manifests[key] = cloneManifest(m)
	return k.persist()
}

// CreateRepair freezes the named source manifest into a new repair session for
// targetID and returns its id. The frozen copy is deep: a later manifest
// publication on the source cannot alter it.
func (s *Service) CreateRepair(ctx context.Context, sourceID, manifestVersion, targetID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	m, ok := k.manifests[manifestKey(sourceID, manifestVersion)]
	if !ok {
		return "", ErrManifestNotFound
	}
	for _, sess := range k.sessions {
		if sess.TargetID == targetID && sess.State == SessionRunning {
			return "", ErrSessionExists
		}
	}

	now := k.clock.Now()
	sess := &session{
		ID:        k.newID("sess"),
		SourceID:  sourceID,
		TargetID:  targetID,
		ObjectID:  m.ObjectID,
		State:     SessionRunning,
		Frozen:    cloneManifest(m),
		Deadline:  now.Add(s.sessionTTL),
		CreatedAt: now,
		Chunks:    make([]chunkState, 0, len(m.Chunks)),
	}
	for _, c := range m.Chunks {
		sess.Chunks = append(sess.Chunks, chunkState{ID: c.ID, State: ChunkPending, Epoch: 0})
	}
	k.sessions[sess.ID] = sess
	if err := k.persist(); err != nil {
		delete(k.sessions, sess.ID)
		return "", err
	}
	return sess.ID, nil
}

// UploadBlob stores a repaired chunk blob and returns its content digest key.
// The blob is keyed by digest, so identical chunks across sessions/replicas
// share storage and cannot be deleted while any reference survives.
func (s *Service) UploadBlob(ctx context.Context, sessionID string, data []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	sess, ok := k.sessions[sessionID]
	if !ok {
		return "", ErrSessionNotFound
	}
	s.sweepLocked(sess)
	if sess.State != SessionRunning {
		return "", ErrSessionNotRunning
	}
	key := digestBytes(data)
	if _, exists := k.blobs[key]; !exists {
		stored := make([]byte, len(data))
		copy(stored, data)
		k.blobs[key] = stored
	}
	if !contains(sess.Uploads, key) {
		sess.Uploads = append(sess.Uploads, key)
	}
	return key, k.persist()
}

// ClaimChunk hands out a lease on one repairable chunk. Each claim — including
// reclaim after expiry — creates a fresh lease id and bumps the chunk epoch,
// which invalidates every receipt the previous worker might still send.
// Returns ErrNoChunkAvailable when nothing can currently be leased.
func (s *Service) ClaimChunk(ctx context.Context, sessionID, workerID string) (*Lease, error) {
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
	if sess.State != SessionRunning {
		return nil, ErrSessionNotRunning
	}
	now := k.clock.Now()
	idx := -1
	for i := range sess.Chunks {
		c := &sess.Chunks[i]
		if c.State == ChunkPending || (c.State == ChunkLeased && !c.ExpiresAt.After(now)) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, ErrNoChunkAvailable
	}
	c := &sess.Chunks[idx]
	c.Epoch++
	c.State = ChunkLeased
	c.LeaseID = k.newID("lease")
	exp := now.Add(s.leaseTTL)
	c.ExpiresAt = &exp
	lease := &Lease{
		ID:        c.LeaseID,
		ChunkID:   c.ID,
		WorkerID:  workerID,
		Epoch:     c.Epoch,
		ExpiresAt: exp,
	}
	if err := k.persist(); err != nil {
		return nil, err
	}
	cp := *lease
	return &cp, nil
}

// SubmitReceipt validates and records a worker's chunk completion.
//
// A receipt is accepted only when:
//   - the session is still running and the chunk is known;
//   - lease id, epoch and chunk all match the current grant;
//   - the lease has not expired;
//   - the referenced blob exists and its digest equals both the reported
//     digest and the digest frozen in the session manifest.
//
// Late receipts from superseded workers fail with ErrLeaseMismatch and never
// overwrite the taking-over worker's result; wrong digests fail with
// ErrDigestMismatch and never mark the chunk verified. When the accepted
// receipt completes the final chunk, the target is switched and the unique
// completion notification is emitted atomically within the same lock.
func (s *Service) SubmitReceipt(ctx context.Context, r Receipt) (*ReceiptResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	sess, ok := k.sessions[r.SessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}
	s.sweepLocked(sess)
	if sess.State != SessionRunning {
		return nil, ErrSessionNotRunning
	}
	c := findChunk(sess, r.ChunkID)
	if c == nil {
		return nil, ErrChunkNotFound
	}
	expected := findManifestChunk(sess.Frozen, r.ChunkID)
	if expected == nil {
		return nil, ErrChunkNotFound
	}

	// Idempotent retry: the same winning lease/epoch reporting again observes
	// its own recorded result rather than a spurious mismatch.
	if c.State == ChunkVerified && c.FinalLease == r.LeaseID && c.FinalEpoch == r.Epoch &&
		c.BlobKey == r.BlobKey && r.BlobKey == expected.Digest && r.Digest == expected.Digest {
		return &ReceiptResult{Accepted: true, State: c.State}, nil
	}

	// Stale-worker defense: the whole tuple must match the current grant.
	if c.State != ChunkLeased || c.LeaseID != r.LeaseID || c.Epoch != r.Epoch {
		return nil, ErrLeaseMismatch
	}
	now := k.clock.Now()
	if c.ExpiresAt == nil || !c.ExpiresAt.After(now) {
		return nil, ErrLeaseMismatch
	}
	// Validate the digest tuple before touching the store: a receipt that
	// disagrees with the frozen manifest fails with ErrDigestMismatch even if
	// the blob it names happens to be missing.
	if r.Digest != expected.Digest || r.BlobKey != expected.Digest {
		return nil, ErrDigestMismatch
	}
	blob, exists := k.blobs[r.BlobKey]
	if !exists {
		return nil, ErrBlobNotFound
	}
	// Never trust the reported digest alone: the stored bytes must hash to
	// the frozen digest too.
	if digestBytes(blob) != expected.Digest {
		return nil, ErrDigestMismatch
	}

	verifiedAt := now
	c.State = ChunkVerified
	c.FinalLease = c.LeaseID
	c.FinalEpoch = c.Epoch
	c.BlobKey = r.BlobKey
	c.VerifiedAt = &verifiedAt
	c.LeaseID = ""
	c.ExpiresAt = nil

	// One-way gate: only the transition of the last pending chunk performs
	// the switch, so concurrent finishers cannot expose an unfinished target
	// or switch twice.
	if allVerified(sess) {
		if err := s.completeLocked(sess, now); err != nil {
			return nil, err
		}
	}
	if err := k.persist(); err != nil {
		return nil, err
	}
	return &ReceiptResult{Accepted: true, State: c.State}, nil
}

// completeLocked performs the one-time target switch and notification
// emission. Caller holds the lock and has just verified the final chunk.
func (s *Service) completeLocked(sess *session, now time.Time) error {
	if sess.NotificationEmitted || sess.State != SessionRunning {
		return nil
	}
	m := sess.Frozen
	rep, ok := s.k.replicas[sess.TargetID]
	if !ok {
		rep = &replica{ID: sess.TargetID, ObjectID: m.ObjectID}
		s.k.replicas[sess.TargetID] = rep
	}
	// Atomic publish: the target only ever points at a fully verified
	// manifest version.
	rep.CurrentManifest = manifestKey(sess.SourceID, m.Version)
	rep.UpdatedAt = now

	note := CompletionNotification{
		SessionID:   sess.ID,
		ObjectID:    m.ObjectID,
		TargetID:    sess.TargetID,
		ManifestVer: m.Version,
		ChunkCount:  len(m.Chunks),
		TotalSize:   m.TotalSize,
		EmittedAt:   now,
	}
	sess.State = SessionSucceeded
	sess.FinishedAt = &now
	sess.Notification = &note
	sess.NotificationEmitted = true
	s.k.notifications[sess.ID] = note
	return nil
}

// Cancel terminates a running session. No new leases are handed out afterward;
// already uploaded, unreferenced blobs become cleanable. Cancelling an
// already-terminal session returns its state via ErrSessionNotRunning.
func (s *Service) Cancel(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	sess, ok := k.sessions[sessionID]
	if !ok {
		return ErrSessionNotFound
	}
	s.sweepLocked(sess)
	if sess.State != SessionRunning {
		return ErrSessionNotRunning
	}
	now := k.clock.Now()
	sess.State = SessionCancelled
	sess.FinishedAt = &now
	for i := range sess.Chunks {
		c := &sess.Chunks[i]
		if c.State == ChunkLeased {
			// Outstanding leases are revoked: pending chunks are reclaimable
			// by nobody once the session is terminal.
			c.State = ChunkPending
			c.LeaseID = ""
			c.ExpiresAt = nil
		}
	}
	return k.persist()
}

// AdvanceTimeouts expires all leases and sessions whose deadline has passed.
// It is normally driven by a ticker or called before requests; it is exposed so
// timeout behavior is deterministic without real sleeps.
func (s *Service) AdvanceTimeouts(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	changed := false
	for _, sess := range k.sessions {
		if sess.State == SessionRunning && !sess.Deadline.After(k.clock.Now()) {
			now := k.clock.Now()
			sess.State = SessionTimedOut
			sess.FinishedAt = &now
			changed = true
		}
		if s.sweepLocked(sess) {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return k.persist()
}

// sweepLocked expires leases (and the session itself if its deadline passed).
// Returns whether any state changed. Sweep is performed at the start of every
// mutating/reading entry point so timeout semantics never lag behind an
// observer.
func (s *Service) sweepLocked(sess *session) bool {
	if sess == nil {
		return false
	}
	now := s.k.clock.Now()
	changed := false
	if sess.State == SessionRunning && !sess.Deadline.After(now) {
		sess.State = SessionTimedOut
		sess.FinishedAt = &now
		changed = true
	}
	for i := range sess.Chunks {
		c := &sess.Chunks[i]
		if c.State == ChunkLeased && (c.ExpiresAt == nil || !c.ExpiresAt.After(now)) {
			// Lease lapsed: chunk is pending again; next claim bumps the
			// epoch and invalidates the late worker.
			c.State = ChunkPending
			c.LeaseID = ""
			c.ExpiresAt = nil
			changed = true
		}
	}
	return changed
}

// GetResult returns the unique completion notification. It is nil for sessions
// that have not succeeded.
func (s *Service) GetResult(ctx context.Context, sessionID string) (*CompletionNotification, error) {
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
	if sess.Notification == nil {
		return nil, nil
	}
	note := *sess.Notification
	return &note, nil
}

// GetProgress reports counts and bytes for a session.
func (s *Service) GetProgress(ctx context.Context, sessionID string) (*Progress, error) {
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
	p := &Progress{
		SessionID:   sess.ID,
		State:       sess.State,
		SourceID:    sess.SourceID,
		TargetID:    sess.TargetID,
		ManifestVer: sess.Frozen.Version,
		TotalChunks: len(sess.Chunks),
		TotalSize:   sess.Frozen.TotalSize,
		Deadline:    sess.Deadline,
		CreatedAt:   sess.CreatedAt,
		FinishedAt:  sess.FinishedAt,
	}
	for i := range sess.Chunks {
		c := &sess.Chunks[i]
		switch c.State {
		case ChunkPending:
			p.Pending++
		case ChunkLeased:
			p.Leased++
		case ChunkVerified:
			p.Verified++
			if mc := findManifestChunk(sess.Frozen, c.ID); mc != nil {
				p.VerifiedSize += mc.Size
			}
		}
	}
	if sess.Notification != nil {
		// The notification id is the session id: one notification per
		// session, retrievable exactly once it exists.
		p.NotificationID = sess.ID
	}
	return p, nil
}

// ListCleanable returns blob keys uploaded by a terminated session that are no
// longer referenced by any valid replica or by other sessions' uploads.
func (s *Service) ListCleanable(ctx context.Context, sessionID string) ([]string, error) {
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
	if sess.State == SessionRunning {
		return nil, ErrSessionTerminal
	}
	if sess.Cleaned {
		return nil, nil
	}
	return s.cleanableLocked(sess), nil
}

// CleanupSession purges the session's orphaned blobs. A blob is removed only if
// nothing else references it: current manifests of all known valid replicas
// and the upload sets of every other session act as protection pins.
func (s *Service) CleanupSession(ctx context.Context, sessionID string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	k := s.k
	k.mu.Lock()
	defer k.mu.Unlock()
	sess, ok := k.sessions[sessionID]
	if !ok {
		return 0, ErrSessionNotFound
	}
	s.sweepLocked(sess)
	if sess.State == SessionRunning {
		return 0, ErrSessionTerminal
	}
	if sess.Cleaned {
		return 0, ErrNothingToClean
	}
	keys := s.cleanableLocked(sess)
	if len(keys) == 0 {
		sess.Cleaned = true
		if err := k.persist(); err != nil {
			return 0, err
		}
		return 0, ErrNothingToClean
	}
	for _, key := range keys {
		delete(k.blobs, key)
	}
	sess.Cleaned = true
	if err := k.persist(); err != nil {
		return 0, err
	}
	return len(keys), nil
}

// cleanableLocked computes the session's orphan blob keys. Caller holds the
// lock. A key is protected when a valid replica's current manifest references
// it or another session uploaded it (that session may still need it).
func (s *Service) cleanableLocked(target *session) []string {
	protected := map[string]bool{}
	// Pin every digest referenced by a current replica manifest.
	for _, rep := range s.k.replicas {
		if rep.CurrentManifest == "" {
			continue
		}
		m, ok := s.k.manifests[rep.CurrentManifest]
		if !ok {
			continue
		}
		for _, c := range m.Chunks {
			protected[c.Digest] = true
		}
	}
	// Pin blobs other sessions still hold.
	for _, other := range s.k.sessions {
		if other.ID == target.ID {
			continue
		}
		for _, key := range other.Uploads {
			protected[key] = true
		}
	}
	var out []string
	seen := map[string]bool{}
	for _, key := range target.Uploads {
		if seen[key] || protected[key] {
			continue
		}
		seen[key] = true
		if _, exists := s.k.blobs[key]; exists {
			out = append(out, key)
		}
	}
	return out
}

// GetReplicaCurrentManifest exposes which manifest version a target currently
// serves (primarily for testing/inspection). Returns "" before the first
// successful switch.
func (s *Service) GetReplicaCurrentManifest(ctx context.Context, targetID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.k.mu.Lock()
	defer s.k.mu.Unlock()
	if rep, ok := s.k.replicas[targetID]; ok {
		return rep.CurrentManifest, nil
	}
	return "", nil
}

// ---- helpers ----

func validateManifest(m Manifest) error {
	if m.Source == "" || m.Version == "" || m.ObjectID == "" || len(m.Chunks) == 0 {
		return ErrManifestInvalid
	}
	ids := map[string]bool{}
	var offset int64
	for _, c := range m.Chunks {
		if c.ID == "" || c.Digest == "" || c.Size <= 0 {
			return ErrManifestInvalid
		}
		if c.Offset != offset {
			return ErrManifestInvalid
		}
		if ids[c.ID] {
			return ErrManifestInvalid
		}
		ids[c.ID] = true
		offset += c.Size
	}
	if offset != m.TotalSize {
		return ErrManifestInvalid
	}
	return nil
}

func cloneManifest(m Manifest) Manifest {
	cp := m
	if m.Chunks != nil {
		cp.Chunks = make([]Chunk, len(m.Chunks))
		copy(cp.Chunks, m.Chunks)
	}
	return cp
}

func findChunk(sess *session, id string) *chunkState {
	for i := range sess.Chunks {
		if sess.Chunks[i].ID == id {
			return &sess.Chunks[i]
		}
	}
	return nil
}

func findManifestChunk(m Manifest, id string) *Chunk {
	for i := range m.Chunks {
		if m.Chunks[i].ID == id {
			return &m.Chunks[i]
		}
	}
	return nil
}

func allVerified(sess *session) bool {
	for i := range sess.Chunks {
		if sess.Chunks[i].State != ChunkVerified {
			return false
		}
	}
	return true
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// DigestOf exposes the content digest used as blob key, for callers and tests.
func DigestOf(data []byte) string { return digestBytes(data) }
