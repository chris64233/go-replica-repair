// Package goreplicarepair implements a replica-repair workflow driven by
// per-chunk content digests.
//
// The workflow is deliberately single-process: a [Service] owns the
// authoritative state and serializes every mutation behind a mutex. All state
// is persisted as a versioned JSON snapshot after each successful mutation, so
// a restarted process resumes exactly where it stopped.
//
// Core guarantees:
//
//   - A repair session freezes the source manifest (version, total size and
//     every chunk digest) at creation time. A manifest published later on the
//     source never mixes into an existing session.
//   - Workers claim chunks through expiring leases; each (re)claim bumps an
//     execution version (epoch). A completion receipt is accepted only when it
//     matches session, chunk, lease and the current epoch, and only when the
//     uploaded blob has the expected digest.
//   - The target replica switches to the repaired manifest exactly once, after
//     every chunk of the frozen manifest has been verified. Exactly one
//     completion notification is produced.
//   - Cancellation and timeout stop new leases, and chunks uploaded for the
//     session but not referenced by the successful manifest become cleanable.
//     Cleanup never removes a blob still referenced by another valid manifest
//     or replica.
package goreplicarepair

import (
	"errors"
	"time"
)

// Session states.
const (
	// SessionRunning accepts new leases and receipts.
	SessionRunning = "running"
	// SessionSucceeded reached the target manifest and the unique completion
	// notification was produced.
	SessionSucceeded = "succeeded"
	// SessionCancelled was cancelled before completion.
	SessionCancelled = "cancelled"
	// SessionTimedOut exceeded its deadline without all chunks verified.
	SessionTimedOut = "timed_out"
)

// Chunk states within a session.
const (
	// ChunkPending is waiting to be (re)claimed.
	ChunkPending = "pending"
	// ChunkLeased is currently held by a worker under a lease.
	ChunkLeased = "leased"
	// ChunkVerified has a receipt matching the current lease/epoch and its
	// uploaded blob digest equals the frozen manifest digest.
	ChunkVerified = "verified"
)

// Sentinel errors. Callers should compare with errors.Is.
var (
	// ErrManifestNotFound is returned when a referenced manifest does not exist.
	ErrManifestNotFound = errors.New("manifest not found")
	// ErrManifestExists is returned when registering a manifest whose
	// (source, version) pair is already registered.
	ErrManifestExists = errors.New("manifest already exists")
	// ErrManifestInvalid is returned for an empty/inconsistent manifest.
	ErrManifestInvalid = errors.New("manifest invalid")
	// ErrSessionNotFound is returned when a session does not exist.
	ErrSessionNotFound = errors.New("session not found")
	// ErrSessionNotRunning is returned when an operation requires a running
	// session but it has already terminated (succeeded/cancelled/timed out).
	ErrSessionNotRunning = errors.New("session not running")
	// ErrChunkNotFound is returned when a chunk id is unknown to the session.
	ErrChunkNotFound = errors.New("chunk not found")
	// ErrNoChunkAvailable is returned from ClaimChunk when the session has no
	// pending (or re-leasable) chunk at this instant.
	ErrNoChunkAvailable = errors.New("no chunk available")
	// ErrLeaseMismatch is returned when a receipt references an unknown,
	// expired or superseded lease/epoch, or targets a different chunk.
	ErrLeaseMismatch = errors.New("lease or execution version mismatch")
	// ErrDigestMismatch is returned when the receipt (or the stored blob)
	// disagrees with the frozen manifest digest for the chunk.
	ErrDigestMismatch = errors.New("chunk digest mismatch")
	// ErrBlobNotFound is returned when a receipt names a blob that was never
	// uploaded to the service.
	ErrBlobNotFound = errors.New("blob not found")
	// ErrSessionExists is returned when a target replica already has a
	// running repair session.
	ErrSessionExists = errors.New("session already exists for target")
	// ErrSessionTerminal is returned when cleanup-related operations are
	// attempted on a session that has not terminated yet.
	ErrSessionTerminal = errors.New("session still running")
	// ErrNothingToClean is returned when cleanup is requested but the session
	// has no orphaned blobs.
	ErrNothingToClean = errors.New("nothing to clean")
	// ErrAcceptanceNotFound is returned when an acceptance certificate does
	// not exist.
	ErrAcceptanceNotFound = errors.New("acceptance not found")
	// ErrAcceptanceInvalid is returned when an acceptance request or result
	// is malformed (missing id/rule/acceptor, unknown or empty sample set).
	ErrAcceptanceInvalid = errors.New("acceptance invalid")
	// ErrAcceptanceConflict is returned when an acceptance number is reused
	// with a different sample set/rule, or when a chunk's second inspection
	// result disagrees with its first one. The first result is preserved.
	ErrAcceptanceConflict = errors.New("acceptance conflict")
	// ErrAcceptanceClosed is returned when results are submitted to an
	// acceptance whose decision is already frozen (passed/failed/stale) or
	// that has been revoked.
	ErrAcceptanceClosed = errors.New("acceptance already decided or revoked")
	// ErrAcceptanceStale is returned when the repair version bound to the
	// acceptance no longer matches the replica's current version: the
	// certificate cannot continue and cannot promote anything.
	ErrAcceptanceStale = errors.New("acceptance bound to a superseded repair version")
	// ErrAcceptanceRace is returned when all samples pass but another repair
	// session for the same target is already running (re-repair in flight):
	// the old acceptance must not mark the contested version usable.
	ErrAcceptanceRace = errors.New("acceptance lost version race against re-repair")
	// ErrReasonRequired is returned when an acceptance is revoked without a
	// reason.
	ErrReasonRequired = errors.New("revocation reason required")
)

// Acceptance states.
const (
	// AcceptancePending is still collecting per-chunk sample results.
	AcceptancePending = "pending"
	// AcceptancePassed means every sampled chunk passed and the replica was
	// promoted to a usable source replica for the bound version.
	AcceptancePassed = "passed"
	// AcceptanceFailed means at least one sampled chunk failed or produced
	// conflicting results; the failed chunks are retained on the certificate
	// and the replica must not be promoted.
	AcceptanceFailed = "failed"
	// AcceptanceStale means the bound repair version was superseded before
	// the decision was frozen.
	AcceptanceStale = "stale"
	// AcceptanceRevoked was explicitly revoked; the revocation reason is
	// retained.
	AcceptanceRevoked = "revoked"
)

// Validation rules understood by the acceptance flow.
const (
	// RuleDigestSHA256 re-hashes the repaired blob and compares it against the
	// chunk digest frozen in the repair manifest.
	RuleDigestSHA256 = "digest-sha256"
)

// Chunk describes one piece of an object. Digest is the content digest
// (hex-encoded SHA-256 in this implementation) and acts as the blob key in the
// content-addressed store.
type Chunk struct {
	ID     string `json:"id"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	Digest string `json:"digest"`
}

// Manifest is an immutable description of one replica content version.
type Manifest struct {
	Source      string    `json:"source"`
	Version     string    `json:"version"`
	ObjectID    string    `json:"object_id"`
	TotalSize   int64     `json:"total_size"`
	Chunks      []Chunk   `json:"chunks"`
	PublishedAt time.Time `json:"published_at"`
}

// Lease is a bounded grant to repair one chunk.
type Lease struct {
	ID        string    `json:"id"`
	ChunkID   string    `json:"chunk_id"`
	WorkerID  string    `json:"worker_id"`
	Epoch     int       `json:"epoch"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Receipt is a worker's completion report for a leased chunk.
type Receipt struct {
	SessionID string `json:"session_id"`
	ChunkID   string `json:"chunk_id"`
	LeaseID   string `json:"lease_id"`
	Epoch     int    `json:"epoch"`
	// BlobKey identifies the uploaded content; it must equal the chunk digest
	// recorded in the frozen manifest.
	BlobKey string `json:"blob_key"`
	// Digest is the digest the worker observed. It is checked against the
	// frozen manifest and the stored blob.
	Digest string `json:"digest"`
}

// ReceiptResult tells the caller how a receipt was handled.
type ReceiptResult struct {
	Accepted bool   `json:"accepted"`
	State    string `json:"state"`
}

// CompletionNotification is the single notification produced when — and only
// when — a session succeeds.
type CompletionNotification struct {
	SessionID   string    `json:"session_id"`
	ObjectID    string    `json:"object_id"`
	TargetID    string    `json:"target_id"`
	ManifestVer string    `json:"manifest_version"`
	ChunkCount  int       `json:"chunk_count"`
	TotalSize   int64     `json:"total_size"`
	EmittedAt   time.Time `json:"emitted_at"`
}

// Progress is a point-in-time view of a repair session.
type Progress struct {
	SessionID      string     `json:"session_id"`
	State          string     `json:"state"`
	SourceID       string     `json:"source_id"`
	TargetID       string     `json:"target_id"`
	ManifestVer    string     `json:"manifest_version"`
	TotalChunks    int        `json:"total_chunks"`
	Pending        int        `json:"pending"`
	Leased         int        `json:"leased"`
	Verified       int        `json:"verified"`
	TotalSize      int64      `json:"total_size"`
	VerifiedSize   int64      `json:"verified_size"`
	Deadline       time.Time  `json:"deadline"`
	CreatedAt      time.Time  `json:"created_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	NotificationID string     `json:"notification_id,omitempty"`
}

// chunkState is the per-session mutable chunk record.
type chunkState struct {
	ID        string     `json:"id"`
	State     string     `json:"state"`
	Epoch     int        `json:"epoch"`
	LeaseID   string     `json:"lease_id,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	// FinalLease keeps the lease/epoch that produced the verified blob, so
	// late receipts from stale workers can be diagnosed.
	FinalLease string     `json:"final_lease_id,omitempty"`
	FinalEpoch int        `json:"final_epoch,omitempty"`
	BlobKey    string     `json:"blob_key,omitempty"`
	VerifiedAt *time.Time `json:"verified_at,omitempty"`
}

// session is the persisted state of one repair session.
type session struct {
	ID       string `json:"id"`
	SourceID string `json:"source_id"`
	TargetID string `json:"target_id"`
	ObjectID string `json:"object_id"`
	State    string `json:"state"`
	// Frozen is the immutable snapshot of the source manifest. Even if the
	// source publishes a new manifest during repair, this copy never changes.
	Frozen     Manifest     `json:"frozen_manifest"`
	Chunks     []chunkState `json:"chunks"`
	Deadline   time.Time    `json:"deadline"`
	CreatedAt  time.Time    `json:"created_at"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
	// Uploads records every blob key uploaded while working on this session.
	// It is the candidate set for post-termination cleanup.
	Uploads []string `json:"uploads,omitempty"`
	// Cleaned is set after the session's orphan blobs were purged.
	Cleaned             bool                    `json:"cleaned"`
	Notification        *CompletionNotification `json:"notification,omitempty"`
	NotificationEmitted bool                    `json:"notification_emitted"`
}

// replica tracks a target replica and the manifest version it currently serves.
type replica struct {
	ID              string    `json:"id"`
	ObjectID        string    `json:"object_id"`
	CurrentManifest string    `json:"current_manifest"`
	UpdatedAt       time.Time `json:"updated_at"`
	// AcceptedManifest is the repair version that passed a frozen
	// acceptance certificate. Only this version may serve as a usable source
	// replica; CurrentManifest alone (repaired but not yet accepted) is not
	// enough. Empty until the first acceptance passes.
	AcceptedManifest string `json:"accepted_manifest,omitempty"`
	// AcceptedAt records when the last successful promotion happened.
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`
}

// snapshot is the persisted on-disk format. Bumping snapshotFormat requires a
// loader migration.
const snapshotFormat = 1

type snapshot struct {
	Format        int                               `json:"format"`
	NextSeq       int                               `json:"next_seq"`
	Manifests     map[string]Manifest               `json:"manifests"`
	Sessions      map[string]*session               `json:"sessions"`
	Replicas      map[string]*replica               `json:"replicas"`
	Blobs         map[string][]byte                 `json:"blobs"`
	Notifications map[string]CompletionNotification `json:"notifications"`
	Acceptances   map[string]*acceptance            `json:"acceptances"`
}

// AcceptanceRequest opens (or re-requests) a repair acceptance certificate.
// Every field is frozen at creation: a later repair version, a changed sample
// set or a changed validation rule can never flow into an existing
// certificate.
type AcceptanceRequest struct {
	// ID is the caller-chosen acceptance number. Reusing the same number with
	// the same frozen parameters returns the original certificate; reusing it
	// with different samples/rule returns ErrAcceptanceConflict.
	ID string
	// SessionID identifies the completed repair whose version is accepted.
	SessionID string
	// SampleChunks is the frozen set of chunk ids subject to inspection.
	SampleChunks []string
	// Rule is the frozen validation rule (e.g. RuleDigestSHA256).
	Rule string
	// Acceptor is the person/system responsible for the decision.
	Acceptor string
}

// ChunkResult is one inspector's verdict for one sampled chunk. Results may
// arrive in batches over time.
type ChunkResult struct {
	ChunkID string `json:"chunk_id"`
	Pass    bool   `json:"pass"`
	// Reason is required when Pass is false.
	Reason string `json:"reason,omitempty"`
}

// ChunkResultView is one sample's recorded outcome, including a conflict flag
// raised when a second submission disagreed with the first.
type ChunkResultView struct {
	ChunkID   string     `json:"chunk_id"`
	Pass      bool       `json:"pass"`
	Reason    string     `json:"reason,omitempty"`
	Conflict  bool       `json:"conflict"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

// AcceptanceView is the query projection of an acceptance certificate.
type AcceptanceView struct {
	ID            string            `json:"id"`
	State         string            `json:"state"`
	SessionID     string            `json:"session_id"`
	TargetID      string            `json:"target_id"`
	SourceID      string            `json:"source_id"`
	RepairVersion string            `json:"repair_version"`
	SampleChunks  []string          `json:"sample_chunks"`
	Rule          string            `json:"rule"`
	Acceptor      string            `json:"acceptor"`
	Results       []ChunkResultView `json:"results"`
	// Failures lists chunk ids that failed or produced conflicting results.
	Failures     []string   `json:"failures,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	DecidedAt    *time.Time `json:"decided_at,omitempty"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
	RevokeReason string     `json:"revoke_reason,omitempty"`
	// Promoted is true only when the certificate actually marked the replica
	// usable as a new source replica.
	Promoted bool `json:"promoted"`
}

// acceptance is the persisted certificate state.
type acceptance struct {
	ID            string `json:"id"`
	State         string `json:"state"`
	SessionID     string `json:"session_id"`
	TargetID      string `json:"target_id"`
	SourceID      string `json:"source_id"`
	RepairVersion string `json:"repair_version"`
	// ManifestKey is manifestKey(source, version) the replica must currently
	// serve for the certificate to remain valid.
	ManifestKey  string     `json:"manifest_key"`
	SampleChunks []string   `json:"sample_chunks"`
	Rule         string     `json:"rule"`
	Acceptor     string     `json:"acceptor"`
	CreatedAt    time.Time  `json:"created_at"`
	DecidedAt    *time.Time `json:"decided_at,omitempty"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
	RevokeReason string     `json:"revoke_reason,omitempty"`
	Promoted     bool       `json:"promoted"`
	// Results keeps the first recorded verdict per chunk. A disagreeing
	// second submission flips Conflict instead of overwriting.
	Results map[string]*acceptanceChunk `json:"results"`
}

type acceptanceChunk struct {
	ChunkID   string     `json:"chunk_id"`
	Pass      bool       `json:"pass"`
	Reason    string     `json:"reason,omitempty"`
	Conflict  bool       `json:"conflict"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}
