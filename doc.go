package goreplicarepair

// Package goreplicarepair implements a replica-repair workflow driven by
// per-chunk content digests.
//
// The workflow is deliberately single-process: a Service owns the
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
//   - Repair success alone does not make the target a usable source replica.
//     An acceptance certificate (修复验收单) freezes the repair version, the
//     sampled chunk set, the validation rule and the acceptor. Sample verdicts
//     may arrive in batches; a second verdict for one chunk must match the
//     first, otherwise the chunk is flagged conflicting instead of being
//     overwritten. Only after every sample passes is the replica promoted to
//     usable, and the promotion re-checks under the same critical section that
//     a re-repair session is not in flight: certificates bound to a version
//     that is later repaired again freeze as stale and can never mark the
//     version they lost as usable.
//   - Cancellation and timeout stop new leases, and chunks uploaded for the
//     session but not referenced by the successful manifest become cleanable.
//     Cleanup never removes a blob still referenced by another valid manifest
//     or replica.
//   - State persists across restarts: sessions, leases, progress, completion
//     notifications, replica pointers and acceptance certificates are all part
//     of the atomic snapshot.
