package goreplicarepair

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"
)

// Clock lets tests advance time deterministically.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// kernel is the serialized authoritative state. Every public Service method
// takes the lock, mutates state and persists before releasing it, which is
// what makes completion/notification single-emission even under concurrent
// workers.
type kernel struct {
	mu        sync.Mutex
	clock     Clock
	store     Persistence
	nextSeq   int
	manifests map[string]Manifest
	sessions  map[string]*session
	replicas  map[string]*replica
	// blobs is a content-addressed blob store keyed by digest. Storing the
	// bytes (rather than just keys) lets the service recompute and verify the
	// digest on every receipt instead of trusting the worker.
	blobs         map[string][]byte
	notifications map[string]CompletionNotification
}

func newKernel(clock Clock, store Persistence) *kernel {
	if clock == nil {
		clock = systemClock{}
	}
	k := &kernel{
		clock:         clock,
		store:         store,
		manifests:     map[string]Manifest{},
		sessions:      map[string]*session{},
		replicas:      map[string]*replica{},
		blobs:         map[string][]byte{},
		notifications: map[string]CompletionNotification{},
	}
	return k
}

// load restores state from the persistence layer. An empty/missing snapshot
// yields an empty kernel.
func (k *kernel) load() error {
	if k.store == nil {
		return nil
	}
	data, err := k.store.Load()
	if err != nil {
		if errors.Is(err, ErrNoSnapshot) {
			return nil
		}
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Format != snapshotFormat {
		return errors.New("unsupported snapshot format")
	}
	k.nextSeq = snap.NextSeq
	if snap.Manifests != nil {
		k.manifests = snap.Manifests
	}
	if snap.Sessions != nil {
		k.sessions = snap.Sessions
	}
	if snap.Replicas != nil {
		k.replicas = snap.Replicas
	}
	if snap.Blobs != nil {
		k.blobs = snap.Blobs
	}
	if snap.Notifications != nil {
		k.notifications = snap.Notifications
	}
	return nil
}

// persist writes the current state. Caller must hold k.mu.
func (k *kernel) persist() error {
	if k.store == nil {
		return nil
	}
	snap := snapshot{
		Format:        snapshotFormat,
		NextSeq:       k.nextSeq,
		Manifests:     k.manifests,
		Sessions:      k.sessions,
		Replicas:      k.replicas,
		Blobs:         k.blobs,
		Notifications: k.notifications,
	}
	data, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return err
	}
	return k.store.Save(data)
}

// newID returns a process-unique id with the given prefix. The counter is part
// of the persisted snapshot so ids never repeat across restarts.
func (k *kernel) newID(prefix string) string {
	k.nextSeq++
	return prefix + "-" + itoa(k.nextSeq)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ---- Persistence abstraction ----

// Persistence stores and loads the opaque state snapshot. Implementations must
// be safe for use by a single writer (the Service serializes writes).
type Persistence interface {
	Save(data []byte) error
	Load() ([]byte, error)
}

// ErrNoSnapshot is returned by a Persistence implementation when no snapshot
// has ever been written.
var ErrNoSnapshot = errors.New("no snapshot")

// FilePersistence keeps the snapshot in a single JSON file. Save is atomic: it
// writes a temp file in the same directory and renames it over the target, so
// a crash cannot leave a torn snapshot.
type FilePersistence struct {
	Path string
}

func (f FilePersistence) Save(data []byte) error {
	dir, name := splitPath(f.Path)
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, f.Path)
}

func (f FilePersistence) Load() ([]byte, error) {
	data, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoSnapshot
	}
	return data, err
}

func splitPath(p string) (dir, name string) {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == os.PathSeparator {
			return p[:i], p[i+1:]
		}
	}
	return ".", p
}

// memoryPersistence keeps the snapshot in memory; useful in tests.
type memoryPersistence struct {
	data []byte
}

func (m *memoryPersistence) Save(data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data = cp
	return nil
}

func (m *memoryPersistence) Load() ([]byte, error) {
	if m.data == nil {
		return nil, ErrNoSnapshot
	}
	cp := make([]byte, len(m.data))
	copy(cp, m.data)
	return cp, nil
}
