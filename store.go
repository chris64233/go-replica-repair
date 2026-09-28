package goreplicarepair

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// State 是持久化的全部内容。
type State struct {
	// NextSeq 是 ID 生成器的单调计数器。
	NextSeq int64 `json:"next_seq"`

	// Manifests 按 ID 保存所有已登记清单（原始清单与修复产物）。
	Manifests map[string]*Manifest `json:"manifests"`
	// ReplicaCurrent 记录 (replicaID -> objectID -> manifestID)，
	// 即每个副本当前对外暴露的清单。
	ReplicaCurrent map[string]map[string]string `json:"replica_current"`
	// Replicas 记录已存在的副本 ID 集合。
	Replicas map[string]struct{} `json:"replicas"`

	// Sessions 按 ID 保存全部修复会话。
	Sessions map[string]*Session `json:"sessions"`
}

func newState() *State {
	return &State{
		Manifests:      map[string]*Manifest{},
		ReplicaCurrent: map[string]map[string]string{},
		Replicas:       map[string]struct{}{},
		Sessions:       map[string]*Session{},
	}
}

// Store 是状态持久化抽象。
//
// Load 返回当前状态的深拷贝；Commit 在 expectedSeq 与内部计数一致时
// 以 next 原子替换当前状态并返回新计数，否则返回 ErrConflict，调用方需要
// 重新 Load 后重试。Service 在单进程内通过自身互斥锁串行化，正常路径不会
// 冲突；CAS 同时保证未来多进程/多实例扩展时不会丢失更新。
type Store interface {
	Load() (*State, int64, error)
	Commit(expectedSeq int64, next *State) (int64, error)
}

// ErrConflict 表示持久化层发生乐观并发冲突。
var ErrConflict = errors.New("goreplicarepair: store commit conflict")

// ---------------------------------------------------------------------------
// MemoryStore：进程内持久化，适合测试与单进程部署。
// ---------------------------------------------------------------------------

// MemoryStore 把状态保存在内存中，深拷贝隔离外部修改。
type MemoryStore struct {
	mu   sync.Mutex
	seq  int64
	data *State
}

// NewMemoryStore 创建空的内存 Store。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: newState()}
}

// Load 实现 Store。
func (m *MemoryStore) Load() (*State, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.data), m.seq, nil
}

// Commit 实现 Store。
func (m *MemoryStore) Commit(expectedSeq int64, next *State) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if expectedSeq != m.seq {
		return m.seq, ErrConflict
	}
	m.seq++
	m.data = cloneState(next)
	return m.seq, nil
}

// ---------------------------------------------------------------------------
// FileStore：JSON 落盘持久化，进程崩溃后可从文件恢复全部状态。
// ---------------------------------------------------------------------------

type fileStateEnvelope struct {
	Seq   int64  `json:"seq"`
	State *State `json:"state"`
}

// FileStore 把状态以 JSON 原子写入单个文件（写临时文件 + fsync + rename）。
// 同一路径通过文件锁串行化，避免多个进程互相覆盖。
type FileStore struct {
	path     string
	lockMu   sync.Mutex
	fileLock *os.File
}

// NewFileStore 打开（不存在则创建）路径 path 的文件 Store。
func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}
	f := &FileStore{path: path}
	if err := f.lock(); err != nil {
		return nil, err
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		env := fileStateEnvelope{Seq: 0, State: newState()}
		if err := f.writeAtomic(env); err != nil {
			_ = f.unlock()
			return nil, err
		}
	} else if err != nil {
		_ = f.unlock()
		return nil, err
	}
	return f, nil
}

// Close 释放文件锁。
func (f *FileStore) Close() error {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	if f.fileLock == nil {
		return nil
	}
	err := f.unlock()
	return err
}

// Load 实现 Store。
func (f *FileStore) Load() (*State, int64, error) {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	env, err := f.read()
	if err != nil {
		return nil, 0, err
	}
	return cloneState(env.State), env.Seq, nil
}

// Commit 实现 Store。
func (f *FileStore) Commit(expectedSeq int64, next *State) (int64, error) {
	f.lockMu.Lock()
	defer f.lockMu.Unlock()
	env, err := f.read()
	if err != nil {
		return 0, err
	}
	if env.Seq != expectedSeq {
		return env.Seq, ErrConflict
	}
	newEnv := fileStateEnvelope{Seq: env.Seq + 1, State: cloneState(next)}
	if err := f.writeAtomic(newEnv); err != nil {
		return env.Seq, err
	}
	return newEnv.Seq, nil
}

func (f *FileStore) read() (fileStateEnvelope, error) {
	var env fileStateEnvelope
	buf, err := os.ReadFile(f.path)
	if err != nil {
		return env, fmt.Errorf("read store: %w", err)
	}
	if err := json.Unmarshal(buf, &env); err != nil {
		return env, fmt.Errorf("decode store: %w", err)
	}
	if env.State == nil {
		env.State = newState()
	}
	return env, nil
}

func (f *FileStore) writeAtomic(env fileStateEnvelope) error {
	buf, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return fmt.Errorf("encode store: %w", err)
	}
	tmp := f.path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open temp store: %w", err)
	}
	if _, err := fh.Write(buf); err != nil {
		_ = fh.Close()
		return fmt.Errorf("write temp store: %w", err)
	}
	if err := fh.Sync(); err != nil {
		_ = fh.Close()
		return fmt.Errorf("fsync temp store: %w", err)
	}
	if err := fh.Close(); err != nil {
		return fmt.Errorf("close temp store: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("rename store: %w", err)
	}
	dir, err := os.Open(filepath.Dir(f.path))
	if err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// cloneState 返回状态的深拷贝，保证 Store 内外的指针互不可见。
func cloneState(s *State) *State {
	if s == nil {
		return newState()
	}
	c := &State{
		NextSeq:        s.NextSeq,
		Manifests:      make(map[string]*Manifest, len(s.Manifests)),
		ReplicaCurrent: make(map[string]map[string]string, len(s.ReplicaCurrent)),
		Replicas:       make(map[string]struct{}, len(s.Replicas)),
		Sessions:       make(map[string]*Session, len(s.Sessions)),
	}
	for id, m := range s.Manifests {
		mc := *m
		mc.Chunks = append([]ChunkSpec(nil), m.Chunks...)
		c.Manifests[id] = &mc
	}
	for r, objs := range s.ReplicaCurrent {
		m2 := make(map[string]string, len(objs))
		for o, v := range objs {
			m2[o] = v
		}
		c.ReplicaCurrent[r] = m2
	}
	for r := range s.Replicas {
		c.Replicas[r] = struct{}{}
	}
	for id, sess := range s.Sessions {
		c.Sessions[id] = cloneSession(sess)
	}
	return c
}

func cloneSession(s *Session) *Session {
	sc := *s
	if s.Chunks != nil {
		sc.Chunks = make([]*ChunkState, len(s.Chunks))
		for i, ch := range s.Chunks {
			cc := *ch
			if ch.LeaseExpiresAt != nil {
				t := *ch.LeaseExpiresAt
				cc.LeaseExpiresAt = &t
			}
			if ch.CompletedAt != nil {
				t := *ch.CompletedAt
				cc.CompletedAt = &t
			}
			sc.Chunks[i] = &cc
		}
	}
	if s.StagedBlobs != nil {
		sc.StagedBlobs = make(map[string]int64, len(s.StagedBlobs))
		for k, v := range s.StagedBlobs {
			sc.StagedBlobs[k] = v
		}
	}
	if s.FinishedAt != nil {
		t := *s.FinishedAt
		sc.FinishedAt = &t
	}
	return &sc
}
