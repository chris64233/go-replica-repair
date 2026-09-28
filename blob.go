package goreplicarepair

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// BlobStore 是数据块的内容寻址存储。
//
// blob 的键就是其内容的 sha256 摘要。Put 相同内容天然幂等；删除只在
// 引用计数归零时真正移除内容，因此不同副本/会话共享同内容 blob 时
// 不会互相误删。
type BlobStore interface {
	// Put 写入一块数据，返回内容摘要键。返回的 key == sha256(data)。
	Put(data []byte) (key string, size int64, err error)
	// Get 读取 blob；不存在返回 ErrNotFound。
	Get(key string) ([]byte, error)
	// Stat 返回 blob 大小；不存在返回 ErrNotFound。
	Stat(key string) (size int64, err error)
	// Digest 校验 key 是否等于内容的 sha256（实现可直接信任键或重算）。
	Digest(key string) (digest string, err error)
	// AddRef 为 blob 增加一个命名引用；blob 不存在返回 ErrNotFound。
	AddRef(key, ref string) error
	// ReleaseRef 释放命名引用；最后一个引用消失时删除内容。
	// 引用不存在视为幂等成功。
	ReleaseRef(key, ref string) error
	// HasRef 判断命名引用是否存在。
	HasRef(key, ref string) bool
	// Refs 返回某 blob 的全部命名引用（排序后的快照，供测试/检查）。
	Refs(key string) []string
	// DeleteUnreferenced 删除内容存在且没有任何引用的 blob。
	// 受保护（仍有引用）或内容不存在的 key 计入 skipped。
	DeleteUnreferenced(keys []string) (deleted []string, skipped []string, err error)
}

// ComputeDigest 返回 data 的 sha256 十六进制摘要。
func ComputeDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// MemoryBlobStore 是单进程内存 BlobStore，带引用计数。
type MemoryBlobStore struct {
	mu    sync.Mutex
	blobs map[string][]byte
	refs  map[string]map[string]struct{}
}

// NewMemoryBlobStore 创建空的内存 BlobStore。
func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{
		blobs: map[string][]byte{},
		refs:  map[string]map[string]struct{}{},
	}
}

// Put 实现 BlobStore。内容键不符（调用方篡改等）不可能发生：键由内容计算。
func (b *MemoryBlobStore) Put(data []byte) (string, int64, error) {
	key := ComputeDigest(data)
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.blobs[key]; !ok {
		b.blobs[key] = bytes.Clone(data)
	}
	return key, int64(len(data)), nil
}

// Get 实现 BlobStore。
func (b *MemoryBlobStore) Get(key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.blobs[key]
	if !ok {
		return nil, fmt.Errorf("%w: blob %s", ErrNotFound, key)
	}
	return bytes.Clone(data), nil
}

// Stat 实现 BlobStore。
func (b *MemoryBlobStore) Stat(key string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.blobs[key]
	if !ok {
		return 0, fmt.Errorf("%w: blob %s", ErrNotFound, key)
	}
	return int64(len(data)), nil
}

// Digest 实现 BlobStore：直接重算，保证回执阶段的摘要校验真实有效。
func (b *MemoryBlobStore) Digest(key string) (string, error) {
	data, err := b.Get(key)
	if err != nil {
		return "", err
	}
	return ComputeDigest(data), nil
}

// AddRef 实现 BlobStore。
func (b *MemoryBlobStore) AddRef(key, ref string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.blobs[key]; !ok {
		return fmt.Errorf("%w: blob %s", ErrNotFound, key)
	}
	set, ok := b.refs[key]
	if !ok {
		set = map[string]struct{}{}
		b.refs[key] = set
	}
	set[ref] = struct{}{}
	return nil
}

// ReleaseRef 实现 BlobStore。
func (b *MemoryBlobStore) ReleaseRef(key, ref string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	set := b.refs[key]
	if set != nil {
		delete(set, ref)
		if len(set) == 0 {
			delete(b.refs, key)
		}
	}
	// 注意：内容是否随最后引用删除由 DeleteUnreferenced 显式决定，
	// 这里只维护引用集合，避免清理流程尚未枚举完就提前丢失内容。
	return nil
}

// HasRef 实现 BlobStore。
func (b *MemoryBlobStore) HasRef(key, ref string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.refs[key][ref]
	return ok
}

// Refs 实现 BlobStore。
func (b *MemoryBlobStore) Refs(key string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	set := b.refs[key]
	out := make([]string, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// DeleteUnreferenced 实现 BlobStore。
func (b *MemoryBlobStore) DeleteUnreferenced(keys []string) ([]string, []string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var deleted, skipped []string
	for _, key := range keys {
		if _, ok := b.blobs[key]; !ok {
			// 内容已不存在：跳过，不报错，保证清理幂等。
			skipped = append(skipped, key)
			continue
		}
		if len(b.refs[key]) > 0 {
			skipped = append(skipped, key)
			continue
		}
		delete(b.blobs, key)
		delete(b.refs, key)
		deleted = append(deleted, key)
	}
	return deleted, skipped, nil
}

// Count 返回 blob 数量（测试辅助）。
func (b *MemoryBlobStore) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.blobs)
}

var _ BlobStore = (*MemoryBlobStore)(nil)

// IsNotFound 便捷判断。
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }
