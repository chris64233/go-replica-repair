// Package goreplicarepair 实现基于分块摘要的数据副本修复流程。
//
// 核心抽象：
//
//   - [Manifest]：不可变的对象清单，冻结在修复会话创建时，包含版本号、
//     对象总大小和每个数据块的摘要。
//   - [Service]：修复编排入口，负责清单登记、会话创建、租约发放、
//     回执校验、原子切换、取消/超时推进与垃圾清理。
//   - [Store]：持久化接口，所有状态变更通过 CompareAndSwap 原子提交。
//   - [BlobStore]：内容寻址的数据块存储。
package goreplicarepair

import "time"

// ChunkSpec 是清单中单个数据块的摘要描述。清单一经发布即不可变。
type ChunkSpec struct {
	// Index 是数据块在对象中的序号，从 0 开始。
	Index int `json:"index"`
	// Digest 是数据块内容的摘要（sha256 十六进制）。
	Digest string `json:"digest"`
	// Size 是数据块字节数。
	Size int64 `json:"size"`
}

// Manifest 是某对象某一版本的不可变清单。
type Manifest struct {
	// ID 是清单唯一标识。
	ID string `json:"id"`
	// ObjectID 是对象标识。
	ObjectID string `json:"object_id"`
	// Version 是对象版本号，同一对象单调递增。
	Version int64 `json:"version"`
	// TotalSize 是对象总大小（各数据块大小之和，发布时校验）。
	TotalSize int64 `json:"total_size"`
	// Chunks 是数据块摘要列表，顺序固定。
	Chunks []ChunkSpec `json:"chunks"`
	// RepairedFrom 非空时表示该清单是一次修复的产物，值为源清单 ID。
	// 修复清单与源清单版本号相同（修复不产生对象新版本），因此允许
	// 同一 (ObjectID, Version) 存在一条原始清单和若干修复清单。
	RepairedFrom string `json:"repaired_from,omitempty"`
}

// SessionStatus 是修复会话状态。
type SessionStatus string

const (
	// SessionRunning 表示会话进行中，可领取/回执数据块。
	SessionRunning SessionStatus = "running"
	// SessionCompleted 表示所有数据块校验通过，目标副本已一次性切换。
	SessionCompleted SessionStatus = "completed"
	// SessionCancelled 表示会话被显式取消。
	SessionCancelled SessionStatus = "cancelled"
	// SessionTimedOut 表示会话超过截止时间仍未完成。
	SessionTimedOut SessionStatus = "timed_out"
)

// Terminal 报告会话是否已进入终态。
func (s SessionStatus) Terminal() bool {
	return s == SessionCompleted || s == SessionCancelled || s == SessionTimedOut
}

// ChunkStatus 是单个数据块的修复状态。
type ChunkStatus string

const (
	// ChunkPending 表示数据块待领取。
	ChunkPending ChunkStatus = "pending"
	// ChunkLeased 表示数据块已被租约占用、等待回执。
	ChunkLeased ChunkStatus = "leased"
	// ChunkCompleted 表示数据块回执通过摘要校验，已被修复会话接受。
	ChunkCompleted ChunkStatus = "completed"
)

// ChunkState 是数据块在某个修复会话内的可变状态。
type ChunkState struct {
	Index  int    `json:"index"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`

	Status ChunkStatus `json:"status"`

	// BlobKey 是成功回执引用的内容寻址 blob。
	BlobKey string `json:"blob_key,omitempty"`

	// 当前租约信息。每次重新领取 LeaseID 与 Epoch 都会变化。
	LeaseID        string     `json:"lease_id,omitempty"`
	Epoch          int64      `json:"epoch"`
	WorkerID       string     `json:"worker_id,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`

	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// Session 是一次修复尝试的完整状态。
type Session struct {
	ID string `json:"id"`

	ObjectID      string        `json:"object_id"`
	SourceReplica string        `json:"source_replica"`
	TargetReplica string        `json:"target_replica"`
	LeaseTTL      time.Duration `json:"lease_ttl"`
	Deadline      time.Time     `json:"deadline"`
	CreatedAt     time.Time     `json:"created_at"`
	FinishedAt    *time.Time    `json:"finished_at,omitempty"`
	Status        SessionStatus `json:"status"`
	CancelReason  string        `json:"cancel_reason,omitempty"`

	// 冻结的源清单快照：会话自始至终只针对这一份清单工作。
	FrozenManifestID string        `json:"frozen_manifest_id"`
	FrozenVersion    int64         `json:"frozen_version"`
	TotalSize        int64         `json:"total_size"`
	Chunks           []*ChunkState `json:"chunks"`

	// 修复完成后生成并登记的清单（目标副本切换到它）。
	RepairedManifestID string `json:"repaired_manifest_id,omitempty"`
	// 完成通知只允许写出一次。
	CompletionNotified bool `json:"completion_notified"`

	// StagedBlobs 记录本会话期间上传的全部 blob（key -> size），
	// 包括迟到、被取代、最终未被成功清单引用的 blob。
	StagedBlobs map[string]int64 `json:"staged_blobs,omitempty"`

	// Claims 是历史领取次数，用于审计。
	Claims int64 `json:"claims"`
}

// ChunkLease 是工作者领取数据块后获得的有期限租约。
type ChunkLease struct {
	SessionID  string    `json:"session_id"`
	LeaseID    string    `json:"lease_id"`
	WorkerID   string    `json:"worker_id"`
	ChunkIndex int       `json:"chunk_index"`
	Epoch      int64     `json:"epoch"`
	Digest     string    `json:"digest"`
	Size       int64     `json:"size"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// ChunkReceipt 是工作者完成数据块上传后的回执。
// 会话、数据块、租约、执行版本四者必须同时匹配当前占用状态才会被接受。
type ChunkReceipt struct {
	SessionID  string `json:"session_id"`
	ChunkIndex int    `json:"chunk_index"`
	LeaseID    string `json:"lease_id"`
	Epoch      int64  `json:"epoch"`
	WorkerID   string `json:"worker_id"`
	// BlobKey 是上传到 BlobStore 后的内容寻址键，必须已通过 StageBlob 登记。
	BlobKey string `json:"blob_key"`
}

// CompletionRecord 是会话完成的唯一结果凭证。
type CompletionRecord struct {
	NotificationID     string    `json:"notification_id"`
	SessionID          string    `json:"session_id"`
	ObjectID           string    `json:"object_id"`
	SourceReplica      string    `json:"source_replica"`
	TargetReplica      string    `json:"target_replica"`
	FrozenManifestID   string    `json:"frozen_manifest_id"`
	RepairedManifestID string    `json:"repaired_manifest_id"`
	Version            int64     `json:"version"`
	TotalChunks        int       `json:"total_chunks"`
	TotalSize          int64     `json:"total_size"`
	CompletedAt        time.Time `json:"completed_at"`
}

// Progress 是会话进度快照。
type Progress struct {
	SessionID          string        `json:"session_id"`
	Status             SessionStatus `json:"status"`
	Total              int           `json:"total"`
	Pending            int           `json:"pending"`
	Leased             int           `json:"leased"`
	Completed          int           `json:"completed"`
	TotalSize          int64         `json:"total_size"`
	DoneSize           int64         `json:"done_size"`
	Deadline           time.Time     `json:"deadline"`
	RepairedManifestID string        `json:"repaired_manifest_id,omitempty"`
}
