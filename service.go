package goreplicarepair

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Clock 抽象时间来源，测试中可注入可控时钟。
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Service 编排分块副本修复的全部流程。
//
// 所有状态变更在单个互斥锁内串行执行，并通过 Store 的 CAS 提交；
// 提交冲突时在锁内重试，因此对调用方表现为原子操作。
type Service struct {
	mu       sync.Mutex
	store    Store
	blobs    BlobStore
	clock    Clock
	leaseTTL time.Duration
	// sessionTimeout 是 CreateRepair 未显式指定超时时使用的默认值。
	sessionTimeout time.Duration
}

// Option 配置 Service。
type Option func(*Service)

// WithClock 注入自定义时钟（测试用）。
func WithClock(c Clock) Option {
	return func(s *Service) { s.clock = c }
}

// WithLeaseTTL 设置租约默认时长，最小 1 秒。
func WithLeaseTTL(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.leaseTTL = d
		}
	}
}

// WithSessionTimeout 设置会话默认超时，最小 1 秒。
func WithSessionTimeout(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.sessionTimeout = d
		}
	}
}

// NewService 创建修复服务。
func NewService(store Store, blobs BlobStore, opts ...Option) *Service {
	s := &Service{
		store:          store,
		blobs:          blobs,
		clock:          realClock{},
		leaseTTL:       30 * time.Second,
		sessionTimeout: 24 * time.Hour,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ---------------------------------------------------------------------------
// 清单与副本
// ---------------------------------------------------------------------------

// RegisterReplica 登记一个副本。重复登记幂等成功。
func (s *Service) RegisterReplica(replicaID string) error {
	if replicaID == "" {
		return fmt.Errorf("%w: replica id is empty", ErrInvalidArgument)
	}
	_, err := s.mutate(func(st *State) error {
		st.Replicas[replicaID] = struct{}{}
		return nil
	})
	return err
}

// PublishManifestInput 是发布清单的入参。
type PublishManifestInput struct {
	ManifestID string
	ObjectID   string
	Version    int64
	Chunks     []ChunkSpec
	// Replicas 非空时，发布后把这些副本的当前清单指向新清单。
	Replicas []string
}

// PublishManifest 登记一份不可变清单。
//
// 同一 (ObjectID, Version) 的原始清单只能发布一次；修复清单（RepairedFrom
// 非空，由系统内部生成）允许与原始清单共存。
func (s *Service) PublishManifest(in PublishManifestInput) (*Manifest, error) {
	if in.ManifestID == "" || in.ObjectID == "" {
		return nil, fmt.Errorf("%w: manifest id and object id are required", ErrInvalidArgument)
	}
	if in.Version <= 0 {
		return nil, fmt.Errorf("%w: version must be positive", ErrInvalidArgument)
	}
	if len(in.Chunks) == 0 {
		return nil, fmt.Errorf("%w: manifest must contain at least one chunk", ErrInvalidArgument)
	}
	var total int64
	for i, c := range in.Chunks {
		if c.Index != i {
			return nil, fmt.Errorf("%w: chunk at position %d has index %d", ErrInvalidArgument, i, c.Index)
		}
		if c.Digest == "" {
			return nil, fmt.Errorf("%w: chunk %d has empty digest", ErrInvalidArgument, i)
		}
		if c.Size < 0 {
			return nil, fmt.Errorf("%w: chunk %d has negative size", ErrInvalidArgument, i)
		}
		total += c.Size
	}

	var out *Manifest
	_, err := s.mutate(func(st *State) error {
		if _, exists := st.Manifests[in.ManifestID]; exists {
			return fmt.Errorf("%w: manifest %s", ErrAlreadyExists, in.ManifestID)
		}
		for _, m := range st.Manifests {
			if m.ObjectID == in.ObjectID && m.Version == in.Version && m.RepairedFrom == "" {
				return fmt.Errorf("%w: object %s version %d", ErrAlreadyExists, in.ObjectID, in.Version)
			}
		}
		for _, r := range in.Replicas {
			if _, ok := st.Replicas[r]; !ok {
				return fmt.Errorf("%w: replica %s is not registered", ErrInvalidArgument, r)
			}
		}
		m := &Manifest{
			ID:        in.ManifestID,
			ObjectID:  in.ObjectID,
			Version:   in.Version,
			TotalSize: total,
			Chunks:    append([]ChunkSpec(nil), in.Chunks...),
		}
		st.Manifests[m.ID] = m
		for _, r := range in.Replicas {
			cur, ok := st.ReplicaCurrent[r]
			if !ok {
				cur = map[string]string{}
				st.ReplicaCurrent[r] = cur
			}
			cur[in.ObjectID] = m.ID
		}
		out = m
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetManifest 返回清单的快照副本。
func (s *Service) GetManifest(manifestID string) (*Manifest, error) {
	st, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	m, ok := st.Manifests[manifestID]
	if !ok {
		return nil, fmt.Errorf("%w: manifest %s", ErrNotFound, manifestID)
	}
	cp := *m
	cp.Chunks = append([]ChunkSpec(nil), m.Chunks...)
	return &cp, nil
}

// CurrentManifest 返回某副本上某对象当前对外暴露的清单。
func (s *Service) CurrentManifest(replicaID, objectID string) (*Manifest, error) {
	st, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	objs, ok := st.ReplicaCurrent[replicaID]
	if !ok {
		return nil, fmt.Errorf("%w: replica %s", ErrNotFound, replicaID)
	}
	id, ok := objs[objectID]
	if !ok {
		return nil, fmt.Errorf("%w: object %s on replica %s", ErrNotFound, objectID, replicaID)
	}
	return s.GetManifest(id)
}

// ---------------------------------------------------------------------------
// 修复会话
// ---------------------------------------------------------------------------

// CreateRepairInput 是创建修复会话的入参。
type CreateRepairInput struct {
	SourceReplica string
	TargetReplica string
	ObjectID      string
	// Timeout<=0 时使用服务默认会话超时。
	Timeout time.Duration
	// LeaseTTL<=0 时使用服务默认租约时长。
	LeaseTTL time.Duration
}

// CreateRepair 创建修复会话，并在创建瞬间冻结源副本当前清单：
// 清单 ID、版本、对象总大小、各数据块摘要全部复制进会话。之后源端即使
// 发布新版本清单，本会话也只针对冻结快照工作，绝不混用两个版本的数据块。
func (s *Service) CreateRepair(in CreateRepairInput) (*Session, error) {
	if in.ObjectID == "" || in.SourceReplica == "" || in.TargetReplica == "" {
		return nil, fmt.Errorf("%w: object, source and target replica are required", ErrInvalidArgument)
	}
	if in.SourceReplica == in.TargetReplica {
		return nil, fmt.Errorf("%w: source and target replica must differ", ErrInvalidArgument)
	}
	timeout := in.Timeout
	if timeout <= 0 {
		timeout = s.sessionTimeout
	}
	leaseTTL := in.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = s.leaseTTL
	}

	var out *Session
	_, err := s.mutate(func(st *State) error {
		if _, ok := st.Replicas[in.SourceReplica]; !ok {
			return fmt.Errorf("%w: source replica %s is not registered", ErrInvalidArgument, in.SourceReplica)
		}
		if _, ok := st.Replicas[in.TargetReplica]; !ok {
			return fmt.Errorf("%w: target replica %s is not registered", ErrInvalidArgument, in.TargetReplica)
		}
		objs, ok := st.ReplicaCurrent[in.SourceReplica]
		if !ok {
			return fmt.Errorf("%w: object %s on source replica %s", ErrNotFound, in.ObjectID, in.SourceReplica)
		}
		manifestID, ok := objs[in.ObjectID]
		if !ok {
			return fmt.Errorf("%w: object %s on source replica %s", ErrNotFound, in.ObjectID, in.SourceReplica)
		}
		m := st.Manifests[manifestID]
		now := s.clock.Now()
		sess := &Session{
			ID:               s.allocID(st, "sess"),
			ObjectID:         in.ObjectID,
			SourceReplica:    in.SourceReplica,
			TargetReplica:    in.TargetReplica,
			LeaseTTL:         leaseTTL,
			Deadline:         now.Add(timeout),
			CreatedAt:        now,
			Status:           SessionRunning,
			FrozenManifestID: m.ID,
			FrozenVersion:    m.Version,
			TotalSize:        m.TotalSize,
			StagedBlobs:      map[string]int64{},
		}
		sess.Chunks = make([]*ChunkState, len(m.Chunks))
		for i, c := range m.Chunks {
			sess.Chunks[i] = &ChunkState{
				Index:  c.Index,
				Digest: c.Digest,
				Size:   c.Size,
				Status: ChunkPending,
			}
		}
		st.Sessions[sess.ID] = sess
		out = sess
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cloneSession(out), nil
}

// GetSession 返回会话快照。
func (s *Service) GetSession(sessionID string) (*Session, error) {
	st, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	sess, ok := st.Sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: session %s", ErrNotFound, sessionID)
	}
	return cloneSession(sess), nil
}

// StageBlob 把工作者上传的 blob 登记到会话暂存区。
//
// 修复期间所有上传都必须先暂存：取消/超时后需要清理的“已上传但未被成功
// 清单引用”的数据块，正是依据暂存记录枚举。blob 内容本身存于 BlobStore。
func (s *Service) StageBlob(sessionID, workerID, key string) error {
	size, err := s.blobs.Stat(key)
	if err != nil {
		return err
	}
	_, err = s.mutate(func(st *State) error {
		sess, ok := st.Sessions[sessionID]
		if !ok {
			return fmt.Errorf("%w: session %s", ErrNotFound, sessionID)
		}
		// 终态会话拒绝新上传登记，避免取消后仍有新垃圾产生。
		if sess.Status.Terminal() {
			return fmt.Errorf("%w: session %s is %s", ErrSessionTerminal, sessionID, sess.Status)
		}
		if sess.StagedBlobs == nil {
			sess.StagedBlobs = map[string]int64{}
		}
		sess.StagedBlobs[key] = size
		// 登记一个存活引用：只要会话还在运行，其暂存内容不得被任何清理删除。
		if !s.blobs.HasRef(key, stageRef(sessionID, key)) {
			if err := s.blobs.AddRef(key, stageRef(sessionID, key)); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// ClaimChunk 领取一个待修复（或租约已过期）的数据块，返回带期限的新租约。
//
// 每次领取（包括同一数据块被不同工作者接管）都生成新的 LeaseID 与
// Epoch（执行版本）；回执必须同时匹配会话、数据块、租约和执行版本，
// 因此旧工作者的迟到回执不可能覆盖接管者的结果。
// 会话取消/超时后停止发放新租约。
func (s *Service) ClaimChunk(sessionID, workerID string) (*ChunkLease, error) {
	if sessionID == "" || workerID == "" {
		return nil, fmt.Errorf("%w: session id and worker id are required", ErrInvalidArgument)
	}
	var lease *ChunkLease
	_, err := s.mutate(func(st *State) error {
		sess, ok := st.Sessions[sessionID]
		if !ok {
			return fmt.Errorf("%w: session %s", ErrNotFound, sessionID)
		}
		now := s.clock.Now()
		if err := s.ensureRunning(sess, now); err != nil {
			return err
		}
		ch := s.pickClaimable(sess, now)
		if ch == nil {
			return ErrNoAvailableChunk
		}
		sess.Claims++
		ch.Status = ChunkLeased
		ch.Epoch++
		ch.LeaseID = s.allocID(st, "lease")
		ch.WorkerID = workerID
		exp := now.Add(sess.LeaseTTL)
		ch.LeaseExpiresAt = &exp
		lease = &ChunkLease{
			SessionID:  sess.ID,
			LeaseID:    ch.LeaseID,
			WorkerID:   workerID,
			ChunkIndex: ch.Index,
			Epoch:      ch.Epoch,
			Digest:     ch.Digest,
			Size:       ch.Size,
			ExpiresAt:  exp,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lease, nil
}

// pickClaimable 找出序号最小的可领取数据块：pending，或 leased 但租约已过期。
// 过期租约的数据块先复位再重新发放，旧引用随复位释放。
func (s *Service) pickClaimable(sess *Session, now time.Time) *ChunkState {
	for _, ch := range sess.Chunks {
		switch ch.Status {
		case ChunkPending:
			return ch
		case ChunkLeased:
			if ch.LeaseExpiresAt != nil && !now.Before(*ch.LeaseExpiresAt) {
				ch.Status = ChunkPending
				ch.WorkerID = ""
				ch.LeaseExpiresAt = nil
				ch.LeaseID = ""
				return ch
			}
		}
	}
	return nil
}

// CompleteChunk 校验并接受数据块完成回执。
//
// 接受条件（任一不满足都拒绝，数据块保持可被重新领取）：
//  1. 会话存在且仍在运行（未取消/未超时）；
//  2. 数据块序号有效；
//  3. 回执的 LeaseID、Epoch、WorkerID 与当前占用完全一致；
//  4. 租约尚未到期；
//  5. blob 已暂存到本会话，且重算摘要、大小与冻结清单逐项一致。
//
// 全部数据块通过后，在同一个状态提交中完成目标副本切换与唯一通知写出，
// 因此并发完成最后几个数据块时不会提前暴露未完成副本，也不会切换多次。
func (s *Service) CompleteChunk(rec ChunkReceipt) (*ChunkState, *CompletionRecord, error) {
	if rec.SessionID == "" || rec.WorkerID == "" || rec.LeaseID == "" || rec.BlobKey == "" {
		return nil, nil, fmt.Errorf("%w: session, worker, lease and blob are required", ErrInvalidArgument)
	}
	var outChunk *ChunkState
	var record *CompletionRecord
	_, err := s.mutate(func(st *State) error {
		sess, ok := st.Sessions[rec.SessionID]
		if !ok {
			return fmt.Errorf("%w: session %s", ErrNotFound, rec.SessionID)
		}
		now := s.clock.Now()
		if err := s.ensureRunning(sess, now); err != nil {
			return err
		}
		if rec.ChunkIndex < 0 || rec.ChunkIndex >= len(sess.Chunks) {
			return fmt.Errorf("%w: chunk index %d out of range", ErrInvalidArgument, rec.ChunkIndex)
		}
		ch := sess.Chunks[rec.ChunkIndex]
		if ch.Status != ChunkLeased {
			return fmt.Errorf("%w: chunk %d is not leased (status=%s)", ErrLeaseMismatch, ch.Index, ch.Status)
		}
		if ch.LeaseID != rec.LeaseID || ch.Epoch != rec.Epoch || ch.WorkerID != rec.WorkerID {
			// 旧工作者的迟到回执、被接管后的重放：一律拒绝。
			return fmt.Errorf("%w: session=%s chunk=%d lease=%s epoch=%d worker=%s", ErrLeaseMismatch, sess.ID, ch.Index, rec.LeaseID, rec.Epoch, rec.WorkerID)
		}
		if ch.LeaseExpiresAt == nil || !now.Before(*ch.LeaseExpiresAt) {
			return fmt.Errorf("%w: lease %s expired", ErrLeaseMismatch, ch.LeaseID)
		}
		if _, staged := sess.StagedBlobs[rec.BlobKey]; !staged {
			return fmt.Errorf("%w: blob %s for session %s", ErrBlobNotStaged, rec.BlobKey, sess.ID)
		}
		digest, err := s.blobs.Digest(rec.BlobKey)
		if err != nil {
			return err
		}
		if digest != ch.Digest {
			// 摘要不符：当前租约作废，数据块回到待领取，等待正确内容重新上传。
			// 复位状态必须随拒绝一起落盘，用 persistWith 标记。
			ch.Status = ChunkPending
			ch.WorkerID = ""
			ch.LeaseExpiresAt = nil
			ch.LeaseID = ""
			return persistWith(fmt.Errorf("%w: chunk %d expected %s got %s", ErrDigestMismatch, ch.Index, ch.Digest, digest))
		}
		size, err := s.blobs.Stat(rec.BlobKey)
		if err != nil {
			return err
		}
		if size != ch.Size {
			return fmt.Errorf("%w: chunk %d expected %d got %d", ErrSizeMismatch, ch.Index, ch.Size, size)
		}

		// 校验通过：标记数据块完成。blob 获得“成功清单成员”长期引用，
		// 暂存引用在 finalize/terminate 时统一释放；这样未引用的垃圾可清理，
		// 被成功清单引用的内容不会被误删。
		ch.Status = ChunkCompleted
		ch.BlobKey = rec.BlobKey
		t := now
		ch.CompletedAt = &t
		if !s.blobs.HasRef(rec.BlobKey, manifestMemberRef(sess.ID, ch.Index)) {
			if err := s.blobs.AddRef(rec.BlobKey, manifestMemberRef(sess.ID, ch.Index)); err != nil {
				return err
			}
		}
		ch.LeaseID = ""
		ch.WorkerID = ""
		ch.LeaseExpiresAt = nil
		outChunk = cloneChunk(ch)

		if s.allChunksDone(sess) {
			rec2, err := s.finalize(st, sess, now)
			if err != nil {
				return err
			}
			record = rec2
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return outChunk, record, nil
}

// allChunksDone 判断所有数据块是否都已校验通过。
func (s *Service) allChunksDone(sess *Session) bool {
	for _, ch := range sess.Chunks {
		if ch.Status != ChunkCompleted {
			return false
		}
	}
	return true
}

// finalize 在一次状态提交内完成收尾：
//  1. 依据冻结快照构造修复清单（版本号与源清单一致，RepairedFrom 指向源清单）；
//  2. 把目标副本的当前清单一次性切换为修复清单——切换之前目标副本对外
//     仍是旧清单，未完成状态从不暴露；
//  3. 会话置为 completed 并写出唯一完成通知（CompletionNotified 守卫，
//     重复进入只返回已有记录，绝不生成第二次切换/通知）。
func (s *Service) finalize(st *State, sess *Session, now time.Time) (*CompletionRecord, error) {
	if sess.Status == SessionCompleted {
		return s.buildRecord(st, sess), nil
	}
	src := st.Manifests[sess.FrozenManifestID]
	repairedID := s.allocID(st, "manifest")
	chunks := make([]ChunkSpec, len(sess.Chunks))
	for i, ch := range sess.Chunks {
		chunks[i] = ChunkSpec{Index: i, Digest: ch.Digest, Size: ch.Size}
	}
	repaired := &Manifest{
		ID:           repairedID,
		ObjectID:     sess.ObjectID,
		Version:      sess.FrozenVersion,
		TotalSize:    sess.TotalSize,
		Chunks:       chunks,
		RepairedFrom: src.ID,
	}
	st.Manifests[repairedID] = repaired

	cur, ok := st.ReplicaCurrent[sess.TargetReplica]
	if !ok {
		cur = map[string]string{}
		st.ReplicaCurrent[sess.TargetReplica] = cur
	}
	cur[sess.ObjectID] = repairedID

	sess.Status = SessionCompleted
	sess.RepairedManifestID = repairedID
	t := now
	sess.FinishedAt = &t
	sess.CompletionNotified = true

	// 所有暂存引用释放：清单成员引用已经覆盖成功清单引用的每个 blob，
	// 其余暂存 blob 变为无引用、可清理。
	for key := range sess.StagedBlobs {
		s.blobs.ReleaseRef(key, stageRef(sess.ID, key))
	}
	return s.buildRecord(st, sess), nil
}

// buildRecord 依据会话当前状态构造完成凭证。
func (s *Service) buildRecord(st *State, sess *Session) *CompletionRecord {
	finishedAt := sess.CreatedAt
	if sess.FinishedAt != nil {
		finishedAt = *sess.FinishedAt
	}
	return &CompletionRecord{
		NotificationID:     "notif-" + sess.ID,
		SessionID:          sess.ID,
		ObjectID:           sess.ObjectID,
		SourceReplica:      sess.SourceReplica,
		TargetReplica:      sess.TargetReplica,
		FrozenManifestID:   sess.FrozenManifestID,
		RepairedManifestID: sess.RepairedManifestID,
		Version:            sess.FrozenVersion,
		TotalChunks:        len(sess.Chunks),
		TotalSize:          sess.TotalSize,
		CompletedAt:        finishedAt,
	}
}

// GetCompletion 返回完成会话的唯一结果凭证；未完成返回 ErrSessionTerminal
// 的反向情况（会话仍在运行时返回 ErrInvalidArgument 包装的提示）。
func (s *Service) GetCompletion(sessionID string) (*CompletionRecord, error) {
	st, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	sess, ok := st.Sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: session %s", ErrNotFound, sessionID)
	}
	if sess.Status != SessionCompleted {
		return nil, fmt.Errorf("%w: session %s is %s", ErrSessionNotTerminal, sessionID, sess.Status)
	}
	return s.buildRecord(st, sess), nil
}

// GetProgress 返回会话进度。
func (s *Service) GetProgress(sessionID string) (*Progress, error) {
	st, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	sess, ok := st.Sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: session %s", ErrNotFound, sessionID)
	}
	p := &Progress{
		SessionID:          sess.ID,
		Status:             sess.Status,
		Total:              len(sess.Chunks),
		TotalSize:          sess.TotalSize,
		Deadline:           sess.Deadline,
		RepairedManifestID: sess.RepairedManifestID,
	}
	for _, ch := range sess.Chunks {
		switch ch.Status {
		case ChunkPending:
			p.Pending++
		case ChunkLeased:
			p.Leased++
		case ChunkCompleted:
			p.Completed++
			p.DoneSize += ch.Size
		}
	}
	return p, nil
}

// Cancel 显式取消会话。取消后停止发放新租约；重复取消已取消会话幂等返回，
// 取消已完成会话会被拒绝（完成不可撤销）。
func (s *Service) Cancel(sessionID, reason string) (*Session, error) {
	var out *Session
	_, err := s.mutate(func(st *State) error {
		sess, ok := st.Sessions[sessionID]
		if !ok {
			return fmt.Errorf("%w: session %s", ErrNotFound, sessionID)
		}
		if sess.Status == SessionCancelled {
			out = cloneSession(sess)
			return nil
		}
		if sess.Status.Terminal() {
			// 已完成或已超时的终态会话不可被显式取消改写。
			return fmt.Errorf("%w: session %s is already %s", ErrSessionTerminal, sessionID, sess.Status)
		}
		now := s.clock.Now()
		s.terminate(sess, SessionCancelled, reason, now)
		out = cloneSession(sess)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SweepResult 是超时推进的统计结果。
type SweepResult struct {
	TimedOutSessions []string
	ExpiredLeases    int
}

// SweepTimeouts 推进所有超时状态：
//   - 超过截止时间仍未完成的运行中会话 -> timed_out，停止发放新租约；
//   - 运行中会话内已过期的数据块租约复位为待领取（若会话尚未整体超时）。
func (s *Service) SweepTimeouts() (*SweepResult, error) {
	res := &SweepResult{}
	_, err := s.mutate(func(st *State) error {
		now := s.clock.Now()
		ids := make([]string, 0, len(st.Sessions))
		for id := range st.Sessions {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			sess := st.Sessions[id]
			if sess.Status != SessionRunning {
				continue
			}
			if !now.Before(sess.Deadline) {
				s.terminate(sess, SessionTimedOut, "session deadline exceeded", now)
				res.TimedOutSessions = append(res.TimedOutSessions, sess.ID)
				continue
			}
			for _, ch := range sess.Chunks {
				if ch.Status == ChunkLeased && ch.LeaseExpiresAt != nil && !now.Before(*ch.LeaseExpiresAt) {
					ch.Status = ChunkPending
					ch.WorkerID = ""
					ch.LeaseExpiresAt = nil
					ch.LeaseID = ""
					res.ExpiredLeases++
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// terminate 把会话推进到取消/超时终态：复位所有未完成租约、释放暂存引用，
// 并释放“已校验通过但终未进入成功清单”的数据块成员引用，使这些已上传内容
// 进入无引用（可清理）状态。是否真正删除仍由 CleanupSession 的引用检查决定，
// 因此被任何有效副本按摘要引用的内容不会丢失。
func (s *Service) terminate(sess *Session, status SessionStatus, reason string, now time.Time) {
	for _, ch := range sess.Chunks {
		switch ch.Status {
		case ChunkLeased:
			ch.Status = ChunkPending
			ch.WorkerID = ""
			ch.LeaseExpiresAt = nil
			ch.LeaseID = ""
		case ChunkCompleted:
			// 没有成功清单引用它：释放本会话授予的成员引用，交清理流程裁决。
			if ch.BlobKey != "" {
				s.blobs.ReleaseRef(ch.BlobKey, manifestMemberRef(sess.ID, ch.Index))
			}
		}
	}
	for key := range sess.StagedBlobs {
		s.blobs.ReleaseRef(key, stageRef(sess.ID, key))
	}
	sess.Status = status
	sess.CancelReason = reason
	t := now
	sess.FinishedAt = &t
}

// CleanupReport 是清理结果。
type CleanupReport struct {
	SessionID string
	// Deleted 是真正从 BlobStore 删除的 blob 键。
	Deleted []string
	// Skipped 是受保护未删（其他有效副本/运行中会话仍引用）或已不存在的键。
	Skipped []string
}

// CleanupSession 清理取消/超时会话遗留的暂存 blob。
//
// 删除前对每个候选 blob 做全量引用检查，以下内容一律跳过：
//   - 任一有效副本当前清单正在引用（按摘要匹配，跨副本共享内容受保护）；
//   - 任一仍在运行的修复会话暂存了它；
//   - 任何成功修复清单的成员引用（BlobStore 引用计数天然覆盖）。
//
// 完成会话调用本方法是无操作（其暂存 blob 已全部被成功清单引用或转为无引用）。
func (s *Service) CleanupSession(sessionID string) (*CleanupReport, error) {
	st, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	sess, ok := st.Sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: session %s", ErrNotFound, sessionID)
	}
	if !sess.Status.Terminal() {
		return nil, fmt.Errorf("%w: session %s is still %s", ErrSessionNotTerminal, sessionID, sess.Status)
	}

	// 枚举候选：会话全部暂存键。
	candidates := make([]string, 0, len(sess.StagedBlobs))
	for key := range sess.StagedBlobs {
		candidates = append(candidates, key)
	}
	sort.Strings(candidates)

	protected := s.collectProtectedDigests(st, sessionID)
	var toDelete []string
	var skipped []string
	for _, key := range candidates {
		if _, ok := protected[key]; ok {
			skipped = append(skipped, key)
			continue
		}
		toDelete = append(toDelete, key)
	}
	deleted, delSkipped, err := s.blobs.DeleteUnreferenced(toDelete)
	if err != nil {
		return nil, err
	}
	skipped = append(skipped, delSkipped...)
	sort.Strings(skipped)

	// 从会话暂存台账中移除已删除的键，保证重复清理幂等。
	if len(deleted) > 0 {
		delSet := map[string]struct{}{}
		for _, k := range deleted {
			delSet[k] = struct{}{}
		}
		_, err = s.mutate(func(st2 *State) error {
			sess2 := st2.Sessions[sessionID]
			if sess2 == nil {
				return fmt.Errorf("%w: session %s vanished", ErrNotFound, sessionID)
			}
			for k := range delSet {
				delete(sess2.StagedBlobs, k)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return &CleanupReport{SessionID: sessionID, Deleted: deleted, Skipped: skipped}, nil
}

// collectProtectedDigests 汇总绝不能删除的 blob 键：
//  1. 所有副本当前清单的成员（按摘要即 blob 键）；
//  2. 所有未终态会话暂存的 blob（其他会话可能正在使用）；
//  3. 所有已完成会话修复清单的成员（与 1 通常重叠，双保险）。
//
// 候选会话自身（selfID）已取消/超时，它的暂存不构成保护。
func (s *Service) collectProtectedDigests(st *State, selfID string) map[string]struct{} {
	p := map[string]struct{}{}
	for _, objs := range st.ReplicaCurrent {
		for _, mid := range objs {
			if m := st.Manifests[mid]; m != nil {
				for _, c := range m.Chunks {
					p[c.Digest] = struct{}{}
				}
			}
		}
	}
	for id, sess := range st.Sessions {
		if id == selfID {
			continue
		}
		if sess.Status == SessionCompleted {
			if m := st.Manifests[sess.RepairedManifestID]; m != nil {
				for _, c := range m.Chunks {
					p[c.Digest] = struct{}{}
				}
			}
			continue
		}
		if !sess.Status.Terminal() {
			for key := range sess.StagedBlobs {
				p[key] = struct{}{}
			}
		}
	}
	return p
}

// ---------------------------------------------------------------------------
// 内部辅助
// ---------------------------------------------------------------------------

// ensureRunning 惰性推进会话截止时间，并返回会话能否接受新操作。
// 惰性终止必须随本次拒绝一起落盘，因此返回 persistError。
func (s *Service) ensureRunning(sess *Session, now time.Time) error {
	if sess.Status == SessionRunning && !now.Before(sess.Deadline) {
		s.terminate(sess, SessionTimedOut, "session deadline exceeded", now)
	}
	if sess.Status != SessionRunning {
		return persistWith(fmt.Errorf("%w: session %s is %s", ErrSessionTerminal, sess.ID, sess.Status))
	}
	return nil
}

func stageRef(sessionID, key string) string {
	return fmt.Sprintf("stage:%s:%s", sessionID, key)
}

func manifestMemberRef(sessionID string, chunkIndex int) string {
	return fmt.Sprintf("manifest-member:%s:%d", sessionID, chunkIndex)
}

func cloneChunk(ch *ChunkState) *ChunkState {
	cc := *ch
	if ch.LeaseExpiresAt != nil {
		t := *ch.LeaseExpiresAt
		cc.LeaseExpiresAt = &t
	}
	if ch.CompletedAt != nil {
		t := *ch.CompletedAt
		cc.CompletedAt = &t
	}
	return &cc
}

// allocID 从状态计数器分配带前缀的唯一 ID。
func (s *Service) allocID(st *State, prefix string) string {
	st.NextSeq++
	return fmt.Sprintf("%s-%06d", prefix, st.NextSeq)
}

// snapshot 读取最新状态（深拷贝）。
func (s *Service) snapshot() (*State, int64, error) {
	st, rev, err := s.store.Load()
	if err != nil {
		return nil, 0, err
	}
	return st, rev, nil
}

// mutate 在服务锁内执行一次状态变更并 CAS 提交；冲突时重新加载重试。
// fn 直接修改传入的 *State（已是 Store 的私有深拷贝）。
func (s *Service) mutate(fn func(st *State) error) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mutateLocked(fn)
}

func (s *Service) mutateLocked(fn func(st *State) error) (int64, error) {
	st, rev, err := s.store.Load()
	if err != nil {
		return 0, err
	}
	fnErr := fn(st)
	var perr *persistError
	commitErr := fnErr
	if errors.As(fnErr, &perr) {
		// 回调在拒绝请求的同时对状态做了必须落盘的推进（如超时终止、
		// 摘要不符后复位数据块）：解包出业务错误，先提交状态再返回。
		commitErr = nil
	}
	if commitErr != nil {
		return rev, fnErr
	}
	newRev, err := s.store.Commit(rev, st)
	if err == ErrConflict {
		// 单进程内不应发生；保留重试以满足 Store 语义。
		// 重试前提：fn 对 BlobStore 的 AddRef/ReleaseRef 均幂等。
		return s.mutateLocked(fn)
	}
	if perr != nil {
		return newRev, perr.err
	}
	return newRev, err
}

// persistError 包装一个业务错误，表示该错误发生时 fn 对 *State 的修改
// 仍然必须被提交（典型场景：惰性超时推进、校验失败后复位数据块）。
type persistError struct{ err error }

func (p *persistError) Error() string { return p.err.Error() }
func (p *persistError) Unwrap() error { return p.err }

// persistWith 标记 err：携带该错误返回时状态变更仍会落盘。
func persistWith(err error) error {
	if err == nil {
		return nil
	}
	return &persistError{err: err}
}
