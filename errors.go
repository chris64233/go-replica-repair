package goreplicarepair

import "errors"

// 哨兵错误：调用方可用 errors.Is 精确判断失败原因。
var (
	// ErrNotFound 表示会话、清单或 blob 不存在。
	ErrNotFound = errors.New("goreplicarepair: not found")
	// ErrAlreadyExists 表示唯一键冲突。
	ErrAlreadyExists = errors.New("goreplicarepair: already exists")
	// ErrInvalidArgument 表示入参不合法。
	ErrInvalidArgument = errors.New("goreplicarepair: invalid argument")
	// ErrSessionTerminal 表示会话已完成/取消/超时，拒绝新操作。
	ErrSessionTerminal = errors.New("goreplicarepair: session already terminal")
	// ErrNoAvailableChunk 表示当前没有可领取的数据块。
	ErrNoAvailableChunk = errors.New("goreplicarepair: no available chunk")
	// ErrLeaseMismatch 表示回执的会话/数据块/租约/执行版本不匹配当前占用状态，
	// 或租约已过期、工作者已被接管。旧工作者的迟到回执一律触发此错误。
	ErrLeaseMismatch = errors.New("goreplicarepair: lease mismatch")
	// ErrDigestMismatch 表示上传 blob 的实际摘要与冻结清单要求不符。
	ErrDigestMismatch = errors.New("goreplicarepair: digest mismatch")
	// ErrSizeMismatch 表示上传 blob 的大小与清单要求不符。
	ErrSizeMismatch = errors.New("goreplicarepair: size mismatch")
	// ErrBlobNotStaged 表示回执引用的 blob 没有先通过 StageBlob 登记。
	ErrBlobNotStaged = errors.New("goreplicarepair: blob not staged")
	// ErrSessionNotTerminal 表示会话仍在运行，尚不能执行清理。
	ErrSessionNotTerminal = errors.New("goreplicarepair: session not terminal")
)
