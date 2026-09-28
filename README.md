# go-replica-repair

基于分块摘要（chunk digest）的数据副本修复服务：把源副本的一个对象按清单
逐块复制到目标副本，完成全部数据块校验后才原子切换目标副本，并对租约、
迟到回执、取消/超时与垃圾清理提供强一致保证。

- 语言：Go 1.23，仅依赖标准库
- 模块路径：`github.com/chris64233/go-replica-repair`
- 持久化：内存 `MemoryStore`（测试/单进程）与 JSON 落盘 `FileStore`
  （写临时文件 + fsync + rename 原子替换，进程重启可恢复）

## 快速开始

```bash
go test ./... -race -count=1
```

最小用法（完整可运行版本见 `example_test.go`）：

```go
svc := goreplicarepair.NewService(
    goreplicarepair.NewMemoryStore(),       // 或 NewFileStore("state.json")
    goreplicarepair.NewMemoryBlobStore(),   // 内容寻址的数据块存储
)

// 1. 登记副本、发布源清单
_ = svc.RegisterReplica("replica-a")
_ = svc.RegisterReplica("replica-b")
specs := []goreplicarepair.ChunkSpec{
    {Index: 0, Digest: d0, Size: int64(len(p0))},
    {Index: 1, Digest: d1, Size: int64(len(p1))},
}
_, _ = svc.PublishManifest(goreplicarepair.PublishManifestInput{
    ManifestID: "man-1", ObjectID: "obj", Version: 1,
    Chunks: specs, Replicas: []string{"replica-a"},
})

// 2. 创建修复会话（此刻冻结源清单快照）
sess, _ := svc.CreateRepair(goreplicarepair.CreateRepairInput{
    SourceReplica: "replica-a", TargetReplica: "replica-b", ObjectID: "obj",
})

// 3. 工作者循环：领取 -> 写入内容寻址存储 -> 暂存 -> 回执
lease, _ := svc.ClaimChunk(sess.ID, "worker-7")
key, _, _ := blobs.Put(payload)
_ = svc.StageBlob(sess.ID, "worker-7", key)
_, completion, err := svc.CompleteChunk(goreplicarepair.ChunkReceipt{
    SessionID: sess.ID, ChunkIndex: lease.ChunkIndex,
    LeaseID: lease.LeaseID, Epoch: lease.Epoch,
    WorkerID: lease.WorkerID, BlobKey: key,
})
// 当 completion != nil 时，目标副本已切换，完成通知唯一
```

## 架构与状态机

```
                     CreateRepair（冻结源清单快照）
                               │
                               ▼
                         ┌──────────┐   全部数据块回执通过且最后一块提交
                         │ running  │──────────────────────────────► completed
                         └──────────┘
             领取过期租约      │  ▲                       （原子切换 + 唯一通知）
             可被新工作者接管  │  │ Cancel            过期/取消后：
                               ▼  │
                    cancelled / timed_out（终态）
                               │
                               ▼
                        CleanupSession（引用检查后删垃圾）
```

数据块状态：`pending → leased → completed`；`leased` 在租约到期后回到
`pending` 等待接管；摘要不符的回执也会让当前租约作废、数据块回到 `pending`。

## 需求与实现对应

### 1. 创建修复时冻结源清单

`CreateRepair` 在同一次状态提交中读取源副本当前清单，并把**清单 ID、版本、
对象总大小、每块摘要**整体复制进会话（`Session.FrozenManifestID /
FrozenVersion / TotalSize / Chunks`）。会话之后只与该快照交互：

- 源端随后发布新版本（甚至块数、总大小都变了）不会影响旧会话；
- 修复产物清单 `RepairedFrom` 指向冻结的源清单，版本号沿用源版本
  （修复不产生对象新版本），不会把两个版本的数据块拼在一起。

### 2. 有期限租约、执行版本与回执四元组匹配

`ClaimChunk` 发放带 `ExpiresAt` 的租约；**每次领取（含接管）都生成全新的
`LeaseID` 且 `Epoch` 单调递增**。`CompleteChunk` 仅当回执的
`(SessionID, ChunkIndex, LeaseID, Epoch, WorkerID)` 与当前占用完全一致、
租约未到期时才接受，并且：

- 旧工作者的迟到回执（旧 lease/epoch）→ `ErrLeaseMismatch`，不能覆盖接管者；
- 摘要与冻结清单不符 → `ErrDigestMismatch`，不能标记成功，租约作废可重领；
- blob 未先 `StageBlob` → `ErrBlobNotStaged`；
- 租约到期未交回，数据块复位为 `pending` 供他人接管。

### 3. 全部校验通过后一次性切换，完成通知唯一

`CompleteChunk` 在校验通过最后一块的**同一次状态提交**内执行 `finalize`：

1. 依据冻结快照构造修复清单并登记；
2. 把目标副本的当前清单指针一次性切到修复清单；
3. 会话置 `completed`、置 `CompletionNotified` 并返回唯一
   `CompletionRecord`。

切换前目标副本对该对象保持旧视图（未暴露/旧清单），绝不会出现“一半新块
一半旧块”的中间态。所有写操作在服务互斥锁内串行并经 Store CAS 提交，
并发完成最后多块只会产生一次切换、一条通知（`TestConcurrentFinalizeSwitchesOnce`
在 8 个工作者并发下断言通知计数恰为 1）。

### 4. 取消/超时停发租约，垃圾可清理且不误删

- `Cancel` 显式取消；`SweepTimeouts`（以及领取/回执路径上的惰性检查）把
  超过 `Deadline` 的会话推进到 `timed_out`。终态会话不再发放租约、不接受
  新暂存与回执；未到期的过期租约由 `SweepTimeouts` 复位。
- 所有上传先经 `StageBlob` 进入会话暂存台账，并在内容寻址存储上持有
  `stage:<session>:<digest>` 引用；终止时释放。已校验但未进入成功清单
  （中途取消）的数据块成员引用同样释放，使其进入可清理状态。
- `CleanupSession` 只处理终态会话，删除前做全量引用检查，跳过：
  - 任一副本**当前清单**按摘要引用的内容（跨副本共享天然受保护）；
  - 其他**仍在运行**的修复会话暂存的内容；
  - 已完成修复清单的成员。

  清理幂等，可重复执行。

### 5. 持久化与 API 面

所有会话/清单/副本指针/租约/暂存台账都在 `State` 中，经 `Store`
（`Load` + CAS `Commit`）持久化；`FileStore` 崩溃重启后完整恢复
（见 `TestFileStorePersistenceAcrossRestart`）。

| 能力 | API |
|---|---|
| 副本登记 | `RegisterReplica` |
| 清单登记 | `PublishManifest` / `GetManifest` / `CurrentManifest` |
| 创建修复（冻结） | `CreateRepair` / `GetSession` |
| 上传暂存 | `StageBlob` |
| 数据块领取 | `ClaimChunk` |
| 数据块回执 | `CompleteChunk` |
| 取消 | `Cancel`（已取消幂等；完成/超时态拒绝改写） |
| 超时推进 | `SweepTimeouts` |
| 完成结果 | `GetCompletion`（唯一 `CompletionRecord`） |
| 进度查询 | `GetProgress`（total/pending/leased/completed、字节数、截止时间） |
| 垃圾清理 | `CleanupSession`（返回 Deleted / Skipped） |

错误通过哨兵值返回，可用 `errors.Is` 判断：`ErrNotFound`、
`ErrAlreadyExists`、`ErrInvalidArgument`、`ErrSessionTerminal`、
`ErrNoAvailableChunk`、`ErrLeaseMismatch`、`ErrDigestMismatch`、
`ErrSizeMismatch`、`ErrBlobNotStaged`、`ErrSessionNotTerminal`。

## 代码结构

| 文件 | 内容 |
|---|---|
| `model.go` | `Manifest` / `Session` / `ChunkState` / `ChunkLease` / `ChunkReceipt` / `CompletionRecord` / `Progress` 与状态枚举 |
| `errors.go` | 哨兵错误 |
| `store.go` | `State`、`Store` CAS 接口、`MemoryStore`、`FileStore`（JSON 原子落盘 + 深拷贝隔离） |
| `store_lock_unix.go` / `store_lock_windows.go` | 文件锁 |
| `blob.go` | 内容寻址 `BlobStore`、`MemoryBlobStore`（sha256 键、引用计数、按引用删除） |
| `service.go` | 编排核心：冻结、租约/epoch、回执校验、`finalize`、取消/超时、清理保护 |
| `*_test.go` | 需求级自动化测试（含并发、故障重启、引用保护） |

## 测试

```bash
go test ./... -race -count=1   # 竞态检测
go test ./... -count=5         # 重复运行，压并发不稳定性
go test ./... -cover           # 语句覆盖率（约 87%）
go vet ./...
gofmt -l .
```

测试通过可注入的 `Clock` 精确推进租约/会话时间，覆盖：

- 快照冻结后源端发布新版本不串块；
- 接管产生新 lease/epoch、旧工作者迟到回执与过期租约回执被拒、伪造四元组被拒；
- 摘要不符不能成功且可重领、未暂存 blob 拒绝回执；
- 8 工作者并发完成时只切换一次、只产生一条通知，目标副本此前不可见；
- 取消/超时停发租约、错误上传可清理，源副本/其他副本/运行中会话引用的内容
  一律保留，清理幂等；
- `FileStore` 多次“重启”后清单、租约 epoch、终态、目标指针与完成凭证完整恢复。
