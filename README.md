# go-replica-repair

基于**分块内容摘要（chunk digest）**的数据副本修复服务。目标副本数据缺失或损坏后，工作者按数据块从源副本重新拉取并上传，服务逐块校验摘要，全部通过后一次性把目标副本切换到修复后的清单。

## 核心保证

1. **清单快照冻结**：创建修复会话时深拷贝源清单（清单版本、对象总大小、每块偏移/长度/摘要）。会话存续期间源端发布新版本清单不影响既有会话，绝不会把两个版本的数据块拼在一起。
2. **有期限租约 + 执行版本（epoch）**：工作者通过租约领取数据块，租约带 TTL；每次领取/接管都生成新的租约 ID 并把该块的 epoch 加一。完成回执必须同时匹配**会话、数据块、租约 ID、epoch**，且租约未过期。旧工作者的迟到回执返回 `ErrLeaseMismatch`，不能覆盖接管者结果；上报摘要或存储内容摘要与冻结清单不符返回 `ErrDigestMismatch`，绝不标记成功。
3. **原子切换、唯一通知**：只有最后一块校验通过的那个临界区内，才会把目标副本指向新清单并写出唯一完成通知（互斥锁内的单向闸门，带幂等标记并持久化）。并发完成最后几块时，未完成副本不会提前暴露，也不会切换两次。
4. **取消/超时收敛**：会话取消或超过截止时间后停止发放新租约、拒绝新上传；已上传但未被成功清单引用的数据块进入可清理状态。清理按引用计数保护——任何有效副本当前清单引用的摘要、以及其他会话上传集仍持有的内容都不会被误删（blob 以内容摘要为键，天然去重共享）。
5. **状态持久化**：每次成功变更后整体快照落盘（JSON，写临时文件 + `rename` 原子替换），进程重启后精确恢复租约、进度、完成通知与副本指针。
6. **修复验收单**：修复成功不等于可作为新源副本。验收单在创建时冻结副本版本、抽检块集合、校验规则（摘要算法）与验收人；抽检结果可分批提交，同一块第二次结果与第一次不一致时标记为冲突而非覆盖；全部抽检通过才把副本标记为可用，部分失败保留失败块与原因并禁止作为新源；验收与再次修复/切换并发时，旧验收自动失效（stale），绝不能把正在修复的版本标成可用；同一验收号重复请求幂等返回原结果，抽检集合或规则变化返回冲突；撤销验收必须留下理由。

## 包结构

| 文件 | 内容 |
| --- | --- |
| `types.go` | 清单 / 会话 / 租约 / 回执 / 通知 / 进度等领域模型与哨兵错误 |
| `kernel.go` | 串行化状态内核、时钟抽象、快照持久化接口与文件实现 |
| `service.go` | 对外服务：登记、创建、上传、领取、回执、取消、超时推进、结果/进度查询、清理 |
| `acceptance.go` | 修复验收单：创建（幂等）、分批抽检提交、撤销、验收/可用性查询 |

## API 流程

```go
svc, err := goreplicarepair.New(
    goreplicarepair.WithPersistence(goreplicarepair.FilePersistence{Path: "state.json"}),
    // 可选：WithClock / WithSessionTTL / WithLeaseTTL
)

// 1) 源端发布并登记清单
err = svc.RegisterManifest(ctx, goreplicarepair.Manifest{
    Source: "src-a", Version: "v17", ObjectID: "obj-1",
    TotalSize: 4096,
    Chunks: []goreplicarepair.Chunk{
        {ID: "c1", Offset: 0,    Size: 2048, Digest: "ab12…"},
        {ID: "c2", Offset: 2048, Size: 2048, Digest: "cd34…"},
    },
})

// 2) 创建修复会话（冻结 v17 快照），返回会话 ID
sessID, err := svc.CreateRepair(ctx, "src-a", "v17", "replica-target-3")

// 3) 工作者循环：领取 → 从源端取数据 → 上传 → 提交回执
for {
    lease, err := svc.ClaimChunk(ctx, sessID, "worker-7")
    if errors.Is(err, goreplicarepair.ErrNoChunkAvailable) { break }

    data := fetchFromSource(lease.ChunkID)
    key, err := svc.UploadBlob(ctx, sessID, data) // key = SHA-256(data)

    _, err = svc.SubmitReceipt(ctx, goreplicarepair.Receipt{
        SessionID: sessID,
        ChunkID:   lease.ChunkID,
        LeaseID:   lease.ID,   // 必须与当前租约一致
        Epoch:     lease.Epoch, // 接管后旧 epoch 的回执会被拒
        BlobKey:   key,
        Digest:    key,
    })
    // ErrLeaseMismatch：租约过期/被接管，重新领取
    // ErrDigestMismatch：数据不对，重新从源端拉取
}

// 4) 查询进度与唯一完成通知
p, _ := svc.GetProgress(ctx, sessID)
note, _ := svc.GetResult(ctx, sessID) // 全部块校验通过后非空

// 5) 取消 / 超时
_ = svc.Cancel(ctx, sessID)
_ = svc.AdvanceTimeouts(ctx) // 通常由定时器周期调用

// 6) 终态会话清理孤立数据块（受引用保护）
keys, _ := svc.ListCleanable(ctx, sessID)
n, err := svc.CleanupSession(ctx, sessID)
```

### 修复验收

```go
// 修复会话成功后，目标副本已切换到新清单，但尚未“可用”（不能作为新源）。
// 7) 创建验收单：冻结版本、抽检块集合、校验规则与验收人
_, err = svc.CreateAcceptance(ctx, goreplicarepair.AcceptanceRequest{
    ID: "acc-1", TargetID: "replica-target-3",
    SourceID: "src-a", Version: "v17",
    ChunkIDs:  []string{"c1", "c2"},
    Rule:      goreplicarepair.AcceptanceRule{Algorithm: goreplicarepair.DigestAlgorithmSHA256},
    Inspector: "inspector-7",
})
// 同一 ID 相同参数重试返回原验收单；抽检集合/规则/验收人变化返回 ErrAcceptanceConflict

// 8) 分批提交抽检结果（服务按冻结清单摘要判定通过/失败）
_, err = svc.SubmitSamples(ctx, "acc-1", []goreplicarepair.SampleReport{
    {ChunkID: "c1", Digest: digestOfChunkC1},
})
// 同一块第二次结果不一致 → ErrSampleConflict，标记冲突且不覆盖首次结果

// 9) 查询验收结论：修复版本、抽检块、失败原因、验收决定
view, _ := svc.GetAcceptance(ctx, "acc-1")
usable, _ := svc.GetReplicaUsable(ctx, "replica-target-3") // 全部通过才为 true

// 10) 撤销验收必须给出理由，理由随验收单保留
_, err = svc.RevokeAcceptance(ctx, "acc-1", "抽检复核发现环境异常")
```

验收单决定：`pending` → `approved`（全部通过并标记副本可用）/ `rejected`（存在失败或冲突块，保留失败记录，副本禁止作为新源）/ `stale`（修复版本已变化或目标正在再次修复，旧验收不能继续也不能标记可用）/ `revoked`（验收人带理由撤销）。

### 错误语义

| 错误 | 含义 |
| --- | --- |
| `ErrManifestNotFound` / `ErrManifestExists` / `ErrManifestInvalid` | 清单不存在 / 重复登记 / 偏移或总大小不一致 |
| `ErrSessionNotFound` / `ErrSessionExists` / `ErrSessionNotRunning` | 会话不存在 / 目标已有运行中会话 / 会话已终止 |
| `ErrChunkNotFound` | 回执引用了会话外的数据块 |
| `ErrNoChunkAvailable` | 当前没有可领取（含过期重领）的数据块 |
| `ErrLeaseMismatch` | 租约 ID/epoch/数据块不匹配，或租约已过期——旧工作者回执的标准结局 |
| `ErrDigestMismatch` | 上报摘要、blob 键或实际存储内容与冻结清单摘要不符 |
| `ErrBlobNotFound` | 回执引用的 blob 从未上传 |
| `ErrSessionTerminal` / `ErrNothingToClean` | 会话仍在运行不能清理 / 没有可清理内容 |
| `ErrAcceptanceNotFound` / `ErrAcceptanceClosed` / `ErrAcceptanceStale` | 验收单不存在 / 已有终态决定 / 验收版本已被取代（再次修复或切换） |
| `ErrAcceptanceConflict` | 同一验收号以不同抽检集合、规则、验收人或版本重复创建 |
| `ErrSampleConflict` / `ErrChunkNotSampled` | 同一块第二次结果与首次不一致（标记冲突，不覆盖）/ 抽检块不在验收单内 |
| `ErrAcceptanceInvalid` / `ErrReasonRequired` | 验收请求不合法（空抽检集、未知块、不支持的规则）/ 撤销验收缺少理由 |

## 设计要点

- **单一互斥临界区**：所有状态变更都在同一把锁内完成并持久化，因此「最后一块验证 → 副本切换 → 通知写出」是原子的，杜绝提前暴露与重复切换。
- **不轻信工作者**：回执中的 `Digest` 只作声明，服务会对存储的 blob 字节重新计算 SHA-256 并与冻结清单比对。
- **内容寻址存储**：blob 以摘要为键，同内容跨会话/副本共享；清理时以「所有有效副本当前清单 + 其他会话上传集」为保护集，只删除本会话产生且全局无引用的键。
- **惰性 + 显式超时推进**：每个入口先执行 `sweepLocked`（过期租约回到 pending、过期会话进入 `timed_out`），另提供 `AdvanceTimeouts` 供定时器批量推进，测试可注入 `Clock` 确定性驱动。
- **崩溃恢复**：自增 ID 计数器一并持久化，重启后不会复用租约/会话 ID。

## 测试

```bash
go test -race ./...
```

> 某些沙箱环境下 `go test .` 的目录枚举可能看不到 `_test.go` 文件；可直接用显式文件列表运行：
>
> ```bash
> go test -race -count=1 doc.go kernel.go service.go types.go service_test.go
> ```

测试覆盖：

- 创建后源端发布新清单，会话仍按冻结版本完成切换；
- 租约过期后接管产生新 epoch，旧工作者迟到回执（验证前后）均被拒；
- 租约 TTL 过期回执被拒、错误摘要/伪造摘要被拒、缺失 blob 被拒；
- 同租约同 epoch 的重复回执幂等成功（at-least-once 重试）；
- 全部块校验完成前目标副本不可见、无通知；并发提交最后几块只切换一次、通知唯一稳定；
- 并发领取不重复分配；
- 取消/超时后拒绝新租约与上传，孤立 blob 可列出、可清理；
- 清理不会删除其他有效副本清单或其他会话仍引用的 blob；
- 文件持久化下重启恢复租约、进度并最终完成；进度统计（块数/字节数）正确。
- 验收单：分批抽检全部通过后副本才标记可用；部分失败保留失败块与原因并禁止作为新源；同一块第二次结果不一致标记冲突且不覆盖首次结果；同一验收号重复创建幂等、参数变化返回冲突；再次修复/版本切换使旧验收失效且不能把修复中版本标成可用；撤销必须带理由且理由可查。
