package goreplicarepair_test

import (
	"fmt"
	"time"

	grr "github.com/chris64233/go-replica-repair"
)

// manualClock 是示例使用的固定时钟。
type manualClock struct{ t time.Time }

func (c *manualClock) Now() time.Time { return c.t }

// Example 演示一次完整的分块副本修复：
// 登记副本 -> 发布源清单 -> 创建修复会话（冻结快照）-> 领取/回执各数据块
// -> 目标副本一次性切换并得到唯一完成通知。
func Example() {
	clk := &manualClock{t: time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)}
	blobs := grr.NewMemoryBlobStore()
	svc := grr.NewService(
		grr.NewMemoryStore(), blobs,
		grr.WithClock(clk),
		grr.WithLeaseTTL(30*time.Second),
	)

	// 1) 登记副本，发布源清单。
	if err := svc.RegisterReplica("replica-a"); err != nil {
		panic(err)
	}
	if err := svc.RegisterReplica("replica-b"); err != nil {
		panic(err)
	}
	chunks := [][]byte{[]byte("hello "), []byte("world")}
	specs := make([]grr.ChunkSpec, len(chunks))
	for i, d := range chunks {
		specs[i] = grr.ChunkSpec{Index: i, Digest: grr.ComputeDigest(d), Size: int64(len(d))}
	}
	if _, err := svc.PublishManifest(grr.PublishManifestInput{
		ManifestID: "man-1", ObjectID: "obj", Version: 1,
		Chunks: specs, Replicas: []string{"replica-a"},
	}); err != nil {
		panic(err)
	}

	// 2) 创建修复会话（此刻冻结源清单快照）。
	sess, err := svc.CreateRepair(grr.CreateRepairInput{
		SourceReplica: "replica-a", TargetReplica: "replica-b", ObjectID: "obj",
	})
	if err != nil {
		panic(err)
	}

	// 3) 工作者逐块领取租约、把内容写入内容寻址存储、暂存到会话并提交回执。
	var completion *grr.CompletionRecord
	for i, d := range chunks {
		lease, err := svc.ClaimChunk(sess.ID, fmt.Sprintf("worker-%d", i))
		if err != nil {
			panic(err)
		}
		key, _, err := blobs.Put(d)
		if err != nil {
			panic(err)
		}
		if err := svc.StageBlob(sess.ID, lease.WorkerID, key); err != nil {
			panic(err)
		}
		_, rec, err := svc.CompleteChunk(grr.ChunkReceipt{
			SessionID: sess.ID, ChunkIndex: lease.ChunkIndex,
			LeaseID: lease.LeaseID, Epoch: lease.Epoch,
			WorkerID: lease.WorkerID, BlobKey: key,
		})
		if err != nil {
			panic(err)
		}
		if rec != nil {
			completion = rec
		}
	}

	// 4) 全部数据块校验通过：目标副本一次性切换，完成通知唯一。
	cur, err := svc.CurrentManifest("replica-b", "obj")
	if err != nil {
		panic(err)
	}
	fmt.Printf("switched: %v\n", cur.ID == completion.RepairedManifestID)
	fmt.Printf("chunks in repaired manifest: %d\n", len(cur.Chunks))

	// 完成通知可重复查询，始终是同一条。
	again, err := svc.GetCompletion(sess.ID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("notification stable: %v\n", again.NotificationID == completion.NotificationID)

	// 取消/超时后的清理不会删除被有效副本引用的 blob。
	report, err := svc.CleanupSession(sess.ID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("protected blobs after completion: %d\n", len(report.Skipped))
	// Output:
	// switched: true
	// chunks in repaired manifest: 2
	// notification stable: true
	// protected blobs after completion: 2
}
