//go:build !windows

package goreplicarepair

import (
	"fmt"
	"os"
	"syscall"
)

// lock 对同目录下的 <statefile>.lock 加排他文件锁，串行化同一路径上
// 可能并发的多个进程（测试并行、服务多实例）。锁随进程退出自动释放。
func (f *FileStore) lock() error {
	lf, err := os.OpenFile(f.path+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open store lock: %w", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		_ = lf.Close()
		return fmt.Errorf("acquire store lock: %w", err)
	}
	f.fileLock = lf
	return nil
}

func (f *FileStore) unlock() error {
	if f.fileLock == nil {
		return nil
	}
	lf := f.fileLock
	f.fileLock = nil
	_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
	return lf.Close()
}
