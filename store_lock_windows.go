//go:build windows

package goreplicarepair

// Windows 平台没有 flock，退化为进程内互斥（多进程部署时应替换为外部锁）。
func (f *FileStore) lock() error { return nil }

func (f *FileStore) unlock() error {
	if f.fileLock != nil {
		err := f.fileLock.Close()
		f.fileLock = nil
		return err
	}
	return nil
}
