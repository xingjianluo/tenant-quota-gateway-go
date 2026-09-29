// 文件版 Store 的实现：snapshot.json（原子 rename 替换）+ events.log（仅追加 JSON Lines）。
package persist

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	snapshotName = "snapshot.json"
	eventsName   = "events.log"
	tmpSuffix    = ".tmp"
)

// NewFileStore 在 dir 下创建（或打开）一个文件存储，目录不存在时自动创建。
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("persist: empty dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) snapshotPath() string { return filepath.Join(s.dir, snapshotName) }
func (s *FileStore) eventsPath() string   { return filepath.Join(s.dir, eventsName) }

// fsyncDir 确保目录项变更（rename/create）落盘；不支持时静默跳过。
func fsyncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}

// LoadSnapshot 读取最近快照；没有快照文件时返回 io.EOF。
func (s *FileStore) LoadSnapshot() (*Snapshot, error) {
	data, err := os.ReadFile(s.snapshotPath())
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNoSnapshot
	}
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// SaveSnapshot 以"临时文件 + fsync + rename"原子替换快照，崩溃后只可能读到旧版或新版。
func (s *FileStore) SaveSnapshot(snap *Snapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.snapshotPath() + tmpSuffix
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	if syncErr := f.Sync(); werr == nil {
		werr = syncErr
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return werr
	}
	if err := os.Rename(tmp, s.snapshotPath()); err != nil {
		return err
	}
	fsyncDir(s.dir)
	return nil
}

// AppendEvent 顺序追加一条事件并 fsync：返回 nil 即表示事件已持久化。
func (s *FileStore) AppendEvent(e CommitEvent) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	f, err := os.OpenFile(s.eventsPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	if syncErr := f.Sync(); werr == nil {
		werr = syncErr
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	return werr
}

// ReadEvents 顺序读取全部事件。容忍末尾半行（崩溃可能留下一条没写完的记录）：
// 无法解析的行只有在它是文件中最后一个非空行时才丢弃，中间坏行视为日志损坏直接报错。
func (s *FileStore) ReadEvents() ([]CommitEvent, error) {
	data, err := os.ReadFile(s.eventsPath())
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(data, []byte{'\n'})
	var out []CommitEvent
	bad := -1
	for i, raw := range lines {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		var e CommitEvent
		if err := json.Unmarshal(line, &e); err != nil {
			if bad != -1 {
				return nil, fmt.Errorf("persist: corrupt event log at line %d", bad+1)
			}
			bad = i
			continue
		}
		if bad != -1 {
			return nil, fmt.Errorf("persist: corrupt event log at line %d", bad+1)
		}
		out = append(out, e)
	}
	return out, nil
}

// TruncateEvents 清空提交日志（快照已包含这些事件后调用）。
func (s *FileStore) TruncateEvents() error {
	f, err := os.OpenFile(s.eventsPath(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fsyncDir(s.dir)
	return nil
}
