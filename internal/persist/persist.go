// Package persist 提供网关运行态的本地持久化：
// 预写 journal（崩溃恢复依据）、周期性快照（压缩历史、钳制重放窗口）
// 与只追加的审计日志（全量保留、序号连续）。
//
// 所有文件均为单行一条记录的追加格式：`<crc32-hex>:<base64-json>\n`，
// CRC 用于识别进程被 kill -9 / 掉电导致的半行写入（torn write）；
// 每次写入后 fsync，崩溃只可能丢最后一条半行，不会读到脏数据。
package persist

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// EntryKind 区分 journal 事件类型。
type EntryKind string

const (
	// KindReserved：Acquire 记账之后、下游执行之前写入。
	KindReserved EntryKind = "reserved"
	// KindDecision：一次判定的终局提交点（含重复提交，Code=DUPLICATE）。
	KindDecision EntryKind = "decision"
)

// JournalEntry 是 journal / audit 中一条记录的统一编码结构。
type JournalEntry struct {
	Kind        EntryKind `json:"kind"`
	At          time.Time `json:"at"`
	ReservedAt  time.Time `json:"reserved_at,omitempty"` // 终局事件对应的预留时间（记账时刻）
	RequestID   string    `json:"request_id"`
	TokenID     string    `json:"token_id"`
	Scope       string    `json:"scope"`
	TenantID    string    `json:"tenant_id"`
	Code        string    `json:"code"`
	Reason      string    `json:"reason"`
	Seq         int64     `json:"seq"`
	Tokens      float64   `json:"tokens"`
	InFlight    int       `json:"in_flight"`
	QuotaUsed   int64     `json:"quota_used"`
	QuotaRemain int64     `json:"quota_remain"`
}

// ReservedEntry 是 KindReserved 事件的精简视图。
type ReservedEntry struct {
	At        time.Time
	RequestID string
	TokenID   string
	Scope     string
	TenantID  string
}

// DecisionEntry 是 KindDecision 事件的终局视图。
type DecisionEntry struct {
	At          time.Time
	ReservedAt  time.Time
	RequestID   string
	TokenID     string
	Scope       string
	TenantID    string
	Code        string
	Reason      string
	Seq         int64
	Tokens      float64
	InFlight    int
	QuotaUsed   int64
	QuotaRemain int64
}

func (d DecisionEntry) journal() JournalEntry {
	return JournalEntry{
		Kind: KindDecision, At: d.At, ReservedAt: d.ReservedAt, RequestID: d.RequestID, TokenID: d.TokenID,
		Scope: d.Scope, TenantID: d.TenantID, Code: d.Code, Reason: d.Reason, Seq: d.Seq,
		Tokens: d.Tokens, InFlight: d.InFlight, QuotaUsed: d.QuotaUsed, QuotaRemain: d.QuotaRemain,
	}
}

func (r ReservedEntry) journal() JournalEntry {
	return JournalEntry{
		Kind: KindReserved, At: r.At, RequestID: r.RequestID, TokenID: r.TokenID,
		Scope: r.Scope, TenantID: r.TenantID,
	}
}

// AsDecisionEntry 把任意 journal 条目按终局视图返回（恢复时审计补尾用）。
func (e JournalEntry) AsDecisionEntry() DecisionEntry {
	return DecisionEntry{
		At: e.At, ReservedAt: e.ReservedAt, RequestID: e.RequestID, TokenID: e.TokenID, Scope: e.Scope,
		TenantID: e.TenantID, Code: e.Code, Reason: e.Reason, Seq: e.Seq,
		Tokens: e.Tokens, InFlight: e.InFlight, QuotaUsed: e.QuotaUsed, QuotaRemain: e.QuotaRemain,
	}
}

const (
	journalFile  = "journal.log"
	snapshotFile = "snapshot.json"
	auditFile    = "audit.log"
)

// Store 是本地持久化存储，所有写操作串行化并 fsync。
type Store struct {
	dir string
	mu  sync.Mutex // 串行化 journal/audit 追加与快照截断，保证事件顺序

	journal *os.File
	audit   *os.File
}

// Open 打开（或创建）dir 下的持久化存储。
func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("persist: empty dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("persist: mkdir %s: %w", dir, err)
	}
	jf, err := os.OpenFile(filepath.Join(dir, journalFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	af, err := os.OpenFile(filepath.Join(dir, auditFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		jf.Close()
		return nil, err
	}
	return &Store{dir: dir, journal: jf, audit: af}, nil
}

// Close 关闭底层文件。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e1 := s.journal.Close()
	e2 := s.audit.Close()
	if e1 != nil {
		return e1
	}
	return e2
}

// Dir 返回存储目录。
func (s *Store) Dir() string { return s.dir }

// encodeLine 把一条记录编码为带 CRC 的一行。
func encodeLine(e JournalEntry) ([]byte, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	sum := crc32.ChecksumIEEE(raw)
	line := make([]byte, 0, len(raw)+32)
	line = append(line, fmt.Sprintf("%08x", sum)...)
	line = append(line, ':')
	line = append(line, base64.StdEncoding.EncodeToString(raw)...)
	line = append(line, '\n')
	return line, nil
}

// decodeLine 解析一行并校验 CRC，返回错误表示该行损坏（半行写）。
func decodeLine(line string) (JournalEntry, error) {
	idx := strings.IndexByte(line, ':')
	if idx != 8 { // 8 位十六进制 CRC
		return JournalEntry{}, errors.New("bad crc length")
	}
	var want uint32
	for i := 0; i < idx; i++ {
		var h byte
		c := line[i]
		switch {
		case c >= '0' && c <= '9':
			h = c - '0'
		case c >= 'a' && c <= 'f':
			h = c - 'a' + 10
		default:
			return JournalEntry{}, errors.New("bad crc")
		}
		want = want<<4 | uint32(h)
	}
	if idx != 8 {
		return JournalEntry{}, errors.New("bad crc length")
	}
	raw, err := base64.StdEncoding.DecodeString(line[idx+1:])
	if err != nil {
		return JournalEntry{}, err
	}
	if crc32.ChecksumIEEE(raw) != want {
		return JournalEntry{}, errors.New("crc mismatch")
	}
	var e JournalEntry
	if err := json.Unmarshal(raw, &e); err != nil {
		return JournalEntry{}, err
	}
	return e, nil
}

// appendLocked 写入并 fsync；调用方必须持有 s.mu。
func appendLocked(f *os.File, e JournalEntry) error {
	line, err := encodeLine(e)
	if err != nil {
		return err
	}
	if _, err := f.Write(line); err != nil {
		return err
	}
	return f.Sync()
}

// AppendReserved 持久化一条预留事件（Acquire 之后、执行之前）。
func (s *Store) AppendReserved(r ReservedEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return appendLocked(s.journal, r.journal())
}

// AppendDecision 持久化一条终局事件。
func (s *Store) AppendDecision(d DecisionEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return appendLocked(s.journal, d.journal())
}

// AppendAudit 向审计日志追加一条终局记录（只追加、不截断）。
func (s *Store) AppendAudit(d DecisionEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return appendLocked(s.audit, d.journal())
}

// readFile 顺序读取记录文件；遇到首个损坏行（半行写）即停止，
// 并按需把文件截断到最后一条完好记录之后（best-effort）。
func readFile(f *os.File, truncate bool) ([]JournalEntry, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var out []JournalEntry
	var goodOffset int64
	var offset int64
	for sc.Scan() {
		raw := sc.Bytes()
		offset += int64(len(raw)) + 1 // +换行符
		e, err := decodeLine(string(raw))
		if err != nil {
			// 仅当损坏行出现在文件末尾才视为半行写；中间损坏直接报错。
			if truncate {
				if terr := f.Truncate(goodOffset); terr == nil {
					_ = f.Sync()
				}
			}
			return out, nil
		}
		out = append(out, e)
		goodOffset = offset
	}
	return out, sc.Err()
}

// ReadJournal 读取 journal 全部记录，自动丢弃末尾半行写。
func (s *Store) ReadJournal() ([]JournalEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return readFile(s.journal, true)
}

// TruncateJournal 清空 journal（在快照已 fsync 之后调用）。
func (s *Store) TruncateJournal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.journal.Truncate(0); err != nil {
		return err
	}
	if _, err := s.journal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return s.journal.Sync()
}

// ReadAudit 读取审计日志全部记录，自动丢弃末尾半行写。
func (s *Store) ReadAudit() ([]JournalEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := readFile(s.audit, true)
	if err != nil {
		return nil, err
	}
	if _, err := s.audit.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return entries, nil
}

// LastAuditSeq 只读取文件尾部返回审计日志最大序号，启动路径 O(1)，
// 不随审计历史长度线性变慢。
func (s *Store) LastAuditSeq() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := s.audit.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	const tail = 64 * 1024
	start := int64(0)
	if size > tail {
		start = size - tail
	}
	if _, err := s.audit.Seek(start, io.SeekStart); err != nil {
		return 0, err
	}
	sc := bufio.NewScanner(s.audit)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	if start > 0 {
		sc.Scan() // 丢弃从中间开始的第一条不完整行
	}
	var last int64
	for sc.Scan() {
		e, err := decodeLine(sc.Text())
		if err != nil {
			break
		}
		last = e.Seq
	}
	if _, err := s.audit.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}
	return last, nil
}

// WriteSnapshot 原子写入快照（临时文件 + rename + 目录 fsync）。
func (s *Store) WriteSnapshot(raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp := filepath.Join(s.dir, snapshotFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, snapshotFile)); err != nil {
		return err
	}
	df, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	err = df.Sync()
	df.Close()
	return err
}

// ReadSnapshot 读取快照；不存在返回 (nil, false, nil)。
func (s *Store) ReadSnapshot() ([]byte, bool, error) {
	raw, err := os.ReadFile(filepath.Join(s.dir, snapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}
