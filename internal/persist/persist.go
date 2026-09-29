// Package persist 提供网关状态的本地持久化（快照 + 提交日志），
// 使网关进程重启后能够恢复配额、幂等结论与审计流水，表现得如同同一个网关在继续服务。
package persist

import (
	"errors"
	"time"
)

// ErrNoSnapshot 表示还没有任何快照（首次启动）。
var ErrNoSnapshot = errors.New("persist: no snapshot")

// EventKind 区分提交日志里的两类事件。
type EventKind int

const (
	// EventCommit 是一次请求首次判定落定（含执行结论与最终配额快照）。
	EventCommit EventKind = iota
	// EventDuplicate 是一次重复提交判定（复用首次结论，不重复扣减）。
	EventDuplicate
)

// TenantState 是单个租户主计数器的持久化形态（quota 包不直接依赖 persist，用 DTO 解耦）。
type TenantState struct {
	Tokens   float64
	LastFill time.Time
	InFlight int
	Used     int64
}

// TenantAudit 是审计流水中携带的配额快照。
type TenantAudit struct {
	Tokens      float64
	InFlight    int
	QuotaUsed   int64
	QuotaRemain int64
}

// CommitEvent 是提交日志中的一条记录：判定输入、判定依据、结论与最终配额。
// Commit 与 Duplicate 共用该结构，Kind 区分类型；重复事件不携带租户状态。
type CommitEvent struct {
	Kind      EventKind
	Seq       int64
	At        time.Time
	RequestID string
	TokenID   string
	TenantID  string
	Scope     string
	Code      string
	Reason    string
	Usage     TenantAudit
	// 以下字段仅 Kind==EventCommit 时有效：落定时该租户的计数器状态（用于精确重建）。
	State *TenantState
}

// Snapshot 是一次检查点：检查点时刻的幂等表、各租户计数器、审计序号水位
// 以及保留窗口内的审计流水尾部。
type Snapshot struct {
	TakenAt       time.Time
	LastSeq       int64
	Tenants       map[string]TenantState
	Seen          map[string]CommitEvent // RequestID -> 首次提交事件
	AuditTail     []CommitEvent          // 序号连续的审计流水尾部
	AuditRetained int
}

// Store 是持久化存储抽象。生产用 FileStore（本地文件），测试可替换为内存假实现。
type Store interface {
	// LoadSnapshot 读取最近一次快照；不存在时返回 io.EOF。
	LoadSnapshot() (*Snapshot, error)
	// SaveSnapshot 原子落盘一份快照（崩溃后最多残留旧/新两版之一，不出现半写文件）。
	SaveSnapshot(*Snapshot) error
	// AppendEvent 顺序追加一条提交事件（持久化成功才视为提交）。
	AppendEvent(CommitEvent) error
	// ReadEvents 顺序读取快照之后追加的全部提交事件。
	ReadEvents() ([]CommitEvent, error)
	// TruncateEvents 丢弃现有提交日志（快照落盘后调用，保证重放有界）。
	TruncateEvents() error
}

// 确保 *FileStore 实现 Store（实现见 file.go）。
var _ Store = (*FileStore)(nil)

// FileStore 是基于本地文件系统的 Store：快照文件 + 仅追加 JSON Lines 日志。
type FileStore struct {
	dir string
}
