package quota

import (
	"sync"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
)

// Limits 描述单个租户的配额配置。
type Limits struct {
	RatePerSecond float64 // 令牌桶填充速率（每秒令牌数）
	MaxConcurrent int     // 最大在途并发数（并发槽）
}

// Snapshot 是判定时刻的配额口径快照。
type Snapshot struct {
	RateRemaining    float64
	RateCapacity     float64
	ConcurrencyInUse int
	ConcurrencyMax   int
}

type bucket struct {
	mu     sync.Mutex
	limits Limits
	tokens float64   // 当前可用速率令牌
	last   time.Time // 上次令牌填充时刻；零值表示尚未开始记账
	inUse  int       // 当前占用的并发槽
}

// Manager 管理所有租户的速率与并发配额。
//
// 关键并发语义：
//   - 每个租户一把互斥锁，租户之间互不阻塞，实现租户间状态隔离；
//   - TryAcquire 在该锁内一次性完成“填充令牌 -> 判速率 -> 判并发 -> 记账”，
//     因此任何并发情况下都不可能出现超发或部分扣减；
//   - 被拒绝路径在任何状态变更之前返回，不会泄漏令牌或并发槽。
type Manager struct {
	clk     clock.Clock
	mu      sync.RWMutex
	tenants map[string]*bucket
}

func New(clk clock.Clock) *Manager {
	return &Manager{clk: clk, tenants: make(map[string]*bucket)}
}

// AddTenant 注册/更新租户配额配置。重复注册时保留当前在途计数与令牌状态，
// 仅替换配额参数（容量按需截断）。
func (m *Manager) AddTenant(tenantID string, l Limits) {
	m.mu.Lock()
	b, ok := m.tenants[tenantID]
	if !ok {
		// 新建桶以满桶起步，但不锁定 last：
		// 首个事件（真实请求或重放）到来时再以其时刻为起点，
		// 避免“配置时空转一段时间”或“重启重放事件早于装配时刻”造成口径偏差。
		b = &bucket{tokens: l.RatePerSecond}
		m.tenants[tenantID] = b
	}
	m.mu.Unlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.limits = l
	if b.tokens > l.RatePerSecond {
		b.tokens = l.RatePerSecond
	}
}

func (m *Manager) get(tenantID string) *bucket {
	m.mu.RLock()
	b := m.tenants[tenantID]
	m.mu.RUnlock()
	return b
}

// refill 必须在 b.mu 持有下调用：按经过时间懒填充令牌桶。
func (b *bucket) refill(now time.Time) {
	if b.last.IsZero() {
		b.last = now
		return
	}
	if now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() * b.limits.RatePerSecond
		if b.tokens > b.limits.RatePerSecond {
			b.tokens = b.limits.RatePerSecond
		}
		b.last = now
	}
}

const eps = 1e-9

// TryAcquire 原子地完成“判定 + 记账”。
func (m *Manager) TryAcquire(tenantID string) (Snapshot, model.RejectCode) {
	b := m.get(tenantID)
	if b == nil {
		return Snapshot{}, model.RejectUnknownTenant
	}
	now := m.clk.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill(now)

	if b.tokens < 1-eps {
		return b.snapshot(), model.RejectRateLimited
	}
	if b.inUse >= b.limits.MaxConcurrent {
		return b.snapshot(), model.RejectConcurrencyFull
	}

	// 全部检查通过后才记账：扣 1 令牌、占 1 并发槽。
	b.tokens--
	b.inUse++
	return b.snapshot(), model.RejectNone
}

// Release 在请求执行结束（成功或失败）后归还并发槽。
// refundRate 为 true 时同时退还 1 个速率令牌（执行失败不计费）。
func (m *Manager) Release(tenantID string, refundRate bool) {
	b := m.get(tenantID)
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inUse > 0 {
		b.inUse--
	}
	if refundRate {
		b.refill(m.clk.Now())
		if b.tokens < b.limits.RatePerSecond {
			b.tokens++
			if b.tokens > b.limits.RatePerSecond {
				b.tokens = b.limits.RatePerSecond
			}
		}
	}
}

// SnapshotOf 返回租户当前配额口径。
func (m *Manager) SnapshotOf(tenantID string) (Snapshot, bool) {
	b := m.get(tenantID)
	if b == nil {
		return Snapshot{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(m.clk.Now())
	return b.snapshot(), true
}

// 调用方必须持有 b.mu。
func (b *bucket) snapshot() Snapshot {
	return Snapshot{
		RateRemaining:    b.tokens,
		RateCapacity:     b.limits.RatePerSecond,
		ConcurrencyInUse: b.inUse,
		ConcurrencyMax:   b.limits.MaxConcurrent,
	}
}

// RestoreAcquire / RestoreRelease 仅供重启重放使用：
// 与 TryAcquire 不同，不做拒绝判定（事件本身就是已发生的事实），只恢复记账。

func (m *Manager) RestoreAcquire(tenantID string, at time.Time) {
	b := m.get(tenantID)
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// 首个重放事件建立时间基线（满桶、last=at），随后按事件间隔填充。
	b.refill(at)
	b.tokens--
	if b.tokens < 0 {
		b.tokens = 0
	}
	b.inUse++
}

func (m *Manager) RestoreRelease(tenantID string, at time.Time, refundRate bool) {
	b := m.get(tenantID)
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if at.After(b.last) {
		b.last = at
	}
	if b.inUse > 0 {
		b.inUse--
	}
	if refundRate {
		b.refill(at)
		b.tokens++
		if b.tokens > b.limits.RatePerSecond {
			b.tokens = b.limits.RatePerSecond
		}
	} else if !b.last.IsZero() && at.After(b.last) {
		b.last = at
	}
}
