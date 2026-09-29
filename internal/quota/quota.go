// Package quota 实现按租户隔离的速率、并发与总量配额控制。
package quota

import (
	"errors"
	"sync"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
)

// 配额拒绝原因，可按类型区分。
var (
	ErrRateLimited        = errors.New("rate limited")
	ErrConcurrencyLimited = errors.New("concurrency limited")
	ErrQuotaExhausted     = errors.New("quota exhausted")
)

// Limit 描述单个租户的配额模型。
type Limit struct {
	// RatePerSec 令牌桶每秒补充速率。
	RatePerSec float64
	// Burst 令牌桶容量。
	Burst int
	// MaxConcurrent 最大并发执行槽。
	MaxConcurrent int
	// TotalQuota 允许成功执行的总请求数配额。
	TotalQuota int64
}

// Usage 是租户当前配额使用快照。
type Usage struct {
	Tokens      float64
	InFlight    int
	QuotaUsed   int64
	QuotaRemain int64
}

// Limiter 按租户隔离的配额管理器，判定与记账原子完成。
type Limiter struct {
	mu     sync.Mutex
	clock  clock.Clock
	limits map[string]Limit
	states map[string]*tenantState
}

type tenantState struct {
	tokens   float64
	lastFill time.Time
	inFlight int
	used     int64
}

// NewLimiter 创建配额管理器。
func NewLimiter(c clock.Clock) *Limiter {
	return &Limiter{clock: c, limits: make(map[string]Limit), states: make(map[string]*tenantState)}
}

// SetLimit 配置租户配额，不同租户互不影响。
func (l *Limiter) SetLimit(tenantID string, lim Limit) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limits[tenantID] = lim
}

// state 返回租户状态，不存在则按当前时间初始化（惰性创建，租户间完全隔离）。
// 调用方必须持有 l.mu。
func (l *Limiter) state(tenantID string, lim Limit, now time.Time) *tenantState {
	st, ok := l.states[tenantID]
	if !ok {
		st = &tenantState{tokens: float64(lim.Burst), lastFill: now}
		l.states[tenantID] = st
	}
	return st
}

// refill 按经过时间补充令牌桶，不超过 Burst。
// 调用方必须持有 l.mu。
func (l *Limiter) refill(st *tenantState, lim Limit, now time.Time) {
	elapsed := now.Sub(st.lastFill).Seconds()
	if elapsed > 0 {
		st.tokens += elapsed * lim.RatePerSec
		if st.tokens > float64(lim.Burst) {
			st.tokens = float64(lim.Burst)
		}
		st.lastFill = now
	}
}

// Acquire 原子地判定并为一次请求记账：消耗一个令牌、占用一个并发槽、预扣一份总量配额。
// 任一条件不满足即拒绝，且不产生任何记账副作用。
func (l *Limiter) Acquire(tenantID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.limits[tenantID]
	if !ok {
		lim = Limit{}
	}
	now := l.clock.Now()
	st := l.state(tenantID, lim, now)
	l.refill(st, lim, now)

	if st.tokens < 1 {
		return ErrRateLimited
	}
	if lim.MaxConcurrent > 0 && st.inFlight >= lim.MaxConcurrent {
		return ErrConcurrencyLimited
	}
	if lim.TotalQuota > 0 && st.used >= lim.TotalQuota {
		return ErrQuotaExhausted
	}
	st.tokens--
	st.inFlight++
	st.used++
	return nil
}

// Refund 在请求执行失败时归还预扣的总量配额与速率令牌，避免失败请求泄漏配额。
func (l *Limiter) Refund(tenantID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim := l.limits[tenantID]
	st, ok := l.states[tenantID]
	if !ok {
		return
	}
	if st.used > 0 {
		st.used--
	}
	if st.tokens < float64(lim.Burst) {
		st.tokens++
	}
}

// Release 释放一个并发槽，无论请求成功或失败都必须调用。
func (l *Limiter) Release(tenantID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.states[tenantID]
	if !ok {
		return
	}
	if st.inFlight > 0 {
		st.inFlight--
	}
}

// Usage 返回租户当前配额快照。
func (l *Limiter) Usage(tenantID string) Usage {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim := l.limits[tenantID]
	now := l.clock.Now()
	st := l.state(tenantID, lim, now)
	l.refill(st, lim, now)
	remain := int64(0)
	if lim.TotalQuota > 0 {
		remain = lim.TotalQuota - st.used
		if remain < 0 {
			remain = 0
		}
	}
	return Usage{
		Tokens:      st.tokens,
		InFlight:    st.inFlight,
		QuotaUsed:   st.used,
		QuotaRemain: remain,
	}
}

// SavedState 是单个租户计数器的可持久化形态（由持久化层负责序列化）。
type SavedState struct {
	Tokens   float64
	LastFill time.Time
	InFlight int
	Used     int64
}

// SnapshotAll 导出当前所有已惰性创建的租户计数器副本，用于写入检查点。
func (l *Limiter) SnapshotAll() map[string]SavedState {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]SavedState, len(l.states))
	for id, st := range l.states {
		out[id] = SavedState{Tokens: st.tokens, LastFill: st.lastFill, InFlight: st.inFlight, Used: st.used}
	}
	return out
}

// RestoreAll 用检查点状态整体替换各租户计数器。
// 必须在配置好全部 Limit 之后、且在处理任何请求之前调用。
func (l *Limiter) RestoreAll(states map[string]SavedState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.states = make(map[string]*tenantState, len(states))
	for id, s := range states {
		l.states[id] = &tenantState{tokens: s.Tokens, lastFill: s.LastFill, inFlight: s.InFlight, used: s.Used}
	}
}

// SettleInFlight 恢复时把崩溃前在途请求占用的并发槽释放，并按实际在途数归还预扣配额。
// 每个崩溃在途请求：占槽 1 个、配额 +1（已提交的成功结论对应的扣减不受影响）。
func (l *Limiter) SettleInFlight(tenantID string, inflight int) {
	if inflight <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.limits[tenantID]
	if !ok {
		lim = Limit{}
	}
	st, ok := l.states[tenantID]
	if !ok {
		return
	}
	if inflight > st.inFlight {
		inflight = st.inFlight
	}
	st.inFlight -= inflight
	st.used -= int64(inflight)
	if st.used < 0 {
		st.used = 0
	}
	for i := 0; i < inflight; i++ {
		if st.tokens < float64(lim.Burst) {
			st.tokens++
		}
	}
}

// Finish 汇总一次请求执行结束后的结算动作，在单把锁内原子完成并返回结算后状态：
// execOK=true  -> 仅释放并发槽（预扣配额保留为已成功用量）；
// execOK=false -> 释放并发槽并归还预扣配额与速率令牌。
// 与分开调用 Release/Refund/Usage 相比，返回的状态不会被并发请求穿插改写，
// 因此持久化的"最终配额"与内存状态严格一致。
func (l *Limiter) Finish(tenantID string, execOK bool) (Usage, SavedState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.limits[tenantID]
	if !ok {
		lim = Limit{}
	}
	st, exists := l.states[tenantID]
	if !exists {
		st = l.state(tenantID, lim, l.clock.Now())
	}
	if st.inFlight > 0 {
		st.inFlight--
	}
	if !execOK {
		if st.used > 0 {
			st.used--
		}
		if st.tokens < float64(lim.Burst) {
			st.tokens++
		}
	}
	remain := int64(0)
	if lim.TotalQuota > 0 {
		remain = lim.TotalQuota - st.used
		if remain < 0 {
			remain = 0
		}
	}
	u := Usage{Tokens: st.tokens, InFlight: st.inFlight, QuotaUsed: st.used, QuotaRemain: remain}
	saved := SavedState{Tokens: st.tokens, LastFill: st.lastFill, InFlight: st.inFlight, Used: st.used}
	return u, saved
}

// Peek 在单把锁内返回当前 Usage 快照与计数器副本（拒绝路径持久化用，
// 保证事件携带的 Usage 与计数器状态严格一致、不被并发穿插）。
func (l *Limiter) Peek(tenantID string) (Usage, SavedState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.limits[tenantID]
	if !ok {
		lim = Limit{}
	}
	now := l.clock.Now()
	st := l.state(tenantID, lim, now)
	l.refill(st, lim, now)
	remain := int64(0)
	if lim.TotalQuota > 0 {
		remain = lim.TotalQuota - st.used
		if remain < 0 {
			remain = 0
		}
	}
	return Usage{Tokens: st.tokens, InFlight: st.inFlight, QuotaUsed: st.used, QuotaRemain: remain},
		SavedState{Tokens: st.tokens, LastFill: st.lastFill, InFlight: st.inFlight, Used: st.used}
}

// RestoreTenant 用单个租户的计数器快照替换其状态（重放提交事件时逐事件推进用）。
func (l *Limiter) RestoreTenant(tenantID string, s SavedState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.states[tenantID] = &tenantState{tokens: s.Tokens, lastFill: s.LastFill, inFlight: s.InFlight, used: s.Used}
}
