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
