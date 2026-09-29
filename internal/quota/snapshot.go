package quota

import "time"

// State 是单个租户配额账本的可持久化快照。
type State struct {
	Tokens   float64
	LastFill time.Time
	InFlight int
	Used     int64
}

// ExportStates 快照所有租户的当前账本（会按当前时间结算令牌桶）。
func (l *Limiter) ExportStates() map[string]State {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	out := make(map[string]State, len(l.states))
	for id, st := range l.states {
		lim := l.limits[id]
		l.refill(st, lim, now)
		out[id] = State{Tokens: st.tokens, LastFill: st.lastFill, InFlight: st.inFlight, Used: st.used}
	}
	return out
}

// ImportStates 用快照账本替换内存状态（恢复时使用）。
func (l *Limiter) ImportStates(in map[string]State) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.states = make(map[string]*tenantState, len(in))
	for id, s := range in {
		l.states[id] = &tenantState{tokens: s.Tokens, lastFill: s.LastFill, inFlight: s.InFlight, used: s.Used}
	}
}

// ReplayCommitted 在快照账本之上增量重放一次已提交的终局请求。
// reservedAt 是 Acquire 记账时刻：先结算到该点并扣令牌/占槽/扣配额；
// succeeded=false 时立即按预留时刻退款（与实时路径"Release 后紧接 Refund"同序，
// 令牌不会先被时间补满再被 Burst 封顶丢弃）；成功时把桶结算到终局时刻，
// 并发槽在终局释放。这样恢复语义与实时路径严格一致。
func (l *Limiter) ReplayCommitted(tenantID string, reservedAt, decidedAt time.Time, succeeded bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.limits[tenantID]
	if !ok {
		lim = Limit{}
	}
	st, ok := l.states[tenantID]
	if !ok {
		st = &tenantState{tokens: float64(lim.Burst), lastFill: reservedAt}
		l.states[tenantID] = st
	}
	l.refill(st, lim, reservedAt)
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
	if !succeeded {
		// 先按预留时刻同序退款（避免补满后退还被 Burst 封顶），再把桶结算到终局时刻，
		// 使后续事件看到的令牌水平与"失败后时间继续流逝"的实时口径一致。
		st.inFlight--
		st.used--
		if st.tokens < float64(lim.Burst) {
			st.tokens++
		}
		l.refill(st, lim, decidedAt)
		return nil
	}
	// 成功：结算到终局时刻后释放并发槽（执行期间占用的槽不提前释放）。
	l.refill(st, lim, decidedAt)
	st.inFlight--
	return nil
}
