// 重启恢复：从快照 + 提交日志重建配额计数器、幂等结论表与审计流水。
package gateway

import (
	"sort"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// restore 在 AttachStore 时执行一次：
//  1. 载入快照（租户计数器、幂等表、审计尾部、序号水位）；
//  2. 按顺序重放快照之后的提交事件：commit 更新计数器与幂等表，duplicate 仅作记录；
//  3. 用最终状态对崩溃时在途的请求做结算（释放槽位、归还预扣配额）。
//
// 首次启动（无快照、无日志）时得到与新建内存网关一致的空状态。
func (g *Gateway) restore() error {
	if g.lastCommit == nil {
		g.lastCommit = make(map[string]persist.CommitEvent)
	}
	snap, err := g.store.LoadSnapshot()
	if err != nil && err != persist.ErrNoSnapshot {
		return err
	}

	events, err := g.store.ReadEvents()
	if err != nil {
		return err
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })

	if snap != nil {
		// 只先恢复元数据（幂等表、审计尾部、序号水位）；租户计数器延后到
		// 与日志最终状态合并后一次性装载，避免被日志里的历史时刻状态中途覆盖。
		g.applySnapshotMeta(snap)
	}

	// 只重放快照水位之后的事件。快照保存成功、日志截断前崩溃时，日志里会残留
	// 已包含在快照中的旧事件；这些事件的序号 <= LastSeq，跳过以避免状态回退与审计重复。
	// 计数器不能边重放边覆盖：事件携带的是判定时刻的历史状态（含旧 lastFill），
	// 真实时钟在重启间隔已经流逝，中途覆盖会被后续事件的旧状态打回。
	// 因此先收集每个租户的"最后状态"，重放完一次性应用。
	waterline := g.seq
	finalStates := map[string]quota.SavedState{}
	replayed := 0
	for _, e := range events {
		if e.Seq <= waterline {
			continue
		}
		replayed++
		if e.Kind == persist.EventCommit {
			// 鉴权失败等无租户状态的结论只进幂等表；其余结论记录该租户的计数器落点。
			if e.State != nil {
				finalStates[e.TenantID] = quota.SavedState{
					Tokens: e.State.Tokens, LastFill: e.State.LastFill,
					InFlight: e.State.InFlight, Used: e.State.Used,
				}
			}
			g.lastCommit[e.RequestID] = e
			g.seen[e.RequestID] = eventToDecision(e)
		}
		g.audit = append(g.audit, eventToAudit(e))
		g.seq = e.Seq
	}
	// 快照计数器先装载；日志中存在更新落点的租户再覆盖。
	if snap != nil {
		snapStates := make(map[string]quota.SavedState, len(snap.Tenants))
		for id, st := range snap.Tenants {
			snapStates[id] = quota.SavedState{Tokens: st.Tokens, LastFill: st.LastFill, InFlight: st.InFlight, Used: st.Used}
		}
		g.limiter.RestoreAll(snapStates)
	}
	for tenant, st := range finalStates {
		g.limiter.RestoreTenant(tenant, st)
	}

	// 崩溃在途结算：恢复出的最终状态里仍标记 InFlight>0 的槽位，都属于
	// "崩溃瞬间还在执行、结论未提交"的请求（已提交的结论会把自己的最终状态
	// 写进日志/快照并覆盖旧值）。无论该状态来自快照（检查点时刻有请求在途，
	// 之后没来得及再提交就崩溃）还是日志最后一条提交事件，统一按"未发生"
	// 回滚：释放并发槽、归还预扣配额与速率令牌。
	// 结算只作用于本次从磁盘重建出来的状态，不落盘；即使刚恢复又崩溃，
	// 下次重启仍从同一份磁盘状态重做一次同样的结算，结果一致、不会重复退款。
	for tenant, st := range g.limiter.SnapshotAll() {
		if st.InFlight > 0 {
			g.limiter.SettleInFlight(tenant, st.InFlight)
		}
	}
	if replayed > 0 {
		g.eventsSinceSnapshot = replayed
	}
	return nil
}

// applySnapshotMeta 把快照中的元数据装载进内存（幂等表、审计尾部、序号水位），
// 不含租户计数器——计数器由 restore 与日志最终状态合并后统一装载。
func (g *Gateway) applySnapshotMeta(snap *persist.Snapshot) {
	g.seq = snap.LastSeq
	g.lastCommit = make(map[string]persist.CommitEvent, len(snap.Seen))
	g.audit = g.audit[:0]
	for _, e := range snap.AuditTail {
		g.audit = append(g.audit, eventToAudit(e))
	}
	g.seen = make(map[string]Decision, len(snap.Seen))
	for id, e := range snap.Seen {
		g.lastCommit[id] = e
		g.seen[id] = eventToDecision(e)
	}
}

func eventToDecision(e persist.CommitEvent) Decision {
	return Decision{
		RequestID: e.RequestID,
		TenantID:  e.TenantID,
		Code:      Code(e.Code),
		Reason:    e.Reason,
		Usage:     fromAuditDTO(e.Usage),
	}
}
