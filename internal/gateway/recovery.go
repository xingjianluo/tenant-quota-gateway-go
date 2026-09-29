package gateway

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// snapshotData 是运行态周期快照的磁盘格式。
type snapshotData struct {
	Seq      int64                   `json:"seq"`
	States   map[string]quota.State  `json:"states"`
	Seen     map[string]snapshotSeen `json:"seen"`
	InFlight map[string]snapshotRes  `json:"in_flight"`
}

type snapshotSeen struct {
	TenantID    string  `json:"tenant_id"`
	Code        Code    `json:"code"`
	Reason      string  `json:"reason"`
	Tokens      float64 `json:"tokens"`
	InFlight    int     `json:"in_flight"`
	QuotaUsed   int64   `json:"quota_used"`
	QuotaRemain int64   `json:"quota_remain"`
}

type snapshotRes struct {
	TokenID  string `json:"token_id"`
	Scope    string `json:"scope"`
	TenantID string `json:"tenant_id"`
}

// RolledBack 描述一条在崩溃时停留在"已记账、未终局"阶段、
// 重启后被判定回滚的请求：速率令牌与总量配额已归还、并发槽已释放。
type RolledBack struct {
	RequestID string
	TenantID  string
}

// RecoveryReport 是一次启动恢复的结论。
type RecoveryReport struct {
	SnapshotSeq  int64 // 快照已覆盖到的最大序号
	Replayed     int   // 从 journal 重放的终局事件数
	RolledBack   []RolledBack
	AuditPatched int
}

// defaultSnapshotEvery 每 N 条终局判定做一次快照（压缩 journal，钳制重放窗口）。
const defaultSnapshotEvery = 128

// restore 从持久化存储恢复运行态：装快照 → 按事件时间重放 journal →
// 回滚未终局预留 → 补齐审计缺尾。
func (g *Gateway) restore(ctx context.Context, store *persist.Store) (*RecoveryReport, error) {
	_ = ctx
	report := &RecoveryReport{}

	// 1. 装快照。
	var snap snapshotData
	if raw, ok, err := store.ReadSnapshot(); err != nil {
		return nil, err
	} else if ok {
		snap, err = decodeSnapshot(raw)
		if err != nil {
			return nil, err
		}
		g.limiter.ImportStates(snap.States)
		g.seq = snap.Seq
		for id, s := range snap.Seen {
			g.seen[id] = Decision{
				RequestID: id,
				TenantID:  s.TenantID,
				Code:      s.Code,
				Reason:    s.Reason,
				Usage:     quota.Usage{Tokens: s.Tokens, InFlight: s.InFlight, QuotaUsed: s.QuotaUsed, QuotaRemain: s.QuotaRemain},
			}
		}
	}
	report.SnapshotSeq = snap.Seq

	// 2. 重放快照之后的 journal 窗口。
	entries, err := store.ReadJournal()
	if err != nil {
		return nil, err
	}
	decided := map[string]bool{}
	for _, e := range entries {
		if e.Kind == persist.KindDecision {
			decided[e.RequestID] = true
			if e.Seq <= snap.Seq {
				continue // 快照已覆盖
			}
			// 在快照账本之上增量重放终局：只有真正发生过 Acquire 的请求需要记账——
			// EXECUTED 净扣一次，EXEC_FAILED 净额为 0；鉴权/配额拒绝与重复事件不记账。
			if e.Code != string(CodeDuplicate) {
				if e.Code == string(CodeExecuted) || e.Code == string(CodeExecFailed) {
					if e.TenantID != "" {
						start := e.ReservedAt
						if start.IsZero() {
							start = e.At // 兼容旧格式（无 ReservedAt）
						}
						if err := g.limiter.ReplayCommitted(e.TenantID, start, e.At, e.Code == string(CodeExecuted)); err != nil {
							if g.Logger != nil {
								g.Logger.Printf("recovery: 重放记账失败 req=%s: %v（当前配额配置可能小于历史）", e.RequestID, err)
							}
						}
					}
				}
				g.seen[e.RequestID] = decisionFromEntry(e)
			}
			if e.Seq > g.seq {
				g.seq = e.Seq
			}
			report.Replayed++
		}
	}

	// 3. 找出崩溃在"记账之后、终局之前"的未决预留，并区分两类：
	//    a) 快照内的在飞预留：其记账已包含在快照账本里（inFlight+1、used+1、tokens-1），
	//       必须 Release+Refund 真正回滚；
	//    b) 仅出现在 journal 窗口（快照之后）的未决预留：快照之后的账本是用终局事件的
	//       终态重建的，未决预留的 Acquire 从未进入任何终态，故账本本来就不包含它，
	//       无需再动账本（再 Refund 反而会多还）。
	type openRes struct {
		r      snapshotRes
		inSnap bool
	}
	open := map[string]openRes{}
	for id, r := range snap.InFlight {
		open[id] = openRes{r: r, inSnap: true}
	}
	for _, e := range entries {
		switch e.Kind {
		case persist.KindReserved:
			if _, ok := open[e.RequestID]; !ok {
				open[e.RequestID] = openRes{
					r:      snapshotRes{TokenID: e.TokenID, Scope: e.Scope, TenantID: e.TenantID},
					inSnap: false,
				}
			}
		case persist.KindDecision:
			delete(open, e.RequestID)
		}
	}
	for id, o := range open {
		if o.inSnap {
			g.limiter.Release(o.r.TenantID)
			g.limiter.Refund(o.r.TenantID)
		}
		report.RolledBack = append(report.RolledBack, RolledBack{RequestID: id, TenantID: o.r.TenantID})
		if g.Logger != nil {
			where := "journal 窗口"
			if o.inSnap {
				where = "快照账本"
			}
			g.Logger.Printf("recovery: 回滚中断请求 req=%s tenant=%s（来源:%s；并发槽已释放，令牌与配额口径已校正）", id, o.r.TenantID, where)
		}
	}
	sort.Slice(report.RolledBack, func(i, j int) bool { return report.RolledBack[i].RequestID < report.RolledBack[j].RequestID })

	// 4. 补审计缺尾：audit 是只追加文件，正常情况下 journal 每条终局都已同时落 audit；
	//    仅在"终局已落 journal、audit 写入前"这个极窄窗口崩溃时需要补齐，保证序号连续。
	lastAuditSeq, err := store.LastAuditSeq()
	if err != nil {
		return nil, err
	}
	// 快照点之前的缺尾也要补（快照会先修 audit 再截 journal，因此正常不会发生）。
	missing := []persist.JournalEntry{}
	if lastAuditSeq < snap.Seq {
		// 旧 journal 已截断、无法逐行恢复；这一步只可能在外部删除 audit.log 时发生。
		if g.Logger != nil {
			g.Logger.Printf("recovery: audit 落后于快照点 last=%d snapshot=%d（audit.log 可能被外部删除）", lastAuditSeq, snap.Seq)
		}
	}
	for _, e := range entries {
		if e.Kind == persist.KindDecision && e.Seq > lastAuditSeq {
			missing = append(missing, e)
		}
	}
	for _, e := range missing {
		if err := store.AppendAudit(e.AsDecisionEntry()); err != nil {
			return nil, err
		}
		report.AuditPatched++
	}

	return report, nil
}

// saveSnapshotLocked 在调用方持有 g.mu 的情况下，于"内存态与 journal 一致的瞬间"
// 导出快照、原子写盘、补齐审计缺尾并截断 journal。持锁保证截断不会与任何
// reserved/decision 追加交错。
func (g *Gateway) saveSnapshotLocked() error {
	if g.store == nil {
		return nil
	}
	snap := snapshotData{
		Seq:      g.seq,
		States:   g.limiter.ExportStates(),
		Seen:     make(map[string]snapshotSeen, len(g.seen)),
		InFlight: make(map[string]snapshotRes, len(g.inFlight)),
	}
	for id, d := range g.seen {
		snap.Seen[id] = snapshotSeen{
			TenantID: d.TenantID, Code: d.Code, Reason: d.Reason,
			Tokens: d.Usage.Tokens, InFlight: d.Usage.InFlight,
			QuotaUsed: d.Usage.QuotaUsed, QuotaRemain: d.Usage.QuotaRemain,
		}
	}
	// 正在执行且已记账的预留也进快照：快照后 journal 被截断，这些预留仍须在崩溃回滚集合里。
	for id, c := range g.inFlight {
		if c.tenantID != "" {
			snap.InFlight[id] = snapshotRes{TokenID: c.req.TokenID, Scope: c.req.Scope, TenantID: c.tenantID}
		}
	}

	raw, err := encodeSnapshot(snap)
	if err != nil {
		return err
	}
	if err := g.store.WriteSnapshot(raw); err != nil {
		return err
	}
	// 快照已 fsync：确保审计不落后于快照点，然后才能截断 journal。
	lastAuditSeq, err := g.store.LastAuditSeq()
	if err != nil {
		return err
	}
	if lastAuditSeq < snap.Seq {
		entries, err := g.store.ReadJournal()
		if err != nil {
			return err
		}
		var missing []persist.JournalEntry
		for _, e := range entries {
			if e.Kind == persist.KindDecision && e.Seq > lastAuditSeq {
				missing = append(missing, e)
			}
		}
		for _, e := range missing {
			if err := g.store.AppendAudit(e.AsDecisionEntry()); err != nil {
				return err
			}
		}
	}
	return g.store.TruncateJournal()
}

// decodeSnapshot 解析快照字节。
func decodeSnapshot(raw []byte) (snapshotData, error) {
	var s snapshotData
	err := json.Unmarshal(raw, &s)
	return s, err
}

// encodeSnapshot 序列化快照。
func encodeSnapshot(s snapshotData) ([]byte, error) {
	return json.Marshal(s)
}

// decisionFromEntry 把持久化终局事件转回内存判定（用于重建幂等表）。
func decisionFromEntry(e persist.JournalEntry) Decision {
	return Decision{
		RequestID: e.RequestID,
		TenantID:  e.TenantID,
		Code:      Code(e.Code),
		Reason:    e.Reason,
		Usage: quota.Usage{
			Tokens: e.Tokens, InFlight: e.InFlight, QuotaUsed: e.QuotaUsed, QuotaRemain: e.QuotaRemain,
		},
	}
}

// auditFromEntry 把持久化事件转为审计记录。
func auditFromEntry(e persist.JournalEntry) AuditRecord {
	return AuditRecord{
		Seq:       e.Seq,
		At:        e.At,
		RequestID: e.RequestID,
		TenantID:  e.TenantID,
		Scope:     e.Scope,
		Code:      Code(e.Code),
		Reason:    e.Reason,
		Usage: quota.Usage{
			Tokens: e.Tokens, InFlight: e.InFlight, QuotaUsed: e.QuotaUsed, QuotaRemain: e.QuotaRemain,
		},
	}
}
