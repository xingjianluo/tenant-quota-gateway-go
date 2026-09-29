// Package gateway 组合鉴权与配额，提供请求准入判定、幂等去重与审计流水。
package gateway

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// Code 是可区分的判定结果码。
type Code string

const (
	CodeAllowed            Code = "ALLOWED"
	CodeExecuted           Code = "EXECUTED"
	CodeExecFailed         Code = "EXEC_FAILED"
	CodeTokenUnknown       Code = "REJECT_TOKEN_UNKNOWN"
	CodeTokenExpired       Code = "REJECT_TOKEN_EXPIRED"
	CodeTokenRevoked       Code = "REJECT_TOKEN_REVOKED"
	CodeScopeDenied        Code = "REJECT_SCOPE_DENIED"
	CodeRateLimited        Code = "REJECT_RATE_LIMITED"
	CodeConcurrencyLimited Code = "REJECT_CONCURRENCY_LIMITED"
	CodeQuotaExhausted     Code = "REJECT_QUOTA_EXHAUSTED"
	CodeDuplicate          Code = "DUPLICATE"
)

// Request 是一次准入请求输入。
type Request struct {
	RequestID string // 幂等键：重复提交返回首次判定结果
	TokenID   string
	Scope     string
}

// Decision 是判定结论。
type Decision struct {
	RequestID string
	TenantID  string
	Code      Code
	Reason    string
	Usage     quota.Usage
}

// AuditRecord 是一条审计流水（首次判定与重复判定都会各记一条，序号连续）。
type AuditRecord struct {
	Seq       int64
	At        time.Time
	Kind      persist.EventKind // EventCommit：首次判定；EventDuplicate：重复提交
	RequestID string
	TokenID   string
	TenantID  string
	Scope     string
	Code      Code
	Reason    string
	Usage     quota.Usage
}

// Handler 模拟真实下游调用；返回 error 表示执行失败（将归还配额）。
type Handler func(ctx context.Context, req Request) error

// inFlightCall 用于幂等去重：同一 RequestID 的并发提交共享首次判定的结果。
type inFlightCall struct {
	done chan struct{}
	dec  Decision
}

// Gateway 是准入网关。
type Gateway struct {
	auth    *auth.Authenticator
	limiter *quota.Limiter
	clock   clock.Clock
	handler Handler

	// Logger 输出审计日志；为 nil 时丢弃日志。
	Logger *log.Logger

	// store 可选；非 nil 时判定结论在返回前持久化（提交日志），并周期性打检查点。
	store persist.Store
	// auditRetain 是检查点中保留的审计流水尾部条数；恢复后只可查询该窗口内流水。
	auditRetain int
	// checkpointEvery 是每隔多少条提交事件打一次检查点（保证重放长度有界、启动开销近似常量）。
	checkpointEvery int
	// eventsSinceSnapshot 是距上次检查点以来追加的事件数。
	eventsSinceSnapshot int

	mu         sync.Mutex
	seq        int64
	inFlight   map[string]*inFlightCall       // 正在执行的 RequestID
	seen       map[string]Decision            // 幂等：RequestID -> 首次判定
	lastCommit map[string]persist.CommitEvent // RequestID -> 最近一次首次提交事件（检查点用）
	audit      []AuditRecord
}

// New 创建网关。
func New(a *auth.Authenticator, l *quota.Limiter, c clock.Clock, h Handler) *Gateway {
	return &Gateway{
		auth: a, limiter: l, clock: c, handler: h,
		Logger:     log.Default(),
		inFlight:   make(map[string]*inFlightCall),
		seen:       make(map[string]Decision),
		lastCommit: make(map[string]persist.CommitEvent),
	}
}

// 默认持久化参数。
const (
	defaultAuditRetain     = 4096
	defaultCheckpointEvery = 256
)

// AttachStore 接入持久化存储并立即恢复状态（必须在处理任何请求前调用）。
// 恢复内容：各租户计数器、幂等结论表、保留窗口内审计流水与序号水位；
// 崩溃时仍在途的请求会释放并发槽并归还预扣配额（其结论视为未提交，可安全重试）。
// auditRetain<=0 使用默认值；checkpointEvery<=0 使用默认值。
func (g *Gateway) AttachStore(store persist.Store, auditRetain, checkpointEvery int) error {
	g.store = store
	if auditRetain <= 0 {
		auditRetain = defaultAuditRetain
	}
	g.auditRetain = auditRetain
	if checkpointEvery <= 0 {
		checkpointEvery = defaultCheckpointEvery
	}
	g.checkpointEvery = checkpointEvery
	return g.restore()
}

// admitOnce 执行一次完整判定（不含幂等去重）。所有结论（含鉴权失败、配额拒绝）
// 都是已处理结论：都会进幂等表并持久化，因此重复提交（含跨重启）一律返回 DUPLICATE。
func (g *Gateway) admitOnce(ctx context.Context, req Request) (Decision, *quota.SavedState) {
	dec := Decision{RequestID: req.RequestID}

	// 1. 鉴权：越权、过期、撤销/未知令牌直接拒绝，不触碰任何配额。
	id, err := g.auth.Authorize(req.TokenID, req.Scope)
	if err != nil {
		dec.Code = authCode(err)
		dec.Reason = err.Error()
		return dec, nil
	}
	dec.TenantID = id.TenantID

	// 2. 配额：判定与记账原子完成，拒绝时不产生副作用；结论同样落盘。
	if err := g.limiter.Acquire(id.TenantID); err != nil {
		dec.Code = quotaCode(err)
		dec.Reason = err.Error()
		u, st := g.limiter.Peek(id.TenantID)
		dec.Usage = u
		return dec, &st
	}

	// 3. 执行下游调用；锁内原子结算（释放槽位，失败归还配额），
	// 返回的 Usage 与计数器状态严格一致，直接用于审计与持久化。
	execOK := g.handler(ctx, req) == nil
	u, st := g.limiter.Finish(id.TenantID, execOK)
	dec.Usage = u
	if execOK {
		dec.Code = CodeExecuted
		dec.Reason = "ok"
	} else {
		dec.Code = CodeExecFailed
		dec.Reason = "downstream execution failed"
	}
	return dec, &st
}

func authCode(err error) Code {
	switch {
	case errors.Is(err, auth.ErrTokenUnknown):
		return CodeTokenUnknown
	case errors.Is(err, auth.ErrTokenExpired):
		return CodeTokenExpired
	case errors.Is(err, auth.ErrTokenRevoked):
		return CodeTokenRevoked
	case errors.Is(err, auth.ErrScopeDenied):
		return CodeScopeDenied
	default:
		return CodeTokenUnknown
	}
}

func quotaCode(err error) Code {
	switch {
	case errors.Is(err, quota.ErrRateLimited):
		return CodeRateLimited
	case errors.Is(err, quota.ErrConcurrencyLimited):
		return CodeConcurrencyLimited
	case errors.Is(err, quota.ErrQuotaExhausted):
		return CodeQuotaExhausted
	default:
		return CodeRateLimited
	}
}

// Admit 对一次请求做鉴权、配额判定、执行与记账，并写入审计流水。
// 相同 RequestID 的重复提交（包括并发提交）返回首次判定结果，不重复扣减配额；
// 重复判定本身也追加一条 DUPLICATE 审计记录（序号连续，含请求输入与原判定依据）。
// 接入持久化后：结论先落盘再返回，因此进程在任意时刻被强制终止，重启后
// 要么看不到该请求（未提交，可安全重试），要么看到与首次完全一致的结论。
func (g *Gateway) Admit(ctx context.Context, req Request) Decision {
	// 幂等去重：已完成的直接返回首次结果；进行中的等待其完成。
	g.mu.Lock()
	if d, ok := g.seen[req.RequestID]; ok {
		dup := g.duplicateLocked(req, d)
		g.mu.Unlock()
		return dup
	}
	if c, ok := g.inFlight[req.RequestID]; ok {
		g.mu.Unlock()
		<-c.done
		g.mu.Lock()
		dup := g.duplicateLocked(req, c.dec)
		g.mu.Unlock()
		return dup
	}
	c := &inFlightCall{done: make(chan struct{})}
	g.inFlight[req.RequestID] = c
	g.mu.Unlock()

	dec, state := g.admitOnce(ctx, req)

	g.mu.Lock()
	c.dec = dec
	close(c.done)
	delete(g.inFlight, req.RequestID)
	g.seen[req.RequestID] = dec
	g.recordLocked(req, dec, persist.EventCommit, state)
	g.mu.Unlock()
	return dec
}

// duplicateLocked 在幂等命中时构造 DUPLICATE 结论并追加审计、持久化。
// 返回结论与首次判定一致（仅 Code/Reason 标记为重复），Usage 取首次结论快照。
// 调用方必须持有 g.mu。
func (g *Gateway) duplicateLocked(req Request, first Decision) Decision {
	dup := first
	dup.Code = CodeDuplicate
	dup.Reason = "duplicate of " + string(first.Code)
	g.recordLocked(req, dup, persist.EventDuplicate, nil)
	return dup
}

// recordLocked 追加审计流水、输出日志，并在接入持久化时先落盘提交事件；
// 每 checkpointEvery 条事件打一次检查点（快照 + 截断日志）。调用方必须持有 g.mu。
//
// 持久化失败时不改变判定结论（请求已实际执行），仅记录日志；下一次提交或
// 检查点会重试。返回给调用方的结论始终以内存状态为准。
func (g *Gateway) recordLocked(req Request, dec Decision, kind persist.EventKind, state *quota.SavedState) {
	g.seq++
	rec := AuditRecord{
		Seq:       g.seq,
		At:        g.clock.Now(),
		Kind:      kind,
		RequestID: req.RequestID,
		TokenID:   req.TokenID,
		TenantID:  dec.TenantID,
		Scope:     req.Scope,
		Code:      dec.Code,
		Reason:    dec.Reason,
		Usage:     dec.Usage,
	}
	g.audit = append(g.audit, rec)
	if g.Logger != nil {
		g.Logger.Printf("audit seq=%d req=%s token=%s scope=%s tenant=%s code=%s reason=%q usage={tokens:%.2f inflight:%d used:%d remain:%d}",
			rec.Seq, req.RequestID, req.TokenID, req.Scope, rec.TenantID, rec.Code, rec.Reason,
			rec.Usage.Tokens, rec.Usage.InFlight, rec.Usage.QuotaUsed, rec.Usage.QuotaRemain)
	}
	if g.store == nil {
		return
	}
	ev := persist.CommitEvent{
		Kind: kind, Seq: rec.Seq, At: rec.At,
		RequestID: rec.RequestID, TokenID: rec.TokenID, TenantID: rec.TenantID,
		Scope: rec.Scope, Code: string(rec.Code), Reason: rec.Reason,
		Usage: toAuditDTO(rec.Usage),
	}
	if kind == persist.EventCommit {
		if state != nil {
			ev.State = &persist.TenantState{Tokens: state.Tokens, LastFill: state.LastFill, InFlight: state.InFlight, Used: state.Used}
		}
		g.lastCommit[req.RequestID] = ev
	}
	if err := g.store.AppendEvent(ev); err != nil {
		if g.Logger != nil {
			g.Logger.Printf("persist append failed (will retry on next commit): %v", err)
		}
		return
	}
	g.eventsSinceSnapshot++
	if g.eventsSinceSnapshot >= g.checkpointEvery {
		g.checkpointLocked()
	}
}

// checkpointLocked 在 g.mu 保护下生成一致性快照落盘并截断提交日志。
// 调用方必须持有 g.mu（期间不会有任何判定穿插，快照天然一致）。
func (g *Gateway) checkpointLocked() {
	tail := g.audit
	if len(tail) > g.auditRetain {
		tail = tail[len(tail)-g.auditRetain:]
	}
	events := make([]persist.CommitEvent, len(tail))
	for i, r := range tail {
		events[i] = auditToEvent(r)
	}
	seen := make(map[string]persist.CommitEvent, len(g.seen))
	// 幂等表记录最近一次首次判定事件（含请求输入 TokenID/Scope 与计数器状态），
	// 保证重启后重复提交仍能返回一致结论，且计数器重放有完整落点。
	for reqID, e := range g.lastCommit {
		seen[reqID] = e
	}
	for id, d := range g.seen {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = persist.CommitEvent{
			Kind: persist.EventCommit,
			// 兜底：理论上每个 seen 都应有对应事件；保留结论与配额即可。
			RequestID: d.RequestID, TenantID: d.TenantID,
			Code: string(d.Code), Reason: d.Reason, Usage: toAuditDTO(d.Usage),
		}
	}
	tenants := g.limiter.SnapshotAll()
	states := make(map[string]persist.TenantState, len(tenants))
	for id, st := range tenants {
		states[id] = persist.TenantState{Tokens: st.Tokens, LastFill: st.LastFill, InFlight: st.InFlight, Used: st.Used}
	}
	snap := &persist.Snapshot{
		TakenAt:       g.clock.Now(),
		LastSeq:       g.seq,
		Tenants:       states,
		Seen:          seen,
		AuditTail:     events,
		AuditRetained: g.auditRetain,
	}
	if err := g.store.SaveSnapshot(snap); err != nil {
		if g.Logger != nil {
			g.Logger.Printf("persist snapshot failed (journal retained): %v", err)
		}
		return
	}
	if err := g.store.TruncateEvents(); err != nil {
		if g.Logger != nil {
			g.Logger.Printf("persist truncate failed (snapshot kept, replay may redo): %v", err)
		}
		return
	}
	g.eventsSinceSnapshot = 0
}

func toAuditDTO(u quota.Usage) persist.TenantAudit {
	return persist.TenantAudit{Tokens: u.Tokens, InFlight: u.InFlight, QuotaUsed: u.QuotaUsed, QuotaRemain: u.QuotaRemain}
}

func fromAuditDTO(u persist.TenantAudit) quota.Usage {
	return quota.Usage{Tokens: u.Tokens, InFlight: u.InFlight, QuotaUsed: u.QuotaUsed, QuotaRemain: u.QuotaRemain}
}

func auditToEvent(r AuditRecord) persist.CommitEvent {
	return persist.CommitEvent{
		Kind:      r.Kind,
		Seq:       r.Seq,
		At:        r.At,
		RequestID: r.RequestID,
		TokenID:   r.TokenID,
		TenantID:  r.TenantID,
		Scope:     r.Scope,
		Code:      string(r.Code),
		Reason:    r.Reason,
		Usage:     toAuditDTO(r.Usage),
	}
}

func eventToAudit(e persist.CommitEvent) AuditRecord {
	return AuditRecord{
		Seq:       e.Seq,
		At:        e.At,
		Kind:      e.Kind,
		RequestID: e.RequestID,
		TokenID:   e.TokenID,
		TenantID:  e.TenantID,
		Scope:     e.Scope,
		Code:      Code(e.Code),
		Reason:    e.Reason,
		Usage:     fromAuditDTO(e.Usage),
	}
}

// Audit 返回审计流水快照。
func (g *Gateway) Audit() []AuditRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]AuditRecord, len(g.audit))
	copy(out, g.audit)
	return out
}
