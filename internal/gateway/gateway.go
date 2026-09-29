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
	CodePersistFailed      Code = "PERSIST_FAILED"
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

// AuditRecord 是一条审计流水。
type AuditRecord struct {
	Seq       int64
	At        time.Time
	RequestID string
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
	done       chan struct{}
	dec        Decision
	req        Request
	tenantID   string // 已记账（Acquire 成功）时非空
	reservedAt time.Time
}

// Gateway 是准入网关。
type Gateway struct {
	auth    *auth.Authenticator
	limiter *quota.Limiter
	clock   clock.Clock
	handler Handler

	// Logger 输出审计日志；为 nil 时丢弃日志。
	Logger *log.Logger

	store         *persist.Store // nil = 纯内存模式
	snapshotEvery int

	mu       sync.Mutex
	seq      int64
	inFlight map[string]*inFlightCall // 正在执行的 RequestID
	seen     map[string]Decision      // 幂等：RequestID -> 首次判定
	audit    []AuditRecord
}

// New 创建纯内存网关（状态不跨进程保留）。
func New(a *auth.Authenticator, l *quota.Limiter, c clock.Clock, h Handler) *Gateway {
	return &Gateway{
		auth: a, limiter: l, clock: c, handler: h,
		Logger:        log.Default(),
		snapshotEvery: defaultSnapshotEvery,
		inFlight:      make(map[string]*inFlightCall),
		seen:          make(map[string]Decision),
	}
}

// Open 创建网关并从 dir 恢复运行态：配额账本、幂等结论、审计序号在重启后延续。
// dir 为空等价于纯内存 New。
func Open(ctx context.Context, dir string, a *auth.Authenticator, l *quota.Limiter, c clock.Clock, h Handler) (*Gateway, *RecoveryReport, error) {
	g := New(a, l, c, h)
	if dir == "" {
		return g, &RecoveryReport{}, nil
	}
	store, err := persist.Open(dir)
	if err != nil {
		return nil, nil, err
	}
	g.store = store
	report, err := g.restore(ctx, store)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	return g, report, nil
}

// Close 关闭持久化句柄。
func (g *Gateway) Close() error {
	if g.store != nil {
		return g.store.Close()
	}
	return nil
}

// reserveOutcome 描述在 g.mu 内完成的预留结果。
type reserveOutcome struct {
	dec        Decision // 终局性拒绝（鉴权/配额/持久化失败）时 Code 非空
	tenantID   string   // Acquire 成功时非空
	reservedAt time.Time
}

// reserveLocked 在 g.mu 内完成"鉴权 → 配额记账 → reserved 落盘 → 登记在飞"。
// 关键：Acquire 的内存记账与 reserved 的 fsync 在同一把网关锁内，
// 与 saveSnapshot（同样在该锁内导出账本并截断 journal）互斥，
// 因此任何被快照捕获的记账必然已有持久化预留，任何 journal 截断都不会丢掉仍需要的事件。
// 调用方持有 g.mu。
func (g *Gateway) reserveLocked(req Request) reserveOutcome {
	dec := Decision{RequestID: req.RequestID}
	id, err := g.auth.Authorize(req.TokenID, req.Scope)
	if err != nil {
		dec.Code = authCode(err)
		dec.Reason = err.Error()
		return reserveOutcome{dec: dec}
	}
	dec.TenantID = id.TenantID

	if err := g.limiter.Acquire(id.TenantID); err != nil {
		dec.Code = quotaCode(err)
		dec.Reason = err.Error()
		dec.Usage = g.limiter.Usage(id.TenantID)
		return reserveOutcome{dec: dec}
	}

	reservedAt := g.clock.Now()
	if g.store != nil {
		rerr := g.store.AppendReserved(persist.ReservedEntry{
			At: reservedAt, RequestID: req.RequestID, TokenID: req.TokenID,
			Scope: req.Scope, TenantID: id.TenantID,
		})
		if rerr != nil {
			// 无法保证崩溃可恢复：撤回本次记账，拒绝请求，绝不带着未落盘的记账继续。
			g.limiter.Release(id.TenantID)
			g.limiter.Refund(id.TenantID)
			dec.Code = CodePersistFailed
			dec.Reason = "persist reservation: " + rerr.Error()
			dec.Usage = g.limiter.Usage(id.TenantID)
			return reserveOutcome{dec: dec}
		}
	}
	return reserveOutcome{tenantID: id.TenantID, reservedAt: reservedAt}
}

// finishLocked 在 g.mu 内把执行结果结算为终局并提交（释放/退款、幂等表、审计、journal/audit）。
// 调用方持有 g.mu。
func (g *Gateway) finishLocked(req Request, ro reserveOutcome, execErr error) Decision {
	dec := Decision{RequestID: req.RequestID, TenantID: ro.tenantID}
	g.limiter.Release(ro.tenantID)
	if execErr != nil {
		g.limiter.Refund(ro.tenantID)
		dec.Code = CodeExecFailed
		dec.Reason = execErr.Error()
	} else {
		dec.Code = CodeExecuted
		dec.Reason = "ok"
	}
	dec.Usage = g.limiter.Usage(ro.tenantID)
	g.commitLocked(req, dec, dec.Usage, ro.reservedAt)
	return dec
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

// duplicateOf 基于首次判定构造一个重复判定（不修改 seen 中保存的原判定）。
func duplicateOf(first Decision) Decision {
	d := first
	d.Code = CodeDuplicate
	d.Reason = "duplicate of " + string(first.Code)
	return d
}

// Admit 对一次请求做鉴权、配额判定、执行与记账，并写入审计流水。
// 相同 RequestID 的重复提交（包括并发提交）返回首次判定结果，不重复扣减配额；
// 重复判定同样追加一条审计记录，序号连续，可追溯输入、依据与配额。
func (g *Gateway) Admit(ctx context.Context, req Request) Decision {
	// 幂等去重：已完成的直接返回首次结果；进行中的等待其完成。
	g.mu.Lock()
	if d, ok := g.seen[req.RequestID]; ok {
		dup := duplicateOf(d)
		g.commitDuplicateLocked(req, dup)
		g.maybeSnapshotLocked()
		g.mu.Unlock()
		return dup
	}
	if c, ok := g.inFlight[req.RequestID]; ok {
		g.mu.Unlock()
		<-c.done
		dup := duplicateOf(c.dec)
		g.mu.Lock()
		g.commitDuplicateLocked(req, dup)
		g.maybeSnapshotLocked()
		g.mu.Unlock()
		return dup
	}
	c := &inFlightCall{done: make(chan struct{}), req: req}
	g.inFlight[req.RequestID] = c

	// 预留（鉴权+记账+reserved fsync）在锁内完成。
	ro := g.reserveLocked(req)
	if ro.dec.Code != "" {
		// 终局性拒绝：没有执行、没有在飞预留，直接记审计。
		delete(g.inFlight, req.RequestID)
		g.commitLocked(req, ro.dec, ro.dec.Usage, time.Time{})
		g.maybeSnapshotLocked()
		g.mu.Unlock()
		return ro.dec
	}
	c.tenantID = ro.tenantID
	c.reservedAt = ro.reservedAt
	g.mu.Unlock()

	// 下游执行在锁外进行（不阻塞其他租户的判定；并发槽仍被占用）。
	execErr := g.handler(ctx, req)

	// 终局结算、幂等登记、journal/audit 落盘与（必要时）快照截断，全部在同一把锁内。
	g.mu.Lock()
	dec := g.finishLocked(req, ro, execErr)
	c.dec = dec
	close(c.done)
	delete(g.inFlight, req.RequestID)
	g.seen[req.RequestID] = dec
	g.maybeSnapshotLocked()
	g.mu.Unlock()
	return dec
}

// maybeSnapshotLocked 在终局序号整除快照间隔时，于同一把锁内完成
// 快照导出 + 写盘 + 审计补齐 + journal 截断，消除预留/决策与截断之间的竞态。
// 调用方持有 g.mu。
func (g *Gateway) maybeSnapshotLocked() {
	if !g.snapshotDueLocked() {
		return
	}
	if err := g.saveSnapshotLocked(); err != nil && g.Logger != nil {
		g.Logger.Printf("snapshot failed: %v", err)
	}
}

// snapshotDueLocked 判断本次终局提交后是否该打快照。调用方持有 g.mu。
func (g *Gateway) snapshotDueLocked() bool {
	return g.store != nil && g.snapshotEvery > 0 && g.seq%int64(g.snapshotEvery) == 0
}

// commitDuplicateLocked 记录重复判定：返回体是首次判定的副本（保持首次结论），
// 但审计与 journal 记录当前最终配额快照，便于事后核对租户账本。调用方持有 g.mu。
func (g *Gateway) commitDuplicateLocked(req Request, dup Decision) {
	current := quota.Usage{}
	if dup.TenantID != "" {
		current = g.limiter.Usage(dup.TenantID)
	}
	g.commitLocked(req, dup, current, time.Time{})
}

// commitLocked 追加内存审计并把终局事件同步落 journal（提交点）与审计日志。
// auditUsage 是审计/日志中记录的配额快照（重复判定取当前账本，其余等于判定本身）。
// 在 g.mu 内完成，保证磁盘事件顺序与审计序号一致；fsync 串行化判定提交，本地单进程可接受。
// 调用方持有 g.mu。
func (g *Gateway) commitLocked(req Request, dec Decision, auditUsage quota.Usage, reservedAt time.Time) {
	rec := g.recordLocked(req, dec, auditUsage)
	if g.store == nil {
		return
	}
	d := persist.DecisionEntry{
		At: rec.At, ReservedAt: reservedAt, RequestID: req.RequestID, TokenID: req.TokenID, Scope: req.Scope,
		TenantID: dec.TenantID, Code: string(dec.Code), Reason: dec.Reason, Seq: rec.Seq,
		Tokens: auditUsage.Tokens, InFlight: auditUsage.InFlight,
		QuotaUsed: auditUsage.QuotaUsed, QuotaRemain: auditUsage.QuotaRemain,
	}
	if err := g.store.AppendDecision(d); err != nil && g.Logger != nil {
		g.Logger.Printf("persist journal failed req=%s: %v", req.RequestID, err)
	}
	if err := g.store.AppendAudit(d); err != nil && g.Logger != nil {
		g.Logger.Printf("persist audit failed req=%s: %v", req.RequestID, err)
	}
}

// recordLocked 追加审计流水并输出日志，返回已追加的记录。
// auditUsage 是审计口径的配额快照（重复判定取当前账本）。调用方必须持有 g.mu。
func (g *Gateway) recordLocked(req Request, dec Decision, auditUsage quota.Usage) AuditRecord {
	g.seq++
	rec := AuditRecord{
		Seq:       g.seq,
		At:        g.clock.Now(),
		RequestID: req.RequestID,
		TenantID:  dec.TenantID,
		Scope:     req.Scope,
		Code:      dec.Code,
		Reason:    dec.Reason,
		Usage:     auditUsage,
	}
	g.audit = append(g.audit, rec)
	if g.Logger != nil {
		g.Logger.Printf("audit seq=%d req=%s token=%s scope=%s tenant=%s code=%s reason=%q usage={tokens:%.2f inflight:%d used:%d remain:%d}",
			rec.Seq, req.RequestID, req.TokenID, req.Scope, rec.TenantID, rec.Code, rec.Reason,
			rec.Usage.Tokens, rec.Usage.InFlight, rec.Usage.QuotaUsed, rec.Usage.QuotaRemain)
	}
	return rec
}

// Audit 返回审计流水快照（持久化模式下从磁盘全量读取，纯内存模式返回本实例审计）。
func (g *Gateway) Audit() []AuditRecord {
	g.mu.Lock()
	store := g.store
	g.mu.Unlock()
	if store == nil {
		g.mu.Lock()
		defer g.mu.Unlock()
		out := make([]AuditRecord, len(g.audit))
		copy(out, g.audit)
		return out
	}
	entries, err := store.ReadAudit()
	if err != nil && g.Logger != nil {
		g.Logger.Printf("read audit failed: %v", err)
	}
	out := make([]AuditRecord, 0, len(entries))
	for _, e := range entries {
		out = append(out, auditFromEntry(e))
	}
	return out
}
