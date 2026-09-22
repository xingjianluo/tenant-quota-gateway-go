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
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// Code 是可区分的判定结果码。
type Code string

const (
	CodeAllowed             Code = "ALLOWED"
	CodeExecuted            Code = "EXECUTED"
	CodeExecFailed          Code = "EXEC_FAILED"
	CodeTokenUnknown        Code = "REJECT_TOKEN_UNKNOWN"
	CodeTokenExpired        Code = "REJECT_TOKEN_EXPIRED"
	CodeTokenRevoked        Code = "REJECT_TOKEN_REVOKED"
	CodeScopeDenied         Code = "REJECT_SCOPE_DENIED"
	CodeRateLimited         Code = "REJECT_RATE_LIMITED"
	CodeConcurrencyLimited  Code = "REJECT_CONCURRENCY_LIMITED"
	CodeQuotaExhausted      Code = "REJECT_QUOTA_EXHAUSTED"
	CodeDuplicate           Code = "DUPLICATE"
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

	mu       sync.Mutex
	seq      int64
	inFlight map[string]*inFlightCall // 正在执行的 RequestID
	seen     map[string]Decision      // 幂等：RequestID -> 首次判定
	audit    []AuditRecord
}

// New 创建网关。
func New(a *auth.Authenticator, l *quota.Limiter, c clock.Clock, h Handler) *Gateway {
	return &Gateway{
		auth: a, limiter: l, clock: c, handler: h,
		Logger:   log.Default(),
		inFlight: make(map[string]*inFlightCall),
		seen:     make(map[string]Decision),
	}
}

// admitOnce 执行一次完整判定（不含幂等去重）。
func (g *Gateway) admitOnce(ctx context.Context, req Request) Decision {
	dec := Decision{RequestID: req.RequestID}

	// 1. 鉴权：越权、过期、撤销的令牌直接拒绝，不触碰任何配额。
	id, err := g.auth.Authorize(req.TokenID, req.Scope)
	if err != nil {
		dec.Code = authCode(err)
		dec.Reason = err.Error()
		return dec
	}
	dec.TenantID = id.TenantID

	// 2. 配额：判定与记账原子完成，拒绝时不产生副作用。
	if err := g.limiter.Acquire(id.TenantID); err != nil {
		dec.Code = quotaCode(err)
		dec.Reason = err.Error()
		dec.Usage = g.limiter.Usage(id.TenantID)
		return dec
	}

	// 3. 执行下游调用；无论成败都释放并发槽，失败时归还配额。
	execErr := g.handler(ctx, req)
	g.limiter.Release(id.TenantID)
	if execErr != nil {
		g.limiter.Refund(id.TenantID)
		dec.Code = CodeExecFailed
		dec.Reason = execErr.Error()
	} else {
		dec.Code = CodeExecuted
		dec.Reason = "ok"
	}
	dec.Usage = g.limiter.Usage(id.TenantID)
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

// Admit 对一次请求做鉴权、配额判定、执行与记账，并写入审计流水。
// 相同 RequestID 的重复提交（包括并发提交）返回首次判定结果，不重复扣减配额。
func (g *Gateway) Admit(ctx context.Context, req Request) Decision {
	// 幂等去重：已完成的直接返回首次结果；进行中的等待其完成。
	g.mu.Lock()
	if d, ok := g.seen[req.RequestID]; ok {
		g.mu.Unlock()
		d.Code = CodeDuplicate
		d.Reason = "duplicate of " + string(d.Code)
		return d
	}
	if c, ok := g.inFlight[req.RequestID]; ok {
		g.mu.Unlock()
		<-c.done
		d := c.dec
		d.Code = CodeDuplicate
		d.Reason = "duplicate of " + string(d.Code)
		return d
	}
	c := &inFlightCall{done: make(chan struct{})}
	g.inFlight[req.RequestID] = c
	g.mu.Unlock()

	dec := g.admitOnce(ctx, req)

	g.mu.Lock()
	c.dec = dec
	close(c.done)
	delete(g.inFlight, req.RequestID)
	g.seen[req.RequestID] = dec
	g.recordLocked(req, dec)
	g.mu.Unlock()
	return dec
}

// recordLocked 追加审计流水并输出日志。调用方必须持有 g.mu。
func (g *Gateway) recordLocked(req Request, dec Decision) {
	g.seq++
	rec := AuditRecord{
		Seq:       g.seq,
		At:        g.clock.Now(),
		RequestID: req.RequestID,
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
}

// Audit 返回审计流水快照。
func (g *Gateway) Audit() []AuditRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]AuditRecord, len(g.audit))
	copy(out, g.audit)
	return out
}
