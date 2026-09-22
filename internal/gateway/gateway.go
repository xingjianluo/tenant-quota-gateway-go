package gateway

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/audit"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/journal"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// inflight 记录一个幂等键对应的在途/已完成判定。
type inflight struct {
	done     chan struct{}
	tenantID string
	decision model.Decision
	// 首个请求被拒绝时置位：所有读者读完结论后清理记录，
	// 之后同键再次提交会重新判定（拒绝没有任何配额副作用）。
	rejected bool
	readers  int // 首请求记 1，每个并发等待者 +1
}

// admittedReq 记录每个已放行请求的归还参数。
type admittedReq struct {
	tenantID string
	idemKey  string
	finished bool
	refunded bool
}

// Gateway 编排鉴权、幂等去重与配额记账。
type Gateway struct {
	clk     clock.Clock
	authn   *auth.Authenticator
	quotas  *quota.Manager
	auditor *audit.Logger
	jrnl    *journal.Journal

	idSeq atomic.Uint64

	mu       sync.Mutex
	inflight map[string]*inflight    // idemKey -> 在途/最近一次判定
	requests map[string]*admittedReq // requestID -> 已放行请求
}

// Deps 为构造依赖。Journal 可为 nil（纯内存模式，测试用）。
type Deps struct {
	Clock   clock.Clock
	Authn   *auth.Authenticator
	Quotas  *quota.Manager
	Auditor *audit.Logger
	Journal *journal.Journal
}

func New(d Deps) *Gateway {
	return &Gateway{
		clk:      d.Clock,
		authn:    d.Authn,
		quotas:   d.Quotas,
		auditor:  d.Auditor,
		jrnl:     d.Journal,
		inflight: make(map[string]*inflight),
		requests: make(map[string]*admittedReq),
	}
}

// Admit 是请求准入入口。
//
// 顺序（任一拒绝都发生在配额记账之前，绝不消耗配额）：
//  1. 幂等在途/历史判定（重复提交直接返回原结论，不重复扣配额）；
//  2. 令牌认证（缺失/未知/过期/撤销）；
//  3. 作用域授权（越权）；
//  4. 配额原子判定与记账（速率 / 并发）。
func (g *Gateway) Admit(req model.Request) model.Decision {
	now := g.clk.Now()
	requestID := g.newRequestID()

	// 1. 幂等去重。
	if req.IdempotencyKey != "" {
		if d, ok := g.lookupReplay(req.IdempotencyKey); ok {
			replayed := d // 复制一份，不改写共享的历史结论
			replayed.RequestID = requestID
			replayed.Replayed = true
			g.audit(requestID, replayed.TenantID, req, "idempotency replay: same key already processed", replayed)
			return replayed
		}
		defer g.completeInflightReader(req.IdempotencyKey)
	}

	dec := model.Decision{RequestID: requestID, At: now}
	reject := func(code model.RejectCode, basis string) model.Decision {
		dec.Allowed = false
		dec.RejectCode = code
		dec.Reason = code.String()
		// dec.TenantID 已由调用方在可识别时填好，保持原样便于追溯。
		g.audit(requestID, dec.TenantID, req, basis, dec)
		g.failInflight(req.IdempotencyKey, dec)
		return dec
	}

	// 2. 认证。
	claims, code := g.authn.Authenticate(req.Token)
	if code != model.RejectNone {
		return reject(code, "authentication failed: "+code.String())
	}
	dec.TenantID = claims.TenantID

	// 3. 授权。
	if code := g.authn.Authorize(claims, req.RequiredScope); code != model.RejectNone {
		return reject(code, "authorization failed: token scopes do not cover required scope")
	}

	// 4. 配额：判定与记账原子完成。
	snap, code := g.quotas.TryAcquire(claims.TenantID)
	fillSnapshot(&dec, snap)
	if code != model.RejectNone {
		dec.TenantID = claims.TenantID
		return reject(code, "quota check failed: "+code.String())
	}

	// 放行：登记并持久化，保证崩溃/重启后口径一致。
	dec.Allowed = true
	g.registerAdmitted(requestID, claims.TenantID, req.IdempotencyKey, dec)
	g.appendJournal(journal.Event{
		Kind:      journal.EventAdmit,
		TenantID:  claims.TenantID,
		RequestID: requestID,
		IdemKey:   req.IdempotencyKey,
		At:        now,
	})
	g.audit(requestID, claims.TenantID, req, "authenticated, authorized and quota acquired", dec)
	return dec
}

// Finish 在请求执行结束后归还并发槽；executeFailed=true 时一并退还速率令牌。
// 同一请求重复结束是幂等的，不会重复归还。
func (g *Gateway) Finish(requestID string, executeFailed bool) {
	g.mu.Lock()
	r, ok := g.requests[requestID]
	if !ok || r.finished {
		g.mu.Unlock()
		return
	}
	r.finished = true
	if !executeFailed {
		r.refunded = true // 成功请求的速率令牌保持消耗
	}
	tenantID, idemKey := r.tenantID, r.idemKey
	g.mu.Unlock()

	g.quotas.Release(tenantID, executeFailed)
	g.appendJournal(journal.Event{
		Kind:       journal.EventFinish,
		TenantID:   tenantID,
		RequestID:  requestID,
		IdemKey:    idemKey,
		At:         g.clk.Now(),
		RefundRate: executeFailed,
	})

	if snap, ok := g.quotas.SnapshotOf(tenantID); ok {
		if g.auditor != nil {
			g.auditor.Log(audit.Entry{
				RequestID: requestID,
				TenantID:  tenantID,
				Basis:     finishBasis(executeFailed),
				Decision: model.Decision{
					Allowed:          true,
					TenantID:         tenantID,
					RequestID:        requestID,
					At:               g.clk.Now(),
					RateRemaining:    snap.RateRemaining,
					RateCapacity:     snap.RateCapacity,
					ConcurrencyInUse: snap.ConcurrencyInUse,
					ConcurrencyMax:   snap.ConcurrencyMax,
				},
			})
		}
	}
}

// Restore 重放持久化事件，恢复幂等表与配额记账状态（进程重启后调用）。
// 注意：调用前必须已完成所有租户的 AddTenant 配置。
func (g *Gateway) Restore() error {
	if g.jrnl == nil {
		return nil
	}
	return g.jrnl.Replay(func(ev journal.Event) {
		journal.Apply(
			func(tenantID, reqID, idemKey string, at time.Time) bool {
				if _, ok := g.quotas.SnapshotOf(tenantID); !ok {
					return false
				}
				g.quotas.RestoreAcquire(tenantID, at)
				g.mu.Lock()
				g.requests[reqID] = &admittedReq{tenantID: tenantID, idemKey: idemKey}
				if idemKey != "" {
					in := &inflight{
						tenantID: tenantID,
						done:     make(chan struct{}),
						decision: model.Decision{
							Allowed:          true,
							TenantID:         tenantID,
							RequestID:        reqID,
							At:               at,
							Replayed:         true,
							RateRemaining:    0,
							ConcurrencyInUse: 0,
						},
					}
					snap, _ := g.quotas.SnapshotOf(tenantID)
					in.decision.RateRemaining = snap.RateRemaining
					in.decision.RateCapacity = snap.RateCapacity
					in.decision.ConcurrencyInUse = snap.ConcurrencyInUse
					in.decision.ConcurrencyMax = snap.ConcurrencyMax
					close(in.done)
					g.inflight[idemKey] = in
				}
				g.mu.Unlock()
				return true
			},
			func(tenantID, reqID string, refund bool, at time.Time) {
				g.quotas.RestoreRelease(tenantID, at, refund)
				g.mu.Lock()
				if r, ok := g.requests[reqID]; ok {
					r.finished = true
					r.refunded = !refund
				}
				g.mu.Unlock()
			},
			ev,
		)
	})
}

func (g *Gateway) newRequestID() string {
	n := g.idSeq.Add(1)
	return fmt.Sprintf("req-%d-%d", g.clk.Now().UnixNano(), n)
}

// lookupReplay 处理幂等键：已存在则等待其判定完成并返回副本；
// 不存在则占用一个在途槽位（相同键的并发重复请求将等待首个结论）。
func (g *Gateway) lookupReplay(key string) (model.Decision, bool) {
	g.mu.Lock()
	in, exists := g.inflight[key]
	if !exists {
		in = &inflight{done: make(chan struct{}), readers: 1}
		g.inflight[key] = in
		g.mu.Unlock()
		return model.Decision{}, false
	}
	in.readers++
	done := in.done
	g.mu.Unlock() // 登记/计数已对其他 goroutine 可见，在锁外等待

	<-done

	g.mu.Lock()
	d := in.decision
	in.readers--
	if in.rejected && in.readers == 0 {
		delete(g.inflight, key)
	}
	g.mu.Unlock()
	return d, true
}

// completeInflightReader 是首个请求读完自身结论后的收尾：
// 被拒绝且已无其他读者时清理在途记录；被放行则永久保留用于后续重放。
func (g *Gateway) completeInflightReader(key string) {
	if key == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	in, ok := g.inflight[key]
	if !ok {
		return
	}
	in.readers--
	if in.rejected && in.readers == 0 {
		delete(g.inflight, key)
	}
}

// failInflight 记录拒绝结论并唤醒所有等待同一幂等键的并发请求。
// 注意：此处不调整 readers 计数，统一由读者读完后收尾。
func (g *Gateway) failInflight(key string, dec model.Decision) {
	if key == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	in, ok := g.inflight[key]
	if !ok {
		return
	}
	in.tenantID = dec.TenantID
	in.decision = dec
	in.rejected = true
	close(in.done)
}

func (g *Gateway) registerAdmitted(requestID, tenantID, idemKey string, dec model.Decision) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requests[requestID] = &admittedReq{tenantID: tenantID, idemKey: idemKey}
	if idemKey != "" {
		if in, ok := g.inflight[idemKey]; ok {
			in.tenantID = tenantID
			in.decision = dec
			close(in.done)
		}
	}
}

func (g *Gateway) appendJournal(ev journal.Event) {
	if g.jrnl != nil {
		_ = g.jrnl.Append(ev)
	}
}

func (g *Gateway) audit(requestID, tenantID string, req model.Request, basis string, dec model.Decision) {
	if g.auditor != nil {
		g.auditor.Log(audit.Entry{
			RequestID: requestID,
			TenantID:  tenantID,
			Input:     req,
			Basis:     basis,
			Decision:  dec,
		})
	}
}

func fillSnapshot(dec *model.Decision, s quota.Snapshot) {
	dec.RateRemaining = s.RateRemaining
	dec.RateCapacity = s.RateCapacity
	dec.ConcurrencyInUse = s.ConcurrencyInUse
	dec.ConcurrencyMax = s.ConcurrencyMax
}

// QuotaSnapshot 返回某租户当前配额口径（演示/测试观察用）。
func (g *Gateway) QuotaSnapshot(tenantID string) (quota.Snapshot, bool) {
	return g.quotas.SnapshotOf(tenantID)
}

func finishBasis(executeFailed bool) string {
	if executeFailed {
		return "execution failed: concurrency slot released, rate token refunded"
	}
	return "execution succeeded: concurrency slot released, rate token consumed"
}
