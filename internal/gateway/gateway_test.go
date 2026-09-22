package gateway

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/audit"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/journal"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

type fixture struct {
	clk    *clock.FakeClock
	authn  *auth.Authenticator
	quotas *quota.Manager
	path   string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	base := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(base)
	path := filepath.Join(t.TempDir(), "events.jsonl")
	return &fixture{
		clk:   clk,
		authn: auth.New(clk),
		path:  path,
	}
}

// freshGateway 模拟一次进程装配：全新内存配额 + 打开本地日志（不重放）。
func (f *fixture) freshGateway() *Gateway {
	f.quotas = quota.New(f.clk)
	f.quotas.AddTenant("a", quota.Limits{RatePerSecond: 3, MaxConcurrent: 2})
	f.quotas.AddTenant("b", quota.Limits{RatePerSecond: 1, MaxConcurrent: 1})
	jrnl, err := journal.Open(f.path)
	if err != nil {
		panic(err)
	}
	return New(Deps{
		Clock:   f.clk,
		Authn:   f.authn,
		Quotas:  f.quotas,
		Auditor: audit.New(),
		Journal: jrnl,
	})
}

// restartedGateway 模拟进程重启：全新内存状态，先注册租户，再重放日志。
func (f *fixture) restartedGateway() *Gateway {
	gw := f.freshGateway()
	if err := gw.Restore(); err != nil {
		panic(err)
	}
	return gw
}

func (f *fixture) token(tenant, id string, scopes []model.Scope, ttl time.Duration) string {
	tok, _ := f.authn.Issue(tenant, id, scopes, ttl)
	return tok
}

func readReq(tok string) model.Request {
	return model.Request{Token: tok, RequiredScope: model.ScopeRead}
}

func TestRejectCodesAreDistinguishableAndFree(t *testing.T) {
	f := newFixture(t)
	gw := f.freshGateway()

	readOnly := f.token("a", "1", []model.Scope{model.ScopeRead}, time.Hour)
	writeNeed := readReq(f.token("a", "2", []model.Scope{model.ScopeRead}, time.Hour))
	writeNeed.RequiredScope = model.ScopeWrite
	expTok := f.token("a", "3", []model.Scope{model.ScopeRead}, time.Minute)
	revTok := f.token("a", "4", []model.Scope{model.ScopeRead}, time.Hour)
	f.authn.Revoke(revTok)

	// 先放行一个请求作为基线（桶 3 -> 2，在途 1）。
	base := gw.Admit(readReq(readOnly))
	if !base.Allowed {
		t.Fatalf("baseline should pass: %+v", base)
	}
	snapBefore, _ := gw.QuotaSnapshot("a")

	assertReject := func(name string, req model.Request, want model.RejectCode) {
		t.Helper()
		d := gw.Admit(req)
		if d.Allowed || d.RejectCode != want {
			t.Fatalf("%s: got allowed=%v code=%s want code=%s", name, d.Allowed, d.RejectCode, want)
		}
	}

	assertReject("missing token", model.Request{RequiredScope: model.ScopeRead}, model.RejectMissingToken)
	assertReject("unknown token", readReq("tok_ghost"), model.RejectUnknownToken)
	assertReject("scope denied", writeNeed, model.RejectScopeDenied)

	f.clk.Advance(time.Minute + time.Second)
	assertReject("expired", readReq(expTok), model.RejectTokenExpired)
	assertReject("revoked", readReq(revTok), model.RejectTokenRevoked)
	f.clk.Advance(-(time.Minute + time.Second))

	// 关键不变量：上述所有拒绝都不得消耗任何配额或并发槽。
	snapAfter, _ := gw.QuotaSnapshot("a")
	if snapAfter != snapBefore {
		t.Fatalf("rejected requests changed quota:\nbefore=%+v\nafter =%+v", snapBefore, snapAfter)
	}
}

func TestRateAndConcurrencyReasons(t *testing.T) {
	f := newFixture(t)
	gw := f.freshGateway()
	tok := f.token("a", "1", []model.Scope{model.ScopeRead}, time.Hour)

	// 打满并发槽（容量 2）。
	d1 := gw.Admit(readReq(tok))
	d2 := gw.Admit(readReq(tok))
	if !d1.Allowed || !d2.Allowed {
		t.Fatal("first two should be admitted")
	}
	d3 := gw.Admit(readReq(tok))
	if d3.RejectCode != model.RejectConcurrencyFull {
		t.Fatalf("got %s want CONCURRENCY_FULL", d3.RejectCode)
	}

	// 结束两个请求（成功：只释放槽，速率令牌仍消耗）。此时桶剩 1（3-2）。
	gw.Finish(d1.RequestID, false)
	gw.Finish(d2.RequestID, false)

	// 再放行 1 个把桶耗尽（2 -> 1 -> 0 之间：实际剩 1 令牌）。
	d4 := gw.Admit(readReq(tok))
	if !d4.Allowed {
		t.Fatalf("should admit on last token: %+v", d4)
	}
	// 在途 1，桶 0：下一个首先撞上速率限制。
	d5 := gw.Admit(readReq(tok))
	if d5.RejectCode != model.RejectRateLimited {
		t.Fatalf("got %s want RATE_LIMITED", d5.RejectCode)
	}
}

func TestFailedExecutionRefundsAndReleases(t *testing.T) {
	f := newFixture(t)
	gw := f.freshGateway()
	tok := f.token("b", "1", []model.Scope{model.ScopeRead}, time.Hour) // B: rate=1 conc=1

	d := gw.Admit(readReq(tok))
	if !d.Allowed {
		t.Fatalf("admit: %+v", d)
	}
	if r := gw.Admit(readReq(tok)); r.RejectCode != model.RejectRateLimited {
		t.Fatalf("got %s want RATE_LIMITED", r.RejectCode)
	}

	// 执行失败：退还速率令牌并释放并发槽。
	gw.Finish(d.RequestID, true)

	retry := gw.Admit(readReq(tok))
	if !retry.Allowed {
		t.Fatalf("retry after failed execution should pass: %+v", retry)
	}
	// Finish 必须幂等：重复结束不能重复释放/退还。
	gw.Finish(d.RequestID, true)
	snap, _ := gw.QuotaSnapshot("b")
	if snap.ConcurrencyInUse != 1 {
		t.Fatalf("double Finish must not release twice: inUse=%d", snap.ConcurrencyInUse)
	}
}

func TestIdempotencyNoDoubleCharge(t *testing.T) {
	f := newFixture(t)
	gw := f.freshGateway()
	tok := f.token("b", "1", []model.Scope{model.ScopeRead}, time.Hour)

	req := model.Request{Token: tok, RequiredScope: model.ScopeRead, IdempotencyKey: "key-1"}
	first := gw.Admit(req)
	if !first.Allowed {
		t.Fatalf("first: %+v", first)
	}
	snap1, _ := gw.QuotaSnapshot("b")

	second := gw.Admit(req)
	if !second.Allowed || !second.Replayed || second.RequestID == first.RequestID {
		t.Fatalf("second must be a replay with new request id: %+v", second)
	}
	snap2, _ := gw.QuotaSnapshot("b")
	if snap1 != snap2 {
		t.Fatalf("replay charged quota: %+v vs %+v", snap1, snap2)
	}
}

func TestConcurrentDuplicateIdempotency(t *testing.T) {
	f := newFixture(t)
	gw := f.freshGateway()
	tok := f.token("b", "1", []model.Scope{model.ScopeRead}, time.Hour)
	req := model.Request{Token: tok, RequiredScope: model.ScopeRead, IdempotencyKey: "hot-key"}

	const n = 32
	var wg sync.WaitGroup
	decisions := make([]model.Decision, n)
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			decisions[i] = gw.Admit(req)
		}(i)
	}
	close(start)
	wg.Wait()

	admitted, replayed := 0, 0
	for i, d := range decisions {
		if d.Allowed && !d.Replayed {
			admitted++
		}
		if d.Replayed {
			replayed++
			if !d.Allowed {
				t.Fatalf("replay %d must mirror the original allowed decision", i)
			}
		}
		if i > 0 && d.RejectCode != decisions[0].RejectCode {
			t.Fatalf("decision %d differs: %s vs %s", i, d.RejectCode, decisions[0].RejectCode)
		}
	}
	if admitted != 1 {
		t.Fatalf("real admissions=%d want exactly 1 (duplicates must replay, not re-charge)", admitted)
	}
	if replayed != n-1 {
		t.Fatalf("replays=%d want %d", replayed, n-1)
	}
	snap, _ := gw.QuotaSnapshot("b")
	if snap.ConcurrencyInUse != 1 {
		t.Fatalf("inUse=%d want 1 (duplicates must not occupy extra slots)", snap.ConcurrencyInUse)
	}
}

func TestRejectedIdempotencyCanRetry(t *testing.T) {
	f := newFixture(t)
	gw := f.freshGateway()
	tok := f.token("b", "1", []model.Scope{model.ScopeRead}, time.Hour)

	// 先占满 B。
	holder := gw.Admit(readReq(tok))
	if !holder.Allowed {
		t.Fatal(holder)
	}
	req := model.Request{Token: tok, RequiredScope: model.ScopeRead, IdempotencyKey: "k"}
	d1 := gw.Admit(req)
	if d1.RejectCode != model.RejectRateLimited {
		t.Fatalf("got %s", d1.RejectCode)
	}
	// 并发“在途重复”也得到相同拒绝。
	d2 := gw.Admit(req)
	if d2.RejectCode != model.RejectRateLimited {
		t.Fatalf("duplicate in-flight: got %s", d2.RejectCode)
	}
	// 释放后，同键应可重新判定（拒绝不占任何资源）。
	gw.Finish(holder.RequestID, false)
	f.clk.Advance(time.Second)
	d3 := gw.Admit(req)
	if !d3.Allowed {
		t.Fatalf("retry after resource freed should be a fresh decision: %+v", d3)
	}
}

func TestRestartReplayRestoresQuota(t *testing.T) {
	f := newFixture(t)
	gw := f.freshGateway()
	tok := f.token("b", "1", []model.Scope{model.ScopeRead}, time.Hour)

	d := gw.Admit(readReq(tok))
	if !d.Allowed {
		t.Fatal(d)
	}
	// 此时 B 在途 1、桶 0，再来必拒。
	if r := gw.Admit(readReq(tok)); r.RejectCode != model.RejectRateLimited {
		t.Fatalf("got %s", r.RejectCode)
	}

	// 重启：全新内存，重放日志。重放后立即（同一时刻）判定：
	// B 桶为 0 且在途 1，速率与并发双满，按判定顺序先返回 RATE_LIMITED。
	gw2 := f.restartedGateway()
	snap, ok := gw2.QuotaSnapshot("b")
	if !ok {
		t.Fatal("tenant b missing after restore")
	}
	if snap.ConcurrencyInUse != 1 {
		t.Fatalf("inUse after restore=%d want 1", snap.ConcurrencyInUse)
	}
	if snap.RateRemaining > 1e-9 {
		t.Fatalf("remaining after restore=%v want 0", snap.RateRemaining)
	}
	if r := gw2.Admit(readReq(tok)); r.RejectCode != model.RejectRateLimited {
		t.Fatalf("after restore got %s want RATE_LIMITED", r.RejectCode)
	}

	// 在 gw2 上结束该请求（成功：释放槽、不退令牌），持久化 finish。
	gw2.Finish(d.RequestID, false)

	// 再次重启并重放全部事件（admit + finish）：并发归零。
	gw3 := f.restartedGateway()
	snap3, _ := gw3.QuotaSnapshot("b")
	if snap3.ConcurrencyInUse != 0 {
		t.Fatalf("inUse=%d want 0 after finished replay", snap3.ConcurrencyInUse)
	}
	// 推进 1 秒回补 1 个速率令牌后，请求应可放行。
	f.clk.Advance(time.Second)
	if r := gw3.Admit(readReq(tok)); !r.Allowed {
		t.Fatalf("after finish replay + refill should admit: %s", r.RejectCode)
	}
}

func TestConcurrentGatewayNoOverIssue(t *testing.T) {
	f := newFixture(t)
	gw := f.freshGateway()
	tok := f.token("a", "1", []model.Scope{model.ScopeRead}, time.Hour)

	const n = 300
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed, rateDenied, concDenied := 0, 0, 0
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			d := gw.Admit(readReq(tok))
			mu.Lock()
			switch {
			case d.Allowed:
				allowed++
			case d.RejectCode == model.RejectRateLimited:
				rateDenied++
			case d.RejectCode == model.RejectConcurrencyFull:
				concDenied++
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if allowed != 2 { // rate=3 但并发=2，首次只允许 2 个在途
		t.Fatalf("allowed=%d want 2", allowed)
	}
	if allowed+rateDenied+concDenied != n {
		t.Fatalf("unaccounted decisions: %d %d %d", allowed, rateDenied, concDenied)
	}
	snap, _ := gw.QuotaSnapshot("a")
	if snap.ConcurrencyInUse != 2 {
		t.Fatalf("inUse=%d want 2", snap.ConcurrencyInUse)
	}
}
