package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

var t0 = time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)

// fixture 组装一套测试网关，日志丢弃。
type fixture struct {
	gw      *Gateway
	limiter *quota.Limiter
	clock   *clock.Fake
}

func newFixture(t *testing.T, handler Handler) *fixture {
	t.Helper()
	c := clock.NewFake(t0)
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(time.Hour)})
	store.Put(auth.Token{ID: "tok-b", TenantID: "tenant-b", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(time.Hour)})
	l := quota.NewLimiter(c)
	l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 100})
	l.SetLimit("tenant-b", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 100})
	gw := New(auth.NewAuthenticator(store, c), l, c, handler)
	gw.Logger = log.New(io.Discard, "", 0)
	return &fixture{gw: gw, limiter: l, clock: c}
}

func okHandler(context.Context, Request) error { return nil }

func TestAdmitOK(t *testing.T) {
	f := newFixture(t, okHandler)
	d := f.gw.Admit(context.Background(), Request{RequestID: "r1", TokenID: "tok-a", Scope: "invoke"})
	if d.Code != CodeExecuted || d.TenantID != "tenant-a" {
		t.Fatalf("unexpected decision: %+v", d)
	}
	if d.Usage.QuotaUsed != 1 || d.Usage.QuotaRemain != 99 {
		t.Fatalf("unexpected usage: %+v", d.Usage)
	}
}

func TestUnauthorizedConsumesNoQuota(t *testing.T) {
	f := newFixture(t, okHandler)
	ctx := context.Background()

	cases := []struct {
		name string
		req  Request
		want Code
	}{
		{"unknown token", Request{RequestID: "u1", TokenID: "nope", Scope: "invoke"}, CodeTokenUnknown},
		{"scope denied", Request{RequestID: "u2", TokenID: "tok-a", Scope: "admin"}, CodeScopeDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := f.gw.Admit(ctx, tc.req)
			if d.Code != tc.want {
				t.Fatalf("want %s, got %+v", tc.want, d)
			}
		})
	}
	// 过期与撤销
	f.clock.Advance(2 * time.Hour)
	if d := f.gw.Admit(ctx, Request{RequestID: "u3", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeTokenExpired {
		t.Fatalf("want expired, got %+v", d)
	}
	// 所有鉴权失败均不得消耗配额。
	u := f.limiter.Usage("tenant-a")
	if u.QuotaUsed != 0 || u.InFlight != 0 {
		t.Fatalf("unauthorized requests consumed quota: %+v", u)
	}
}

func TestRevokedToken(t *testing.T) {
	c := clock.NewFake(t0)
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-x", TenantID: "tenant-x", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(time.Hour)})
	l := quota.NewLimiter(c)
	l.SetLimit("tenant-x", quota.Limit{RatePerSec: 10, Burst: 10, MaxConcurrent: 1, TotalQuota: 10})
	gw := New(auth.NewAuthenticator(store, c), l, c, okHandler)
	gw.Logger = log.New(io.Discard, "", 0)

	store.Revoke("tok-x")
	d := gw.Admit(context.Background(), Request{RequestID: "r1", TokenID: "tok-x", Scope: "invoke"})
	if d.Code != CodeTokenRevoked {
		t.Fatalf("want revoked, got %+v", d)
	}
	if u := l.Usage("tenant-x"); u.QuotaUsed != 0 {
		t.Fatalf("revoked token consumed quota: %+v", u)
	}
}

func TestQuotaExhaustedRejections(t *testing.T) {
	c := clock.NewFake(t0)
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(time.Hour)})
	l := quota.NewLimiter(c)
	l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 100, TotalQuota: 3})
	gw := New(auth.NewAuthenticator(store, c), l, c, okHandler)
	gw.Logger = log.New(io.Discard, "", 0)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		d := gw.Admit(ctx, Request{RequestID: fmt.Sprintf("ok-%d", i), TokenID: "tok-a", Scope: "invoke"})
		if d.Code != CodeExecuted {
			t.Fatalf("req %d: %+v", i, d)
		}
	}
	d := gw.Admit(ctx, Request{RequestID: "over", TokenID: "tok-a", Scope: "invoke"})
	if d.Code != CodeQuotaExhausted {
		t.Fatalf("want quota exhausted, got %+v", d)
	}
	if u := l.Usage("tenant-a"); u.QuotaUsed != 3 {
		t.Fatalf("rejected request consumed quota: %+v", u)
	}
}

func TestRateLimitedRejection(t *testing.T) {
	c := clock.NewFake(t0)
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(time.Hour)})
	l := quota.NewLimiter(c)
	l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1, Burst: 1, MaxConcurrent: 100, TotalQuota: 100})
	gw := New(auth.NewAuthenticator(store, c), l, c, okHandler)
	gw.Logger = log.New(io.Discard, "", 0)
	ctx := context.Background()

	if d := gw.Admit(ctx, Request{RequestID: "r1", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeExecuted {
		t.Fatalf("first: %+v", d)
	}
	if d := gw.Admit(ctx, Request{RequestID: "r2", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeRateLimited {
		t.Fatalf("want rate limited, got %+v", d)
	}
}

func TestExecFailureRefundsQuota(t *testing.T) {
	boom := errors.New("downstream boom")
	f := newFixture(t, func(context.Context, Request) error { return boom })
	d := f.gw.Admit(context.Background(), Request{RequestID: "f1", TokenID: "tok-a", Scope: "invoke"})
	if d.Code != CodeExecFailed {
		t.Fatalf("want exec failed, got %+v", d)
	}
	u := f.limiter.Usage("tenant-a")
	if u.QuotaUsed != 0 || u.InFlight != 0 {
		t.Fatalf("failed request leaked quota or slot: %+v", u)
	}
}

func TestDuplicateRequestID(t *testing.T) {
	f := newFixture(t, okHandler)
	ctx := context.Background()
	req := Request{RequestID: "dup-1", TokenID: "tok-a", Scope: "invoke"}

	d1 := f.gw.Admit(ctx, req)
	d2 := f.gw.Admit(ctx, req)
	if d1.Code != CodeExecuted {
		t.Fatalf("first: %+v", d1)
	}
	if d2.Code != CodeDuplicate {
		t.Fatalf("want duplicate, got %+v", d2)
	}
	// 重复提交不得重复扣减。
	if u := f.limiter.Usage("tenant-a"); u.QuotaUsed != 1 {
		t.Fatalf("duplicate consumed quota: %+v", u)
	}
}

// TestConcurrentAdmitNoOverGrant 并发压测：同一租户并发 N 个不同请求，
// 成功数不得超过配额，结束后不得泄漏并发槽，审计流水条数与请求数一致。
func TestConcurrentAdmitNoOverGrant(t *testing.T) {
	c := clock.NewFake(t0)
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(time.Hour)})
	l := quota.NewLimiter(c)
	const quotaN = 64
	l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1e9, Burst: 1e9, MaxConcurrent: 1e9, TotalQuota: quotaN})
	gw := New(auth.NewAuthenticator(store, c), l, c, okHandler)
	gw.Logger = log.New(io.Discard, "", 0)
	ctx := context.Background()

	const total = 640
	var wg sync.WaitGroup
	codes := make(chan Code, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := gw.Admit(ctx, Request{RequestID: fmt.Sprintf("c-%d", i), TokenID: "tok-a", Scope: "invoke"})
			codes <- d.Code
		}(i)
	}
	wg.Wait()
	close(codes)

	executed, exhausted, other := 0, 0, 0
	for code := range codes {
		switch code {
		case CodeExecuted:
			executed++
		case CodeQuotaExhausted:
			exhausted++
		default:
			other++
		}
	}
	if executed != quotaN {
		t.Fatalf("executed=%d, want exactly %d (over-grant or loss)", executed, quotaN)
	}
	if exhausted != total-quotaN || other != 0 {
		t.Fatalf("exhausted=%d other=%d, want %d/0", exhausted, other, total-quotaN)
	}
	u := l.Usage("tenant-a")
	if u.InFlight != 0 || u.QuotaUsed != quotaN {
		t.Fatalf("leaked state: %+v", u)
	}
	if got := len(gw.Audit()); got != total {
		t.Fatalf("audit records=%d, want %d", got, total)
	}
}

// TestConcurrentDuplicateSubmissions 同一 RequestID 并发提交：仅执行一次。
func TestConcurrentDuplicateSubmissions(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	counting := func(context.Context, Request) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	}
	f := newFixture(t, counting)
	ctx := context.Background()
	req := Request{RequestID: "same-id", TokenID: "tok-a", Scope: "invoke"}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.gw.Admit(ctx, req)
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
	if u := f.limiter.Usage("tenant-a"); u.QuotaUsed != 1 {
		t.Fatalf("concurrent duplicates consumed quota: %+v", u)
	}
}

// TestTenantIsolationUnderLoad 一个租户打满配额不影响另一租户。
func TestTenantIsolationUnderLoad(t *testing.T) {
	f := newFixture(t, okHandler)
	ctx := context.Background()
	// tenant-a 打满 100 配额
	for i := 0; i < 100; i++ {
		f.gw.Admit(ctx, Request{RequestID: fmt.Sprintf("a-%d", i), TokenID: "tok-a", Scope: "invoke"})
	}
	if d := f.gw.Admit(ctx, Request{RequestID: "a-over", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeQuotaExhausted {
		t.Fatalf("tenant-a should be exhausted: %+v", d)
	}
	if d := f.gw.Admit(ctx, Request{RequestID: "b-1", TokenID: "tok-b", Scope: "invoke"}); d.Code != CodeExecuted {
		t.Fatalf("tenant-b affected by tenant-a: %+v", d)
	}
}

// TestReplayDeterminism 相同配置与输入序列，在新实例（模拟进程重启）上重放，
// 判定序列与最终统计口径完全一致。
func TestReplayDeterminism(t *testing.T) {
	build := func() *Gateway {
		c := clock.NewFake(t0)
		store := auth.NewStore()
		store.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(time.Hour)})
		l := quota.NewLimiter(c)
		l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 10, TotalQuota: 3})
		gw := New(auth.NewAuthenticator(store, c), l, c, okHandler)
		gw.Logger = log.New(io.Discard, "", 0)
		return gw
	}
	inputs := []Request{
		{RequestID: "r1", TokenID: "tok-a", Scope: "invoke"},
		{RequestID: "r2", TokenID: "tok-a", Scope: "invoke"},
		{RequestID: "r3", TokenID: "tok-a", Scope: "invoke"},
		{RequestID: "r4", TokenID: "tok-a", Scope: "invoke"}, // 超额
		{RequestID: "r5", TokenID: "bad", Scope: "invoke"},   // 鉴权失败
	}
	run := func(gw *Gateway) []Code {
		codes := make([]Code, 0, len(inputs))
		for _, r := range inputs {
			codes = append(codes, gw.Admit(context.Background(), r).Code)
		}
		return codes
	}
	first := run(build())
	second := run(build())
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("replay diverged at %d: %v vs %v", i, first, second)
		}
	}
}

func TestAuditTrailContent(t *testing.T) {
	f := newFixture(t, okHandler)
	f.gw.Admit(context.Background(), Request{RequestID: "r1", TokenID: "tok-a", Scope: "invoke"})
	recs := f.gw.Audit()
	if len(recs) != 1 {
		t.Fatalf("audit len=%d", len(recs))
	}
	r := recs[0]
	if r.Seq != 1 || r.RequestID != "r1" || r.TenantID != "tenant-a" || r.Code != CodeExecuted || r.At != t0 {
		t.Fatalf("bad audit record: %+v", r)
	}
}
