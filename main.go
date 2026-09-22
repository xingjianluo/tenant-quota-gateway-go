// Command tenant-quota-gateway 是本地多租户请求准入网关的内存演示程序：
// 用内存状态 + 本地追加日志模拟真实调用，展示鉴权、限流、并发配额、
// 幂等去重与进程重启后的状态恢复。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/audit"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/gateway"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/journal"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

type env struct {
	clk   *clock.FakeClock
	authn *auth.Authenticator
	path  string
}

func main() {
	start := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	dir, err := os.MkdirTemp("", "tqg-journal-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "events.jsonl")

	clk := clock.NewFakeClock(start)
	e := &env{
		clk:   clk,
		authn: auth.New(clk),
		path:  path,
	}

	gw := e.newGateway()

	// 令牌签发。
	tokA, _ := e.authn.Issue("tenant-a", "a1", []model.Scope{model.ScopeRead}, time.Hour)
	tokB, _ := e.authn.Issue("tenant-b", "b1", []model.Scope{model.ScopeRead}, time.Hour)
	tokWrite, _ := e.authn.Issue("tenant-a", "a2", []model.Scope{model.ScopeRead}, time.Hour)
	tokExp, _ := e.authn.Issue("tenant-a", "a3", []model.Scope{model.ScopeRead}, time.Minute)
	tokRev, _ := e.authn.Issue("tenant-a", "a4", []model.Scope{model.ScopeRead}, time.Hour)
	e.authn.Revoke(tokRev)

	show := func(tag string, d model.Decision) {
		fmt.Printf("%-30s allowed=%-5v code=%-17s tenant=%s rate=%.2f/%.2f conc=%d/%d replay=%v\n",
			tag, d.Allowed, d.RejectCode, d.TenantID,
			d.RateRemaining, d.RateCapacity,
			d.ConcurrencyInUse, d.ConcurrencyMax, d.Replayed)
	}
	var admitted []model.Decision
	admit := func(tag string, req model.Request) model.Decision {
		d := gw.Admit(req)
		show(tag, d)
		if d.Allowed {
			admitted = append(admitted, d)
		}
		return d
	}
	// finishAll 结束所有在途请求：成功结束（速率令牌保持消耗，仅释放并发槽）。
	finishAll := func() {
		for _, d := range admitted {
			gw.Finish(d.RequestID, false)
		}
		admitted = nil
	}

	fmt.Println("== 1. 正常放行 ==")
	admit("A/read#1", model.Request{Token: tokA, RequiredScope: model.ScopeRead})

	fmt.Println("== 2. 越权（仅 read，请求 write），不占配额 ==")
	admit("A/write SCOPE_DENIED", model.Request{Token: tokWrite, RequiredScope: model.ScopeWrite})

	fmt.Println("== 3. 已撤销令牌 ==")
	admit("A/revoked TOKEN_REVOKED", model.Request{Token: tokRev, RequiredScope: model.ScopeRead})

	fmt.Println("== 4. 并发打满：A MaxConcurrent=3，连续 3 个在途后第 4 个被拒 ==")
	// #1 已在途，再放行 2 个。
	admit("A/read#2", model.Request{Token: tokA, RequiredScope: model.ScopeRead})
	admit("A/read#3", model.Request{Token: tokA, RequiredScope: model.ScopeRead})
	admit("A/read#4 CONCURRENCY_FULL", model.Request{Token: tokA, RequiredScope: model.ScopeRead})

	fmt.Println("== 5. 在途请求执行失败：释放并发槽并退还速率令牌，随后可重试 ==")
	fail := admitted[0]
	gw.Finish(fail.RequestID, true)
	admit("A/read retry ok after refund", model.Request{Token: tokA, RequiredScope: model.ScopeRead})

	fmt.Println("== 6. 全部成功结束后，时间快进 1 秒，观察令牌桶回补 ==")
	finishAll()
	e.clk.Advance(time.Second)
	admit("A/read refilled", model.Request{Token: tokA, RequiredScope: model.ScopeRead})
	finishAll()

	fmt.Println("== 7. 租户隔离 + 速率耗尽：B 桶容量 1，与 A 互不影响 ==")
	admit("B/read#1", model.Request{Token: tokB, RequiredScope: model.ScopeRead})
	admit("B/read#2 RATE_LIMITED", model.Request{Token: tokB, RequiredScope: model.ScopeRead})

	fmt.Println("== 8. 幂等重复提交：同一 IdempotencyKey 返回原结论且不重复扣配额 ==")
	keyReq := model.Request{Token: tokB, RequiredScope: model.ScopeRead, IdempotencyKey: "k-1"}
	admit("B/idem first RATE_LIMITED", keyReq)
	admit("B/idem replay same", keyReq)
	finishAll()

	fmt.Println("== 9. 过期令牌：时间快进 2 分钟后短期令牌失效 ==")
	e.clk.Advance(2 * time.Minute)
	admit("A/expired TOKEN_EXPIRED", model.Request{Token: tokExp, RequiredScope: model.ScopeRead})

	fmt.Println("== 10. 模拟进程重启：重放本地日志恢复配额口径 ==")
	e.clk.Advance(time.Second)
	gw2 := e.newGateway()
	if err := gw2.Restore(); err != nil {
		panic(err)
	}
	if snap, ok := gw2.QuotaSnapshot("tenant-a"); ok {
		fmt.Printf("after restore tenant-a: rate=%.2f/%.2f conc=%d/%d\n",
			snap.RateRemaining, snap.RateCapacity, snap.ConcurrencyInUse, snap.ConcurrencyMax)
	}
	if snap, ok := gw2.QuotaSnapshot("tenant-b"); ok {
		fmt.Printf("after restore tenant-b: rate=%.2f/%.2f conc=%d/%d\n",
			snap.RateRemaining, snap.RateCapacity, snap.ConcurrencyInUse, snap.ConcurrencyMax)
	}
	admit2 := func(tag string, req model.Request) { show(tag, gw2.Admit(req)) }
	e.clk.Advance(2 * time.Second) // B 桶回补 1 个令牌
	admit2("B/read after restart ok", model.Request{Token: tokB, RequiredScope: model.ScopeRead, IdempotencyKey: "k-2"})
}

// newGateway 装配一个网关实例。每次调用都使用全新的内存配额管理器，
// 用于“重启”场景时配合 Restore() 重放本地日志恢复记账口径。
func (e *env) newGateway() *gateway.Gateway {
	quotas := quota.New(e.clk)
	quotas.AddTenant("tenant-a", quota.Limits{RatePerSecond: 5, MaxConcurrent: 3})
	quotas.AddTenant("tenant-b", quota.Limits{RatePerSecond: 1, MaxConcurrent: 1})
	jrnl, err := journal.Open(e.path)
	if err != nil {
		panic(err)
	}
	return gateway.New(gateway.Deps{
		Clock:   e.clk,
		Authn:   e.authn,
		Quotas:  quotas,
		Auditor: audit.New(),
		Journal: jrnl,
	})
}
