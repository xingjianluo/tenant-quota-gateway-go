// Command gateway 在本地演示多租户准入网关：鉴权、限流、配额扣减与审计流水。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/gateway"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

func main() {
	logger := log.New(os.Stdout, "[gateway] ", log.LstdFlags)
	clk := clock.Real{}

	// 令牌仓库：两个租户，各持一个令牌。
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-alpha", TenantID: "alpha", Scopes: []string{"invoke"}, ExpiresAt: time.Now().Add(time.Hour)})
	store.Put(auth.Token{ID: "tok-beta", TenantID: "beta", Scopes: []string{"invoke"}, ExpiresAt: time.Now().Add(time.Hour)})

	// 配额：alpha 小配额便于演示超额拒绝。
	limiter := quota.NewLimiter(clk)
	limiter.SetLimit("alpha", quota.Limit{RatePerSec: 100, Burst: 100, MaxConcurrent: 64, TotalQuota: 8})
	limiter.SetLimit("beta", quota.Limit{RatePerSec: 5, Burst: 5, MaxConcurrent: 2, TotalQuota: 100})

	gw := gateway.New(auth.NewAuthenticator(store, clk), limiter, clk,
		func(ctx context.Context, req gateway.Request) error { return nil })
	gw.Logger = logger

	ctx := context.Background()

	fmt.Println("== 1. 正常请求 ==")
	gw.Admit(ctx, gateway.Request{RequestID: "demo-1", TokenID: "tok-alpha", Scope: "invoke"})

	fmt.Println("== 2. 越权 / 未知令牌（不消耗配额） ==")
	gw.Admit(ctx, gateway.Request{RequestID: "demo-2", TokenID: "tok-alpha", Scope: "admin"})
	gw.Admit(ctx, gateway.Request{RequestID: "demo-3", TokenID: "forged", Scope: "invoke"})

	fmt.Println("== 3. 幂等重复提交 ==")
	gw.Admit(ctx, gateway.Request{RequestID: "demo-1", TokenID: "tok-alpha", Scope: "invoke"})

	fmt.Println("== 4. 并发压测 alpha（配额 8，并发 64 请求） ==")
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			gw.Admit(ctx, gateway.Request{RequestID: fmt.Sprintf("burst-%d", i), TokenID: "tok-alpha", Scope: "invoke"})
		}(i)
	}
	wg.Wait()

	fmt.Println("== 5. 租户隔离：beta 不受 alpha 超额影响 ==")
	gw.Admit(ctx, gateway.Request{RequestID: "beta-1", TokenID: "tok-beta", Scope: "invoke"})

	fmt.Println("== 审计流水汇总 ==")
	counts := map[gateway.Code]int{}
	for _, r := range gw.Audit() {
		counts[r.Code]++
	}
	for code, n := range counts {
		fmt.Printf("  %-28s %d\n", code, n)
	}
	fmt.Printf("alpha 最终用量: %+v\n", limiter.Usage("alpha"))
	fmt.Printf("beta  最终用量: %+v\n", limiter.Usage("beta"))
}
