// Command gateway 在本地演示多租户准入网关：鉴权、限流、配额扣减、审计流水与重启恢复。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/gateway"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

func main() {
	stateDir := flag.String("state", "./.gateway-state", "持久化状态目录（快照 + 提交日志）；留空则纯内存运行")
	reset := flag.Bool("reset", false, "启动前清空状态目录（从头演示）")
	flag.Parse()

	logger := log.New(os.Stdout, "[gateway] ", log.LstdFlags)
	clk := clock.Real{}

	if *reset && *stateDir != "" {
		if err := os.RemoveAll(*stateDir); err != nil {
			log.Fatalf("reset state: %v", err)
		}
	}

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

	// 接入本地持久化：结论先落盘再返回，重启后配额、幂等结论与审计接着延续。
	if *stateDir != "" {
		ps, err := persist.NewFileStore(*stateDir)
		if err != nil {
			log.Fatalf("open state store: %v", err)
		}
		if err := gw.AttachStore(ps, 4096, 256); err != nil {
			log.Fatalf("restore state: %v", err)
		}
		logger.Printf("state restored from %s", *stateDir)
	}

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
	if *stateDir != "" {
		fmt.Printf("\n状态已持久化到 %s；再次运行（不带 -reset）可观察重启恢复：同一批请求将全部判 DUPLICATE，用量不变。\n", *stateDir)
	}
}
