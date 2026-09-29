// Command gateway 在本地演示多租户准入网关：鉴权、限流、配额扣减、审计流水，
// 以及基于本地文件的重启恢复（journal + 周期快照 + 只追加审计日志）。
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
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

func main() {
	// -data 指向目录时启用持久化：进程退出后再启动，配额/幂等/审计从该目录延续。
	// 不传则为纯内存模式（与早期版本一致）。
	dataDir := flag.String("data", "", "持久化数据目录（留空=纯内存模式）")
	flag.Parse()

	logger := log.New(os.Stdout, "[gateway] ", log.LstdFlags)
	clk := clock.Real{}

	// 令牌仓库：两个租户，各持一个令牌（部署期配置，不随状态持久化）。
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-alpha", TenantID: "alpha", Scopes: []string{"invoke"}, ExpiresAt: time.Now().Add(time.Hour)})
	store.Put(auth.Token{ID: "tok-beta", TenantID: "beta", Scopes: []string{"invoke"}, ExpiresAt: time.Now().Add(time.Hour)})

	newLimiter := func() *quota.Limiter {
		l := quota.NewLimiter(clk)
		// 配额：alpha 小配额便于演示超额拒绝。
		l.SetLimit("alpha", quota.Limit{RatePerSec: 100, Burst: 100, MaxConcurrent: 64, TotalQuota: 8})
		l.SetLimit("beta", quota.Limit{RatePerSec: 5, Burst: 5, MaxConcurrent: 2, TotalQuota: 100})
		return l
	}

	ctx := context.Background()
	var gw *gateway.Gateway
	if *dataDir != "" {
		var rep *gateway.RecoveryReport
		var err error
		gw, rep, err = gateway.Open(ctx, *dataDir, auth.NewAuthenticator(store, clk), newLimiter(), clk,
			func(ctx context.Context, req gateway.Request) error { return nil })
		if err != nil {
			logger.Fatalf("open gateway: %v", err)
		}
		if rep.SnapshotSeq > 0 {
			fmt.Printf("== 0. 从 %s 恢复：快照覆盖到 seq=%d，重放终局 %d 条，回滚中断请求 %d 条，补齐审计 %d 条 ==\n",
				*dataDir, rep.SnapshotSeq, rep.Replayed, len(rep.RolledBack), rep.AuditPatched)
		} else {
			fmt.Printf("== 0. 从 %s 首次启动（无快照）：重放终局 %d 条，回滚中断请求 %d 条，补齐审计 %d 条 ==\n",
				*dataDir, rep.Replayed, len(rep.RolledBack), rep.AuditPatched)
		}
		for _, rb := range rep.RolledBack {
			fmt.Printf("    回滚: %s (%s)\n", rb.RequestID, rb.TenantID)
		}
	} else {
		gw = gateway.New(auth.NewAuthenticator(store, clk), newLimiter(), clk,
			func(ctx context.Context, req gateway.Request) error { return nil })
	}
	gw.Logger = logger
	defer gw.Close()

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
}
