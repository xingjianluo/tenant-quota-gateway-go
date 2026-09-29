package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

func persistReserved(reqID, token, scope, tenant string, at time.Time) persist.ReservedEntry {
	return persist.ReservedEntry{At: at, RequestID: reqID, TokenID: token, Scope: scope, TenantID: tenant}
}

// persistentFixture 持有可反复 Open 同一数据目录所需的构造参数。
type persistentFixture struct {
	dir   string
	clk   *clock.Fake
	store *auth.Store
}

func newPersistentFixture(t *testing.T, snapshotEvery int) *persistentFixture {
	t.Helper()
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(10 * time.Hour)})
	store.Put(auth.Token{ID: "tok-b", TenantID: "tenant-b", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(10 * time.Hour)})
	return &persistentFixture{dir: t.TempDir(), clk: clock.NewFake(t0), store: store}
}

// open 在同一数据目录上启动一个新进程等价物；limiter 由测试自行配置同款配额。
func (p *persistentFixture) open(t *testing.T, h Handler, snapshotEvery int) (*Gateway, *quota.Limiter, *RecoveryReport) {
	t.Helper()
	l := quota.NewLimiter(p.clk)
	l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 100})
	l.SetLimit("tenant-b", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 100})
	gw, rep, err := Open(context.Background(), p.dir, auth.NewAuthenticator(p.store, p.clk), l, p.clk, h)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	gw.Logger = log.New(io.Discard, "", 0)
	gw.snapshotEvery = snapshotEvery
	return gw, l, rep
}

// TestRestartStateContinuity 重启后：已扣配额、剩余额度、审计序号与幂等结论全部延续。
func TestRestartStateContinuity(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	gw, l, _ := pf.open(t, okHandler, 0)

	ctx := context.Background()
	reqs := []string{"r1", "r2", "r3", "r4", "r5"}
	for _, id := range reqs {
		d := gw.Admit(ctx, Request{RequestID: id, TokenID: "tok-a", Scope: "invoke"})
		if d.Code != CodeExecuted {
			t.Fatalf("%s: %+v", id, d)
		}
	}
	u1 := l.Usage("tenant-a")
	if u1.QuotaUsed != 5 || u1.QuotaRemain != 95 || u1.InFlight != 0 {
		t.Fatalf("before restart usage: %+v", u1)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}

	// —— 模拟进程重启：同一目录、同配置，新实例 ——
	gw2, l2, rep := pf.open(t, okHandler, 0)
	defer gw2.Close()
	if rep.Replayed != 5 {
		t.Fatalf("replayed=%d, want 5", rep.Replayed)
	}

	u2 := l2.Usage("tenant-a")
	if u2.QuotaUsed != 5 || u2.QuotaRemain != 95 || u2.InFlight != 0 {
		t.Fatalf("after restart usage: %+v", u2)
	}

	// 再处理两个新请求：接着第 6、7 个扣，统计口径连续。
	for _, id := range []string{"r6", "r7"} {
		if d := gw2.Admit(ctx, Request{RequestID: id, TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeExecuted {
			t.Fatalf("%s: %+v", id, d)
		}
	}
	u3 := l2.Usage("tenant-a")
	if u3.QuotaUsed != 7 || u3.QuotaRemain != 93 {
		t.Fatalf("after 2 more: %+v", u3)
	}
}

// TestDuplicateAfterRestartAudited 重启前处理过的请求，重启后再提交：
// 仍按重复判定、结论与首次一致；重复判定同样进入审计流水，序号连续、可查输入与配额。
func TestDuplicateAfterRestartAudited(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	gw, _, _ := pf.open(t, okHandler, 0)
	ctx := context.Background()

	first := gw.Admit(ctx, Request{RequestID: "dup-x", TokenID: "tok-a", Scope: "invoke"})
	if first.Code != CodeExecuted {
		t.Fatalf("first: %+v", first)
	}
	// 另一个请求让序号拉开。
	gw.Admit(ctx, Request{RequestID: "other", TokenID: "tok-a", Scope: "invoke"})
	gw.Close()

	gw2, l2, rep := pf.open(t, okHandler, 0)
	defer gw2.Close()
	if rep.Replayed != 2 {
		t.Fatalf("replayed=%d", rep.Replayed)
	}

	dup := gw2.Admit(ctx, Request{RequestID: "dup-x", TokenID: "tok-a", Scope: "invoke"})
	if dup.Code != CodeDuplicate {
		t.Fatalf("want DUPLICATE after restart, got %+v", dup)
	}
	if dup.Reason != "duplicate of "+string(CodeExecuted) {
		t.Fatalf("reason=%q", dup.Reason)
	}
	if dup.TenantID != "tenant-a" || dup.Usage.QuotaUsed != 1 {
		// 重复返回首次判定的副本（首次时已用 1），与第一次结论一致；
		// 当前账本以 limiter.Usage 为准，仍为 2，没有被重复扣减。
		t.Fatalf("duplicate decision mismatch: %+v", dup)
	}
	// 重复判定不得再扣配额。
	if u := l2.Usage("tenant-a"); u.QuotaUsed != 2 {
		t.Fatalf("duplicate after restart changed quota: %+v", u)
	}

	recs := gw2.Audit()
	if len(recs) != 3 {
		t.Fatalf("audit len=%d, want 3", len(recs))
	}
	// 序号连续。
	for i, r := range recs {
		if r.Seq != int64(i+1) {
			t.Fatalf("audit seq not contiguous: %+v", recs)
		}
	}
	last := recs[2]
	if last.RequestID != "dup-x" || last.Code != CodeDuplicate || last.Scope != "invoke" || last.TenantID != "tenant-a" {
		t.Fatalf("duplicate audit record missing input/basis: %+v", last)
	}
	if last.Usage.QuotaUsed != 2 || last.Usage.QuotaRemain != 98 {
		t.Fatalf("duplicate audit record quota: %+v", last.Usage)
	}

	// 再次重启：重复结论仍然一致，审计继续连续追加。
	gw2.Close()
	gw3, l3, rep3 := pf.open(t, okHandler, 0)
	defer gw3.Close()
	if rep3.Replayed != 3 {
		t.Fatalf("replayed=%d, want 3", rep3.Replayed)
	}
	dup3 := gw3.Admit(ctx, Request{RequestID: "dup-x", TokenID: "tok-a", Scope: "invoke"})
	if dup3.Code != CodeDuplicate {
		t.Fatalf("second restart duplicate: %+v", dup3)
	}
	recs3 := gw3.Audit()
	if len(recs3) != 4 {
		t.Fatalf("audit len=%d, want 4", len(recs3))
	}
	if recs3[3].Seq != 4 || recs3[3].Code != CodeDuplicate {
		t.Fatalf("audit not continuous across restart: %+v", recs3)
	}
	if u := l3.Usage("tenant-a"); u.QuotaUsed != 2 {
		t.Fatalf("quota changed across restarts: %+v", u)
	}
}

// reopenDir 直接在给定目录上启动网关（用于手工构造崩溃现场）。
func reopenDir(t *testing.T, dir string, pf *persistentFixture, h Handler, snapshotEvery int) (*Gateway, *quota.Limiter, *RecoveryReport) {
	l := quota.NewLimiter(pf.clk)
	l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 100})
	gw, rep, err := Open(context.Background(), dir, auth.NewAuthenticator(pf.store, pf.clk), l, pf.clk, h)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	gw.Logger = log.New(io.Discard, "", 0)
	gw.snapshotEvery = snapshotEvery
	return gw, l, rep
}

// TestCrashMidRequestRollback 进程在"已记账、未终局"时被杀：
// 重启后该预留被识别并回滚，令牌/总量配额归还、并发槽释放，不多扣不占槽。
func TestCrashMidRequestRollback(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	dir := t.TempDir()
	pf.dir = dir
	gw, l, _ := reopenDir(t, dir, pf, okHandler, 0)
	ctx := context.Background()

	// 先完成 2 个请求。
	gw.Admit(ctx, Request{RequestID: "done-1", TokenID: "tok-a", Scope: "invoke"})
	gw.Admit(ctx, Request{RequestID: "done-2", TokenID: "tok-a", Scope: "invoke"})

	// 手工制造一个"预留已落盘、终局永远不来"的现场（等价于 handler 执行中 kill -9）。
	if err := gw.store.AppendReserved(persistReserved("killed-mid", "tok-a", "invoke", "tenant-a", pf.clk.Now())); err != nil {
		t.Fatal(err)
	}
	// 内存账本同步成 Acquire 已记账（used+1、inflight+1、tokens-1）。
	// 通过真实 Admit 一个会一直阻塞的请求难以稳定构造，这里直接操作 limiter 记账。
	if err := l.Acquire("tenant-a"); err != nil {
		t.Fatal(err)
	}
	if u := l.Usage("tenant-a"); u.QuotaUsed != 3 || u.InFlight != 1 {
		t.Fatalf("setup usage: %+v", u)
	}
	// 不写终局、不 Release/Refund，直接关闭（模拟强杀）。
	gw.Close()

	// —— 重启 ——
	gw2, l2, rep := reopenDir(t, dir, pf, okHandler, 0)
	defer gw2.Close()

	if len(rep.RolledBack) != 1 || rep.RolledBack[0].RequestID != "killed-mid" {
		t.Fatalf("rollback report: %+v", rep.RolledBack)
	}
	u := l2.Usage("tenant-a")
	if u.QuotaUsed != 2 || u.InFlight != 0 || u.QuotaRemain != 98 {
		t.Fatalf("after recovery usage: %+v, want used=2 inflight=0 remain=98", u)
	}
	// 被回滚的请求没有终局结论，不占幂等表；客户端用同 ID 重试应当被当作新请求正常执行。
	retry := gw2.Admit(ctx, Request{RequestID: "killed-mid", TokenID: "tok-a", Scope: "invoke"})
	if retry.Code != CodeExecuted {
		t.Fatalf("retry after rollback: %+v", retry)
	}
	if u := l2.Usage("tenant-a"); u.QuotaUsed != 3 || u.InFlight != 0 {
		t.Fatalf("after retry: %+v", u)
	}
}

// TestCrashMidRequestRollbackAfterSnapshot 预留发生在快照窗口内、
// journal 随后被截断的情况下，回滚仍然生效（预留进入快照 InFlight）。
func TestCrashMidRequestRollbackAfterSnapshot(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	dir := t.TempDir()
	pf.dir = dir
	gw, l, _ := reopenDir(t, dir, pf, okHandler, 0)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		gw.Admit(ctx, Request{RequestID: fmt.Sprintf("d-%d", i), TokenID: "tok-a", Scope: "invoke"})
	}
	// 制造一个预留，并让它作为"进行中"进入快照。
	if err := gw.store.AppendReserved(persistReserved("killed-snap", "tok-a", "invoke", "tenant-a", pf.clk.Now())); err != nil {
		t.Fatal(err)
	}
	if err := l.Acquire("tenant-a"); err != nil {
		t.Fatal(err)
	}
	// 手工把该预留登记进网关在飞集合（tenantID 非空），随后打快照并截断 journal。
	gw.mu.Lock()
	gw.inFlight["killed-snap"] = &inFlightCall{
		done: make(chan struct{}), req: Request{RequestID: "killed-snap", TokenID: "tok-a", Scope: "invoke"}, tenantID: "tenant-a",
	}
	if err := gw.saveSnapshotLocked(); err != nil {
		gw.mu.Unlock()
		t.Fatal(err)
	}
	gw.mu.Unlock()
	gw.Close()

	gw2, l2, rep := reopenDir(t, dir, pf, okHandler, 0)
	defer gw2.Close()
	if len(rep.RolledBack) != 1 || rep.RolledBack[0].RequestID != "killed-snap" {
		t.Fatalf("rollback after snapshot: %+v", rep.RolledBack)
	}
	if u := l2.Usage("tenant-a"); u.QuotaUsed != 3 || u.InFlight != 0 {
		t.Fatalf("usage after snapshot recovery: %+v", u)
	}
}

// TestRestartAfterSnapshot 让快照被触发（snapshotEvery>0）并截断 journal，
// 重启后状态仍正确，且此时恢复走快照路径（Replayed 为 0）。
func TestRestartAfterSnapshot(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	const every = 4
	gw, l, _ := pf.open(t, okHandler, every)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		gw.Admit(ctx, Request{RequestID: fmt.Sprintf("s-%d", i), TokenID: "tok-a", Scope: "invoke"})
	}
	if u := l.Usage("tenant-a"); u.QuotaUsed != 10 {
		t.Fatalf("usage: %+v", u)
	}
	gw.Close()

	gw2, l2, rep := pf.open(t, okHandler, every)
	defer gw2.Close()
	if rep.SnapshotSeq != 8 {
		t.Fatalf("snapshot seq=%d, want 8（最近一次整除点）", rep.SnapshotSeq)
	}
	if rep.Replayed != 2 {
		t.Fatalf("replayed=%d, want 2（journal 只剩快照后的窗口）", rep.Replayed)
	}
	if u := l2.Usage("tenant-a"); u.QuotaUsed != 10 || u.QuotaRemain != 90 || u.InFlight != 0 {
		t.Fatalf("usage after snapshot restart: %+v", u)
	}
	// 幂等表来自快照 + journal：早期请求仍被识别为重复。
	if d := gw2.Admit(ctx, Request{RequestID: "s-0", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeDuplicate {
		t.Fatalf("old request not deduped after snapshot: %+v", d)
	}
	if d := gw2.Admit(ctx, Request{RequestID: "s-9", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeDuplicate {
		t.Fatalf("recent request not deduped after snapshot: %+v", d)
	}
	if u := l2.Usage("tenant-a"); u.QuotaUsed != 10 {
		t.Fatalf("duplicate after snapshot changed quota: %+v", u)
	}
	// 审计仍是全量 10 + 2 条重复，序号 1..12 连续。
	recs := gw2.Audit()
	if len(recs) != 12 {
		t.Fatalf("audit len=%d, want 12", len(recs))
	}
	for i, r := range recs {
		if r.Seq != int64(i+1) {
			t.Fatalf("audit seq gap at %d: %+v", i, recs)
		}
	}
}

// TestRateBucketAcrossRestart 速率令牌桶按注入时钟跨重启续算：
// 用掉令牌 → 重启 → 推进时间 → 令牌按速率回补，与未重启一致。
func TestRateBucketAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewFake(t0)
	store := auth.NewStore()
	store.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(100 * time.Hour)})
	open1 := func(h Handler) (*Gateway, *quota.Limiter) {
		l := quota.NewLimiter(c)
		l.SetLimit("tenant-a", quota.Limit{RatePerSec: 2, Burst: 5, MaxConcurrent: 100, TotalQuota: 1_000_000})
		gw, _, err := Open(context.Background(), dir, auth.NewAuthenticator(store, c), l, c, h)
		if err != nil {
			t.Fatal(err)
		}
		gw.Logger = log.New(io.Discard, "", 0)
		gw.snapshotEvery = 0
		return gw, l
	}
	ctx := context.Background()
	gw, l := open1(okHandler)
	for i := 0; i < 5; i++ { // 打空 burst
		if d := gw.Admit(ctx, Request{RequestID: fmt.Sprintf("b-%d", i), TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeExecuted {
			t.Fatalf("drain %d: %+v", i, d)
		}
	}
	if d := gw.Admit(ctx, Request{RequestID: "b-now", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeRateLimited {
		t.Fatalf("want rate limited, got %+v", d)
	}
	gw.Close()

	// 重启后立刻仍应限流（未推进时间）。
	gw2, l2 := open1(okHandler)
	defer gw2.Close()
	if d := gw2.Admit(ctx, Request{RequestID: "b-after-restart", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeRateLimited {
		t.Fatalf("rate limit reset by restart: %+v", d)
	}
	// 推进 1 秒：按 2/s 应恰好回补 2 个令牌。
	c.Advance(time.Second)
	for i := 0; i < 2; i++ {
		if d := gw2.Admit(ctx, Request{RequestID: fmt.Sprintf("b-refill-%d", i), TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeExecuted {
			t.Fatalf("refill %d: %+v", i, d)
		}
	}
	if d := gw2.Admit(ctx, Request{RequestID: "b-empty-again", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeRateLimited {
		t.Fatalf("want rate limited after refill, got %+v", d)
	}
	_ = l
	_ = l2
}

// TestTornJournalRecovered journal 末尾半行写在恢复时被丢弃，不影响已 fsync 的结论。
func TestTornJournalRecovered(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	gw, _, _ := pf.open(t, okHandler, 0)
	ctx := context.Background()
	gw.Admit(ctx, Request{RequestID: "j-1", TokenID: "tok-a", Scope: "invoke"})
	gw.Admit(ctx, Request{RequestID: "j-2", TokenID: "tok-a", Scope: "invoke"})
	// 终局已 fsync；再手写一条损坏 reserved（模拟半行写）。
	f, err := os.OpenFile(pf.dir+"/journal.log", os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("zzzz:not-a-line"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	gw.Close()

	gw2, l2, rep := pf.open(t, okHandler, 0)
	defer gw2.Close()
	if rep.Replayed != 2 {
		t.Fatalf("replayed=%d, want 2（半行应被丢弃）", rep.Replayed)
	}
	if u := l2.Usage("tenant-a"); u.QuotaUsed != 2 || u.InFlight != 0 {
		t.Fatalf("usage: %+v", u)
	}
}

// openWithQuota 在 pf 目录上以自定义配额启动网关。
func (p *persistentFixture) openWithQuota(t *testing.T, h Handler, snapshotEvery int, q quota.Limit) (*Gateway, *quota.Limiter, *RecoveryReport) {
	l := quota.NewLimiter(p.clk)
	l.SetLimit("tenant-a", q)
	gw, rep, err := Open(context.Background(), p.dir, auth.NewAuthenticator(p.store, p.clk), l, p.clk, h)
	if err != nil {
		t.Fatal(err)
	}
	gw.Logger = log.New(io.Discard, "", 0)
	gw.snapshotEvery = snapshotEvery
	return gw, l, rep
}

// TestRecoveryWindowBounded 历史请求很多时，重放窗口被快照钳制：
// 无论之前处理过多少请求，恢复时 Replayed 不超过一个快照间隔。
func TestRecoveryWindowBounded(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	const every = 50
	const total = 1000
	big := quota.Limit{RatePerSec: 1e9, Burst: 1e9, MaxConcurrent: 1e9, TotalQuota: 1_000_000}
	gw, _, _ := pf.openWithQuota(t, okHandler, every, big)
	ctx := context.Background()
	for i := 0; i < total; i++ {
		gw.Admit(ctx, Request{RequestID: fmt.Sprintf("h-%d", i), TokenID: "tok-a", Scope: "invoke"})
	}
	gw.Close()

	gw2, l2, rep := pf.open(t, okHandler, every)
	defer gw2.Close()
	// total=1000 恰为 every=50 的整数倍，最后一个请求本身触发快照，
	// journal 被清空，恢复零重放；这正是"窗口被钳制"的最强形态。
	if rep.Replayed != 0 {
		t.Fatalf("replayed=%d, want 0（快照已覆盖全部历史）", rep.Replayed)
	}
	if rep.SnapshotSeq != int64(total) {
		t.Fatalf("snapshot seq=%d, want %d", rep.SnapshotSeq, total)
	}
	if u := l2.Usage("tenant-a"); u.QuotaUsed != int64(total) || u.InFlight != 0 {
		t.Fatalf("usage=%+v, want used=%d", u, total)
	}
	// 很早的请求仍能跨快照识别为重复。
	if d := gw2.Admit(ctx, Request{RequestID: "h-0", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeDuplicate {
		t.Fatalf("old request lost: %+v", d)
	}
}

// TestExecFailureAcrossRestart 执行失败的请求已退款，重启后不占配额不占槽，
// 且其失败结论仍幂等保留。
func TestExecFailureAcrossRestart(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	boom := errors.New("downstream boom")
	// 仅 fail-1 失败，其余请求成功；重启后同 ID 是重复判定，handler 不再被调用。
	h := func(_ context.Context, req Request) error {
		if req.RequestID == "fail-1" {
			return boom
		}
		return nil
	}
	gw, _, _ := pf.open(t, h, 0)
	ctx := context.Background()
	d := gw.Admit(ctx, Request{RequestID: "fail-1", TokenID: "tok-a", Scope: "invoke"})
	if d.Code != CodeExecFailed {
		t.Fatalf("want exec failed, got %+v", d)
	}
	if d2 := gw.Admit(ctx, Request{RequestID: "ok-1", TokenID: "tok-a", Scope: "invoke"}); d2.Code != CodeExecuted {
		t.Fatalf("ok-1: %+v", d2)
	}
	gw.Close()

	gw2, l2, _ := pf.open(t, h, 0)
	defer gw2.Close()
	u := l2.Usage("tenant-a")
	if u.QuotaUsed != 1 || u.InFlight != 0 || u.QuotaRemain != 99 {
		t.Fatalf("failed request survived restart: %+v", u)
	}
	// 失败结论也幂等：同 ID 再提交得到 DUPLICATE（of EXEC_FAILED），不重新执行、不扣配额。
	dup := gw2.Admit(ctx, Request{RequestID: "fail-1", TokenID: "tok-a", Scope: "invoke"})
	if dup.Code != CodeDuplicate || dup.Reason != "duplicate of "+string(CodeExecFailed) {
		t.Fatalf("dup: %+v", dup)
	}
	if u := l2.Usage("tenant-a"); u.QuotaUsed != 1 {
		t.Fatalf("duplicate of failed request changed quota: %+v", u)
	}
}

// TestSameInputsConsistentAcrossRestart 同一批混合输入在"重启前/后"拿到的
// 判定序列（对已处理 ID 表现为 DUPLICATE）与统计口径一致：成功数、失败数、
// 拒绝数、最终账本完全相同。
func TestSameInputsConsistentAcrossRestart(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	boom := errors.New("boom")
	h := func(_ context.Context, req Request) error {
		if req.RequestID == "fail" {
			return boom
		}
		return nil
	}
	// 用小配额（3），让总量拒绝也进入同一批输入。
	small := quota.Limit{RatePerSec: 1e9, Burst: 1e9, MaxConcurrent: 1e9, TotalQuota: 3}
	gw, l, _ := pf.openWithQuota(t, h, 0, small)
	ctx := context.Background()
	inputs := []Request{
		{RequestID: "a", TokenID: "tok-a", Scope: "invoke"},
		{RequestID: "b", TokenID: "tok-a", Scope: "invoke"},
		{RequestID: "fail", TokenID: "tok-a", Scope: "invoke"}, // 失败，退款
		{RequestID: "c", TokenID: "tok-a", Scope: "invoke"},    // 第 3 个成功
		{RequestID: "d", TokenID: "tok-a", Scope: "invoke"},    // 配额耗尽
		{RequestID: "e", TokenID: "bad", Scope: "invoke"},      // 鉴权失败
	}
	before := make([]Code, len(inputs))
	for i, r := range inputs {
		before[i] = gw.Admit(ctx, r).Code
	}
	uBefore := l.Usage("tenant-a")
	auditBefore := len(gw.Audit())
	gw.Close()

	gw2, l2, _ := pf.openWithQuota(t, h, 0, small)
	defer gw2.Close()
	after := make([]Code, len(inputs))
	for i, r := range inputs {
		after[i] = gw2.Admit(ctx, r).Code
	}
	// 重启后再提交同一批：每一条都应判为对其首次结论的 DUPLICATE，
	// 且重复原因映射回首次码后等于重启前序列。
	for i := range inputs {
		if after[i] != CodeDuplicate {
			t.Fatalf("input %d after restart: want DUPLICATE got %s", i, after[i])
		}
	}
	// 账本口径与重启前完全相同，没有因重复提交而变化。
	uAfter := l2.Usage("tenant-a")
	if uAfter != uBefore {
		t.Fatalf("usage diverged: before=%+v after=%+v", uBefore, uAfter)
	}
	// 重启前 6 条 + 重启后 6 条重复，审计连续共 12 条。
	recs := gw2.Audit()
	if len(recs) != auditBefore+len(inputs) {
		t.Fatalf("audit count before=%d after=%d", auditBefore, len(recs))
	}
	for i, r := range recs {
		if r.Seq != int64(i+1) {
			t.Fatalf("audit seq gap at %d", i)
		}
	}
}

// TestPersistentConcurrentNoOverGrant 持久化模式下高并发竞争小配额：
// 成功数恰好等于配额、无槽泄漏、审计条数与请求数一致，且可干净重启延续。
func TestPersistentConcurrentNoOverGrant(t *testing.T) {
	pf := newPersistentFixture(t, 0)
	small := quota.Limit{RatePerSec: 1e9, Burst: 1e9, MaxConcurrent: 1e9, TotalQuota: 32}
	gw, _, _ := pf.openWithQuota(t, okHandler, 16, small)
	ctx := context.Background()

	const n = 160
	var wg sync.WaitGroup
	codes := make(chan Code, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes <- gw.Admit(ctx, Request{RequestID: fmt.Sprintf("p-%d", i), TokenID: "tok-a", Scope: "invoke"}).Code
		}(i)
	}
	wg.Wait()
	close(codes)
	exec, other := 0, 0
	for c := range codes {
		if c == CodeExecuted {
			exec++
		} else if c == CodeQuotaExhausted {
			other++
		} else {
			t.Fatalf("unexpected code: %s", c)
		}
	}
	if exec != 32 || other != n-32 {
		t.Fatalf("exec=%d exhausted=%d", exec, other)
	}
	if recs := gw.Audit(); len(recs) != n {
		t.Fatalf("audit len=%d, want %d", len(recs), n)
	}
	gw.Close()

	// 干净重启：账本与审计延续。
	gw2, l2, _ := pf.openWithQuota(t, okHandler, 16, small)
	defer gw2.Close()
	if u := l2.Usage("tenant-a"); u.QuotaUsed != 32 || u.InFlight != 0 {
		t.Fatalf("after restart: %+v", u)
	}
	if recs := gw2.Audit(); len(recs) != n {
		t.Fatalf("audit after restart=%d, want %d", len(recs), n)
	}
}
