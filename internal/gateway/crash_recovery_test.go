package gateway

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// checkpoint 在在途请求尚未了结时手动打一次检查点（与周期触发同一代码路径），
// 用于构造"崩溃前已经落过状态检查点"的场景。
func (e *crashEnv) checkpoint() {
	e.gw.mu.Lock()
	e.gw.checkpointLocked()
	e.gw.mu.Unlock()
}

// snapshotOf 直接读出当前已落盘的快照（测试 setup 校验用）。
func (e *crashEnv) snapshotOf(t *testing.T) *persist.Snapshot {
	t.Helper()
	snap, err := persist.NewFileStore(filepath.Join(e.dir, "state"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	s, err := snap.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	return s
}

// TestCrashAfterCheckpointSettlesInFlight 有请求在途时落过检查点、随后被强杀：
// 重启后在途占用一律按未发生处理——释放并发槽、退回预扣配额与速率令牌；
// 检查点之后没有任何新事件（刚打完检查点就崩）也必须是同一结论。
func TestCrashAfterCheckpointSettlesInFlight(t *testing.T) {
	env := newCrashEnv(t)

	// 1 个已完成提交（占用 1 配额、1 令牌，不退）。
	if d := env.admit("done-1"); d.Code != CodeExecuted {
		t.Fatalf("done-1: %+v", d)
	}
	// 2 个卡在下游的在途请求（各占 1 槽 + 预扣 1 配额 + 1 令牌）。
	env.hang["hang-1"] = true
	env.hang["hang-2"] = true
	go func() { _ = env.admit("hang-1") }()
	go func() { _ = env.admit("hang-2") }()
	<-env.entered
	<-env.entered

	// 在途期间落检查点，随后立刻崩溃（检查点后无任何新事件）。
	env.checkpoint()
	snap := env.snapshotOf(t)
	if st := snap.Tenants["tenant-a"]; st.InFlight != 2 || st.Used != 3 {
		t.Fatalf("setup: snapshot should carry in-flight occupation, got %+v", st)
	}
	env.kill()

	u := env.limiter.Usage("tenant-a")
	t.Logf("after recovery: %+v", u)
	if u.InFlight != 0 {
		t.Fatalf("in-flight slot permanently held after checkpoint+crash: %+v", u)
	}
	if u.QuotaUsed != 1 || u.QuotaRemain != 4 {
		t.Fatalf("in-flight quota not refunded after checkpoint+crash: %+v", u)
	}
	// 速率令牌同样退回：1000 初始 - 3 次 Acquire + 2 次回滚 = 999。
	if u.Tokens != 999 {
		t.Fatalf("rate tokens not refunded after checkpoint+crash: %+v", u)
	}

	// 未提交的两个请求按新请求处理，必须能正常执行（槽位与配额确已归还）。
	if d := env.admit("hang-1"); d.Code != CodeExecuted {
		t.Fatalf("uncommitted request should execute anew, got %+v", d)
	}
	if d := env.admit("hang-2"); d.Code != CodeExecuted {
		t.Fatalf("uncommitted request should execute anew, got %+v", d)
	}
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 3 {
		t.Fatalf("after retry usage=%+v, want used=3", u)
	}
}

// TestCrashCheckpointMixedCommittedAndInFlight 同一次崩溃里既有已完成请求、
// 又有还卡在执行中的请求，且跨越一次检查点：恢复后的用量必须严格等于
// "只有已完成的那部分生效"，两类互不串账。
func TestCrashCheckpointMixedCommittedAndInFlight(t *testing.T) {
	env := newCrashEnv(t)

	// 检查点之前：1 个已完成 + 1 个在途。
	if d := env.admit("done-1"); d.Code != CodeExecuted {
		t.Fatalf("done-1: %+v", d)
	}
	env.hang["hang-1"] = true
	go func() { _ = env.admit("hang-1") }()
	<-env.entered
	env.checkpoint() // 快照 {InFlight:1, Used:2}

	// 检查点之后：再完成 1 个（提交事件进日志），再来 1 个在途，然后崩溃。
	if d := env.admit("done-2"); d.Code != CodeExecuted {
		t.Fatalf("done-2: %+v", d)
	}
	env.hang["hang-2"] = true
	go func() { _ = env.admit("hang-2") }()
	<-env.entered
	if u := env.limiter.Usage("tenant-a"); u.InFlight != 2 || u.QuotaUsed != 4 {
		t.Fatalf("pre-crash state wrong: %+v", u)
	}
	env.kill()

	// 严格等于只有 done-1、done-2 生效：used=2，两个在途全部回滚。
	u := env.limiter.Usage("tenant-a")
	t.Logf("after recovery: %+v", u)
	if u.InFlight != 0 || u.QuotaUsed != 2 || u.QuotaRemain != 3 {
		t.Fatalf("mixed committed/in-flight not reconciled: %+v", u)
	}

	// 已完成的两个请求结论保留：重提判定 DUPLICATE，不重复扣减。
	if d := env.admit("done-1"); d.Code != CodeDuplicate {
		t.Fatalf("done-1 after crash: %+v", d)
	}
	if d := env.admit("done-2"); d.Code != CodeDuplicate {
		t.Fatalf("done-2 after crash: %+v", d)
	}
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 2 {
		t.Fatalf("committed conclusions double charged: %+v", u)
	}
	// 在途的两个请求按未发生处理：可作为新请求执行。
	if d := env.admit("hang-1"); d.Code != CodeExecuted {
		t.Fatalf("hang-1 retry: %+v", d)
	}
	if d := env.admit("hang-2"); d.Code != CodeExecuted {
		t.Fatalf("hang-2 retry: %+v", d)
	}
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 4 {
		t.Fatalf("after retries usage=%+v, want used=4", u)
	}
}

// TestCrashSettlementAppliedExactlyOnce 崩溃在途回滚只生效一次：
// 反复重启（含恢复后再次崩溃）结论都一致，不出现重复退款。
func TestCrashSettlementAppliedExactlyOnce(t *testing.T) {
	env := newCrashEnv(t)

	if d := env.admit("done-1"); d.Code != CodeExecuted {
		t.Fatalf("done-1: %+v", d)
	}
	env.hang["hang-1"] = true
	env.hang["hang-2"] = true
	go func() { _ = env.admit("hang-1") }()
	go func() { _ = env.admit("hang-2") }()
	<-env.entered
	<-env.entered
	env.checkpoint()
	env.kill() // 第一次恢复：回滚 2 份在途占用并落盘

	for i := 0; i < 3; i++ {
		u := env.limiter.Usage("tenant-a")
		t.Logf("restart #%d: %+v", i+1, u)
		if u.InFlight != 0 || u.QuotaUsed != 1 || u.QuotaRemain != 4 {
			t.Fatalf("restart #%d rolled back again (double refund): %+v", i+1, u)
		}
		env.restartPlain()
	}

	// 恢复后继续服务：新请求正常记账，再重启依旧一致。
	if d := env.admit("after-1"); d.Code != CodeExecuted {
		t.Fatalf("after-1: %+v", d)
	}
	env.restartPlain()
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 2 || u.InFlight != 0 {
		t.Fatalf("post-recovery usage not continued: %+v", u)
	}
}

// restartPlain 模拟一次普通重启（无在途、无冻结写），复用同一状态目录。
func (e *crashEnv) restartPlain() { e.boot(false) }

// TestExecFailureReasonPreservesDownstreamError 执行失败时，下游真实错误原样进入
// 判定依据与审计流水，且跨重启恢复后依然可见（不是一句笼统套话）。
func TestExecFailureReasonPreservesDownstreamError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	boom := errors.New("billing-svc: connection refused (10.0.0.7:8443)")
	boot := func() *Gateway {
		c := clock.NewFake(t0)
		store, err := persist.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		l := quota.NewLimiter(c)
		l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 100})
		s := auth.NewStore()
		s.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(24 * time.Hour)})
		gw := New(auth.NewAuthenticator(s, c), l, c,
			func(context.Context, Request) error { return boom })
		gw.Logger = log.New(io.Discard, "", 0)
		if err := gw.AttachStore(store, 4096, 256); err != nil {
			t.Fatal(err)
		}
		return gw
	}
	req := Request{RequestID: "fail-1", TokenID: "tok-a", Scope: "invoke"}

	gw1 := boot()
	d := gw1.Admit(context.Background(), req)
	if d.Code != CodeExecFailed {
		t.Fatalf("want exec failed, got %+v", d)
	}
	if !strings.Contains(d.Reason, boom.Error()) {
		t.Fatalf("decision reason lost downstream error: %q", d.Reason)
	}
	recs := gw1.Audit()
	if len(recs) != 1 || !strings.Contains(recs[0].Reason, boom.Error()) {
		t.Fatalf("audit reason lost downstream error: %+v", recs)
	}
	t.Logf("failure basis recorded: %q", d.Reason)

	// 重启后：首次判定依据随审计与幂等结论一起恢复，仍然能看到下游真实错误。
	gw2 := boot()
	found := false
	for _, r := range gw2.Audit() {
		if r.RequestID == "fail-1" && r.Kind == persist.EventCommit && strings.Contains(r.Reason, boom.Error()) {
			found = true
		}
	}
	if !found {
		t.Fatalf("downstream error not preserved across restart: %+v", gw2.Audit())
	}
	dup := gw2.Admit(context.Background(), req)
	if dup.Code != CodeDuplicate {
		t.Fatalf("failed request should be duplicate after restart, got %+v", dup)
	}
}
