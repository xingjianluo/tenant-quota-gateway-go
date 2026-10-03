package gateway

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// checkpoint 强制当前（未冻结的）进程立即打一次检查点：快照落盘并截断日志。
// 在途请求正卡在下游、不会触碰 g.mu，因此快照里会带上它们的在途占用。
func (e *crashEnv) checkpoint() {
	e.gw.mu.Lock()
	e.gw.checkpointLocked()
	e.gw.mu.Unlock()
}

// TestCrashWithInFlightAtCheckpointRollsBack 核心链路：
// 请求在途（已预扣配额/占槽）→ 打检查点（快照含在途占用）→ 强杀 → 重启。
// 重启后在途占用必须按"未发生"回滚：槽位释放、预扣配额与速率令牌归还；
// 检查点之前已提交的成功扣减原样保留。
func TestCrashWithInFlightAtCheckpointRollsBack(t *testing.T) {
	env := newCrashEnv(t)

	// 1 个已完成并提交（占用 1 配额）。
	if d := env.admit("done-1"); d.Code != CodeExecuted {
		t.Fatalf("done-1: %+v", d)
	}
	// 1 个卡在下游的在途请求（占 1 槽 + 预扣 1 配额 + 1 令牌）。
	env.hang["hang-1"] = true
	go func() { _ = env.admit("hang-1") }()
	<-env.entered

	// 在途期间打检查点：快照里 tenant-a 为 inFlight=1, used=2, tokens=999。
	env.checkpoint()

	env.kill()
	u := env.limiter.Usage("tenant-a")
	t.Logf("在途请求经检查点后崩溃，恢复结果: %+v", u)
	if u.InFlight != 0 {
		t.Fatalf("in-flight slot leaked across checkpointed crash: %+v", u)
	}
	if u.QuotaUsed != 1 || u.QuotaRemain != 4 {
		t.Fatalf("in-flight pre-deduction not refunded (or committed usage rolled back): %+v", u)
	}
	// 令牌账目：done-1 成功消耗 1 枚不退；hang-1 预扣的 1 枚必须退回 → 999。
	if u.Tokens != 999 {
		t.Fatalf("in-flight rate token not refunded: %+v", u)
	}

	// 未提交的 hang-1 按新请求重试，必须能正常执行并只扣一份。
	if d := env.admit("hang-1"); d.Code != CodeExecuted {
		t.Fatalf("uncommitted request should execute anew, got %+v", d)
	}
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 2 {
		t.Fatalf("after retry usage=%+v, want used=2", u)
	}
}

// TestCrashInFlightSlotReusableAfterRecovery 并发槽上限 1：在途请求占满唯一的槽，
// 此时新请求确实被并发限制挡住；检查点后强杀重启，槽位必须被释放——
// 同样的新请求恢复后必须放行，而不是继续被"幽灵占用"挡住。
func TestCrashInFlightSlotReusableAfterRecovery(t *testing.T) {
	env := newCrashEnvWithLimit(t, quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 1, TotalQuota: 10})

	env.hang["hang-1"] = true
	go func() { _ = env.admit("hang-1") }()
	<-env.entered

	// 唯一的并发槽被在途请求占住：新请求被并发限制拒绝（拒绝本身也是已提交结论）。
	if d := env.admit("blocked-1"); d.Code != CodeConcurrencyLimited {
		t.Fatalf("want concurrency limited while slot held, got %+v", d)
	}

	// 在途期间打检查点，随后强杀。
	env.checkpoint()
	env.kill()

	// 重启后槽位已归还：hang-1 重试必须能拿到槽执行。
	d := env.admit("hang-1")
	t.Logf("唯一并发槽在检查点+崩溃后恢复，重试判定: %s (%s)", d.Code, d.Reason)
	if d.Code != CodeExecuted {
		t.Fatalf("slot still occupied by crashed in-flight request: %+v", d)
	}
	// 崩溃前的并发拒绝结论不丢：重放 blocked-1 判重复。
	if d := env.admit("blocked-1"); d.Code != CodeDuplicate || d.Reason != "duplicate of REJECT_CONCURRENCY_LIMITED" {
		t.Fatalf("committed rejection should survive crash as duplicate: %+v", d)
	}
}

// TestCrashMixedCommittedAndMultipleInFlight 同一次崩溃里既有已完成、又有多个在途：
// 检查点在两类都存在时落盘。恢复后用量必须严格等于"只有已完成部分生效"——
// 已完成的不许被一起退掉，在途的不许残留，两类互不串账。
func TestCrashMixedCommittedAndMultipleInFlight(t *testing.T) {
	env := newCrashEnv(t) // 并发 4 槽、总量 5

	// 2 个已完成提交。
	for _, id := range []string{"done-1", "done-2"} {
		if d := env.admit(id); d.Code != CodeExecuted {
			t.Fatalf("%s: %+v", id, d)
		}
	}
	// 3 个在途（同一次崩溃崩掉多个）。hang 表先一次性写好再启动，避免与 handler 并发读写。
	for _, id := range []string{"hang-1", "hang-2", "hang-3"} {
		env.hang[id] = true
	}
	for _, id := range []string{"hang-1", "hang-2", "hang-3"} {
		go func(id string) { _ = env.admit(id) }(id)
	}
	for i := 0; i < 3; i++ {
		<-env.entered
	}
	if u := env.limiter.Usage("tenant-a"); u.InFlight != 3 || u.QuotaUsed != 5 {
		t.Fatalf("pre-crash state wrong: %+v", u)
	}

	// 混合状态下打检查点（快照：used=5, inFlight=3），随后强杀。
	env.checkpoint()
	env.kill()

	u := env.limiter.Usage("tenant-a")
	t.Logf("2 已完成 + 3 在途混合崩溃，恢复结果: %+v", u)
	if u.InFlight != 0 {
		t.Fatalf("in-flight slots leaked: %+v", u)
	}
	if u.QuotaUsed != 2 || u.QuotaRemain != 3 {
		t.Fatalf("usage must equal committed-only (2), got %+v", u)
	}
	// 已完成的两个重放判重复，且不再扣减。
	for _, id := range []string{"done-1", "done-2"} {
		if d := env.admit(id); d.Code != CodeDuplicate {
			t.Fatalf("committed %s should be duplicate, got %+v", id, d)
		}
	}
	// 在途的三个按新请求重试成功，最终用量 = 2 + 3 = 5，恰好打满。
	for _, id := range []string{"hang-1", "hang-2", "hang-3"} {
		if d := env.admit(id); d.Code != CodeExecuted {
			t.Fatalf("retry %s: %+v", id, d)
		}
	}
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 5 || u.QuotaRemain != 0 {
		t.Fatalf("final usage=%+v, want used=5 remain=0", u)
	}
	if d := env.admit("overflow"); d.Code != CodeQuotaExhausted {
		t.Fatalf("quota should be exactly exhausted, got %+v", d)
	}
}

// TestCrashRepeatedCyclesNoDoubleRefund 反复"在途+检查点+强杀"多个周期，
// 再叠加多次无任何新动作的 plain 重启：每周期只回滚当周期的在途占用，
// 已恢复的结论不会因再次重启被二次退款，最终用量严格等于各周期已完成之和。
func TestCrashRepeatedCyclesNoDoubleRefund(t *testing.T) {
	env := newCrashEnv(t) // 总量 5

	// 两个崩溃周期：每周期 1 个已完成 + 1 个在途（检查点后强杀）。
	for cycle := 1; cycle <= 2; cycle++ {
		done := fmt.Sprintf("done-c%d", cycle)
		hang := fmt.Sprintf("hang-c%d", cycle)
		if d := env.admit(done); d.Code != CodeExecuted {
			t.Fatalf("cycle %d %s: %+v", cycle, done, d)
		}
		env.hang[hang] = true
		go func() { _ = env.admit(hang) }()
		<-env.entered
		env.checkpoint()
		env.kill()
		u := env.limiter.Usage("tenant-a")
		t.Logf("崩溃周期 %d 恢复结果: %+v", cycle, u)
		if u.InFlight != 0 || u.QuotaUsed != int64(cycle) {
			t.Fatalf("cycle %d: want inflight=0 used=%d, got %+v", cycle, cycle, u)
		}
		// 下一周期继续以"会崩溃"的方式拉起。
		if cycle < 2 {
			env.boot(true)
		}
	}

	// 多次 plain 重启（无新请求）：磁盘状态不变，恢复结算幂等，不得重复退款。
	for i := 1; i <= 3; i++ {
		env.boot(false)
		u := env.limiter.Usage("tenant-a")
		t.Logf("第 %d 次空重启后: %+v", i, u)
		if u.InFlight != 0 || u.QuotaUsed != 2 || u.QuotaRemain != 3 {
			t.Fatalf("plain restart #%d changed settled state: %+v", i, u)
		}
	}

	// 账目仍可对上：重试两个在途请求 + 1 个新请求恰好打满 5。
	for _, id := range []string{"hang-c1", "hang-c2", "extra"} {
		if d := env.admit(id); d.Code != CodeExecuted {
			t.Fatalf("retry %s: %+v", id, d)
		}
	}
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 5 || u.QuotaRemain != 0 {
		t.Fatalf("final usage=%+v, want used=5 remain=0", u)
	}
}

// TestCrashRightAfterCheckpointOnlyInFlight 刚打完检查点就崩、且崩溃前没有任何
// 已提交请求（日志为空、快照里只有在途占用）：恢复后全部回滚，租户账目回到初始状态。
func TestCrashRightAfterCheckpointOnlyInFlight(t *testing.T) {
	env := newCrashEnv(t)

	env.hang["hang-1"] = true
	env.hang["hang-2"] = true
	go func() { _ = env.admit("hang-1") }()
	go func() { _ = env.admit("hang-2") }()
	<-env.entered
	<-env.entered

	// 检查点是崩溃前最后一个动作：快照 used=2/inFlight=2，日志已截断为空。
	env.checkpoint()
	env.kill()

	u := env.limiter.Usage("tenant-a")
	t.Logf("仅在途请求+检查点后立即崩溃，恢复结果: %+v", u)
	if u.InFlight != 0 || u.QuotaUsed != 0 || u.QuotaRemain != 5 || u.Tokens != 1000 {
		t.Fatalf("only-in-flight checkpointed crash should roll back to pristine: %+v", u)
	}
	// 两个请求按新请求重试均可执行。
	for _, id := range []string{"hang-1", "hang-2"} {
		if d := env.admit(id); d.Code != CodeExecuted {
			t.Fatalf("retry %s: %+v", id, d)
		}
	}
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 2 {
		t.Fatalf("after retries usage=%+v, want used=2", u)
	}
}

// TestExecFailureReasonPreserved 下游真实错误原样进入判定依据：
// 首次判定结论、审计流水、跨重启恢复出的审计与重复判定，都能看到下游报的具体错误，
// 而不是一句和具体原因对不上的套话。
func TestExecFailureReasonPreserved(t *testing.T) {
	boom := errors.New("downstream boom: connection refused")
	env := &restartEnv{
		t: t, dir: t.TempDir(), c: clock.NewFake(t0),
		handler: func(context.Context, Request) error { return boom },
	}
	env.start()

	d := env.admit("fail-1")
	if d.Code != CodeExecFailed {
		t.Fatalf("want EXEC_FAILED, got %+v", d)
	}
	if d.Reason != boom.Error() {
		t.Fatalf("reason must carry downstream error verbatim, got %q", d.Reason)
	}

	// 审计流水里的判定依据同样是下游真实错误。
	recs := env.gw.Audit()
	last := recs[len(recs)-1]
	if last.Code != CodeExecFailed || last.Reason != boom.Error() {
		t.Fatalf("audit record lost downstream error: %+v", last)
	}
	// 失败请求已退款：不占用量。
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 0 {
		t.Fatalf("failed request charged quota: %+v", u)
	}

	// 跨重启：恢复出的首次判定记录仍带真实错误；重复提交指向首次结论。
	env.restart()
	found := false
	for _, r := range env.gw.Audit() {
		if r.RequestID == "fail-1" && r.Kind == persist.EventCommit {
			found = true
			if r.Reason != boom.Error() {
				t.Fatalf("recovered audit reason=%q, want downstream error", r.Reason)
			}
		}
	}
	if !found {
		t.Fatal("commit audit record lost after restart")
	}
	dup := env.admit("fail-1")
	t.Logf("执行失败判定依据保留并跨重启恢复，重复提交判定: %s (%s)", dup.Code, dup.Reason)
	if dup.Code != CodeDuplicate || dup.Reason != "duplicate of EXEC_FAILED" {
		t.Fatalf("duplicate after restart: %+v", dup)
	}
}
