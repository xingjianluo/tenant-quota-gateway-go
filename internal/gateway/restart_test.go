package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// restartEnv 是一套可反复"重启"的网关环境：同一持久化目录、同一 fake 时钟水位与配置。
type restartEnv struct {
	t       *testing.T
	dir     string
	c       *clock.Fake
	store   persist.Store
	limiter *quota.Limiter
	gw      *Gateway
	handler Handler
	starts  int
}

func newRestartEnv(t *testing.T) *restartEnv {
	t.Helper()
	env := &restartEnv{
		t:       t,
		dir:     t.TempDir(),
		c:       clock.NewFake(t0),
		handler: okHandler,
	}
	env.store, _ = persist.NewFileStore(filepath.Join(env.dir, "state"))
	env.start()
	return env
}

// start 用相同配置拉起一个全新进程（全新 store 句柄/限流器/网关），复用同一状态目录。
func (e *restartEnv) start() {
	e.starts++
	store, err := persist.NewFileStore(filepath.Join(e.dir, "state"))
	if err != nil {
		e.t.Fatalf("open store: %v", err)
	}
	e.store = store
	e.limiter = quota.NewLimiter(e.c)
	e.limiter.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 5})
	s := auth.NewStore()
	s.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(24 * time.Hour)})
	e.gw = New(auth.NewAuthenticator(s, e.c), e.limiter, e.c, e.handler)
	e.gw.Logger = log.New(io.Discard, "", 0)
	if err := e.gw.AttachStore(store, 4096, 256); err != nil {
		e.t.Fatalf("attach/restore: %v", err)
	}
}

func (e *restartEnv) restart() { e.start() }

func (e *restartEnv) admit(id string) Decision {
	return e.gw.Admit(context.Background(), Request{RequestID: id, TokenID: "tok-a", Scope: "invoke"})
}

// TestRestartPreservesQuotaAndUsage 重启后已扣配额、剩余额度接着延续，不回到初始状态。
func TestRestartPreservesQuotaAndUsage(t *testing.T) {
	env := newRestartEnv(t)
	for i := 0; i < 3; i++ {
		if d := env.admit(fmt.Sprintf("r%d", i)); d.Code != CodeExecuted {
			t.Fatalf("first run req %d: %+v", i, d)
		}
	}
	env.restart()

	u := env.limiter.Usage("tenant-a")
	if u.QuotaUsed != 3 || u.QuotaRemain != 2 || u.InFlight != 0 {
		t.Fatalf("usage not continued after restart: %+v", u)
	}
	// 再提交 2 个恰好打满，第 6 个必须是配额耗尽，与不重启时的判定口径一致。
	if d := env.admit("r3"); d.Code != CodeExecuted {
		t.Fatalf("r3 after restart: %+v", d)
	}
	if d := env.admit("r4"); d.Code != CodeExecuted {
		t.Fatalf("r4 after restart: %+v", d)
	}
	if d := env.admit("r5"); d.Code != CodeQuotaExhausted {
		t.Fatalf("want quota exhausted across restart, got %+v", d)
	}
}

// TestRestartDuplicateReturnsSameConclusion 重启前处理过的请求，重启后再提交：
// 仍判 DUPLICATE、结论与首次一致、不重复扣减，且本次重复判定也写入连续审计。
func TestRestartDuplicateReturnsSameConclusion(t *testing.T) {
	env := newRestartEnv(t)
	first := env.admit("dup")
	if first.Code != CodeExecuted {
		t.Fatalf("first: %+v", first)
	}
	beforeRestartAudit := len(env.gw.Audit())

	env.restart()
	dup := env.admit("dup")
	if dup.Code != CodeDuplicate {
		t.Fatalf("want duplicate after restart, got %+v", dup)
	}
	if dup.TenantID != first.TenantID {
		t.Fatalf("duplicate tenant mismatch: %+v", dup)
	}
	if !sameFinalUsage(dup.Usage, first.Usage) {
		t.Fatalf("duplicate final usage mismatch: first=%+v dup=%+v", first.Usage, dup.Usage)
	}
	// 不重复扣减。
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 1 {
		t.Fatalf("duplicate after restart charged quota: %+v", u)
	}

	recs := env.gw.Audit()
	if len(recs) != beforeRestartAudit+1 {
		t.Fatalf("audit len=%d, want %d", len(recs), beforeRestartAudit+1)
	}
	last := recs[len(recs)-1]
	if last.Code != CodeDuplicate || last.RequestID != "dup" || last.TokenID != "tok-a" ||
		last.Scope != "invoke" || last.TenantID != "tenant-a" {
		t.Fatalf("duplicate audit record missing input/basis: %+v", last)
	}
	if last.Reason != "duplicate of EXECUTED" {
		t.Fatalf("duplicate reason=%q", last.Reason)
	}
	// 序号必须连续。
	prev := recs[len(recs)-2]
	if last.Seq != prev.Seq+1 {
		t.Fatalf("audit seq not continuous: %d after %d", last.Seq, prev.Seq)
	}
}

func sameFinalUsage(a, b quota.Usage) bool {
	return a.QuotaUsed == b.QuotaUsed && a.QuotaRemain == b.QuotaRemain &&
		a.InFlight == b.InFlight && a.Tokens == b.Tokens
}

// ---- 处理中途被强制终止的场景 ----

// crashStore 包装真实文件存储：freeze 之后旧网关的所有写操作永久阻塞，
// 模拟进程被 SIGKILL（写线程随进程消失），从而不会写穿到重启后的新进程。
type crashStore struct {
	persist.Store
	mu      sync.Mutex
	frozen  bool
	blocked chan struct{} // 永不关闭：冻结的写永远卡在这里
}

func newCrashStore(inner persist.Store) *crashStore {
	return &crashStore{Store: inner, blocked: make(chan struct{})}
}

func (s *crashStore) freeze() {
	s.mu.Lock()
	s.frozen = true
	s.mu.Unlock()
}

func (s *crashStore) waitIfFrozen() {
	s.mu.Lock()
	frozen := s.frozen
	s.mu.Unlock()
	if frozen {
		<-s.blocked // 进程已"死亡"：写永不返回
	}
}

func (s *crashStore) AppendEvent(e persist.CommitEvent) error {
	s.waitIfFrozen()
	return s.Store.AppendEvent(e)
}

func (s *crashStore) SaveSnapshot(p *persist.Snapshot) error {
	s.waitIfFrozen()
	return s.Store.SaveSnapshot(p)
}

func (s *crashStore) TruncateEvents() error {
	s.waitIfFrozen()
	return s.Store.TruncateEvents()
}

type crashEnv struct {
	t       *testing.T
	dir     string
	c       *clock.Fake
	limiter *quota.Limiter
	gw      *Gateway
	crashed *crashStore
	release chan struct{}
	entered chan struct{}
	hang    map[string]bool // 只有这些 RequestID 的下游会阻塞
}

func newCrashEnv(t *testing.T) *crashEnv {
	dir := t.TempDir()
	env := &crashEnv{
		t: t, dir: dir, c: clock.NewFake(t0),
		release: make(chan struct{}), entered: make(chan struct{}, 16),
		hang: map[string]bool{},
	}
	env.boot(true)
	return env
}

// boot 拉起一个新进程；crashStore 只包在"会崩溃的旧进程"外，重启后的进程直接用文件存储。
func (e *crashEnv) boot(asCrash bool) {
	store, err := persist.NewFileStore(filepath.Join(e.dir, "state"))
	if err != nil {
		e.t.Fatalf("open store: %v", err)
	}
	var attached persist.Store = store
	if asCrash {
		e.crashed = newCrashStore(store)
		attached = e.crashed
	}
	e.limiter = quota.NewLimiter(e.c)
	e.limiter.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 5})
	s := auth.NewStore()
	s.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(24 * time.Hour)})
	h := okHandler
	if asCrash {
		h = func(_ context.Context, r Request) error {
			if e.hang[r.RequestID] {
				e.entered <- struct{}{}
				<-e.release
			}
			return nil
		}
	}
	e.gw = New(auth.NewAuthenticator(s, e.c), e.limiter, e.c, h)
	e.gw.Logger = log.New(io.Discard, "", 0)
	if err := e.gw.AttachStore(attached, 4096, 256); err != nil {
		e.t.Fatalf("attach/restore: %v", err)
	}
}

func (e *crashEnv) admit(id string) Decision {
	return e.gw.Admit(context.Background(), Request{RequestID: id, TokenID: "tok-a", Scope: "invoke"})
}

// kill 模拟 SIGKILL：冻结旧进程的持久化写并解除下游阻塞（旧 goroutine 会卡在冻结的写上，
// 不再触碰状态目录），然后以全新进程拉起。
func (e *crashEnv) kill() {
	e.crashed.freeze()
	close(e.release)
	e.boot(false)
}

// TestCrashMidRequestReleasesSlotAndQuota 两个请求在下游执行途中进程被杀：
// 已完成并提交的扣减保留，未提交的在途扣减（配额+槽位）全部回滚。
func TestCrashMidRequestReleasesSlotAndQuota(t *testing.T) {
	env := newCrashEnv(t)

	// 1 个已完成提交（占用 1 配额）。
	if d := env.admit("done-1"); d.Code != CodeExecuted {
		t.Fatalf("done-1: %+v", d)
	}
	// 2 个卡在下游的在途请求（各占 1 槽 + 预扣 1 配额）。
	env.hang["hang-1"] = true
	env.hang["hang-2"] = true
	go func() { _ = env.admit("hang-1") }()
	go func() { _ = env.admit("hang-2") }()
	<-env.entered
	<-env.entered

	if u := env.limiter.Usage("tenant-a"); u.InFlight != 2 || u.QuotaUsed != 3 {
		t.Fatalf("pre-crash state wrong: %+v", u)
	}

	// 进程被强制终止，重启恢复：在途的 2 份必须回滚，已提交的 1 份保留。
	env.kill()
	u := env.limiter.Usage("tenant-a")
	if u.InFlight != 0 {
		t.Fatalf("in-flight slot permanently held after crash: %+v", u)
	}
	if u.QuotaUsed != 1 || u.QuotaRemain != 4 {
		t.Fatalf("quota not reconciled after crash: %+v", u)
	}

	// 崩溃前未提交的两个请求没有结论，重启后按新请求处理，必须能正常执行（槽位已归还）。
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

// TestCrashAfterCommitIsDuplicate 结论已提交（持久化完成）之后崩溃：
// 重启后同一请求判重复，配额不被二次扣减。
func TestCrashAfterCommitIsDuplicate(t *testing.T) {
	env := newCrashEnv(t)
	if d := env.admit("committed"); d.Code != CodeExecuted {
		t.Fatalf("committed: %+v", d)
	}
	env.kill() // 提交后、下一条请求前被杀
	if d := env.admit("committed"); d.Code != CodeDuplicate {
		t.Fatalf("want duplicate after committed-then-crash, got %+v", d)
	}
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 1 {
		t.Fatalf("quota double charged after crash: %+v", u)
	}
}

// TestRecoveryBoundedByCheckpoint 历史请求越积越多，但检查点周期性截断日志：
// 恢复时读取的提交事件数被检查点间隔限定，不随总历史增长；
// 审计流水只保留配置的尾部窗口，序号仍连续、水位正确。
func TestRecoveryBoundedByCheckpoint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	const retain, every = 4, 8
	boot := func() (*Gateway, *quota.Limiter) {
		c := clock.NewFake(t0)
		store, err := persist.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		l := quota.NewLimiter(c)
		l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1e9, Burst: 1e9, MaxConcurrent: 1e9, TotalQuota: 1 << 30})
		s := auth.NewStore()
		s.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(24 * time.Hour)})
		gw := New(auth.NewAuthenticator(s, c), l, c, okHandler)
		gw.Logger = log.New(io.Discard, "", 0)
		if err := gw.AttachStore(store, retain, every); err != nil {
			t.Fatal(err)
		}
		return gw, l
	}

	gw, _ := boot()
	const total = 500
	for i := 0; i < total; i++ {
		if d := gw.Admit(context.Background(), Request{RequestID: fmt.Sprintf("h-%d", i), TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeExecuted {
			t.Fatalf("req %d: %+v", i, d)
		}
	}

	// 重启：重放事件数必须被检查点间隔限定，而不是 500 条全量重放。
	started := time.Now()
	gw2, _ := boot()
	elapsed := time.Since(started)
	t.Logf("recovered after %d requests in %v (journal replay bounded by %d)", total, elapsed, every)

	// 最后检查点在 seq=496：快照保留 493..496（retain 条），日志另有 497..500
	// （不足一个检查点间隔的尾部新事件）。合并后窗口 <= retain+every，序号连续不重复。
	recs := gw2.Audit()
	if len(recs) != retain+(total%every) {
		t.Fatalf("retained audit=%d, want %d", len(recs), retain+(total%every))
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].Seq != recs[i-1].Seq+1 {
			t.Fatalf("retained audit seq gap: %d after %d", recs[i].Seq, recs[i-1].Seq)
		}
	}
	if last := recs[len(recs)-1]; last.Seq != total {
		t.Fatalf("audit waterline LastSeq=%d, want %d", last.Seq, total)
	}
	if want := int64(total - len(recs) + 1); recs[0].Seq != want {
		t.Fatalf("audit window start seq=%d, want %d", recs[0].Seq, want)
	}
	// 幂等表不丢：很早的请求重放仍判重复。
	if d := gw2.Admit(context.Background(), Request{RequestID: "h-0", TokenID: "tok-a", Scope: "invoke"}); d.Code != CodeDuplicate {
		t.Fatalf("old request should still be duplicate: %+v", d)
	}
	// 新请求序号接着历史水位，不因只保留尾部而重置。
	d := gw2.Admit(context.Background(), Request{RequestID: "post", TokenID: "tok-a", Scope: "invoke"})
	if d.Code != CodeExecuted {
		t.Fatalf("post-recovery: %+v", d)
	}
	// duplicate 占 seq=501，post 占 seq=502。
	if last := gw2.Audit()[len(gw2.Audit())-1]; last.Seq != total+2 {
		t.Fatalf("seq reset after recovery: %d, want %d", last.Seq, total+2)
	}
}

// TestRestartMixedOutcomesConsistent 执行失败（退款）与限流拒绝跨重启保持口径，
// 且每类结论重启后重放都返回与首次一致的判定。
func TestRestartMixedOutcomesConsistent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	type call struct {
		id   string
		fail bool
	}
	boot := func(fails map[string]bool) *Gateway {
		c := clock.NewFake(t0)
		store, err := persist.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		l := quota.NewLimiter(c)
		// Burst=3：ok-1/ok-2/fail-1 各取一令牌；fail-1 执行失败退还令牌，
		// rate-1 仍可取到令牌成功，故需要再提交 rate-1 第二次（新 RequestID 不行）……
		// 这里直接用 3 个成功耗尽令牌后第 4 个限流，再补一个失败退款案例：
		// Burst=3，ok-1/ok-2/ok-3 成功，rate-1 限流；fail 放在成功之前验证退款。
		l.SetLimit("tenant-a", quota.Limit{RatePerSec: 0, Burst: 3, MaxConcurrent: 10, TotalQuota: 100})
		s := auth.NewStore()
		s.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(24 * time.Hour)})
		gw := New(auth.NewAuthenticator(s, c), l, c,
			func(_ context.Context, r Request) error {
				if fails[r.RequestID] {
					return errDownstream
				}
				return nil
			})
		gw.Logger = log.New(io.Discard, "", 0)
		if err := gw.AttachStore(store, 4096, 256); err != nil {
			t.Fatal(err)
		}
		return gw
	}

	fails := map[string]bool{"fail-1": true}
	// 顺序：1 成功 + 1 失败（退款）+ 2 成功 = 共净消耗 3 令牌（Burst=3），第 5 个限流。
	inputs := []call{{"ok-1", false}, {"fail-1", true}, {"ok-2", false}, {"ok-3", false}, {"rate-1", false}}
	run := func(gw *Gateway) map[string]Code {
		out := map[string]Code{}
		for _, in := range inputs {
			out[in.id] = gw.Admit(context.Background(), Request{RequestID: in.id, TokenID: "tok-a", Scope: "invoke"}).Code
		}
		return out
	}

	gw1 := boot(fails)
	first := run(gw1)
	if first["ok-1"] != CodeExecuted || first["fail-1"] != CodeExecFailed ||
		first["ok-2"] != CodeExecuted || first["ok-3"] != CodeExecuted ||
		first["rate-1"] != CodeRateLimited {
		t.Fatalf("first run outcomes wrong: %+v", first)
	}
	// 失败请求已退款：3 次成功计入用量，失败与限流均不计。
	if u := gw1.limiter.Usage("tenant-a"); u.QuotaUsed != 3 {
		t.Fatalf("failed request charged across boundary: %+v", u)
	}

	gw2 := boot(fails)
	second := run(gw2)
	for id := range first {
		if second[id] != CodeDuplicate {
			t.Fatalf("%s after restart: want DUPLICATE, got %s", id, second[id])
		}
	}
	// 重复判定不改变任何用量；失败退款的口径在重启后仍然成立。
	if u := gw2.limiter.Usage("tenant-a"); u.QuotaUsed != 3 {
		t.Fatalf("duplicates after restart changed quota: %+v", u)
	}
}

var errDownstream = errors.New("downstream boom")

// TestRestartRejectedRequestsAreDuplicates 鉴权失败与配额拒绝也是"已处理结论"：
// 跨重启重放仍按重复判定，且返回与首次一致的结论码（包在 DUPLICATE 的 reason 中）。
func TestRestartRejectedRequestsAreDuplicates(t *testing.T) {
	env := newRestartEnv(t)
	ctx := context.Background()

	// 未知令牌 + 越权（不触碰配额）。
	if d := env.gw.Admit(ctx, Request{RequestID: "unknown", TokenID: "nope", Scope: "invoke"}); d.Code != CodeTokenUnknown {
		t.Fatalf("unknown first: %+v", d)
	}
	if d := env.gw.Admit(ctx, Request{RequestID: "scope", TokenID: "tok-a", Scope: "admin"}); d.Code != CodeScopeDenied {
		t.Fatalf("scope first: %+v", d)
	}
	// 打满 5 配额后第 6 个被拒。
	for i := 0; i < 5; i++ {
		env.admit(fmt.Sprintf("q-%d", i))
	}
	if d := env.admit("exhausted"); d.Code != CodeQuotaExhausted {
		t.Fatalf("exhausted first: %+v", d)
	}

	env.restart()

	if d := env.gw.Admit(ctx, Request{RequestID: "unknown", TokenID: "nope", Scope: "invoke"}); d.Code != CodeDuplicate || d.Reason != "duplicate of REJECT_TOKEN_UNKNOWN" {
		t.Fatalf("unknown after restart: %+v", d)
	}
	if d := env.gw.Admit(ctx, Request{RequestID: "scope", TokenID: "tok-a", Scope: "admin"}); d.Code != CodeDuplicate || d.Reason != "duplicate of REJECT_SCOPE_DENIED" {
		t.Fatalf("scope after restart: %+v", d)
	}
	if d := env.admit("exhausted"); d.Code != CodeDuplicate || d.Reason != "duplicate of REJECT_QUOTA_EXHAUSTED" {
		t.Fatalf("exhausted after restart: %+v", d)
	}
	// 三类重复都不得改动用量。
	if u := env.limiter.Usage("tenant-a"); u.QuotaUsed != 5 || u.QuotaRemain != 0 {
		t.Fatalf("rejected duplicates changed usage: %+v", u)
	}
}
