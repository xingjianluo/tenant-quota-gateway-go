package quota

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
)

func newTestManager(t *testing.T, rate float64, conc int) (*Manager, *clock.FakeClock) {
	t.Helper()
	base := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(base)
	m := New(clk)
	m.AddTenant("a", Limits{RatePerSecond: rate, MaxConcurrent: conc})
	return m, clk
}

func TestRateExhaustionNoSideEffect(t *testing.T) {
	m, _ := newTestManager(t, 3, 10)

	// 同一时刻连续请求：满桶 3，恰好 3 个放行，其余全部速率拒绝。
	var allowed int
	for i := 0; i < 8; i++ {
		_, code := m.TryAcquire("a")
		switch code {
		case model.RejectNone:
			allowed++
		case model.RejectRateLimited:
			// 期望拒绝
		default:
			t.Fatalf("unexpected code %s", code)
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed=%d want 3", allowed)
	}
	snap, _ := m.SnapshotOf("a")
	if snap.RateRemaining < -1e-9 || snap.RateRemaining > 1e-9 {
		t.Fatalf("remaining=%v want ~0", snap.RateRemaining)
	}
	if snap.ConcurrencyInUse != 3 {
		t.Fatalf("inUse=%d want 3 (rejected must not occupy slot)", snap.ConcurrencyInUse)
	}
}

func TestRefillOverTime(t *testing.T) {
	m, clk := newTestManager(t, 10, 100)
	for i := 0; i < 10; i++ {
		if _, c := m.TryAcquire("a"); c != model.RejectNone {
			t.Fatalf("initial acquire %d: %s", i, c)
		}
	}
	if _, c := m.TryAcquire("a"); c != model.RejectRateLimited {
		t.Fatalf("expected rate limited, got %s", c)
	}
	clk.Advance(500 * time.Millisecond) // 回补 5
	for i := 0; i < 5; i++ {
		if _, c := m.TryAcquire("a"); c != model.RejectNone {
			t.Fatalf("refill acquire %d: %s", i, c)
		}
	}
	if _, c := m.TryAcquire("a"); c != model.RejectRateLimited {
		t.Fatalf("expected rate limited after refill consumed, got %s", c)
	}
}

func TestConcurrencyLimitAndRelease(t *testing.T) {
	m, _ := newTestManager(t, 100, 2)
	if _, c := m.TryAcquire("a"); c != model.RejectNone {
		t.Fatal(c)
	}
	if _, c := m.TryAcquire("a"); c != model.RejectNone {
		t.Fatal(c)
	}
	if _, c := m.TryAcquire("a"); c != model.RejectConcurrencyFull {
		t.Fatalf("got %s want CONCURRENCY_FULL", c)
	}
	m.Release("a", false)
	if _, c := m.TryAcquire("a"); c != model.RejectNone {
		t.Fatalf("slot should be reusable, got %s", c)
	}
}

func TestFailedExecutionRefundsRateToken(t *testing.T) {
	m, _ := newTestManager(t, 1, 10)
	before, _ := m.SnapshotOf("a")
	if _, c := m.TryAcquire("a"); c != model.RejectNone {
		t.Fatal(c)
	}
	afterAcquire, _ := m.SnapshotOf("a")
	if d := before.RateRemaining - afterAcquire.RateRemaining; d < 1-1e-9 || d > 1+1e-9 {
		t.Fatalf("rate token not consumed: before=%v after=%v", before.RateRemaining, afterAcquire.RateRemaining)
	}
	m.Release("a", true) // 执行失败，退还速率令牌
	afterRefund, _ := m.SnapshotOf("a")
	if d := before.RateRemaining - afterRefund.RateRemaining; d > 1e-9 || d < -1e-9 {
		t.Fatalf("rate token not refunded: before=%v after=%v", before.RateRemaining, afterRefund.RateRemaining)
	}
}

func TestUnknownTenant(t *testing.T) {
	m, _ := newTestManager(t, 1, 1)
	if _, c := m.TryAcquire("ghost"); c != model.RejectUnknownTenant {
		t.Fatalf("got %s want UNKNOWN_TENANT", c)
	}
	// 未注册租户的 Release 也应安全返回。
	m.Release("ghost", true)
}

func TestTenantIsolation(t *testing.T) {
	base := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(base)
	m := New(clk)
	m.AddTenant("a", Limits{RatePerSecond: 1, MaxConcurrent: 1})
	m.AddTenant("b", Limits{RatePerSecond: 5, MaxConcurrent: 5})

	if _, c := m.TryAcquire("a"); c != model.RejectNone {
		t.Fatal(c)
	}
	if _, c := m.TryAcquire("a"); c != model.RejectRateLimited {
		t.Fatalf("a should be rate limited, got %s", c)
	}
	// A 耗尽不应影响 B。
	for i := 0; i < 5; i++ {
		if _, c := m.TryAcquire("b"); c != model.RejectNone {
			t.Fatalf("b acquire %d affected by tenant a: %s", i, c)
		}
	}
}

// TestConcurrentNoOverIssue 是核心并发不变量：
// 速率容量为 capacity 时，同一时刻发起 N 个并发获取，
// 放行数量必须严格等于 min(capacity, maxConcurrent)，绝无超发。
func TestConcurrentNoOverIssue(t *testing.T) {
	const capacity = 50
	base := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(base)
	m := New(clk)
	m.AddTenant("a", Limits{RatePerSecond: capacity, MaxConcurrent: capacity})

	const n = 500
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, code := m.TryAcquire("a")
			if code == model.RejectNone {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if allowed != capacity {
		t.Fatalf("allowed=%d want exactly %d (no over-issue, no under-count)", allowed, capacity)
	}
	snap, _ := m.SnapshotOf("a")
	if snap.ConcurrencyInUse != capacity {
		t.Fatalf("inUse=%d want %d", snap.ConcurrencyInUse, capacity)
	}
	if snap.RateRemaining > 1e-9 {
		t.Fatalf("remaining=%v want 0", snap.RateRemaining)
	}
}

// TestConcurrentAcquireReleaseNoLeak 验证高并发“获取-释放”下并发槽不泄漏、不超发。
func TestConcurrentAcquireReleaseNoLeak(t *testing.T) {
	const maxConc = 10
	base := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(base)
	m := New(clk)
	m.AddTenant("a", Limits{RatePerSecond: 1_000_000, MaxConcurrent: maxConc})

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(fail bool) {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				snap, code := m.TryAcquire("a")
				if code == model.RejectNone {
					if snap.ConcurrencyInUse > maxConc {
						t.Errorf("over-issue: inUse=%d > %d", snap.ConcurrencyInUse, maxConc)
						return
					}
					m.Release("a", fail)
				}
			}
		}(i%7 == 0)
	}
	close(start)
	wg.Wait()
	snap, _ := m.SnapshotOf("a")
	if snap.ConcurrencyInUse != 0 {
		t.Fatalf("slot leaked: inUse=%d want 0", snap.ConcurrencyInUse)
	}
}
