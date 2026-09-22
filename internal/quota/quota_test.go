package quota

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
)

var t0 = time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)

func TestAcquireWithinLimit(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	l.SetLimit("t1", Limit{RatePerSec: 1, Burst: 2, MaxConcurrent: 2, TotalQuota: 10})

	if err := l.Acquire("t1"); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := l.Acquire("t1"); err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	u := l.Usage("t1")
	if u.InFlight != 2 || u.QuotaUsed != 2 || u.QuotaRemain != 8 {
		t.Fatalf("unexpected usage: %+v", u)
	}
	// Burst=2 已耗尽，第三次应被限流。
	if err := l.Acquire("t1"); err != ErrRateLimited {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
	// 拒绝不得产生记账副作用。
	u = l.Usage("t1")
	if u.QuotaUsed != 2 || u.InFlight != 2 {
		t.Fatalf("rejected acquire leaked accounting: %+v", u)
	}
}

func TestRateRefillWithFakeClock(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	l.SetLimit("t1", Limit{RatePerSec: 2, Burst: 1, MaxConcurrent: 10, TotalQuota: 100})

	if err := l.Acquire("t1"); err != nil {
		t.Fatal(err)
	}
	if err := l.Acquire("t1"); err != ErrRateLimited {
		t.Fatalf("want rate limited, got %v", err)
	}
	c.Advance(500 * time.Millisecond) // 补充 1 个令牌
	if err := l.Acquire("t1"); err != nil {
		t.Fatalf("after refill: %v", err)
	}
}

func TestConcurrencyLimitAndRelease(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	l.SetLimit("t1", Limit{RatePerSec: 100, Burst: 100, MaxConcurrent: 1, TotalQuota: 100})

	if err := l.Acquire("t1"); err != nil {
		t.Fatal(err)
	}
	if err := l.Acquire("t1"); err != ErrConcurrencyLimited {
		t.Fatalf("want concurrency limited, got %v", err)
	}
	l.Release("t1")
	if err := l.Acquire("t1"); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

func TestQuotaExhausted(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	l.SetLimit("t1", Limit{RatePerSec: 100, Burst: 100, MaxConcurrent: 100, TotalQuota: 2})

	for i := 0; i < 2; i++ {
		if err := l.Acquire("t1"); err != nil {
			t.Fatal(err)
		}
		l.Release("t1")
	}
	if err := l.Acquire("t1"); err != ErrQuotaExhausted {
		t.Fatalf("want quota exhausted, got %v", err)
	}
}

func TestRefundRestoresQuota(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	l.SetLimit("t1", Limit{RatePerSec: 0, Burst: 1, MaxConcurrent: 1, TotalQuota: 1})

	if err := l.Acquire("t1"); err != nil {
		t.Fatal(err)
	}
	l.Refund("t1")
	l.Release("t1")
	u := l.Usage("t1")
	if u.QuotaUsed != 0 || u.InFlight != 0 {
		t.Fatalf("refund/release leaked: %+v", u)
	}
	// 退款后同一份配额可再次使用。
	if err := l.Acquire("t1"); err != nil {
		t.Fatalf("acquire after refund: %v", err)
	}
}

func TestTenantIsolation(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	l.SetLimit("a", Limit{RatePerSec: 0, Burst: 1, MaxConcurrent: 1, TotalQuota: 1})
	l.SetLimit("b", Limit{RatePerSec: 0, Burst: 1, MaxConcurrent: 1, TotalQuota: 1})

	if err := l.Acquire("a"); err != nil {
		t.Fatal(err)
	}
	if err := l.Acquire("a"); err == nil {
		t.Fatal("tenant a should be limited")
	}
	// 租户 b 不受 a 的流量影响。
	if err := l.Acquire("b"); err != nil {
		t.Fatalf("tenant b affected by tenant a: %v", err)
	}
}

// TestConcurrentNoOverGrant 并发压测：成功 Acquire 数不得超过 TotalQuota，
// 且结束后并发槽不得泄漏。
func TestConcurrentNoOverGrant(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	const quotaN = 50
	l.SetLimit("t1", Limit{RatePerSec: 1e9, Burst: 1e9, MaxConcurrent: 1e9, TotalQuota: quotaN})

	var wg sync.WaitGroup
	var mu sync.Mutex
	grants := 0
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Acquire("t1"); err == nil {
				mu.Lock()
				grants++
				mu.Unlock()
				l.Release("t1")
			}
		}()
	}
	wg.Wait()
	if grants != quotaN {
		t.Fatalf("grants=%d, want exactly %d (no over-grant, no loss)", grants, quotaN)
	}
	u := l.Usage("t1")
	if u.InFlight != 0 || u.QuotaUsed != quotaN {
		t.Fatalf("leaked state: %+v", u)
	}
}
