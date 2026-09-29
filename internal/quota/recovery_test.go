package quota

import (
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
)

// TestSnapshotRestoreContinues 快照恢复后，已扣配额、剩余额度、令牌桶水位接着延续。
func TestSnapshotRestoreContinues(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	l.SetLimit("t1", Limit{RatePerSec: 10, Burst: 10, MaxConcurrent: 4, TotalQuota: 5})

	for i := 0; i < 3; i++ {
		if err := l.Acquire("t1"); err != nil {
			t.Fatal(err)
		}
		l.Release("t1")
	}
	c.Advance(time.Second) // 恢复前推进时间，令牌桶水位应被精确保留
	want := l.Usage("t1")
	snap := l.SnapshotAll()

	// 新实例（模拟重启）：配置相同 Limit 后恢复。
	c2 := clock.NewFake(t0.Add(time.Second))
	l2 := NewLimiter(c2)
	l2.SetLimit("t1", Limit{RatePerSec: 10, Burst: 10, MaxConcurrent: 4, TotalQuota: 5})
	l2.RestoreAll(snap)
	got := l2.Usage("t1")
	if got != want {
		t.Fatalf("after restore got %+v, want %+v", got, want)
	}
	// 再放行 2 个后配额应恰好耗尽（3+2=5），而不是重新从满额开始。
	if err := l2.Acquire("t1"); err != nil {
		t.Fatalf("acquire 4: %v", err)
	}
	l2.Release("t1")
	if err := l2.Acquire("t1"); err != nil {
		t.Fatalf("acquire 5: %v", err)
	}
	l2.Release("t1")
	if err := l2.Acquire("t1"); err != ErrQuotaExhausted {
		t.Fatalf("want quota exhausted after restored usage, got %v", err)
	}
}

// TestSettleInFlightAfterCrash 崩溃时在途的请求：恢复后释放槽位、归还预扣配额，
// 已提交的成功结论对应的扣减不被回滚。
func TestSettleInFlightAfterCrash(t *testing.T) {
	c := clock.NewFake(t0)
	l := NewLimiter(c)
	l.SetLimit("t1", Limit{RatePerSec: 10, Burst: 10, MaxConcurrent: 4, TotalQuota: 5})

	// 2 个已完成、2 个在途时崩溃。
	for i := 0; i < 2; i++ {
		if err := l.Acquire("t1"); err != nil {
			t.Fatal(err)
		}
		l.Release("t1")
	}
	for i := 0; i < 2; i++ {
		if err := l.Acquire("t1"); err != nil {
			t.Fatal(err)
		}
	}
	snap := l.SnapshotAll()
	if got := snap["t1"].InFlight; got != 2 {
		t.Fatalf("setup inflight=%d, want 2", got)
	}

	c2 := clock.NewFake(t0)
	l2 := NewLimiter(c2)
	l2.SetLimit("t1", Limit{RatePerSec: 10, Burst: 10, MaxConcurrent: 4, TotalQuota: 5})
	l2.RestoreAll(snap)
	l2.SettleInFlight("t1", snap["t1"].InFlight)

	u := l2.Usage("t1")
	if u.InFlight != 0 {
		t.Fatalf("inflight slot leaked after recovery: %+v", u)
	}
	if u.QuotaUsed != 2 || u.QuotaRemain != 3 {
		t.Fatalf("in-flight quota not refunded / committed quota rolled back: %+v", u)
	}
	// 归还后剩余 3 份配额必须可以继续使用，不多不少。
	for i := 0; i < 3; i++ {
		if err := l2.Acquire("t1"); err != nil {
			t.Fatalf("acquire %d after settle: %v", i, err)
		}
		l2.Release("t1")
	}
	if err := l2.Acquire("t1"); err != ErrQuotaExhausted {
		t.Fatalf("want exhausted, got %v", err)
	}
}
