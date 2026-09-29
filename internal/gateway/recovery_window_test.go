package gateway

import (
	"context"
	"io"
	"log"
	"sync"
	"testing"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/auth"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/persist"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/quota"
)

// memStore 是内存版 persist.Store，可精确模拟崩溃窗口。
type memStore struct {
	mu        sync.Mutex
	snapshot  *persist.Snapshot
	events    []persist.CommitEvent
	failTrunc bool // 为 true 时 TruncateEvents 假装"进程在截断前被杀"（保留日志）
}

func (m *memStore) LoadSnapshot() (*persist.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snapshot == nil {
		return nil, persist.ErrNoSnapshot
	}
	cp := *m.snapshot
	return &cp, nil
}

func (m *memStore) SaveSnapshot(s *persist.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *s
	cp.Tenants = cloneTenantStates(s.Tenants)
	cp.Seen = cloneSeen(s.Seen)
	cp.AuditTail = append([]persist.CommitEvent(nil), s.AuditTail...)
	m.snapshot = &cp
	return nil
}

func (m *memStore) AppendEvent(e persist.CommitEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return nil
}

func (m *memStore) ReadEvents() ([]persist.CommitEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]persist.CommitEvent(nil), m.events...), nil
}

func (m *memStore) TruncateEvents() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failTrunc {
		// 模拟"快照 fsync 成功、进程在 truncate 前被 SIGKILL"：日志原样保留。
		return nil
	}
	m.events = nil
	return nil
}

func cloneTenantStates(in map[string]persist.TenantState) map[string]persist.TenantState {
	out := make(map[string]persist.TenantState, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneSeen(in map[string]persist.CommitEvent) map[string]persist.CommitEvent {
	out := make(map[string]persist.CommitEvent, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func newMemGateway(t *testing.T, store persist.Store) *Gateway {
	c := clock.NewFake(t0)
	l := quota.NewLimiter(c)
	l.SetLimit("tenant-a", quota.Limit{RatePerSec: 1000, Burst: 1000, MaxConcurrent: 4, TotalQuota: 100})
	s := auth.NewStore()
	s.Put(auth.Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"invoke"}, ExpiresAt: t0.Add(24 * 60 * 60 * 1e9)})
	gw := New(auth.NewAuthenticator(s, c), l, c, okHandler)
	gw.Logger = log.New(io.Discard, "", 0)
	if err := gw.AttachStore(store, 4096, 256); err != nil {
		t.Fatalf("attach: %v", err)
	}
	return gw
}

// TestCheckpointWithoutTruncateNoDuplicateReplay 快照已落盘但日志未截断时崩溃：
// 重启后旧事件不得被重放成重复审计，状态也不得回退。
func TestCheckpointWithoutTruncateNoDuplicateReplay(t *testing.T) {
	store := &memStore{failTrunc: true}
	gw := newMemGateway(t, store)
	ctx := context.Background()
	// 用很小的检查点间隔不便配置（AttachStore 固定 256），直接手动触发一次检查点。
	for i := 0; i < 5; i++ {
		gw.Admit(ctx, Request{RequestID: idFor(i), TokenID: "tok-a", Scope: "invoke"})
	}
	gw.mu.Lock()
	gw.checkpointLocked() // 快照成功，但因 failTrunc 日志保留
	gw.mu.Unlock()
	if len(store.events) == 0 || store.snapshot == nil {
		t.Fatal("setup: expect snapshot saved AND events retained")
	}
	auditBefore := len(gw.Audit())

	// 重启：同一 store（含快照 + 未截断的旧日志）。
	gw2 := newMemGateway(t, store)
	recs := gw2.Audit()
	if len(recs) != auditBefore {
		t.Fatalf("old journal events replayed into audit: got %d, want %d", len(recs), auditBefore)
	}
	for i, r := range recs {
		if r.Seq != int64(i+1) {
			t.Fatalf("audit seq not continuous after recovery: %+v", recs)
		}
	}
	if u := gw2.limiter.Usage("tenant-a"); u.QuotaUsed != 5 {
		t.Fatalf("state rolled back after recovery: %+v", u)
	}
	// 新事件接着水位继续编号。
	d := gw2.Admit(ctx, Request{RequestID: "after-restart", TokenID: "tok-a", Scope: "invoke"})
	if d.Code != CodeExecuted {
		t.Fatalf("post-restart request: %+v", d)
	}
	last := gw2.Audit()[len(gw2.Audit())-1]
	if last.Seq != 6 {
		t.Fatalf("seq after restart = %d, want 6", last.Seq)
	}
}

func idFor(i int) string {
	return "m-" + string(rune('a'+i))
}
