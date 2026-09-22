package audit

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
)

// Entry 是一条可追溯的审计流水。
type Entry struct {
	RequestID string
	TenantID  string
	Input     model.Request
	Basis     string         // 判定依据（为什么放行/拒绝）
	Decision  model.Decision // 最终结论与最终配额
}

// Logger 记录审计流水：同时输出到标准日志并保留在内存中供断言。
type Logger struct {
	mu      sync.Mutex
	entries []Entry
	out     *log.Logger
}

func New() *Logger {
	return &Logger{out: log.Default()}
}

// Log 写入一条审计记录。
func (l *Logger) Log(e Entry) {
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
	l.out.Printf("[audit] request_id=%s tenant=%s token=%q scope=%s idem=%q => allowed=%t code=%s basis=%q | rate=%.3f/%.3f concurrency=%d/%d replayed=%t at=%s",
		e.RequestID,
		e.TenantID,
		mask(e.Input.Token),
		e.Input.RequiredScope,
		e.Input.IdempotencyKey,
		e.Decision.Allowed,
		e.Decision.RejectCode,
		e.Basis,
		e.Decision.RateRemaining,
		e.Decision.RateCapacity,
		e.Decision.ConcurrencyInUse,
		e.Decision.ConcurrencyMax,
		e.Decision.Replayed,
		e.Decision.At.Format("2006-01-02T15:04:05.000Z07:00"),
	)
}

// Entries 返回进程内已记录流水的副本。
func (l *Logger) Entries() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, len(l.entries))
	copy(out, l.entries)
	return out
}

// mask 避免把完整令牌打进日志，仅保留可辨认前后缀。
func mask(token string) string {
	if token == "" {
		return ""
	}
	if len(token) <= 8 {
		return strings.Repeat("*", len(token))
	}
	return fmt.Sprintf("%s***%s", token[:5], token[len(token)-3:])
}
