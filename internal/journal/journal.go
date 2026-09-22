package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
)

// EventKind 标识追加事件类型。
type EventKind string

const (
	EventAdmit  EventKind = "admit"  // 放行（扣减速率令牌、占用并发槽）
	EventFinish EventKind = "finish" // 请求结束（归还并发槽；可选退还速率令牌）
)

// Event 是持久化的配额记账事件。
// 同一输入（含 IdempotencyKey）在进程重启后重放，可得到一致的判定与统计口径。
type Event struct {
	Kind       EventKind `json:"kind"`
	TenantID   string    `json:"tenant_id"`
	RequestID  string    `json:"request_id"`
	IdemKey    string    `json:"idem_key,omitempty"`
	At         time.Time `json:"at"`
	RefundRate bool      `json:"refund_rate,omitempty"` // 仅 finish：执行失败时退还速率令牌
}

// Journal 为本地追加式记账日志（JSON Lines）。
type Journal struct {
	path string
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	enc  *json.Encoder
}

// Open 打开（必要时创建）日志文件，后续 Append 立即 fsync 落盘。
func Open(path string) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)
	j := &Journal{path: path, f: f, w: w, enc: json.NewEncoder(w)}
	return j, nil
}

// Append 序列化事件并 fsync，保证返回后事件已持久化。
func (j *Journal) Append(ev Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.enc.Encode(ev); err != nil {
		return err
	}
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Sync()
}

// Replay 按写入顺序读取全部事件并逐条回调；损坏/截断的末行将被忽略。
func (j *Journal) Replay(handle func(Event)) error {
	f, err := os.Open(j.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var ev Event
		if err := dec.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// 末行被截断（崩溃写了一半）时停止重放，忽略该半行。
			return nil
		}
		handle(ev)
	}
}

func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Close()
}

// Apply 把一条历史事件作用到内存状态（恢复用），返回 admit 事件是否成功恢复。
//
//	admit:  恢复扣令牌/占槽；返回 false 表示幂等记录缺失等异常（正常不会发生）。
//	finish: 归还并发槽并按需退还速率令牌。
func Apply(
	admit func(tenantID, requestID, idemKey string, at time.Time) bool,
	finish func(tenantID, requestID string, refund bool, at time.Time),
	ev Event,
) model.RejectCode {
	switch ev.Kind {
	case EventAdmit:
		if !admit(ev.TenantID, ev.RequestID, ev.IdemKey, ev.At) {
			return model.RejectUnknownTenant
		}
	case EventFinish:
		finish(ev.TenantID, ev.RequestID, ev.RefundRate, ev.At)
	}
	return model.RejectNone
}
