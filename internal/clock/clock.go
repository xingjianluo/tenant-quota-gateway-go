// Package clock 提供可注入的时间源，便于测试在不依赖真实等待的情况下推进时间。
package clock

import (
	"sync"
	"time"
)

// Clock 抽象时间源。
type Clock interface {
	Now() time.Time
}

// Real 返回真实墙钟时间。
type Real struct{}

// Now 实现 Clock。
func (Real) Now() time.Time { return time.Now() }

// Fake 是可手动推进的测试时钟，并发安全。
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake 创建从 start 开始的假时钟。
func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

// Now 实现 Clock。
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance 将假时钟向前推进 d。
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}
