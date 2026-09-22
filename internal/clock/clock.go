package clock

import "time"

// Clock 是可注入的时间源，单测使用假时钟，避免真实等待。
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func NewRealClock() Clock { return &realClock{} }

func (c *realClock) Now() time.Time { return time.Now() }

// FakeClock 是测试用可控时钟。
type FakeClock struct {
	t time.Time
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{t: start}
}

func (c *FakeClock) Now() time.Time { return c.t }

func (c *FakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
