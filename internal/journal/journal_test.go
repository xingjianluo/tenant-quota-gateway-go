package journal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	events := []Event{
		{Kind: EventAdmit, TenantID: "a", RequestID: "r1", IdemKey: "k1", At: at},
		{Kind: EventFinish, TenantID: "a", RequestID: "r1", At: at.Add(time.Millisecond), RefundRate: true},
		{Kind: EventAdmit, TenantID: "b", RequestID: "r2", At: at.Add(2 * time.Millisecond)},
	}
	for _, ev := range events {
		if err := j.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()

	var got []Event
	if err := j2.Replay(func(ev Event) { got = append(got, ev) }); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(events) {
		t.Fatalf("replayed %d events want %d", len(got), len(events))
	}
	for i := range events {
		if got[i] != events[i] {
			t.Errorf("event %d mismatch:\n got=%+v\nwant=%+v", i, got[i], events[i])
		}
	}
}

func TestReplayTruncatedLastLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	if err := j.Append(Event{Kind: EventAdmit, TenantID: "a", RequestID: "r1", At: at}); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃时写了一半的末行（非合法 JSON）。
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"kind":"admit","tenant_id":"a","requ`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	j2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	count := 0
	if err := j2.Replay(func(ev Event) { count++ }); err != nil {
		t.Fatalf("replay should tolerate trailing garbage: %v", err)
	}
	if count != 1 {
		t.Fatalf("replayed %d want 1 (truncated last line ignored)", count)
	}
}
