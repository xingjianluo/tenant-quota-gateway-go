package persist

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNoSnapshotOnFirstStart(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadSnapshot(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("want ErrNoSnapshot, got %v", err)
	}
	evs, err := s.ReadEvents()
	if err != nil || len(evs) != 0 {
		t.Fatalf("empty log: evs=%v err=%v", evs, err)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	now := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	snap := &Snapshot{
		TakenAt: now, LastSeq: 7,
		Tenants:       map[string]TenantState{"a": {Tokens: 2.5, LastFill: now, InFlight: 1, Used: 3}},
		Seen:          map[string]CommitEvent{"r1": {Kind: EventCommit, Seq: 1, RequestID: "r1", Code: "EXECUTED"}},
		AuditTail:     []CommitEvent{{Kind: EventCommit, Seq: 6, At: now, RequestID: "r6", Code: "EXECUTED"}},
		AuditRetained: 4096,
	}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.LastSeq != 7 || got.Tenants["a"].Used != 3 || got.Tenants["a"].Tokens != 2.5 {
		t.Fatalf("snapshot mismatch: %+v", got)
	}
	if got.Seen["r1"].Code != "EXECUTED" || got.AuditTail[0].RequestID != "r6" {
		t.Fatalf("snapshot maps/tail mismatch: %+v", got)
	}
	// 再次保存必须整体替换而非残留旧内容。
	snap2 := &Snapshot{TakenAt: now, LastSeq: 1, Tenants: map[string]TenantState{}, Seen: map[string]CommitEvent{}}
	if err := s.SaveSnapshot(snap2); err != nil {
		t.Fatal(err)
	}
	got2, _ := s.LoadSnapshot()
	if got2.LastSeq != 1 || len(got2.Tenants) != 0 || len(got2.Seen) != 0 {
		t.Fatalf("snapshot not replaced cleanly: %+v", got2)
	}
}

func TestEventLogAppendAndRead(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	evs := []CommitEvent{
		{Kind: EventCommit, Seq: 1, RequestID: "r1", Code: "EXECUTED"},
		{Kind: EventDuplicate, Seq: 2, RequestID: "r1", Code: "DUPLICATE"},
	}
	for _, e := range evs {
		if err := s.AppendEvent(e); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ReadEvents()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Kind != EventCommit || got[1].Kind != EventDuplicate ||
		got[1].RequestID != "r1" {
		t.Fatalf("event round-trip mismatch: %+v", got)
	}
	if err := s.TruncateEvents(); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ReadEvents()
	if len(got) != 0 {
		t.Fatalf("events not truncated: %+v", got)
	}
}

func TestTornTailLineTolerated(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStore(dir)
	if err := s.AppendEvent(CommitEvent{Kind: EventCommit, Seq: 1, RequestID: "r1", Code: "EXECUTED"}); err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃留下的半行（没有结尾换行、JSON 不完整）。
	f, err := os.OpenFile(filepath.Join(dir, eventsName), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"Kind":0,"Seq":2,"RequestI`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	got, err := s.ReadEvents()
	if err != nil {
		t.Fatalf("torn tail should be tolerated: %v", err)
	}
	if len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("want only the 1 complete event, got %+v", got)
	}

	// 中间坏行不能被静默吞掉（坏行之后还有完整行，说明不是崩溃尾行）。
	dir2 := t.TempDir()
	path := filepath.Join(dir2, eventsName)
	bad := []byte("{\"Seq\":1}\n{not-json\n{\"Seq\":3}\n")
	if err := os.WriteFile(path, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	s3 := &FileStore{dir: dir2}
	if _, err := s3.ReadEvents(); err == nil {
		t.Fatal("corrupt mid-log line must return error")
	}
}
