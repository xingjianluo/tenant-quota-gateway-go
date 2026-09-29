package persist

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	if err := s.AppendReserved(ReservedEntry{At: at, RequestID: "r1", TokenID: "tok", Scope: "invoke", TenantID: "ten"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendDecision(DecisionEntry{At: at, RequestID: "r1", TenantID: "ten", Code: "EXECUTED", Seq: 1, QuotaUsed: 1, QuotaRemain: 9}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendAudit(DecisionEntry{At: at, RequestID: "r1", TenantID: "ten", Code: "EXECUTED", Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开，数据仍在。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	je, err := s2.ReadJournal()
	if err != nil || len(je) != 2 {
		t.Fatalf("journal=%v err=%v", je, err)
	}
	if je[0].Kind != KindReserved || je[1].Kind != KindDecision || je[1].QuotaUsed != 1 {
		t.Fatalf("bad journal: %+v", je)
	}
	last, err := s2.LastAuditSeq()
	if err != nil || last != 1 {
		t.Fatalf("last audit seq=%d err=%v", last, err)
	}
	ae, err := s2.ReadAudit()
	if err != nil || len(ae) != 1 || ae[0].RequestID != "r1" {
		t.Fatalf("audit=%v err=%v", ae, err)
	}
}

func TestTornWriteDropped(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.AppendDecision(DecisionEntry{RequestID: "r1", Seq: 1, Code: "EXECUTED"})
	s.AppendDecision(DecisionEntry{RequestID: "r2", Seq: 2, Code: "EXECUTED"})
	// 直接在末尾追加半行损坏数据，模拟 kill -9 时的半行写。
	f, err := os.OpenFile(filepath.Join(dir, journalFile), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("deadbe:garbage~partial"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	got, err := s.ReadJournal()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].RequestID != "r2" {
		t.Fatalf("torn write not handled: %+v", got)
	}
	// 再次读取时半行已被截断，只剩两条完好记录。
	got2, _ := s.ReadJournal()
	if len(got2) != 2 {
		t.Fatalf("journal not trimmed: %+v", got2)
	}
}

func TestSnapshotAtomic(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.ReadSnapshot(); err != nil || ok {
		t.Fatalf("want no snapshot, got ok=%v err=%v", ok, err)
	}
	if err := s.WriteSnapshot([]byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := s.ReadSnapshot()
	if err != nil || !ok || string(raw) != `{"v":1}` {
		t.Fatalf("snapshot=%q ok=%v err=%v", raw, ok, err)
	}
}
