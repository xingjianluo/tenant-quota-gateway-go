package audit

import (
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
)

func TestLogCapturesInputBasisAndQuota(t *testing.T) {
	l := New()
	entry := Entry{
		RequestID: "req-1",
		TenantID:  "tenant-a",
		Input: model.Request{
			Token:          "tok_secretvalue",
			RequiredScope:  model.ScopeWrite,
			IdempotencyKey: "idem-9",
		},
		Basis: "quota check failed: RATE_LIMITED",
		Decision: model.Decision{
			Allowed:          false,
			RejectCode:       model.RejectRateLimited,
			TenantID:         "tenant-a",
			RequestID:        "req-1",
			At:               time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC),
			RateRemaining:    0,
			RateCapacity:     3,
			ConcurrencyInUse: 1,
			ConcurrencyMax:   2,
		},
	}
	l.Log(entry)

	got := l.Entries()
	if len(got) != 1 {
		t.Fatalf("entries=%d want 1", len(got))
	}
	if got[0].Basis != entry.Basis || got[0].Input.IdempotencyKey != "idem-9" {
		t.Fatalf("stored entry mismatch: %+v", got[0])
	}

	// 审计必须可区分失败原因并携带最终配额口径。
	if got[0].Decision.RejectCode != model.RejectRateLimited {
		t.Fatalf("code=%s", got[0].Decision.RejectCode)
	}
	if got[0].Decision.ConcurrencyMax != 2 || got[0].Decision.RateCapacity != 3 {
		t.Fatalf("quota snapshot missing: %+v", got[0].Decision)
	}
}

func TestMaskDoesNotLeakFullToken(t *testing.T) {
	tok := "tok_supersecrettokenvalue123"
	masked := mask(tok)
	if masked == tok || strings.Contains(masked, "supersecret") {
		t.Fatalf("masked token leaks contents: %q", masked)
	}
	if mask("") != "" {
		t.Fatal("empty token should stay empty")
	}
}
