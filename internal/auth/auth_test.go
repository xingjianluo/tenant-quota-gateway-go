package auth

import (
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
)

func TestAuthenticate(t *testing.T) {
	base := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(base)
	a := New(clk)

	tok, claims := a.Issue("t1", "id1", []model.Scope{model.ScopeRead}, time.Minute)
	if claims.TenantID != "t1" || tok == "" {
		t.Fatalf("issue returned bad values: %q %+v", tok, claims)
	}

	cases := []struct {
		name string
		run  func() model.RejectCode
		want model.RejectCode
	}{
		{"missing token", func() model.RejectCode {
			_, c := a.Authenticate("")
			return c
		}, model.RejectMissingToken},
		{"unknown token", func() model.RejectCode {
			_, c := a.Authenticate("tok_nope")
			return c
		}, model.RejectUnknownToken},
		{"valid token", func() model.RejectCode {
			_, c := a.Authenticate(tok)
			return c
		}, model.RejectNone},
		{"expired token", func() model.RejectCode {
			clk.Advance(time.Minute + time.Second)
			_, c := a.Authenticate(tok)
			return c
		}, model.RejectTokenExpired},
	}
	for _, tc := range cases {
		if got := tc.run(); got != tc.want {
			t.Errorf("%s: got %s want %s", tc.name, got, tc.want)
		}
	}
}

func TestRevoke(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	a := New(clk)
	tok, _ := a.Issue("t", "id", []model.Scope{model.ScopeRead}, time.Hour)
	a.Revoke(tok)
	if _, c := a.Authenticate(tok); c != model.RejectTokenRevoked {
		t.Fatalf("got %s want TOKEN_REVOKED", c)
	}
}

func TestAuthorizeScopeHierarchy(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	a := New(clk)

	readTok, readClaims := a.Issue("t", "r", []model.Scope{model.ScopeRead}, time.Hour)
	writeTok, _ := a.Issue("t", "w", []model.Scope{model.ScopeWrite}, time.Hour)
	adminTok, _ := a.Issue("t", "a", []model.Scope{model.ScopeAdmin}, time.Hour)
	_ = readTok

	cases := []struct {
		token    string
		required model.Scope
		want     model.RejectCode
	}{
		{readTok, model.ScopeRead, model.RejectNone},
		{readTok, model.ScopeWrite, model.RejectScopeDenied},
		{writeTok, model.ScopeRead, model.RejectNone},
		{writeTok, model.ScopeWrite, model.RejectNone},
		{writeTok, model.ScopeAdmin, model.RejectScopeDenied},
		{adminTok, model.ScopeAdmin, model.RejectNone},
		{adminTok, model.ScopeWrite, model.RejectNone},
	}
	for _, tc := range cases {
		claims, c := a.Authenticate(tc.token)
		if c != model.RejectNone {
			t.Fatalf("authenticate %s: %s", tc.token, c)
		}
		if got := a.Authorize(claims, tc.required); got != tc.want {
			t.Errorf("token=%s required=%s got=%s want=%s",
				tc.token, tc.required, got, tc.want)
		}
	}
	_ = readClaims
}
