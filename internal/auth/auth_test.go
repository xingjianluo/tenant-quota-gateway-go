package auth

import (
	"testing"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
)

var t0 = time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)

func setup() (*Store, *Authenticator, *clock.Fake) {
	c := clock.NewFake(t0)
	s := NewStore()
	s.Put(Token{ID: "tok-a", TenantID: "tenant-a", Scopes: []string{"read", "write"}, ExpiresAt: t0.Add(time.Hour)})
	s.Put(Token{ID: "tok-b", TenantID: "tenant-b", Scopes: []string{"read"}, ExpiresAt: t0.Add(time.Hour)})
	return s, NewAuthenticator(s, c), c
}

func TestAuthenticateOK(t *testing.T) {
	_, a, _ := setup()
	id, err := a.Authenticate("tok-a")
	if err != nil {
		t.Fatal(err)
	}
	if id.TenantID != "tenant-a" {
		t.Fatalf("wrong tenant: %s", id.TenantID)
	}
}

func TestUnknownToken(t *testing.T) {
	_, a, _ := setup()
	if _, err := a.Authenticate("nope"); err != ErrTokenUnknown {
		t.Fatalf("want ErrTokenUnknown, got %v", err)
	}
}

func TestExpiredToken(t *testing.T) {
	_, a, c := setup()
	c.Advance(2 * time.Hour)
	if _, err := a.Authenticate("tok-a"); err != ErrTokenExpired {
		t.Fatalf("want ErrTokenExpired, got %v", err)
	}
}

func TestRevokedToken(t *testing.T) {
	s, a, _ := setup()
	s.Revoke("tok-a")
	if _, err := a.Authenticate("tok-a"); err != ErrTokenRevoked {
		t.Fatalf("want ErrTokenRevoked, got %v", err)
	}
}

func TestScopeDenied(t *testing.T) {
	_, a, _ := setup()
	if _, err := a.Authorize("tok-b", "write"); err != ErrScopeDenied {
		t.Fatalf("want ErrScopeDenied, got %v", err)
	}
	if _, err := a.Authorize("tok-b", "read"); err != nil {
		t.Fatalf("read should be allowed: %v", err)
	}
}
