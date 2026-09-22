// Package auth 提供基于令牌的调用方鉴权与作用域判定。
package auth

import (
	"errors"
	"sync"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
)

// 鉴权失败原因，可按类型区分。
var (
	ErrTokenUnknown = errors.New("token unknown")
	ErrTokenExpired = errors.New("token expired")
	ErrTokenRevoked = errors.New("token revoked")
	ErrScopeDenied  = errors.New("scope denied")
)

// Token 承载租户身份与访问作用域。
type Token struct {
	ID        string
	TenantID  string
	Scopes    []string
	ExpiresAt time.Time
	Revoked   bool
}

// Identity 是鉴权通过后的调用方身份。
type Identity struct {
	TenantID string
	TokenID  string
}

// Store 是内存令牌仓库，并发安全。
type Store struct {
	mu     sync.RWMutex
	tokens map[string]Token
}

// NewStore 创建空令牌仓库。
func NewStore() *Store {
	return &Store{tokens: make(map[string]Token)}
}

// Put 写入或覆盖一个令牌。
func (s *Store) Put(t Token) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[t.ID] = t
}

// Revoke 撤销指定令牌。
func (s *Store) Revoke(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tokens[id]; ok {
		t.Revoked = true
		s.tokens[id] = t
	}
}

// Authenticator 校验令牌并判定作用域。
type Authenticator struct {
	store *Store
	clock clock.Clock
}

// NewAuthenticator 创建鉴权器。
func NewAuthenticator(store *Store, c clock.Clock) *Authenticator {
	return &Authenticator{store: store, clock: c}
}

// lookup 读取令牌副本。
func (a *Authenticator) lookup(tokenID string) (Token, error) {
	a.store.mu.RLock()
	defer a.store.mu.RUnlock()
	t, ok := a.store.tokens[tokenID]
	if !ok {
		return Token{}, ErrTokenUnknown
	}
	return t, nil
}

// checkValidity 校验撤销与过期。
func (a *Authenticator) checkValidity(t Token) error {
	if t.Revoked {
		return ErrTokenRevoked
	}
	if !a.clock.Now().Before(t.ExpiresAt) {
		return ErrTokenExpired
	}
	return nil
}

// Authenticate 校验令牌有效性，不判定作用域。
func (a *Authenticator) Authenticate(tokenID string) (Identity, error) {
	t, err := a.lookup(tokenID)
	if err != nil {
		return Identity{}, err
	}
	if err := a.checkValidity(t); err != nil {
		return Identity{}, err
	}
	return Identity{TenantID: t.TenantID, TokenID: t.ID}, nil
}

// Authorize 校验令牌有效性并判定其是否具备指定作用域。
func (a *Authenticator) Authorize(tokenID string, scope string) (Identity, error) {
	t, err := a.lookup(tokenID)
	if err != nil {
		return Identity{}, err
	}
	if err := a.checkValidity(t); err != nil {
		return Identity{}, err
	}
	for _, s := range t.Scopes {
		if s == scope || s == "*" {
			return Identity{TenantID: t.TenantID, TokenID: t.ID}, nil
		}
	}
	return Identity{}, ErrScopeDenied
}
