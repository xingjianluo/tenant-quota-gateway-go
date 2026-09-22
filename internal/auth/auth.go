package auth

import (
	"sync"
	"time"

	"github.com/xingjianluo/tenant-quota-gateway-go/internal/clock"
	"github.com/xingjianluo/tenant-quota-gateway-go/internal/model"
)

type entry struct {
	claims  model.TokenClaims
	revoked bool
}

// Authenticator 负责令牌校验：身份、过期、撤销与作用域判定。
// 所有被拒绝的请求（过期/撤销/越权）都在配额记账之前返回，绝不消耗配额。
type Authenticator struct {
	clk    clock.Clock
	mu     sync.RWMutex
	tokens map[string]*entry
}

func New(clk clock.Clock) *Authenticator {
	return &Authenticator{clk: clk, tokens: make(map[string]*entry)}
}

// Issue 在本地登记一个令牌并返回其不透明串（仅用于本地演示/测试）。
func (a *Authenticator) Issue(tenantID, tokenID string, scopes []model.Scope, ttl time.Duration) (string, *model.TokenClaims) {
	token := "tok_" + tokenID
	copied := append([]model.Scope(nil), scopes...)
	claims := &model.TokenClaims{
		TokenID:   tokenID,
		TenantID:  tenantID,
		Scopes:    copied,
		ExpiresAt: a.clk.Now().Add(ttl),
	}
	a.mu.Lock()
	a.tokens[token] = &entry{claims: *claims}
	a.mu.Unlock()
	return token, claims
}

// Revoke 撤销令牌；撤销后任何使用该令牌的请求都必须被拒绝。
func (a *Authenticator) Revoke(token string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e, ok := a.tokens[token]; ok {
		e.revoked = true
	}
}

// Authenticate 只校验令牌本身（不含作用域）。
func (a *Authenticator) Authenticate(token string) (*model.TokenClaims, model.RejectCode) {
	if token == "" {
		return nil, model.RejectMissingToken
	}
	a.mu.RLock()
	e, ok := a.tokens[token]
	a.mu.RUnlock()
	if !ok {
		return nil, model.RejectUnknownToken
	}
	if e.revoked {
		return nil, model.RejectTokenRevoked
	}
	now := a.clk.Now()
	if !e.claims.ExpiresAt.After(now) {
		return nil, model.RejectTokenExpired
	}
	c := e.claims
	return &c, model.RejectNone
}

// scopeRank 定义作用域层级：admin 覆盖 write/read，write 覆盖 read。
var scopeRank = map[model.Scope]int{
	model.ScopeRead:  1,
	model.ScopeWrite: 2,
	model.ScopeAdmin: 3,
}

// Authorize 在已认证基础上判定令牌是否持有不低于要求的作用域。
func (a *Authenticator) Authorize(claims *model.TokenClaims, required model.Scope) model.RejectCode {
	need, ok := scopeRank[required]
	if !ok {
		return model.RejectScopeDenied
	}
	best := 0
	for _, s := range claims.Scopes {
		if r := scopeRank[s]; r > best {
			best = r
		}
	}
	if best < need {
		return model.RejectScopeDenied
	}
	return model.RejectNone
}
