package model

import "time"

// RejectCode 描述准入被拒绝的原因类型。
type RejectCode string

const (
	RejectNone            RejectCode = ""                 // 放行
	RejectMissingToken    RejectCode = "MISSING_TOKEN"    // 未携带令牌
	RejectTokenExpired    RejectCode = "TOKEN_EXPIRED"    // 令牌已过期
	RejectTokenRevoked    RejectCode = "TOKEN_REVOKED"    // 令牌已撤销
	RejectUnknownToken    RejectCode = "UNKNOWN_TOKEN"    // 无法识别的令牌
	RejectScopeDenied     RejectCode = "SCOPE_DENIED"     // 访问作用域不足（越权）
	RejectRateLimited     RejectCode = "RATE_LIMITED"     // 超过租户速率配额
	RejectConcurrencyFull RejectCode = "CONCURRENCY_FULL" // 超过租户并发配额
	RejectUnknownTenant   RejectCode = "UNKNOWN_TENANT"   // 租户未配置配额（不应放行）
)

func (c RejectCode) String() string { return string(c) }

// Scope 表示令牌的访问作用域。
type Scope string

const (
	ScopeRead  Scope = "read"
	ScopeWrite Scope = "write"
	ScopeAdmin Scope = "admin"
)

// TokenClaims 为令牌承载的身份信息。
type TokenClaims struct {
	TokenID   string
	TenantID  string
	Scopes    []Scope
	ExpiresAt time.Time
}

// Request 为进入网关的准入请求输入。
type Request struct {
	Token          string
	RequiredScope  Scope
	IdempotencyKey string // 可选；非空时启用重复提交判定
}

// Decision 为一次准入判定的可解释结论。
type Decision struct {
	Allowed    bool
	RejectCode RejectCode
	Reason     string
	TenantID   string
	RequestID  string
	At         time.Time

	// 判定时刻租户配额快照（便于解释最终配额）。
	RateRemaining    float64
	RateCapacity     float64
	ConcurrencyInUse int
	ConcurrencyMax   int

	// 幂等重放标记：同一 IdempotencyKey 再次到达时不重复扣配额。
	Replayed bool
}
