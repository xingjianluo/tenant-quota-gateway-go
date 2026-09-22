# tenant-quota-gateway-go

本地多租户请求准入网关：以内存状态模拟真实调用，在并发请求下保证鉴权、限流与配额扣减的**一致且可解释**的结论。

## 架构

```
cmd/gateway            演示程序（本地完整调用链）
internal/clock         可注入时间源（Real / Fake），测试不依赖真实等待
internal/auth          令牌鉴权与作用域判定
internal/quota         按租户隔离的速率 + 并发 + 总量配额
internal/gateway       准入编排：鉴权 → 配额 → 执行 → 记账，幂等去重 + 审计流水
```

调用链：`Gateway.Admit(req)` → `auth.Authorize` → `quota.Acquire`（原子判定+记账）→ `Handler` 执行 → `Release`（必释放并发槽）→ 失败时 `Refund`（归还配额与令牌）→ 追加审计流水并输出日志。

## 配额模型

每个租户独立配置 `quota.Limit`，状态完全隔离，互不影响：

| 字段 | 含义 | 模型 |
|---|---|---|
| `RatePerSec` / `Burst` | 速率限制 | 令牌桶：按注入时钟经过时间补充，上限 `Burst` |
| `MaxConcurrent` | 并发槽上限 | 执行期间占用一个槽，结束（无论成败）必须 `Release` |
| `TotalQuota` | 成功执行总量配额 | `Acquire` 时预扣，执行失败时 `Refund` 归还 |

一致性保证：

- **判定与记账原子**：`Acquire` 在单把互斥锁内完成"检查令牌/并发槽/配额 + 扣减"，任何并发下都不会超额放行（`internal/quota` 压测验证：500 并发竞争 50 配额，放行数恒等于 50）。
- **拒绝零副作用**：任一条件不满足即拒绝，不产生任何记账。
- **失败不泄漏**：执行失败归还预扣配额与令牌；并发槽无论成败都释放。
- **幂等**：相同 `RequestID` 的重复提交（含并发提交）共享首次判定，返回 `DUPLICATE`，不重复扣减、不重复执行下游。
- **可重放**：相同配置 + 相同输入序列，在新实例（模拟进程重启）上重放得到完全一致的判定序列与统计口径（见 `TestReplayDeterminism`）。

## 鉴权与拒绝码语义

令牌（`auth.Token`）承载 `TenantID`、`Scopes`、`ExpiresAt`、`Revoked`。鉴权失败**不触碰任何配额**。

| 判定码 | 含义 |
|---|---|
| `EXECUTED` | 鉴权通过、配额内、下游执行成功 |
| `EXEC_FAILED` | 下游执行失败，配额与令牌已归还，并发槽已释放 |
| `REJECT_TOKEN_UNKNOWN` | 令牌不存在 |
| `REJECT_TOKEN_EXPIRED` | 令牌已过期（按注入时钟判定） |
| `REJECT_TOKEN_REVOKED` | 令牌已被撤销 |
| `REJECT_SCOPE_DENIED` | 令牌不具备请求的作用域 |
| `REJECT_RATE_LIMITED` | 令牌桶为空，速率超限 |
| `REJECT_CONCURRENCY_LIMITED` | 并发槽已满 |
| `REJECT_QUOTA_EXHAUSTED` | 总量配额耗尽 |
| `DUPLICATE` | 重复提交，结果为首次判定的副本（`Reason` 中含原判定码） |

## 审计与日志

每次判定追加一条 `AuditRecord`（序号、时间、请求 ID、租户、作用域、判定码、原因、配额快照），可通过 `Gateway.Audit()` 读取。同时输出日志，包含**请求输入、判定依据与最终配额**：

```
audit seq=1 req=demo-1 token=tok-alpha scope=invoke tenant=alpha code=EXECUTED reason="ok" usage={tokens:99.00 inflight:0 used:1 remain:7}
```

## 本地验证

```bash
# 全部单元测试（含 race 检测；测试使用 Fake 时钟，不依赖真实等待）
go test ./... -race

# 运行演示：正常、越权、幂等、并发压测、租户隔离与审计汇总
go run ./cmd/gateway
```

测试覆盖：正常放行、未知/过期/撤销令牌、越权作用域、速率/并发/配额三类超额拒绝、执行失败退款、幂等重复（含并发重复）、并发压测不超发不泄漏、租户隔离、重放确定性、审计流水内容。
