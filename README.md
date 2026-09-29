# tenant-quota-gateway-go

本地多租户请求准入网关：以内存状态模拟真实调用，在并发请求下保证鉴权、限流与配额扣减的**一致且可解释**的结论；并通过本地文件的 **journal + 周期快照 + 只追加审计日志**，让进程重启后像"同一个网关"继续服务。

## 架构

```
cmd/gateway            演示程序（纯内存 / -data 持久化两种模式）
internal/clock         可注入时间源（Real / Fake），测试不依赖真实等待
internal/auth          令牌鉴权与作用域判定
internal/quota         按租户隔离的速率 + 并发 + 总量配额（含快照导入/导出与增量重放）
internal/persist       本地持久化：CRC 行格式的 journal、原子快照、只追加审计日志
internal/gateway       准入编排：鉴权 → 预留记账 → 执行 → 终局，幂等去重 + 审计 + 崩溃恢复
```

调用链（持久化模式）：`Gateway.Admit(req)` → `auth.Authorize` → `quota.Acquire`（原子判定+记账）→ **`reserved` 事件 fsync 落盘** → `Handler` 执行 → `Release`（必释放并发槽）/ 失败 `Refund` → 追加内存审计并把 **`decision` 终局事件 fsync 到 journal 与 audit**。每 128 个终局写一次快照并截断 journal（`defaultSnapshotEvery`）。

## 重启恢复（持久化语义）

数据目录（`gateway.Open(ctx, dir, …)` 或 `cmd/gateway -data dir`）下三个文件：

| 文件 | 写入方式 | 内容 | 截断策略 |
|---|---|---|---|
| `journal.log` | 顺序追加 + fsync | `reserved`（记账后、执行前）与 `decision`（终局，含 DUPLICATE） | 每次快照后截断，窗口恒 ≤ 一个快照间隔 |
| `snapshot.json` | 临时文件 + rename + 目录 fsync（原子） | 各租户配额账本、幂等表（RequestID→首次判定）、快照瞬间在飞的预留、审计 seq | 整文件周期重写 |
| `audit.log` | 顺序追加 + fsync | 全部终局判定，**永不截断**，seq 从 1 连续 | 不截断 |

启动恢复顺序（`gateway/recovery.go`）：

1. **装快照**：导入配额账本、幂等表、在飞预留与 seq。
2. **重放 journal 窗口**：对 `EXECUTED` / `EXEC_FAILED` 按**事件时间**增量重放一次"预留记账→终局"（成功净扣一次、失败净额为 0、执行期间占用并发槽），令牌桶按注入时钟续算；鉴权/配额拒绝与 `DUPLICATE` 不记账，只重建幂等表。
3. **回滚未决预留**：reserved 有记录却没有 decision 的请求，即"执行中途被 kill -9 / 掉电"。
   - 预留已在**快照账本**里的：`Release + Refund` 真正归还并发槽、令牌与总量配额；
   - 仅在**快照后 journal 窗口**里的：窗口账本由终态重建，本就不含其记账，不重复退款。
   两类都会进入 `RecoveryReport.RolledBack` 并打日志；这些请求没有终局结论、不进幂等表，客户端用同一 RequestID 重试会作为新请求正常执行（**at-least-once**，下游需自行幂等）。
4. **补齐审计缺尾**：仅当崩溃落在"decision 已落 journal、audit 写入前"的极窄窗口，按 journal 顺序把缺失终局追加进 audit，保证 seq 连续。
5. journal/audit 末尾若有 CRC 校验失败的半行写（torn write），直接丢弃并截断，不影响此前已 fsync 的结论。

### 保留 vs 不保留的状态

| 状态 | 是否跨重启保留 | 说明 |
|---|---|---|
| 总量配额已用量 / 剩余量 | ✅ | 快照账本 + journal 增量重放 |
| 速率令牌桶余额与时间轴 | ✅ | 按注入时钟与事件时间续算，重启本身不回补、不重置 |
| 并发槽占用 | ✅ 语义归零 | 正常请求终局已释放；崩溃在飞的预留按回滚释放，不会永久占槽 |
| 幂等结论（每个 RequestID 的首次判定） | ✅ | 含鉴权/配额拒绝与执行失败；重启后再提交仍判 DUPLICATE，返回首次结论副本 |
| 审计流水（输入、判定依据、最终配额） | ✅ | 只追加，全量保留，seq 连续；DUPLICATE 也逐条记录 |
| 审计 seq 计数器 | ✅ | 重启后继续递增，不复用、不空洞 |
| 令牌仓库 / 租户配额 `Limit` 配置 | ❌（部署期配置） | 由程序启动时重新装配；见下方兼容范围 |
| 正在执行的 Handler 本身 | ❌ | 进程死亡即中止；按"未决预留回滚 + 客户端重试"处理 |

### 一致性 vs 启动开销的取舍

- **选择 fsync + 周期快照，而不是每次判定全量重写**：终局提交点串行 fsync（在 `Gateway` 锁内），换来的是"进程在任意点被杀，账本都不会多扣、不会永久占槽"。代价是提交吞吐受限于磁盘 fsync——对本地单进程演示网关可接受，分布式场景应换成分段 WAL / 分组提交 / 外部存储。
- **恢复重放窗口被钳制在一个快照间隔（默认 128 条终局）**，不随总历史变长；`RecoveryReport.Replayed ≤ 128`（整除点为 0）。全量审计不进快照、不参与恢复记账，启动只读 audit 末尾 64KB 取最后 seq，因此**启动时间 O(快照窗口 + 尾部读取)，不随审计历史增长**（见 `TestRecoveryWindowBounded`）。
- **唯一随历史线性增长的是幂等表（seen）**：要对"历史任意 RequestID"判重，就必须记住它们；快照整文件重写的摊销成本为 O(历史/快照间隔)。这是本地单机版的刻意边界，超大规模应改为分桶存储或外部 KV。
- **只保证本地单机、单写者**：不处理两个进程同时写同一目录（无文件锁选主），也不提供多副本。

### 兼容范围

- 磁盘格式为带 CRC32 的 JSON 行（journal/audit）与单个 JSON 快照，字段带 `omitempty` 演进；同版本、同模块可直接恢复。
- 恢复时必须用**与崩溃前一致或更宽松**的 `quota.Limit`：改小 `TotalQuota` / `Burst` / `MaxConcurrent` 不会回滚历史已用量，重放记账若在新配置下无法通过会记录告警，账本以快照与事件为准。令牌与配额配置属部署期输入，不随状态持久化。
- `gateway.New(...)`（不传目录）仍是纯内存模式，行为与早期版本一致。

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
- **幂等**：相同 `RequestID` 的重复提交（含并发提交、含跨进程重启）共享首次判定，返回 `DUPLICATE`，不重复扣减、不重复执行下游。
- **可重放**：相同配置 + 相同输入序列，在新实例（模拟进程重启）上重放得到完全一致的判定序列与统计口径（见 `TestReplayDeterminism`）。
- **崩溃可恢复**：持久化模式下记账先于执行落盘（reserved），任意时刻被杀，重启后账本不多扣、不泄漏并发槽，未决请求回滚后可安全重试。

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
| `DUPLICATE` | 重复提交，返回首次判定的副本（`Reason` 中含原判定码）；重复本身也记一条审计，审计中的配额取提交时的当前账本 |
| `PERSIST_FAILED` | 持久化模式下预留事件无法落盘：撤回本次记账并拒绝，避免带着无法恢复的记账继续 |

## 审计与日志

每次判定（包括 `DUPLICATE` 重复判定）追加一条 `AuditRecord`（序号、时间、请求 ID、token、租户、作用域、判定码、原因、配额快照）。纯内存模式保存在实例切片中；持久化模式同步写入只追加的 `audit.log`，`Gateway.Audit()` 从磁盘全量读取，因此重启后仍能查到全部历史且 seq 连续。日志包含**请求输入、判定依据与最终配额**：

```
audit seq=1 req=demo-1 token=tok-alpha scope=invoke tenant=alpha code=EXECUTED reason="ok" usage={tokens:99.00 inflight:0 used:1 remain:7}
```

## 本地验证

```bash
# 全部单元测试（含 race 检测；测试使用 Fake 时钟，不依赖真实等待、不依赖外部服务）
go test ./... -race

# 纯内存演示：正常、越权、幂等、并发压测、租户隔离与审计汇总
go run ./cmd/gateway

# 持久化演示：第一遍跑出来的数据落在临时目录
D=$(mktemp -d)
go run ./cmd/gateway -data "$D"
# 再跑一遍（等价于进程重启）：观察第 0 节"恢复"结论——同一批 demo-* 全部判为
# DUPLICATE，配额/审计 seq 接着上次继续；可反复执行，账本与审计持续累积
go run ./cmd/gateway -data "$D"
ls -la "$D"   # journal.log / snapshot.json / audit.log
```

测试覆盖（除原有场景外，新增于 `internal/gateway/recovery_test.go`、`internal/persist/persist_test.go`）：

- 纯内存既有场景：正常放行、未知/过期/撤销令牌、越权作用域、速率/并发/配额三类超额拒绝、执行失败退款、幂等重复（含并发重复）、并发压测不超发不泄漏、租户隔离、重放确定性、审计流水内容——**全部保持通过，无回归**。
- 重启延续：配额已用/剩余/并发槽与审计 seq 跨重启一致（`TestRestartStateContinuity`）；同一批混合输入重启前后统计口径相同（`TestSameInputsConsistentAcrossRestart`）。
- 跨重启幂等与审计：已处理请求重启后再提交仍判 DUPLICATE 且返回首次结论，重复判定有连续、含输入/依据/最终配额的审计记录，多次重启亦然（`TestDuplicateAfterRestartAudited`）。
- 中途强杀恢复：reserved 已落盘而无终局时，重启后回滚令牌/配额/并发槽，且分别覆盖"journal 窗口内"与"已进快照、journal 已截断"两种来源（`TestCrashMidRequestRollback`、`TestCrashMidRequestRollbackAfterSnapshot`）。
- 快照与启动开销：快照触发后走快照路径恢复，重放窗口被钳制、不随历史线性变慢（`TestRestartAfterSnapshot`、`TestRecoveryWindowBounded`）。
- 速率令牌桶按注入时钟跨重启续算（`TestRateBucketAcrossRestart`）；journal 末尾半行写被 CRC 识别并丢弃（`TestTornJournalRecovered`、`persist.TestTornWriteDropped`）；执行失败结论与退款跨重启保留（`TestExecFailureAcrossRestart`）；持久化模式高并发不超发、可干净重启（`TestPersistentConcurrentNoOverGrant`，-race）。
