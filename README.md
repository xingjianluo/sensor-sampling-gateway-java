# 传感器采集网关（Sensor Sampling Gateway）

一套**完全本地运行、不依赖真实硬件/外部服务/凭据**的传感器采集网关。它在
**多传感器并发采样、有界缓冲、设备故障**并存的情况下，仍给出**一致、可解释、
可复现**的采样结论。

- Java 21+（本地在 JDK 25 上构建）、Spring Boot 3.4.x、Maven
- 无数据库、无消息中间件、无网络依赖；“设备”由确定性脚本模拟
- 全部行为可通过单元/并发测试与只读 REST 端点在本地复现

---

## 1. 总体架构

```
                 每个传感器一个生产者线程                         N 个消费者线程
 ScriptedSensor ──► 断路器(隔离?) ──► 有界重试+退避 ──► 有界缓冲(BoundedBuffer) ──► 校验 ──► 校准 ──► 裁决(Verdict)
   (确定性脚本)        CircuitBreaker     RetryPolicy        容量/溢出策略可配       ReadingValidator  CalibrationModel   VerdictLog
        ▲                                                                                                       │
        │                                                                                                       ▼
 ManualClock(测试)                                                                   统计 StatsCollector + 裁决日志 VerdictLog + 对账总账 ReconcileLedger
```

- `model`：`RawReading` / `Verdict` / `SensorConfig` / `CalibrationModel` /
  `RejectReason` / `OverflowPolicy` / `GatewayStats` 等不可变类型。
- `sensor`：`Sensor` 抽象与 `ScriptedSensor`（按脚本逐拍产出读数或故障）。
- `reliability`：`CircuitBreaker`（故障隔离）、`RetryPolicy`（有界退避）、`Sleeper`（可替换睡眠）。
- `buffer`：`BoundedBuffer`（容量 + 三种溢出策略，统一关闭/排空语义）。
- `pipeline`：`ReadingValidator`（固定优先级校验）、`VerdictLog`（有界环形裁决记录）。
- `reconcile`：`ReconcileLedger` / `ReconcileQuery` / `ReconcileReport` /
  `ReconcileEntry`（只增不改、计数与明细分存的采样对账层，见第 7 节）。
- `gateway`：`SamplingGateway`（生命周期、并发协调与对账入账）、`GatewayConfig`
  （配置快照）、`GatewayProperties`（`application.properties` 绑定）、`StatsCollector`（统一计数）。
- `web`：`SamplingController` 与 `ReconciliationController`，本地验证用只读端点。

---

## 2. 采样模型与拒绝原因（可区分、不静默）

每次采样尝试用 `(sensorId, tick)` 唯一标识，`tick` 在该传感器生产者线程内**单调递增**，
因此并发下也不会重复或错位。每个 `sensor#tick` **恰好**终结为一条 `Verdict`：

- **接受**：`accepted=true`，含校准后的值 `calibratedValue`；
- **拒绝**：`accepted=false`，且必须带下列**可区分**原因之一，绝不静默丢弃：

| 原因 | 触发规则 |
|------|----------|
| `DATA_CORRUPT` | 读数校验和与重算值不一致（数据损坏，数值不可信） |
| `TIMESTAMP_ANOMALY` | `|读数时间戳 − 网关时钟| > timestampToleranceMs` |
| `OUT_OF_RANGE` | 值超出 `[rangeMin, rangeMax]`（边界值有效，闭区间） |
| `BUFFER_OVERFLOW` | 有界缓冲满，按溢出策略拒绝新读数或驱逐旧读数 |
| `SENSOR_ISOLATED` | 传感器断路器 OPEN（已隔离）期间不再受理 |
| `ACQUISITION_FAILED` | 采样动作在有界重试后仍失败 |

**固定校验优先级**（保证同输入同结论）：`DATA_CORRUPT` → `TIMESTAMP_ANOMALY` → `OUT_OF_RANGE`。
即使一条读数同时存在多种问题，也稳定地只报最高优先级原因。

每条 `Verdict` 都带 `basis` 文本，打印**输入与判定依据**，例如：

```
input{sensor=temp-1,tick=3,ts=...,value=99.0000,checksum=...,now=...,drift=0ms};
rule=99.0000 outside [-40.0000,80.0000] -> OUT_OF_RANGE
```

### 2.1 数据完整性

`RawReading` 携带 `checksum = fnv1a(tick, timestampMs, value)`。模拟传感器可在脚本中
翻转校验和制造“数据损坏”。校验和在消费者侧重算比对。

### 2.2 时间戳

- 生产环境使用 `SystemClock`；
- 测试使用 `ManualClock`（时间只随测试推进），因此时间戳异常判定完全确定、可复现。

---

## 3. 校准模型与可复现性

校准为线性模型，带**版本号**：

```
calibrated = gain(sensorId) * raw + offset(sensorId)
```

- 校准模型与所有规则封装在不可变的 `GatewayConfig` 快照中，含 `rulesVersion`；
- 每条裁决记录作出该裁决时的 `rulesVersion` 与校准模型版本；
- **相同输入 + 相同规则版本 ⇒ 完全相同的裁决序列**
  （见 `sameInputsAndRulesVersion_produceReproducibleVerdicts`）；
- 校准/配置更新 = 整体替换为新版本快照（`restart`），历史裁决可按版本区分复核。

---

## 4. 有界缓冲与背压（确定、可观测、可配置）

缓冲容量与溢出策略可配置（`gateway.buffer-capacity`、`gateway.overflow-policy`）：

| 策略 | 满时行为 | 计数处理 |
|------|----------|----------|
| `BLOCK` | 生产者等待空位；关闭后被唤醒并拒绝 | 正常时不丢；关闭期拒绝记 `BUFFER_OVERFLOW` |
| `DROP_OLDEST` | 驱逐队列最旧读数为新读数让位 | 新读数计入队；被驱逐旧读数单独形成 `BUFFER_OVERFLOW` 裁决 |
| `DROP_NEWEST` | 直接拒绝最新读数 | 该读数记 `BUFFER_OVERFLOW` |

**关闭语义（不重不漏的基础）**：`close()` 后不再受理任何新读数并唤醒全部等待者；
已在队列中的读数允许消费者**排空（drain）**，排空后 `poll` 返回 null。

### 4.1 统一计数不变量

所有计数由单一的 `StatsCollector` 以原子长整型维护，杜绝重复/丢失计数。
每个采样尝试恰好终结于一次计数：

```
submitted  = enqueued + rejectedTotal
consumed   = accepted   + (DATA_CORRUPT | TIMESTAMP_ANOMALY | OUT_OF_RANGE)
enqueued   = consumed   + droppedOldest      // DROP_OLDEST 的驱逐发生在消费前
dropped    = DROP_OLDEST 驱逐数 + DROP_NEWEST(及关闭期 BLOCK) 拒绝数
```

集成测试对每一场景都断言 `submitted = enqueued + rejected` 与“无重复 key”。

---

## 5. 重试、退避与故障隔离

- **瞬时故障**（`SensorFaultException.recoverable=true`）：在 `RetryPolicy` 预算内
  按 `min(initial * multiplier^(attempt-1), maxBackoff)` 指数退避后重试；
  退避抖动由可注入的 `LongSupplier` 决定（测试固定为 0，生产可随机），成功即恢复，
  断路器保持 `CLOSED`（见 `TransientRecoveryIntegrationTest`）。
- **持续故障**（`recoverable=false`）或重试耗尽：记 `ACQUISITION_FAILED`，
  并向对应传感器的 `CircuitBreaker` 记一次失败。
- **断路器三态**：
  - `CLOSED`：正常；连续失败达到阈值 → `OPEN`（隔离，读数直接记 `SENSOR_ISOLATED`）；
  - `OPEN`：冷却期内拒绝一切采样；冷却期满**仅原子放行一次探测** → `HALF_OPEN`；
  - `HALF_OPEN`：探测成功 → `CLOSED` 恢复；探测失败 → 回到 `OPEN` 重新冷却。
- **隔离互不影响**：断路器是每传感器一个；某传感器持续失败被隔离，不影响其他传感器
  （见 `persistentFailure_isolatesSensor_withoutAffectingOthers`）。

---

## 6. 关闭与重启（已受理数据不重不漏）

- `stop()`：置停止位 → `buffer.close()` 停止受理 → 等待消费者排空已入队数据
  → 关闭线程池。排空后缓冲为 0（见 `LifecycleRestartTest`）。
- `requestStop()`（测试/精确控制用）：请求生产者在当前拍完成后停止，但**不关闭缓冲**。
- `SamplingLimitReached`：传感器侧的协作式停止信号（非故障），生产者干净退出，
  不产生裁决、不计入任何失败统计，使采样拍数在源头精确可复现。
- `restart(newConfig[, newSensors])`：先优雅停止并排空旧数据，再以新配置快照重建
  缓冲/断路器并启动；生命周期计数保留，裁决日志按新一代重置，`tick` 从 0 重新计。

---

## 7. 采样对账（Reconciliation）

在采样/裁决链路之上新增一层**只增不改**的对账总账 `reconcile.ReconcileLedger`，
供复盘核对：对**任意一次运行**或**任意一组筛选条件**（运行代次 / 传感器 / 规则版本 /
时间范围），都能拿到生产总数、接受数、按原因分列的拒绝数、因背压丢弃/顶掉的数量，
以及真正交付与被拒绝的逐条明细。

### 7.1 运行代次（runId）与入账

- 每次 `start()` / `restart()` 开启一个**新一代运行**，`runId` 从 1 单调递增；
  `stop()` **封口**当前运行（只记录结束时间，计数与明细一律不动）。
- 网关产生的每条 `Verdict` 在原裁决点同步入账为一条 `ReconcileEntry`
  （`runId + recordedAtMs + Verdict + evicted`）。同一运行内 `sensor#tick` 唯一，
  总账对重复键直接抛错，从源头杜绝同一条读数算两次。
- 优雅关闭→重启后，旧代次账目仍在，可独立回溯；`restart` 不清除对账总账
  （注意这与 `VerdictLog` 按代次重置不同：后者只是排障用的最近裁决窗口）。
- 运行中可用 `switchRulesVersion(newConfig)` **热切换规则版本**（同一 runId 内，
  不重置缓冲/断路器/线程）。切换前的裁决带旧版本、切换后的带新版本，二者以裁决上的
  `rulesVersion` 分界，互不重叠，每拍仍只有一条结论。

### 7.2 对账口径（与有效性判定完全一致）

接受/拒绝的判定唯一来源是消费者侧的 `ReadingValidator` + 断路器 + 背压策略，
对账层不做第二次判定、也不放宽口径：

- 只有**通过当前规则版本判定**的读数才进入“接受/交付”；
- 数值非法、超量程、时间戳异常、数据损坏，以及采集失败、传感器隔离、缓冲背压，
  全部以**可区分原因**进入拒绝明细（原因表见第 2 节）；
- DROP_NEWEST 的“拒入”与 DROP_OLDEST 的“顶掉”都记 `BUFFER_OVERFLOW`，
  顶掉条目额外带 `evicted=true` 以便区分两种背压去向。

### 7.3 账目恒等式（不闭合直接抛错，绝不返回对不上的账）

对任意非时间范围筛选的报告 `ReconcileReport`：

```
produced                     == accepted + Σ rejectedByReason[reason]
dropped                      == rejectedByReason[BUFFER_OVERFLOW]
交付明细条数 + 已截断缺失数    == accepted
拒绝明细条数 + 已截断缺失数    == Σ rejectedByReason
```

按 run/传感器/规则版本筛选时，计数取自**精确聚合立方体**，与明细保留无关，
`countersExact` 恒为 true；因此“顶掉/丢弃”的数量即使明细已过期也不会少算或漏算。

### 7.4 保留与容量边界（确定、可配置、不篡改历史）

总账同时维护两处存储：

| 存储 | 容量 | 超限行为 | 被保留策略影响的内容 |
|------|------|----------|----------------------|
| 精确聚合（run×传感器×规则版本） | 随**维度数**增长，与读数条数无关；运行封口后释放逐读数去重表 | 不删，历史数字永久可查 | 计数永不被修改或删除 |
| 明细窗口 | `gateway.reconcile-detail-capacity`（默认 100000） | FIFO 顶掉最旧**明细** | 只影响逐条明细，报告以 `complete` / `rejectedMissing` / `deliveredMissing` 显式告知缺了多少 |

- 保留策略**只顶明细、不动数字**：已经出具的聚合账目不会被悄悄改掉或删掉。
- 带**时间范围**的查询只能基于保留窗口统计：若查询下界早于最旧保留明细时间，
  报告 `complete=false`、`countersExact=false`，明确表示“数字只覆盖保留窗口”；
  数字彼此仍闭合，但不会被伪装成全量结果。

### 7.5 只读端点

```bash
# 全部 / 按运行 / 按传感器 / 按规则版本 / 按时间范围（参数可任意组合）
curl -s "http://localhost:8080/api/reconcile/report"
curl -s "http://localhost:8080/api/reconcile/report?runId=1"
curl -s "http://localhost:8080/api/reconcile/report?sensor=temp-1&rulesVersion=rules-v1"
curl -s "http://localhost:8080/api/reconcile/report?fromMillis=...&toMillis=..."
curl -s "http://localhost:8080/api/reconcile/runs"        # 所有运行代次
```

`ReconcileReport` 字段：`produced / accepted / rejectedByReason / dropped /
rejectedDetail / deliveredDetail / countersExact / complete /
rejectedMissing / deliveredMissing / retainedDetailCount`。

### 7.6 与现有能力的兼容范围

- **并发采样**：每拍一个 `sensor#tick`，入账随裁决同步，重复键被拒绝；
- **背压/缓冲**：DROP_OLDEST（顶掉，`evicted=true`）与 DROP_NEWEST（拒入）都落账；
  BLOCK 正常不丢，关闭期拒入同样记 `BUFFER_OVERFLOW`；
- **故障隔离/恢复**：`ACQUISITION_FAILED` 与 `SENSOR_ISOLATED` 分列，逐传感器可对；
- **版本切换**：热切换（同 runId 多版本）与重启切换（新 runId 单版本）都可按
  `rulesVersion` / `runId` 分别核对；
- **优雅重启**：封口运行不可再写入但永久可查，跨代汇总“只算一次”。

---

## 8. 本地验证

### 8.1 构建与测试（离线）

```bash
mvn test -o          # 使用本地 Maven 仓库；联网时可去掉 -o
```

测试共 33 个，覆盖：

| 测试类 | 覆盖点 |
|--------|--------|
| `ReadingValidatorTest` | 三类拒绝原因可区分、边界值有效、固定优先级；日志打印输入与依据 |
| `BoundedBufferTest` | 三种溢出策略的确定性、BLOCK 等待、关闭后排空与拒绝新读数 |
| `CircuitBreakerTest` | 阈值隔离、冷却后单次探测、成功恢复/失败再隔离 |
| `RetryRecoveryTest` | 瞬时故障重试恢复、瞬时 vs 持续故障可区分 |
| `ConcurrentSamplingIntegrationTest` | 并发采样不重不漏、DROP_OLDEST/NEWEST 压力、持续故障隔离不影响他者、同规则版本可复现 |
| `LifecycleRestartTest` | 优雅关闭排空、重启不重不漏、规则版本切换 |
| `TransientRecoveryIntegrationTest` | 端到端瞬时故障经退避重试后恢复、不被隔离 |
| `ReconcileLedgerTest` | 对账恒等式、run/传感器/规则版本筛选、FIFO 保留边界与缺失计数、时间范围完整性、重复键拒绝、封口运行 |
| `ReconciliationIntegrationTest` | 并发+缓冲压力（顶掉/拒入）+故障隔离/恢复+优雅重启可回溯+运行中热切规则版本，账目全程闭合、不合规读数全落拒绝明细 |

测试日志会打印 `VERDICT ACCEPT/REJECT:<原因>` 及 `[场景] submitted=... byReason=...`，
便于逐条核对输入、判定依据与计数。

### 8.2 启动应用并观察（无硬件）

```bash
mvn spring-boot:run
# 另开终端：
curl -s http://localhost:8080/api/gateway/status
curl -s http://localhost:8080/api/gateway/stats
curl -s http://localhost:8080/api/gateway/verdicts                 # 最近裁决（含 basis）
curl -s "http://localhost:8080/api/gateway/verdicts?sensor=temp-1" # 按传感器
curl -s "http://localhost:8080/api/gateway/verdicts/temp-1%230"    # 按 sensor#tick 精确查
curl -s http://localhost:8080/api/reconcile/runs
curl -s "http://localhost:8080/api/reconcile/report?runId=1"       # 该代次完整对账
```

应用启动后，三个脚本化模拟传感器会周期性演示正常/超量程/坏时间戳/数据损坏/
瞬时故障/持续故障等全部裁决原因。`Ctrl+C`（SIGTERM）触发优雅关闭、排空与运行封口。

### 8.3 配置（`src/main/resources/application.properties`）

关键项：`rules-version`、`buffer-capacity`、`overflow-policy`、`consumer-threads`、
`retry-*`（重试退避）、`breaker-threshold` / `breaker-cooldown-ms`（隔离）、
`shutdown-drain-timeout-ms`、`verdict-log-capacity`、
`reconcile-detail-capacity`（对账明细保留窗口，默认 100000），以及每个传感器的
`type / period-ms / range-min/max / timestamp-tolerance-ms / calibration-gain/offset`。

---

## 9. 关键设计取舍

- **单一计数源**：缓冲自身不计数，全部在网关侧由 `StatsCollector` 记录，避免重复计数。
- **tick 由生产者内单调分配**：天然消除并发下的重复/错位。
- **控制流与故障流分离**：`SamplingLimitReached` 不是异常故障，不污染统计。
- **时间/睡眠/抖动/传感器全部可注入**：测试在确定时钟与零等待退避下快速复现。
- **配置即不可变快照**：规则版本随裁决落盘，更新可追溯、结论可复现。
- **对账计数与明细分存**：精确聚合不受保留窗口影响，保证长跑/高压下计数恒闭合；
  明细截断显式暴露（`complete`/缺失条数），而不是悄悄删改账目。
- **网关级拍数预算 `maxTicksPerSensor`**：在断路器/采样之前生效（隔离拍也受限），
  让一次运行的边界在测试里确定，不依赖线程调度竞争（默认 0=不限）。
