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
 ManualClock(测试)                                                                                   统计 StatsCollector + 日志
```

- `model`：`RawReading` / `Verdict` / `SensorConfig` / `CalibrationModel` /
  `RejectReason` / `OverflowPolicy` / `GatewayStats` 等不可变类型。
- `sensor`：`Sensor` 抽象与 `ScriptedSensor`（按脚本逐拍产出读数或故障）。
- `reliability`：`CircuitBreaker`（故障隔离）、`RetryPolicy`（有界退避）、`Sleeper`（可替换睡眠）。
- `buffer`：`BoundedBuffer`（容量 + 三种溢出策略，统一关闭/排空语义）。
- `pipeline`：`ReadingValidator`（固定优先级校验）、`VerdictLog`（有界环形裁决记录）。
- `reconcile`：`ReconciliationStore`（按“代次/批次”组织的采样对账台账）、
  `ReconciliationEntry/Report/Filter`、`GenerationInfo`、`BackpressureKind`、`RetentionPolicy`。
- `gateway`：`SamplingGateway`（生命周期与并发协调）、`GatewayConfig`（配置快照）、
  `GatewayProperties`（`application.properties` 绑定）、`StatsCollector`（统一计数）。
- `web`：`SamplingController`，本地验证用只读端点。

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

**统一计数不变量**

所有计数由单一的 `StatsCollector` 以原子长整型维护，杜绝重复/丢失计数。
每个采样尝试恰好终结于一次计数：

```
submitted  = accepted  + rejectedTotal                 // 每个 sensor#tick 恰一条结论
consumed   = accepted  + (DATA_CORRUPT | TIMESTAMP_ANOMALY | OUT_OF_RANGE)
enqueued   = consumed  + evictedOld                    // DROP_OLDEST 的驱逐发生在消费前
dropped    = DROP_OLDEST 驱逐数 + DROP_NEWEST(及关闭期 BLOCK) 拒绝数
           = rejectedByReason[BUFFER_OVERFLOW]
```

被顶掉（DROP_OLDEST）的旧读数同样形成一条 `BUFFER_OVERFLOW` 拒绝裁决，既计入
`rejected` 也计入 `dropped`，因此“生产总数 = 接受 + 各原因拒绝”严格成立，
并与第 9 节的对账台账（`evictedOld` / `rejectedNew` 分列）数字完全对得上。

集成测试对每一场景都断言 `submitted = accepted + rejected`、`dropped =
byReason[BUFFER_OVERFLOW]` 与“无重复 key”。

---

## 4a. 采样对账（Reconciliation）

`reconcile.ReconciliationStore` 是所有裁决的**单一对账事实源**：网关在每条裁决产生时
（与写 `VerdictLog`、更新 `StatsCollector` 同一处）同步落账。它面向“拿采样结论去复盘
核对”的场景，提供按**批次 / 传感器 / 规则版本 / 时间范围**的查询，并保证账目闭合、
可回溯、结论可复现。

### 4a.1 代次（批次）

- 每次 `start()` / `restart()` 开启一个**代次（generation）**，id 从 1 起在 JVM 内
  单调递增；优雅 `stop()` 时**封代**（seal）。一次运行即一个批次。
- 一条读数的对账身份键是 `(generation, sensorId, tick)`。重启后 tick 从 0 重新计，
  但代次不同，因此两代的 `s#0` 不会互相覆盖或重复计算。

### 4a.2 闭合口径（与有效性判定完全一致）

对任意筛选条件返回的 `ReconciliationReport` 都是计算后即冻结的**不可变快照**，并在
返回前做自校验，任何不闭合都会直接抛错而不是静默给出错账：

```
total      = accepted + rejected
rejected   = Σ rejectedByReason                       // 每条非接受落账恰归一个可区分原因
byReason[BUFFER_OVERFLOW] = evictedOld + rejectedNew  // 被顶掉 / 被丢弃分列
acceptedEntries  = 真正交付出去的接受明细（含校准值，按 seq 升序）
rejectedEntries  = 拒绝明细（含原因、依据所在 Verdict.basis、背压方式）
```

- **接受口径与 `ReadingValidator` 完全一致**：只有通过当前规则版本判定（校验和、时间戳、
  量程固定优先级全部通过）的读数才进 `acceptedEntries`；其余全部进 `rejectedEntries`，
  并带 `DATA_CORRUPT / TIMESTAMP_ANOMALY / OUT_OF_RANGE / BUFFER_OVERFLOW /
  SENSOR_ISOLATED / ACQUISITION_FAILED` 之一。
- 背压在条目上用 `BackpressureKind` 区分：`EVICTED_OLD`（DROP_OLDEST 顶掉最旧）与
  `REJECTED_NEW`（DROP_NEWEST / 关闭期 BLOCK 直接丢弃最新）。
- 同一批输入 + 同一规则版本反复运行，报告的总数/接受/各原因拒绝签名完全相同
  （见 `sameInputsAndRulesVersion_reconciliationIsReproducible`）。
- 报告数字与 `StatsCollector` 快照逐项互证
  （见 `assertMatchesStats`：accepted/rejected/submitted/dropped/byReason 全一致）。

### 4a.3 聚合精确、明细有界：保留与容量边界

台账把“**精确聚合计数**”与“**有界明细**”分开：

- 代次级、传感器级的 total/accepted/byReason/evicted/rejected 在落账时原子更新，
  **永不因保留策略丢失**——所以对整代（或任意不带时间切片的组合）对账永远闭合精确。
- 每代明细按 `gateway.reconcile-detail-capacity` 有界保留，超容量后行为由
  `gateway.reconcile-retention-policy` 确定：
  - `ROLL_OLDEST`：确定性顶掉最旧明细（默认）；
  - `IGNORE_NEW`：超容量后新裁决只更新聚合计数、**已保留明细一条不改、一条不删**。
- 明细被滚动后，报告的聚合计数仍然精确，但 `complete=false` 且给出 `detailRollover`，
  明细列表是精确计数的真子集——边界对调用方显式可见，**绝不会把已出过的账目悄悄改掉
  或删掉**；已出具的报告对象本身也不受后续滚动影响。
- 已封代按 `gateway.reconcile-max-sealed-generations` 有界保留，超出后确定性清除**最旧的
  已封代**（永不清除运行代），清除代数在 `/reconcile/store` 的 `generationsPurged` 可见。
- 时间范围切片（`from/to`，按网关记录时间 `recordedAt` 闭区间）需要逐条扫描明细，仅在
  涉及代次明细完整保留时 `complete=true`；否则同样以 `complete=false` 显式标注。

### 4a.4 与现有规则的兼容范围

- **采样/背压/故障/版本切换逻辑零改动**：对账是在既有裁决落地点旁路增加的一份事实记录，
  不改变任何接受/拒绝结论；本次仅修正了 DROP_OLDEST 驱逐的统一计数（使其同时计入
  rejected，见 4.1），结论口径本身不变。
- 并发：落账在台账内部用一把锁 + 全局原子序列号保护，多生产者/多消费者下身份键唯一。
- 故障恢复：隔离/采集失败与正常读数一样每条恰好落账一次。
- 版本切换：规则版本随代次固定并写入每条落账，可跨代按 `rulesVersion` 切片复核。
- 重启：封代/开代与生命周期绑定；封代后历史批次仍可按 id 复核（受封代保留代数限制）。
- 优雅关闭排空后，本代提交的每个 `sensor#tick` 都有恰好一条落账；若关闭排空超时仍有
  读数滞留缓冲（异常配置），它们不产生裁决，会在对账中表现为可观测的缺口（应调大
  `shutdown-drain-timeout-ms`），而不会被伪装成正常采样。

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
- `restart(newConfig[, newSensors])`：先优雅停止并排空旧数据（封代），再以新配置快照重建
  缓冲/断路器并启动（开新一代）；`StatsCollector` 生命周期计数保留，环形 `VerdictLog`
  按新一代重置、`tick` 从 0 重新计；对账台账则把每代作为独立批次保留（受
  `reconcile-max-sealed-generations` 限制），跨代用 `(generation, sensor, tick)` 区分。

---

## 7. 本地验证

### 7.1 构建与测试（离线）

```bash
mvn test -o          # 使用本地 Maven 仓库；联网时可去掉 -o
```

测试共 34 个，覆盖：

| 测试类 | 覆盖点 |
|--------|--------|
| `ReadingValidatorTest` | 三类拒绝原因可区分、边界值有效、固定优先级；日志打印输入与依据 |
| `BoundedBufferTest` | 三种溢出策略的确定性、BLOCK 等待、关闭后排空与拒绝新读数 |
| `CircuitBreakerTest` | 阈值隔离、冷却后单次探测、成功恢复/失败再隔离 |
| `RetryRecoveryTest` | 瞬时故障重试恢复、瞬时 vs 持续故障可区分 |
| `ConcurrentSamplingIntegrationTest` | 并发采样不重不漏、DROP_OLDEST/NEWEST 压力、持续故障隔离不影响他者、同规则版本可复现 |
| `LifecycleRestartTest` | 优雅关闭排空、重启不重不漏、规则版本切换 |
| `TransientRecoveryIntegrationTest` | 端到端瞬时故障经退避重试后恢复、不被隔离 |
| `reconcile.ReconciliationStoreTest` | 对账闭合与身份键唯一、按代次/传感器/规则版本/时间切片、ROLL_OLDEST/IGNORE_NEW 边界、封代清除、报告快照不可变 |
| `ReconciliationIntegrationTest` | 并发混合读数每种不合规都落拒绝明细、对账与 StatsCollector 互证、背压两种方式分列、故障隔离后对平、重启版本切换按代隔离、同输入同版本可复现、容量边界确定 |
| `tests.ReconciliationEndpointTests` | 对账只读 REST 端点冒烟：闭合数字、批次列表、单条明细、保留边界 |

测试日志会打印 `VERDICT ACCEPT/REJECT:<原因>` 及 `[场景] submitted=... byReason=...`，
便于逐条核对输入、判定依据与计数。

### 7.2 启动应用并观察（无硬件）

```bash
mvn spring-boot:run
# 另开终端：
curl -s http://localhost:8080/api/gateway/status
curl -s http://localhost:8080/api/gateway/stats
curl -s http://localhost:8080/api/gateway/verdicts                 # 最近裁决（含 basis）
curl -s "http://localhost:8080/api/gateway/verdicts?sensor=temp-1" # 按传感器
curl -s "http://localhost:8080/api/gateway/verdicts/temp-1%230"    # 按 sensor#tick 精确查

# —— 采样对账 ——
curl -s http://localhost:8080/api/gateway/reconcile                       # 全部代次汇总对账
curl -s "http://localhost:8080/api/gateway/reconcile?generation=1"        # 按批次（任意一次运行）
curl -s "http://localhost:8080/api/gateway/reconcile?sensor=temp-1"       # 按传感器
curl -s "http://localhost:8080/api/gateway/reconcile?rulesVersion=rules-v1" # 按规则版本
curl -s "http://localhost:8080/api/gateway/reconcile?from=<ms>&to=<ms>"   # 按记录时间闭区间
curl -s http://localhost:8080/api/gateway/reconcile/generations           # 批次列表（每批精确聚合）
curl -s http://localhost:8080/api/gateway/reconcile/generations/1         # 单批次对账
curl -s "http://localhost:8080/api/gateway/reconcile/entries/temp-1%230?generation=1"
curl -s http://localhost:8080/api/gateway/reconcile/store                 # 保留与容量边界统计
```

报告中的关键数字：`total / accepted / rejected / evictedOld / rejectedNew /
rejectedByReason / complete / detailRollover / perSensor / acceptedEntries /
rejectedEntries`。`acceptedEntries` 即“最终真正交付出去”的明细。

应用启动后，三个脚本化模拟传感器会周期性演示正常/超量程/坏时间戳/数据损坏/
瞬时故障/持续故障等全部裁决原因。`Ctrl+C`（SIGTERM）触发优雅关闭与排空（并封代）。

### 7.3 配置（`src/main/resources/application.properties`）

关键项：`rules-version`、`buffer-capacity`、`overflow-policy`、`consumer-threads`、
`retry-*`（重试退避）、`breaker-threshold` / `breaker-cooldown-ms`（隔离）、
`shutdown-drain-timeout-ms`、`verdict-log-capacity`，对账保留
`reconcile-detail-capacity`（每代明细容量）、`reconcile-max-sealed-generations`
（封代保留代数）、`reconcile-retention-policy`（`ROLL_OLDEST` / `IGNORE_NEW`），
以及每个传感器的
`type / period-ms / range-min/max / timestamp-tolerance-ms / calibration-gain/offset`。

---

## 8. 关键设计取舍

- **单一计数源**：缓冲自身不计数，全部在网关侧由 `StatsCollector` 记录，避免重复计数。
- **tick 由生产者内单调分配**：天然消除并发下的重复/错位。
- **控制流与故障流分离**：`SamplingLimitReached` 不是异常故障，不污染统计。
- **时间/睡眠/抖动/传感器全部可注入**：测试在确定时钟与零等待退避下快速复现。
- **配置即不可变快照**：规则版本随裁决落盘，更新可追溯、结论可复现。
- **对账 = 旁路事实源，精确聚合与有界明细分离**：采样/背压/故障判定零改动；聚合计数
  永不因保留滚动而失准，报告出具即冻结并自校验闭合，容量边界显式可见（`complete`/
  `detailRollover`/`generationsPurged`），不静默改账。
