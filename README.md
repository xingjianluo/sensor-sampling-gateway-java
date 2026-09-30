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
因此并发下也不会重复或错位。每个**已提交**的 `sensor#tick` 恰好落入四类终结之一
（接受 / 拒绝 / 被顶掉 / 关闭未完成，见 4a.2）；其中 Verdict 层面：

- **接受**：`accepted=true`，含校准后的值 `calibratedValue`；
- **拒绝**：`accepted=false`，且必须带下列**可区分**原因之一，绝不静默丢弃：

| 原因 | 触发规则 |
|------|----------|
| `DATA_CORRUPT` | 读数校验和与重算值不一致（数据损坏，数值不可信） |
| `TIMESTAMP_ANOMALY` | `|读数时间戳 − 网关时钟| > timestampToleranceMs` |
| `OUT_OF_RANGE` | 值超出 `[rangeMin, rangeMax]`（边界值有效，闭区间） |
| `BUFFER_OVERFLOW` | 有界缓冲满，直接拒绝/丢弃最新读数（DROP_NEWEST/关闭期）；被顶掉旧读数的 Verdict 也写此原因，但台账单列 `EVICTED_OLD` |
| `SENSOR_ISOLATED` | 传感器断路器 OPEN（已隔离）期间不再受理 |
| `ACQUISITION_FAILED` | 采样动作在有界重试后仍失败 |
| `SHUTDOWN_INCOMPLETE` | 已提交读数在关闭排空超时/消费者中断后仍未得到裁决（独立的未完成终结） |

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
- 更新规则有两种方式：
  - **运行中热切换**（`switchRulesVersion`，见 3.1）：不重启、不开新批次；
  - **重启切换**（`restart`）：优雅停止并排空，开新代次；
- 校准模型也支持通过 `CalibrationModel.custom(version, fn)` 注入非线性/可观测的校准函数。

### 3.1 运行中切换规则版本（不重启、同一批次）

`SamplingGateway.switchRulesVersion(newConfig)`（或仅切换版本标签的
`switchRulesVersion("rules-v2")` 便捷重载；REST：`POST /api/gateway/rules-version?rulesVersion=...`）
在**不停采样、不重建缓冲/线程/断路器、不开新代次**的前提下原子替换规则相关配置
（规则版本、校准、传感器量程/容差、重试与断路器参数；既有断路器隔离状态延续）。

关键语义——**读数在“受理（入队）”那一刻盖规则版本章，判定时按盖章版本取规则**：

- 切换前**已受理**的读数（还在缓冲里、甚至随后会被顶掉）带着旧版本章，**按原规则版本
  完成**判定；
- 切换后**新受理**的读数按新版本判定；
- 盖章在 `BoundedBuffer` 的锁内完成，与版本切换严格线性化，因此 `BLOCK` 策略下生产者
  在满缓冲上阻塞期间发生切换，也不会出现“旧章配新规则”；
- 同一次运行仍是**同一个代次（批次）**，代次内每切换一次增加一个“规则版本阶段”，
  `GenerationInfo.rulesVersions` 按生效顺序列出全部版本；
- 两个版本各自可用 `reconcile?rulesVersion=...`（或代次+版本组合）筛选对账，
  **整代总账仍闭合**。

仅规则类参数可热切换；缓冲容量/溢出策略/消费者线程数/排空超时属于结构性参数，
热切换会被拒绝（抛 `IllegalArgumentException`），这类变更需 `restart()`。

---

## 4. 有界缓冲与背压（确定、可观测、可配置）

缓冲容量与溢出策略可配置（`gateway.buffer-capacity`、`gateway.overflow-policy`）：

| 策略 | 满时行为 | 计数处理 |
|------|----------|----------|
| `BLOCK` | 生产者等待空位；关闭后被唤醒并拒绝 | 正常时不丢；关闭期拒绝记 `BUFFER_OVERFLOW`（rejectedNew） |
| `DROP_OLDEST` | 驱逐队列最旧读数为新读数让位 | 新读数计入队；被驱逐旧读数**只计背压 `evictedOld`，不进拒绝总数** |
| `DROP_NEWEST` | 直接拒绝最新读数 | 该读数计 `BUFFER_OVERFLOW`（rejectedNew，进拒绝总数） |

**关闭语义（不重不漏的基础）**：`close()` 后不再受理任何新读数并唤醒全部等待者；
已在队列中的读数允许消费者**排空（drain）**，排空后 `poll` 返回 null。

**统一计数不变量（老看板/脚本口径保持不变，两类背压单独分列）**

所有计数由单一的 `StatsCollector` 以原子长整型维护。每个采样尝试恰好终结于一次计数：

```
submitted     = enqueued + rejected                       // 关闭前；驱逐不进 rejected
submitted     = accepted + rejected + evictedOld + shutdownIncomplete   // 总闭合
consumed      = accepted + (DATA_CORRUPT | TIMESTAMP_ANOMALY | OUT_OF_RANGE)
enqueued      = consumed + evictedOld + shutdownIncomplete // 驱逐发生在消费前；滞留读数不再被消费
dropped       = evictedOld + rejectedNew                   // 两类背压分列
rejectedNew   = rejectedByReason[BUFFER_OVERFLOW]          // 只有“直接拒绝/丢弃最新者”
```

- **被顶掉（DROP_OLDEST）的旧读数保持既有统计口径**：它只使 `evictedOld`（以及
  `dropped`）+1，**不抬高 `rejectedTotal`、不抬高 `rejectedByReason[BUFFER_OVERFLOW]`**。
  老看板上“提交/入队/拒绝/丢弃”的关系因此原样成立。
- 被顶掉与被直接丢弃在网关统计中分别是 `evictedOld` 与 `rejectedNew`（两者都含在老的
  `dropped` 里），在对账台账中分别落为 `EVICTED_OLD` 与 `REJECTED`(BUFFER_OVERFLOW /
  `REJECTED_NEW`) 两类，**不通过改老计数来凑对账**。
- 关闭边界的已提交滞留读数走第三类独立口径 `shutdownIncomplete`（见第 6 节），
  同样不进 `rejectedTotal`。

集成测试对每一场景都断言上述总闭合、`dropped = evictedOld + rejectedNew`、
`rejectedByReason[BUFFER_OVERFLOW] == rejectedNew` 与“无重复 key”
（见 `BackpressureSplitIntegrationTest`，同一场景内两类背压并存）。

---

## 4a. 采样对账（Reconciliation）

`reconcile.ReconciliationStore` 是所有裁决的**单一对账事实源**：网关在每条裁决产生时
（与写 `VerdictLog`、更新 `StatsCollector` 同一处）同步落账。它面向“拿采样结论去复盘
核对”的场景，提供按**批次 / 传感器 / 规则版本 / 时间范围**的查询，并保证账目闭合、
可回溯、结论可复现。

### 4a.1 代次（批次）与规则版本阶段

- 每次 `start()` / `restart()` 开启一个**代次（generation）**，id 从 1 起在 JVM 内
  单调递增；优雅 `stop()` 时**封代**（seal）。一次 start/restart 是一个批次。
- **运行中切换规则版本不开新代**：同代内每切换一次开启一个“规则版本阶段”，
  `GenerationInfo.rulesVersions` 按生效顺序列出（`rulesVersion` 为当前生效版本）。
  于是同一次运行里即可把切换前后的账放在一起核对，又能按版本分别筛选（见 3.1）。
- 一条读数的对账身份键是 `(generation, sensorId, tick)`。重启后 tick 从 0 重新计，
  但代次不同，因此两代的 `s#0` 不会互相覆盖或重复计算。台账对同代 `sensor#tick`
  幂等去重，故障恢复/关闭竞争下的重复落账被确定性忽略（`/reconcile/store` 的
  `duplicateAppendsIgnored` 可见），并发、缓冲压力与故障恢复并存时也不重不漏。

### 4a.2 闭合口径：四类终结（与有效性判定完全一致）

每条已提交读数恰好归入下列**四类互斥且穷尽**的终结方式之一
（`ReconciliationEntry.disposition`）：

| 终结方式 | 含义 | 是否进 rejected |
|----------|------|------------------|
| `ACCEPTED` | 通过受理时盖章版本的有效性判定并交付 | accepted 桶 |
| `REJECTED` | 有效性/故障/隔离拒绝，或缓冲满/关闭时**直接拒绝最新读数**（REJECTED_NEW） | 是 |
| `EVICTED_OLD` | DROP_OLDEST **为腾空间顶掉**的最旧读数（保持老统计口径） | **否，单列** |
| `INCOMPLETE` | 已提交但关闭排空超时/消费者中断，未得到接受或拒绝裁决 | **否，单列** |

对任意筛选条件返回的 `ReconciliationReport` 都是计算后即冻结的**不可变快照**，并在
返回前做自校验，任何不闭合都会直接抛错而不是静默给出错账：

```
total        = accepted + rejected + evictedOld + incomplete
rejected     = Σ rejectedByReason                       // 每条 REJECTED 恰归一个可区分原因
rejectedNew  = rejectedByReason[BUFFER_OVERFLOW]        // 仅直接拒绝/丢弃最新者
backpressure = evictedOld + rejectedNew                 // 两类背压分列
acceptedEntries   = 交付出去的接受明细（含校准值，按 seq 升序）
rejectedEntries   = REJECTED 明细（含原因、依据、REJECTED_NEW 背压）
evictedEntries    = EVICTED_OLD 明细（被顶掉）
incompleteEntries = INCOMPLETE 明细（关闭边界未完成，reason=SHUTDOWN_INCOMPLETE）
```

- **接受口径与 `ReadingValidator` 完全一致**：只有通过读数**受理时盖章版本**的判定
  （校验和、时间戳、量程固定优先级全部通过）才进 `acceptedEntries`。
- 拒绝原因仍可区分：`DATA_CORRUPT / TIMESTAMP_ANOMALY / OUT_OF_RANGE / BUFFER_OVERFLOW /
  SENSOR_ISOLATED / ACQUISITION_FAILED`。被顶掉的 Verdict 文本仍写 BUFFER_OVERFLOW，
  但在台账归入 `EVICTED_OLD` 桶、**不进 `rejectedByReason`**；`SHUTDOWN_INCOMPLETE`
  同样是独立桶、不进 `rejectedByReason`。背压方式用 `BackpressureKind`
  （`EVICTED_OLD` / `REJECTED_NEW`）在条目上区分。
- **规则版本切片**：`reconcile?rulesVersion=...` 基于落账时维护的“版本阶段精确聚合”，
  无时间切片时永远精确闭合，不受明细保留滚动影响。
- 同一批输入 + 同一规则版本反复运行，报告的总数/接受/各原因拒绝签名完全相同
  （见 `sameInputsAndRulesVersion_reconciliationIsReproducible`）。
- 报告数字与 `StatsCollector` 快照逐项互证（见集成测试 `assertMatchesStats`：
  accepted/rejected/evictedOld/rejectedNew/incomplete/dropped/byReason 全一致）。

### 4a.3 聚合精确、明细有界：保留与容量边界

台账把“**精确聚合计数**”与“**有界明细**”分开：

- 代次级、版本阶段级、传感器级的 total/accepted/rejected/evictedOld/rejectedNew/incomplete
  在落账时原子更新，**永不因保留策略丢失**——所以对整代（或任意不带时间切片的组合）
  对账永远闭合精确。
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
  涉及代次明细完整保留且无未完成记录时 `complete=true`；否则同样以 `complete=false`
  显式标注。
- 任何口径下只要存在 `INCOMPLETE` 记录，报告都强制 `complete=false`。

### 4a.4 与现有规则的兼容范围

- **不改老计数凑对账**：DROP_OLDEST 驱逐恢复为只计背压 `evictedOld`（dropped 的一部分），
  不再抬高 `rejectedTotal` / `rejectedByReason[BUFFER_OVERFLOW]`；顶掉与直接拒绝两类
  背压在网关统计与对账台账中各自单列（见第 4 节与 4a.2）。
- 并发：落账在台账内部用一把锁 + 全局原子序列号保护，身份键 `(generation,sensor,tick)`
  唯一且落账幂等，多生产者/多消费者下不重不漏。
- 故障恢复：隔离/采集失败与正常读数一样每条恰好落账一次。
- 版本切换：运行中热切换在同代内分阶段、按受理盖章版本切片；重启则按代次隔离。
  两者都可按 `rulesVersion` 复核，总账闭合。
- 重启：封代/开代与生命周期绑定；封代后历史批次仍可按 id 复核（受封代保留代数限制）。
- 关闭排空边界：见第 6 节——滞留读数会被明确落为 `SHUTDOWN_INCOMPLETE`，绝不漏算。

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

- `stop()`：置停止位 → `buffer.close()` 停止受理 → 在 `shutdown-drain-timeout-ms`
  内等待消费者排空已入队数据 → 关闭消费者 → 兜底处理滞留读数 → 关闭生产者并封代。
  正常排空路径缓冲为 0（见 `LifecycleRestartTest`）。
- **关闭排空超时 / 消费者短暂卡住的边界（不漏算）**：当排空超时后仍有读数滞留缓冲，
  或消费者在裁决前被关闭中断（例如消费者在校准/校验处短暂卡住）时：
  - 缓冲中剩余的读数由 `buffer.drainRemaining()` 兜底取出，逐条落为
    **`SHUTDOWN_INCOMPLETE`** 记录；被中断的在途读数同样落为该类（二者靠台账幂等
    去重，不会重复）；
  - 这些记录进报告的 `incomplete` / `incompleteEntries`，是与 accepted/rejected/
    evictedOld 并列的第四类终结，带可区分原因 `SHUTDOWN_INCOMPLETE` 与原读数信息
    （sensor#tick、时间戳、盖章规则版本），后续按批次复查可直接定位；
  - 报告因此显式 `complete=false`；`StatsCollector.shutdownIncomplete` 独立计数
    （不进 rejectedTotal）。**生产总数与对账总数因此永久对得上**，不会再出现重启后
    “提交了却既不在接受也不在拒绝明细”的永久缺口。
  - 若该数值持续非 0，应调大 `gateway.shutdown-drain-timeout-ms` 或排查卡住的消费者。
  - （注意：这里指读数已入队后的关闭边界；关闭开始后才采样成功、尚未入队的读数
    本来就不算“已受理”，生产者会直接丢弃不产生裁决。）
- `requestStop()`（测试/精确控制用）：请求生产者在当前拍完成后停止，但**不关闭缓冲**。
- `SamplingLimitReached`：传感器侧的协作式停止信号（非故障），生产者干净退出，
  不产生裁决、不计入任何失败统计，使采样拍数在源头精确可复现。
- `restart(newConfig[, newSensors])`：先优雅停止并排空旧数据（封代，滞留读数落
  INCOMPLETE），再以新配置快照重建缓冲/断路器并启动（开新一代）；`StatsCollector`
  生命周期计数保留，环形 `VerdictLog` 按新一代重置、`tick` 从 0 重新计；对账台账则把
  每代作为独立批次保留（受 `reconcile-max-sealed-generations` 限制），跨代用
  `(generation, sensor, tick)` 区分。重启后新一代恢复完整处理，跨代总账仍闭合。

---

## 7. 本地验证

### 7.1 构建与测试（离线）

```bash
mvn test -o          # 使用本地 Maven 仓库；联网时可去掉 -o
```

测试共 41 个，覆盖：

| 测试类 | 覆盖点 |
|--------|--------|
| `ReadingValidatorTest` | 三类拒绝原因可区分、边界值有效、固定优先级；日志打印输入与依据 |
| `BoundedBufferTest` | 三种溢出策略的确定性、BLOCK 等待、关闭后排空与拒绝新读数、**入队受理盖章（被顶掉保留旧版本章/新读数盖当前章）** |
| `CircuitBreakerTest` | 阈值隔离、冷却后单次探测、成功恢复/失败再隔离 |
| `RetryRecoveryTest` | 瞬时故障重试恢复、瞬时 vs 持续故障可区分 |
| `ConcurrentSamplingIntegrationTest` | 并发采样不重不漏、DROP_OLDEST/NEWEST 压力（老口径：驱逐不抬高 rejected）、持续故障隔离不影响他者、同规则版本可复现 |
| `LifecycleRestartTest` | 优雅关闭排空、重启不重不漏、规则版本切换 |
| `TransientRecoveryIntegrationTest` | 端到端瞬时故障经退避重试后恢复、不被隔离 |
| `reconcile.ReconciliationStoreTest` | 四类闭合与身份键唯一、按代次/传感器/规则版本/时间切片、**同代多版本阶段**、幂等去重、ROLL_OLDEST/IGNORE_NEW 边界、封代清除、报告快照不可变 |
| `ReconciliationIntegrationTest` | 并发混合读数每种不合规都落拒绝明细、对账与 StatsCollector 互证、背压两种方式分列、故障隔离后对平、重启版本切换按代隔离、同输入同版本可复现、容量边界确定 |
| **`HotRulesSwitchIntegrationTest`** | **运行中切换规则版本：同代两阶段各自可筛选且总账闭合、切换前读数按旧版本/切换后按新版本、结构参数热切换被拒、与 stats 互证** |
| **`BackpressureSplitIntegrationTest`** | **同一场景内顶掉最旧（EVICTED_OLD）与直接丢弃（REJECTED_NEW）并存：分列、老口径不被改动、账目闭合、同一条读数只算一次** |
| **`ShutdownDrainIncompleteTest`** | **排空超时/消费者卡住时滞留与在途读数落 SHUTDOWN_INCOMPLETE、报告不完整标记、按批次定位、重启后新一代正常且跨代闭合** |
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

# —— 运行中切换规则版本（不重启、同批次内开新阶段）——
curl -s -X POST "http://localhost:8080/api/gateway/rules-version?rulesVersion=rules-v2"
# 返回 {"switched":true,"rulesVersion":"rules-v2","generation":1,"sameGeneration":true}
# 切换前已受理读数按 rules-v1 完成，切换后新读数按 rules-v2；两者在同一 generation 内可分别筛：
curl -s "http://localhost:8080/api/gateway/reconcile?generation=1&rulesVersion=rules-v1"
curl -s "http://localhost:8080/api/gateway/reconcile?generation=1&rulesVersion=rules-v2"
curl -s http://localhost:8080/api/gateway/reconcile/generations          # rulesVersions 含 [rules-v1, rules-v2]
```

报告中的关键数字：`complete / total / accepted / rejected / evictedOld / rejectedNew /
incomplete / backpressure / rejectedByReason / detailRollover / perSensor` 以及四类明细
`acceptedEntries / rejectedEntries / evictedEntries / incompleteEntries`。
`acceptedEntries` 即“最终真正交付出去”的明细；`incompleteEntries` 是关闭边界的未完成
读数（`complete=false`）。`/api/gateway/stats` 也新增了分列字段
`evictedOld / rejectedNew / shutdownIncomplete`（老字段 `rejected/dropped` 含义不变）。

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
- **配置即不可变快照**：规则版本随裁决落盘，更新可追溯、结论可复现；运行中热切换只换
  规则快照、不动缓冲/线程，并在受理时给读数盖版本章，保证“旧读数走旧规则、新读数走新规则”。
- **对账 = 旁路事实源，精确聚合与有界明细分离**：四类终结（接受/拒绝/顶掉/未完成）
  互斥穷尽，聚合计数永不因保留滚动失准；报告出具即冻结并自校验闭合，容量边界与未完成
  状态显式可见（`complete`/`detailRollover`/`incomplete`/`generationsPurged`），
  不靠改老计数凑对账，也不静默改账。
- **关闭不静默丢账**：排空超时/消费者中断的已提交读数一律补记为可定位的
  `SHUTDOWN_INCOMPLETE`，配幂等去重，使生产总数与对账总数在任意边界下都永久闭合。
