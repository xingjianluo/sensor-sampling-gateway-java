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
- 规则更新有两种方式：
  - **运行中切换 `switchRules(newConfig)`**（推荐用于复盘同一次运行）：不停机、不重启、
    不开新批次，在同一代次内原子换版本；切换前**已受理（已入队）**的读数继续按旧版本
    判定，切换后新产生的读数按新版本判定（见第 4b 节）；
  - **重启 `restart(newConfig[, newSensors])`**：封代并开新一代，用于更换传感器集合/缓冲结构。

---

## 4. 有界缓冲与背压（确定、可观测、可配置）

缓冲容量与溢出策略可配置（`gateway.buffer-capacity`、`gateway.overflow-policy`）：

| 策略 | 满时行为 | 计数处理 |
|------|----------|----------|
| `BLOCK` | 生产者等待空位；关闭后被唤醒并拒绝 | 正常时不丢；关闭期拒绝记 `BUFFER_OVERFLOW`（直接丢弃最新） |
| `DROP_OLDEST` | 驱逐队列最旧读数为新读数让位 | 新读数计入队；被驱逐旧读数形成 `BUFFER_OVERFLOW` 裁决，**单列**为“被顶掉” |
| `DROP_NEWEST` | 直接拒绝最新读数 | 该读数记 `BUFFER_OVERFLOW`（直接丢弃最新） |

**关闭语义（不重不漏的基础）**：`close()` 后不再受理任何新读数并唤醒全部等待者；
已在队列中的读数允许消费者**排空（drain）**，排空后 `poll` 返回 null。
排空超时仍滞留的读数由 `BoundedBuffer.drainAll()` 兜底逐条落账为
`SHUTDOWN_INCOMPLETE`（见第 6 节），绝不静默丢失。

**统一计数不变量（保留历史统计口径，老看板/脚本不变）**

所有计数由单一的 `StatsCollector` 以原子长整型维护，杜绝重复/丢失计数。
**被顶掉（DROP_OLDEST 驱逐最旧）的读数是一条独立裁决，但只单列进 `evictedOld` /
`dropped`，不进入老的 `rejected` 与 `byReason[BUFFER_OVERFLOW]`**——后两者只统计
“直接丢弃最新读数”。于是上一版已核对过的 `submitted / enqueued / consumed /
rejected / dropped` 关系原样成立，对账在不改动任何老计数的前提下把两类背压分列：

```
submitted   = enqueued + rejected                                  // 受理点口径
consumed    = accepted + (DATA_CORRUPT | TIMESTAMP_ANOMALY | OUT_OF_RANGE)
enqueued    = consumed + evictedOld                                // DROP_OLDEST 的驱逐发生在消费前
submitted   = accepted + rejected + evictedOld                     // 每个 sensor#tick 恰一条结论
dropped     = evictedOld + rejectedNew
rejectedNew = rejectedByReason[BUFFER_OVERFLOW]                    // 只含直接丢弃最新
dropped     = evictedOld + rejectedByReason[BUFFER_OVERFLOW]
```

> 被顶掉读数在 `VerdictLog` 与对账明细中仍是一条可解释、可定位的 `BUFFER_OVERFLOW`
> 拒绝裁决（带 `BackpressureKind.EVICTED_OLD` 标记），只是**统计计数上单列**，不抬高
> 老的拒绝总数；直接丢弃（DROP_NEWEST / 关闭期 BLOCK）带 `REJECTED_NEW` 标记并进入
> 老的 `BUFFER_OVERFLOW` 计数。两类背压在报告中分别由 `evictedOld` / `rejectedNew`
> 给出。`GatewayStats` 另提供 `evictedOld()`、`unfinished()` 与 `rejectedNew()` 便于核对。

集成测试对每一场景都断言上述全部恒等式、两类背压分列与“无重复 key”。

---

## 4a. 采样对账（Reconciliation）

`reconcile.ReconciliationStore` 是所有裁决的**单一对账事实源**：网关在每条裁决产生时
（与写 `VerdictLog`、更新 `StatsCollector` 同一处）同步落账。它面向“拿采样结论去复盘
核对”的场景，提供按**批次 / 传感器 / 规则版本 / 时间范围**的查询，并保证账目闭合、
可回溯、结论可复现。

### 4a.1 代次（批次）与运行中切换

- 每次 `start()` / `restart()` 开启一个**代次（generation）**，id 从 1 起在 JVM 内
  单调递增；优雅 `stop()` 时**封代**（seal）。一次运行即一个批次。
- **运行中切换规则版本（`switchRules`）不开新代次**：同一次运行（同一批次）内可包含
  多个规则版本，`GenerationInfo.rulesVersions` 按首次出现顺序列出全部版本，
  `rulesVersion` 保留为开代时的起始版本。详见第 4b 节。
- 一条读数的对账身份键是 `(generation, sensorId, tick)`。重启后 tick 从 0 重新计，
  但代次不同，因此两代的 `s#0` 不会互相覆盖或重复计算。

### 4a.2 闭合口径（保留历史统计口径 + 两类背压分列）

对任意筛选条件返回的 `ReconciliationReport` 都是计算后即冻结的**不可变快照**，并在
返回前做自校验，任何不闭合都会直接抛错而不是静默给出错账：

```
total       = accepted + rejected + evictedOld
rejected    = Σ rejectedByReason                        // 非接受、非顶掉落账恰归一个可区分原因
rejectedNew = rejectedByReason[BUFFER_OVERFLOW]         // 只含“直接丢弃最新”
evictedOld  = DROP_OLDEST 被顶掉数（单列，不进 rejected / byReason）
backpressure= evictedOld + rejectedNew
unfinished  = rejectedByReason[SHUTDOWN_INCOMPLETE]     // 关闭滞留未完成读数（见第 6 节）
acceptedEntries = 真正交付出去的接受明细（含校准值，按 seq 升序）
rejectedEntries = 全部非接受明细（有效性拒绝 / REJECTED_NEW / EVICTED_OLD / 未完成）
```

- **老统计口径原样保留**：被顶掉读数是一条 `BUFFER_OVERFLOW` 裁决明细，但单列在
  `evictedOld`，不抬高老的 `rejected` 与 `byReason[BUFFER_OVERFLOW]`；
  `submitted = accepted + rejected + evictedOld` 与
  `dropped = evictedOld + byReason[BUFFER_OVERFLOW]` 严格成立，老看板/脚本无需改动。
- **接受口径与 `ReadingValidator` 完全一致**：只有通过其**受理时规则版本**判定（校验和、
  时间戳、量程固定优先级全部通过）的读数才进 `acceptedEntries`；其余进 `rejectedEntries`，
  带 `DATA_CORRUPT / TIMESTAMP_ANOMALY / OUT_OF_RANGE / BUFFER_OVERFLOW /
  SENSOR_ISOLATED / ACQUISITION_FAILED / SHUTDOWN_INCOMPLETE` 之一。
- 背压在条目上用 `BackpressureKind` 区分：`EVICTED_OLD`（DROP_OLDEST 顶掉最旧）与
  `REJECTED_NEW`（DROP_NEWEST / 关闭期 BLOCK 直接丢弃最新）。
- **未完成可见**：`unfinished>0`（关闭排空超时仍滞留）的报告 `complete=false`，
  滞留读数以 `SHUTDOWN_INCOMPLETE` 原因出现在拒绝明细中，可按批次/传感器定位。
- 同一批输入 + 同一规则版本反复运行，报告的总数/接受/各原因拒绝签名完全相同
  （见 `sameInputsAndRulesVersion_reconciliationIsReproducible`）。
- 报告数字与 `StatsCollector` 快照逐项互证（见集成测试 `assertMatchesStats`：
  accepted/rejected/submitted/dropped/evictedOld/unfinished/byReason 全一致）。

### 4a.3 聚合精确、明细有界：保留与容量边界

台账把“**精确聚合计数**”与“**有界明细**”分开：

- 代次级、**规则版本级**、传感器级的 total/accepted/byReason/evicted/rejected/unfinished
  在落账时原子更新，**永不因保留策略丢失**——所以对整代（或任意不带时间切片的组合）
  对账永远闭合精确；同一代次内即使切换过规则版本，每个版本也能各自精确切片。
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
  涉及代次明细完整保留且无未完成读数时 `complete=true`；否则同样以 `complete=false`
  显式标注。

### 4a.4 与现有规则的兼容范围

- **采样/背压/故障判定零改动**：对账是在裁决落地点旁路增加的事实记录；本轮恢复了
  DROP_OLDEST 的历史统计口径（被顶掉单列、不进老 rejected），结论本身不变。
- 并发：落账在台账内部用一把锁 + 全局原子序列号保护，多生产者/多消费者下身份键唯一。
- 缓冲压力 + 故障恢复 + 运行中切换叠加：每个 `sensor#tick` 仍恰好落账一次，总账闭合。
- 版本切换：运行中切换在同代内按 `rulesVersion` 分列（见 4b）；重启则按代次隔离，
  两种方式都可用 `rulesVersion` / `generation` 切片复核。
- 重启：封代/开代与生命周期绑定；封代后历史批次仍可按 id 复核（受封代保留代数限制）。
- 优雅关闭排空后，本代提交的每个 `sensor#tick` 都有恰好一条落账；排空超时仍滞留缓冲的
  读数会被兜底落为 `SHUTDOWN_INCOMPLETE` 并把报告标为 `complete=false`（见第 6 节），
  既不静默丢失，也不会被伪装成完整采样。

---

## 4b. 运行中切换规则版本（同一批次内）

复盘时常见诉求：**不停采样**就把规则从 v1 切到 v2，并在同一次运行里把切换前后的账
放在一起核对。`SamplingGateway.switchRules(newConfig)` 支持这一点：

- **不开新代次、不重启线程、不重置缓冲/断路器/tick**：切换是一次受网关锁保护的原子
  配置快照替换，切换前后落账属于同一个 generation。
- **切换前已受理（已入队）的读数按旧版本走完**：读数在入队前一刻被盖上“受理版本”
  （`RawReading.rulesVersion`），消费者始终按该版本对应的 `GatewayConfig` 快照做校验
  与校准；切换瞬间仍在缓冲里的在途读数结论与旧版本完全一致，绝不重判。
- **切换后新产生的读数按新版本判定**：包括新入队读数，以及切换后发生的隔离
  （`SENSOR_ISOLATED`）、采集失败（`ACQUISITION_FAILED`）等生产者侧结论。
- **各自可筛选、总账闭合**：代内维护“规则版本级”精确聚合，
  `reconcile?generation=G&rulesVersion=rules-v1` 与 `...&rulesVersion=rules-v2`
  各自闭合；两版本切片之和恰为整代总数；身份键 `(generation,sensor,tick)` 唯一，
  同一读数只算一次。
- **结构约束**：切换只允许改规则相关内容（量程/容差/校准/重试/断路器参数、版本号）；
  传感器 id 集合、缓冲容量与溢出策略必须保持不变（运行中更换会破坏在途背压口径），
  这类结构性变更仍需走 `restart`；相同版本号也会被拒绝。
- 并发/缓冲压力/故障恢复与切换叠加时不重不漏，见
  `InRunRulesSwitchIntegrationTest`（含一个容量 1、DROP_OLDEST、切换后持续故障
  被隔离的高压场景）。

> 编程式入口：`gateway.switchRules(cfgV2)`。REST 只读端点不提供切换（避免误操作），
> 但 `/status` 的 `runningRulesVersions` 会反映当前批次内出现过的全部版本。

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

## 6. 关闭与重启（已受理数据不重不漏，含排空边界）

- `stop()`：置停止位 → `buffer.close()` 停止受理 → 在 `shutdown-drain-timeout-ms`
  内等待消费者排空已入队数据 → 关闭线程池 → **排空兜底** → 封代。
- **排空超时 / 消费者卡住的边界（不漏算、不假完整）**：线程池关闭后，任何仍滞留在
  缓冲里的已受理读数由 `BoundedBuffer.drainAll()` 一次性取出（与消费者 `poll()` 互斥，
  一条读数要么被消费、要么被兜底，恰好一次），逐条落为原因 **`SHUTDOWN_INCOMPLETE`**
  的拒绝记录：计入 `submitted`/`rejected`/生产总数，可在拒绝明细中按
  `(generation, sensor, tick)` 定位；同时：
  - 报告 `unfinished` 给出滞留数量，`complete=false` 给出**清晰的不完整标记**，
    `/reconcile/generations` 的 `unfinished`/`completeRun` 同样可见；
  - 因此重启后“生产总数 = 对账总数”永久对平，后续按批次复查能立刻定位到不完整批次与
    具体读数；日志另有 `shutdown incomplete: N accepted reading(s) stuck...` 告警，
    处置建议是调大 `shutdown-drain-timeout-ms` 或排查消费者卡顿。
  - 正常优雅关闭（缓冲排空为 0）时 `unfinished=0`、`complete=true`，行为与之前一致
    （见 `LifecycleRestartTest`）。
- `requestStop()`（测试/精确控制用）：请求生产者在当前拍完成后停止，但**不关闭缓冲**。
- `SamplingLimitReached`：传感器侧的协作式停止信号（非故障），生产者干净退出，
  不产生裁决、不计入任何失败统计，使采样拍数在源头精确可复现。
- `restart(newConfig[, newSensors])`：先优雅停止并排空旧数据（封代，含未完成兜底），
  再以新配置快照重建缓冲/断路器并启动（开新一代）；`StatsCollector` 生命周期计数保留，
  环形 `VerdictLog` 按新一代重置、`tick` 从 0 重新计；对账台账则把每代作为独立批次
  保留（受 `reconcile-max-sealed-generations` 限制），跨代用 `(generation, sensor, tick)`
  区分。上一代若不完整，其 `unfinished` 标记与明细在重启后仍保留可查。
- `switchRules(newConfig)`：**不停止、不封代、不开新代**的运行中规则切换（见第 4b 节）。

---

## 7. 本地验证

### 7.1 构建与测试（离线）

```bash
mvn test -o          # 使用本地 Maven 仓库；联网时可去掉 -o
```

只跑本轮三个重点场景（断言账目闭合、同一读数只算一次）：

```bash
mvn test -o -Dtest=InRunRulesSwitchIntegrationTest       # 运行中切换规则版本
mvn test -o -Dtest=BackpressureAndShutdownBoundaryTest   # 两类背压并存 + 排空超时重启
mvn test -o -Dtest=ReconciliationIntegrationTest         # 上一轮对账能力不回归
```

测试共 41 个，覆盖：

| 测试类 | 覆盖点 |
|--------|--------|
| `ReadingValidatorTest` | 三类拒绝原因可区分、边界值有效、固定优先级；日志打印输入与依据 |
| `BoundedBufferTest` | 三种溢出策略的确定性、BLOCK 等待、关闭后排空与拒绝新读数 |
| `CircuitBreakerTest` | 阈值隔离、冷却后单次探测、成功恢复/失败再隔离 |
| `RetryRecoveryTest` | 瞬时故障重试恢复、瞬时 vs 持续故障可区分 |
| `ConcurrentSamplingIntegrationTest` | 并发采样不重不漏、DROP_OLDEST/NEWEST 压力（含历史计数恒等式）、持续故障隔离不影响他者、同规则版本可复现 |
| `LifecycleRestartTest` | 优雅关闭排空、重启不重不漏、规则版本切换 |
| `TransientRecoveryIntegrationTest` | 端到端瞬时故障经退避重试后恢复、不被隔离 |
| `reconcile.ReconciliationStoreTest` | 对账闭合与身份键唯一、按代次/传感器/规则版本/时间切片、**同代两版本切换分列**、ROLL_OLDEST/IGNORE_NEW 边界、封代清除、**未完成读数不完整标记**、报告快照不可变 |
| `ReconciliationIntegrationTest` | 并发混合读数每种不合规都落拒绝明细、对账与 StatsCollector 互证、背压两种方式分列、故障隔离后对平、重启版本切换按代隔离、同输入同版本可复现、容量边界确定 |
| `InRunRulesSwitchIntegrationTest` | **运行中切换规则版本**：旧受理走旧版本/新读数走新版本、同代两版本各自可筛且总账闭合、身份唯一；结构变更被拒；切换叠加并发+容量 1 背压+持续故障隔离仍不重不漏 |
| `BackpressureAndShutdownBoundaryTest` | **顶掉最旧与直接丢弃在同一批次并存且分列**（老计数口径不变、全部恒等式）；**排空超时/消费者卡住时滞留读数落 SHUTDOWN_INCOMPLETE、报告不完整、重启后跨批次总账仍对平可定位** |
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

报告中的关键数字：`total / accepted / rejected / evictedOld / rejectedNew / unfinished /
rejectedByReason / complete / detailRollover / perSensor / acceptedEntries /
rejectedEntries`；批次列表还有每批的 `rulesVersions / unfinished / completeRun`。
`acceptedEntries` 即“最终真正交付出去”的明细；`complete=false` 表示明细被滚动或存在
`unfinished>0` 的未完成读数。

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

- **单一计数源 + 历史口径不破坏**：缓冲自身不计数，全部在网关侧由 `StatsCollector`
  记录；“被顶掉最旧”只单列（`evictedOld`/`dropped`），不改动老的 `rejected` /
  `byReason[BUFFER_OVERFLOW]`，两类背压的分拆放到对账层完成，老看板与脚本继续对得上。
- **tick 由生产者内单调分配**：天然消除并发下的重复/错位。
- **控制流与故障流分离**：`SamplingLimitReached` 不是异常故障，不污染统计。
- **时间/睡眠/抖动/传感器全部可注入**：测试在确定时钟与零等待退避下快速复现；
  消费者另提供测试钩子，用于确定性制造“关闭时仍有读数滞留”的边界。
- **配置即不可变快照，受理版本跟随读数**：规则版本在入队前盖到读数上，消费者按该版本
  快照判定，因此运行中切换规则时在途读数不重判、新读数走新版本；历史配置快照按版本保留。
- **对账 = 旁路事实源，精确聚合与有界明细分离**：采样/背压/故障判定零改动；代/版本/
  传感器三级聚合计数永不因保留滚动而失准，报告出具即冻结并自校验闭合，容量边界与未完成
  运行显式可见（`complete`/`detailRollover`/`unfinished`/`generationsPurged`），
  不静默改账、不漏账、不假装完整。
