# 传感器采样网关（Java / Spring Boot）

一套可在本地运行、**不依赖真实硬件 / 外部服务 / 凭据** 的传感器采集网关。它让多种传感器在
**并发采样、缓冲受限、设备故障并存** 的情况下，仍能产出 **一致、可解释、可复现** 的采样结论。

所有传感器都可由内存中的“脚本化传感器”（`ScriptedSensor`）模拟，故障、超量程、时间戳异常、
数据损坏、生产快于消费等场景都可确定性地复现，并由单元 / 集成测试覆盖。

---

## 1. 总体结构

```
model        领域类型：SensorSpec / RawReading / Sample / SampleOutcome / RejectReason / StatsSnapshot
validation   ReadingValidator   固定顺序的有效性判定（损坏→时间戳→量程）
calibration  CalibrationModel   线性校准 raw*scale+offset，规则版本可追溯
buffer       BoundedSamplingBuffer / OfferResult   有界缓冲 + 三种溢出策略
fault        FaultTracker / FaultDecision / RetryPolicy   有界重试、退避、隔离、恢复探测
sensor       SensorDriver / SensorFaultException / ScriptedSensor  驱动抽象与确定性模拟器
registry     SensorRegistry     线程安全的配置/规则版本登记，保留历史版本
engine       SamplingEngine     多生产者 + 单消费者并发引擎，生命周期与排空
gateway      SamplingGateway / DefaultSamplingGateway / OutcomeSink / RecordingOutcomeSink
web          GatewayController  只读可观测 HTTP 接口（/api/gateway/**）
```

一条读数的生命周期：

```
驱动读取(可故障→有界重试) → produced 计数 → 有界缓冲(溢出确定、可记账)
   → 单消费者取出 → 有效性判定 → 校准 → SampleOutcome(Accepted|Rejected) → OutcomeSink
```

---

## 2. 采样与校准模型

### 2.1 原始读数 `RawReading`

字段：`sensorId, seq, value, timestampMs, checksum`。

- `seq` 为每个传感器**单调递增**的序列号，是“不重不漏”核对的依据。
- `checksum` 采用对字段做 FNV-1a 风格的 64 位确定性混合；`checksumValid()` 用同样输入重算比对。
  模拟器可把校验和翻转，以确定性地制造“数据损坏”。

### 2.2 有效性判定 `ReadingValidator`（顺序固定，原因可区分）

按**固定顺序**逐条应用规则，任一不过即拒绝并返回带原因与“判定依据”的结论：

1. `CORRUPT_DATA`：校验和不符（数据已不可信，最先判）。
2. `FUTURE_TIMESTAMP`：`ts > now + maxSkewFutureMs`。
3. `STALE_TIMESTAMP`：`ts < now - maxSkewPastMs`。
4. `OUT_OF_RANGE`：值为 NaN或超出 `[minValue, maxValue]`。

固定顺序保证：一条同时“损坏且超量程”的读数，无论何时判定都得到同一个原因（`CORRUPT_DATA`），
结论确定、可复现。拒绝结论 `SampleOutcome.Rejected` 携带 `basis` 字符串，记录输入值、阈值、
规则版本，例如：

```
OUT_OF_RANGE sensor=t1 seq=1 rule=rules-v1 value=120 range=[-40,80]
```

读数**绝不静默进入结果、也不静默丢弃**；每条读数最终恰有一条 `SampleOutcome`。

### 2.3 校准模型 `CalibrationModel`

线性模型：`calibrated = raw * scale + offset`。通过全部校验后才校准并生成 `Sample`，
样本上带 `ruleVersion`。相同输入 + 相同规则版本 ⇒ 完全相同的结果（纯函数，无随机、无系统时钟
依赖——判定基准时间 `now` 由参数注入）。

### 2.4 规则版本与可复现性 `SensorRegistry`

- 每个传感器保留**全部历史 `SensorSpec`**（含 scale/offset/量程/容忍度/版本号）。
- 校准 / 配置更新必须携带**严格递增**的 `ruleVersion`，回退会被显式拒绝
  （`ruleVersion must increase ...`），防止旧配置覆盖新配置造成结论不确定。
- 因此“相同输入 + 指定规则版本”的结论随时可追溯、可复现。

---

## 3. 背压与有界缓冲

`BoundedSamplingBuffer` 容量固定、入队/出队全部加锁，溢出策略 `OverflowPolicy` 可配置：

| 策略 | 缓冲满时行为 | 是否丢数 |
|------|--------------|----------|
| `BLOCK` | 生产者 `put()` 阻塞，直到消费者腾出空位（背压） | 不丢数 |
| `DROP_NEWEST` | `offer()` 立即返回 `REJECTED_FULL`，拒绝新读数 | 丢“新”，显式计数 |
| `DROP_OLDEST` | 挤出最旧读数再入队，返回 `ENQUEUED_WITH_EVICTION` 与被挤出者 | 丢“旧”，显式计数 |

被 `DROP_*` 丢弃的读数同样形成一条 `Rejected(BUFFER_OVERFLOW)` 结论，带原因、带依据并计入统计，
**可观测、不静默**。账目恒等式：

```
produced == accepted + Σ rejected
delivered（结论条数） == produced
```

### 为什么不会重复计数 / 丢失计数 / 数据竞争

- **多生产者**：每个传感器一个周期任务，各自有界重试；互不共享可变状态。
- **单消费者**：只有一个线程做“判定 + 计数 + 交付”，每条读数有**唯一归属处理点**，
  从结构上消除对同一条读数重复记账或无人记账的可能。
- 计数使用 `LongAdder`；缓冲使用 `ReentrantLock` + 双 `Condition`；登记表使用并发集合。

---

## 4. 故障链路：有界重试、退避、隔离、恢复

`FaultTracker` 为每个传感器维护一个确定性状态机（时间可注入）：

1. **瞬时故障（transient）**：在一个采样周期内最多尝试 `maxAttempts` 次，
   每次重试前按指数退避等待 `initialBackoff * multiplier^(n-1)`，以 `maxBackoff` 封顶。
   退避只暂停**该传感器当前轮次**，不影响其他传感器。
2. **轮次失败**：重试耗尽仍失败，则“连续失败轮次” +1，并设置跨轮退避窗口（指数、封顶）。
3. **持续失败隔离**：连续失败轮次达到 `failureThreshold`，或发生**持续（permanent）故障**，
   立即将传感器 `ISOLATED`。被隔离传感器退出正常调度，仅每隔 `probeInterval` 放行一次
   **恢复探测**。
4. **恢复**：任一轮次成功即完全复位（清失败计数、解除隔离）。

隔离是**每传感器独立**的：一个传感器持续故障不会拖垮或阻塞其他传感器的采样。
重启**不会悄悄清除隔离状态**（故障跟踪器随引擎复用），避免“重启掩盖故障”。

---

## 5. 关闭与重启的“不重不漏”

`SamplingEngine.stop()`：

1. 置 `running=false`，取消所有周期任务；
2. `shutdown()` 生产者池并 `awaitTermination`，等待**所有在途采样轮次（含退避重试）结束**，
   保证此后不会再有读数入缓冲；
3. 单消费者继续 `poll`，**直到生产者池已终止且缓冲排空**才退出（排空期间不停机，
   从条件上杜绝“停的瞬间恰好空轮询导致提前退出”）。

因此每条**已成功受理入缓冲**的读数都恰好被交付一次；`restart()` 后序列号继续单调递增，
跨阶段合并仍满足“无重复 seq、无缺漏、从 1 连续”。

---

## 6. 本地验证

### 6.1 运行测试（推荐，零外部依赖）

```bash
mvn test
```

共 25 个用例（Spring 上下文 1 + 校验器 7 + 故障状态机 4 + 缓冲 4 + 并发集成 5 + 校准版本 3 + Web 接口 1）。
单测日志会打印 **输入与判定依据**，例如：

```
[accept] INPUT sensor=t1 seq=1 value=20.0 ts=1000000 checksumValid=true | OUTCOME=ACCEPTED sample=...calibratedValue=41.0...
[range-high] ... OUTCOME=REJECTED(OUT_OF_RANGE) basis=OUT_OF_RANGE sensor=t1 ... range=[-40,80]
[drop-oldest] produced=78 accepted=28 rejected={BUFFER_OVERFLOW=50} ...
[isolate] bad sensor produced=0; good sensors kept sampling
[transient] recovered via bounded retry; accepted >= 1, isolated=false
[restart] phase1Produced=31 totalDelivered=62 uniqueSeqs=62
```

可单独运行某一类：

```bash
mvn -Dtest=ReadingValidatorTest test          # 超量程/时间戳/损坏可区分
mvn -Dtest=FaultTrackerTest test              # 退避/隔离/恢复探测
mvn -Dtest=BoundedSamplingBufferTest test     # 三种溢出策略 + BLOCK 背压
mvn -Dtest=SamplingConcurrencyTest test       # 并发、溢出账目、故障隔离、关闭重启
mvn -Dtest=CalibrationReproducibilityTest test# 校准版本可复现、拒绝版本回退
mvn -Dtest=GatewayWebEndpointsTest test       # /api/gateway/** 只读观测接口
```

### 6.2 同步单步判定（无需启动线程）

```java
GatewayProperties props = new GatewayProperties();
SamplingGateway gw = new DefaultSamplingGateway(props, System::currentTimeMillis);
gw.attach(spec, new ScriptedSensor("t1", List.of(Step.readAtNow(20.0))));
SampleOutcome o = gw.sampleOnce("t1", System.currentTimeMillis());
// Accepted -> sample().calibratedValue()；Rejected -> reason() + basis()
```

### 6.3 启动 Web 应用观察只读接口

```bash
mvn spring-boot:run
# 另开终端：
curl -s localhost:8080/api/gateway/health
curl -s localhost:8080/api/gateway/stats      # produced/accepted/rejected/retries/isolated/buffered
curl -s localhost:8080/api/gateway/sensors    # 量程、校准参数、规则版本
curl -s localhost:8080/api/gateway/outcomes   # 已交付结论（含拒绝原因与依据）
```

> 应用启动时默认不接入传感器（空网关）；传感器通过 `attach(spec, driver)` 接入，
> 测试中即按此方式完整演示，故本地无需任何硬件或第三方服务。

### 6.4 关键配置（`src/main/resources/application.properties`）

```properties
gateway.buffer-capacity=64
gateway.overflow-policy=BLOCK          # BLOCK / DROP_NEWEST / DROP_OLDEST
gateway.retry.max-attempts=3
gateway.retry.initial-backoff=10ms
gateway.retry.max-backoff=200ms
gateway.retry.backoff-multiplier=2.0
gateway.retry.failure-threshold=3
gateway.retry.probe-interval=1s
```

---

## 7. 拒收原因一览（`RejectReason`，彼此可区分）

| 原因 | 触发规则 |
|------|----------|
| `CORRUPT_DATA` | 校验和不符 |
| `FUTURE_TIMESTAMP` | 时间戳超前超阈值 |
| `STALE_TIMESTAMP` | 时间戳滞后超阈值 |
| `OUT_OF_RANGE` | 超量程或 NaN |
| `BUFFER_OVERFLOW` | 缓冲满被 DROP 策略拒绝/挤出 |
| `SENSOR_FAULT` | 读取时设备故障（同步采样入口） |
| `SENSOR_ISOLATED` | 传感器已隔离 / 配置缺失 |

