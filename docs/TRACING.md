# 本地请求追踪与耗时剖析

本服务内置一套**纯本地**（不依赖任何外部采集服务）的请求追踪与耗时剖析能力。
每个入口请求拥有独立追踪标识，并在同步调用、异步调用、线程池后台任务与重试
路径间延续同一上下文；失败被归入四个稳定类别并保留原始异常链；耗时与调用
次数按入口维度在内存中聚合，写入并发安全、内存占用有明确上界。

## 1. 目录与模块

| 模块 | 职责 |
| --- | --- |
| `app/context.py` | Trace/Span 数据结构、ID 生成、`contextvars` 上下文载体、线程/异步分支传播、后台完成门闩 |
| `app/errors.py` | 四类失败、`ClassifiedError` 包装、全量确定性分类器 `classify()`、安全对外载荷 |
| `app/metrics.py` | 按入口聚合的有界并发安全注册表（计数、耗时、固定桶直方图、重试/降级统计） |
| `app/tracer.py` | `trace_scope` / `span_scope` / 逻辑调用、同步与异步重试、降级、有界 trace 存储 |
| `app/services.py` | 示例业务与下游（同步/异步），全部由输入驱动、行为确定可复现 |
| `app/middleware.py` | ASGI 中间件：每请求一开 trace、记录指标、兜底安全错误响应 |
| `app/routes.py` | 入口：同步下单、异步下单、线程池+后台任务、指标/trace 查询 |

## 2. 追踪模型

### 2.1 Trace 与 Span

* 每个入口请求生成一个 **trace**：`trace_id = uuid4().hex`（32 位），通过
  响应头 `X-Trace-Id` 返回。
* trace 内含一棵 **span 树**：根 span 覆盖整个请求；业务步骤、下游调用、
  每次物理尝试、降级与后台任务都是子 span。span 记录：
  `span_id`、`parent_id`、`name`、起止耗时、失败类别、对外信息、
  原始异常类型与原文（`cause_type` / `cause_message`）、自定义属性。
* 入口名在路由匹配后由原始路径改写为**路由模板**（如 `POST /orders`），
  作为聚合键，避免路径中的业务 ID 造成基数膨胀。

### 2.2 上下文传播（并发不串号、不泄漏）

当前 trace 与“当前活跃 span 栈”分别存放在两个 `contextvars.ContextVar`：

* `asyncio` 创建任务时会复制调用方上下文，因此交错执行的异步任务天然携带
  同一 trace；span 栈在每个分支的上下文副本中独立 push/pop，**并发的
  兄弟分支不会互相成为父子**。
* 线程池/后台线程通过
  `run_with_context(ctx, fn, parent_span_id=...)` 显式绑定：
  内部使用 `copy_context()` 在隔离副本中执行，线程结束后调用方的 span 栈
  不被污染，其他请求的上下文也不可能流入。
* 请求结束后仍运行的后台任务（示例 `POST /orders/background` 中的
  `post_response_hook`）通过 trace 上的完成门闩
  （`begin_background/end_background`）被中间件等待（上限 2 秒），
  其 span 与主请求归入同一 trace。

### 2.3 重试与降级的 span 形态

一次**逻辑下游调用**只有一个父 span；物理尝试与降级作为其子 span：

```
POST /orders
└─ payment                         # 逻辑调用（无论试几次只计一次）
   ├─ payment/attempt-1            # 失败：error_category + cause 保留
   ├─ payment/attempt-2            # 失败
   └─ payment/attempt-3            # 成功
```

全部尝试失败且提供了降级函数时，额外出现 `payment/fallback` 子 span，并在
trace 上标记 `fell_back=true`。

## 3. 失败类别

| 类别 | 含义 | HTTP 状态 | 对外固定文案 |
| --- | --- | --- | --- |
| `validation_error` | 参数/载荷不合法（含 JSON 解析失败、原始 `ValueError/TypeError/KeyError`） | 400 | `invalid request parameters` |
| `business_error` | 业务规则拒绝（库存不足、优惠券不存在） | 422 | `business rule rejected` |
| `downstream_error` | 下游调用失败（原始 `ConnectionError/TimeoutError/OSError` 等） | 502 | `dependent service unavailable` |
| `internal_error` | 其余未预期异常的兜底类别 | 500 | `internal server error` |

关键约定：

* **分类是全量且确定的**：`classify()` 对任意异常映射到且仅映射到一个类别；
  已经分类的异常原样透传（幂等）。同一失败经同步入口、异步入口或重试路径
  进入，类别、状态码与对外载荷完全一致。
* **原始异常不外泄**：对外响应只包含类别与固定文案，不含库异常类型、
  主机地址、堆栈等；原始原因通过 `ClassifiedError.cause`（标准异常链）和
  span 上的 `cause_type/cause_message` 在 trace 记录中可回溯。
* 对外错误体统一为：
  ```json
  {"error": {"category": "downstream_error", "message": "dependent service unavailable"}}
  ```

## 4. 本地聚合（`GET /metrics`）

按入口名聚合，字段包括：

* `count` / `success_count` / `error_count`：逻辑请求数（不是物理尝试数）；
* `avg_ms / min_ms / max_ms`；
* `histogram`：固定 12 桶分布，桶边界
  `5,10,25,50,100,250,500,1000,2500,5000,10000` ms；
* `by_category`：各类失败计数；
* `retry_attempts`：额外物理尝试总数；`fallback_count`：走降级的次数；
* `recent`：最近 64 条样本（环形缓冲）。

**不重复计数**：中间件对每个入口请求只调用一次 `registry.record(...)`；
重试/降级仅累加专用字段。

**并发一致**：所有读写在单一 `threading.Lock` 下完成，测试以
16 线程 × 250 次写入验证无丢失更新。

**内存上界**：

* 入口键数量上限 `DEFAULT_MAX_ENTRIES = 128`，超出后丢弃新键并计入
  `dropped_entries`；
* 每入口只保留最近 64 条原始样本；直方图为定长数组；
* trace 存储为有界双端队列，默认保留最近 `DEFAULT_MAX_TRACES = 512` 条，
  超出淘汰最旧记录（见 `GET /traces` 的 `store.dropped`）。

## 5. HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `GET /` | 健康检查，响应体内带当前 `trace_id` |
| `POST /orders` | 同步下单（校验→库存→优惠券→支付，含重试） |
| `POST /orders/async` | 异步下单（异步下游，可交错） |
| `POST /orders/background` | 线程池执行 + 响应后后台钩子，同 trace 延续 |
| `GET /metrics` | 本地聚合结果 |
| `GET /traces` | 最近 trace 列表 |
| `GET /traces?trace_id=<id>` | 单条 trace（含完整 span 树与原始原因） |

示例输入触发的失败（全部由输入决定、可复现）：

* `{"item_id":"net-down",...}` → 下游连接错误（原始 `ConnectionError`）；
* `{"item_id":"slow-host",...}` → 下游超时（原始 `TimeoutError`）；
* `quantity` 大于库存 → 业务错误；缺字段 / 非 JSON → 校验错误；
* `payment_token="flaky"` → 支付首次连接重置、重试成功；
* `payment_token="down"/"timeout"` → 支付持续失败/超时。

## 6. 验证方法

```bash
# 全部单测（19 个），含输入、trace 标识与归因日志
python3 -m pytest tests/ -s

# 仅并发串号用例（5 波 × 7 个交错请求，断言 trace_id 两两不同、归因全对）
python3 -m pytest tests/test_tracing.py::test_interleaved_concurrent_requests_stay_isolated -s

# 手工体验
uv run fastapi dev main.py
curl -s localhost:8000/metrics | python3 -m json.tool
curl -s 'localhost:8000/traces?trace_id=<X-Trace-Id>' | python3 -m json.tool
```

测试覆盖点：

1. **上下文传播**：ID 唯一与父子关系；24 线程屏障交错不串号、上下文不外泄；
   6 个交错异步任务互为兄弟；工作线程隔离 span 栈；
2. **失败归因**：原始库异常包装且不外泄；四类可区分；同步/异步/重试入口
   对同一失败归因一致；根 span 与尝试 span 均保留原始异常链；
3. **聚合与上界**：重试不重复计数逻辑调用；降级仅计一次并打标；注册表
   并发写入无丢失、入口数有上界；trace 存储有上界；端到端 5 个逻辑请求
   计数为 5（其中 1 次内部重试）；
4. 每条用例均通过 `tracing-tests` logger 打印**输入、trace 标识与归因结论**。
