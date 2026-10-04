# request-tracing-demo

本地请求追踪演示：追踪上下文跨同步/异步/线程传播、失败稳定归因、
有界本地存储与聚合，以及**追踪数据的本地导出/导入**（本轮新增）。

## 运行服务

```bash
uv sync
.venv/bin/uvicorn tracing_demo.app:app --port 8137
```

业务入口：`POST /orders`、`POST /payments`、`GET /status`。
本地查看：`GET /traces`、`GET /traces/{trace_id}`、`GET /metrics`
（查看/管理端点自身不产生追踪记录）。

## 追踪数据导出（本地文件）

`POST /traces/export`，请求体：

```json
{
  "path": "/tmp/export.ndjson",
  "entry": "POST /orders",        // 可选，按入口精确筛选
  "since": 1759600000.0,          // 可选，started_at 下界（epoch 秒，含）
  "until": 1759610000.0,          // 可选，started_at 上界（含）
  "failed_only": true,            // 可选，只导出失败链路
  "error_category": "downstream"  // 可选，按失败类别筛选
}
```

- **一致快照**：导出在服务正常接待并发请求时也可进行。每条记录在其
  自身锁内序列化（spans 与 errors 同一把锁），单条链路不会只导出一半，
  父子关系与失败归因（含原始原因 `cause_type`/`cause_message`）保持一致；
  后台任务后续追加的 span 要么整体进入下一次快照，要么不出现，
  不会产生悬空父引用。
- **原子落盘**：先写同目录临时文件再 `os.replace`，失败不留半截文件。
- **内存有界**：NDJSON 逐条序列化逐行写，文件大小不占额外内存。

### 导出文件内容结构

NDJSON（每行一个 JSON 对象）：

```
{"format": "request-tracing-demo/export", "version": 1, "exported_at": ..., "filters": {...}}   // 第 1 行：头部
{"trace_id": ..., "entry": ..., "started_at": ..., "duration_ms": ..., "status": ...,
 "spans": [{"span_id", "parent_id", "name", "started_at", "duration_ms", "status",
            "error_category", "attempt"}, ...],
 "errors": [{"category", "public_message", "cause_type", "cause_message",
             "span_name", "trace_id"}, ...]}                                                     // 后续每行一条链路
```

### 版本兼容范围

- 当前内容版本为 **1**（`EXPORT_VERSION`），导入端接受
  `SUPPORTED_VERSIONS = {1}`。
- 同版本内的**旧内容尽量还原**：记录/span/error 的可选字段
  （如 span 的 `attempt`、`status`、`duration_ms`，error 的 `cause_*`）
  缺失时按默认值还原（`attempt=1`、`status="ok"`、耗时 0、原因为空）。
- 版本不在支持集合、格式标识不符、JSON 损坏、字段类型非法、内容不完整
  （如半截行）：导入返回明确失败（`ok=false` + 具体原因），
  且**不会在环境中留下半份数据**——导入分两遍流式处理，
  第一遍只校验，全部通过后才在第二遍应用。

## 追踪数据导入

`POST /traces/import`，请求体：`{"path": "/tmp/export.ndjson"}`。

- **幂等**：按 `trace_id` 去重。同一文件重复导入不会产生第二份记录，
  聚合指标也不会重复累加；环境中已存在的同 `trace_id` 记录
  **不会被覆盖或改动**（导入整体跳过该条）。
- **容量行为**：导入记录走与实时流量相同的 `TraceRecorder` 容量上界
  （默认 1024 条），超出按既有 FIFO 策略淘汰；流式逐行解析，
  大文件不会一次性吃满内存。
- **聚合还原**：导入时按记录补登聚合指标——入口耗时分布/调用次数按
  记录重放；下游调用按 `dep:{dependency}.{operation}` span 命名约定还原
  物理尝试次数，同一 `(trace, dependency)` 的逻辑调用只计一次，
  重试与降级（`:fallback` 本地决策）不会被重复计数。干净环境导入后，
  `GET /metrics` 的入口分布与依赖计数与导出端一致。

## 自动化测试

```bash
.venv/bin/python -m pytest
```

覆盖：并发导出时快照一致、重复导入幂等且不覆盖已有数据、
版本兼容与损坏/不完整文件处理、导入后记录与聚合结果还原一致、
导出筛选条件。测试日志（`log_cli` 已开启）包含每个用例的输入、
追踪标识（trace_id）与归因结论。

## 本地实机验证

```bash
# 终端 1
.venv/bin/uvicorn tracing_demo.app:app --port 8137
# 终端 2：并发打混合流量（含失败/重试/降级/后台任务），导出后在干净环境导入核对
PYTHONPATH=. .venv/bin/python scripts/verify_live.py
```

脚本会打印每个请求的输入与 trace_id、导出端聚合快照、导入结果
（含重复导入幂等检查）、逐条失败链路的归因类别与原始原因，
最后输出 `VERIFY OK` 表示导出文件在干净环境还原后与导出端一致。
