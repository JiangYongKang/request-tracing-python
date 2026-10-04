# request-tracing-python

本地请求追踪演示：每个入口请求一条 trace（父子 span、失败归因、原始原因留痕），
本地有界存储与聚合指标，并支持把追踪数据**导出为可携带的本地文件**、在另一个
干净环境中**导入复现**。完全本地实现，不依赖任何外部存储或采集服务。

## 运行服务

```bash
uv sync --dev
.venv/bin/python -m uvicorn main:app --port 8000
```

业务入口：`GET /status`、`POST /orders`、`POST /payments`。
本地查看：`GET /traces`、`GET /traces/{trace_id}`、`GET /metrics`（管理端点自身不产生追踪数据）。

## 导出：可携带的一致快照

`POST /export`，请求体：

```json
{
  "path": "/tmp/traces.jsonl",
  "entry": "/orders",            // 可选，按入口筛选
  "started_after": 1791125000.0, // 可选，时间范围（epoch 秒，含边界）
  "started_before": null,
  "only_failed": true,           // 可选，只导失败链路
  "error_category": "downstream" // 可选，按失败类别筛选
}
```

导出期间服务正常接并发请求、后台任务可能继续往同一条链路写 span。
快照一致性保证：先取记录集合的点时快照，每条链路再在各自的 `span_lock`
内序列化，因此文件里每条链路都是完整自洽的——父子关系闭合、失败归因
与原始原因（`cause_type` / `cause_message`）一并带上，不会出现半条链路。

### 文件内容结构与版本

JSON Lines，逐行流式读写，大文件不会一次性吃满内存：

| 行 | 内容 |
| --- | --- |
| 第 1 行 | 头部：`format`（固定 `trace-export`）、`version`、`created_at`、`filter` |
| 中间每行 | `{"type": "record", "record": {...}}`，一条完整 trace（spans + errors） |
| 最后 1 行 | 尾部：`record_count` 与记录行的 `sha256`，用于发现损坏/不完整文件 |

版本兼容范围：当前写入版本 **v2**；可导入 **v1、v2**。
v1 的 span 没有 `attempt` 字段，导入时自动补默认值 `1`。
其余版本号、非本格式文件、JSON 损坏、缺尾部、校验和/条数不匹配，
都会返回明确的失败结果（`ok=false` + 原因），且环境中不留任何半份脏数据
（先整体流式校验通过，再逐条应用）。

## 导入：幂等、有界、可复现

`POST /import`，请求体 `{"path": "/tmp/traces.jsonl"}`。

- **幂等**：按 `trace_id` 去重。同一文件重复导入不会变成两份；环境中
  已有的同 id 追踪数据不会被覆盖或改动（`save_if_absent`）。
- **容量**：导入记录进入与本地写入相同的有界存储（`TraceRecorder`，
  默认 1024 条），超出上界按既有 FIFO 策略淘汰，返回结果中的
  `evicted` 字段说明淘汰条数。
- **聚合还原**：每条真正新增的链路会补登入口聚合指标（耗时分布、
  调用次数、失败数），因此导入后 `GET /metrics` 的按入口统计与导出端
  对得上；重复导入不会重复计数，重试/降级链路的去重逻辑
  （`logical_calls` vs `attempts`）不受影响。

## 本地验证

```bash
# 1. 自动化测试（含并发导出快照一致、重复导入幂等、版本兼容与损坏文件、
#    聚合还原一致，以及上一轮核心回归；日志含输入、trace_id 与归因结论）
.venv/bin/python -m pytest -q

# 2. 真实服务手工验收
.venv/bin/python -m uvicorn main:app --port 8000 &
# 并发打一批成功/降级/失败请求后：
curl -X POST localhost:8000/export -H 'content-type: application/json' \
     -d '{"path": "/tmp/all.jsonl"}'
curl -X POST localhost:8000/export -H 'content-type: application/json' \
     -d '{"path": "/tmp/failed.jsonl", "only_failed": true}'
# 在另一个干净进程里导入核对：
.venv/bin/python - <<'PY'
from tracing_demo.recorder import TraceRecorder
from tracing_demo.metrics import MetricsRegistry
from tracing_demo.transfer import import_traces
rec, m = TraceRecorder(), MetricsRegistry()
print(import_traces(rec, m, "/tmp/all.jsonl"))
print(m.snapshot()["entries"])  # 与服务端 GET /metrics 的入口聚合一致
PY
```
