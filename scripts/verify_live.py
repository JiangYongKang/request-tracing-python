"""本地实机验收脚本：对运行中的服务打并发/失败流量，导出后再导入干净环境核对。

用法：
    1) uvicorn tracing_demo.app:app --port 8137
    2) python scripts/verify_live.py [--url http://127.0.0.1:8137]

脚本会：并发打混合流量 -> 调导出端点 -> 在本进程（相对服务端是干净环境）
用全新的 recorder/metrics 导入导出文件 -> 逐项核对记录与聚合结论。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import tempfile

import httpx

from tracing_demo.importer import import_traces
from tracing_demo.metrics import MetricsRegistry
from tracing_demo.recorder import TraceRecorder

CASES = [
    *[(f"order-ok-{i}", "POST", "/orders", {"item_id": f"item-live-{i}", "qty": 2}, 200)
      for i in range(8)],
    ("order-validation", "POST", "/orders", {"item_id": "", "qty": 2}, 400),
    ("order-business", "POST", "/orders", {"item_id": "item-restricted", "qty": 1}, 409),
    ("payment-downstream", "POST", "/payments",
     {"order_id": "o-live-fail", "amount": 5, "payment_fail_times": 9}, 502),
    ("order-degraded", "POST", "/orders",
     {"item_id": "item-live-deg", "qty": 1, "inventory_fail_times": 9}, 200),
    ("order-audit-fail", "POST", "/orders",
     {"item_id": "item-live-audit", "qty": 1, "audit_fail": True}, 200),
    *[(f"payment-ok-{i}", "POST", "/payments", {"order_id": f"o-live-{i}", "amount": 3.3}, 200)
      for i in range(3)],
    ("status", "GET", "/status", None, 200),
]


async def drive(url: str) -> None:
    async with httpx.AsyncClient(base_url=url, timeout=30) as client:
        async def one(name, method, path, payload, expect):
            resp = await client.request(method, path, json=payload)
            body = resp.json()
            assert resp.status_code == expect, (name, resp.status_code, body)
            print(f"[traffic] {name:22s} input={payload} -> http={resp.status_code} "
                  f"trace_id={body.get('trace_id')} category={(body.get('error') or {}).get('category')}")

        await asyncio.gather(*[one(*c) for c in CASES])

        # 等后台审计任务落盘（轮询订单链路出现 background:audit）。
        await asyncio.sleep(0.5)

        export_path = tempfile.mktemp(prefix="live-export-", suffix=".ndjson")
        resp = await client.post("/traces/export", json={"path": export_path})
        resp.raise_for_status()
        print(f"[export] {resp.json()}")
        metrics_src = (await client.get("/metrics")).json()
        print(f"[export-side metrics] {json.dumps(metrics_src, indent=2)}")

        # ---- 干净环境导入核对（本进程全新的 recorder/metrics）----
        recorder, metrics = TraceRecorder(), MetricsRegistry()
        result = import_traces(recorder, metrics, export_path)
        assert result.ok, result
        print(f"[import] {result}")
        again = import_traces(recorder, metrics, export_path)
        assert again.ok and again.imported == 0 and again.skipped == result.imported
        print(f"[import-again] {again} (idempotent)")

        restored = metrics.snapshot()
        problems = []
        for entry, src in metrics_src["entries"].items():
            dst = restored["entries"].get(entry)
            if dst is None:
                problems.append(f"missing entry {entry}")
                continue
            for key in ("count", "error_count"):
                if src[key] != dst[key]:
                    problems.append(f"entry {entry} {key}: {src[key]} != {dst[key]}")
            for key in ("avg_ms", "min_ms", "max_ms", "p50_ms", "p95_ms", "p99_ms"):
                if not math.isclose(src[key], dst[key], rel_tol=1e-9, abs_tol=1e-12):
                    problems.append(f"entry {entry} {key}: {src[key]} != {dst[key]}")
        if metrics_src["dependencies"] != restored["dependencies"]:
            problems.append(
                f"dependencies differ: {metrics_src['dependencies']} != {restored['dependencies']}")
        print(f"[restore-check] entries+dependencies {'MATCH' if not problems else problems}")

        # 抽查失败链路的归因与原始原因是否随文件带走。
        with open(export_path, encoding="utf-8") as fh:
            header = json.loads(fh.readline())
            records = [json.loads(line) for line in fh if line.strip()]
        print(f"[file] version={header['version']} records={len(records)}")
        for rec in records:
            if rec["status"] == "error":
                cats = [e["category"] for e in rec["errors"]]
                causes = [e["cause_type"] for e in rec["errors"]]
                print(f"[attribution] trace_id={rec['trace_id']} entry={rec['entry']} "
                      f"categories={cats} causes={causes}")
                assert all(c for c in causes), "原始原因必须随文件带走"
        if problems:
            sys.exit(f"VERIFY FAILED: {problems}")
        print("VERIFY OK: 导出文件在干净环境还原后，记录/归因/聚合均与导出端一致")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8137")
    args = parser.parse_args()
    asyncio.run(drive(args.url))
