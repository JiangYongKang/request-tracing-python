"""应用层端到端：中间件追踪、统一脱敏错误响应、本地查看与导出/导入端点。"""
from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from tracing_demo.app import app
from tracing_demo import service

log = logging.getLogger("test.app")


def _run(coro):
    return asyncio.run(coro)


async def _client():
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


def _drain_background():
    for f in service.background_futures():
        f.result(timeout=5)


def test_success_and_failure_requests_traced_end_to_end(tmp_path):
    async def main():
        async with await _client() as c:
            ok = await c.post("/orders", json={"item_id": "item-1", "qty": 2})
            assert ok.status_code == 200
            ok_tid = ok.json()["trace_id"]

            degraded = await c.post("/orders", json={"item_id": "item-2", "qty": 1,
                                                     "inventory_fail_times": 9})
            assert degraded.status_code == 200
            assert degraded.json()["reservation"]["degraded"] is True

            bad = await c.post("/orders", json={"item_id": "", "qty": 1})
            assert bad.status_code == 400
            body = bad.json()
            assert body["category"] == "validation"
            assert "trace_id" in body
            # 对外响应不含底层库原始异常文本
            assert "raw" not in body["error"].lower()

            down = await c.post("/payments", json={"order_id": "o1", "amount": 5,
                                                   "payment_fail_times": 9})
            assert down.status_code == 502
            assert down.json()["category"] == "downstream"
            down_tid = down.json()["trace_id"]

            _drain_background()

            # 本地查看：每条链路完整（含后台 span 与失败归因）
            traces = (await c.get("/traces")).json()["traces"]
            assert len(traces) == 4
            detail = (await c.get(f"/traces/{ok_tid}")).json()
            names = [s["name"] for s in detail["spans"]]
            log.info("trace_id=%s spans=%s", ok_tid[:8], names)
            assert "background:audit" in names, "后台任务的 span 挂在同一链路下"

            fail_detail = (await c.get(f"/traces/{down_tid}")).json()
            assert fail_detail["status"] == "error"
            err = fail_detail["errors"][0]
            log.info("trace_id=%s 归因=%s 原始原因类型=%s",
                     down_tid[:8], err["category"], err["cause_type"])
            assert err["category"] == "downstream"
            assert err["cause_type"] == "DownstreamConnectionError", "原始原因一并保留"

            # 聚合指标按入口可见
            metrics = (await c.get("/metrics")).json()
            assert metrics["entries"]["/orders"]["count"] == 3
            assert metrics["entries"]["/orders"]["error_count"] == 1
            # 3 个不同逻辑键的支付调用各计 1 次逻辑调用；
            # attempts = 成功 1 + 降级单 1 + 失败重试 2 = 4，重试不重复计逻辑调用。
            assert metrics["dependencies"]["payment"]["logical_calls"] == 3
            assert metrics["dependencies"]["payment"]["attempts"] == 4
            assert metrics["dependencies"]["payment"]["failures"] == 2

            # 导出 -> 干净环境无法直接测这里，验证端点往返与文件内容
            path = str(tmp_path / "api-export.jsonl")
            r = await c.post("/export", json={"path": path, "only_failed": True})
            assert r.status_code == 200 and r.json()["record_count"] == 2
            r = await c.post("/import", json={"path": path})
            assert r.status_code == 200
            assert r.json()["added"] == 0 and r.json()["skipped_existing"] == 2, "同环境重复导入幂等"
            log.info("导出/导入端点: export=%s import=%s", 2, r.json())

    _run(main())


def test_management_endpoints_not_traced():
    async def main():
        async with await _client() as c:
            await c.get("/metrics")
            await c.get("/traces")
            traces = (await c.get("/traces")).json()["traces"]
            assert traces == [], "管理端点自身不产生追踪数据"

    _run(main())
