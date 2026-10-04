"""演示业务服务。

覆盖追踪模型要求的全部传播与失败路径：
- 同步下游调用（库存，带重试与本地降级）；
- 异步下游调用（支付，带重试）；
- 后台任务（审计线程，沿用同一 trace 与父子关系）；
- 参数校验失败 / 业务规则失败 / 下游调用失败，归因类别互不相同。

所有下游客户端都模拟“底层库原始异常”（如 ConnectionError / TimeoutError），
由追踪边界统一包装为 :class:`~tracing_demo.errors.TracedError`，
业务代码之外永远拿不到底层库原始异常文本。
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Awaitable, Callable
from typing import Any

from tracing_demo import context as ctxmod
from tracing_demo.context import bind_context, current_trace_id, detach_trace, use_span
from tracing_demo.errors import ErrorCategory, TracedError, as_traced
from tracing_demo.metrics import get_metrics


# ---------------------------------------------------------------------------
# 模拟底层库抛出的“原始异常”（不得透出到对外响应）
# ---------------------------------------------------------------------------
class DownstreamConnectionError(Exception):
    """模拟某 HTTP/SDK 库的原始连接异常。"""


class DownstreamTimeoutError(Exception):
    """模拟某 HTTP/SDK 库的原始超时异常。"""


class InventoryRejected(Exception):
    """库存系统的业务拒绝（原始库异常形态，由边界归类为业务失败）。"""


# ---------------------------------------------------------------------------
# 下游客户端
# ---------------------------------------------------------------------------
class InventoryClient:
    """同步库存下游。``fail_times`` 控制前 N 次尝试抛底层库原始异常。"""

    def check_reserve(self, item_id: str, qty: int, *, attempt: int, fail_times: int = 0) -> dict:
        if attempt <= fail_times:
            # 随机形态的原始异常，证明归因不依赖具体异常类型文本。
            if attempt % 2 == 1:
                raise DownstreamConnectionError(f"raw socket reset to inventory svc: {item_id}")
            raise DownstreamTimeoutError(f"raw read timed out after 5000ms: {item_id}")
        return {"reserved": qty, "item_id": item_id, "via": "inventory-service"}

    def reject(self, item_id: str) -> None:
        raise InventoryRejected(f"raw policy reject from inventory: {item_id}")


class PaymentClient:
    """异步支付下游。"""

    async def charge(self, order_id: str, amount: float, *, attempt: int, fail_times: int = 0) -> dict:
        await asyncio.sleep(0)
        if attempt <= fail_times:
            raise DownstreamConnectionError(f"raw connection refused by payment gateway: {order_id}")
        return {"paid": amount, "order_id": order_id, "via": "payment-gateway"}


inventory_client = InventoryClient()
payment_client = PaymentClient()


# ---------------------------------------------------------------------------
# 通用：带追踪与指标的下游调用（重试 / 降级）
# ---------------------------------------------------------------------------
async def _call_with_retries(
    dependency: str,
    logical_key: str,
    fn: Callable[..., Any],
    *args: Any,
    max_attempts: int = 3,
    fail_times: int = 0,
    span_name: str | None = None,
    fallback: Callable[[BaseException], Any] | None = None,
    **kwargs: Any,
) -> Any:
    """在独立 span 内执行下游调用，失败按 attempt 重试。

    - 每次物理尝试都进入指标 ``attempts``；同一逻辑键只计一次
      ``logical_calls``（见 :class:`~tracing_demo.metrics.MetricsRegistry`）；
    - 底层库原始异常在 span 内即包装为 DOWNSTREAM 类别的
      :class:`TracedError`，保证归因一致、原始异常链保留；
    - 重试耗尽后调用 ``fallback``（降级），否则抛出最后一次异常。
    """
    trace_id = current_trace_id()
    name = span_name or f"dep:{dependency}"
    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        is_retry = attempt > 1
        with use_span(f"{name}#attempt{attempt}" if is_retry else name, attempt=attempt):
            try:
                result = fn(*args, attempt=attempt, fail_times=fail_times, **kwargs)
                if asyncio.iscoroutine(result):
                    result = await result
            except TracedError:
                raise  # 已归因（如业务拒绝），原样向上传播
            except BaseException as exc:  # 底层库原始异常 -> DOWNSTREAM
                wrapped = TracedError(
                    ErrorCategory.DOWNSTREAM,
                    f"downstream {dependency} unavailable",
                    cause=exc,
                )
                get_metrics().record_dependency(
                    trace_id or "",
                    dependency,
                    logical_key,
                    is_error=True,
                    is_retry=is_retry,
                )
                last_exc = wrapped
            else:
                get_metrics().record_dependency(
                    trace_id or "",
                    dependency,
                    logical_key,
                    is_error=False,
                    is_retry=is_retry,
                )
                return result
    if fallback is not None:
        with use_span(f"{name}:fallback"):
            return fallback(last_exc)  # type: ignore[arg-type]
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 校验与业务规则（显式、稳定的归因）
# ---------------------------------------------------------------------------
RESTRICTED_ITEM = "item-restricted"


def _validate_order(payload: dict) -> tuple[str, int, bool]:
    if not isinstance(payload, dict):
        raise TracedError(
            ErrorCategory.VALIDATION, "request body must be an object"
        )
    item_id = payload.get("item_id")
    qty = payload.get("qty")
    if not isinstance(item_id, str) or not item_id:
        raise TracedError(
            ErrorCategory.VALIDATION, "field 'item_id' is required",
            cause=KeyError("item_id"),
        )
    if not isinstance(qty, int) or isinstance(qty, bool) or qty <= 0:
        raise TracedError(
            ErrorCategory.VALIDATION, "field 'qty' must be a positive integer",
            cause=TypeError("qty"),
        )
    return item_id, qty, bool(payload.get("vip", False))


def _validate_payment(payload: dict) -> tuple[str, float]:
    if not isinstance(payload, dict):
        raise TracedError(
            ErrorCategory.VALIDATION, "request body must be an object"
        )
    order_id = payload.get("order_id")
    amount = payload.get("amount")
    if not isinstance(order_id, str) or not order_id:
        raise TracedError(
            ErrorCategory.VALIDATION, "field 'order_id' is required",
            cause=KeyError("order_id"),
        )
    if not isinstance(amount, (int, float)) or isinstance(amount, bool) or amount <= 0:
        raise TracedError(
            ErrorCategory.VALIDATION, "field 'amount' must be a positive number",
            cause=TypeError("amount"),
        )
    return order_id, float(amount)


# ---------------------------------------------------------------------------
# 后台任务：独立线程，沿用同一 trace 上下文
# ---------------------------------------------------------------------------
_background_futures: list[concurrent.futures.Future] = []
_background_lock = threading.Lock()


def run_in_background(ctx: ctxmod.TraceContext, fn: Callable[[], None]) -> concurrent.futures.Future:
    """在后台线程执行 ``fn``，绑定入口 trace 的上下文。"""

    def _runner() -> None:
        token = bind_context(ctx)
        try:
            fn()
        finally:
            detach_trace(token)

    future: concurrent.futures.Future = concurrent.futures.Future()

    def _target() -> None:
        try:
            _runner()
        except BaseException as exc:  # 后台异常也必须留痕
            future.set_exception(exc)
        else:
            future.set_result(None)

    thread = threading.Thread(
        target=_target, name=f"trace-bg-{ctx.trace_id[:8]}", daemon=True
    )
    with _background_lock:
        _background_futures.append(future)
        # 仅保留最近 256 个后台任务句柄（有界）。
        if len(_background_futures) > 256:
            del _background_futures[:-256]
    thread.start()
    return future


def background_futures() -> list[concurrent.futures.Future]:
    """供测试等待后台任务完成。"""
    with _background_lock:
        return list(_background_futures)


def reset_background() -> None:
    with _background_lock:
        _background_futures.clear()


def _audit_job(item_id: str, qty: int, *, fail: bool = False) -> None:
    """审计后台任务：span 挂在入口根 span 下，异常不外抛但留痕。"""
    with use_span("background:audit"):
        if fail:
            exc = RuntimeError(f"raw audit writer disk full for {item_id}")
            ctxmod.record_error(
                TracedError(ErrorCategory.INTERNAL, "audit failed", cause=exc),
                span_name="background:audit",
            )
        with use_span("background:audit:write"):
            pass


# ---------------------------------------------------------------------------
# 入口业务
# ---------------------------------------------------------------------------
async def submit_order(payload: dict) -> dict:
    """下单入口：校验 -> 库存(同步, 重试+降级) -> 支付(异步, 重试) -> 后台审计。"""
    item_id, qty, vip = _validate_order(payload)

    with use_span("business:reserve_inventory"):
        if item_id == RESTRICTED_ITEM and not vip:
            # 同一业务拒绝无论从哪个入口进入，都稳定归为 BUSINESS。
            try:
                inventory_client.reject(item_id)
            except InventoryRejected as raw:
                raise TracedError(
                    ErrorCategory.BUSINESS,
                    f"item {item_id} is restricted for non-vip orders",
                    cause=raw,
                )

        def _inventory_fallback(exc: BaseException) -> dict:
            with use_span("business:reserve_inventory:local_decision"):
                # 纯本地降级决策，不产生新的下游网络调用，因此不会重复计数。
                return {"reserved": qty, "item_id": item_id, "via": "local-cache", "degraded": True}

        reservation = await _call_with_retries(
            "inventory",
            f"reserve#{item_id}#{qty}",
            inventory_client.check_reserve,
            item_id,
            qty,
            max_attempts=3,
            fail_times=int(payload.get("inventory_fail_times", 0)),
            span_name="dep:inventory.reserve",
            fallback=_inventory_fallback,
        )

    with use_span("business:charge_payment"):
        payment = await _call_with_retries(
            "payment",
            f"charge#order-{item_id}",
            payment_client.charge,
            f"order-{item_id}",
            float(qty) * 10.0,
            max_attempts=2,
            fail_times=int(payload.get("payment_fail_times", 0)),
            span_name="dep:payment.charge",
        )

    result = {
        "trace_id": current_trace_id(),
        "order": {"item_id": item_id, "qty": qty},
        "reservation": reservation,
        "payment": payment,
    }

    # 后台任务：响应先返回，审计在同一 trace 下继续。
    ctx = ctxmod.current_context()
    assert ctx is not None
    run_in_background(ctx, lambda: _audit_job(item_id, qty, fail=bool(payload.get("audit_fail", False))))
    return result


async def submit_payment(payload: dict) -> dict:
    """独立支付入口：与下单共用支付下游，验证跨入口归因一致。"""
    order_id, amount = _validate_payment(payload)
    with use_span("business:charge_payment"):
        payment = await _call_with_retries(
            "payment",
            f"charge#{order_id}",
            payment_client.charge,
            order_id,
            amount,
            max_attempts=2,
            fail_times=int(payload.get("payment_fail_times", 0)),
            span_name="dep:payment.charge",
        )
    return {"trace_id": current_trace_id(), "payment": payment}


def get_status() -> dict:
    """纯成功路径入口。"""
    with use_span("business:status"):
        status = {"ok": True, "service": "request-tracing-demo"}
    return {"trace_id": current_trace_id(), "status": status}
