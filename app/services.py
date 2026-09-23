"""Simulated business logic & downstream services (sync + async).

All behaviour is *input-driven* and deterministic so concurrent/interleaved
tests are reproducible: no randomness, real time or network involved.

Failure sources exercised
-------------------------
* parameter validation  -> ``validation_error`` (missing/typed fields,
  raw ``ValueError``/``KeyError`` from "parsing")
* business rule         -> ``business_error`` (stock exhausted, bad coupon)
* downstream call       -> raw ``ConnectionError`` / ``TimeoutError`` raised
  by the simulated client, wrapped by classification as ``downstream_error``
"""
from __future__ import annotations

import asyncio

from .errors import (
    ClassifiedError,
    business_error,
    downstream_error,
    validation_error,
)
from .tracer import call_with_retry, call_with_retry_async, span_scope

# Deterministic "database" of stock.
_INVENTORY: dict[str, int] = {
    "widget": 5,
    "gadget": 2,
    "instant-sellout": 1,
}
_COUPONS: dict[str, float] = {"SAVE10": 0.10, "BIG20": 0.20}

# Simulated downstream failure tokens understood by the fake payment host.
PAYMENT_FAIL_ALWAYS = "down"
PAYMENT_FAIL_ONCE = "flaky"
PAYMENT_TIMEOUT = "timeout"


def validate_order(payload: object) -> dict:
    """Validate the raw request payload; raises ``validation_error``.

    Demonstrates wrapping a raw library-style error (``KeyError``) rather than
    letting it escape: the original cause is retained on the exception.
    """
    with span_scope("validate_order"):
        if not isinstance(payload, dict):
            raise validation_error("payload must be a JSON object")
        try:
            item_id = payload["item_id"]
            quantity = payload["quantity"]
        except KeyError as exc:
            raise validation_error(
                f"missing required field: {exc.args[0]}", cause=exc
            ) from exc
        if not isinstance(item_id, str) or not item_id:
            raise validation_error("item_id must be a non-empty string")
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
            raise validation_error("quantity must be a positive integer")
        amount = payload.get("amount", quantity)
        if not isinstance(amount, (int, float)) or isinstance(amount, bool) or amount <= 0:
            raise validation_error("amount must be a positive number")
        return {
            "item_id": item_id,
            "quantity": quantity,
            "amount": float(amount),
            "coupon": payload.get("coupon"),
            "payment_token": payload.get("payment_token", "ok"),
        }


def query_inventory(item_id: str) -> dict:
    """Simulated *synchronous* downstream inventory lookup.

    Raises raw standard library exceptions to prove classification/wrapping.
    """
    if item_id == "net-down":
        raise ConnectionError("inventory host unreachable: 10.0.0.9:6433")
    if item_id == "slow-host":
        raise TimeoutError("inventory host timed out after 500ms")
    stock = _INVENTORY.get(item_id, 0)
    return {"item_id": item_id, "in_stock": stock, "available": stock > 0}


async def query_inventory_async(item_id: str) -> dict:
    """Simulated *asynchronous* downstream inventory lookup."""
    if item_id == "net-down":
        raise ConnectionError("inventory host unreachable: 10.0.0.9:6433")
    if item_id == "slow-host":
        raise TimeoutError("inventory host timed out after 500ms")
    await asyncio.sleep(0)  # cooperative point, lets tasks interleave
    stock = _INVENTORY.get(item_id, 0)
    return {"item_id": item_id, "in_stock": stock, "available": stock > 0}


def _payment_host(order_id: str, amount: float, token: str) -> dict:
    """Raw fake downstream: raises library exceptions, never ClassifiedError."""
    if token == PAYMENT_FAIL_ALWAYS:
        raise ConnectionError(f"payment gateway refused conn for {order_id}")
    if token == PAYMENT_TIMEOUT:
        raise TimeoutError(f"payment gateway timeout for {order_id}")
    if token == PAYMENT_FAIL_ONCE:
        # Fail exactly once per order id, then recover: deterministic retry.
        if not getattr(_payment_host, f"seen_{order_id}", False):
            setattr(_payment_host, f"seen_{order_id}", True)
            raise ConnectionResetError(f"transient reset for {order_id}")
    return {"order_id": order_id, "charged": amount, "status": "captured"}


def charge_payment(order_id: str, amount: float, token: str = "ok",
                   attempts: int = 1) -> dict:
    """Logical payment call with retries; wraps the raw host error on failure."""
    try:
        return call_with_retry(
            lambda: _payment_host(order_id, amount, token),
            "payment",
            attempts=attempts,
        )
    except Exception as exc:  # noqa: BLE001 - raw host error never escapes
        # Retry helper already classifies; preserve the originating network
        # exception as cause while presenting business-facing wording.
        if isinstance(exc, ClassifiedError):
            root_cause = exc.cause or exc
        else:
            root_cause = exc
        raise downstream_error("payment call failed", cause=root_cause) from exc


def fetch_coupon(coupon_id: str | None) -> float:
    with span_scope("coupon_lookup"):
        if not coupon_id:
            return 0.0
        if coupon_id not in _COUPONS:
            # Unknown coupon is a business rejection, not a downstream fault.
            raise business_error(f"unknown coupon: {coupon_id}")
        return _COUPONS[coupon_id]


def place_order(payload: object) -> dict:
    """Synchronous order flow: validate -> inventory -> coupon -> payment."""
    order = validate_order(payload)
    with span_scope("place_order"):
        inv = call_with_retry(
            lambda: query_inventory(order["item_id"]),
            "inventory",
            attempts=2 if order["item_id"] == "retry-inv" else 1,
        )
        if inv["in_stock"] < order["quantity"]:
            raise business_error(
                f"insufficient stock: have {inv['in_stock']}, "
                f"requested {order['quantity']}"
            )
        discount = fetch_coupon(order["coupon"])
        final_amount = order["amount"] * (1 - discount)
        charge = charge_payment(
            order_id=f"ord-{order['item_id']}",
            amount=final_amount,
            token=order["payment_token"],
            attempts=2 if order["payment_token"] == PAYMENT_FAIL_ONCE else 1,
        )
        return {
            "order_id": charge["order_id"],
            "item_id": order["item_id"],
            "quantity": order["quantity"],
            "charged": charge["charged"],
            "discount": discount,
            "mode": "sync",
        }


async def charge_payment_async(order_id: str, amount: float, token: str,
                               attempts: int = 1) -> dict:
    async def host() -> dict:
        return _payment_host(order_id, amount, token)

    try:
        return await call_with_retry_async(
            host, "payment", attempts=attempts
        )
    except Exception as exc:  # noqa: BLE001
        raise downstream_error("payment call failed", cause=exc) from exc


async def place_order_async(payload: object) -> dict:
    """Async order flow: inventory runs as a retried async logical call."""
    order = validate_order(payload)
    with span_scope("place_order_async"):
        inv = await call_with_retry_async(
            lambda: query_inventory_async(order["item_id"]),
            "inventory",
            attempts=2 if order["item_id"] == "retry-inv" else 1,
        )
        if inv["in_stock"] < order["quantity"]:
            raise business_error(
                f"insufficient stock: have {inv['in_stock']}, "
                f"requested {order['quantity']}"
            )
        discount = fetch_coupon(order["coupon"])
        final_amount = order["amount"] * (1 - discount)
        charge = await charge_payment_async(
            order_id=f"aord-{order['item_id']}",
            amount=final_amount,
            token=order["payment_token"],
            attempts=2 if order["payment_token"] == PAYMENT_FAIL_ONCE else 1,
        )
        return {
            "order_id": charge["order_id"],
            "item_id": order["item_id"],
            "quantity": order["quantity"],
            "charged": charge["charged"],
            "discount": discount,
            "mode": "async",
        }
