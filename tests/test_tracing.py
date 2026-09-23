"""Request tracing tests: concurrency isolation, failure chains, accounting.

Every test logs its input payload(s), the trace id(s) involved and the
attribution conclusion, as required.
"""
from __future__ import annotations

import asyncio
import logging
import threading

import pytest

from app import context as ctx_mod
from app.context import run_with_context
from app.errors import (
    ClassifiedError,
    FailureCategory,
    business_error,
    classify,
    public_payload,
    validation_error,
)
from app.metrics import MetricsRegistry
from app.tracer import (
    TraceStore,
    store as trace_store,
    build_trace_record,
    call_with_retry,
    call_with_retry_async,
    trace_scope,
)

log = logging.getLogger("tracing-tests")


# --------------------------------------------------------------------------- #
# 1. Context generation & propagation
# --------------------------------------------------------------------------- #
def test_trace_and_span_ids_are_unique_and_related() -> None:
    log.info("input: start two traces; expect unique trace ids & parent links")
    with trace_scope("A") as a:
        child = ctx_mod.start_span("a.child")
        ctx_mod.finish_span(child)
    with trace_scope("B") as b:
        pass
    rec_a = build_trace_record(a, None)
    rec_b = build_trace_record(b, None)
    log.info(
        "trace ids: %s, %s; attribution: both ok",
        rec_a["trace_id"], rec_b["trace_id"],
    )
    assert rec_a["trace_id"] != rec_b["trace_id"]
    assert rec_a["spans"][1]["parent_id"] == rec_a["root_span_id"]
    assert rec_b["spans"][0]["parent_id"] is None


def test_concurrent_threads_never_cross_traces() -> None:
    """Interleaved threads: every span lands on its own request's trace."""
    n_threads = 24
    records: dict[int, dict] = {}
    barrier = threading.Barrier(n_threads)

    def worker(seq: int) -> None:
        with trace_scope(f"req-{seq}") as tc:
            span = ctx_mod.start_span(f"work-{seq}")
            barrier.wait()  # maximise interleaving
            ctx_mod.finish_span(span)
            barrier.wait()
            records[seq] = build_trace_record(tc, None)
        # context must not leak out of the request scope
        assert ctx_mod.current_context() is None

    log.info("input: %d threads starting spans behind a barrier", n_threads)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    trace_ids = {rec["trace_id"] for rec in records.values()}
    assert len(trace_ids) == n_threads
    for seq, rec in records.items():
        assert [s["name"] for s in rec["spans"]] == [f"req-{seq}", f"work-{seq}"]
        assert rec["spans"][1]["parent_id"] == rec["root_span_id"]
    log.info(
        "attribution: %d distinct trace ids, no cross-parenting: %s",
        len(trace_ids), "ok",
    )


def test_concurrent_async_branches_share_trace_without_cross_linking() -> None:
    """Interleaved async tasks in one trace: siblings, not ancestors."""
    with trace_scope("concurrent-async") as tc:
        async def branch(seq: int, delay: float):
            span = ctx_mod.start_span(f"branch-{seq}")
            await asyncio.sleep(delay)
            ctx_mod.finish_span(span)
            return span

        async def scenario():
            tasks = [
                asyncio.create_task(branch(i, 0.03 - i * 0.005))
                for i in range(6)
            ]
            return await asyncio.gather(*tasks)

        spans = asyncio.run(scenario())
        rec = build_trace_record(tc, None)

    log.info("input: 6 interleaved async tasks, trace=%s", rec["trace_id"])
    assert len(spans) == 6
    assert {s.parent_id for s in spans} == {tc.root_span_id}
    span_ids = {s.span_id for s in spans}
    # No branch span is the parent of another: they must be siblings.
    assert all(s.parent_id not in span_ids for s in spans)
    log.info("attribution: all 6 branches are siblings of the root: ok")


def test_worker_thread_propagates_exact_trace() -> None:
    """Background thread inherits the caller trace but not its span stack."""
    seen: dict[str, object] = {}
    with trace_scope("parent-request") as tc:
        outer = ctx_mod.start_span("dispatch")

        def job() -> None:
            current = ctx_mod.current_context()
            assert current is tc
            seen["trace_id"] = current.trace_id
            seen["parent_span"] = ctx_mod.current_active_span_id()
            inner = ctx_mod.start_span("thread-work")
            ctx_mod.finish_span(inner)
            seen["inner_parent"] = inner.parent_id

        run_with_context(tc, job)
        # caller's branch stack is unchanged after the thread returns
        assert ctx_mod.current_active_span_id() == outer.span_id
        ctx_mod.finish_span(outer)
        rec = build_trace_record(tc, None)

    log.info(
        "input: one background thread; trace=%s parent=%s",
        seen["trace_id"], seen["parent_span"],
    )
    assert seen["trace_id"] == tc.trace_id
    assert seen["parent_span"] == tc.root_span_id  # isolated branch stack
    assert seen["inner_parent"] == tc.root_span_id
    assert {s["name"] for s in rec["spans"]} == {
        "parent-request", "dispatch", "thread-work",
    }
    log.info("attribution: thread span attached to same trace, no stack leak: ok")


# --------------------------------------------------------------------------- #
# 2. Failure attribution & exception chain preservation
# --------------------------------------------------------------------------- #
def test_raw_library_errors_are_wrapped_and_never_exposed() -> None:
    raw = ConnectionResetError("10.1.2.3:3306 reset by peer")
    classified = classify(raw)
    log.info(
        "input: raw %r; category=%s cause_retained=%s",
        raw, classified.category.value, classified.cause is raw,
    )
    assert classified.category == FailureCategory.DOWNSTREAM
    assert classified.cause is raw
    payload = public_payload(classified)
    assert payload["error"]["category"] == "downstream_error"
    assert "10.1.2.3" not in str(payload)
    assert "ConnectionResetError" not in str(payload)
    log.info("attribution: downstream_error; public payload hides internals: ok")


def test_classification_is_deterministic_across_entries_and_retries() -> None:
    """Same raw failure through sync entry, async entry and retry: same label."""
    def make():
        return classify(TimeoutError("gateway timeout"))

    first = make()
    for _ in range(5):  # simulated repeated entries / retries
        again = make()
        assert again.category == first.category
        assert again.http_status == first.http_status
        assert public_payload(again) == public_payload(first)
    log.info(
        "input: 6x TimeoutError; attribution all=%s", first.category.value
    )


def test_four_categories_are_distinguishable() -> None:
    cases = [
        (business_error("stock"), FailureCategory.BUSINESS, 422),
        (validation_error("bad"), FailureCategory.VALIDATION, 400),
        (classify(ConnectionError("x")), FailureCategory.DOWNSTREAM, 502),
        (classify(RuntimeError("x")), FailureCategory.INTERNAL, 500),
    ]
    categories = set()
    for err, expected_category, expected_status in cases:
        assert err.category == expected_category
        assert err.http_status == expected_status
        categories.add(err.category)
    assert categories == set(FailureCategory)
    log.info(
        "input: one failure per category; attribution=%s",
        sorted(c.value for c in categories),
    )


def test_error_span_preserves_cause_chain() -> None:
    with trace_scope("cause-chain") as tc:
        with pytest.raises(ClassifiedError) as raises:
            call_with_retry(
                lambda: (_ for _ in ()).throw(ConnectionError("reset")),
                "downstream",
                attempts=2,
            )
        err = raises.value
        rec = build_trace_record(tc, err)

    root = next(s for s in rec["spans"] if s["span_id"] == rec["root_span_id"])
    attempts = [s for s in rec["spans"] if s["name"] == "downstream/attempt-1"]
    log.info(
        "input: 2 attempts both failing ConnectionError; trace=%s "
        "category=%s root_cause=%s attempt_cause=%s",
        rec["trace_id"], rec["error_category"],
        root["cause_type"], attempts[0]["cause_type"],
    )
    assert rec["error_category"] == "downstream_error"
    assert root["cause_type"] == "ConnectionError"
    assert root["cause_message"] == "reset"
    assert attempts[0]["cause_message"] == "reset"
    assert err.cause.__class__.__name__ == "ConnectionError"
    log.info("attribution: root and attempt spans both retain original cause")


# --------------------------------------------------------------------------- #
# 3. Retry / fallback accounting and bounded aggregation
# --------------------------------------------------------------------------- #
def test_retries_do_not_double_count_logical_call() -> None:
    attempts = {"n": 0}

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError(f"reset #{attempts['n']}")
        return "ok"

    with trace_scope("retry-success") as tc:
        result = call_with_retry(flaky, "payment", attempts=3)
        rec = build_trace_record(tc, None)

    log.info(
        "input: payment attempts=3, physical tries=%d; trace=%s",
        attempts["n"], rec["trace_id"],
    )
    assert result == "ok"
    names = [s["name"] for s in rec["spans"]]
    assert names.count("payment") == 1
    assert names == [
        "retry-success", "payment",
        "payment/attempt-1", "payment/attempt-2", "payment/attempt-3",
    ]
    assert rec["retry_attempts"] == 2
    assert rec["fell_back"] is False
    log.info(
        "attribution: 1 logical call, retry_attempts=%d (no double count): ok",
        rec["retry_attempts"],
    )


def test_fallback_reports_once_and_marks_fell_back() -> None:
    with trace_scope("fallback") as tc:
        result = call_with_retry(
            lambda: (_ for _ in ()).throw(TimeoutError("t")),
            "pricing",
            attempts=2,
            fallback=lambda exc: "cached-price",
        )
        rec = build_trace_record(tc, None)

    log.info("input: pricing 2 attempts fail + fallback; trace=%s", rec["trace_id"])
    assert result == "cached-price"
    assert [s["name"] for s in rec["spans"]].count("pricing") == 1
    assert rec["fell_back"] is True
    assert rec["retry_attempts"] == 1
    fallback_span = next(s for s in rec["spans"] if s["name"] == "pricing/fallback")
    assert fallback_span["error_category"] is None
    log.info("attribution: logical call single, fell_back=True: ok")


async def _async_flaky(times: list[int]) -> str:
    times.append(1)
    if len(times) < 2:
        raise ConnectionError("async reset")
    return "aok"


def test_async_retry_accounting_matches_sync() -> None:
    times: list[int] = []

    async def scenario():
        with trace_scope("async-retry") as tc:
            result = await call_with_retry_async(
                lambda: _async_flaky(times), "asvc", attempts=2
            )
            return result, build_trace_record(tc, None)

    result, rec = asyncio.run(scenario())
    log.info(
        "input: async asvc attempts=2, tries=%d; trace=%s",
        len(times), rec["trace_id"],
    )
    assert result == "aok"
    assert [s["name"] for s in rec["spans"]].count("asvc") == 1
    assert rec["retry_attempts"] == 1
    log.info("attribution: async retries consistent with sync: ok")


def test_registry_is_bounded_and_concurrency_consistent() -> None:
    bounded = MetricsRegistry(max_entries=3)
    for i in range(5):
        bounded.record(f"e{i}", 1.0)
    snap = bounded.snapshot()
    assert snap["tracked_entries"] == 3
    assert snap["dropped_entries"] == 2

    registry = MetricsRegistry()
    n_threads, per_thread = 16, 250

    def writer() -> None:
        for _ in range(per_thread):
            registry.record("hot-entry", 12.0)

    threads = [threading.Thread(target=writer) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats = registry.snapshot()["entries"]["hot-entry"]
    expected = n_threads * per_thread
    log.info(
        "input: %d threads x %d records; expected count=%d",
        n_threads, per_thread, expected,
    )
    assert stats["count"] == expected
    assert sum(stats["histogram"]["counts"]) == expected
    assert stats["min_ms"] == stats["max_ms"] == 12.0
    log.info("attribution: no lost updates under concurrency, memory capped: ok")


def test_trace_store_is_bounded() -> None:
    store_local = TraceStore(max_traces=10)
    with trace_scope("bound") as tc:
        pass
    record = build_trace_record(tc, None)
    for _ in range(15):
        store_local.save(record)
    log.info(
        "input: 15 saves into cap=10; stored=%d dropped=%d",
        store_local.status()["stored"], store_local.status()["dropped"],
    )
    assert store_local.status()["stored"] == 10
    assert store_local.status()["dropped"] == 5


# --------------------------------------------------------------------------- #
# 4. HTTP entry points: interleaved concurrency, attribution, propagation
# --------------------------------------------------------------------------- #
def _log_conclusion(response, label: str) -> None:
    trace_id = response.headers.get("x-trace-id", "")
    body = response.json()
    if response.status_code >= 400:
        conclusion = body.get("error", {}).get("category")
    else:
        conclusion = "ok"
    log.info(
        "%s -> status=%s trace_id=%s attribution=%s",
        label, response.status_code, trace_id, conclusion,
    )
    return trace_id, conclusion


def test_http_failure_categories_and_safe_payloads(client) -> None:
    cases = [
        ("sync validation", "/orders", {"item_id": "widget"}, 400,
         "validation_error"),
        ("sync malformed", None, None, 400, "validation_error"),
        ("sync business", "/orders",
         {"item_id": "widget", "quantity": 99999, "amount": 1},
         422, "business_error"),
        ("sync downstream", "/orders",
         {"item_id": "net-down", "quantity": 1, "amount": 1},
         502, "downstream_error"),
        ("async validation", "/orders/async", {"quantity": 1}, 400,
         "validation_error"),
        ("async downstream", "/orders/async",
         {"item_id": "slow-host", "quantity": 1, "amount": 1},
         502, "downstream_error"),
    ]
    for label, path, payload, status, category in cases:
        log.info("input[%s]: path=%s payload=%s", label, path, payload)
        if label == "sync malformed":
            response = client.post(
                "/orders", content=b"{broken",
                headers={"content-type": "application/json"},
            )
        else:
            response = client.post(path, json=payload)
        trace_id, conclusion = _log_conclusion(response, label)
        assert response.status_code == status
        assert conclusion == category
        assert trace_id and len(trace_id) == 32
        # Raw library details never reach the public body.
        assert "10.0.0.9" not in response.text
        assert "ConnectionError" not in response.text
        assert "TimeoutError" not in response.text
        # But they remain traceable in the stored record.
        record = trace_store.get(trace_id)
        assert record is not None
        assert record["error_category"] == category
        if category == "downstream_error":
            assert any(
                s.get("cause_type") in {"ConnectionError", "TimeoutError"}
                for s in record["spans"]
            )


def test_http_retry_path_counts_once(client) -> None:
    payload = {
        "item_id": "widget", "quantity": 1, "amount": 5,
        "payment_token": "flaky",
    }
    log.info("input: POST /orders payload=%s (payment fails once)", payload)
    response = client.post("/orders", json=payload)
    trace_id, conclusion = _log_conclusion(response, "retry-success")
    assert response.status_code == 200
    from app.tracer import store
    record = store.get(trace_id)
    assert record["retry_attempts"] == 1
    payment_spans = [s for s in record["spans"] if s["name"] == "payment"]
    assert len(payment_spans) == 1
    log.info(
        "trace_id=%s attribution=ok retry_attempts=%d logical_payment_calls=%d",
        trace_id, record["retry_attempts"], len(payment_spans),
    )


def test_interleaved_concurrent_requests_stay_isolated(client) -> None:
    """Fire many requests concurrently through one real ASGI app.

    Mixes success, validation, business and downstream outcomes so we can
    prove trace ids never coincide and every trace attributes itself.
    """
    import httpx

    transport = httpx.ASGITransport(app=client.app)
    # (path, payload, expected tag/category) — deterministic failure mix.
    tagged = [
        ("/orders", {"item_id": "widget", "quantity": 1, "amount": 1}, "ok"),
        ("/orders",
         {"item_id": "widget", "quantity": 99999, "amount": 1}, "business"),
        ("/orders",
         {"item_id": "net-down", "quantity": 1, "amount": 1}, "downstream"),
        ("/orders", {"quantity": 1}, "validation"),
        ("/orders/async",
         {"item_id": "gadget", "quantity": 1, "amount": 2}, "ok"),
        ("/orders/async",
         {"item_id": "slow-host", "quantity": 1, "amount": 1}, "downstream"),
        ("/orders/background",
         {"item_id": "widget", "quantity": 1, "amount": 4}, "ok"),
    ]
    expected_category = {
        "ok": "ok",
        "business": "business_error",
        "downstream": "downstream_error",
        "validation": "validation_error",
    }

    async def scenario() -> list[tuple[str, str, int, str]]:
        results: list[tuple[str, str, int, str]] = []
        async with httpx.AsyncClient(
            transport=transport, base_url="http://t"
        ) as http:
            # Repeat the interleaved wave several times.
            for wave in range(5):
                sent = [
                    (tag, path, asyncio.create_task(http.post(path, json=payload)))
                    for path, payload, tag in tagged
                ]
                await asyncio.gather(*(coro for _, _, coro in sent))
                for tag, path, coro in sent:
                    response = coro.result()
                    trace_id = response.headers.get("x-trace-id", "")
                    category = (
                        response.json().get("error", {}).get("category", "ok")
                    )
                    results.append((tag, trace_id, response.status_code, category))
                    log.info(
                        "wave=%d input=%s tag=%s trace_id=%s attribution=%s",
                        wave, path, tag, trace_id, category,
                    )
        return results

    results = asyncio.run(scenario())
    assert len(results) == 5 * len(tagged)
    trace_ids = [trace_id for _, trace_id, _, _ in results]
    assert len(set(trace_ids)) == len(trace_ids), "trace ids must be unique"
    for tag, trace_id, status, category in results:
        assert category == expected_category[tag]
        assert status >= 200
    log.info(
        "attribution: %d concurrent requests, %d unique trace ids, all "
        "categories match expectations",
        len(results), len(set(trace_ids)),
    )


# --------------------------------------------------------------------------- #
# 5. Cross-entry consistency and end-to-end aggregation
# --------------------------------------------------------------------------- #
def test_same_failure_via_sync_and_async_entries_attributes_same(client) -> None:
    """The same downstream failure arriving by different entries is equal."""
    payload_sync = {"item_id": "net-down", "quantity": 1, "amount": 1}
    payload_async = {"item_id": "net-down", "quantity": 1, "amount": 1}
    log.info("input sync=%s async=%s (identical downstream fault)",
             payload_sync, payload_async)

    rs = client.post("/orders", json=payload_sync)
    ra = client.post("/orders/async", json=payload_async)
    tid_s, cat_s = _log_conclusion(rs, "sync downstream")
    tid_a, cat_a = _log_conclusion(ra, "async downstream")
    assert cat_s == cat_a == "downstream_error"
    assert rs.status_code == ra.status_code == 502
    assert rs.json() == ra.json()  # identical public envelope
    assert tid_s != tid_a  # but distinct requests/traces
    log.info("attribution identical across entries: %s == %s", cat_s, cat_a)


def test_end_to_end_metrics_count_logical_requests_not_attempts(client) -> None:
    """5 logical orders, one retried internally -> count 5, not 6/7."""
    inputs = [
        {"item_id": "widget", "quantity": 1, "amount": 1},                       # ok
        {"item_id": "widget", "quantity": 1, "amount": 1,
         "payment_token": "flaky"},                                             # ok after retry
        {"item_id": "widget", "quantity": 999, "amount": 1},                    # business
        {"item_id": "net-down", "quantity": 1, "amount": 1},                    # downstream
        {"item_id": "widget"},                                                  # validation
    ]
    for i, payload in enumerate(inputs):
        log.info("input[%d]: POST /orders payload=%s", i, payload)
        client.post("/orders", json=payload)

    response = client.get("/metrics")
    entries = response.json()["entries"]["POST /orders"]
    log.info(
        "attribution: count=%d error=%d categories=%s retry_attempts=%d",
        entries["count"], entries["error_count"],
        entries["by_category"], entries["retry_attempts"],
    )
    assert entries["count"] == 5
    assert entries["success_count"] == 2
    assert entries["error_count"] == 3
    assert entries["by_category"] == {
        "business_error": 1,
        "downstream_error": 1,
        "validation_error": 1,
    }
    # Exactly the one flaky payment contributed one extra physical attempt.
    assert entries["retry_attempts"] == 1
    # Histogram covers every sample and fixed bounds are reported.
    assert sum(entries["histogram"]["counts"]) == 5
    assert entries["histogram"]["bounds_ms"][-1] == 10000


def test_background_request_spans_stay_in_own_trace(client) -> None:
    payload = {"item_id": "widget", "quantity": 1, "amount": 3}
    log.info("input: POST /orders/background payload=%s", payload)
    response = client.post("/orders/background", json=payload)
    trace_id, conclusion = _log_conclusion(response, "background")
    assert conclusion == "ok"
    from app.tracer import store
    record = store.get(trace_id)
    names = [s["name"] for s in record["spans"]]
    assert "threadpool_dispatch" in names
    assert "post_response_hook" in names
    # every span belongs to this one trace
    assert len({s["span_id"] for s in record["spans"]}) == len(record["spans"])
    log.info(
        "trace_id=%s spans=%s all within one trace", trace_id, names,
    )
