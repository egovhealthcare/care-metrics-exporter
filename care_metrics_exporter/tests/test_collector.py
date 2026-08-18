"""Tests for the scrape-time Celery queue collector."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable

import pytest
import redis

from care_metrics_exporter.collector import (
    BROKER_UP,
    COLLECTION_ERRORS,
    LAST_SUCCESS,
    QUEUE_LENGTH,
    CeleryQueueCollector,
)
from care_metrics_exporter.config import Settings

BROKER_URL = "redis://127.0.0.1:6379/0"


class FakePipeline:
    def __init__(self, client: FakeRedis) -> None:
        self._client = client
        self._keys: list[bytes] = []

    def llen(self, key: bytes) -> FakePipeline:
        self._keys.append(key)
        return self

    def execute(self) -> list[object]:
        with self._client.execution_state_lock:
            self._client.active_executions += 1
            self._client.max_active_executions = max(
                self._client.max_active_executions,
                self._client.active_executions,
            )

        try:
            time.sleep(self._client.execution_delay)
            self._client.executed_batches.append(tuple(self._keys))
            if self._client.error is not None:
                raise self._client.error
            if self._client.raw_results is not None:
                return list(self._client.raw_results)
            return [self._client.lengths.get(key, 0) for key in self._keys]
        finally:
            with self._client.execution_state_lock:
                self._client.active_executions -= 1


class FakeRedis:
    """Minimal stand-in exposing only the pipeline surface the collector uses."""

    def __init__(
        self,
        lengths: dict[bytes, int] | None = None,
        error: Exception | None = None,
        raw_results: Iterable[object] | None = None,
        execution_delay: float = 0,
    ) -> None:
        self.lengths = lengths or {}
        self.error = error
        self.raw_results = raw_results
        self.executed_batches: list[tuple[bytes, ...]] = []
        self.pipeline_calls = 0
        self.execution_delay = execution_delay
        self.execution_state_lock = threading.Lock()
        self.active_executions = 0
        self.max_active_executions = 0

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        assert transaction is False, "collector must use a non-transactional pipeline"
        self.pipeline_calls += 1
        return FakePipeline(self)


def build_settings(**overrides: object) -> Settings:
    return Settings(broker_url=BROKER_URL, **overrides)


def scrape(collector: CeleryQueueCollector) -> dict[str, object]:
    """Collect once and index the emitted families by metric name."""
    return {family.name: family for family in collector.collect()}


def sample_value(families: dict[str, object], name: str) -> float:
    return families[name].samples[0].value


def test_empty_queue_reports_zero_depth() -> None:
    collector = CeleryQueueCollector(FakeRedis(), build_settings())

    families = scrape(collector)

    assert families[QUEUE_LENGTH].samples[0].value == 0.0
    assert sample_value(families, BROKER_UP) == 1.0


def test_depth_is_summed_across_priority_shards() -> None:
    client = FakeRedis(
        {
            b"celery": 4,
            b"celery\x06\x163": 3,
            b"celery\x06\x166": 2,
            b"celery\x06\x169": 1,
        },
    )
    collector = CeleryQueueCollector(client, build_settings())

    families = scrape(collector)

    assert families[QUEUE_LENGTH].samples[0].value == 10.0


def test_each_queue_is_reported_separately() -> None:
    client = FakeRedis({b"celery": 2, b"reports": 5})
    collector = CeleryQueueCollector(
        client,
        build_settings(queues=("celery", "reports"), priority_steps=(0,)),
    )

    families = scrape(collector)
    depths = {
        sample.labels["queue"]: sample.value
        for sample in families[QUEUE_LENGTH].samples
    }

    assert depths == {"celery": 2.0, "reports": 5.0}


def test_global_keyprefix_is_applied_to_reads() -> None:
    client = FakeRedis({b"care:celery": 7})
    collector = CeleryQueueCollector(
        client,
        build_settings(global_keyprefix="care:", priority_steps=(0,)),
    )

    families = scrape(collector)

    assert families[QUEUE_LENGTH].samples[0].value == 7.0
    assert client.executed_batches == [(b"care:celery",)]


def test_one_pipeline_execution_per_scrape() -> None:
    client = FakeRedis()
    collector = CeleryQueueCollector(client, build_settings())

    scrape(collector)
    scrape(collector)

    assert client.pipeline_calls == 2
    assert len(client.executed_batches) == 2


def test_describe_does_not_touch_redis() -> None:
    client = FakeRedis()
    collector = CeleryQueueCollector(client, build_settings())

    names = {family.name for family in collector.describe()}

    assert client.pipeline_calls == 0
    assert names == {
        QUEUE_LENGTH,
        BROKER_UP,
        "celery_queue_collection_duration_seconds",
        COLLECTION_ERRORS,
        LAST_SUCCESS,
    }


def test_the_error_counter_is_exposed_with_the_total_suffix() -> None:
    collector = CeleryQueueCollector(
        FakeRedis(error=redis.ConnectionError("refused")),
        build_settings(),
    )

    families = scrape(collector)
    sample_names = {sample.name for sample in families[COLLECTION_ERRORS].samples}

    assert "celery_queue_collection_errors_total" in sample_names


@pytest.mark.parametrize(
    "error",
    [
        redis.ConnectionError("refused"),
        redis.TimeoutError("timed out"),
        redis.AuthenticationError("bad password"),
        redis.ResponseError("WRONGTYPE Operation against a key"),
        OSError("socket gone"),
    ],
)
def test_broker_failures_omit_queue_depth_entirely(error: Exception) -> None:
    collector = CeleryQueueCollector(FakeRedis(error=error), build_settings())

    families = scrape(collector)

    assert QUEUE_LENGTH not in families
    assert sample_value(families, BROKER_UP) == 0.0
    assert sample_value(families, COLLECTION_ERRORS) == 1.0


@pytest.mark.parametrize("raw", [[None], ["12"], [-1], [True]])
def test_untrustworthy_replies_are_treated_as_failures(raw: list[object]) -> None:
    collector = CeleryQueueCollector(
        FakeRedis(raw_results=raw),
        build_settings(priority_steps=(0,)),
    )

    families = scrape(collector)

    assert QUEUE_LENGTH not in families
    assert sample_value(families, BROKER_UP) == 0.0


def test_partial_replies_are_rejected_rather_than_mismapped() -> None:
    collector = CeleryQueueCollector(
        FakeRedis(raw_results=[1]),
        build_settings(queues=("celery", "reports"), priority_steps=(0,)),
    )

    families = scrape(collector)

    assert QUEUE_LENGTH not in families
    assert sample_value(families, BROKER_UP) == 0.0


def test_last_success_is_absent_until_the_first_success() -> None:
    collector = CeleryQueueCollector(
        FakeRedis(error=redis.ConnectionError("refused")),
        build_settings(),
    )

    families = scrape(collector)

    assert LAST_SUCCESS not in families


def test_last_success_survives_a_later_failure() -> None:
    client = FakeRedis()
    collector = CeleryQueueCollector(client, build_settings())

    first = scrape(collector)
    recorded = sample_value(first, LAST_SUCCESS)

    client.error = redis.ConnectionError("refused")
    second = scrape(collector)

    assert QUEUE_LENGTH not in second
    assert sample_value(second, LAST_SUCCESS) == recorded


def test_recovery_reports_fresh_values_without_replaying_stale_ones() -> None:
    client = FakeRedis({b"celery": 3}, error=redis.ConnectionError("refused"))
    collector = CeleryQueueCollector(client, build_settings(priority_steps=(0,)))

    failed = scrape(collector)
    client.error = None
    recovered = scrape(collector)

    assert QUEUE_LENGTH not in failed
    assert recovered[QUEUE_LENGTH].samples[0].value == 3.0
    assert sample_value(recovered, BROKER_UP) == 1.0
    assert sample_value(recovered, COLLECTION_ERRORS) == 1.0


def test_error_counter_accumulates_monotonically() -> None:
    collector = CeleryQueueCollector(
        FakeRedis(error=redis.ConnectionError("refused")),
        build_settings(),
    )

    counts = [sample_value(scrape(collector), COLLECTION_ERRORS) for _ in range(3)]

    assert counts == [1.0, 2.0, 3.0]


def test_a_busy_collection_fails_fast_instead_of_queueing() -> None:
    collector = CeleryQueueCollector(FakeRedis(), build_settings())
    collector._collection_lock.acquire()

    try:
        collector._settings = build_settings(collection_lock_timeout_seconds=0.01)
        families = scrape(collector)
    finally:
        collector._collection_lock.release()

    assert QUEUE_LENGTH not in families
    assert sample_value(families, BROKER_UP) == 0.0
    assert sample_value(families, COLLECTION_ERRORS) == 1.0


def test_concurrent_scrapes_are_serialised() -> None:
    client = FakeRedis({b"celery": 1}, execution_delay=0.03)
    collector = CeleryQueueCollector(
        client,
        build_settings(priority_steps=(0,), collection_lock_timeout_seconds=5),
    )
    results: list[dict[str, object]] = []
    barrier = threading.Barrier(4)

    def worker() -> None:
        barrier.wait()
        results.append(scrape(collector))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 4
    assert all(QUEUE_LENGTH in families for families in results)
    assert len(client.executed_batches) == 4
    assert client.max_active_executions == 1
