"""Integration tests against a real Redis on localhost:6379.

These skip automatically when no Redis is listening. Start one with
``make redis-up`` (or ``docker compose up -d redis``) to run them.
"""

from __future__ import annotations

import threading
import urllib.request
from collections.abc import Iterator

import pytest
import redis

from care_metrics_exporter.collector import (
    BROKER_UP,
    LAST_SUCCESS,
    QUEUE_LENGTH,
    CeleryQueueCollector,
)
from care_metrics_exporter.config import Settings
from care_metrics_exporter.server import ExporterServer
from care_metrics_exporter.tests.conftest import REDIS_TEST_URL

pytestmark = pytest.mark.integration


def scrape(collector: CeleryQueueCollector) -> dict[str, object]:
    return {family.name: family for family in collector.collect()}


def depth(families: dict[str, object]) -> float:
    return families[QUEUE_LENGTH].samples[0].value


def test_an_empty_broker_reports_zero_depth(
    redis_client: redis.Redis,
    settings: Settings,
) -> None:
    families = scrape(CeleryQueueCollector(redis_client, settings))

    assert depth(families) == 0.0
    assert families[BROKER_UP].samples[0].value == 1.0
    assert LAST_SUCCESS in families


def test_messages_pushed_to_the_default_shard_are_counted(
    redis_client: redis.Redis,
    settings: Settings,
) -> None:
    redis_client.lpush("celery", "task-a", "task-b", "task-c")

    families = scrape(CeleryQueueCollector(redis_client, settings))

    assert depth(families) == 3.0


def test_priority_shards_are_summed_into_one_logical_queue(
    redis_client: redis.Redis,
    settings: Settings,
) -> None:
    redis_client.lpush("celery", "default")
    redis_client.lpush(b"celery\x06\x163", "priority-3")
    redis_client.lpush(b"celery\x06\x166", "priority-6a", "priority-6b")
    redis_client.lpush(b"celery\x06\x169", "priority-9")

    families = scrape(CeleryQueueCollector(redis_client, settings))

    assert depth(families) == 5.0


def test_a_wrong_type_key_is_reported_as_a_failure_not_a_zero(
    redis_client: redis.Redis,
    settings: Settings,
) -> None:
    redis_client.set("celery", "not-a-list")

    families = scrape(CeleryQueueCollector(redis_client, settings))

    assert QUEUE_LENGTH not in families
    assert families[BROKER_UP].samples[0].value == 0.0


def test_unconfigured_queues_are_never_read(
    redis_client: redis.Redis,
    settings: Settings,
) -> None:
    redis_client.lpush("some-other-queue", "ignored")

    families = scrape(CeleryQueueCollector(redis_client, settings))

    assert depth(families) == 0.0


@pytest.fixture
def live_server(redis_client: redis.Redis) -> Iterator[ExporterServer]:
    settings = Settings(broker_url=REDIS_TEST_URL, host="127.0.0.1", port=0)
    exporter = ExporterServer(settings)
    thread = threading.Thread(target=exporter.serve_forever, daemon=True)
    thread.start()
    try:
        yield exporter
    finally:
        exporter.shutdown()
        thread.join(timeout=5)
        exporter.close()


def test_end_to_end_scrape_reports_real_queue_depth(
    redis_client: redis.Redis,
    live_server: ExporterServer,
) -> None:
    redis_client.lpush("celery", "task-a", "task-b")

    url = f"http://127.0.0.1:{live_server.port}/metrics"
    with urllib.request.urlopen(url, timeout=5) as response:
        body = response.read().decode()

    assert 'celery_queue_length{queue="celery"} 2.0' in body
    assert "celery_broker_up 1.0" in body
