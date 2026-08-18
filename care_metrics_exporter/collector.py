"""Prometheus collector that reads Celery queue depth from the Redis broker.

The collector runs once per scrape. It never caches a queue length: if a scrape
cannot read the broker it omits the queue-depth metric entirely rather than
reporting a misleading zero.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator

import redis
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from care_metrics_exporter.config import Settings
from care_metrics_exporter.redis_keys import build_queue_key_map

logger = logging.getLogger(__name__)

QUEUE_LENGTH = "celery_queue_length"
BROKER_UP = "celery_broker_up"
COLLECTION_DURATION = "celery_queue_collection_duration_seconds"
# prometheus_client owns the counter suffix: this family is exposed as
# ``celery_queue_collection_errors_total`` in the text exposition.
COLLECTION_ERRORS = "celery_queue_collection_errors"
LAST_SUCCESS = "celery_queue_last_success_timestamp_seconds"

QUEUE_LENGTH_HELP = (
    "Messages waiting in the Redis broker for a logical Celery queue, summed "
    "across all priority shards. Excludes tasks already reserved by a worker."
)
BROKER_UP_HELP = (
    "1 if the most recent scrape read every queue shard successfully, 0 otherwise."
)
COLLECTION_DURATION_HELP = (
    "Wall-clock seconds spent on the most recent broker collection attempt."
)
COLLECTION_ERRORS_HELP = (
    "Total broker collection attempts that failed since the exporter started."
)
LAST_SUCCESS_HELP = (
    "Unix timestamp of the most recent fully successful broker collection."
)


class CollectionError(RuntimeError):
    """Raised when the broker responds but the reply cannot be trusted."""


class CeleryQueueCollector(Collector):
    """Collects Celery ready-queue depth from Redis on every scrape."""

    def __init__(self, client: redis.Redis, settings: Settings) -> None:
        self._client = client
        self._settings = settings
        self._queue_keys = build_queue_key_map(
            settings.queues,
            settings.priority_steps,
            settings.global_keyprefix,
        )
        self._collection_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._error_count = 0
        self._last_success: float | None = None

    def describe(self) -> Iterator[Metric]:
        """Describe the metric contract without contacting Redis.

        Defining this stops ``CollectorRegistry.register`` from calling
        ``collect``, which would otherwise make the broker a startup dependency.
        """
        yield GaugeMetricFamily(QUEUE_LENGTH, QUEUE_LENGTH_HELP, labels=["queue"])
        yield GaugeMetricFamily(BROKER_UP, BROKER_UP_HELP)
        yield GaugeMetricFamily(COLLECTION_DURATION, COLLECTION_DURATION_HELP)
        yield CounterMetricFamily(COLLECTION_ERRORS, COLLECTION_ERRORS_HELP)
        yield GaugeMetricFamily(LAST_SUCCESS, LAST_SUCCESS_HELP)

    def collect(self) -> Iterator[Metric]:
        """Read every configured queue and emit the metric contract."""
        started = time.perf_counter()
        lengths, failure_reason = self._attempt_collection()
        duration = time.perf_counter() - started

        error_count, last_success = self._record_outcome(failure_reason)

        if failure_reason is None:
            logger.debug(
                "queue collection succeeded queues=%d duration_seconds=%.4f",
                len(lengths or {}),
                duration,
            )
        else:
            logger.warning(
                "queue collection failed reason=%s duration_seconds=%.4f",
                failure_reason,
                duration,
            )

        if lengths is not None:
            queue_length = GaugeMetricFamily(
                QUEUE_LENGTH,
                QUEUE_LENGTH_HELP,
                labels=["queue"],
            )
            for queue in self._settings.queues:
                queue_length.add_metric([queue], lengths[queue])
            yield queue_length

        yield GaugeMetricFamily(
            BROKER_UP,
            BROKER_UP_HELP,
            value=1.0 if failure_reason is None else 0.0,
        )
        yield GaugeMetricFamily(
            COLLECTION_DURATION, COLLECTION_DURATION_HELP, value=duration
        )
        yield CounterMetricFamily(
            COLLECTION_ERRORS, COLLECTION_ERRORS_HELP, value=error_count
        )

        if last_success is not None:
            yield GaugeMetricFamily(LAST_SUCCESS, LAST_SUCCESS_HELP, value=last_success)

    def _attempt_collection(self) -> tuple[dict[str, int] | None, str | None]:
        """Run one single-flight collection, returning lengths or a failure tag."""
        acquired = self._collection_lock.acquire(
            timeout=self._settings.collection_lock_timeout_seconds,
        )
        if not acquired:
            # Another scrape is already blocked on a slow broker. Failing fast
            # keeps request threads from piling up behind it.
            return None, "collection_busy"

        try:
            return self._read_queue_lengths(), None
        except (redis.RedisError, CollectionError, OSError) as error:
            return None, type(error).__name__
        finally:
            self._collection_lock.release()

    def _read_queue_lengths(self) -> dict[str, int]:
        """Read every shard in one pipeline and fold shards into logical queues."""
        ordered_keys = [
            key for queue in self._settings.queues for key in self._queue_keys[queue]
        ]

        pipeline = self._client.pipeline(transaction=False)
        for key in ordered_keys:
            pipeline.llen(key)
        results = pipeline.execute()

        if len(results) != len(ordered_keys):
            message = "broker returned an unexpected number of queue lengths"
            raise CollectionError(message)

        for value in results:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                message = "broker returned a non-numeric queue length"
                raise CollectionError(message)

        lengths: dict[str, int] = {}
        offset = 0
        for queue in self._settings.queues:
            shard_count = len(self._queue_keys[queue])
            lengths[queue] = sum(results[offset : offset + shard_count])
            offset += shard_count

        return lengths

    def _record_outcome(self, failure_reason: str | None) -> tuple[int, float | None]:
        """Update process-lifetime telemetry and return a consistent snapshot."""
        with self._state_lock:
            if failure_reason is None:
                self._last_success = time.time()
            else:
                self._error_count += 1
            return self._error_count, self._last_success
