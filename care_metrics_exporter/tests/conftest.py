"""Shared fixtures for the exporter test suite."""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest
import redis

from care_metrics_exporter.config import Settings

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
#: Database used only by integration tests. It is flushed between tests, so it
#: must not be a database any real application writes to.
REDIS_TEST_DB = 15
REDIS_TEST_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_TEST_DB}"


def redis_is_available() -> bool:
    """Return True when a Redis server accepts connections on localhost."""
    try:
        with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=0.5):
            return True
    except OSError:
        return False


@pytest.fixture
def settings() -> Settings:
    """Settings pointing at the local integration database."""
    return Settings.from_env({"CELERY_BROKER_URL": REDIS_TEST_URL})


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    """A live Redis client against database 15, flushed before and after."""
    if not redis_is_available():
        pytest.skip(f"no Redis listening on {REDIS_HOST}:{REDIS_PORT}")

    client = redis.Redis.from_url(REDIS_TEST_URL, socket_connect_timeout=2)
    client.flushdb()
    try:
        yield client
    finally:
        client.flushdb()
        client.close()
        client.connection_pool.disconnect()
