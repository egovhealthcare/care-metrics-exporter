"""HTTP contract tests for the exporter server.

These run against a broker address where nothing is listening, which is the
most important production scenario: the exporter must stay healthy and keep
reporting that the broker is down.
"""

from __future__ import annotations

import threading
import urllib.error
import urllib.request
from collections.abc import Iterator

import pytest
import redis
from prometheus_client.parser import text_string_to_metric_families

from care_metrics_exporter.collector import BROKER_UP, QUEUE_LENGTH
from care_metrics_exporter.config import ConfigurationError, Settings
from care_metrics_exporter.server import ExporterServer, create_redis_client

# A port deliberately left empty so every broker read fails fast.
UNREACHABLE_BROKER = "redis://:hunter2@127.0.0.1:6390/0"


def test_redis_client_parse_errors_never_expose_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_url(*_args: object, **_kwargs: object) -> None:
        message = f"cannot parse {UNREACHABLE_BROKER}"
        raise ValueError(message)

    monkeypatch.setattr(redis.Redis, "from_url", reject_url)

    with pytest.raises(ConfigurationError) as captured:
        create_redis_client(Settings(broker_url=UNREACHABLE_BROKER))

    message = str(captured.value)
    assert "hunter2" not in message
    assert UNREACHABLE_BROKER not in message
    assert captured.value.__cause__ is None


@pytest.fixture
def server() -> Iterator[ExporterServer]:
    settings = Settings(
        broker_url=UNREACHABLE_BROKER,
        host="127.0.0.1",
        port=0,
        redis_socket_connect_timeout_seconds=0.2,
        redis_socket_timeout_seconds=0.2,
    )
    exporter = ExporterServer(settings)
    thread = threading.Thread(target=exporter.serve_forever, daemon=True)
    thread.start()
    try:
        yield exporter
    finally:
        exporter.shutdown()
        thread.join(timeout=5)
        exporter.close()


def fetch(
    server: ExporterServer, path: str, method: str = "GET"
) -> tuple[int, str, str]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.port}{path}",
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:  # noqa: S310
            return (
                response.status,
                response.headers.get("Content-Type", ""),
                response.read().decode(),
            )
    except urllib.error.HTTPError as error:
        return error.code, error.headers.get("Content-Type", ""), error.read().decode()


def test_health_stays_up_while_the_broker_is_unreachable(
    server: ExporterServer,
) -> None:
    status, content_type, body = fetch(server, "/healthz")

    assert status == 200
    assert content_type.startswith("text/plain")
    assert body.strip() == "ok"


def test_metrics_returns_200_so_the_outage_is_still_scraped(
    server: ExporterServer,
) -> None:
    status, content_type, body = fetch(server, "/metrics")

    assert status == 200
    assert "text/plain" in content_type
    assert body


def test_metrics_report_the_broker_as_down_without_a_zero_depth(
    server: ExporterServer,
) -> None:
    _, _, body = fetch(server, "/metrics")
    families = {family.name: family for family in text_string_to_metric_families(body)}

    assert families[BROKER_UP].samples[0].value == 0.0
    assert QUEUE_LENGTH not in families


def test_metrics_never_leak_the_broker_url_or_credentials(
    server: ExporterServer,
) -> None:
    _, _, body = fetch(server, "/metrics")

    assert "hunter2" not in body
    assert UNREACHABLE_BROKER not in body
    assert "127.0.0.1" not in body


def test_unknown_paths_return_404(server: ExporterServer) -> None:
    status, _, body = fetch(server, "/admin")

    assert status == 404
    assert "hunter2" not in body


def test_head_requests_are_supported_for_probes(server: ExporterServer) -> None:
    status, _, body = fetch(server, "/healthz", method="HEAD")

    assert status == 200
    assert body == ""


def test_readiness_is_cleared_after_shutdown() -> None:
    settings = Settings(broker_url=UNREACHABLE_BROKER, host="127.0.0.1", port=0)
    exporter = ExporterServer(settings)
    thread = threading.Thread(target=exporter.serve_forever, daemon=True)
    thread.start()

    assert fetch(exporter, "/healthz")[0] == 200

    exporter.shutdown()
    thread.join(timeout=5)
    exporter.close()

    assert not thread.is_alive()
