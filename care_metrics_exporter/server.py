"""HTTP server exposing the exporter's Prometheus and health endpoints."""

from __future__ import annotations

import logging
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import FrameType
from urllib.parse import urlsplit

import redis
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

from care_metrics_exporter.collector import CeleryQueueCollector
from care_metrics_exporter.config import ConfigurationError, Settings

logger = logging.getLogger(__name__)

PLAIN_TEXT = "text/plain; charset=utf-8"
METRICS_PATH = "/metrics"
HEALTH_PATHS = frozenset({"/healthz", "/health"})


def create_redis_client(settings: Settings) -> redis.Redis:
    """Create the process-lifetime client without exposing its URL on failure."""
    try:
        return redis.Redis.from_url(
            settings.broker_url,
            socket_connect_timeout=settings.redis_socket_connect_timeout_seconds,
            socket_timeout=settings.redis_socket_timeout_seconds,
        )
    except (TypeError, ValueError):
        message = "CELERY_BROKER_URL contains options Redis cannot parse"
        raise ConfigurationError(message) from None


def build_handler(
    registry: CollectorRegistry,
    ready: threading.Event,
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to one registry and readiness flag."""

    class ExporterHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "care-metrics-exporter"
        sys_version = ""

        def do_GET(self) -> None:
            self._dispatch(include_body=True)

        def do_HEAD(self) -> None:
            self._dispatch(include_body=False)

        def _dispatch(self, include_body: bool) -> None:
            path = urlsplit(self.path).path

            if path == METRICS_PATH:
                self._serve_metrics(include_body)
            elif path in HEALTH_PATHS:
                self._serve_health(include_body)
            else:
                self._reply(404, PLAIN_TEXT, b"not found\n", include_body)

        def _serve_metrics(self, include_body: bool) -> None:
            try:
                payload = generate_latest(registry)
            except Exception:
                # A collector bug must not leak a traceback to the scraper, but
                # it must also not masquerade as a successful scrape.
                logger.exception("failed to render the metrics exposition")
                self._reply(500, PLAIN_TEXT, b"collection error\n", include_body)
                return

            self._reply(200, CONTENT_TYPE_LATEST, payload, include_body)

        def _serve_health(self, include_body: bool) -> None:
            """Report process health only; this deliberately never touches Redis.

            A broker outage is reported through ``celery_broker_up``. Failing
            the probe instead would restart the pod and delete the very target
            that reports the outage.
            """
            if ready.is_set():
                self._reply(200, PLAIN_TEXT, b"ok\n", include_body)
            else:
                self._reply(503, PLAIN_TEXT, b"starting\n", include_body)

        def _reply(
            self,
            status: int,
            content_type: str,
            body: bytes,
            include_body: bool,
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if include_body:
                self.wfile.write(body)

        def log_message(self, fmt: str, *args: object) -> None:
            logger.debug("http %s %s", self.address_string(), fmt % args)

    return ExporterHandler


class ExporterServer:
    """Owns the Redis client, registry, and HTTP server for the exporter."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ready = threading.Event()
        self._registry = CollectorRegistry()

        # One client, and therefore one connection pool, for the process
        # lifetime. Building a client per scrape would leak sockets.
        self._client = create_redis_client(settings)
        self._registry.register(CeleryQueueCollector(self._client, settings))

        self._httpd = ThreadingHTTPServer(
            (settings.host, settings.port),
            build_handler(self._registry, self._ready),
        )
        self._httpd.daemon_threads = True

    @property
    def registry(self) -> CollectorRegistry:
        return self._registry

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def serve_forever(self) -> None:
        self._ready.set()
        logger.info(
            "exporter listening host=%s port=%d queues=%s",
            self._settings.host,
            self.port,
            ",".join(self._settings.queues),
        )
        try:
            self._httpd.serve_forever()
        finally:
            self._ready.clear()

    def shutdown(self) -> None:
        self._ready.clear()
        self._httpd.shutdown()

    def close(self) -> None:
        """Release the listening socket and the Redis connection pool."""
        self._httpd.server_close()
        self._client.close()
        self._client.connection_pool.disconnect()


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )


def run(settings: Settings) -> None:
    """Serve until a termination signal arrives, then shut down cleanly."""
    server = ExporterServer(settings)

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        logger.info("received signal %d, shutting down", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        server.serve_forever()
    finally:
        server.close()
        logger.info("exporter stopped")
