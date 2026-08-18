"""Environment-backed exporter configuration."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

DEFAULT_QUEUES = ("celery",)
DEFAULT_PRIORITY_STEPS = (0, 3, 6, 9)
SUPPORTED_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"})
SUPPORTED_REDIS_SCHEMES = frozenset({"redis", "rediss"})

MIN_PORT = 1
MAX_PORT = 65535

# Celery/Kombu only recognises priority steps within this range.
MIN_PRIORITY_STEP = 0
MAX_PRIORITY_STEP = 9

# Ordinals below SPACE, plus DELETE, are the control characters that would
# corrupt a Redis key or a Prometheus label if they reached them.
FIRST_PRINTABLE_ORDINAL = 32
DELETE_ORDINAL = 127


def _has_control_characters(value: str) -> bool:
    return any(
        ord(character) < FIRST_PRINTABLE_ORDINAL or ord(character) == DELETE_ORDINAL
        for character in value
    )


class ConfigurationError(ValueError):
    """Raised when exporter configuration is invalid."""


@dataclass(frozen=True)
class Settings:
    """Validated exporter settings."""

    broker_url: str = field(repr=False)
    queues: tuple[str, ...] = DEFAULT_QUEUES
    priority_steps: tuple[int, ...] = DEFAULT_PRIORITY_STEPS
    global_keyprefix: str = ""
    redis_socket_connect_timeout_seconds: float = 2.0
    redis_socket_timeout_seconds: float = 5.0
    collection_lock_timeout_seconds: float = 1.0
    # Binding all interfaces is required inside a container; the pod is only
    # reachable through its ClusterIP Service.
    host: str = "0.0.0.0"  # noqa: S104
    port: int = 8000
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Load and validate settings from an environment mapping."""
        values = os.environ if env is None else env
        broker_url = values.get("CELERY_BROKER_URL") or values.get("REDIS_URL")

        if not broker_url:
            raise ConfigurationError(
                "CELERY_BROKER_URL is required (REDIS_URL is accepted as a fallback)"
            )

        _validate_broker_url(broker_url)

        return cls(
            broker_url=broker_url,
            queues=_parse_queues(values.get("CELERY_QUEUES", "celery")),
            priority_steps=_parse_priority_steps(
                values.get("CELERY_REDIS_PRIORITY_STEPS", "0,3,6,9")
            ),
            global_keyprefix=values.get("CELERY_REDIS_GLOBAL_KEYPREFIX", ""),
            redis_socket_connect_timeout_seconds=_parse_bounded_float(
                values.get("REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS", "2"),
                "REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS",
                maximum=60,
            ),
            redis_socket_timeout_seconds=_parse_bounded_float(
                values.get("REDIS_SOCKET_TIMEOUT_SECONDS", "5"),
                "REDIS_SOCKET_TIMEOUT_SECONDS",
                maximum=60,
            ),
            collection_lock_timeout_seconds=_parse_bounded_float(
                values.get("COLLECTION_LOCK_TIMEOUT_SECONDS", "1"),
                "COLLECTION_LOCK_TIMEOUT_SECONDS",
                maximum=30,
            ),
            host=_parse_host(values.get("EXPORTER_HOST", "0.0.0.0")),  # noqa: S104
            port=_parse_port(values.get("EXPORTER_PORT", "8000")),
            log_level=_parse_log_level(values.get("LOG_LEVEL", "INFO")),
        )


def _validate_broker_url(value: str) -> None:
    """Validate the broker URL without ever echoing it back to the caller.

    Error messages intentionally omit the value because it can embed a password
    in environments where CARE builds ``rediss://:TOKEN@host:port/db`` URLs.
    """
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ConfigurationError("CELERY_BROKER_URL is not a valid Redis URL") from None

    if parsed.scheme.lower() not in SUPPORTED_REDIS_SCHEMES:
        raise ConfigurationError(
            "CELERY_BROKER_URL must use the redis:// or rediss:// scheme"
        )

    hostname = parsed.hostname
    if not hostname:
        raise ConfigurationError("CELERY_BROKER_URL must include a hostname")
    if any(character.isspace() for character in hostname) or _has_control_characters(
        hostname
    ):
        raise ConfigurationError("CELERY_BROKER_URL hostname is invalid")
    if port is not None and not MIN_PORT <= port <= MAX_PORT:
        raise ConfigurationError("CELERY_BROKER_URL contains an invalid port")

    _validate_broker_database(parsed.path)


def _validate_broker_database(path: str) -> None:
    """Reject database paths that redis-py would silently ignore."""
    if path in ("", "/"):
        return

    database = path.removeprefix("/")
    if not database.isdigit():
        raise ConfigurationError(
            "CELERY_BROKER_URL must end with a non-negative database number"
        )


def _parse_queues(value: str) -> tuple[str, ...]:
    raw_queues = value.split(",")
    queues: list[str] = []

    for raw_queue in raw_queues:
        if _has_control_characters(raw_queue):
            raise ConfigurationError(
                "CELERY_QUEUES names must not contain control characters"
            )
        queue = raw_queue.strip()
        if not queue:
            raise ConfigurationError("CELERY_QUEUES must not contain empty names")
        if queue not in queues:
            queues.append(queue)

    return tuple(queues)


def _parse_priority_steps(value: str) -> tuple[int, ...]:
    try:
        steps = tuple(int(item.strip()) for item in value.split(","))
    except ValueError as error:
        raise ConfigurationError(
            "CELERY_REDIS_PRIORITY_STEPS must contain integers"
        ) from error

    if not steps or any(
        step < MIN_PRIORITY_STEP or step > MAX_PRIORITY_STEP for step in steps
    ):
        raise ConfigurationError(
            "CELERY_REDIS_PRIORITY_STEPS values must be between 0 and 9"
        )
    if steps[0] != MIN_PRIORITY_STEP:
        raise ConfigurationError("CELERY_REDIS_PRIORITY_STEPS must start with 0")
    if tuple(sorted(set(steps))) != steps:
        raise ConfigurationError(
            "CELERY_REDIS_PRIORITY_STEPS must be unique and strictly increasing"
        )

    return steps


def _parse_bounded_float(value: str, name: str, maximum: float) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        message = f"{name} must be a number"
        raise ConfigurationError(message) from error

    if not math.isfinite(parsed) or parsed <= 0 or parsed > maximum:
        message = f"{name} must be greater than 0 and at most {maximum}"
        raise ConfigurationError(message)

    return parsed


def _parse_host(value: str) -> str:
    host = value.strip()
    if not host:
        raise ConfigurationError("EXPORTER_HOST must not be empty")
    if _has_control_characters(host):
        raise ConfigurationError("EXPORTER_HOST must not contain control characters")
    return host


def _parse_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise ConfigurationError("EXPORTER_PORT must be an integer") from error

    if not MIN_PORT <= port <= MAX_PORT:
        message = f"EXPORTER_PORT must be between {MIN_PORT} and {MAX_PORT}"
        raise ConfigurationError(message)
    return port


def _parse_log_level(value: str) -> str:
    log_level = value.strip().upper()
    if log_level not in SUPPORTED_LOG_LEVELS:
        message = f"LOG_LEVEL must be one of {', '.join(sorted(SUPPORTED_LOG_LEVELS))}"
        raise ConfigurationError(message)
    return log_level
