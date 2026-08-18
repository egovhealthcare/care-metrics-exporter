import pytest

from care_metrics_exporter.config import (
    DEFAULT_PRIORITY_STEPS,
    DEFAULT_QUEUES,
    ConfigurationError,
    Settings,
)


def test_defaults() -> None:
    settings = Settings.from_env(
        {"CELERY_BROKER_URL": "redis://redis.care.svc.cluster.local:6379/0"}
    )

    assert settings.broker_url == "redis://redis.care.svc.cluster.local:6379/0"
    assert settings.queues == DEFAULT_QUEUES
    assert settings.priority_steps == DEFAULT_PRIORITY_STEPS
    assert settings.global_keyprefix == ""
    assert settings.redis_socket_connect_timeout_seconds == 2
    assert settings.redis_socket_timeout_seconds == 5
    assert settings.collection_lock_timeout_seconds == 1
    assert settings.host == "0.0.0.0"  # noqa: S104
    assert settings.port == 8000
    assert settings.log_level == "INFO"


def test_broker_url_takes_precedence_over_redis_fallback() -> None:
    settings = Settings.from_env(
        {
            "CELERY_BROKER_URL": "redis://broker:6379/1",
            "REDIS_URL": "redis://cache:6379/0",
        }
    )

    assert settings.broker_url == "redis://broker:6379/1"


def test_redis_url_is_supported_as_local_fallback() -> None:
    settings = Settings.from_env({"REDIS_URL": "rediss://:secret@redis:6380/2"})

    assert settings.broker_url == "rediss://:secret@redis:6380/2"


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"CELERY_BROKER_URL": "http://:hunter2@redis:6379/0"},
        {"CELERY_BROKER_URL": "redis://:hunter2@/0"},
        {"CELERY_BROKER_URL": "redis://:hunter2@redis:99999/0"},
        {"CELERY_BROKER_URL": "redis://:hunter2@redis:6379/not-a-db"},
        {"CELERY_BROKER_URL": "rediss://:hunter2@redis:6379/0/extra"},
        {"REDIS_URL": "amqp://:hunter2@broker:5672//"},
    ],
)
def test_invalid_broker_configuration_never_echoes_the_url(
    env: dict[str, str],
) -> None:
    with pytest.raises(ConfigurationError) as captured:
        Settings.from_env(env)

    message = str(captured.value)
    assert "hunter2" not in message
    for value in env.values():
        assert value not in message


def test_repr_does_not_expose_broker_credentials() -> None:
    settings = Settings.from_env(
        {"CELERY_BROKER_URL": "rediss://:hunter2@redis.care.svc.cluster.local:6380/2"}
    )

    rendered = repr(settings)

    assert "hunter2" not in rendered
    assert settings.broker_url not in rendered


def test_urls_without_explicit_port_or_database_are_accepted() -> None:
    settings = Settings.from_env({"CELERY_BROKER_URL": "redis://redis.care"})

    assert settings.broker_url == "redis://redis.care"


def test_query_parameters_are_preserved_for_redis_py() -> None:
    url = "rediss://:hunter2@redis:6380/2?ssl_cert_reqs=none"

    settings = Settings.from_env({"CELERY_BROKER_URL": url})

    assert settings.broker_url == url


def test_queue_names_are_trimmed_and_deduplicated() -> None:
    settings = Settings.from_env(
        {
            "CELERY_BROKER_URL": "redis://redis:6379/0",
            "CELERY_QUEUES": " celery,notifications,celery ",
        }
    )

    assert settings.queues == ("celery", "notifications")


@pytest.mark.parametrize("queues", ["", "celery,,notifications", "celery,\nother"])
def test_invalid_queue_names_are_rejected(queues: str) -> None:
    with pytest.raises(ConfigurationError):
        Settings.from_env(
            {
                "CELERY_BROKER_URL": "redis://redis:6379/0",
                "CELERY_QUEUES": queues,
            }
        )


def test_custom_priority_steps_and_prefix_are_loaded() -> None:
    settings = Settings.from_env(
        {
            "CELERY_BROKER_URL": "redis://redis:6379/0",
            "CELERY_REDIS_PRIORITY_STEPS": "0,5,9",
            "CELERY_REDIS_GLOBAL_KEYPREFIX": "care:",
        }
    )

    assert settings.priority_steps == (0, 5, 9)
    assert settings.global_keyprefix == "care:"


@pytest.mark.parametrize("steps", ["", "1,3", "0,3,3", "0,9,3", "0,10", "0,x"])
def test_invalid_priority_steps_are_rejected(steps: str) -> None:
    with pytest.raises(ConfigurationError):
        Settings.from_env(
            {
                "CELERY_BROKER_URL": "redis://redis:6379/0",
                "CELERY_REDIS_PRIORITY_STEPS": steps,
            }
        )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS", "0"),
        ("REDIS_SOCKET_TIMEOUT_SECONDS", "nan"),
        ("COLLECTION_LOCK_TIMEOUT_SECONDS", "31"),
        ("EXPORTER_PORT", "0"),
        ("EXPORTER_PORT", "not-a-port"),
        ("EXPORTER_HOST", " "),
        ("LOG_LEVEL", "TRACE"),
    ],
)
def test_invalid_scalar_settings_are_rejected(name: str, value: str) -> None:
    with pytest.raises(ConfigurationError):
        Settings.from_env(
            {
                "CELERY_BROKER_URL": "redis://redis:6379/0",
                name: value,
            }
        )


def test_scalar_settings_are_normalized() -> None:
    settings = Settings.from_env(
        {
            "CELERY_BROKER_URL": "redis://redis:6379/0",
            "REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS": "1.5",
            "REDIS_SOCKET_TIMEOUT_SECONDS": "4",
            "COLLECTION_LOCK_TIMEOUT_SECONDS": "0.5",
            "EXPORTER_HOST": "127.0.0.1",
            "EXPORTER_PORT": "9000",
            "LOG_LEVEL": "debug",
        }
    )

    assert settings.redis_socket_connect_timeout_seconds == 1.5
    assert settings.redis_socket_timeout_seconds == 4
    assert settings.collection_lock_timeout_seconds == 0.5
    assert settings.host == "127.0.0.1"
    assert settings.port == 9000
    assert settings.log_level == "DEBUG"
