"""Tests for Kombu-compatible Redis key construction."""

from __future__ import annotations

from care_metrics_exporter.redis_keys import (
    PRIORITY_SEPARATOR,
    build_queue_key_map,
    shard_keys,
)


def test_separator_matches_kombu() -> None:
    assert PRIORITY_SEPARATOR == b"\x06\x16"


def test_default_priority_steps_produce_kombu_keys() -> None:
    assert shard_keys("celery", (0, 3, 6, 9)) == (
        b"celery",
        b"celery\x06\x163",
        b"celery\x06\x166",
        b"celery\x06\x169",
    )


def test_priority_zero_uses_the_bare_queue_name() -> None:
    assert shard_keys("celery", (0,)) == (b"celery",)


def test_global_keyprefix_is_prepended_to_every_shard() -> None:
    assert shard_keys("celery", (0, 3), global_keyprefix="care:") == (
        b"care:celery",
        b"care:celery\x06\x163",
    )


def test_custom_priority_steps_are_honoured_in_order() -> None:
    assert shard_keys("images", (0, 5)) == (b"images", b"images\x06\x165")


def test_non_ascii_queue_names_are_utf8_encoded() -> None:
    assert shard_keys("wärteschlange", (0,)) == ("wärteschlange".encode(),)


def test_build_queue_key_map_covers_every_queue() -> None:
    key_map = build_queue_key_map(("celery", "reports"), (0, 3))

    assert key_map == {
        "celery": (b"celery", b"celery\x06\x163"),
        "reports": (b"reports", b"reports\x06\x163"),
    }


def test_build_queue_key_map_accepts_an_iterator_of_steps() -> None:
    key_map = build_queue_key_map(("celery", "reports"), iter((0, 3)))

    assert key_map["celery"] == (b"celery", b"celery\x06\x163")
    assert key_map["reports"] == (b"reports", b"reports\x06\x163")
