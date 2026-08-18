"""Deterministic construction of the Redis keys Celery uses for its queues.

Celery's Kombu Redis transport does not store a queue in a single list. It
shards the queue across one Redis list per configured priority step, so the
logical depth of a queue is the sum of the lengths of all of its shards.

This module reimplements that key layout with pure functions so the exporter
never has to import Celery or Kombu at runtime just to build four keys.
"""

from __future__ import annotations

from collections.abc import Iterable

# Kombu separates a queue name from its priority suffix using these two control
# bytes. See ``kombu.transport.redis.Channel.sep``. The priority-zero shard uses
# the bare queue name with no separator and no suffix.
PRIORITY_SEPARATOR = b"\x06\x16"

#: Byte encoding Kombu uses for queue names.
KEY_ENCODING = "utf-8"


def shard_keys(
    queue: str,
    priority_steps: Iterable[int],
    global_keyprefix: str = "",
) -> tuple[bytes, ...]:
    """Return the Redis list keys backing one logical queue, in step order.

    Keys are returned as bytes because the priority separator is not printable
    and round-tripping it through ``str`` invites encoding mistakes.
    """
    prefix = global_keyprefix.encode(KEY_ENCODING)
    name = queue.encode(KEY_ENCODING)

    keys = []
    for step in priority_steps:
        suffix = b"" if step == 0 else PRIORITY_SEPARATOR + str(step).encode("ascii")
        keys.append(prefix + name + suffix)

    return tuple(keys)


def build_queue_key_map(
    queues: Iterable[str],
    priority_steps: Iterable[int],
    global_keyprefix: str = "",
) -> dict[str, tuple[bytes, ...]]:
    """Map every logical queue to its ordered tuple of Redis shard keys."""
    steps = tuple(priority_steps)
    return {queue: shard_keys(queue, steps, global_keyprefix) for queue in queues}
