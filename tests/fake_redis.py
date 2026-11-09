"""A tiny in-memory Redis, enough for the speed layer (tests only, not a dependency).

Only the commands ``bikeflow.streaming.redis_sink`` uses are implemented, with the
same semantics for the parts we rely on (``SADD``/``SREM``/``SCARD``, sorted sets by
score, hash fields as strings, keys that expire). It lets the writer, the alert
index and the counters be tested with no server running; the real thing is tested
by ``test_streaming_redis.py`` when a Redis is reachable.
"""

from __future__ import annotations

import fnmatch
import time


class FakePipeline:
    """Queues calls and applies them in order on ``execute()``, like a real pipeline."""

    def __init__(self, redis: "FakeRedis"):
        self._redis = redis
        self._ops: list[tuple[str, tuple, dict]] = []

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return lambda *args, **kwargs: self._queue(name, *args, **kwargs)

    def _queue(self, name: str, *args, **kwargs):
        self._ops.append((name, args, kwargs))
        return self

    def execute(self) -> list:
        results = [getattr(self._redis, name)(*args, **kwargs) for name, args, kwargs in self._ops]
        self._ops = []
        return results


class FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict] = {}
        self.sets: dict[str, set] = {}
        self.zsets: dict[str, dict] = {}
        self.streams: dict[str, list] = {}
        self.expires: dict[str, float] = {}
        self.published: list[tuple[str, str]] = []

    # ------------------------------------------------------------------ plumbing
    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self)

    def _purge(self, key: str) -> None:
        deadline = self.expires.get(key)
        if deadline is not None and deadline <= time.monotonic():
            for store in (self.hashes, self.sets, self.zsets, self.streams):
                store.pop(key, None)
            self.expires.pop(key, None)

    def keys(self) -> list[str]:
        return sorted(set(self.hashes) | set(self.sets) | set(self.zsets) | set(self.streams))

    def ping(self) -> bool:
        return True

    def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    def scan(self, cursor: int = 0, match: str | None = None, count: int = 1000):
        keys = self.keys()
        if match:
            keys = [key for key in keys if fnmatch.fnmatch(key, match)]
        return 0, keys

    # -------------------------------------------------------------------- hashes
    def hset(self, key: str, mapping: dict | None = None, **kwargs) -> int:
        self._purge(key)
        data = dict(mapping or {})
        data.update(kwargs)
        # Like redis-py, which raises DataError before sending anything.
        if any(v is None for v in data.values()):
            raise TypeError(f"HSET {key}: None is not a valid Redis value ({data})")
        bucket = self.hashes.setdefault(key, {})
        bucket.update({str(k): str(v) for k, v in data.items()})
        return len(data)

    def hgetall(self, key: str) -> dict:
        self._purge(key)
        return dict(self.hashes.get(key, {}))

    def expire(self, key: str, seconds: int) -> bool:
        self.expires[key] = time.monotonic() + float(seconds)
        return True

    def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            found = False
            for store in (self.hashes, self.sets, self.zsets, self.streams):
                if store.pop(key, None) is not None:
                    found = True
            self.expires.pop(key, None)
            deleted += int(found)
        return deleted

    # ---------------------------------------------------------------------- sets
    def sadd(self, key: str, member: str) -> int:
        self._purge(key)
        bucket = self.sets.setdefault(key, set())
        before = len(bucket)
        bucket.add(str(member))
        return len(bucket) - before

    def srem(self, key: str, member: str) -> int:
        self._purge(key)
        bucket = self.sets.get(key, set())
        before = len(bucket)
        bucket.discard(str(member))
        return before - len(bucket)

    def smembers(self, key: str) -> set:
        self._purge(key)
        return set(self.sets.get(key, set()))

    def scard(self, key: str) -> int:
        self._purge(key)
        return len(self.sets.get(key, set()))

    # ----------------------------------------------------------------- sorted sets
    def zadd(self, key: str, mapping: dict) -> int:
        self._purge(key)
        bucket = self.zsets.setdefault(key, {})
        bucket.update({str(member): float(score) for member, score in mapping.items()})
        return len(mapping)

    def zrem(self, key: str, member: str) -> int:
        self._purge(key)
        return int(self.zsets.get(key, {}).pop(str(member), None) is not None)

    def zcard(self, key: str) -> int:
        self._purge(key)
        return len(self.zsets.get(key, {}))

    def _sorted(self, key: str, reverse: bool = False) -> list:
        self._purge(key)
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1], reverse=reverse)
        return [member for member, _ in items]

    def zrange(self, key: str, start: int, end: int) -> list:
        items = self._sorted(key)
        return items[start:] if end == -1 else items[start : end + 1]

    def zrevrange(self, key: str, start: int, end: int) -> list:
        items = self._sorted(key, reverse=True)
        return items[start:] if end == -1 else items[start : end + 1]

    def zrangebyscore(self, key: str, minimum, maximum) -> list:
        self._purge(key)
        low = float("-inf") if str(minimum) == "-inf" else float(minimum)
        high = float("inf") if str(maximum) == "+inf" else float(maximum)
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        return [member for member, score in items if low <= score <= high]

    def zremrangebyrank(self, key: str, start: int, end: int) -> int:
        items = self._sorted(key)
        total = len(items)
        first = start if start >= 0 else total + start
        last = end if end >= 0 else total + end
        for member in items[max(0, first) : last + 1]:
            self.zsets.get(key, {}).pop(member, None)
        return 1

    # -------------------------------------------------------------------- streams
    def xadd(self, key: str, fields: dict, maxlen: int | None = None, approximate: bool = True) -> str:
        self._purge(key)
        stream = self.streams.setdefault(key, [])
        stream.append(dict(fields))
        if maxlen is not None and len(stream) > maxlen:
            del stream[: len(stream) - maxlen]
        return f"{len(stream)}-0"

    def xlen(self, key: str) -> int:
        self._purge(key)
        return len(self.streams.get(key, []))

