from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Counters:
    # Connection lifecycle
    ws_connects: int = 0
    ws_reconnects: int = 0
    ws_disconnects: int = 0
    ws_errors: int = 0
    stale_disconnects: int = 0

    # Data health
    messages_received: int = 0
    messages_parsed: int = 0
    parse_errors: int = 0
    price_updates: int = 0

    # Mapping / signal health placeholders
    low_confidence_rejects: int = 0
    opportunities_published: int = 0
    opportunities_expired: int = 0


@dataclass
class Gauges:
    last_message_unix_s: float = 0.0
    last_price_update_unix_s: float = 0.0
    active_subscriptions: int = 0


class Metrics:
    """In-memory metrics. Single-process friendly.

    In prod, you can export these to Prometheus/OpenTelemetry.
    """

    def __init__(self) -> None:
        self.counters = Counters()
        self.gauges = Gauges()
        self._lock = asyncio.Lock()

    async def incr(self, field_name: str, value: int = 1) -> None:
        async with self._lock:
            current = getattr(self.counters, field_name)
            setattr(self.counters, field_name, current + value)

    async def set_gauge(self, field_name: str, value: float) -> None:
        async with self._lock:
            setattr(self.gauges, field_name, value)

    async def snapshot(self) -> Dict:
        async with self._lock:
            return {
                "counters": self.counters.__dict__.copy(),
                "gauges": self.gauges.__dict__.copy(),
                "unix_s": time.time(),
            }


GLOBAL_METRICS = Metrics()
