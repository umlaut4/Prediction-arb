from __future__ import annotations

import asyncio
import os
import random
import string
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set

import orjson

from .schemas import Opportunity, MetricsSnapshot


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(obj) -> bytes:
    return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS)


@dataclass
class _Conn:
    queue: asyncio.Queue[bytes]


class OpportunityStore:
    """In-memory latest-state store + broadcaster.

    Single-process by design for v1. Later can be backed by Redis/pubsub.
    """

    def __init__(self, max_items: int = 500):
        self._max_items = max_items
        self._lock = asyncio.Lock()
        self._items: Dict[str, Opportunity] = {}
        self._conns: Set[_Conn] = set()

        self.total_published = 0
        self.total_expired = 0

    async def list(self) -> List[Opportunity]:
        async with self._lock:
            # newest first
            return sorted(self._items.values(), key=lambda o: o.updated_at, reverse=True)

    async def get(self, opp_id: str) -> Opportunity | None:
        async with self._lock:
            return self._items.get(opp_id)

    async def publish(self, opp: Opportunity) -> None:
        now = utcnow()
        if opp.expires_at <= now:
            return

        async with self._lock:
            self._items[opp.id] = opp
            self.total_published += 1

            # trim
            if len(self._items) > self._max_items:
                # drop oldest
                oldest = sorted(self._items.values(), key=lambda o: o.updated_at)[: len(self._items) - self._max_items]
                for o in oldest:
                    self._items.pop(o.id, None)

        await self._broadcast({"type": "opportunity", "data": opp.model_dump(mode="json")})

    async def expire(self, opp_id: str, reason: str = "stale") -> None:
        removed = None
        async with self._lock:
            removed = self._items.pop(opp_id, None)
            if removed is not None:
                self.total_expired += 1

        if removed is not None:
            await self._broadcast({"type": "expired", "id": opp_id, "reason": reason})

    async def expire_stale(self) -> int:
        """Expire any opportunities whose expires_at has passed."""
        now = utcnow()
        to_expire: List[str] = []
        async with self._lock:
            for oid, o in self._items.items():
                if o.expires_at <= now:
                    to_expire.append(oid)

        for oid in to_expire:
            await self.expire(oid, reason="expired")

        return len(to_expire)

    async def metrics(self) -> MetricsSnapshot:
        async with self._lock:
            return MetricsSnapshot(
                ws_connections=len(self._conns),
                opportunities_active=len(self._items),
                total_published=self.total_published,
                total_expired=self.total_expired,
            )

    def register_conn(self) -> _Conn:
        c = _Conn(queue=asyncio.Queue(maxsize=500))
        self._conns.add(c)
        return c

    def unregister_conn(self, c: _Conn) -> None:
        self._conns.discard(c)

    async def _broadcast(self, payload: dict) -> None:
        msg = _json_dumps(payload)
        dead: List[_Conn] = []
        for c in list(self._conns):
            try:
                c.queue.put_nowait(msg)
            except asyncio.QueueFull:
                # Backpressure: drop this connection (client not reading)
                dead.append(c)
        for c in dead:
            self._conns.discard(c)


STORE = OpportunityStore()


# ---- Integration contract (arb engine calls these) ----
async def publish_opportunity(opportunity: dict) -> None:
    """Publish a validated opportunity dict (must match Opportunity schema)."""
    opp = Opportunity.model_validate(opportunity)
    await STORE.publish(opp)


async def expire_opportunity(opportunity_id: str, reason: str = "stale") -> None:
    await STORE.expire(opportunity_id, reason=reason)


# ---- Demo mode (optional) ----

TEAM_PAIRS = [
    ("Lakers", "Knicks"),
    ("Celtics", "Heat"),
    ("Warriors", "Suns"),
    ("Nets", "Bulls"),
]


def _rid(n: int = 10) -> str:
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))


async def demo_publisher_task() -> None:
    if os.getenv("DEMO_MODE") != "1":
        return

    while True:
        await asyncio.sleep(random.uniform(0.7, 2.5))
        now = utcnow()
        a, b = random.choice(TEAM_PAIRS)
        market_type = random.choice(["moneyline", "spread", "total"])

        # fabricate plausible prices
        leg_a_ask = round(random.uniform(0.43, 0.52), 2)
        leg_b_ask = round(random.uniform(0.43, 0.52), 2)

        # fabricate small positive roi
        roi_pct = round(random.uniform(0.4, 2.8), 2)
        confidence = round(random.uniform(0.8, 1.0), 2)
        size = round(random.uniform(25, 450), 2)

        opp = {
            "id": f"demo_{_rid()}_{market_type}",
            "league": "NBA",
            "market_type": market_type,
            "event_name": f"{a} @ {b}",
            "start_time": (now + timedelta(hours=random.randint(1, 48))).isoformat(),
            "confidence": confidence,
            "roi_pct": roi_pct,
            "size_usd": size,
            "leg_a_venue": "polymarket",
            "leg_b_venue": "kalshi",
            "leg_a_desc": f"YES @ {leg_a_ask:.2f}",
            "leg_b_desc": f"NO @ {leg_b_ask:.2f}",
            "leg_a_ask": leg_a_ask,
            "leg_b_ask": leg_b_ask,
            "updated_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=6)).isoformat(),
            "mapping_id": None,
            "notes": "demo",
        }
        await publish_opportunity(opp)
