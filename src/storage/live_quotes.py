from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class Quote:
    venue: str
    market_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    ts_unix_s: float


class LiveQuoteStore:
    """Hot in-memory state for latest quotes only (no history)."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._quotes: Dict[Tuple[str, str], Quote] = {}

    async def upsert(self, quote: Quote) -> None:
        async with self._lock:
            self._quotes[(quote.venue, quote.market_id)] = quote

    async def snapshot(self, max_age_s: float = 60.0) -> Dict:
        now = time.time()
        async with self._lock:
            items = []
            for q in self._quotes.values():
                if (now - q.ts_unix_s) <= max_age_s:
                    items.append(q.__dict__.copy())
            return {
                "count": len(items),
                "max_age_s": max_age_s,
                "items": sorted(items, key=lambda x: x["ts_unix_s"], reverse=True)[:500],
            }


GLOBAL_QUOTES = LiveQuoteStore()
