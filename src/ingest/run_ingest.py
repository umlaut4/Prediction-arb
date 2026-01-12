from __future__ import annotations

import asyncio
import os
from typing import List

from src.ingest.kalshi_ws import build_kalshi_manager
from src.ingest.polymarket_ws import build_polymarket_manager


def _parse_csv_env(name: str) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


async def start_ingest_tasks() -> None:
    """Start WS clients in background.

    Controlled by env vars:
      - KALSHI_MARKET_TICKERS: comma-separated market tickers to subscribe to orderbook deltas
      - POLYMARKET_ASSET_IDS: comma-separated token IDs to subscribe to market channel

    Notes:
      - Kalshi ticker channel requires auth even for market data.
      - Polymarket market channel is public.
    """

    # Kalshi
    kalshi_market_tickers = _parse_csv_env("KALSHI_MARKET_TICKERS")
    kalshi = build_kalshi_manager(market_tickers=kalshi_market_tickers or None)
    await kalshi.start()

    # Polymarket
    polymarket_asset_ids = _parse_csv_env("POLYMARKET_ASSET_IDS")
    if polymarket_asset_ids:
        poly = build_polymarket_manager(asset_ids=polymarket_asset_ids)
        await poly.start()

    # Keep references alive by stashing them on the module (simple single-process approach)
    # In a larger app you would register these in a DI container.
    globals()["_kalshi_manager"] = kalshi
    globals()["_polymarket_manager"] = poly if polymarket_asset_ids else None
