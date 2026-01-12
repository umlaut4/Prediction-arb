from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, List, Optional

from src.ingest.base import WSConfig, WSConnectionManager
from src.observability.metrics import GLOBAL_METRICS
from src.storage.live_quotes import Quote, GLOBAL_QUOTES


def polymarket_build_headers() -> Dict[str, str]:
    """Polymarket market channel is public.

    You can optionally provide API keys for user channel later.
    """
    # No auth required for market channel.
    return {}


def polymarket_subscribe_messages(asset_ids: List[str]) -> Iterable[Dict[str, Any]]:
    # Market channel subscription payload (sent after connect).
    # Docs show: {"assets_ids": [...], "type": "market"}
    yield {"assets_ids": asset_ids, "type": "market"}


def _best_from_book(msg: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    bids = msg.get("bids") or msg.get("buys") or []
    asks = msg.get("asks") or msg.get("sells") or []

    best_bid = None
    best_ask = None
    try:
        if bids:
            best_bid = max(float(level["price"]) for level in bids)
        if asks:
            best_ask = min(float(level["price"]) for level in asks)
    except Exception:
        return None, None
    return best_bid, best_ask


async def polymarket_on_message(msg: Dict[str, Any]) -> None:
    # Market channel message types include: book, price_change, best_bid_ask, etc.
    event_type = msg.get("event_type")

    # We key by asset_id (token id), because that's what subscriptions are based on.
    asset_id = msg.get("asset_id")
    if not asset_id:
        # Some messages may include changes array; ignore for now.
        return

    bid: Optional[float] = None
    ask: Optional[float] = None

    if event_type == "book":
        bid, ask = _best_from_book(msg)
    elif event_type in ("best_bid_ask", "price_change"):
        # price_change includes best_bid/best_ask fields according to docs.
        try:
            if msg.get("best_bid") is not None:
                bid = float(msg.get("best_bid"))
            if msg.get("best_ask") is not None:
                ask = float(msg.get("best_ask"))
        except Exception:
            bid, ask = None, None
    else:
        return

    if bid is None and ask is None:
        return

    q = Quote(venue="polymarket", market_id=str(asset_id), best_bid=bid, best_ask=ask, ts_unix_s=time.time())
    await GLOBAL_QUOTES.upsert(q)
    await GLOBAL_METRICS.incr("price_updates")
    await GLOBAL_METRICS.set_gauge("last_price_update_unix_s", q.ts_unix_s)


def build_polymarket_manager(*, asset_ids: List[str]) -> WSConnectionManager:
    # Full endpoint from Polymarket docs: wss://ws-subscriptions-clob.polymarket.com/ws/
    base = os.getenv("POLYMARKET_WS_BASE", "wss://ws-subscriptions-clob.polymarket.com")
    url = base.rstrip("/") + "/ws/market"
    cfg = WSConfig(name="polymarket", url=url, stale_after_s=float(os.getenv("POLYMARKET_STALE_AFTER_S", "15")))
    return WSConnectionManager(
        cfg=cfg,
        build_headers=polymarket_build_headers,
        build_subscribe_messages=lambda: polymarket_subscribe_messages(asset_ids),
        on_message=polymarket_on_message,
    )
