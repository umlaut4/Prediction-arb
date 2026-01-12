from __future__ import annotations

import base64
import os
import time
from typing import Any, Dict, Iterable, List, Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from src.ingest.base import WSConfig, WSConnectionManager
from src.observability.metrics import GLOBAL_METRICS
from src.storage.live_quotes import Quote, GLOBAL_QUOTES


def _load_private_key_pem() -> bytes:
    """Load Kalshi private key.

    Supply ONE of:
      - KALSHI_PRIVATE_KEY_PEM (the PEM text)
      - KALSHI_PRIVATE_KEY_PATH (path to .key file)
    """
    pem = os.getenv("KALSHI_PRIVATE_KEY_PEM")
    path = os.getenv("KALSHI_PRIVATE_KEY_PATH")
    if pem:
        return pem.encode("utf-8")
    if path:
        with open(path, "rb") as f:
            return f.read()
    raise RuntimeError("Missing Kalshi private key. Set KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH")


def _kalshi_signature(timestamp_ms: str, method: str, path: str) -> str:
    # Kalshi requires signing: timestamp + METHOD + path_without_query
    path_no_query = path.split("?")[0]
    message = f"{timestamp_ms}{method}{path_no_query}".encode("utf-8")
    private_key = serialization.load_pem_private_key(
        _load_private_key_pem(), password=None, backend=default_backend()
    )
    sig = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")


def kalshi_build_headers() -> Dict[str, str]:
    api_key_id = os.getenv("KALSHI_ACCESS_KEY")
    if not api_key_id:
        raise RuntimeError("Missing KALSHI_ACCESS_KEY")
    ts_ms = str(int(time.time() * 1000))
    signature = _kalshi_signature(ts_ms, "GET", "/trade-api/ws/v2")
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": signature,
    }


def kalshi_subscribe_messages(market_tickers: Optional[List[str]] = None) -> Iterable[Dict[str, Any]]:
    # Default: ticker channel (bid/ask). Optionally orderbook deltas for specific markets.
    yield {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["ticker"],
        },
    }

    if market_tickers:
        yield {
            "id": 2,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": market_tickers,
            },
        }


async def kalshi_on_message(msg: Dict[str, Any]) -> None:
    """Parse Kalshi ticker messages into quote store.

    Docs show:
      type == 'ticker'
      data.market_ticker, data.bid, data.ask
    """
    msg_type = msg.get("type")
    if msg_type != "ticker":
        return

    data = msg.get("data") or {}
    market = data.get("market_ticker")
    if not market:
        return
    bid = data.get("bid")
    ask = data.get("ask")
    try:
        bid_f = float(bid) if bid is not None else None
        ask_f = float(ask) if ask is not None else None
    except Exception:
        await GLOBAL_METRICS.incr("parse_errors")
        return

    q = Quote(venue="kalshi", market_id=str(market), best_bid=bid_f, best_ask=ask_f, ts_unix_s=time.time())
    await GLOBAL_QUOTES.upsert(q)
    await GLOBAL_METRICS.incr("price_updates")
    await GLOBAL_METRICS.set_gauge("last_price_update_unix_s", q.ts_unix_s)


def build_kalshi_manager(*, market_tickers: Optional[List[str]] = None) -> WSConnectionManager:
    url = os.getenv("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
    cfg = WSConfig(name="kalshi", url=url, stale_after_s=float(os.getenv("KALSHI_STALE_AFTER_S", "15")))

    return WSConnectionManager(
        cfg=cfg,
        build_headers=kalshi_build_headers,
        build_subscribe_messages=lambda: kalshi_subscribe_messages(market_tickers),
        on_message=kalshi_on_message,
    )
