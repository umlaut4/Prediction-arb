from __future__ import annotations

import asyncio
import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional

import websockets
from websockets.client import WebSocketClientProtocol

from src.observability.metrics import GLOBAL_METRICS


JsonDict = Dict[str, Any]


@dataclass(frozen=True)
class WSConfig:
    name: str
    url: str
    # If no message is received for this long, treat as stale and reconnect.
    stale_after_s: float = 15.0
    # Heartbeat interval (if protocol supports app-level ping). websockets also has built-in ping.
    heartbeat_s: float = 10.0
    # Backoff
    backoff_initial_s: float = 0.5
    backoff_max_s: float = 20.0


class WSConnectionManager:
    """Persistent WS manager with reconnect + stale detection.

    Requirements implemented from your framework:
    - auto-reconnect w/ exponential backoff + jitter
    - re-authenticate + re-subscribe after reconnect
    - stale detection using last_message timestamps
    - never block message loop (parsing offloaded)
    """

    def __init__(
        self,
        cfg: WSConfig,
        build_headers: Callable[[], Dict[str, str]],
        build_subscribe_messages: Callable[[], Iterable[JsonDict]],
        on_message: Callable[[JsonDict], Awaitable[None]],
        *,
        max_queue: int = 5000,
    ) -> None:
        self.cfg = cfg
        self._build_headers = build_headers
        self._build_subscribe_messages = build_subscribe_messages
        self._on_message = on_message

        self._queue: asyncio.Queue[JsonDict] = asyncio.Queue(maxsize=max_queue)
        self._stop = asyncio.Event()
        self._last_message_s: float = 0.0
        self._ws: Optional[WebSocketClientProtocol] = None

        self._consumer_task: Optional[asyncio.Task] = None
        self._stale_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._stop.clear()
        self._consumer_task = asyncio.create_task(self._consumer_loop(), name=f"{self.cfg.name}-consumer")
        self._stale_task = asyncio.create_task(self._stale_watchdog(), name=f"{self.cfg.name}-stale")
        asyncio.create_task(self._run_forever(), name=f"{self.cfg.name}-ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        for t in (self._consumer_task, self._stale_task):
            if t:
                t.cancel()

    async def _run_forever(self) -> None:
        backoff = self.cfg.backoff_initial_s
        first = True
        while not self._stop.is_set():
            try:
                headers = self._build_headers()
                # websockets has its own ping/pong, we keep it enabled.
                async with websockets.connect(
                    self.cfg.url,
                    additional_headers=headers,
                    ping_interval=self.cfg.heartbeat_s,
                    ping_timeout=max(5.0, self.cfg.heartbeat_s * 1.5),
                    close_timeout=5,
                    max_queue=1024,
                ) as ws:
                    self._ws = ws
                    self._last_message_s = time.time()
                    await GLOBAL_METRICS.set_gauge("last_message_unix_s", self._last_message_s)
                    if first:
                        await GLOBAL_METRICS.incr("ws_connects")
                        first = False
                    else:
                        await GLOBAL_METRICS.incr("ws_reconnects")

                    # Subscribe
                    for msg in self._build_subscribe_messages():
                        await ws.send(json.dumps(msg))

                    backoff = self.cfg.backoff_initial_s

                    async for raw in ws:
                        self._last_message_s = time.time()
                        await GLOBAL_METRICS.incr("messages_received")
                        await GLOBAL_METRICS.set_gauge("last_message_unix_s", self._last_message_s)
                        try:
                            data = json.loads(raw)
                            await GLOBAL_METRICS.incr("messages_parsed")
                        except Exception:
                            await GLOBAL_METRICS.incr("parse_errors")
                            continue

                        # Never block: put into queue, drop oldest if overwhelmed.
                        if self._queue.full():
                            try:
                                _ = self._queue.get_nowait()
                            except Exception:
                                pass
                        try:
                            self._queue.put_nowait(data)
                        except Exception:
                            pass

            except asyncio.CancelledError:
                return
            except Exception:
                await GLOBAL_METRICS.incr("ws_errors")
                await GLOBAL_METRICS.incr("ws_disconnects")

                # Backoff with jitter
                jitter = random.uniform(0, backoff * 0.2)
                await asyncio.sleep(min(self.cfg.backoff_max_s, backoff + jitter))
                backoff = min(self.cfg.backoff_max_s, backoff * 2)

    async def _consumer_loop(self) -> None:
        while not self._stop.is_set():
            try:
                msg = await self._queue.get()
                await self._on_message(msg)
            except asyncio.CancelledError:
                return
            except Exception:
                # swallow parse/handler errors to keep ingest alive
                await GLOBAL_METRICS.incr("ws_errors")

    async def _stale_watchdog(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            if not self._ws:
                continue
            if self._last_message_s <= 0:
                continue
            if (time.time() - self._last_message_s) > self.cfg.stale_after_s:
                # Force reconnect
                await GLOBAL_METRICS.incr("stale_disconnects")
                try:
                    await self._ws.close()
                except Exception:
                    pass


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")
