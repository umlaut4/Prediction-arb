from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .schemas import OpportunitySnapshot
from .state import STORE, demo_publisher_task, utcnow
from src.observability.metrics import GLOBAL_METRICS
from src.storage.live_quotes import GLOBAL_QUOTES
from src.ingest.run_ingest import start_ingest_tasks

app = FastAPI(title="Prediction Arb Dashboard", version="1.0.0")

# Static UI
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "time": utcnow().isoformat()}


@app.get("/api/opportunities", response_model=OpportunitySnapshot)
async def list_opportunities() -> OpportunitySnapshot:
    items = await STORE.list()
    return OpportunitySnapshot(opportunities=items, server_time=utcnow())


@app.get("/api/metrics")
async def metrics() -> dict:
    m = await STORE.metrics()
    return m.model_dump(mode="json") | {"time": utcnow().isoformat()}


@app.get("/api/system_metrics")
async def system_metrics() -> dict:
    """Framework-required minimum observability."""
    return await GLOBAL_METRICS.snapshot()


@app.get("/api/quotes")
async def quotes(max_age_s: float = 60.0) -> dict:
    """Latest venue quotes (hot state only)."""
    return await GLOBAL_QUOTES.snapshot(max_age_s=max_age_s)


@app.websocket("/ws/opportunities")
async def ws_opportunities(ws: WebSocket):
    await ws.accept()
    conn = STORE.register_conn()
    try:
        # send initial snapshot
        items = await STORE.list()
        await ws.send_json(
            {
                "type": "snapshot",
                "data": {
                    "opportunities": [o.model_dump(mode="json") for o in items],
                    "server_time": utcnow().isoformat(),
                },
            }
        )

        while True:
            msg = await conn.queue.get()
            # we store bytes for speed; websocket can send bytes
            await ws.send_bytes(msg)
    except WebSocketDisconnect:
        pass
    finally:
        STORE.unregister_conn(conn)


async def _stale_reaper_loop() -> None:
    while True:
        await asyncio.sleep(0.5)
        await STORE.expire_stale()


@app.on_event("startup")
async def _startup() -> None:
    asyncio.create_task(_stale_reaper_loop())

    # Demo-mode opportunity generator (for UI smoke tests)
    if os.getenv("DEMO_MODE", "").strip().lower() in ("1", "true", "yes", "on"):
        asyncio.create_task(demo_publisher_task())

    # Ingest tasks (Kalshi/Polymarket clients)
    if os.getenv("ENABLE_INGEST", "").strip().lower() in ("1", "true", "yes", "on"):
        await start_ingest_tasks()
