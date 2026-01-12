# Prediction Market Arbitrage — Dashboard (v1)

This is the **dashboard + API layer** for the Prediction Market Arbitrage System (alerts-only v1).

## What’s included
- FastAPI HTTP API
- WebSocket stream of opportunity updates
- Simple built-in web UI (single HTML page)
- Health + basic metrics endpoints
- In-memory opportunity store + broadcaster

## What’s included in v2 (this build)
- Venue connectors **implemented**:
  - Kalshi WebSocket client (auth + subscribe + reconnect + stale detection)
  - Polymarket CLOB WebSocket client (market channel)
- Live quote state (hot cache):
  - `GET /api/quotes` (latest bid/ask by venue + market)
- Minimum observability endpoints:
  - `GET /api/system_metrics` (reconnect counts, errors, last message time)

## What’s still not included
- Market discovery + NBA-only filtering (coming next)
- Cross-venue market matching engine
- Arbitrage detector + fee-aware ROI engine
- Auto-execution (explicitly disabled)

## Definition of Done (v1)
- Dashboard shows live opportunities within **2s** of a backend update
- Stale feed / stale signal detection: opportunities expire immediately once stale
- Signals require **confidence ≥ configured threshold** and **ROI% ≥ configured threshold** (net of fees)
- System survives venue outage without crashing (dashboard remains available)
- Opportunities display: ROI% (net of fees), size, confidence, timestamps, and source venues

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# optional demo mode generates fake opportunities
export DEMO_MODE=1

uvicorn src.dashboard.api:app --host 0.0.0.0 --port 8000 --reload
```

## Enable venue ingest (Kalshi + Polymarket)
The ingest clients run inside the FastAPI process (startup tasks).

### 1) Set environment variables

#### Kalshi (required)
Kalshi websocket requires authentication headers. Provide:

- `KALSHI_ACCESS_KEY` — your API key ID
- `KALSHI_PRIVATE_KEY_PEM` **or** `KALSHI_PRIVATE_KEY_PATH` — your RSA private key

Optional:
- `KALSHI_WS_URL` (default: production)
- `KALSHI_MARKET_TICKERS` (comma-separated, for orderbook_delta subscriptions)

#### Polymarket (optional)
Polymarket market channel is public, but you must tell the system which tokens to subscribe to:

- `POLYMARKET_ASSET_IDS` (comma-separated token IDs)

### 2) Run with ingest enabled
```bash
export ENABLE_INGEST=1
uvicorn src.dashboard.api:app --host 0.0.0.0 --port 8000 --reload
```

### 3) Verify ingest is working
- Quotes: `http://localhost:8000/api/quotes`
- System metrics: `http://localhost:8000/api/system_metrics`

Open:
- UI: `http://localhost:8000/`
- API: `http://localhost:8000/api/opportunities`
- WS: `ws://localhost:8000/ws/opportunities`

## Integration contract (for the arb engine)
The arb engine should call:
- `src.dashboard.state.publish_opportunity(opportunity_dict)` on validated opportunities
- `src.dashboard.state.expire_opportunity(opportunity_id)` when invalid/stale

Opportunity dict must match the schema in `src/dashboard/schemas.py`.
