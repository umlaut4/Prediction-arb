from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


Venue = Literal["polymarket", "kalshi"]
MarketType = Literal["moneyline", "spread", "total"]


class Opportunity(BaseModel):
    """A validated, display-ready arbitrage opportunity (alerts-only)."""

    id: str = Field(..., description="Deterministic id for dedupe (e.g., hash of mapping+side)")
    league: str = Field(..., examples=["NBA"])
    market_type: MarketType

    event_name: str = Field(..., description="Human readable: 'Lakers @ Knicks'")
    start_time: datetime

    # Mapping confidence
    confidence: float = Field(..., ge=0.0, le=1.0)

    # Fee-aware ROI
    roi_pct: float = Field(..., description="Net ROI percent after fees/buffers")

    # Executable size (conservative)
    size_usd: float = Field(..., ge=0.0)

    # Where the arb is (buy yes/no at what venue)
    leg_a_venue: Venue
    leg_b_venue: Venue
    leg_a_desc: str = Field(..., description="e.g., 'YES @ 0.47 (ask)'")
    leg_b_desc: str = Field(..., description="e.g., 'NO  @ 0.49 (ask)'")

    # Prices used to compute ROI
    leg_a_ask: float = Field(..., ge=0.0, le=1.0)
    leg_b_ask: float = Field(..., ge=0.0, le=1.0)

    # Freshness
    updated_at: datetime
    expires_at: datetime

    # Optional debugging / audit
    mapping_id: Optional[str] = None
    notes: Optional[str] = None


class OpportunitySnapshot(BaseModel):
    opportunities: list[Opportunity]
    server_time: datetime


class MetricsSnapshot(BaseModel):
    ws_connections: int
    opportunities_active: int
    total_published: int
    total_expired: int
