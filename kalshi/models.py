from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class OrderbookLevel(BaseModel):
    price: int  # cents
    quantity: int


class Orderbook(BaseModel):
    yes: list[OrderbookLevel] = Field(default_factory=list)
    no: list[OrderbookLevel] = Field(default_factory=list)


class Market(BaseModel):
    ticker: str
    title: str
    yes_bid: int = 0   # cents
    yes_ask: int = 0
    volume: float = 0.0
    open_interest: float = 0.0
    close_time: Optional[datetime] = None
    status: str = "open"
    orderbook: Optional[Orderbook] = None

    @property
    def mid_price(self) -> float:
        """Mid-price as a probability (0.0–1.0)."""
        if self.yes_bid == 0 and self.yes_ask == 0:
            return 0.5
        if self.yes_bid == 0:
            return self.yes_ask / 100
        if self.yes_ask == 0:
            return self.yes_bid / 100
        return ((self.yes_bid + self.yes_ask) / 2) / 100


class OrderbookDelta(BaseModel):
    """Incremental update from the WS orderbook channel."""
    ticker: str
    side: str        # "yes" | "no"
    price: int       # cents
    delta: int       # positive = add, negative = remove quantity
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TradeDecision(BaseModel):
    market_id: str
    signal: str      # "BUY_YES" | "BUY_NO" | "HOLD"
    probability_yes: float
    confidence: float
    edge: float
    position_size_usd: float
    reasoning: str
    approved: bool
    reject_reason: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
