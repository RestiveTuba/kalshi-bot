"""
latency/gap_detector.py — Detects price gaps between Binance and Kalshi BTC contracts.

Every 2 seconds, fetches current YES/NO prices on Kalshi's 15-minute BTC contracts.
Compares the implied probability to what Binance momentum suggests the true
probability should be.

When the gap exceeds MIN_GAP_PCT, returns a trade signal.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

import structlog

from config import settings
from latency import binance_feed

log = structlog.get_logger(__name__)

# Minimum gap (in probability points) to trigger a trade signal
# e.g. 0.05 = Kalshi shows 50% but true prob is >55% or <45%
MIN_GAP_PCT = 0.03

# Poll Kalshi contracts this often (seconds)
POLL_INTERVAL = 2.0

# Kalshi series ticker for 15-minute BTC contracts
# Adjust if Kalshi changes their ticker format
BTC_SERIES = "KXBTC"


@dataclass
class GapSignal:
    ticker: str
    kalshi_yes_price: float       # 0-100 cents
    kalshi_implied_prob: float    # 0.0-1.0
    binance_implied_prob: float   # 0.0-1.0
    gap: float                    # binance_implied - kalshi_implied
    action: str                   # "BUY_YES" or "BUY_NO"
    confidence: float             # from Binance momentum
    btc_price: float
    momentum_pct: float
    detected_at: float            # unix timestamp


class GapDetector:
    def __init__(self, kalshi_client):
        self._client = kalshi_client
        self._active_contracts: list[dict] = []
        self._last_contract_refresh = 0.0
        self._signals: list[GapSignal] = []

    async def scan(self) -> list[GapSignal]:
        """
        Run one scan cycle. Returns any gap signals found.
        Called every POLL_INTERVAL seconds by the latency loop.
        """
        # Refresh contract list every 5 minutes
        now = time.time()
        if now - self._last_contract_refresh > 300:
            await self._refresh_contracts()

        if not self._active_contracts:
            log.warning("gap_detector_no_contracts")
            return []

        momentum = binance_feed.get_momentum()
        if momentum is None or momentum["direction"] == "FLAT":
            return []  # No directional signal — nothing to trade

        signals = []
        for contract in self._active_contracts:
            signal = self._evaluate(contract, momentum)
            if signal:
                signals.append(signal)
                log.info(
                    "gap_signal_found",
                    ticker=signal.ticker,
                    action=signal.action,
                    gap=f"{signal.gap:+.1%}",
                    kalshi_prob=f"{signal.kalshi_implied_prob:.1%}",
                    binance_prob=f"{signal.binance_implied_prob:.1%}",
                    confidence=f"{signal.confidence:.1%}",
                )

        return signals

    def _evaluate(self, contract: dict, momentum: dict) -> Optional[GapSignal]:
        """
        Compare Kalshi contract price to Binance momentum signal.

        Binance momentum implies a probability:
        - Strong DOWN move → high probability contract resolves NO (BTC lower)
        - Strong UP move   → high probability contract resolves YES (BTC higher)

        We convert momentum confidence into an implied probability,
        then compare to Kalshi's current price.
        """
        yes_bid = contract.get("yes_bid", 0)
        yes_ask = contract.get("yes_ask", 100)
        if yes_bid == 0 and yes_ask == 100:
            return None  # No liquidity

        # Kalshi mid price as probability
        kalshi_mid = (yes_bid + yes_ask) / 2
        kalshi_implied = kalshi_mid / 100.0

        direction = momentum["direction"]
        confidence = momentum["confidence"]
        pct_change = momentum["pct_change"]

        # Convert momentum to implied probability
        # Flat market → 50/50. Strong move → up to 85% confidence one way.
        # We cap at 0.85 because even strong momentum isn't certain.
        if direction == "UP":
            binance_implied = 0.50 + (confidence * 0.35)  # 0.50 → 0.85
            action = "BUY_YES"
        else:  # DOWN
            binance_implied = 0.50 - (confidence * 0.35)  # 0.50 → 0.15
            action = "BUY_NO"

        gap = binance_implied - kalshi_implied

        # Only signal if gap is large enough
        # For BUY_YES: gap should be positive (binance thinks higher prob than Kalshi)
        # For BUY_NO:  gap should be negative
        if action == "BUY_YES" and gap < MIN_GAP_PCT:
            return None
        if action == "BUY_NO" and gap > -MIN_GAP_PCT:
            return None

        return GapSignal(
            ticker=contract["ticker"],
            kalshi_yes_price=kalshi_mid,
            kalshi_implied_prob=kalshi_implied,
            binance_implied_prob=binance_implied,
            gap=gap,
            action=action,
            confidence=confidence,
            btc_price=momentum["current_price"],
            momentum_pct=pct_change,
            detected_at=time.time(),
        )

    async def _refresh_contracts(self):
        """Fetch active 15-minute BTC contracts from Kalshi."""
        try:
            params = {
                "series_ticker": BTC_SERIES,
                "status": "open",
                "limit": 50,
            }
            data = await self._client.get("/markets", params=params)
            markets = data.get("markets", [])

            # Filter to short-duration contracts only
            # These have "15" or "1H" in their ticker or title
            short_duration = []
            for m in markets:
                ticker = m.get("ticker", "")
                title = m.get("title", "").lower()
                # Include markets that resolve within ~2 hours
                if any(x in ticker.upper() for x in ["15", "1H", "30"]):
                    short_duration.append(m)
                elif "15 minute" in title or "15min" in title or "1 hour" in title:
                    short_duration.append(m)

            # If filter is too aggressive, take all BTC contracts
            self._active_contracts = short_duration if short_duration else markets[:10]
            self._last_contract_refresh = time.time()

            log.info("gap_detector_contracts_refreshed",
                     total=len(markets), short_duration=len(self._active_contracts))

        except Exception as exc:
            log.error("gap_detector_refresh_error", error=str(exc))
            # Back off 30s on failure so we don't hammer the API every 2 seconds
            self._last_contract_refresh = time.time() - 270  # retry in 30s
