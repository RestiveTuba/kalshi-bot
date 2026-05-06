#!/usr/bin/env python3
"""
Read-only smoke test for Kalshi portfolio endpoints used by market_maker.py.

Usage:
    python3 validate_kalshi_portfolio_endpoints.py --ticker KXBTC15M-... --series KXBTC15M
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any

from market_maker import _SimpleClient


def _shape(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"endpoint": name, "top_level_keys": sorted(payload.keys())}
    for key in ("fills", "market_positions", "settlements", "orders"):
        value = payload.get(key)
        if isinstance(value, list):
            out[f"{key}_count"] = len(value)
            out[f"{key}_sample_keys"] = sorted(value[0].keys()) if value and isinstance(value[0], dict) else []
    if "cursor" in payload:
        out["has_cursor"] = bool(payload.get("cursor"))
    return out


async def main() -> None:
    parser = argparse.ArgumentParser(description="Validate read-only Kalshi portfolio endpoint shapes.")
    parser.add_argument("--ticker", default="", help="Optional market ticker filter.")
    parser.add_argument("--series", default="", help="Optional series/event ticker filter.")
    parser.add_argument("--lookback-seconds", type=int, default=3600, help="min_ts lookback for fills/settlements.")
    args = parser.parse_args()

    cli = _SimpleClient()
    try:
        if not cli._private_key:
            raise SystemExit("Kalshi API key/private key not configured; portfolio endpoints require auth.")

        min_ts = int(time.time()) - max(0, args.lookback_seconds)
        calls: list[tuple[str, str, dict[str, Any]]] = [
            ("fills", "portfolio/fills", {"limit": 100, "min_ts": min_ts}),
            ("positions", "portfolio/positions", {"limit": 100, "count_filter": "position"}),
            ("settlements", "portfolio/settlements", {"limit": 100, "min_ts": min_ts}),
        ]
        if args.ticker:
            for _, _, params in calls:
                params["ticker"] = args.ticker
        if args.series:
            calls[2][2]["event_ticker"] = args.series

        summaries = []
        for name, path, params in calls:
            payload = await cli.get(path, params=params)
            summaries.append(_shape(name, payload))

        print(json.dumps({"ok": True, "summaries": summaries}, indent=2, sort_keys=True))
    finally:
        await cli.close()


if __name__ == "__main__":
    asyncio.run(main())
