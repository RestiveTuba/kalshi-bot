"""
latency/loop.py — Main latency arb loop. Runs as a background asyncio task.

Sequence every POLL_INTERVAL seconds:
1. Check Binance momentum (already streaming via WebSocket)
2. Fetch Kalshi 15-min BTC contract prices
3. Detect any price gaps
4. Execute trades on valid signals
5. Tracker monitors open positions in parallel
"""
from __future__ import annotations

import asyncio

import structlog

from latency import binance_feed
from latency.gap_detector import GapDetector, POLL_INTERVAL
from latency.executor import execute
from latency.tracker import PositionTracker

log = structlog.get_logger(__name__)


async def run_latency_loop(kalshi_client, portfolio_value_ref: list[float]):
    """
    Main entry point. Call from main.py as:
        asyncio.create_task(run_latency_loop(kalshi_client, portfolio_value_ref))

    portfolio_value_ref is a mutable list [float] so the value
    stays current as the main loop updates it.
    """
    log.info("latency_loop_starting")

    # Start Binance WebSocket feed
    await binance_feed.start()

    # Wait for first price data
    log.info("latency_waiting_for_binance_feed")
    for _ in range(30):  # wait up to 30 seconds
        if binance_feed.is_connected():
            break
        await asyncio.sleep(1)

    if not binance_feed.is_connected():
        log.error("latency_binance_feed_failed_to_connect")
        return

    log.info("latency_binance_feed_ready", price=binance_feed.get_price())

    detector = GapDetector(kalshi_client)
    tracker = PositionTracker(kalshi_client)

    # Run tracker as separate task
    tracker_task = asyncio.create_task(tracker.run())

    try:
        while True:
            try:
                # Scan for gap signals
                signals = await detector.scan()

                for signal in signals:
                    portfolio_value = portfolio_value_ref[0]
                    await execute(
                        signal=signal,
                        kalshi_client=kalshi_client,
                        portfolio_value=portfolio_value,
                    )

            except Exception as exc:
                log.error("latency_loop_error", error=str(exc))

            await asyncio.sleep(POLL_INTERVAL)

    except asyncio.CancelledError:
        log.info("latency_loop_stopping")
    finally:
        tracker_task.cancel()
        await binance_feed.stop()
        log.info("latency_loop_stopped")
