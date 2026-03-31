"""
data/db.py — SQLite schema and async query helpers via aiosqlite.

Tables
------
decisions   — every trade decision (approved or skipped), written before acting
positions   — current open positions (upserted on each fill)
portfolio   — daily snapshots for P&L tracking
"""
from __future__ import annotations

import json
from datetime import datetime, date
from typing import Optional

import aiosqlite
import structlog

from config import settings

log = structlog.get_logger(__name__)

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id       TEXT    NOT NULL,
    signal          TEXT    NOT NULL,
    probability_yes REAL    NOT NULL,
    confidence      REAL    NOT NULL,
    edge            REAL    NOT NULL,
    position_size   REAL    NOT NULL,
    reasoning       TEXT,
    approved        INTEGER NOT NULL,
    reject_reason   TEXT,
    timestamp       TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    ticker          TEXT    PRIMARY KEY,
    side            TEXT    NOT NULL,
    quantity        INTEGER NOT NULL DEFAULT 0,
    avg_price_cents INTEGER NOT NULL DEFAULT 0,
    cur_price_cents INTEGER NOT NULL DEFAULT 0,
    opened_at       TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio (
    snapshot_date   TEXT    PRIMARY KEY,
    starting_value  REAL    NOT NULL,
    end_value       REAL,
    daily_pnl       REAL,
    trade_count     INTEGER DEFAULT 0,
    created_at      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_decisions_market ON decisions(market_id);
CREATE INDEX IF NOT EXISTS idx_decisions_ts     ON decisions(timestamp);
"""


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

async def init_db() -> None:
    """Create tables if they don't exist. Call once at startup."""
    async with aiosqlite.connect(settings.db_path) as db:
        await db.executescript(_SCHEMA)
        await db.commit()
    log.info("db_initialized", path=settings.db_path)


# ---------------------------------------------------------------------------
# Decision log
# ---------------------------------------------------------------------------

async def log_decision(decision) -> int:
    """Insert a TradeDecision and return its row id.

    Accepts either a TradeDecision pydantic model or a plain dict.
    """
    d = decision.model_dump() if hasattr(decision, "model_dump") else decision
    async with aiosqlite.connect(settings.db_path) as db:
        cursor = await db.execute(
            """
            INSERT INTO decisions
              (market_id, signal, probability_yes, confidence, edge,
               position_size, reasoning, approved, reject_reason, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                d["market_id"],
                d["signal"],
                d["probability_yes"],
                d["confidence"],
                d["edge"],
                d["position_size_usd"],
                d.get("reasoning", ""),
                1 if d["approved"] else 0,
                d.get("reject_reason"),
                d.get("timestamp", datetime.utcnow()).isoformat()
                if isinstance(d.get("timestamp"), datetime)
                else str(d.get("timestamp", datetime.utcnow())),
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def get_recent_decisions(limit: int = 50) -> list[dict]:
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM decisions ORDER BY timestamp DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

async def upsert_position(
    ticker: str,
    side: str,
    quantity: int,
    avg_price_cents: int,
    cur_price_cents: int,
) -> None:
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            """
            INSERT INTO positions
              (ticker, side, quantity, avg_price_cents, cur_price_cents, opened_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
              side            = excluded.side,
              quantity        = excluded.quantity,
              avg_price_cents = excluded.avg_price_cents,
              cur_price_cents = excluded.cur_price_cents,
              updated_at      = excluded.updated_at
            """,
            (ticker, side, quantity, avg_price_cents, cur_price_cents, now, now),
        )
        await db.commit()


async def get_positions() -> list[dict]:
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM positions WHERE quantity > 0 ORDER BY updated_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_exposure(ticker: str) -> float:
    """Return current dollar exposure for *ticker* (quantity × avg_price in USD)."""
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT quantity, avg_price_cents FROM positions WHERE ticker = ?",
            (ticker,),
        )
        row = await cursor.fetchone()
        if not row:
            return 0.0
        return row["quantity"] * row["avg_price_cents"] / 100.0


async def close_position(ticker: str) -> None:
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            "UPDATE positions SET quantity = 0, updated_at = ? WHERE ticker = ?",
            (datetime.utcnow().isoformat(), ticker),
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Portfolio snapshots
# ---------------------------------------------------------------------------

async def ensure_portfolio_snapshot(starting_value: float) -> None:
    """Create today's snapshot row if it doesn't exist."""
    today = date.today().isoformat()
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            """
            INSERT OR IGNORE INTO portfolio
              (snapshot_date, starting_value, created_at)
            VALUES (?, ?, ?)
            """,
            (today, starting_value, now),
        )
        await db.commit()


async def update_portfolio_snapshot(
    end_value: float,
    daily_pnl: float,
    trade_count: int,
) -> None:
    today = date.today().isoformat()
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(
            """
            UPDATE portfolio
            SET end_value = ?, daily_pnl = ?, trade_count = ?
            WHERE snapshot_date = ?
            """,
            (end_value, daily_pnl, trade_count, today),
        )
        await db.commit()


async def get_daily_pnl() -> float:
    """Return today's realised P&L (end_value - starting_value), or 0."""
    today = date.today().isoformat()
    async with aiosqlite.connect(settings.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT starting_value, end_value FROM portfolio WHERE snapshot_date = ?",
            (today,),
        )
        row = await cursor.fetchone()
        if not row or row["end_value"] is None:
            return 0.0
        return row["end_value"] - row["starting_value"]


async def get_daily_loss() -> float:
    """Return today's loss as a positive number (0 if in profit)."""
    pnl = await get_daily_pnl()
    return max(0.0, -pnl)
