#!/usr/bin/env python3
"""
Daily snapshot and fill-balance audit for Kalshi bot.

Run manually or via cron. Outputs snapshot growth and fill balance to stdout
and appends to audit log. Helps track data collection progress and validate
market maker behavior.

Usage:
  python3 audit_daily.py                    # Run audit, print to stdout
  python3 audit_daily.py >> audit_daily.log # Append to log file
"""

import json
import sqlite3
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path

# Paths
DB_PATH = Path("/root/kalshi-bot/kalshi_data.db")
LEDGER_PATH = Path("/root/kalshi-bot/market_maker_ledger.jsonl")
LOG_PATH = Path("/root/kalshi-bot/audit_daily.log")

def snapshot_audit():
    """Report snapshot growth per series."""
    if not DB_PATH.exists():
        return {"error": "No database found"}
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    result = cur.execute("""
        SELECT series, COUNT(*) as count, MAX(snapshot_ts) as latest
        FROM market_snapshots
        GROUP BY series
        ORDER BY series
    """).fetchall()
    conn.close()
    
    return {
        series: {
            "snapshots": count,
            "latest": latest
        }
        for series, count, latest in result
    }

def fill_balance_audit(date_filter=None):
    """
    Report YES/NO fill balance and realized P&L per ticker.
    
    If date_filter is None, audits all time.
    If date_filter is a date string (YYYY-MM-DD), audits that day only.
    
    Returns:
        dict: {ticker: {YES: count, NO: count, imbalance: int, pnl_dollars: float}}
    """
    if not LEDGER_PATH.exists():
        return {"error": "No ledger found"}
    
    fills_by_ticker = defaultdict(lambda: {"YES": 0, "NO": 0})
    closes_by_ticker = defaultdict(float)
    
    with open(LEDGER_PATH) as f:
        for line in f:
            row = json.loads(line)
            
            # Filter by date if specified
            if date_filter:
                ts = row.get("ts", "")
                if not ts.startswith(date_filter):
                    continue
            
            ticker = row.get("ticker")
            if not ticker:
                continue
            
            event_type = row.get("event_type")
            
            if event_type == "fill":
                side = row.get("side")
                if side in ("YES", "NO"):
                    fills_by_ticker[ticker][side] += 1
            
            elif event_type in ("manual_close", "settlement"):
                pnl = float(row.get("pnl_dollars") or 0)
                closes_by_ticker[ticker] += pnl
    
    result = {}
    for ticker in sorted(fills_by_ticker.keys()):
        y = fills_by_ticker[ticker]["YES"]
        n = fills_by_ticker[ticker]["NO"]
        imbalance = abs(y - n)
        pnl = closes_by_ticker[ticker]
        
        result[ticker] = {
            "YES": y,
            "NO": n,
            "imbalance": imbalance,
            "pnl_dollars": round(pnl, 4),
        }
    
    return result

def main():
    now = datetime.now(timezone.utc)
    timestamp = now.isoformat()
    today_str = now.strftime("%Y-%m-%d")
    
    print(f"\n{'='*70}")
    print(f"Audit: {timestamp}")
    print(f"{'='*70}")
    
    # Snapshot audit (all time)
    print("\nSnapshot Count (all time):")
    snapshots = snapshot_audit()
    if "error" not in snapshots:
        for series in sorted(snapshots.keys()):
            info = snapshots[series]
            print(f"  {series}: {info['snapshots']:4d} snapshots (latest: {info['latest']})")
    else:
        print(f"  {snapshots['error']}")
    
    # Fill balance (all time)
    print("\nFill Balance & P&L (all time):")
    fills_all = fill_balance_audit()
    if "error" not in fills_all:
        for ticker in sorted(fills_all.keys()):
            info = fills_all[ticker]
            print(
                f"  {ticker}: YES={info['YES']:3d} NO={info['NO']:3d} "
                f"imbalance={info['imbalance']:2d} pnl=${info['pnl_dollars']:+7.2f}"
            )
    else:
        print(f"  {fills_all['error']}")
    
    # Fill balance (today only)
    print(f"\nFill Balance & P&L (today: {today_str}):")
    fills_today = fill_balance_audit(date_filter=today_str)
    if "error" not in fills_today:
        if fills_today:
            for ticker in sorted(fills_today.keys()):
                info = fills_today[ticker]
                print(
                    f"  {ticker}: YES={info['YES']:3d} NO={info['NO']:3d} "
                    f"imbalance={info['imbalance']:2d} pnl=${info['pnl_dollars']:+7.2f}"
                )
        else:
            print("  (no trades today yet)")
    else:
        print(f"  {fills_today['error']}")
    
    # Summary
    print("\n" + "="*70)
    if "error" not in snapshots:
        total_snapshots = sum(s["snapshots"] for s in snapshots.values())
        progress = min(100, int(100 * total_snapshots / (500 * 3)))
        print(f"Data collection progress: {progress}% toward 500 snapshots per series")
    
    if "error" not in fills_all:
        total_pnl = sum(f["pnl_dollars"] for f in fills_all.values())
        print(f"Cumulative P&L (ledger realized): ${total_pnl:+.2f}")
    
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
