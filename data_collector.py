#!/usr/bin/env python3
"""
data_collector.py - Collects historical Kalshi contract data + Coinbase spot prices.
Stores in SQLite for probability model training. Runs every 15 minutes.
"""
import sqlite3, json, time, logging, sys, re
from datetime import datetime, timezone
from pathlib import Path
import urllib.request, urllib.error

DB_PATH = Path(__file__).resolve().parent / "kalshi_data.db"
LOG_PATH = Path(__file__).resolve().parent / "data_collector.log"
POLL_INTERVAL_SECS = 900
SERIES = ["KXBTC15M", "KXETH15M", "KXSOL15M"]
COINBASE_SYMBOLS = {"KXBTC15M": "BTC-USD", "KXETH15M": "ETH-USD", "KXSOL15M": "SOL-USD"}
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
STRIKE_RE = re.compile(r"\$?([\d,]+(?:\.\d+)?)")
CONTRACT_COLUMNS = {
    "strike_price": "REAL",
    "spot_open_minus_strike": "REAL",
    "spot_open_distance_pct": "REAL",
    "spot_close_minus_strike": "REAL",
    "spot_close_distance_pct": "REAL",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("data_collector")

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            ticker TEXT PRIMARY KEY, series TEXT NOT NULL,
            open_time TEXT, close_time TEXT,
            open_price_yes REAL, close_price_yes REAL, settlement TEXT,
            strike_price REAL,
            spot_open_minus_strike REAL, spot_open_distance_pct REAL,
            spot_close_minus_strike REAL, spot_close_distance_pct REAL,
            spot_open REAL, spot_close REAL, spot_return_pct REAL,
            time_of_day_utc INTEGER, day_of_week INTEGER,
            collected_at TEXT, raw_json TEXT)""")
    ensure_contract_columns(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT, series TEXT, contracts_found INTEGER,
            contracts_new INTEGER, errors TEXT)""")
    conn.commit()
    log.info("DB ready at %s", db_path)
    return conn

def ensure_contract_columns(conn):
    existing = {row[1] for row in conn.execute("PRAGMA table_info(contracts)").fetchall()}
    for name, col_type in CONTRACT_COLUMNS.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE contracts ADD COLUMN {name} {col_type}")
    conn.commit()

def kalshi_get(endpoint, params=None):
    url = f"{KALSHI_BASE}/{endpoint}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        log.error("Kalshi error: %s", e)
        return {}

def fetch_settled_markets(series):
    all_m, cursor = [], None
    while True:
        params = {"series_ticker": series, "status": "finalized", "limit": "200"}
        if cursor:
            params["cursor"] = cursor
        data = kalshi_get("markets", params)
        markets = data.get("markets") or []
        if not markets:
            break
        all_m.extend(markets)
        cursor = data.get("cursor")
        if not cursor or len(all_m) >= 500:
            break
        time.sleep(0.5)
    log.info("[%s] Fetched %d settled markets", series, len(all_m))
    return all_m

def fetch_coinbase_candle(symbol, timestamp_iso):
    try:
        dt = datetime.fromisoformat(timestamp_iso.replace("Z", "+00:00"))
        ts = int(dt.timestamp())
        url = (f"https://api.exchange.coinbase.com/products/{symbol}/candles"
               f"?start={ts-900}&end={ts+900}&granularity=900")
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            candles = json.loads(r.read().decode())
            if candles and isinstance(candles, list):
                return float(candles[0][4])
    except Exception as e:
        log.debug("Coinbase error for %s: %s", symbol, e)
    return None

def extract_strike_price(market):
    for field in ("yes_sub_title", "subtitle", "title", "rules_primary"):
        value = market.get(field)
        if not value:
            continue
        match = STRIKE_RE.search(str(value))
        if match:
            return float(match.group(1).replace(",", ""))
    return None

def extract_open_price_yes(market):
    for field in (
        "open_price_yes",
        "yes_open_price",
        "yes_open",
        "open_yes_price",
        "initial_yes_price",
        "open_price",
        "initial_price",
    ):
        raw = market.get(field)
        if raw is None:
            continue
        try:
            price = float(raw)
            return price * 100 if price <= 1.0 else price
        except (TypeError, ValueError):
            continue
    return None

def extract_features(market, series):
    ticker = market.get("ticker") or ""
    if not ticker:
        return None
    result = (market.get("result") or "").upper()
    if result not in ("YES", "NO"):
        return None
    open_time = market.get("open_time") or ""
    close_time = market.get("close_time") or market.get("expiration_time") or ""
    open_price_yes = extract_open_price_yes(market)
    close_price_yes = None
    yes_bid, yes_ask = market.get("yes_bid"), market.get("yes_ask")
    if yes_bid is not None and yes_ask is not None:
        mid = (float(yes_bid) + float(yes_ask)) / 2
        close_price_yes = mid * 100 if mid <= 1.0 else mid
    strike_price = extract_strike_price(market)
    time_of_day = day_of_week = None
    if close_time:
        try:
            ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            time_of_day, day_of_week = ct.hour, ct.weekday()
        except Exception:
            pass
    return {
        "ticker": ticker, "series": series,
        "open_time": open_time, "close_time": close_time,
        "open_price_yes": open_price_yes, "close_price_yes": close_price_yes,
        "strike_price": strike_price,
        "spot_open_minus_strike": None, "spot_open_distance_pct": None,
        "spot_close_minus_strike": None, "spot_close_distance_pct": None,
        "settlement": result, "spot_open": None, "spot_close": None,
        "spot_return_pct": None, "time_of_day_utc": time_of_day,
        "day_of_week": day_of_week, "raw_json": json.dumps(market),
    }

def enrich_spot(features, series):
    symbol = COINBASE_SYMBOLS.get(series)
    if not symbol:
        return features
    spot_open = fetch_coinbase_candle(symbol, features["open_time"]) if features["open_time"] else None
    time.sleep(0.3)
    spot_close = fetch_coinbase_candle(symbol, features["close_time"]) if features["close_time"] else None
    time.sleep(0.3)
    features["spot_open"] = spot_open
    features["spot_close"] = spot_close
    if spot_open and spot_close and spot_open > 0:
        features["spot_return_pct"] = (spot_close - spot_open) / spot_open * 100
    strike = features.get("strike_price")
    if strike and strike > 0:
        if spot_open is not None:
            features["spot_open_minus_strike"] = spot_open - strike
            features["spot_open_distance_pct"] = (spot_open - strike) / strike
        if spot_close is not None:
            features["spot_close_minus_strike"] = spot_close - strike
            features["spot_close_distance_pct"] = (spot_close - strike) / strike
    return features

def insert_contract(conn, features):
    try:
        conn.execute("""
            INSERT OR IGNORE INTO contracts (
                ticker, series, open_time, close_time,
                open_price_yes, close_price_yes, settlement,
                strike_price,
                spot_open_minus_strike, spot_open_distance_pct,
                spot_close_minus_strike, spot_close_distance_pct,
                spot_open, spot_close, spot_return_pct,
                time_of_day_utc, day_of_week, collected_at, raw_json)
            VALUES (:ticker, :series, :open_time, :close_time,
                :open_price_yes, :close_price_yes, :settlement,
                :strike_price,
                :spot_open_minus_strike, :spot_open_distance_pct,
                :spot_close_minus_strike, :spot_close_distance_pct,
                :spot_open, :spot_close, :spot_return_pct,
                :time_of_day_utc, :day_of_week, :collected_at, :raw_json)
        """, {**features, "collected_at": datetime.now(timezone.utc).isoformat()})
        conn.commit()
        return conn.execute("SELECT changes()").fetchone()[0] > 0
    except Exception as e:
        log.error("DB insert error: %s", e)
        return False

def get_existing_tickers(conn):
    return {r[0] for r in conn.execute("SELECT ticker FROM contracts").fetchall()}

def print_stats(conn):
    rows = conn.execute("""
        SELECT series, COUNT(*),
            SUM(CASE WHEN settlement='YES' THEN 1 ELSE 0 END),
            SUM(CASE WHEN settlement='NO'  THEN 1 ELSE 0 END),
            AVG(close_price_yes), AVG(spot_return_pct)
        FROM contracts GROUP BY series""").fetchall()
    log.info("--- DB Stats ---")
    for series, total, yes_c, no_c, avg_close, avg_ret in rows:
        yes_pct = (yes_c / total * 100) if total else 0
        log.info("  %s: %d contracts | YES=%d(%.0f%%) NO=%d | avg_close=%.1fc | avg_spot_ret=%.2f%%",
            series, total, yes_c, yes_pct, no_c, avg_close or 0, avg_ret or 0)

def run_collection(conn):
    total_new = 0
    for series in SERIES:
        log.info("=== %s ===", series)
        new_count, errors = 0, []
        markets = []
        try:
            existing = get_existing_tickers(conn)
            markets = fetch_settled_markets(series)
            for market in markets:
                ticker = market.get("ticker") or ""
                if ticker in existing:
                    continue
                features = extract_features(market, series)
                if not features:
                    continue
                features = enrich_spot(features, series)
                if insert_contract(conn, features):
                    new_count += 1
                    log.info("  NEW %s | %s | spot_ret=%s%%", ticker, features["settlement"],
                        f"{features['spot_return_pct']:.2f}" if features["spot_return_pct"] is not None else "N/A")
                time.sleep(0.1)
        except Exception as e:
            log.error("[%s] error: %s", series, e)
            errors.append(str(e))
        conn.execute(
            "INSERT INTO collection_log (run_at, series, contracts_found, contracts_new, errors) VALUES (?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(), series, len(markets), new_count, json.dumps(errors) if errors else None))
        conn.commit()
        total_new += new_count
        log.info("[%s] %d new contracts", series, new_count)
    return total_new

def main():
    log.info("Data collector starting")
    conn = init_db(DB_PATH)
    run_collection(conn)
    print_stats(conn)
    while True:
        log.info("Sleeping 15 min...")
        time.sleep(POLL_INTERVAL_SECS)
        run_collection(conn)
        print_stats(conn)

if __name__ == "__main__":
    main()
