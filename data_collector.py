#!/usr/bin/env python3
"""
data_collector.py - Collects historical Kalshi contract data + Coinbase spot prices.
Stores in SQLite for probability model training. Runs every 15 minutes.
"""
import sqlite3, json, time, logging, sys, re
from datetime import datetime, timezone
from pathlib import Path
import urllib.request, urllib.error, urllib.parse

DB_PATH = Path(__file__).resolve().parent / "kalshi_data.db"
LOG_PATH = Path(__file__).resolve().parent / "data_collector.log"
POLL_INTERVAL_SECS = 900
SNAPSHOT_LOOP_SECS = 10
MAX_NEW_SETTLED_PER_SERIES_PER_CYCLE = 20
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            series TEXT NOT NULL,
            snapshot_sequence INTEGER NOT NULL,
            snapshot_ts TEXT NOT NULL,
            open_time TEXT,
            close_time TEXT,
            seconds_to_close REAL,
            yes_bid REAL,
            yes_ask REAL,
            no_bid REAL,
            no_ask REAL,
            mid_yes REAL,
            spread_yes REAL,
            target_price REAL,
            spot_price REAL,
            spot_minus_target REAL,
            spot_distance_pct REAL,
            raw_json TEXT,
            UNIQUE(ticker, snapshot_sequence))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_snapshots_ticker_ts ON market_snapshots(ticker, snapshot_ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_snapshots_series_ts ON market_snapshots(series, snapshot_ts)")
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
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        log.error("Kalshi error: %s", e)
        return {}

def parse_dt(timestamp_iso):
    if not timestamp_iso:
        return None
    try:
        return datetime.fromisoformat(timestamp_iso.replace("Z", "+00:00"))
    except Exception:
        return None

def parse_price_cents(market, dollars_field, cents_field):
    raw = market.get(dollars_field)
    if raw is not None:
        try:
            return float(raw) * 100.0
        except (TypeError, ValueError):
            pass
    raw = market.get(cents_field)
    if raw is not None:
        try:
            price = float(raw)
            return price * 100.0 if price <= 1.0 else price
        except (TypeError, ValueError):
            pass
    return None

def fetch_settled_markets(series):
    all_m, cursor = [], None
    while True:
        params = {"series_ticker": series, "status": "settled", "limit": "200"}
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

def fetch_active_markets(series):
    data = kalshi_get("markets", {"series_ticker": series, "limit": "100"})
    markets = data.get("markets") or []
    now = datetime.now(timezone.utc)
    active = []
    for market in markets:
        open_time = parse_dt(market.get("open_time"))
        close_time = parse_dt(market.get("close_time") or market.get("expiration_time"))
        if open_time and close_time and open_time <= now < close_time:
            active.append(market)
    active.sort(key=lambda market: market.get("close_time", ""))
    return active

def fetch_coinbase_candle(symbol, timestamp_iso):
    try:
        dt = datetime.fromisoformat(timestamp_iso.replace("Z", "+00:00"))
        ts = int(dt.timestamp())
        start = datetime.fromtimestamp(ts - 900, timezone.utc).isoformat().replace("+00:00", "Z")
        end = datetime.fromtimestamp(ts + 900, timezone.utc).isoformat().replace("+00:00", "Z")
        url = (f"https://api.exchange.coinbase.com/products/{symbol}/candles"
               f"?start={start}&end={end}&granularity=900")
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "kalshi-data-collector/1.0",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            candles = json.loads(r.read().decode())
            if candles and isinstance(candles, list):
                closest = min(candles, key=lambda candle: abs(int(candle[0]) - ts))
                return float(closest[4])
    except Exception as e:
        log.debug("Coinbase error for %s: %s", symbol, e)
    return None

def fetch_coinbase_ticker(symbol):
    try:
        url = f"https://api.exchange.coinbase.com/products/{symbol}/ticker"
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "kalshi-data-collector/1.0",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
            return float(data["price"])
    except Exception as e:
        log.debug("Coinbase ticker error for %s: %s", symbol, e)
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

def snapshot_interval_secs(seconds_to_close):
    if seconds_to_close > 600:
        return 120
    if seconds_to_close > 120:
        return 30
    return 10

def should_snapshot(last_ts, seconds_to_close):
    if last_ts is None:
        return True
    return time.time() - last_ts >= snapshot_interval_secs(seconds_to_close)

def next_snapshot_sequence(conn, ticker):
    row = conn.execute(
        "SELECT COALESCE(MAX(snapshot_sequence), 0) + 1 FROM market_snapshots WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    return int(row[0] or 1)

def build_snapshot(conn, market, series):
    ticker = market.get("ticker") or ""
    close_time = market.get("close_time") or market.get("expiration_time") or ""
    close_dt = parse_dt(close_time)
    if not ticker or not close_dt:
        return None
    seconds_to_close = max(0.0, (close_dt - datetime.now(timezone.utc)).total_seconds())
    yes_bid = parse_price_cents(market, "yes_bid_dollars", "yes_bid")
    yes_ask = parse_price_cents(market, "yes_ask_dollars", "yes_ask")
    no_bid = 100.0 - yes_ask if yes_ask is not None else None
    no_ask = 100.0 - yes_bid if yes_bid is not None else None
    mid_yes = (yes_bid + yes_ask) / 2.0 if yes_bid is not None and yes_ask is not None else None
    spread_yes = yes_ask - yes_bid if yes_bid is not None and yes_ask is not None else None
    target_price = extract_strike_price(market)
    spot_price = fetch_coinbase_ticker(COINBASE_SYMBOLS.get(series)) if COINBASE_SYMBOLS.get(series) else None
    spot_minus_target = None
    spot_distance_pct = None
    if target_price and target_price > 0 and spot_price is not None:
        spot_minus_target = spot_price - target_price
        spot_distance_pct = spot_minus_target / target_price
    return {
        "ticker": ticker,
        "series": series,
        "snapshot_sequence": next_snapshot_sequence(conn, ticker),
        "snapshot_ts": datetime.now(timezone.utc).isoformat(),
        "open_time": market.get("open_time") or "",
        "close_time": close_time,
        "seconds_to_close": seconds_to_close,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "mid_yes": mid_yes,
        "spread_yes": spread_yes,
        "target_price": target_price,
        "spot_price": spot_price,
        "spot_minus_target": spot_minus_target,
        "spot_distance_pct": spot_distance_pct,
        "raw_json": json.dumps(market),
    }

def insert_snapshot(conn, snapshot):
    conn.execute("""
        INSERT OR IGNORE INTO market_snapshots (
            ticker, series, snapshot_sequence, snapshot_ts,
            open_time, close_time, seconds_to_close,
            yes_bid, yes_ask, no_bid, no_ask, mid_yes, spread_yes,
            target_price, spot_price, spot_minus_target, spot_distance_pct,
            raw_json)
        VALUES (
            :ticker, :series, :snapshot_sequence, :snapshot_ts,
            :open_time, :close_time, :seconds_to_close,
            :yes_bid, :yes_ask, :no_bid, :no_ask, :mid_yes, :spread_yes,
            :target_price, :spot_price, :spot_minus_target, :spot_distance_pct,
            :raw_json)
    """, snapshot)
    conn.commit()
    return conn.execute("SELECT changes()").fetchone()[0] > 0

def collect_active_snapshots(conn, last_snapshot_by_ticker):
    inserted = 0
    for series in SERIES:
        try:
            for market in fetch_active_markets(series):
                ticker = market.get("ticker") or ""
                close_dt = parse_dt(market.get("close_time") or market.get("expiration_time"))
                if not ticker or not close_dt:
                    continue
                seconds_to_close = max(0.0, (close_dt - datetime.now(timezone.utc)).total_seconds())
                if not should_snapshot(last_snapshot_by_ticker.get(ticker), seconds_to_close):
                    continue
                snapshot = build_snapshot(conn, market, series)
                if snapshot and insert_snapshot(conn, snapshot):
                    last_snapshot_by_ticker[ticker] = time.time()
                    inserted += 1
                    log.info(
                        "SNAP %s seq=%d ttc=%.0fs mid=%s spot=%s dist=%s",
                        ticker,
                        snapshot["snapshot_sequence"],
                        snapshot["seconds_to_close"],
                        f"{snapshot['mid_yes']:.1f}c" if snapshot["mid_yes"] is not None else "N/A",
                        f"{snapshot['spot_price']:.2f}" if snapshot["spot_price"] is not None else "N/A",
                        f"{snapshot['spot_distance_pct']:.5f}" if snapshot["spot_distance_pct"] is not None else "N/A",
                    )
                time.sleep(0.1)
        except Exception as e:
            log.error("[%s] snapshot error: %s", series, e)
    return inserted

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
                    if new_count >= MAX_NEW_SETTLED_PER_SERIES_PER_CYCLE:
                        log.info("[%s] settled backfill cycle cap reached (%d)", series, new_count)
                        break
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
    last_snapshot_by_ticker = {}
    next_settled_collection_at = time.time()
    while True:
        collect_active_snapshots(conn, last_snapshot_by_ticker)
        if time.time() >= next_settled_collection_at:
            run_collection(conn)
            print_stats(conn)
            next_settled_collection_at = time.time() + POLL_INTERVAL_SECS
        time.sleep(SNAPSHOT_LOOP_SECS)

if __name__ == "__main__":
    main()
