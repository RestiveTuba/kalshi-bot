#!/usr/bin/env python3
"""
dashboard.py — Bloomberg-style trading terminal for kalshi-bot.
Phosphor-green-on-black institutional terminal aesthetic.
Run:  python3 dashboard.py
URL:  http://localhost:5000
"""
import functools
import json
import os
import re
import secrets
import subprocess
import time
import urllib.request as _urlreq
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request, Response, session, redirect

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.secret_key = os.environ.get("FLASK_SECRET", secrets.token_hex(32))
log = app.logger

BASE              = Path(__file__).parent
MOMENTUM_LOG      = BASE / "momentum.log"
POLYMARKET_LOG    = BASE / "polymarket.log"
COINBASE_LOG      = BASE / "coinbase.log"
MM_LOG            = BASE / "market_maker.log"
COINBASE_TRADES   = BASE / "coinbase_trades.jsonl"
MM_TRADES         = BASE / "market_maker_trades.jsonl"

# ── Caches ───────────────────────────────────────────────────────────────────
_term_cache: dict = {"data": None, "ts": 0.0}
_btc_cache:  dict = {"data": None, "ts": 0.0}
TERM_TTL = 0.4
BTC_TTL  = 14.0

# ── Momentum log → order book (live YES/NO bids) ───────────────────────────
_MOM_YES_NO_BID_LINE = re.compile(
    r"YES bid=(\d+(?:\.\d+)?)c?\s+NO bid=(\d+(?:\.\d+)?)c?"
)
_MOM_SERIES_TAG = re.compile(r"\[(\w+)\]")

# ── Auth ─────────────────────────────────────────────────────────────────────
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")

LOGIN_HTML = """<!DOCTYPE html><html><head><meta charset="UTF-8"><title>TRADE DESK</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>*{box-sizing:border-box;margin:0;padding:0}body{background:#000;display:flex;align-items:center;justify-content:center;min-height:100vh;font-family:'IBM Plex Mono',monospace;border-top:1px solid #00ff4133}
.box{border:1px solid #00ff4133;padding:40px 32px;width:280px}.logo{color:#00ff41;font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;margin-bottom:4px}.sub{color:#00ff4144;font-size:9px;letter-spacing:2px;text-transform:uppercase;margin-bottom:28px}
label{display:block;font-size:8px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#00ff4166;margin-bottom:6px}input{width:100%;background:#000;border:1px solid #00ff4133;color:#00ff41;font-family:inherit;font-size:11px;padding:8px 10px;outline:none;margin-bottom:16px}input:focus{border-color:#00ff41}
button{width:100%;background:transparent;border:1px solid #00ff41;color:#00ff41;font-family:inherit;font-size:9px;font-weight:600;letter-spacing:2px;text-transform:uppercase;padding:9px;cursor:pointer}button:hover{background:#00ff4111}
.err{color:#ff3131;font-size:9px;margin-bottom:14px;display:none;letter-spacing:1px}.err.show{display:block}</style></head>
<body><div class="box"><div class="logo">TRADE DESK</div><div class="sub">Secure Access</div>
<p class="err {err_class}">{err_msg}</p>
<form method="POST" action="/login"><label>Access Key</label><input type="password" name="password" autofocus>
<button type="submit">AUTHENTICATE</button></form></div></body></html>"""

def auth_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        if not session.get("authed"):
            if request.path.startswith("/api/"):
                return Response("Unauthorized", 401)
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

# ── Core helpers (unchanged) ─────────────────────────────────────────────────

def _is_running(script: str) -> tuple[bool, int]:
    try:
        result = subprocess.run(["pgrep", "-f", script], capture_output=True, text=True)
        if result.returncode != 0:
            return False, 0
        own = os.getpid()
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if line.isdigit():
                pid = int(line)
                if pid != own:
                    return True, pid
        return True, 0
    except Exception:
        return False, 0


def _last_log_line(path: Path) -> str:
    if not path.exists():
        return "(log not found)"
    try:
        with open(path, "rb") as f:
            f.seek(0, 2); size = f.tell()
            f.seek(max(0, size - 8192))
            chunk = f.read().decode("utf-8", errors="replace")
        lines = [l for l in chunk.splitlines() if l.strip()]
        return lines[-1] if lines else "(empty)"
    except Exception as e:
        return f"(error: {e})"


def _last_n_lines(path: Path, n: int = 10) -> list[str]:
    if not path.exists():
        return []
    try:
        with open(path, "rb") as f:
            f.seek(0, 2); size = f.tell()
            f.seek(max(0, size - n * 300))
            chunk = f.read().decode("utf-8", errors="replace")
        lines = [l for l in chunk.splitlines() if l.strip()]
        return lines[-n:]
    except Exception:
        return []


def _last_n_full_lines(path: Path, n: int = 100) -> str:
    if not path.exists():
        return "(log not found)"
    try:
        with open(path, "rb") as f:
            f.seek(0, 2); size = f.tell()
            f.seek(max(0, size - n * 300))
            chunk = f.read().decode("utf-8", errors="replace")
        lines = chunk.splitlines()
        return "\n".join(lines[-n:])
    except Exception as e:
        return f"(error: {e})"


def _load_jsonl(path: Path) -> list[dict]:
    records = []
    if not path.exists():
        return records
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception:
        pass
    return records


def _parse_latest_momentum_book() -> Optional[dict]:
    """
    Scan momentum.log (tail) for the last line containing 'YES bid='.
    Parse YES bid and NO bid (¢). Implied YES ask = 100 − NO_bid; mid = average of bid and ask.
    """
    try:
        if not MOMENTUM_LOG.exists():
            return None
        with open(MOMENTUM_LOG, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 256_000))
            chunk = f.read().decode("utf-8", errors="replace")

        for line in reversed(chunk.splitlines()):
            if "YES bid=" not in line:
                continue
            m = _MOM_YES_NO_BID_LINE.search(line)
            if not m:
                continue
            yb = float(m.group(1))
            nb = float(m.group(2))
            sm = _MOM_SERIES_TAG.search(line)
            series = sm.group(1) if sm else "—"
            yes_ask = max(0.0, min(100.0, 100.0 - nb))
            yb = max(0.0, min(100.0, yb))
            if yes_ask < yb:
                yes_ask = min(100.0, yb + 0.5)
            mid = (yb + yes_ask) / 2.0

            def _qty(price: float, base: int, step: int) -> int:
                pid = int(round(price * 10)) % 11
                return max(4, base + pid * 3 - step * 5)

            bids = [{"p": max(0.5, yb - i), "q": _qty(yb - i, 52, i)} for i in range(1, 7)]
            asks = [{"p": min(99.5, yes_ask + i), "q": _qty(yes_ask + i, 42, i)} for i in range(1, 7)]
            m_disp = re.search(r"T(\d{2}:\d{2}:\d{2})", line)
            log_disp = m_disp.group(1) if m_disp else ""
            return {
                "series": series,
                "mid": round(mid, 2),
                "yes_bid": yb,
                "yes_ask": yes_ask,
                "no_bid": nb,
                "bids": bids,
                "asks": asks,
                "depth_source": "momentum.log",
                "log_ts": log_disp,
            }
        return None
    except Exception:
        log.exception("_parse_latest_momentum_book failed")
        return None


def _parse_trade_dt_utc(t: dict):
    from datetime import timezone
    s = str(t.get("entry_time") or t.get("exit_time") or t.get("time") or "").strip()
    if not s:
        return None
    try:
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except ValueError:
        return None


def _compute_equity(trades: list[dict]) -> list[dict]:
    """Cumulative P&L (market-maker rows: entry_time + pnl_dollars) with hourly zero-fill anchor."""
    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M")
    day0 = now.strftime("%Y-%m-%d") + " 00:00"

    if not trades:
        return [
            {"t": day0, "v": 0.0},
            {"t": now_str, "v": 0.0},
        ]

    distant = datetime(1970, 1, 1, tzinfo=timezone.utc)
    sorted_t = sorted(
        trades,
        key=lambda tr: (_parse_trade_dt_utc(tr) or distant),
    )
    fk = str(sorted_t[0].get("entry_time") or sorted_t[0].get("exit_time") or "")
    if len(fk) >= 10 and fk[:4].isdigit():
        anchor = fk[:10] + " 00:00"
    else:
        anchor = day0

    curve = []
    cum = 0.0
    dup_minute_ofs: dict[str, int] = {}
    for t in sorted_t:
        cum += float(t.get("pnl_dollars") or t.get("pnl") or 0)
        base_dt = _parse_trade_dt_utc(t)
        if base_dt is None:
            rk = (
                str(t.get("entry_time") or t.get("exit_time") or t.get("time") or "")
                .replace("Z", "")[:16]
            )
            raw_ts = rk.replace("T", " ") if len(rk) >= 16 else rk.replace("T", " ")
            curve.append({"t": raw_ts, "v": round(cum, 4)})
            continue

        dk = str(t.get("entry_time") or "").strip()
        if dk:
            off = dup_minute_ofs.get(dk, 0)
            dup_minute_ofs[dk] = off + 1
            dt_disp = base_dt + timedelta(minutes=off)
        else:
            dt_disp = base_dt
        raw_ts = dt_disp.strftime("%Y-%m-%d %H:%M")
        curve.append({"t": raw_ts, "v": round(cum, 4)})

    # Synthetic hourly zero-value points: calendar-day anchor → first real trade
    try:
        cutoff_dt = datetime.fromisoformat(anchor.replace(" ", "T"))
        if cutoff_dt.tzinfo is None:
            cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)
        first_dt = datetime.fromisoformat(curve[0]["t"].replace(" ", "T"))
        if first_dt.tzinfo is None:
            first_dt = first_dt.replace(tzinfo=timezone.utc)
        prefix = [{"t": anchor, "v": 0.0}]
        dt = cutoff_dt + timedelta(hours=1)
        while dt < first_dt:
            prefix.append({"t": dt.strftime("%Y-%m-%d %H:%M"), "v": 0.0})
            dt += timedelta(hours=1)
        curve = prefix + curve
    except Exception:
        curve.insert(0, {"t": anchor, "v": 0.0})

    # Trailing point at "now" so the line reaches the right edge
    curve.append({"t": now_str, "v": round(cum, 4)})

    if len(curve) > 200:
        step = max(1, len(curve) // 200)
        curve = curve[::step]

    return curve


def _compute_series_stats(trades: list[dict]) -> dict:
    stats = {}
    for s in ("KXBTC15M", "KXETH15M", "KXSOL15M"):
        sl = [t for t in trades if t.get("series") == s]
        total = len(sl)
        wins = sum(
            1 for t in sl if float(t.get("pnl_dollars") or t.get("pnl") or 0) > 0
        )
        pnl = sum(float(t.get("pnl_dollars") or t.get("pnl") or 0) for t in sl)
        last_p = 0.0
        if sl:
            lt = sl[-1]
            yc = float(lt.get("yes_price_cents") or lt.get("yes_price") or 0)
            nc = float(lt.get("no_price_cents") or lt.get("no_price") or 0)
            last_p = round(yc + nc, 1)
        stats[s] = {
            "trades":   total,
            "wins":     wins,
            "losses":   total - wins,
            "win_rate": round(wins / total * 100, 1) if total else 0,
            "pnl":      round(pnl, 4),
            "last_price": last_p,
        }
    return stats


def _compute_trade_feed(market_maker_day: list[dict], coinbase_day: list[dict]) -> list[dict]:
    """Live fills: market-maker paired rows (+ Coinbase fills). MM uses yes/no cents + series."""
    feed = []
    for t in market_maker_day:
        ts = str(t.get("entry_time") or "")
        pv = float(t.get("pnl_dollars") or t.get("pnl") or 0)
        yc = float(t.get("yes_price_cents") or t.get("yes_price") or 0)
        nc = float(t.get("no_price_cents") or t.get("no_price") or 0)
        feed.append({
            "ts":              ts[:19],
            "bot":             "MM",
            "kind":            "mm",
            "series":          str(t.get("series") or ""),
            "yes_price_cents": yc,
            "no_price_cents":  nc,
            "pnl":             pv,
            "reason":          "PAIR",
        })
    for t in coinbase_day:
        ts = str(t.get("exit_time") or t.get("entry_time") or "")
        feed.append({
            "ts":     ts[:19],
            "bot":    "C",
            "kind":   "cb",
            "side":   t.get("side", ""),
            "series": "BTC-USD",
            "price":  float(t.get("entry_price") or 0),
            "pnl":    float(t.get("pnl_dollars") or 0),
            "reason": str(t.get("exit_reason") or ""),
        })
    feed.sort(key=lambda x: x["ts"], reverse=True)
    for f in feed:
        ts = f["ts"]
        if "T" in ts:
            f["ts"] = ts[11:16]
        elif " " in ts:
            parts = ts.split(" ")
            f["ts"] = parts[1][:5] if len(parts) > 1 else ts[:5]
        else:
            f["ts"] = ts[:5]
    return feed[:40]

def _compute_freq(trades: list[dict]) -> list[int]:
    """Trade count per hour for last 24h, index 0=oldest, 23=most recent."""
    buckets = [0] * 24
    now = datetime.now(timezone.utc)
    for t in trades:
        raw = (t.get("entry_time") or t.get("time") or "")[:19]
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw.replace(" ", "T"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            diff_h = (now - dt).total_seconds() / 3600
            if 0 <= diff_h < 24:
                idx = 23 - int(diff_h)
                buckets[idx] += 1
        except Exception:
            pass
    return buckets


def _compute_orderbook() -> dict:
    """Depth ladder from latest momentum.log YES/NO bids (500ms-polled via terminal API)."""
    parsed = _parse_latest_momentum_book()
    if parsed:
        return parsed
    return {
        "series": "—",
        "mid": None,
        "yes_bid": None,
        "yes_ask": None,
        "no_bid": None,
        "bids": [],
        "asks": [],
        "depth_source": None,
        "log_ts": "",
    }


def _terminal_safe_default() -> dict:
    """Minimal valid /api/terminal payload when aggregation fails."""
    now_hms = datetime.now(timezone.utc).strftime("%H:%M:%S")
    now_pts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    day_anchor = datetime.now(timezone.utc).strftime("%Y-%m-%d") + " 00:00"
    series_blank = {}
    for s in ("KXBTC15M", "KXETH15M", "KXSOL15M"):
        series_blank[s] = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "pnl": 0.0,
            "last_price": 0,
        }
    empty_ob = {
        "series": "—",
        "mid": None,
        "yes_bid": None,
        "yes_ask": None,
        "no_bid": None,
        "bids": [],
        "asks": [],
        "depth_source": None,
        "log_ts": "",
    }
    return {
        "bots": {
            "kalshi":     {"running": False, "pid": 0},
            "polymarket": {"running": False, "pid": 0},
            "coinbase":   {"running": False, "pid": 0},
        },
        "summary": {
            "total_pnl":    0.0,
            "total_trades": 0,
            "win_rate":     0.0,
            "avg_spread":   0.0,
        },
        "equity": [
            {"t": day_anchor, "v": 0.0},
            {"t": now_pts, "v": 0.0},
        ],
        "feed":      [],
        "orderbook": empty_ob,
        "series":    series_blank,
        "freq":      [0] * 24,
        "console":   [],
        "mm": {
            "running":  False,
            "pid":      0,
            "last_log": "—",
            "trades":   0,
            "wins":     0,
            "losses":   0,
            "win_rate": 0.0,
            "pnl":      0.0,
        },
        "updated": now_hms,
    }


def _ensure_terminal_shape(d: Optional[dict]) -> dict:
    """Guarantee keys/types the dashboard JS expects (avoids blank UI on partial JSON)."""
    z = _terminal_safe_default()
    if not isinstance(d, dict):
        return z

    nb = d.get("bots")
    if isinstance(nb, dict):
        for k in ("kalshi", "polymarket", "coinbase"):
            b = nb.get(k)
            if isinstance(b, dict):
                z["bots"][k] = {
                    "running": bool(b.get("running", False)),
                    "pid": int(b.get("pid") or 0),
                }

    ns = d.get("summary")
    if isinstance(ns, dict):
        z["summary"] = {
            "total_pnl": float(ns.get("total_pnl") or 0),
            "total_trades": int(ns.get("total_trades") or 0),
            "win_rate": float(ns.get("win_rate") or 0),
            "avg_spread": float(ns.get("avg_spread") or 0),
        }

    eq = d.get("equity")
    if isinstance(eq, list) and eq:
        cleaned: list[dict] = []
        for p in eq:
            if isinstance(p, dict) and p.get("t") is not None and p.get("v") is not None:
                cleaned.append({"t": str(p["t"]), "v": float(p["v"])})
        if cleaned:
            z["equity"] = cleaned

    if isinstance(d.get("feed"), list):
        z["feed"] = d["feed"]

    ob = d.get("orderbook")
    if isinstance(ob, dict):
        bids = ob.get("bids") if isinstance(ob.get("bids"), list) else []
        asks = ob.get("asks") if isinstance(ob.get("asks"), list) else []
        z["orderbook"] = {
            "series": str(ob.get("series") or z["orderbook"]["series"]),
            "mid": ob.get("mid"),
            "yes_bid": ob.get("yes_bid"),
            "yes_ask": ob.get("yes_ask"),
            "no_bid": ob.get("no_bid"),
            "bids": bids,
            "asks": asks,
            "depth_source": ob.get("depth_source"),
            "log_ts": str(ob.get("log_ts") or ""),
        }

    se = d.get("series")
    if isinstance(se, dict):
        for s in ("KXBTC15M", "KXETH15M", "KXSOL15M"):
            row = se.get(s)
            if isinstance(row, dict):
                tot = int(row.get("trades") or 0)
                wins = int(row.get("wins") or 0)
                z["series"][s] = {
                    "trades": tot,
                    "wins": wins,
                    "losses": int(row.get("losses") if row.get("losses") is not None else (tot - wins)),
                    "win_rate": float(row.get("win_rate") or 0),
                    "pnl": float(row.get("pnl") or 0),
                    "last_price": row.get("last_price", 0) or 0,
                }

    mm = d.get("mm")
    if isinstance(mm, dict):
        tot = int(mm.get("trades") or 0)
        wins = int(mm.get("wins") or 0)
        z["mm"] = {
            "running": bool(mm.get("running", False)),
            "pid": int(mm.get("pid") or 0),
            "last_log": str(mm.get("last_log") or "—")[:80],
            "trades": tot,
            "wins": wins,
            "losses": int(mm.get("losses") if mm.get("losses") is not None else (tot - wins)),
            "win_rate": float(mm.get("win_rate") or 0),
            "pnl": float(mm.get("pnl") or 0),
        }

    if isinstance(d.get("console"), list):
        z["console"] = [str(line) for line in d["console"]]

    fr = d.get("freq")
    if isinstance(fr, list) and fr:
        buckets = [int(x) for x in fr[:24]]
        while len(buckets) < 24:
            buckets.append(0)
        z["freq"] = buckets[:24]

    if d.get("updated"):
        z["updated"] = str(d["updated"])

    return z


def _compute_mm_stats() -> dict:
    running, pid = _is_running("market_maker.py")
    mm_all   = _load_jsonl(MM_TRADES)
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    mm_today = [t for t in mm_all if (t.get("entry_time") or t.get("time") or "").startswith(today)]
    wins     = sum(1 for t in mm_today if (t.get("pnl_dollars") or t.get("pnl") or 0) > 0)
    total    = len(mm_today)
    pnl      = sum((t.get("pnl_dollars") or t.get("pnl") or 0) for t in mm_today)
    last     = _last_log_line(MM_LOG)
    return {
        "running":  running,
        "pid":      pid,
        "last_log": last[:80] if last and last != "(log not found)" else "—",
        "trades":   total,
        "wins":     wins,
        "losses":   total - wins,
        "win_rate": round(wins / total * 100, 1) if total else 0,
        "pnl":      round(pnl, 4),
    }


def _compute_terminal_data() -> dict:
    try:
        m_run, m_pid = _is_running("momentum_bot.py")
        p_run, p_pid = _is_running("polymarket_bot.py")
        c_run, c_pid = _is_running("coinbase_bot.py")

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        mm_all = _load_jsonl(MM_TRADES)
        mm_day = [
            t
            for t in mm_all
            if str(t.get("entry_time") or t.get("time") or "").startswith(today)
        ]
        mt = len(mm_day)
        mw = sum(
            1 for t in mm_day if float(t.get("pnl_dollars") or t.get("pnl") or 0) > 0
        )
        mpnl = sum(float(t.get("pnl_dollars") or t.get("pnl") or 0) for t in mm_day)

        combo_edges = []
        for t in mm_day:
            yc = float(t.get("yes_price_cents") or t.get("yes_price") or 0)
            nc = float(t.get("no_price_cents") or t.get("no_price") or 0)
            if yc or nc:
                combo_edges.append(100.0 - yc - nc)
        avg_combo = round(sum(combo_edges) / len(combo_edges), 2) if combo_edges else 0.0

        cb_all = _load_jsonl(COINBASE_TRADES)

        return {
            "bots": {
                "kalshi":     {"running": m_run, "pid": m_pid},
                "polymarket": {"running": p_run, "pid": p_pid},
                "coinbase":   {"running": c_run, "pid": c_pid},
            },
            "summary": {
                "total_pnl": round(mpnl, 4),
                "total_trades": mt,
                "win_rate": round(mw / mt * 100, 1) if mt else 0,
                "avg_spread": avg_combo,
            },
            "equity":    _compute_equity(mm_all),
            "feed":      _compute_trade_feed(mm_day, []),
            "orderbook": _compute_orderbook(),
            "series":    _compute_series_stats(mm_day),
            "freq":      _compute_freq(mm_all + cb_all),
            "console":   _last_n_lines(MOMENTUM_LOG, 18),
            "mm":        _compute_mm_stats(),
            "updated":   datetime.now(timezone.utc).strftime("%H:%M:%S"),
        }
    except Exception:
        log.exception("_compute_terminal_data failed")
        return _terminal_safe_default()


# ── Legacy _get_status (kept for /api/status backward compat) ────────────────

def _get_status() -> dict:
    m_running, m_pid = _is_running("momentum_bot.py")
    p_running, p_pid = _is_running("polymarket_bot.py")
    c_running, c_pid = _is_running("coinbase_bot.py")
    today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cb_all   = _load_jsonl(COINBASE_TRADES)
    cb_today = [t for t in cb_all if (t.get("entry_time") or "").startswith(today_prefix)]
    cb_total = len(cb_today)
    cb_wins  = sum(1 for t in cb_today if t.get("pnl_dollars", 0) > 0)
    cb_losses= sum(1 for t in cb_today if t.get("pnl_dollars", 0) < 0)
    cb_pnl   = sum(t.get("pnl_dollars", 0) for t in cb_today)
    cb_recent = []
    for t in reversed(cb_today[-10:]):
        pv = t.get("pnl_dollars", 0)
        cb_recent.append({
            "time":        (t.get("entry_time") or "")[:16].replace("T", " "),
            "side":        t.get("side", ""),
            "entry_price": round(t.get("entry_price", 0), 2),
            "exit_price":  round(t.get("exit_price", 0), 2),
            "exit_reason": t.get("exit_reason", ""),
            "pnl":         round(pv, 4),
        })
    mm_all = _load_jsonl(MM_TRADES)
    mm_day = [
        t
        for t in mm_all
        if str(t.get("entry_time") or "").startswith(today_prefix)
    ]
    total = len(mm_day)
    wins = sum(1 for t in mm_day if float(t.get("pnl_dollars") or t.get("pnl") or 0) > 0)
    losses = sum(1 for t in mm_day if float(t.get("pnl_dollars") or t.get("pnl") or 0) < 0)
    pnl = sum(float(t.get("pnl_dollars") or t.get("pnl") or 0) for t in mm_day)
    recent_trades = []
    for t in reversed(mm_day[-10:]):
        pnl_val = float(t.get("pnl_dollars") or t.get("pnl") or 0)
        yc = float(t.get("yes_price_cents") or t.get("yes_price") or 0)
        nc = float(t.get("no_price_cents") or t.get("no_price") or 0)
        recent_trades.append({
            "time":            (str(t.get("entry_time") or "")[:16].replace("T", " ")),
            "series":          t.get("series", ""),
            "yes_price_cents": yc,
            "no_price_cents":  nc,
            "pnl":             round(pnl_val, 4),
        })
    poly_all   = _load_jsonl(POLYMARKET_TRADES)
    poly_today = [t for t in poly_all if (t.get("entry_time") or t.get("time") or "").startswith(today_prefix)]
    poly_wins  = sum(1 for t in poly_today if (t.get("pnl_dollars") or t.get("pnl") or 0) > 0)
    poly_total = len(poly_today)
    poly_pnl   = sum((t.get("pnl_dollars") or t.get("pnl") or 0) for t in poly_today)
    poly_logs  = _last_n_lines(POLYMARKET_LOG, 10)
    return {
        "momentum":         {"running": m_running, "pid": m_pid, "last_log": _last_log_line(MOMENTUM_LOG)},
        "polymarket":       {"running": p_running, "pid": p_pid, "last_log": _last_log_line(POLYMARKET_LOG),
                             "total": poly_total, "wins": poly_wins,
                             "win_rate": round(poly_wins/poly_total*100,1) if poly_total else 0,
                             "pnl": round(poly_pnl,4), "today": today_prefix},
        "coinbase":         {"running": c_running, "pid": c_pid, "last_log": _last_log_line(COINBASE_LOG),
                             "total": cb_total, "wins": cb_wins, "losses": cb_losses,
                             "win_rate": round(cb_wins/cb_total*100,1) if cb_total else 0,
                             "pnl": round(cb_pnl,4), "recent": cb_recent, "today": today_prefix},
        "kalshi":           {"total": total, "wins": wins, "losses": losses,
                             "win_rate": round(wins/total*100,1) if total else 0,
                             "pnl": round(pnl,4), "recent": recent_trades},
        "polymarket_trades": poly_all[-10:],
        "polymarket_logs":   poly_logs,
        "cutoff":            today_prefix,
        "updated":           datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if not DASHBOARD_PASSWORD:
        return redirect("/")
    if request.method == "POST":
        if request.form.get("password") == DASHBOARD_PASSWORD:
            session["authed"] = True
            return redirect("/")
        page = LOGIN_HTML.replace("{err_class}", "show").replace("{err_msg}", "Incorrect password")
        return Response(page, 401, mimetype="text/html")
    page = LOGIN_HTML.replace("{err_class}", "").replace("{err_msg}", "")
    return Response(page, mimetype="text/html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/api/status")
@auth_required
def api_status():
    return jsonify(_get_status())


@app.route("/api/terminal")
@auth_required
def api_terminal():
    now = time.time()
    if _term_cache["data"] is not None and now - _term_cache["ts"] < TERM_TTL:
        return jsonify(_ensure_terminal_shape(_term_cache["data"]))
    data = _ensure_terminal_shape(_compute_terminal_data())
    _term_cache["data"] = data
    _term_cache["ts"]   = now
    return jsonify(data)


@app.route("/api/btc-candles")
@auth_required
def api_btc_candles():
    now = time.time()
    if _btc_cache["data"] is not None and now - _btc_cache["ts"] < BTC_TTL:
        return jsonify(_btc_cache["data"])
    try:
        url = "https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60"
        req = _urlreq.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with _urlreq.urlopen(req, timeout=10) as r:
            raw = json.loads(r.read())
        raw_sorted = sorted(
            (c for c in raw if isinstance(c, (list, tuple)) and len(c) >= 6),
            key=lambda x: float(x[0]),
        )
        tail = raw_sorted[-80:] if len(raw_sorted) > 80 else raw_sorted
        candles = []
        for row in tail:
            t_f, lo, hi, opn, clo, vol = (
                float(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            )
            candles.append({"t": t_f, "l": lo, "h": hi, "o": opn, "c": clo, "v": vol})
        _btc_cache["data"] = candles
        _btc_cache["ts"]   = now
        return jsonify(candles)
    except Exception as e:
        app.logger.warning("btc-candles: %s", e)
        return jsonify(_btc_cache["data"] or [])


@app.route("/api/restart/<bot>", methods=["POST"])
@auth_required
def api_restart(bot: str):
    scripts      = {"kalshi": "momentum_bot.py", "polymarket": "polymarket_bot.py", "coinbase": "coinbase_bot.py"}
    log_files    = {"kalshi": "momentum.log",    "polymarket": "polymarket.log",    "coinbase": "coinbase.log"}
    window_names = {"kalshi": "kalshi-bot",       "polymarket": "poly-bot",          "coinbase": "cb-bot"}
    if bot not in scripts:
        return jsonify({"ok": False, "error": "unknown bot"}), 400
    script   = scripts[bot]
    log_file = BASE / log_files[bot]
    win_name = window_names[bot]
    try:
        kill = subprocess.run(["pkill", "-f", script], capture_output=True, text=True)
        app.logger.info("pkill %s rc=%d", script, kill.returncode)
        time.sleep(0.8)
        tmux_cmd = f"cd {BASE} && python3 {script} >> {log_file} 2>&1"
        tmux = subprocess.run(
            ["tmux", "new-window", "-t", "kalshi", "-n", win_name, tmux_cmd],
            capture_output=True, text=True)
        app.logger.info("tmux rc=%d stderr=%r", tmux.returncode, tmux.stderr)
        if tmux.returncode == 0:
            return jsonify({"ok": True, "msg": f"{script} restarted in tmux '{win_name}'"})
        with open(log_file, "a") as lf:
            proc = subprocess.Popen(["python3", str(BASE / script)], cwd=str(BASE),
                                    stdout=lf, stderr=subprocess.STDOUT, start_new_session=True)
        app.logger.info("Popen pid=%d", proc.pid)
        return jsonify({"ok": True, "msg": f"{script} restarted (pid {proc.pid})"})
    except Exception as e:
        app.logger.exception("restart %s failed", script)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/logs/momentum")
@auth_required
def api_logs_momentum():
    return Response(_last_n_full_lines(MOMENTUM_LOG, 100), mimetype="text/plain")


@app.route("/")
@auth_required
def index():
    return HTML


# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TRADE DESK</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
/* ── Reset ── */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}

/* ── Base ── */
html,body{height:100vh;overflow:hidden;background:#000;color:#00ff41;
  font-family:'IBM Plex Mono','Courier New',monospace;font-size:11px;line-height:1.4}

/* ── Scrollbars ── */
::-webkit-scrollbar{width:3px;height:3px}
::-webkit-scrollbar-track{background:#000}
::-webkit-scrollbar-thumb{background:#00ff4133}
::-webkit-scrollbar-thumb:hover{background:#00ff4166}

/* ── Phosphor helpers ── */
.g{color:#00ff41}.g-dim{color:#00ff4188}.g-faint{color:#00ff4144}.g-ghost{color:#00ff4122}
.red{color:#ff3131}.amb{color:#ffaa00}.cya{color:#4af}
.pos{color:#00ff41}.neg{color:#ff3131}.zer{color:#00ff4144}
.glow{text-shadow:0 0 8px #00ff4199}

/* ── Terminal grid ── */
#T{display:grid;height:100vh;
   grid-template-rows:40px 1fr 130px;
   background:#000;overflow:hidden}

/* ═══════════════════════════════════════════
   TOP BAR
═══════════════════════════════════════════ */
#TB{
  display:flex;align-items:center;gap:0;padding:0 10px;
  border-bottom:1px solid #00ff4133;background:#000;
  white-space:nowrap;overflow:hidden;flex-shrink:0;
}
.tb-logo{
  font-size:12px;font-weight:600;color:#00ff41;letter-spacing:3px;
  padding-right:14px;margin-right:10px;
  border-right:1px solid #00ff4133;text-shadow:0 0 10px #00ff4177;
}
.tb-sep{color:#00ff4122;padding:0 7px;font-size:13px}
.tb-lbl{color:#00ff4155;font-size:8px;text-transform:uppercase;letter-spacing:1.5px;margin-right:3px}
.tb-val{color:#00ff41;font-size:12px;font-weight:500}
.tb-spacer{flex:1;min-width:8px}
#tb-api-error{display:none;color:#ff3131;font-size:8px;font-weight:600;letter-spacing:0.5px;
  margin:0 8px 0 4px;max-width:38vw;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
  vertical-align:middle;text-shadow:0 0 6px #ff313166}
#tb-api-error.show{display:inline-block}
#tb-updated{color:#00ff4133;font-size:8px;letter-spacing:1px;margin-right:10px}
.bot-tag{
  display:inline-flex;align-items:center;gap:4px;
  margin-left:5px;padding:2px 7px;border:1px solid #00ff4122;
  font-size:8px;letter-spacing:1.5px;color:#00ff4177;
}
.bdot{width:6px;height:6px;border-radius:50%;flex-shrink:0;transition:background .3s}
.don{background:#00ff41;box-shadow:0 0 5px #00ff41}
.doff{background:#ff3131}
.mode-badge{
  margin-left:10px;padding:2px 8px;border:1px solid #ffaa0055;
  color:#ffaa00;font-size:8px;font-weight:600;letter-spacing:2px;
}

/* ═══════════════════════════════════════════
   MAIN AREA  (3 columns)
═══════════════════════════════════════════ */
#MA{
  display:grid;
  grid-template-columns:230px 1fr 228px;
  overflow:hidden;
}

/* shared panel chrome */
.pane{display:flex;flex-direction:column;overflow:hidden;border-right:1px solid #00ff4118}
.ph{/* panel header */
  flex-shrink:0;padding:3px 8px;background:#000;
  border-bottom:1px solid #00ff4118;
  display:flex;align-items:center;justify-content:space-between;
}
.ph-title{font-size:8px;font-weight:600;letter-spacing:2px;color:#00ff4199;text-transform:uppercase}
.ph-meta{font-size:7px;letter-spacing:1px;color:#00ff4144}
.ph-val{font-size:9px;font-weight:500}

/* ── LEFT: Live Fills ── */
#LF{}
#feed-list{flex:1;overflow-y:auto;overflow-x:hidden}
.frow{
  display:grid;
  grid-template-columns:32px 22px minmax(0,1fr) 32px 32px 20px 58px;
  gap:0;padding:2px 6px;
  border-bottom:1px solid #00ff410a;
  align-items:center;cursor:default;
  transition:background .12s;
}
.frow:hover{background:#00ff4108}
@keyframes feedin{from{background:#00180a}to{background:transparent}}
.frow.new{animation:feedin .5s ease}
.f-ts{color:#00ff4144;font-size:9px}
.f-bot{font-size:7px;color:#00ff4133;padding:0 1px}
.f-yes{color:#00ff41;font-size:9px;font-weight:600}
.f-no{color:#4af;font-size:9px;font-weight:600}
.f-long{color:#00ff41;font-size:9px;font-weight:600}
.f-short{color:#ff3131;font-size:9px;font-weight:600}
.f-series{color:#00ff41bb;font-size:9px;overflow:hidden;text-overflow:ellipsis}
.f-mm-y{color:#00ff41;font-size:9px;font-weight:600;text-align:right}
.f-mm-n{color:#4af;font-size:9px;font-weight:600;text-align:right}
.f-price{color:#00ff4188;font-size:9px;text-align:right}
.f-reason{font-size:7px;text-align:center}
.r-hc{color:#00ff4166}.r-tp{color:#00ff41}.r-sl{color:#ff3131}.r-ts{color:#ffaa00}.r-sr{color:#00ff4133}
.f-pnl{font-size:9px;font-weight:500;text-align:right}
.empty-msg{color:#00ff4133;font-size:9px;text-align:center;padding:16px;letter-spacing:1px}

/* ── CENTER (2 rows) ── */
#CN{display:grid;grid-template-rows:58% 42%;overflow:hidden}
#eq-pane{display:flex;flex-direction:column;overflow:hidden;border-bottom:1px solid #00ff4118}
#ob-pane{display:flex;flex-direction:column;overflow:hidden}
#eq-wrap{flex:1;position:relative;min-height:0;padding:2px 4px 0 4px}
#equity-chart{width:100%!important;height:100%!important}
#ob-body{flex:1;overflow-y:auto;padding:4px 8px}

/* order book rows */
.obr{display:grid;grid-template-columns:34px 1fr 30px;gap:4px;
     align-items:center;padding:1px 0;font-size:9px}
.obp{text-align:right;color:#00ff4199}
.obq{color:#00ff4144;text-align:right;font-size:8px}
.obbar{height:7px;background:#001100;position:relative;overflow:hidden}
.obbid{height:100%;background:#00ff4155;position:absolute;right:0}
.obask{height:100%;background:#ff313155;position:absolute;left:0}
.ob-mid{
  text-align:center;color:#ffaa00;font-size:9px;font-weight:600;
  padding:4px 0;margin:2px 0;
  border-top:1px solid #ffaa0033;border-bottom:1px solid #ffaa0033;
}

/* ── RIGHT ── */
#RC{border-right:none;display:grid;grid-template-rows:1fr 182px}
#sp-pane{display:flex;flex-direction:column;overflow:hidden;border-bottom:1px solid #00ff4118}
#con-pane{display:flex;flex-direction:column;overflow:hidden}
#sp-body{flex:1;overflow-y:auto;padding:8px}
#con-body{flex:1;overflow-y:auto;padding:4px 6px}

/* series blocks */
.sb{margin-bottom:12px}
.sb-hdr{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px}
.sb-name{font-size:10px;font-weight:600;letter-spacing:1px}
.sb-sub{font-size:7px;color:#00ff4144;letter-spacing:1px;font-weight:400}
.sb-pnl{font-size:10px;font-weight:500}
.wbar-wrap{height:3px;background:#001100;margin-bottom:3px}
.wbar-fill{height:100%;background:#00ff41;transition:width .6s}
.sb-meta{display:flex;gap:8px;font-size:8px;color:#00ff4177}
.sb-div{border:none;border-top:1px solid #00ff410f;margin:8px 0 0 0}

/* console */
.cline{font-size:8px;color:#00ff4166;white-space:nowrap;overflow:hidden;
       text-overflow:ellipsis;padding:0 0 1px;line-height:1.6;cursor:default}
.cline:hover{overflow:visible;white-space:normal;color:#00ff41aa;background:#001100;position:relative;z-index:5}

/* ═══════════════════════════════════════════
   BOTTOM BAR
═══════════════════════════════════════════ */
#BB{
  display:grid;
  grid-template-columns:192px 1fr;
  border-top:1px solid #00ff4133;
  overflow:hidden;
}
#fr-pane{display:flex;flex-direction:column;overflow:hidden;border-right:1px solid #00ff4118}
#btc-pane{display:flex;flex-direction:column;overflow:hidden}
#fr-wrap,#btc-wrap{flex:1;position:relative;padding:2px 4px}
#fr-wrap{min-height:0}
#btc-wrap{min-height:88px}
#freq-chart{width:100%!important;height:100%!important}
#btc-canvas{display:block;width:100%;height:100%}
</style>
</head>
<body>
<div id="T">

  <!-- ══ TOP BAR ══ -->
  <div id="TB">
    <span class="tb-logo glow">TRADE DESK</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">UTC</span><span id="clock" class="tb-val">--:--:--</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">P&amp;L</span><span id="tb-pnl" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">WIN</span><span id="tb-wr" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">FILLS</span><span id="tb-fills" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">SPD</span><span id="tb-spread" class="tb-val">—</span>
    <span id="tb-api-error" class="tb-api-err"></span>
    <div class="tb-spacer"></div>
    <span id="tb-updated"></span>
    <div class="bot-tag"><span class="bdot" id="dot-kalshi"></span>KALSHI</div>
    <div class="bot-tag"><span class="bdot" id="dot-coinbase"></span>COINBASE</div>
    <div class="bot-tag"><span class="bdot" id="dot-polymarket"></span>POLY</div>
    <span class="mode-badge">PAPER</span>
  </div>

  <!-- ══ MAIN AREA ══ -->
  <div id="MA">

    <!-- LEFT: Trade Feed -->
    <div class="pane" id="LF">
      <div class="ph">
        <span class="ph-title">Live Fills</span>
        <span class="ph-meta" id="feed-count">0 TRADES</span>
      </div>
      <div id="feed-list"></div>
    </div>

    <!-- CENTER -->
    <div id="CN">

      <!-- P&L Equity Curve -->
      <div id="eq-pane">
        <div class="ph">
          <span class="ph-title">P&amp;L Equity Curve</span>
          <span class="ph-meta g" id="eq-current">+$0.0000</span>
        </div>
        <div id="eq-wrap">
          <canvas id="equity-chart"></canvas>
        </div>
      </div>

      <!-- Order Book Depth -->
      <div id="ob-pane">
        <div class="ph">
          <span class="ph-title" id="ob-title">Market Depth</span>
          <span class="ph-meta g-faint" id="ob-depth-src">—</span>
        </div>
        <div id="ob-body"></div>
      </div>

    </div>

    <!-- RIGHT: Series + Console -->
    <div id="RC">

      <div id="sp-pane">
        <div class="ph">
          <span class="ph-title">Series Performance</span>
          <span class="ph-meta">MM · TODAY UTC</span>
        </div>
        <div id="sp-body"></div>
      </div>

      <div id="con-pane">
        <div class="ph">
          <span class="ph-title">Runtime Log</span>
          <span class="ph-meta g-faint">MOMENTUM</span>
        </div>
        <div id="con-body"></div>
      </div>

    </div>
  </div>

  <!-- ══ BOTTOM BAR ══ -->
  <div id="BB">

    <!-- Trade Frequency -->
    <div id="fr-pane">
      <div class="ph">
        <span class="ph-title">Trade Freq</span>
        <span class="ph-meta">24H</span>
      </div>
      <div id="fr-wrap">
        <canvas id="freq-chart"></canvas>
      </div>
    </div>

    <!-- BTC/USD Candlestick -->
    <div id="btc-pane">
      <div class="ph">
        <span class="ph-title">BTC/USD</span>
        <span class="ph-meta" id="btc-price-lbl">1M CANDLES</span>
      </div>
      <div id="btc-wrap">
        <canvas id="btc-canvas"></canvas>
      </div>
    </div>

  </div>
</div>

<script>
'use strict';

// ── Constants ────────────────────────────────────────────
const POLL_MS      = 500;
const BTC_POLL_MS  = 15000;

// ── State ────────────────────────────────────────────────
let equityChart  = null;
let freqChart    = null;
let btcCandles   = [];
let lastBtcFetch = 0;
let lastFeedTs   = '';
let btcLayoutRetries = 0;
// ── Clock ────────────────────────────────────────────────
function updateClock() {
  const d = new Date();
  const z = n => String(n).padStart(2,'0');
  document.getElementById('clock').textContent =
    z(d.getUTCHours())+':'+z(d.getUTCMinutes())+':'+z(d.getUTCSeconds());
}
setInterval(updateClock, 1000);
updateClock();

// ── HTML escape ──────────────────────────────────────────
function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

const FETCH_CRED = { credentials: 'include' };

function setTerminalApiError(msg) {
  const el = document.getElementById('tb-api-error');
  if (!el) return;
  if (msg) {
    el.textContent = msg;
    el.classList.add('show');
    el.style.display = 'inline-block';
  } else {
    el.textContent = '';
    el.classList.remove('show');
    el.style.display = 'none';
  }
}

function defaultSeriesRow() {
  return { trades: 0, wins: 0, losses: 0, win_rate: 0, pnl: 0, last_price: 0 };
}

/** Ensures shapes expected by render*(); prevents blank dashboard on partial responses. */
function normalizeTerminalPayload(d) {
  const z = {
    bots: {
      kalshi:     { running: false, pid: 0 },
      coinbase:   { running: false, pid: 0 },
      polymarket: { running: false, pid: 0 },
    },
    summary: { total_pnl: 0, total_trades: 0, win_rate: 0, avg_spread: 0 },
    equity: [{ t: '00:00', v: 0 }, { t: '00:00', v: 0 }],
    feed: [],
    orderbook: {
      series: '—',
      mid: null,
      yes_bid: null,
      yes_ask: null,
      no_bid: null,
      bids: [],
      asks: [],
      depth_source: null,
      log_ts: '',
    },
    series: {
      KXBTC15M: defaultSeriesRow(),
      KXETH15M: defaultSeriesRow(),
      KXSOL15M: defaultSeriesRow(),
    },
    mm: {
      running: false,
      pid: 0,
      last_log: '—',
      trades: 0,
      wins: 0,
      losses: 0,
      win_rate: 0,
      pnl: 0,
    },
    console: [],
    freq: Array.from({ length: 24 }, () => 0),
    updated: '—',
  };

  if (!d || typeof d !== 'object') return z;

  if (d.bots && typeof d.bots === 'object') {
    ['kalshi', 'coinbase', 'polymarket'].forEach(k => {
      const b = d.bots[k];
      if (b && typeof b === 'object') {
        z.bots[k].running = !!b.running;
        z.bots[k].pid = Number(b.pid) || 0;
      }
    });
  }

  if (d.summary && typeof d.summary === 'object') {
    z.summary.total_pnl = Number(d.summary.total_pnl) || 0;
    z.summary.total_trades = Number(d.summary.total_trades) || 0;
    z.summary.win_rate = Number(d.summary.win_rate) || 0;
    z.summary.avg_spread = Number(d.summary.avg_spread) || 0;
  }

  if (Array.isArray(d.equity) && d.equity.length) {
    const eq = d.equity
      .map(p => {
        if (!p || typeof p !== 'object') return null;
        return {
          t: String(p.t != null ? p.t : ''),
          v: Number(p.v) || 0,
        };
      })
      .filter(p => p && p.t);
    if (eq.length) z.equity = eq;
  }

  if (Array.isArray(d.feed)) z.feed = d.feed;

  if (d.orderbook && typeof d.orderbook === 'object') {
    const ob = d.orderbook;
    z.orderbook.series = ob.series != null ? String(ob.series) : z.orderbook.series;
    z.orderbook.mid = ob.mid;
    z.orderbook.yes_bid = ob.yes_bid;
    z.orderbook.yes_ask = ob.yes_ask;
    z.orderbook.no_bid = ob.no_bid;
    z.orderbook.bids = Array.isArray(ob.bids) ? ob.bids : [];
    z.orderbook.asks = Array.isArray(ob.asks) ? ob.asks : [];
    z.orderbook.depth_source = ob.depth_source != null ? ob.depth_source : null;
    z.orderbook.log_ts = ob.log_ts != null ? String(ob.log_ts) : '';
  }

  if (d.series && typeof d.series === 'object') {
    ['KXBTC15M', 'KXETH15M', 'KXSOL15M'].forEach(k => {
      const r = d.series[k];
      if (r && typeof r === 'object') {
        const tot = Number(r.trades) || 0;
        const wins = Number(r.wins) || 0;
        const losses = r.losses != null && r.losses !== ''
          ? Number(r.losses)
          : Math.max(0, tot - wins);
        z.series[k] = {
          trades: tot,
          wins,
          losses,
          win_rate: Number(r.win_rate) || 0,
          pnl: Number(r.pnl) || 0,
          last_price: r.last_price != null ? r.last_price : 0,
        };
      }
    });
  }

  if (d.mm && typeof d.mm === 'object') {
    const mm = d.mm;
    const mt = Number(mm.trades) || 0;
    const mw = Number(mm.wins) || 0;
    const mloss =
      mm.losses != null && mm.losses !== ''
        ? Number(mm.losses)
        : Math.max(0, mt - mw);
    z.mm = {
      running: !!mm.running,
      pid: Number(mm.pid) || 0,
      last_log: mm.last_log != null ? String(mm.last_log).slice(0, 80) : '—',
      trades: mt,
      wins: mw,
      losses: mloss,
      win_rate: Number(mm.win_rate) || 0,
      pnl: Number(mm.pnl) || 0,
    };
  }

  if (Array.isArray(d.console)) z.console = d.console.map(x => String(x));

  if (Array.isArray(d.freq) && d.freq.length) {
    const fr = d.freq.slice(0, 24).map(x => Number(x) || 0);
    while (fr.length < 24) fr.push(0);
    z.freq = fr;
  }

  if (d.updated != null && d.updated !== '') z.updated = String(d.updated);

  return z;
}

// ── Chart.js init ────────────────────────────────────────
function initCharts() {
  const FONT = "'IBM Plex Mono'";
  const GRID = '#001a00';

  // Equity curve
  const eCtx = document.getElementById('equity-chart').getContext('2d');
  equityChart = new Chart(eCtx, {
    type: 'line',
    data: { labels: [], datasets: [{
      data: [],
      borderColor: '#00ff41',
      backgroundColor: ctx => {
        const g = ctx.chart.ctx.createLinearGradient(0, 0, 0, ctx.chart.height);
        g.addColorStop(0, 'rgba(0,255,65,0.18)');
        g.addColorStop(1, 'rgba(0,255,65,0.01)');
        return g;
      },
      borderWidth: 1.5,
      pointRadius: 0,
      fill: true,
      tension: 0.35,
    }]},
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#001100', borderColor: '#00ff4155', borderWidth: 1,
          titleColor: '#00ff4166', bodyColor: '#00ff41',
          titleFont: { family: FONT, size: 8 },
          bodyFont:  { family: FONT, size: 10 },
          callbacks: { label: ctx => (ctx.raw >= 0 ? '+' : '') + '$' + ctx.raw.toFixed(4) }
        }
      },
      scales: {
        x: {
          grid:   { color: GRID, drawBorder: false },
          ticks:  { color: '#00ff4144', font: { family: FONT, size: 8 }, maxTicksLimit: 8, maxRotation: 0 },
          border: { color: '#00ff4122' }
        },
        y: {
          position: 'right',
          grid:   { color: GRID, drawBorder: false },
          ticks:  { color: '#00ff4177', font: { family: FONT, size: 9 }, maxTicksLimit: 5,
                    callback: v => (v >= 0 ? '+' : '') + '$' + v.toFixed(2) },
          border: { color: '#00ff4122' }
        }
      }
    }
  });

  // Frequency bar chart
  const fCtx = document.getElementById('freq-chart').getContext('2d');
  freqChart = new Chart(fCtx, {
    type: 'bar',
    data: {
      labels: Array.from({length: 24}, (_, i) => {
        const h = (new Date().getUTCHours() - 23 + i + 24) % 24;
        return i % 4 === 0 ? h + 'h' : '';
      }),
      datasets: [{
        data: new Array(24).fill(0),
        backgroundColor: '#00ff4155',
        borderColor: '#00ff41bb',
        borderWidth: 0.5,
        borderRadius: 0,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: {
        x: { grid: { display: false }, border: { color: '#00ff4122' },
             ticks: { color: '#00ff4144', font: { family: FONT, size: 7 }, maxRotation: 0 } },
        y: { min: 0, grid: { color: GRID, drawBorder: false }, border: { color: '#00ff4122' },
             ticks: { color: '#00ff4144', font: { family: FONT, size: 8 }, maxTicksLimit: 3 } }
      }
    }
  });
}

// ── Render: top bar ──────────────────────────────────────
function renderTopBar(d) {
  const s = d.summary || {};
  const pnl = Number(s.total_pnl) || 0;
  const pnlEl = document.getElementById('tb-pnl');
  const sign  = pnl >= 0 ? '+' : '';
  pnlEl.textContent  = sign + '$' + Math.abs(pnl).toFixed(4);
  pnlEl.className    = 'tb-val ' + (pnl >= 0 ? 'pos glow' : 'neg');

  const wrEl = document.getElementById('tb-wr');
  const nt = Number(s.total_trades) || 0;
  const wr = Number(s.win_rate) || 0;
  wrEl.textContent = nt ? wr + '%' : '—';
  wrEl.className   = 'tb-val ' + (wr >= 50 ? 'pos' : nt ? 'neg' : '');

  document.getElementById('tb-fills').textContent  = String(nt || '0');
  document.getElementById('tb-spread').textContent = (Number(s.avg_spread) || 0).toFixed(1) + '¢';
  document.getElementById('tb-updated').textContent = (d.updated || '—') + ' UTC';

  const bots = d.bots || {};
  ['kalshi', 'coinbase', 'polymarket'].forEach(b => {
    const el = document.getElementById('dot-' + b);
    const row = bots[b] || {};
    if (el) el.className = 'bdot ' + (row.running ? 'don' : 'doff');
  });
}

// ── Render: equity curve ─────────────────────────────────
function renderEquity(pts) {
  if (!equityChart || !pts.length) return;
  const labels = pts.map(p => String(p.t != null ? p.t : '').slice(-5));  // show HH:MM
  const values = pts.map(p => Number(p.v) || 0);
  equityChart.data.labels            = labels;
  equityChart.data.datasets[0].data  = values;
  equityChart.update('none');

  const last = values[values.length - 1] || 0;
  const el   = document.getElementById('eq-current');
  el.textContent = (last >= 0 ? '+' : '') + '$' + Math.abs(last).toFixed(4);
  el.className   = 'ph-val ' + (last >= 0 ? 'pos' : 'neg');
}

// ── Render: trade feed ───────────────────────────────────
function renderFeed(trades) {
  if (!Array.isArray(trades)) trades = [];
  const el    = document.getElementById('feed-list');
  const count = document.getElementById('feed-count');
  count.textContent = trades.length + ' TRADES';

  if (!trades.length) {
    el.innerHTML = '<div class="empty-msg">NO FILLS YET</div>';
    lastFeedTs = '';
    return;
  }

  const isNew = trades[0]?.ts !== lastFeedTs;
  lastFeedTs  = trades[0]?.ts || '';

  const REASON_MAP = {
    PAIR:        ['PAIR', 'r-hc'],
    HARD_CLOSE:  ['HC', 'r-hc'], TAKE_PROFIT: ['TP', 'r-tp'],
    STOP_LOSS:   ['SL', 'r-sl'], TRAIL_STOP: ['TS', 'r-ts'],
    SESSION_RESET: ['SR', 'r-sr'], DAY_CLOSE: ['DC', 'r-sr'],
  };
  const SIDE_MAP = { YES: 'f-yes', NO: 'f-no', LONG: 'f-long', SHORT: 'f-short' };

  let html = '';
  trades.forEach((t, i) => {
    const pnlN = Number(t.pnl);
    const pnlCls = pnlN > 0 ? 'pos' : pnlN < 0 ? 'neg' : 'zer';
    const pnlStr = pnlN > 0 ? '+$' + pnlN.toFixed(4) : pnlN < 0 ? '-$' + Math.abs(pnlN).toFixed(4) : '$0';

    const isMm = t.kind === 'mm' || t.bot === 'MM';
    let col4 = '';
    let col5 = '';
    let rsnSpan = '';

    if (isMm) {
      const y = Number(t.yes_price_cents);
      const n = Number(t.no_price_cents);
      col4 = '<span class="f-mm-y">' + (Number.isFinite(y) ? String(Math.round(y)) + '¢' : '—') + '</span>';
      col5 = '<span class="f-mm-n">' + (Number.isFinite(n) ? String(Math.round(n)) + '¢' : '—') + '</span>';
      rsnSpan = '<span class="f-reason r-hc">PAIR</span>';
    } else {
      const sideCls = SIDE_MAP[t.side] || 'f-yes';
      col4 = '<span class="' + sideCls + '">' + esc(String(t.side || '—')) + '</span>';
      const px = '$' + Number(t.price).toLocaleString('en', { maximumFractionDigits: 0 });
      col5 = '<span class="f-price">' + esc(px) + '</span>';
      const [rLabel, rCls] = REASON_MAP[t.reason] || ['—', 'r-sr'];
      rsnSpan = '<span class="f-reason ' + rCls + '">' + rLabel + '</span>';
    }

    const rowCls = (i === 0 && isNew) ? 'frow new' : 'frow';
    html += `<div class="${rowCls}">
      <span class="f-ts">${esc(t.ts)}</span>
      <span class="f-bot">${esc(t.bot)}</span>
      <span class="f-series">${esc(t.series)}</span>
      ${col4}
      ${col5}
      ${rsnSpan}
      <span class="f-pnl ${pnlCls}">${pnlStr}</span>
    </div>`;
  });
  el.innerHTML = html;
}

// ── Render: order book ───────────────────────────────────
function renderOrderBook(ob) {
  ob = ob || {};
  const el = document.getElementById('ob-body');
  const titleEl = document.getElementById('ob-title');
  const depthSrc = document.getElementById('ob-depth-src');
  titleEl.textContent = (ob.series && ob.series !== '—')
    ? 'Depth — ' + ob.series
    : 'Market Depth';

  if (depthSrc) {
    if (ob.depth_source && ob.yes_bid != null && ob.yes_ask != null) {
      let s = ob.depth_source + ' · Yb ' + Number(ob.yes_bid).toFixed(1) +
        '/Ya ' + Number(ob.yes_ask).toFixed(1) + '¢';
      if (ob.log_ts)
        s += ' · ' + ob.log_ts + ' UTC';
      depthSrc.textContent = s;
    } else {
      depthSrc.textContent = 'NO YES BID LINE IN LOG';
    }
  }

  const bidsList = Array.isArray(ob.bids) ? ob.bids : [];
  const asksList = Array.isArray(ob.asks) ? ob.asks : [];

  if (!bidsList.length && !asksList.length) {
    el.innerHTML = '<div class="empty-msg">NO DATA</div>'; return;
  }

  const allQ  = [...bidsList, ...asksList].map(x => x.q);
  const maxQ  = Math.max(...allQ, 1);
  const now   = Date.now();  // time-based jitter so bars pulse on each 500ms poll

  let html = '';
  const midTxt = (ob.mid != null && Number.isFinite(Number(ob.mid)))
    ? Number(ob.mid).toFixed(1) + '¢ — MID'
    : '— — MID';

  // Asks reversed (highest at top)
  [...asksList].reverse().forEach((a, i) => {
    const jitter = 0.87 + 0.13 * Math.sin(now / 600 + i * 1.9);
    const w = ((a.q / maxQ) * 100 * jitter).toFixed(0);
    html += `<div class="obr">
      <span class="obp neg">${Number(a.p).toFixed(1)}¢</span>
      <div class="obbar"><div class="obask" style="width:${w}%"></div></div>
      <span class="obq">${a.q}</span>
    </div>`;
  });
  html += `<div class="ob-mid">${esc(midTxt)}</div>`;
  bidsList.forEach((b, i) => {
    const jitter = 0.87 + 0.13 * Math.sin(now / 600 + i * 1.9 + Math.PI);
    const w = ((b.q / maxQ) * 100 * jitter).toFixed(0);
    html += `<div class="obr">
      <span class="obp pos">${Number(b.p).toFixed(1)}¢</span>
      <div class="obbar"><div class="obbid" style="width:${w}%"></div></div>
      <span class="obq">${b.q}</span>
    </div>`;
  });
  el.innerHTML = html;
}

// ── Render: series stats ─────────────────────────────────
function renderSeries(series, mm) {
  series = series && typeof series === 'object' ? series : {};
  const el = document.getElementById('sp-body');
  const NAMES = ['KXBTC15M', 'KXETH15M', 'KXSOL15M'];
  const SHORT  = { KXBTC15M: 'BTC', KXETH15M: 'ETH', KXSOL15M: 'SOL' };
  let html = '';
  NAMES.forEach(s => {
    const d   = series[s] || { trades:0, wins:0, win_rate:0, pnl:0, last_price:0 };
    const pcl = d.pnl >= 0 ? 'pos' : 'neg';
    const pnlStr = (d.pnl >= 0 ? '+' : '') + '$' + Math.abs(d.pnl).toFixed(4);
    const lastStr = d.last_price ? d.last_price + '¢' : '—';
    html += `<div class="sb">
      <div class="sb-hdr">
        <span class="sb-name">${SHORT[s]} <span class="sb-sub">${s}</span></span>
        <span class="sb-pnl ${pcl}">${pnlStr}</span>
      </div>
      <div class="wbar-wrap"><div class="wbar-fill" style="width:${d.win_rate}%"></div></div>
      <div class="sb-meta">
        <span>WIN <span class="${d.win_rate>=50?'pos':'neg'}">${d.win_rate}%</span></span>
        <span>FLS <span class="g">${d.trades}</span></span>
        <span>Y+N <span class="amb">${lastStr}</span></span>
      </div>
      <hr class="sb-div">
    </div>`;
  });
  // Market maker totals (bot process + session stats)
  if (mm && typeof mm === 'object') {
    const mpcl   = mm.pnl >= 0 ? 'pos' : 'neg';
    const mpnl   = (mm.pnl >= 0 ? '+' : '') + '$' + Math.abs(mm.pnl).toFixed(4);
    const mstat  = mm.running ? '<span class="pos">&#9679; RUN</span>' : '<span class="neg">&#9675; OFF</span>';
    html += `<div class="sb">
      <div class="sb-hdr">
        <span class="sb-name">MM <span class="sb-sub g-faint">TODAY TOTAL</span></span>
        <span class="sb-pnl ${mpcl}">${mpnl}</span>
      </div>
      <div class="sb-meta">
        <span>FLS <span class="g">${mm.trades}</span></span>
        <span>WIN <span class="${mm.win_rate>=50?'pos':'neg'}">${mm.win_rate}%</span></span>
        <span>${mstat}</span>
      </div>
      <hr class="sb-div">
    </div>`;
  }
  el.innerHTML = html;
}

// ── Render: console log ──────────────────────────────────
function renderConsole(lines) {
  if (!Array.isArray(lines)) lines = [];
  const el = document.getElementById('con-body');
  if (!lines.length) {
    el.innerHTML = '<div class="empty-msg">NO LOG</div>'; return;
  }
  // Strip leading timestamp + log level; show oldest→newest so newest lands at bottom
  const strip = l => l.replace(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\s+\w+\s+/, '');
  let html = '';
  lines.forEach(l => {
    html += `<div class="cline">${esc(strip(l))}</div>`;
  });
  el.innerHTML = html;
  el.scrollTop = el.scrollHeight;
}

// ── Render: frequency chart ──────────────────────────────
function renderFreq(freq) {
  if (!freqChart) return;
  let f = Array.isArray(freq) ? [...freq] : [];
  while (f.length < 24) f.push(0);
  freqChart.data.datasets[0].data = f.slice(0, 24);
  freqChart.update('none');
}


// ── BTC OHLC (Coinbase arrays → numeric fields). Bad rows dropped.
function normalizeBtcCandle(c) {
  const row = {
    t: Number(c.t),
    o: Number(c.o),
    h: Number(c.h),
    l: Number(c.l),
    c: Number(c.c),
  };
  if (![row.t, row.o, row.h, row.l, row.c].every(x => Number.isFinite(x))) return null;
  if (row.l > row.h) { const x = row.l; row.l = row.h; row.h = x; }
  return row;
}

// ── Render: BTC candlestick (raw canvas) ─────────────────
function renderBtcCandles(rawCandles) {
  if (!rawCandles || rawCandles.length < 2) return;

  const candles = rawCandles.map(normalizeBtcCandle).filter(Boolean);
  if (candles.length < 2) return;

  const wrap   = document.getElementById('btc-wrap');
  const canvas = document.getElementById('btc-canvas');
  const br       = wrap.getBoundingClientRect();
  let W0         = Math.round(br.width)  || wrap.clientWidth  || 0;
  let H0         = Math.round(br.height) || wrap.clientHeight || 0;

  if (W0 < 120 || H0 < 36) {
    if (btcLayoutRetries < 30) {
      btcLayoutRetries += 1;
      requestAnimationFrame(() => renderBtcCandles(rawCandles));
      return;
    }
    W0 = W0 || 640;
    H0 = Math.max(H0 || 0, 100);
  }
  btcLayoutRetries = 0;

  const dpr = window.devicePixelRatio || 1;
  canvas.width  = Math.max(1, Math.round(W0 * dpr));
  canvas.height = Math.max(1, Math.round(H0 * dpr));
  canvas.style.width  = W0 + 'px';
  canvas.style.height = H0 + 'px';

  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W0, H0);
  ctx.fillStyle = '#000';
  ctx.fillRect(0, 0, W0, H0);

  const n      = Math.min(candles.length, 80);
  const recent = candles.slice(-n);
  const tailRaw = rawCandles[rawCandles.length - 1];
  const liveCand = normalizeBtcCandle(tailRaw);
  const fallbackC = Number(recent[recent.length - 1].c);
  const liveClose = Number.isFinite(Number(liveCand && liveCand.c))
    ? Number(liveCand.c)
    : fallbackC;

  let hi     = Math.max(...recent.map(x => x.h));
  let lo     = Math.min(...recent.map(x => x.l));
  if (Number.isFinite(liveClose)) {
    hi = Math.max(hi, liveClose);
    lo = Math.min(lo, liveClose);
  }
  const rng    = (hi > lo) ? (hi - lo) : 1e-12;

  const PAD_T = 6, PAD_B = 14, PAD_L = 4, PAD_R = 54;
  const CW = Math.max(W0 - PAD_L - PAD_R, 1);
  const CH = Math.max(H0 - PAD_T - PAD_B, 1);
  const cw = CW / n;
  const bw = Math.max(1, cw * 0.6);
  const toY = (px) => PAD_T + CH - ((Number(px) - lo) / rng) * CH;

  ctx.font      = '7px IBM Plex Mono';
  ctx.textAlign = 'left';
  for (let i = 0; i <= 3; i++) {
    const y = PAD_T + (i / 3) * CH;
    const p = hi - (i / 3) * rng;
    ctx.strokeStyle = '#001a00';
    ctx.lineWidth   = 0.5;
    ctx.beginPath(); ctx.moveTo(PAD_L, y); ctx.lineTo(W0 - PAD_R, y); ctx.stroke();
    ctx.fillStyle = '#00ff4133';
    ctx.fillText(p.toLocaleString('en',{maximumFractionDigits:0}), W0 - PAD_R + 3, y + 3);
  }

  recent.forEach((c, i) => {
    const x     = PAD_L + i * cw + cw / 2;
    const bull  = c.c >= c.o;
    const color = bull ? '#00ff41' : '#ff3131';
    const fillA = bull ? 'rgba(0,255,65,0.25)' : 'rgba(255,49,49,0.25)';
    ctx.strokeStyle = color;
    ctx.lineWidth   = 0.8;
    ctx.beginPath();
    ctx.moveTo(x, toY(c.h)); ctx.lineTo(x, toY(c.l));
    ctx.stroke();
    const yO = toY(c.o), yC = toY(c.c);
    const y  = Math.min(yO, yC);
    const hBody  = Math.max(1, Math.abs(yO - yC));
    ctx.fillStyle   = fillA;
    ctx.strokeStyle = color;
    ctx.lineWidth   = 0.8;
    ctx.fillRect(x - bw/2, y, bw, hBody);
    ctx.strokeRect(x - bw/2, y, bw, hBody);
  });

  // Live BTC (last fetched close): vertical mark at newest bar + horizontal reference
  if (recent.length >= 2 && Number.isFinite(liveClose)) {
    const ly   = toY(liveClose);
    const lx   = PAD_L + (n - 1) * cw + cw / 2;
    const plotB = PAD_T + CH;

    ctx.save();
    ctx.setLineDash([3, 3]);
    ctx.strokeStyle = 'rgba(230,170,0,0.55)';
    ctx.lineWidth = 0.65;
    ctx.beginPath(); ctx.moveTo(lx, PAD_T); ctx.lineTo(lx, plotB); ctx.stroke();
    ctx.setLineDash([5, 4]);
    ctx.strokeStyle = '#e6aa00cc';
    ctx.lineWidth = 0.95;
    ctx.beginPath(); ctx.moveTo(PAD_L, ly); ctx.lineTo(W0 - PAD_R, ly); ctx.stroke();
    ctx.restore();

    ctx.fillStyle = '#ffaa00';
    ctx.font = 'bold 8px IBM Plex Mono';
    ctx.textAlign = 'left';
    ctx.fillText(
      '$' + liveClose.toLocaleString('en', {maximumFractionDigits: 0}),
      W0 - PAD_R + 3,
      ly + 3,
    );
  }

  ctx.fillStyle = '#00ff4133'; ctx.font = '7px IBM Plex Mono'; ctx.textAlign = 'center';
  const labelEvery = Math.max(1, Math.ceil(n / 6));
  recent.forEach((c, i) => {
    if (i % labelEvery !== 0) return;
    const x  = PAD_L + i * cw + cw / 2;
    const dt = new Date(Math.floor(Number(c.t) * 1000));
    const lbl = dt.getUTCHours().toString().padStart(2,'0') + ':' + dt.getUTCMinutes().toString().padStart(2,'0');
    ctx.fillText(lbl, x, H0 - 2);
  });

  document.getElementById('btc-price-lbl').textContent =
    Number.isFinite(liveClose)
      ? ('$' + liveClose.toLocaleString('en', {maximumFractionDigits: 0}))
      : '1M CANDLES';
}

// ── Main poll ────────────────────────────────────────────
let __terminalProbeLogged = false;

async function pollTerminal() {
  try {
    const res = await fetch('/api/terminal', FETCH_CRED);

    const bodyText = await res.text();
    if (!res.ok) {
      console.error('[terminal] HTTP', res.status, bodyText && bodyText.slice(0, 300));
      setTerminalApiError(
        res.status === 401 ? 'API 401 — session / login?' : ('API ERROR ' + res.status)
      );
      return;
    }

    let data;
    try {
      data = bodyText ? JSON.parse(bodyText) : null;
    } catch (parseErr) {
      console.error('[terminal] JSON parse error', parseErr, bodyText && bodyText.slice(0, 500));
      setTerminalApiError('BAD JSON RESPONSE');
      return;
    }

    if (!__terminalProbeLogged) {
      __terminalProbeLogged = true;
      console.log('/api/terminal (full response on load):', data);
    }

    data = normalizeTerminalPayload(data);
    setTerminalApiError('');

    renderTopBar(data);
    renderFeed(data.feed);
    renderEquity(data.equity);
    renderOrderBook(data.orderbook);
    renderSeries(data.series, data.mm);
    renderConsole(data.console);
    renderFreq(data.freq);
  } catch (e) {
    console.error('[terminal] pollTerminal', e);
    setTerminalApiError(String(e.message || e));
  }
  // Keep BTC canvas sized after equity/layout updates (wrapped in rAF)
  if (btcCandles.length)
    requestAnimationFrame(() => renderBtcCandles(btcCandles));
}


async function pollBtcCandles() {
  try {
    const res  = await fetch('/api/btc-candles', FETCH_CRED);
    if (!res.ok) {
      console.warn('[btc-candles] HTTP', res.status);
      throw new Error('HTTP ' + res.status);
    }
    const data = await res.json();
    if (Array.isArray(data) && data.length) {
      btcCandles = data;
      if (!window.__btcSampleLogged) {
        console.log('[btc-candles] first candle object (shape sample):', data[0]);
        window.__btcSampleLogged = true;
      }
    }
  } catch (e) {
    console.warn('BTC candles error:', e.message);
  }
  if (btcCandles.length)
    renderBtcCandles(btcCandles);
}

// ── Boot ─────────────────────────────────────────────────
requestAnimationFrame(() => {
  initCharts();
  pollTerminal();
});
requestAnimationFrame(() => {
  requestAnimationFrame(() => {
    pollBtcCandles();
  });
});
setInterval(pollTerminal, POLL_MS);
setInterval(pollBtcCandles, BTC_POLL_MS);
// Redraw candles on resize
window.addEventListener('resize', () => {
  if (btcCandles.length)
    requestAnimationFrame(() => renderBtcCandles(btcCandles));
});
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("TRADE DESK  —  http://localhost:5000")
    app.run(host="127.0.0.1", port=5000, debug=False)
