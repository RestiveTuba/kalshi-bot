#!/usr/bin/env python3
"""
dashboard.py — Bloomberg-style trading terminal for kalshi-bot.
Phosphor-green-on-black institutional terminal aesthetic.
Run:  python3 dashboard.py
URL:  http://localhost:5000
"""
import functools
import asyncio
import hashlib
import json
import os
import re
import secrets
import subprocess
import time
import urllib.request as _urlreq
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Any

from flask import Flask, jsonify, request, Response, session, redirect, send_from_directory

BASE              = Path(__file__).parent
STATIC_DIR        = BASE / "static"
MOMENTUM_LOG      = BASE / "momentum.log"
POLYMARKET_LOG    = BASE / "polymarket.log"
POLYMARKET_TRADES = BASE / "polymarket_trades.jsonl"
COINBASE_LOG      = BASE / "coinbase.log"
MM_LOG            = BASE / "market_maker.log"
COINBASE_TRADES   = BASE / "coinbase_trades.jsonl"
MM_TRADES         = BASE / "market_maker_trades.jsonl"
MM_LEDGER         = BASE / "market_maker_ledger.jsonl"
ENV_FILE          = BASE / ".env"


def _load_dotenv_into_environ() -> None:
    """Load repo .env before module-level config reads os.environ."""
    if not ENV_FILE.is_file():
        return
    try:
        for raw in ENV_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            key = k.strip()
            if key and (key not in os.environ or not os.environ[key].strip()):
                os.environ[key] = v.strip().strip('"').strip("'")
    except OSError:
        pass


_load_dotenv_into_environ()

app = Flask(__name__, static_folder=None)
app.config["JSON_SORT_KEYS"] = False
app.secret_key = os.environ.get("FLASK_SECRET", secrets.token_hex(32))
log = app.logger

# ── Caches ───────────────────────────────────────────────────────────────────
_term_cache: dict = {"data": None, "ts": 0.0}
_btc_cache:  dict = {"data": None, "ts": 0.0}
TERM_TTL = 0.4
BTC_TTL  = 9.0

# ── Cached market-hours probe (avoid recomputing on every terminal poll) ─────
_MARKET_SESSION_CACHE: dict[str, Any] = {"open": False, "ts": 0.0}

# ── Kalshi REST (reuse market_maker._SimpleClient) ──────────────────────────


def _ensure_kalshi_credentials_path() -> None:
    """Default key path to repo root when env not set."""
    kp = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "").strip()
    if kp and Path(kp).is_file():
        return
    for name in ("kalshi_private_key.pem",):
        cand = BASE / name
        if cand.is_file():
            os.environ["KALSHI_PRIVATE_KEY_PATH"] = str(cand)
            break


async def _mm_fetch_balance() -> tuple[Optional[float], Optional[int], Optional[str]]:
    """(usd, cents, error) via GET portfolio/balance."""
    try:
        from market_maker import _SimpleClient, _coerce_balance_cent_int

        cli = _SimpleClient()
        try:
            if not cli._private_key:
                return None, None, "Kalshi API key / private key not configured"
            r = await cli.get("portfolio/balance")
            cents = _coerce_balance_cent_int(r.get("balance"))
            if cents is None:
                return None, None, "balance field missing"
            usd = round(cents / 100.0, 6)
            return usd, int(cents), None
        finally:
            await cli.close()
    except Exception as exc:
        log.warning("Kalshi portfolio/balance: %s", exc)
        return None, None, str(exc)


def _kalshi_unpack_order(blob: dict) -> dict:
    od = blob.get("order") if isinstance(blob.get("order"), dict) else blob
    return od if isinstance(od, dict) else {}


def _extract_kalshi_order_id(blob: dict) -> Optional[str]:
    if not isinstance(blob, dict):
        return None
    od = _kalshi_unpack_order(blob)
    for k in ("order_id", "id"):
        v = od.get(k) if isinstance(od, dict) else None
        if v is None:
            v = blob.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


async def _mm_cancel_all_resting_orders() -> tuple[bool, int, list[str]]:
    """
    GET resting orders then DELETE portfolio/orders/{id} each.
    Returns (ok_summary, cancelled_count, per-order error strings).
    """
    errs: list[str] = []
    cancelled = 0
    try:
        from market_maker import _SimpleClient

        cli = _SimpleClient()
        try:
            if not cli._private_key:
                return False, 0, ["Kalshi API key / private key not configured"]
            r = await cli.get(
                "portfolio/orders",
                params={"status": "resting", "limit": 200},
            )
            raw_list = r.get("orders") or []
            if not isinstance(raw_list, list):
                return False, 0, ["unexpected orders response"]
            ids: list[str] = []
            for raw in raw_list:
                if not isinstance(raw, dict):
                    continue
                oid = _extract_kalshi_order_id(raw)
                if oid:
                    ids.append(oid)
            for oid in ids:
                try:
                    await cli.delete(f"portfolio/orders/{oid}")
                    cancelled += 1
                    log.info("cancel-all DELETE %s", oid)
                except Exception as exc:  # noqa: BLE001
                    msg = str(exc).strip() or repr(exc)
                    errs.append(f"{oid}: {msg}")
                    log.warning("cancel-all DELETE failed %s: %s", oid, msg)
            return True, cancelled, errs
        finally:
            await cli.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("Kalshi cancel-all: %s", exc)
        return False, cancelled, errs + [str(exc)]


# ── Momentum log → order book (live YES/NO bids) ───────────────────────────
_MOM_YES_NO_BID_LINE = re.compile(
    r"YES\s+bid\s*=\s*(\d+(?:\.\d+)?)c?\s+NO\s+bid\s*=\s*(\d+(?:\.\d+)?)c?",
    re.IGNORECASE,
)
# "*** ACTIVATION WINDOW OPEN *** … | YES bid=10c ask=15c" (no separate NO bid in line)
_MOM_YES_BID_ASK_LINE = re.compile(
    r"YES\s+bid\s*=\s*(\d+(?:\.\d+)?)c?\s+ask\s*=\s*(\d+(?:\.\d+)?)c?",
    re.IGNORECASE,
)
_MOM_SERIES_TAG = re.compile(r"\[(\w+)\]")

# ── Auth ─────────────────────────────────────────────────────────────────────
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "").strip()
DASHBOARD_AUTH_MARKER = (
    hashlib.sha256(DASHBOARD_PASSWORD.encode("utf-8")).hexdigest()
    if DASHBOARD_PASSWORD
    else ""
)

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
        if (
            not session.get("authed")
            or session.get("auth_marker") != DASHBOARD_AUTH_MARKER
        ):
            session.pop("authed", None)
            session.pop("auth_marker", None)
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


def _compute_mm_ledger_summary() -> dict[str, Any]:
    open_lots: dict[str, dict[str, Any]] = {}
    realized = 0.0
    mismatches = 0
    last_reconcile_status = "ok"
    for row in _load_jsonl(MM_LEDGER):
        et = str(row.get("event_type") or "")
        lot_ids = row.get("lot_ids") if isinstance(row.get("lot_ids"), list) else []
        if et == "fill":
            raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
            lot = raw.get("lot") if isinstance(raw.get("lot"), dict) else {}
            lot_id = str((lot_ids or [row.get("lot_id") or lot.get("lot_id")])[0])
            if lot_id:
                open_lots[lot_id] = {
                    "lot_id": lot_id,
                    "ticker": str(row.get("ticker") or lot.get("ticker") or ""),
                    "series": str(row.get("series") or lot.get("series") or ""),
                    "side": str(row.get("side") or lot.get("side") or ""),
                    "qty": int(float(row.get("qty") or lot.get("qty") or 0)),
                    "entry_price_cents": float(row.get("price_cents") or lot.get("entry_price_cents") or 0),
                }
        elif et in ("manual_close", "settlement"):
            realized += float(row.get("pnl_dollars") or 0)
            for lot_id in lot_ids:
                open_lots.pop(str(lot_id), None)
        elif et == "reconcile_mismatch":
            mismatches += 1
            last_reconcile_status = "halted"
    open_yes = sum(int(l.get("qty") or 0) for l in open_lots.values() if str(l.get("side")).upper() == "YES")
    open_no = sum(int(l.get("qty") or 0) for l in open_lots.values() if str(l.get("side")).upper() == "NO")
    exposure = sum(float(l.get("entry_price_cents") or 0) / 100.0 for l in open_lots.values())
    return {
        "open_lots": len(open_lots),
        "open_yes": open_yes,
        "open_no": open_no,
        "realized_pnl": round(realized, 4),
        "unsettled_exposure": round(exposure, 4),
        "reconciliation_status": last_reconcile_status,
        "reconcile_mismatches": mismatches,
        "recent_open_lots": list(open_lots.values())[-20:],
    }


# ── market_maker.PAPER_MODE (shared with /api/mode) ─────────────────────────────
_MM_PAPER_MODE_ASSIGN = re.compile(
    r"^PAPER_MODE\s*=\s*(True|False)\b",
    re.MULTILINE | re.IGNORECASE,
)


def _read_market_maker_paper_mode() -> Optional[bool]:
    """Parse PAPER_MODE from market_maker.py; None if ambiguous or unreadable."""
    path = BASE / "market_maker.py"
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    m = _MM_PAPER_MODE_ASSIGN.search(text)
    if not m:
        return None
    return m.group(1).lower() == "true"


def _toggle_market_maker_paper_mode_in_file() -> tuple[Optional[bool], Optional[str]]:
    """
    Flip PAPER_MODE True↔False in market_maker.py (first assignment line match).
    Returns (new_is_paper, error_message_or_None).
    """
    path = BASE / "market_maker.py"
    if not path.is_file():
        return None, "market_maker.py not found"
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return None, str(e)
    m = _MM_PAPER_MODE_ASSIGN.search(text)
    if not m:
        return None, "PAPER_MODE assignment not found"
    cur_paper = m.group(1).lower() == "true"
    new_lit = "False" if cur_paper else "True"
    new_text = text[: m.start(1)] + new_lit + text[m.end(1) :]
    try:
        path.write_text(new_text, encoding="utf-8")
    except OSError as e:
        return None, str(e)
    return (not cur_paper), None


def _restart_market_maker_subprocess() -> None:
    """pkill existing market_maker.py, then detach a new python3 process."""
    script = "market_maker.py"
    try:
        subprocess.run(["pkill", "-f", script], capture_output=True, text=True, timeout=15)
    except Exception:
        log.warning("market_maker pkill", exc_info=True)
    time.sleep(0.8)
    proc = subprocess.Popen(
        ["python3", script],
        cwd=str(BASE),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    log.info("market_maker restarted pid=%s", proc.pid)


_BOT_SCRIPT_META = {
    "momentum": {"script": "momentum_bot.py", "log": MOMENTUM_LOG},
    "market_maker": {"script": "market_maker.py", "log": MM_LOG},
}


def _tail_raw_log(path: Path, n: int = 50) -> str:
    """Last n lines including blank lines."""
    if not path.exists():
        return ""
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - max(8192, n * 240)))
            chunk = f.read().decode("utf-8", errors="replace")
        lines = chunk.splitlines()
        return "\n".join(lines[-n:]) if lines else ""
    except Exception:
        return ""


def _pkill_by_script_substring(pat: str) -> None:
    try:
        subprocess.run(["pkill", "-f", pat], capture_output=True, text=True, timeout=20)
    except Exception:
        log.warning("pkill -f %s", pat, exc_info=True)


def _start_bot_logged(script_fname: str, log_file: Path) -> int:
    """Kill existing matching processes, spawn python3 detached with stdout/stderr to log."""
    _pkill_by_script_substring(script_fname)
    time.sleep(0.5)
    script_path = BASE / script_fname
    lf = open(log_file, "a", encoding="utf-8")
    proc = subprocess.Popen(
        ["python3", str(script_path)],
        cwd=str(BASE),
        stdin=subprocess.DEVNULL,
        stdout=lf,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        lf.close()
    except OSError:
        pass
    log.info("started %s pid=%s → %s", script_fname, proc.pid, log_file)
    return proc.pid


def _bot_controls_snapshot() -> dict:
    mom = _BOT_SCRIPT_META["momentum"]["script"]
    mm = _BOT_SCRIPT_META["market_maker"]["script"]
    mr, mp = _is_running(mom)
    rr, rp = _is_running(mm)
    return {
        "bots": {
            "momentum": {"running": bool(mr), "pid": int(mp)},
            "market_maker": {"running": bool(rr), "pid": int(rp)},
        },
        "logs": {
            "momentum": _tail_raw_log(MOMENTUM_LOG, 50),
            "market_maker": _tail_raw_log(MM_LOG, 50),
        },
    }


def _parse_latest_momentum_book() -> Optional[dict]:
    """
    Scan momentum.log (tail) for the last line containing 'YES bid='.

    Handles:
    - WATCHING:  ``YES bid=Xc NO bid=Yc``
    - ACTIVATION: ``YES bid=Xc ask=Yc`` — NO bid derived as ``100 − YES ask``.
    Implied YES ask for the first format is 100 − NO_bid.
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
            m_bn = _MOM_YES_NO_BID_LINE.search(line)
            m_ba = _MOM_YES_BID_ASK_LINE.search(line)
            if m_bn:
                yb = float(m_bn.group(1))
                nb = float(m_bn.group(2))
                yes_ask = max(0.0, min(100.0, 100.0 - nb))
            elif m_ba:
                yb = float(m_ba.group(1))
                yes_ask = max(0.0, min(100.0, float(m_ba.group(2))))
                nb = max(0.0, min(100.0, 100.0 - yes_ask))
            else:
                continue
            sm = _MOM_SERIES_TAG.search(line)
            series = sm.group(1) if sm else "—"
            yb = max(0.0, min(100.0, yb))
            yes_ask = max(0.0, min(100.0, yes_ask))
            if yes_ask < yb:
                yes_ask = min(100.0, yb + 0.5)
            if m_ba:
                nb = max(0.0, min(100.0, 100.0 - yes_ask))
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


_MOM_ISO_TS_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})")


def _parse_momentum_wall_time(line: str) -> Optional[datetime]:
    m = _MOM_ISO_TS_PREFIX.match(line.strip())
    if not m:
        return None
    try:
        dt = datetime.fromisoformat(m.group(1))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _yes_bid_volatility_by_series(window_min: int = 15) -> dict[str, float]:
    """Absolute YES bid change %% over recent window derived from momentum.log."""
    keys = ("KXBTC15M", "KXETH15M", "KXSOL15M")
    buckets: dict[str, list[tuple[datetime, float]]] = {s: [] for s in keys}
    if not MOMENTUM_LOG.exists():
        return dict.fromkeys(keys, 0.0)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=window_min)
    try:
        with open(MOMENTUM_LOG, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 1_600_000))
            chunk = f.read().decode("utf-8", errors="replace")
    except OSError:
        return dict.fromkeys(keys, 0.0)

    for line in chunk.splitlines():
        if "YES bid=" not in line:
            continue
        wall = _parse_momentum_wall_time(line)
        if wall is None or wall < cutoff:
            continue
        sm = _MOM_SERIES_TAG.search(line)
        ser = sm.group(1) if sm else ""
        if ser not in buckets:
            continue
        m_bn = _MOM_YES_NO_BID_LINE.search(line)
        m_ba = _MOM_YES_BID_ASK_LINE.search(line)
        if m_bn:
            yb = float(m_bn.group(1))
        elif m_ba:
            yb = float(m_ba.group(1))
        else:
            continue
        buckets[ser].append((wall, max(0.0, min(100.0, yb))))

    out: dict[str, float] = {}
    for ser in keys:
        pts = sorted(buckets[ser], key=lambda x: x[0])
        if len(pts) < 2:
            out[ser] = 0.0
            continue
        first = pts[0][1]
        last = pts[-1][1]
        base = abs(first) if abs(first) > 1e-6 else 1.0
        out[ser] = round(abs(last - first) / base * 100.0, 3)
    return out


def _compute_weekly_pnl(mm_filtered: list[dict]) -> list[dict]:
    """Last 7 calendar days UTC aggregated P&L and trade counts."""
    today = datetime.now(timezone.utc).date()
    day_list = [(today - timedelta(days=k)).isoformat() for k in range(6, -1, -1)]
    acc = {ds: {"date": ds, "pnl": 0.0, "trades": 0} for ds in day_list}
    for t in mm_filtered:
        dt = _parse_trade_dt_utc(t)
        if dt is None:
            continue
        ds = dt.date().isoformat()
        if ds not in acc:
            continue
        acc[ds]["pnl"] += float(t.get("pnl_dollars") or t.get("pnl") or 0)
        acc[ds]["trades"] += 1
    return [{"date": d, "pnl": round(acc[d]["pnl"], 4), "trades": int(acc[d]["trades"])} for d in day_list]


def _compute_streak_mm(mm_filtered: list[dict]) -> dict[str, Any]:
    distant = datetime(1970, 1, 1, tzinfo=timezone.utc)
    ordered = sorted(
        mm_filtered,
        key=lambda tr: (_parse_trade_dt_utc(tr) or distant),
        reverse=True,
    )
    streak = 0
    streak_kind = ""
    for t in ordered:
        pnl_v = float(t.get("pnl_dollars") or t.get("pnl") or 0)
        if pnl_v > 0:
            k = "win"
        elif pnl_v < 0:
            k = "loss"
        else:
            break
        if not streak_kind:
            streak_kind = k
        elif k != streak_kind:
            break
        streak += 1
    if streak_kind == "win" and streak > 0:
        return {"kind": streak_kind, "n": streak, "label": f"↑{streak}W", "tone": "pos"}
    if streak_kind == "loss" and streak > 0:
        return {"kind": streak_kind, "n": streak, "label": f"↓{streak}L", "tone": "neg"}
    return {"kind": "flat", "n": 0, "label": "", "tone": ""}


def _sessions_recent_via_momentum(max_age_min: float = 90.0) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_min)
    if not MOMENTUM_LOG.exists():
        return False
    try:
        with open(MOMENTUM_LOG, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 800_000))
            chunk = f.read().decode("utf-8", errors="replace")
    except OSError:
        return False
    for line in reversed(chunk.splitlines()):
        if "New session:" not in line:
            continue
        dt = _parse_momentum_wall_time(line)
        if dt is not None and dt >= cutoff:
            return True
    return False


def _probe_market_open_cached() -> bool:
    """Momentum log recent 'New session' (30 s server TTL)."""
    global _MARKET_SESSION_CACHE  # noqa: PLW0603
    nowt = time.time()
    if nowt - float(_MARKET_SESSION_CACHE["ts"]) < 30.0:
        return bool(_MARKET_SESSION_CACHE["open"])
    opened = _sessions_recent_via_momentum(90.0)
    _MARKET_SESSION_CACHE["open"] = opened
    _MARKET_SESSION_CACHE["ts"] = nowt
    return opened


def _read_dotenv_values() -> dict[str, str]:
    out: dict[str, str] = {}
    if not ENV_FILE.is_file():
        return out
    try:
        for raw in ENV_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            out[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return out


def _upsert_env_file(updates: dict[str, str]) -> Optional[str]:
    lines: list[str] = []
    try:
        if ENV_FILE.is_file():
            lines = ENV_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as e:
        return str(e)
    seen: set[str] = set()
    out_lines: list[str] = []
    for line in lines:
        st = line.strip()
        if st and not st.startswith("#") and "=" in st:
            k = st.split("=", 1)[0].strip()
            if k in updates:
                out_lines.append(f"{k}={updates[k]}")
                seen.add(k)
                continue
        out_lines.append(line)
    for k, v in updates.items():
        if k not in seen:
            out_lines.append(f"{k}={v}")
    try:
        ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
        ENV_FILE.write_text("\n".join(out_lines) + ("\n" if out_lines else ""), encoding="utf-8")
    except OSError as e:
        return str(e)
    return None


async def _mm_ping_balance_ms() -> tuple[Optional[int], Optional[str]]:
    t0 = time.perf_counter()
    try:
        from market_maker import _SimpleClient

        cli = _SimpleClient()
        try:
            if not cli._private_key:
                return None, "Kalshi API key / private key not configured"
            await cli.get("portfolio/balance")
            return int((time.perf_counter() - t0) * 1000), None
        finally:
            await cli.close()
    except Exception as exc:
        return None, str(exc)


def _fetch_coinbase_spot_prices() -> dict[str, Any]:
    now = time.time()
    cached = _btc_cache.get("spot")
    if isinstance(cached, dict) and now - float(cached.get("ts", 0.0)) < 3.0:
        return dict(cached["data"])
    out: dict[str, Any] = {"ok": True, "pairs": {}, "ts": time.time()}
    for sym, label in (("BTC-USD", "BTC"), ("ETH-USD", "ETH"), ("SOL-USD", "SOL")):
        try:
            url = f"https://api.exchange.coinbase.com/products/{sym}/ticker"
            req = _urlreq.Request(
                url,
                headers={"User-Agent": "kalshi-trade-desk/1", "Accept": "application/json"},
            )
            with _urlreq.urlopen(req, timeout=8) as r:
                raw_body = r.read().decode("utf-8", errors="replace")
            log.info("coinbase spot raw %s: %s", sym, raw_body[:500])
            jd = json.loads(raw_body)
            px = float(jd.get("price") or 0)
            rnd = 2 if label == "SOL" else (0 if label == "BTC" else 2)
            out["pairs"][label] = round(px, rnd)
            out[label] = {"price": round(px, rnd)}
        except Exception as exc:
            out["ok"] = False
            out["pairs"][label] = None
            out[label] = None
            out["err"] = str(exc)
    _btc_cache["spot"] = {"data": out, "ts": now}
    return out


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
    vol = _yes_bid_volatility_by_series(15)
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
            "volatility_pct": float(vol.get(s) or 0.0),
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


def _compute_orderbook() -> dict:
    """Depth ladder from latest momentum.log YES/NO bids (500ms-polled via terminal API)."""
    parsed = _parse_latest_momentum_book()
    if parsed:
        return parsed
    bids = [{"p": 49.0 - i, "q": max(4, 48 - i * 5)} for i in range(0, 6)]
    asks = [{"p": 51.0 + i, "q": max(4, 44 - i * 5)} for i in range(0, 6)]
    return {
        "series": "—",
        "mid": 50.0,
        "yes_bid": 49.0,
        "yes_ask": 51.0,
        "no_bid": 49.0,
        "bids": bids,
        "asks": asks,
        "depth_source": "synthetic",
        "log_ts": "",
    }


def _terminal_safe_default() -> dict:
    """Minimal valid /api/terminal payload when aggregation fails."""
    now_hms = datetime.now(timezone.utc).strftime("%H:%M:%S")
    now_pts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    day_anchor = datetime.now(timezone.utc).strftime("%Y-%m-%d") + " 00:00"
    wd = datetime.now(timezone.utc).date()
    week0 = [
        {"date": (wd - timedelta(days=k)).isoformat(), "pnl": 0.0, "trades": 0}
        for k in range(6, -1, -1)
    ]
    series_blank = {}
    for s in ("KXBTC15M", "KXETH15M", "KXSOL15M"):
        series_blank[s] = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "pnl": 0.0,
            "last_price": 0,
            "volatility_pct": 0.0,
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
        "weekly_pnl": list(week0),
        "console":   [],
        "mm": {
            "running":  False,
            "pid":      0,
            "last_log": "—",
            "total":    0,
            "trades":   0,
            "wins":     0,
            "losses":   0,
            "win_rate": 0.0,
            "pnl":      0.0,
        },
        "updated": now_hms,
        "dashboard_mm": {
            "paper": True,
            "equity_pnl_title": "PAPER P&L",
        },
        "streak": {"kind": "flat", "n": 0, "label": "", "tone": ""},
        "market_open": False,
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
                    "volatility_pct": float(row.get("volatility_pct") or 0),
                }

    mm = d.get("mm")
    if isinstance(mm, dict):
        tot = int(mm.get("trades") or 0)
        wins = int(mm.get("wins") or 0)
        z["mm"] = {
            "running": bool(mm.get("running", False)),
            "pid": int(mm.get("pid") or 0),
            "last_log": str(mm.get("last_log") or "—")[:80],
            "total": int(mm.get("total") if mm.get("total") is not None else tot),
            "trades": tot,
            "wins": wins,
            "losses": int(mm.get("losses") if mm.get("losses") is not None else (tot - wins)),
            "win_rate": float(mm.get("win_rate") or 0),
            "pnl": float(mm.get("pnl") or 0),
        }

    if isinstance(d.get("console"), list):
        z["console"] = [str(line) for line in d["console"]]

    wp = d.get("weekly_pnl")
    if isinstance(wp, list) and wp:
        clean_wp: list[dict] = []
        for p in wp[:14]:
            if isinstance(p, dict):
                clean_wp.append(
                    {
                        "date": str(p.get("date") or ""),
                        "pnl": float(p.get("pnl") or 0),
                        "trades": int(p.get("trades") or 0),
                    }
                )
        if len(clean_wp) >= 7:
            z["weekly_pnl"] = clean_wp[-7:]
        elif clean_wp:
            z["weekly_pnl"] = clean_wp

    stk = d.get("streak")
    if isinstance(stk, dict):
        z["streak"] = {
            "kind": str(stk.get("kind") or "flat"),
            "n": int(stk.get("n") or 0),
            "label": str(stk.get("label") or ""),
            "tone": str(stk.get("tone") or ""),
        }

    if "market_open" in d:
        z["market_open"] = bool(d.get("market_open"))

    if d.get("updated"):
        z["updated"] = str(d["updated"])

    dm = d.get("dashboard_mm")
    if isinstance(dm, dict):
        z["dashboard_mm"] = {
            "paper": bool(dm.get("paper", True)),
            "equity_pnl_title": str(
                dm.get("equity_pnl_title")
                or ("PAPER P&L" if dm.get("paper", True) else "LIVE P&L")
            ),
        }

    return z


def _compute_mm_stats(mm_trades: Optional[list] = None) -> dict:
    running, pid = _is_running("market_maker.py")
    src = mm_trades if mm_trades is not None else _load_jsonl(MM_TRADES)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    mm_today = [t for t in src if (t.get("entry_time") or t.get("time") or "").startswith(today)]
    wins = sum(1 for t in mm_today if (t.get("pnl_dollars") or t.get("pnl") or 0) > 0)
    total = len(mm_today)
    pnl = sum((t.get("pnl_dollars") or t.get("pnl") or 0) for t in mm_today)
    ledger = _compute_mm_ledger_summary()
    last = _last_log_line(MM_LOG)
    return {
        "running":  running,
        "pid":      pid,
        "last_log": last[:80] if last and last != "(log not found)" else "—",
        "total":    total,
        "trades":   total,
        "wins":     wins,
        "losses":   total - wins,
        "win_rate": round(wins / total * 100, 1) if total else 0,
        "pnl":      round(pnl, 4),
        "ledger":   ledger,
    }


def _compute_terminal_data() -> dict:
    try:
        m_run, m_pid = _is_running("momentum_bot.py")
        p_run, p_pid = _is_running("polymarket_bot.py")
        c_run, c_pid = _is_running("coinbase_bot.py")

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        mm_all = _load_jsonl(MM_TRADES)
        mm_live = [t for t in mm_all if not t.get("paper", True)]
        mm_paper = [t for t in mm_all if t.get("paper", True)]
        paper_mm = _read_market_maker_paper_mode()
        if paper_mm is None:
            paper_mm = True
        mm_mode = mm_paper if paper_mm else mm_live

        mm_day = [
            t
            for t in mm_mode
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
            "equity":    _compute_equity(mm_mode),
            "feed":      _compute_trade_feed(mm_day, []),
            "orderbook": _compute_orderbook(),
            "series":    _compute_series_stats(mm_day),
            "weekly_pnl": _compute_weekly_pnl(mm_mode),
            "console":   _last_n_lines(MOMENTUM_LOG, 18),
            "mm":        _compute_mm_stats(mm_mode),
            "dashboard_mm": {
                "paper": bool(paper_mm),
                "equity_pnl_title": "PAPER P&L" if paper_mm else "LIVE P&L",
                "ledger": _compute_mm_ledger_summary(),
            },
            "streak":    _compute_streak_mm(mm_mode),
            "market_open": _probe_market_open_cached(),
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
                             "pnl": round(pnl,4), "recent": recent_trades,
                             "ledger": _compute_mm_ledger_summary()},
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
            session["auth_marker"] = DASHBOARD_AUTH_MARKER
            return redirect("/")
        page = LOGIN_HTML.replace("{err_class}", "show").replace("{err_msg}", "Incorrect password")
        return Response(page, 401, mimetype="text/html")
    session.pop("authed", None)
    session.pop("auth_marker", None)
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


@app.route("/api/mode")
@auth_required
def api_mode():
    paper = _read_market_maker_paper_mode()
    if paper is None:
        return jsonify({"mode": "PAPER"})
    return jsonify({"mode": "PAPER" if paper else "LIVE"})


@app.route("/api/toggle-mode", methods=["POST"])
@auth_required
def api_toggle_mode():
    """
    Live toggles are disabled until the ledger validation gate passes.
    """
    if _read_market_maker_paper_mode() is True:
        return jsonify(
            {
                "ok": False,
                "error": "PAPER_MODE is locked on until ledger validation passes",
                "mode": "PAPER",
                "paper": True,
            }
        ), 400
    new_paper, err = _toggle_market_maker_paper_mode_in_file()
    if err:
        log.warning("toggle-mode: %s", err)
        return jsonify({"ok": False, "error": err}), 400

    _term_cache["data"] = None
    _term_cache["ts"] = 0.0
    try:
        _restart_market_maker_subprocess()
    except OSError as e:
        log.exception("toggle-mode restart")
        return jsonify({"ok": False, "error": f"restart failed: {e}"}), 500

    mode = "PAPER" if new_paper else "LIVE"
    log.info("toggle-mode → %s (paper=%s)", mode, new_paper)
    return jsonify({"ok": True, "mode": mode, "paper": bool(new_paper)})


@app.route("/api/bot-controls", methods=["GET", "POST"])
@auth_required
def api_bot_controls():
    if request.method == "GET":
        return jsonify(_bot_controls_snapshot())

    body = request.get_json(force=True, silent=True)
    if not isinstance(body, dict):
        body = {}

    action = body.get("action")
    bot = body.get("bot")

    if action in ("start", "stop"):
        if bot not in _BOT_SCRIPT_META:
            return jsonify({"ok": False, "error": "unknown bot"}), 400
        meta = _BOT_SCRIPT_META[str(bot)]
        script = meta["script"]
        log_path = meta["log"]
        if action == "stop":
            _pkill_by_script_substring(script)
        else:
            _start_bot_logged(script, log_path)
        _term_cache["data"] = None
        _term_cache["ts"] = 0.0
        out = _bot_controls_snapshot()
        out["ok"] = True
        return jsonify(out)

    return jsonify({"ok": False, "error": 'use action "start" or "stop"'}), 400


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


@app.route("/api/balance")
@auth_required
def api_balance():
    """Live Kalshi GET portfolio/balance → dollars."""
    _ensure_kalshi_credentials_path()
    try:
        usd, cents, err = asyncio.run(_mm_fetch_balance())
    except Exception as e:
        log.exception("api/balance")
        return jsonify(
            {"ok": False, "balance_usd": None, "balance_cents": None, "error": str(e)},
        )
    if err:
        return jsonify(
            {
                "ok": False,
                "balance_usd": None if usd is None else float(usd),
                "balance_cents": cents,
                "error": err,
            },
        )
    return jsonify(
        {
            "ok": True,
            "balance_usd": float(usd) if usd is not None else None,
            "balance_cents": int(cents) if cents is not None else None,
        },
    )


@app.route("/api/cancel-all", methods=["POST"])
@auth_required
def api_cancel_all():
    """Emergency: cancel every resting Kalshi order (up to 200)."""
    _ensure_kalshi_credentials_path()
    try:
        ok_run, n, errs = asyncio.run(_mm_cancel_all_resting_orders())
    except Exception as e:  # noqa: BLE001
        log.exception("api/cancel-all")
        return jsonify({"ok": False, "cancelled": 0, "errors": [str(e)]})
    _term_cache["data"] = None
    _term_cache["ts"] = 0.0
    return jsonify(
        {
            "ok": bool(ok_run and len(errs) == 0),
            "cancelled": int(n),
            "errors": errs,
        },
    )


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


@app.route("/api/coinbase-spot")
@auth_required
def api_coinbase_spot():
    """Spot prices for ticker strip (BTC, ETH, SOL)."""
    return jsonify(_fetch_coinbase_spot_prices())


@app.route("/static/<path:filename>")
@auth_required
def static_files(filename: str):
    return send_from_directory(STATIC_DIR, filename)


@app.route("/api/kalshi-latency")
@auth_required
def api_kalshi_latency():
    """GET portfolio/balance round-trip latency (ms)."""
    _ensure_kalshi_credentials_path()
    try:
        ms, err = asyncio.run(_mm_ping_balance_ms())
    except Exception as exc:
        log.exception("api/kalshi-latency")
        return jsonify({"ok": False, "ms": None, "error": str(exc)})
    return jsonify({"ok": err is None and ms is not None, "ms": ms, "error": err})


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
   grid-template-rows:40px minmax(380px,1fr) auto minmax(112px,calc(10vh + 40px));
   background:#000;overflow:hidden}

.tb-streak{font-size:11px;font-weight:600;margin:0 2px}
.tb-mkt{font-size:11px;font-weight:600}
.tb-ping{font-size:8px;margin-left:4px;color:#00ff41}
.tb-ping.amb{color:#ffaa00}
.tb-ping.slow{color:#ff3131}
.sound-toggle{
  margin-left:6px;background:transparent;border:1px solid #00ff4133;color:#00ff41;
  font-family:inherit;font-size:12px;line-height:1;cursor:pointer;padding:2px 6px;
}
.sound-toggle.muted{color:#00ff4144;border-color:#00ff4118}

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
.tb-val.tb-bal{color:#00ff41;text-shadow:0 0 8px rgba(0,255,65,0.35)}
.tb-val.tb-orders{color:#00ff41;font-size:11px}
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
.mode-badge{margin-left:10px;padding:2px 8px;font-size:8px;font-weight:600;letter-spacing:2px}
.mode-badge.mode-paper{border:1px solid #ffffff55;color:#fff}
.mode-badge.mode-live{
  border:1px solid #ffaa0099;color:#ffb020;
  text-shadow:0 0 10px rgba(255,178,32,0.45);
}
.mm-mode-toggle{
  margin-left:6px;font-family:inherit;font-size:7px;font-weight:600;
  letter-spacing:1px;text-transform:uppercase;padding:4px 10px;
  cursor:pointer;background:transparent;transition:opacity .18s,color .12s;border-radius:0;
  vertical-align:middle;line-height:1.2;
}
.mm-mode-toggle:disabled{opacity:.5;cursor:wait}
.mm-mode-toggle-live{
  border:1px solid #ffaa0099;color:#ffffff;
}
.mm-mode-toggle-paper{
  border:1px solid #ffffff55;color:#ffb020;
  text-shadow:0 0 8px rgba(255,178,32,0.35);
}
.mm-mode-toggle.switching{border-color:#00ff4166;color:#00ff4188;text-shadow:none}

/* ═══════════════════════════════════════════
   MAIN AREA  (3 columns)
═══════════════════════════════════════════ */
#MA{
  display:grid;
  grid-template-columns:230px 1fr 228px;
  overflow:hidden;
  min-height:0;
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
#LF{min-height:0}
#lf-feed-block{
  flex:1;min-height:0;display:flex;flex-direction:column;overflow:hidden;
}

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

/* ── CENTER (2 rows) — equity above, depth always gets min height ── */
#CN{
  display:grid;
  grid-template-rows:minmax(140px,1fr) 280px;
  overflow:hidden;
  min-height:0;
}
#eq-pane{display:flex;flex-direction:column;overflow:hidden;border-bottom:1px solid #00ff4118;min-height:0}
#ob-pane{display:flex;flex-direction:column;overflow:hidden;height:280px;min-height:280px;max-height:280px}
#eq-wrap{flex:1;position:relative;min-height:0;padding:2px 4px 0 4px}
#equity-chart{width:100%!important;height:100%!important}
#ob-body{flex:1;overflow:hidden;padding:5px 8px;display:flex;flex-direction:column;justify-content:stretch}

/* order book rows */
.obr{display:grid;grid-template-columns:34px 1fr 30px;gap:4px;
     align-items:center;flex:1;min-height:0;font-size:9px}
.obp{text-align:right;color:#00ff4199}
.obq{color:#00ff4144;text-align:right;font-size:8px}
.obbar{height:78%;min-height:9px;background:#001100;position:relative;overflow:hidden}
.obbid{height:100%;background:#00ff4155;position:absolute;right:0}
.obask{height:100%;background:#ff313155;position:absolute;left:0}
.ob-mid{
  text-align:center;color:#ffaa00;font-size:9px;font-weight:600;
  padding:5px 0;margin:3px 0;flex:0 0 auto;
  border-top:1px solid #ffaa0033;border-bottom:1px solid #ffaa0033;
}

/* ── RIGHT ── */
#RC{border-right:none;display:grid;grid-template-rows:1fr 182px;min-height:0}
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
#btc-pane{display:flex;flex-direction:column;overflow:hidden;min-height:0}
#fr-wrap,#btc-wrap{flex:1;position:relative;padding:2px 4px;min-height:0}
#fr-wrap{min-height:88px}
#pnl-chart{width:100%!important;height:100%!important}

.vol-wrap{display:flex;align-items:center;gap:4px;margin-top:4px}
.vol-cap{font-size:6px;color:#00ff4188;text-transform:uppercase;letter-spacing:.5px;width:52px}
.vol-bar-track{flex:1;height:4px;background:#001100;border-radius:0;overflow:hidden}
.vol-bar{height:100%;background:#558866;min-width:0}
.vol-bar.vol-hi{background:linear-gradient(90deg,#553300,#ffaa00)}

/* ═══════════════════════════════════════════
   BOT CONTROLS STRIP
═══════════════════════════════════════════ */
#BOT-CTRL{
  border-top:1px solid #00ff4118;
  border-bottom:1px solid #00ff4133;
  background:#000811;
  overflow:auto;
  padding:0;
  max-height:192px;
  flex-shrink:0;
}
#bc-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:8px;align-items:start;padding:4px 8px 6px;height:auto;min-height:0}
.bc-card{
  border:1px solid #00ff4122;display:flex;flex-direction:column;
  min-height:0;overflow:hidden;background:#000;
}
.bc-hdr{
  flex-shrink:0;display:flex;align-items:center;gap:6px;padding:3px 6px;
  border-bottom:1px solid #00ff4118;flex-wrap:wrap;
}
.bc-title{font-size:8px;font-weight:600;letter-spacing:1.6px;color:#00ff4199;text-transform:uppercase;flex:1}
.bc-led{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.bc-led.on{background:#00ff41;box-shadow:0 0 6px #00ff41}
.bc-led.off{background:#ff3131;box-shadow:0 0 4px #ff313166}
.bc-btns{display:flex;gap:4px;flex-wrap:wrap}
.bc-btn{
  font-family:inherit;font-size:7px;font-weight:600;letter-spacing:0.5px;
  text-transform:uppercase;padding:3px 8px;cursor:pointer;border-radius:0;
  background:transparent;border:1px solid #00ff4144;color:#00ff41;
}
.bc-btn:hover{background:#00ff410d;border-color:#00ff4177}
.bc-btn:disabled{opacity:.4;cursor:not-allowed}
.bc-btn-danger{border-color:#ff313144;color:#ff6b6b}
.bc-btn-cancel-all{
  border-color:#cc2222!important;color:#ff3838!important;font-weight:700;
  text-shadow:0 0 8px rgba(255,56,56,0.25);
}
.bc-btn-cancel-all:hover{background:#44000033!important;border-color:#ff5555!important;color:#ff8a8a!important}
.bc-log{
  flex:0 0 auto;margin:0;padding:4px 6px;
  max-height:140px;
  overflow-x:hidden;
  overflow-y:scroll;
  font-size:8px;line-height:1.45;color:#00ff4199;white-space:pre-wrap;word-break:break-all;
  font-family:'IBM Plex Mono',monospace;background:#000402;
}
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
    <span class="tb-lbl">BAL</span><span id="tb-bal" class="tb-val tb-bal" title="Live Kalshi portfolio balance">—</span>
    <span id="tb-mkt" class="tb-val tb-mkt" title="Recent Kalshi sessions (momentum.log)">MKT …</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">P&amp;L</span><span id="tb-pnl" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span id="tb-streak" class="tb-val tb-streak g-faint" title="Recent closed-trade streak (mode-filtered MM)">—</span>
    <span class="tb-lbl">WIN</span><span id="tb-wr" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">FILLS</span><span id="tb-fills" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">SPD</span><span id="tb-spread" class="tb-val">—</span>
    <span id="tb-api-error" class="tb-api-err"></span>
    <div class="tb-spacer"></div>
    <span id="tb-updated"></span>
    <button type="button" id="sound-toggle" class="sound-toggle" title="Toggle fill sound alerts">🔊</button>
    <div class="bot-tag"><span class="bdot" id="dot-kalshi"></span>KALSHI <span id="tb-kal-ping" class="tb-ping" title="GET portfolio/balance latency">—</span></div>
    <div class="bot-tag"><span class="bdot" id="dot-coinbase"></span>COINBASE</div>
    <div class="bot-tag"><span class="bdot" id="dot-polymarket"></span>POLY</div>
    <span id="tb-mm-mode" class="mode-badge mode-paper">PAPER</span>
    <button type="button" id="btn-toggle-mm-mode" class="mm-mode-toggle mm-mode-toggle-paper" title="Restart market maker in the other mode">SWITCH TO LIVE</button>
  </div>

  <!-- ══ MAIN AREA ══ -->
  <div id="MA">

    <!-- LEFT: Trade Feed -->
    <div class="pane" id="LF">
      <div id="lf-feed-block">
        <div class="ph">
          <span class="ph-title">Live Fills</span>
          <span class="ph-meta g-faint" id="feed-mm-mode-meta">PAPER</span>
          <span class="ph-meta" id="feed-count">0 TRADES</span>
        </div>
        <div id="feed-list"></div>
      </div>
    </div>

    <!-- CENTER -->
    <div id="CN">

      <!-- P&L Equity Curve -->
      <div id="eq-pane">
        <div class="ph">
          <span class="ph-title" id="eq-pnl-title">PAPER P&amp;L</span>
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
          <span class="ph-meta" id="sp-mm-mode-meta">MM · PAPER · TODAY UTC</span>
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

  <!-- ══ BOT CONTROLS ══ -->
  <div id="BOT-CTRL">
    <div id="bc-grid">
      <div class="bc-card">
        <div class="bc-hdr">
          <span class="bc-led off" id="bc-led-mom" title="Process status"></span>
          <span class="bc-title">momentum_bot.py</span>
          <span class="bc-btns">
            <button type="button" class="bc-btn" id="bc-start-mom">Start</button>
            <button type="button" class="bc-btn bc-btn-danger" id="bc-stop-mom">Stop</button>
          </span>
        </div>
        <pre class="bc-log" id="bc-log-mom"></pre>
      </div>
      <div class="bc-card">
        <div class="bc-hdr">
          <span class="bc-led off" id="bc-led-mm"></span>
          <span class="bc-title">market_maker.py</span>
          <span class="bc-btns">
            <button type="button" class="bc-btn" id="bc-start-mm">Start</button>
            <button type="button" class="bc-btn bc-btn-danger" id="bc-stop-mm">Stop</button>
            <button type="button" class="bc-btn bc-btn-cancel-all" id="bc-cancel-all" title="Cancel all resting Kalshi orders">CANCEL ALL ORDERS</button>
          </span>
        </div>
        <pre class="bc-log" id="bc-log-mm"></pre>
      </div>
    </div>
  </div>

  <!-- ══ BOTTOM BAR ══ -->
  <div id="BB">

    <div id="fr-pane">
      <div class="ph">
        <span class="ph-title">Daily P&amp;L</span>
        <span class="ph-meta">7D UTC · mode-filtered MM</span>
      </div>
      <div id="fr-wrap">
        <canvas id="pnl-week-chart"></canvas>
      </div>
    </div>

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
const BTC_POLL_MS  = 10000;
const KAL_PING_MS  = 10000;

// ── State ────────────────────────────────────────────────
let equityChart       = null;
let pnlWeekChart      = null;
let btcCandles        = [];
let lastBtcFetch      = 0;
let lastFeedTs        = '';
let btcLayoutRetries  = 0;
let lastFillCount     = null;
let fillSoundMuted    = localStorage.getItem('fillSoundMuted') === '1';
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
const BOT_CTRL_POLL_MS = 2800;
const BAL_POLL_MS = 30000;

function updateSoundToggle() {
  const btn = document.getElementById('sound-toggle');
  if (!btn) return;
  btn.textContent = fillSoundMuted ? '🔇' : '🔊';
  btn.className = 'sound-toggle' + (fillSoundMuted ? ' muted' : '');
  btn.title = fillSoundMuted ? 'Fill sound alerts muted' : 'Fill sound alerts enabled';
}

function attachSoundToggle() {
  const btn = document.getElementById('sound-toggle');
  if (!btn || btn.dataset.bound === '1') return;
  btn.dataset.bound = '1';
  updateSoundToggle();
  btn.addEventListener('click', function () {
    fillSoundMuted = !fillSoundMuted;
    localStorage.setItem('fillSoundMuted', fillSoundMuted ? '1' : '0');
    updateSoundToggle();
  });
}

function currentFillCount(d) {
  const mmTotal = d && d.mm && d.mm.total;
  if (mmTotal != null && Number.isFinite(Number(mmTotal))) return Number(mmTotal);
  return Array.isArray(d && d.feed) ? d.feed.length : 0;
}

function playFillSoundOnce() {
  if (fillSoundMuted) return;
  const audio = new Audio('/static/order_fill.mp3');
  audio.volume = 0.6;
  audio.play().catch(() => {});
}

function maybePlayFillSounds(d) {
  const count = currentFillCount(d);
  if (lastFillCount === null) {
    lastFillCount = count;
    return;
  }
  const delta = Math.max(0, count - lastFillCount);
  for (let i = 0; i < delta; i++) playFillSoundOnce();
  lastFillCount = count;
}

async function pollKalshiBalance() {
  const balEl = document.getElementById('tb-bal');
  try {
    const res = await fetch('/api/balance', FETCH_CRED);
    const d = res.ok ? await res.json() : {};
    if (balEl) {
      if (d.ok && d.balance_usd != null && Number.isFinite(Number(d.balance_usd)))
        balEl.textContent = '$' + Number(d.balance_usd).toFixed(2);
      else balEl.textContent = '—';
    }
  } catch (_ex) {
    if (balEl) balEl.textContent = '—';
  }
}

async function refreshBotControls() {
  try {
    const res = await fetch('/api/bot-controls', FETCH_CRED);
    if (!res.ok) return;
    const d = await res.json();
    const mom = d.bots && d.bots.momentum;
    const mm = d.bots && d.bots.market_maker;
    var ledM = document.getElementById('bc-led-mom');
    var ledR = document.getElementById('bc-led-mm');
    if (ledM) ledM.className = 'bc-led ' + (mom && mom.running ? 'on' : 'off');
    if (ledR) ledR.className = 'bc-led ' + (mm && mm.running ? 'on' : 'off');
    var lm = document.getElementById('bc-log-mom');
    var lr = document.getElementById('bc-log-mm');
    if (lm && d.logs && d.logs.momentum != null) lm.textContent = d.logs.momentum || '(empty)';
    if (lr && d.logs && d.logs.market_maker != null) lr.textContent = d.logs.market_maker || '(empty)';
  } catch (_e) {}
}

function attachBotControlsHandlers() {
  async function act(action, bot) {
    try {
      var res = await fetch('/api/bot-controls', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ action: action, bot: bot }),
      });
      var j = await res.json().catch(function () { return {}; });
      if (!res.ok || !j.ok) {
        alert((j && j.error) || ('HTTP ' + res.status));
        return;
      }
      await refreshBotControls();
    } catch (e) {
      alert(String(e.message || e));
    }
  }
  var sm = document.getElementById('bc-start-mom');
  var xm = document.getElementById('bc-stop-mom');
  var sr = document.getElementById('bc-start-mm');
  var xr = document.getElementById('bc-stop-mm');
  if (sm) sm.addEventListener('click', function () { act('start', 'momentum'); });
  if (xm) xm.addEventListener('click', function () { act('stop', 'momentum'); });
  if (sr) sr.addEventListener('click', function () { act('start', 'market_maker'); });
  if (xr) xr.addEventListener('click', function () { act('stop', 'market_maker'); });

  var ca = document.getElementById('bc-cancel-all');
  if (ca) {
    ca.addEventListener('click', async function () {
      if (!confirm('Cancel all resting orders?')) return;
      ca.disabled = true;
      try {
        var res = await fetch('/api/cancel-all', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: '{}',
        });
        var j = await res.json().catch(function () { return {}; });
        if (j.ok) {
          alert('Cancelled ' + Number(j.cancelled || 0) + ' orders');
        } else if (Number(j.cancelled || 0) > 0) {
          alert(
            'Cancelled ' +
              Number(j.cancelled || 0) +
              ' orders (some failed):\n' +
              (j.errors || []).join('\n'),
          );
        } else {
          alert((j.errors && j.errors.join('\n')) || 'Cancel-all failed');
        }
      } catch (e) {
        alert(String(e.message || e));
      }
      ca.disabled = false;
    });
  }

}

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
  return { trades: 0, wins: 0, losses: 0, win_rate: 0, pnl: 0, last_price: 0, volatility_pct: 0 };
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
    weekly_pnl: [],
    streak: { kind: 'flat', n: 0, label: '', tone: '' },
    market_open: false,
    updated: '—',
    dashboard_mm: { paper: true, equity_pnl_title: 'PAPER P&L' },
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
          volatility_pct: Number(r.volatility_pct) || 0,
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

  if (Array.isArray(d.weekly_pnl)) {
    const wp = d.weekly_pnl
      .map(p => {
        if (!p || typeof p !== 'object') return null;
        return {
          date: String(p.date || ''),
          pnl: Number(p.pnl) || 0,
          trades: Number(p.trades) || 0,
        };
      })
      .filter(Boolean);
    if (wp.length) z.weekly_pnl = wp;
  }

  if (d.streak && typeof d.streak === 'object') {
    z.streak = {
      kind: String(d.streak.kind || 'flat'),
      n: Number(d.streak.n) || 0,
      label: String(d.streak.label || ''),
      tone: String(d.streak.tone || ''),
    };
  }

  if ('market_open' in d && d.market_open !== null && d.market_open !== undefined)
    z.market_open = !!d.market_open;

  if (d.updated != null && d.updated !== '') z.updated = String(d.updated);

  if (d.dashboard_mm && typeof d.dashboard_mm === 'object') {
    z.dashboard_mm.paper = d.dashboard_mm.paper === true;
    const et =
      typeof d.dashboard_mm.equity_pnl_title === 'string'
        ? d.dashboard_mm.equity_pnl_title
        : '';
    if (et) z.dashboard_mm.equity_pnl_title = et;
    else z.dashboard_mm.equity_pnl_title = z.dashboard_mm.paper ? 'PAPER P&L' : 'LIVE P&L';
  }

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

  // 7-day P&L (UTC, mode-filtered on server)
  const pnlEl = document.getElementById('pnl-week-chart');
  if (!pnlEl) return;
  const fCtx = pnlEl.getContext('2d');
  const today = new Date();
  const dayLbl = (_, i) => {
    const d = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() - 6 + i));
    return (d.getUTCMonth() + 1) + '/' + String(d.getUTCDate()).padStart(2, '0');
  };
  pnlWeekChart = new Chart(fCtx, {
    type: 'bar',
    data: {
      labels: Array.from({ length: 7 }, dayLbl),
      datasets: [{
        data: Array(7).fill(0),
        backgroundColor: Array(7).fill('rgba(0,255,65,0.45)'),
        borderColor: Array(7).fill('#00ff41aa'),
        borderWidth: 0.5,
        borderRadius: 0,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#001100',
          borderColor: '#00ff4155',
          borderWidth: 1,
          titleColor: '#00ff4166',
          bodyColor: '#00ff41',
          titleFont: { family: FONT, size: 8 },
          bodyFont: { family: FONT, size: 9 },
          callbacks: {
            afterLabel(ctx) {
              const w = (ctx.chart.data._wp && ctx.chart.data._wp[ctx.dataIndex]) || {};
              const t = Number(w.trades) || 0;
              return 'trades ' + t;
            },
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          border: { color: '#00ff4122' },
          ticks: { color: '#00ff4144', font: { family: FONT, size: 7 }, maxRotation: 0 },
        },
        y: {
          grid: { color: GRID, drawBorder: false },
          border: { color: '#00ff4122' },
          ticks: {
            color: '#00ff4144',
            font: { family: FONT, size: 8 },
            maxTicksLimit: 4,
            callback: v => (v >= 0 ? '+' : '') + '$' + Number(v).toFixed(2),
          },
        },
      },
    },
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

  const mk = document.getElementById('tb-mkt');
  if (mk) {
    const mo = !!d.market_open;
    mk.textContent = mo ? 'MKT OPEN' : 'MKT CLOSED';
    mk.className = 'tb-val tb-mkt ' + (mo ? 'pos' : 'neg');
    mk.title = 'Momentum log Kalshi sessions (recent "New session" lines)';
  }

  const stEl = document.getElementById('tb-streak');
  if (stEl) {
    const sk = d.streak && d.streak.label ? String(d.streak.label) : '';
    stEl.textContent = sk || '—';
    const tone =
      d.streak && d.streak.tone === 'pos'
        ? 'pos'
        : d.streak && d.streak.tone === 'neg'
          ? 'neg'
          : 'g-faint';
    stEl.className = 'tb-val tb-streak ' + tone;
    stEl.title = 'Recent consecutive closes (mode-filtered MM JSONL)';
  }

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
    const d =
      series[s] || {
        trades: 0,
        wins: 0,
        win_rate: 0,
        pnl: 0,
        last_price: 0,
        volatility_pct: 0,
      };
    const pcl = d.pnl >= 0 ? 'pos' : 'neg';
    const pnlStr = (d.pnl >= 0 ? '+' : '') + '$' + Math.abs(d.pnl).toFixed(4);
    const lastStr = d.last_price ? d.last_price + '¢' : '—';
    const vpc = Number(d.volatility_pct) || 0;
    const vw = Math.max(6, Math.min(100, vpc * 6));
    const vHi = vpc >= 0.75;
    html += `<div class="sb">
      <div class="sb-hdr">
        <span class="sb-name">${SHORT[s]} <span class="sb-sub">${s}</span></span>
        <span class="sb-pnl ${pcl}">${pnlStr}</span>
      </div>
      <div class="wbar-wrap"><div class="wbar-fill" style="width:${d.win_rate}%"></div></div>
      <div class="sb-meta">
        <span>WIN <span class="${d.win_rate >= 50 ? 'pos' : 'neg'}">${d.win_rate}%</span></span>
        <span>FLS <span class="g">${d.trades}</span></span>
        <span>Y+N <span class="amb">${lastStr}</span></span>
      </div>
      <div class="vol-wrap">
        <span class="vol-cap">VOL 15m</span>
        <div class="vol-bar-track"><div class="vol-bar ${vHi ? 'vol-hi' : ''}" style="width:${vw}%"></div></div>
        <span class="g-dim" style="font-size:7px">${vpc.toFixed(2)}%</span>
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

// ── 7-day P&L bars ────────────────────────────────────────
function renderWeeklyPnl(rows) {
  if (!pnlWeekChart) return;
  let r = Array.isArray(rows) ? [...rows] : [];
  if (r.length > 7) r = r.slice(-7);
  while (r.length < 7) r.unshift({ date: '', pnl: 0, trades: 0 });
  const labels = r.map(w => {
    const ds = String((w && w.date) || '');
    const p = ds.split('-');
    if (p.length >= 3)
      return String(Number(p[1])) + '/' + String(Number(p[2]));
    return '?';
  });
  const vals = r.map(w => Number((w && w.pnl) || 0));
  const colors = vals.map(v => (v >= 0 ? 'rgba(0,255,65,0.52)' : 'rgba(255,49,49,0.52)'));
  const borders = vals.map(v => (v >= 0 ? '#00ff41cc' : '#ff3131cc'));
  pnlWeekChart.data.labels = labels;
  pnlWeekChart.data.datasets[0].data = vals;
  pnlWeekChart.data.datasets[0].backgroundColor = colors;
  pnlWeekChart.data.datasets[0].borderColor = borders;
  pnlWeekChart.data._wp = r;
  pnlWeekChart.update('none');
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

  const PAD_T = 6, PAD_B = 14, PAD_L = 4, PAD_R = 70;
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

// ── MM display: badge / labels match market_maker PAPER_MODE ─────────────
function updateMmModeToggleButton(paper) {
  const btn = document.getElementById('btn-toggle-mm-mode');
  if (!btn || btn.disabled) return;
  if (paper) {
    btn.textContent = 'SWITCH TO LIVE';
    btn.className = 'mm-mode-toggle mm-mode-toggle-paper';
    btn.dataset.currentMode = 'paper';
  } else {
    btn.textContent = 'SWITCH TO PAPER';
    btn.className = 'mm-mode-toggle mm-mode-toggle-live';
    btn.dataset.currentMode = 'live';
  }
}

function attachMmModeToggleHandler() {
  const btn = document.getElementById('btn-toggle-mm-mode');
  if (!btn || btn.dataset.bound === '1') return;
  btn.dataset.bound = '1';
  btn.addEventListener('click', async function () {
    if (btn.disabled) return;
    btn.disabled = true;
    btn.textContent = 'Switching...';
    btn.className = 'mm-mode-toggle switching';
    try {
      const res = await fetch('/api/toggle-mode', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: '{}',
      });
      const txt = await res.text();
      if (!res.ok) {
        let err = txt || ('HTTP ' + res.status);
        try {
          var j = JSON.parse(txt);
          if (j && j.error) err = j.error;
        } catch (_e) {}
        console.warn('[toggle-mode]', err);
        alert('Toggle failed: ' + err);
        btn.disabled = false;
        var bdFail = document.getElementById('tb-mm-mode');
        updateMmModeToggleButton(!!(bdFail && bdFail.textContent === 'PAPER'));
        return;
      }
    } catch (e) {
      console.error('[toggle-mode]', e);
      alert(String(e.message || e));
      btn.disabled = false;
      var bdErr = document.getElementById('tb-mm-mode');
      updateMmModeToggleButton(!!(bdErr && bdErr.textContent === 'PAPER'));
      pollTerminal();
      return;
    }
    await new Promise(function (resolve) { setTimeout(resolve, 3000); });
    window.location.reload();
  });
}

function applyMmDashboardDisplay(dm) {
  const paper = !!(dm && dm.paper === true);
  const el = document.getElementById('tb-mm-mode');
  if (el) {
    el.textContent = paper ? 'PAPER' : 'LIVE';
    el.className = 'mode-badge ' + (paper ? 'mode-paper' : 'mode-live');
  }
  updateMmModeToggleButton(paper);
  const eqEl = document.getElementById('eq-pnl-title');
  const title =
    dm && typeof dm.equity_pnl_title === 'string' && dm.equity_pnl_title
      ? dm.equity_pnl_title
      : (paper ? 'PAPER P&L' : 'LIVE P&L');
  if (eqEl) eqEl.textContent = title;
  const fm = document.getElementById('feed-mm-mode-meta');
  if (fm) fm.textContent = paper ? 'PAPER' : 'LIVE';
  const sp = document.getElementById('sp-mm-mode-meta');
  if (sp) sp.textContent = paper ? 'MM · PAPER · TODAY UTC' : 'MM · LIVE · TODAY UTC';
}

async function pollKalshiLatency() {
  const el = document.getElementById('tb-kal-ping');
  if (!el) return;
  try {
    const res = await fetch('/api/kalshi-latency', FETCH_CRED);
    const d = res.ok ? await res.json() : {};
    const ms = d.ms != null ? Number(d.ms) : NaN;
    if (!Number.isFinite(ms)) {
      const er = d.error != null ? String(d.error) : '—';
      el.textContent = er.length > 22 ? er.slice(0, 22) + '…' : er;
      el.className = 'tb-ping slow';
      return;
    }
    el.textContent = ms + 'ms';
    el.className = ms < 100 ? 'tb-ping' : ms < 500 ? 'tb-ping amb' : 'tb-ping slow';
  } catch (_e) {
    el.textContent = '—';
    el.className = 'tb-ping slow';
  }
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
    maybePlayFillSounds(data);
    setTerminalApiError('');
    applyMmDashboardDisplay(data.dashboard_mm || {});

    renderTopBar(data);
    renderFeed(data.feed);
    renderEquity(data.equity);
    renderOrderBook(data.orderbook);
    renderSeries(data.series, data.mm);
    renderConsole(data.console);
    renderWeeklyPnl(data.weekly_pnl);
  } catch (e) {
    console.error('[terminal] pollTerminal', e);
    setTerminalApiError(String(e.message || e));
  }
  // Keep BTC canvas sized after equity/layout updates.
  if (btcCandles.length)
    renderBtcCandles(btcCandles);
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
  attachSoundToggle();
  attachMmModeToggleHandler();
  attachBotControlsHandlers();
  pollTerminal();
  refreshBotControls();
  pollKalshiBalance();
  pollKalshiLatency();
});
setInterval(refreshBotControls, BOT_CTRL_POLL_MS);
setInterval(pollKalshiBalance, BAL_POLL_MS);
setInterval(pollKalshiLatency, KAL_PING_MS);
pollBtcCandles();
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
