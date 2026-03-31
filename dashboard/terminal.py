"""
dashboard/terminal.py — Rich live terminal dashboard.

Shows:
  - Live P&L (paper or real)
  - Open positions
  - Recent trade decisions (approved + skipped)
  - Top markets by volume
  - Bot status / last scan time
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional

from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from config import settings

console = Console()

# ---------------------------------------------------------------------------
# Shared state — updated by other modules via the setters below
# ---------------------------------------------------------------------------

_state: dict = {
    "portfolio_value": 0.0,
    "starting_value": 0.0,
    "daily_pnl": 0.0,
    "total_pnl": 0.0,
    "positions": [],        # list[dict]  (ticker, side, qty, avg_price, cur_price)
    "decisions": [],        # list[dict]  (recent TradeDecision dicts, newest first)
    "top_markets": [],      # list[Market]
    "last_scan": None,      # datetime | None
    "daily_loss": 0.0,
    "kill_switch_active": False,
    "mode": "PAPER" if settings.paper_trading else "LIVE",
    "scan_count": 0,
    "error_msg": None,
}


# ---------------------------------------------------------------------------
# Public setters called by other modules
# ---------------------------------------------------------------------------

def update_portfolio(
    portfolio_value: float,
    starting_value: float,
    daily_pnl: float,
    total_pnl: float,
    daily_loss: float,
    kill_switch: bool,
) -> None:
    _state.update(
        portfolio_value=portfolio_value,
        starting_value=starting_value,
        daily_pnl=daily_pnl,
        total_pnl=total_pnl,
        daily_loss=daily_loss,
        kill_switch_active=kill_switch,
    )


def update_positions(positions: list[dict]) -> None:
    _state["positions"] = positions


def add_decision(decision: dict) -> None:
    """Prepend a decision; keep the most recent 50."""
    _state["decisions"].insert(0, decision)
    _state["decisions"] = _state["decisions"][:50]


def update_markets(markets: list) -> None:
    _state["top_markets"] = markets[:15]
    _state["last_scan"] = datetime.utcnow()
    _state["scan_count"] += 1


def set_error(msg: Optional[str]) -> None:
    _state["error_msg"] = msg


# ---------------------------------------------------------------------------
# Renderable builders
# ---------------------------------------------------------------------------

def _header() -> Panel:
    mode = _state["mode"]
    mode_color = "red bold" if mode == "LIVE" else "yellow bold"
    scan_info = (
        f"Scan #{_state['scan_count']}  "
        f"Last: {_state['last_scan'].strftime('%H:%M:%S') if _state['last_scan'] else '--:--:--'}"
    )
    title = Text()
    title.append("Kalshi AI Trading Bot  ", style="bold white")
    title.append(f"[{mode}]", style=mode_color)
    title.append(f"  {scan_info}", style="dim")
    return Panel(title, box=box.HORIZONTALS, padding=(0, 1))


def _pnl_panel() -> Panel:
    pv = _state["portfolio_value"]
    sv = _state["starting_value"]
    dpnl = _state["daily_pnl"]
    tpnl = _state["total_pnl"]
    dl = _state["daily_loss"]
    ks = _state["kill_switch_active"]

    def _fmt(val: float) -> Text:
        color = "green" if val >= 0 else "red"
        sign = "+" if val >= 0 else ""
        return Text(f"{sign}${val:,.2f}", style=color)

    table = Table.grid(padding=(0, 2))
    table.add_column(style="dim")
    table.add_column()
    table.add_row("Portfolio", f"${pv:,.2f}")
    table.add_row("Starting value", f"${sv:,.2f}")
    table.add_row("Daily P&L", _fmt(dpnl))
    table.add_row("Total P&L", _fmt(tpnl))
    table.add_row("Daily loss", Text(f"${dl:,.2f}", style="red" if dl > 0 else "default"))
    table.add_row(
        "Kill switch",
        Text("ACTIVE — trading halted", style="red bold") if ks else Text("off", style="dim"),
    )

    border_style = "red" if ks else ("green" if dpnl >= 0 else "red")
    return Panel(table, title="[bold]P&L[/bold]", border_style=border_style, box=box.ROUNDED)


def _positions_panel() -> Panel:
    positions = _state["positions"]
    table = Table(
        "Ticker", "Side", "Qty", "Avg", "Current", "Unrealized",
        box=box.SIMPLE_HEAD,
        header_style="bold cyan",
        show_edge=False,
        min_width=55,
    )

    if not positions:
        table.add_row("[dim]No open positions[/dim]", "", "", "", "", "")
    else:
        for pos in positions:
            unreal = (pos.get("cur_price", 0) - pos.get("avg_price", 0)) * pos.get("qty", 0)
            color = "green" if unreal >= 0 else "red"
            sign = "+" if unreal >= 0 else ""
            table.add_row(
                pos.get("ticker", ""),
                pos.get("side", ""),
                str(pos.get("qty", 0)),
                f"{pos.get('avg_price', 0):.0f}¢",
                f"{pos.get('cur_price', 0):.0f}¢",
                Text(f"{sign}${unreal:.2f}", style=color),
            )

    return Panel(table, title="[bold]Open Positions[/bold]", box=box.ROUNDED)


def _decisions_panel() -> Panel:
    decisions = _state["decisions"][:12]
    table = Table(
        "Time", "Ticker", "Signal", "Conf", "Edge", "Size", "Status",
        box=box.SIMPLE_HEAD,
        header_style="bold cyan",
        show_edge=False,
        min_width=80,
    )

    if not decisions:
        table.add_row("[dim]No decisions yet[/dim]", "", "", "", "", "", "")
    else:
        for d in decisions:
            ts = d.get("timestamp")
            time_str = ts.strftime("%H:%M:%S") if isinstance(ts, datetime) else str(ts)[:8]
            signal = d.get("signal", "HOLD")
            approved = d.get("approved", False)
            status_text = Text(
                "PLACED" if approved else "SKIP",
                style="green bold" if approved else "dim",
            )
            signal_color = {
                "BUY_YES": "green",
                "BUY_NO": "red",
                "HOLD": "dim",
            }.get(signal, "default")

            table.add_row(
                time_str,
                d.get("market_id", "")[:20],
                Text(signal, style=signal_color),
                f"{d.get('confidence', 0):.0%}",
                f"{d.get('edge', 0):+.1%}",
                f"${d.get('position_size_usd', 0):.2f}",
                status_text,
            )

    return Panel(table, title="[bold]Recent Decisions[/bold]", box=box.ROUNDED)


def _markets_panel() -> Panel:
    markets = _state["top_markets"]
    table = Table(
        "Ticker", "YES bid", "YES ask", "Mid", "Volume",
        box=box.SIMPLE_HEAD,
        header_style="bold cyan",
        show_edge=False,
        min_width=55,
    )

    if not markets:
        table.add_row("[dim]Waiting for scan...[/dim]", "", "", "", "")
    else:
        for m in markets[:10]:
            mid = m.mid_price if hasattr(m, "mid_price") else 0.5
            table.add_row(
                m.ticker[:24],
                f"{m.yes_bid}¢",
                f"{m.yes_ask}¢",
                f"{mid:.0%}",
                f"${m.volume:,.0f}",
            )

    return Panel(table, title="[bold]Top Markets by Volume[/bold]", box=box.ROUNDED)


def _error_bar() -> Optional[Panel]:
    msg = _state.get("error_msg")
    if not msg:
        return None
    return Panel(
        Text(f"ERROR: {msg}", style="red bold"),
        box=box.HORIZONTALS,
        border_style="red",
    )


# ---------------------------------------------------------------------------
# Layout assembly
# ---------------------------------------------------------------------------

def _build_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=2),
    )
    layout["left"].split_column(
        Layout(name="pnl"),
        Layout(name="markets"),
    )
    layout["right"].split_column(
        Layout(name="positions"),
        Layout(name="decisions"),
    )
    return layout


def _render(layout: Layout) -> Layout:
    layout["header"].update(_header())
    layout["pnl"].update(_pnl_panel())
    layout["markets"].update(_markets_panel())
    layout["positions"].update(_positions_panel())
    layout["decisions"].update(_decisions_panel())

    err = _error_bar()
    layout["footer"].update(
        err
        if err
        else Panel(
            Text(
                f"Interval: {settings.scan_interval}s  "
                f"Min edge: {settings.min_edge_pct:.0%}  "
                f"Min conf: {settings.min_confidence:.0%}  "
                f"Max pos: {settings.max_position_pct:.0%}  "
                f"Daily limit: ${settings.daily_loss_limit_usd:.0f}",
                style="dim",
            ),
            box=box.HORIZONTALS,
        )
    )
    return layout


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run(refresh_rate: float = 1.0) -> None:
    """Run the live dashboard until cancelled.

    Call this as an asyncio task alongside the trading loop.
    """
    layout = _build_layout()
    with Live(
        _render(layout),
        console=console,
        screen=True,
        refresh_per_second=int(1 / refresh_rate),
    ) as live:
        try:
            while True:
                live.update(_render(layout))
                await asyncio.sleep(refresh_rate)
        except asyncio.CancelledError:
            pass
