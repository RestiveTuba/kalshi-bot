from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

import market_maker as mm


class LedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.old_ledger = mm._ledger
        self.ledger = mm.MarketMakerLedger(Path(self.tmp.name) / "market_maker_ledger.jsonl")
        mm._ledger = self.ledger

    def tearDown(self) -> None:
        mm._ledger = self.old_ledger
        self.tmp.cleanup()

    def test_lots_survive_hard_close_and_reload(self) -> None:
        st = mm.MMState(series="KXBTC15M", ticker="KXBTC15M-TEST")
        self.ledger.record_fill(
            ticker=st.ticker,
            series=st.series,
            side="YES",
            qty=1,
            price_cents=90,
            order_id="order-1",
            ts="2026-05-06T00:00:00Z",
        )

        asyncio.run(mm._flatten_all(None, st, 88, 10, "HARD_CLOSE", "00:00:01"))

        self.assertEqual(self.ledger.open_qty(ticker=st.ticker, side="YES"), 1)
        reloaded = mm.MarketMakerLedger(self.ledger.path)
        self.assertEqual(reloaded.open_qty(ticker=st.ticker, side="YES"), 1)
        self.assertEqual(reloaded.close_intents[st.ticker], "LET_SETTLE")

    def test_settlement_realizes_pnl_and_closes_only_matching_lots(self) -> None:
        self.ledger.record_fill(
            ticker="KXBTC15M-A",
            series="KXBTC15M",
            side="YES",
            qty=1,
            price_cents=92,
            order_id="a",
            ts="2026-05-06T00:00:00Z",
        )
        self.ledger.record_fill(
            ticker="KXETH15M-B",
            series="KXETH15M",
            side="YES",
            qty=1,
            price_cents=91,
            order_id="b",
            ts="2026-05-06T00:01:00Z",
        )

        pnl = self.ledger.record_settlement(ticker="KXBTC15M-A", series="KXBTC15M", result="YES")

        self.assertAlmostEqual(pnl, 0.08)
        self.assertEqual(self.ledger.open_qty(ticker="KXBTC15M-A"), 0)
        self.assertEqual(self.ledger.open_qty(ticker="KXETH15M-B"), 1)

    def test_manual_close_preserves_unrelated_lots(self) -> None:
        self.ledger.record_fill(
            ticker="KXSOL15M-A",
            series="KXSOL15M",
            side="NO",
            qty=1,
            price_cents=89,
            order_id="a",
        )
        self.ledger.record_fill(
            ticker="KXSOL15M-B",
            series="KXSOL15M",
            side="NO",
            qty=1,
            price_cents=88,
            order_id="b",
        )

        pnl = self.ledger.record_manual_close(
            ticker="KXSOL15M-A",
            series="KXSOL15M",
            side="NO",
            qty=1,
            exit_price_cents=84,
        )

        self.assertAlmostEqual(pnl, -0.05)
        self.assertEqual(self.ledger.open_qty(ticker="KXSOL15M-A"), 0)
        self.assertEqual(self.ledger.open_qty(ticker="KXSOL15M-B"), 1)

    def test_reentry_risk_uses_open_exposure(self) -> None:
        st = mm.MMState(series="KXBTC15M", ticker="KXBTC15M-RISK")
        self.ledger.record_fill(
            ticker=st.ticker,
            series=st.series,
            side="YES",
            qty=1,
            price_cents=90,
            order_id="risk",
        )

        self.assertLessEqual(mm._risk_pnl_with_open_exposure(st), -0.90)

    def test_reconcile_mismatch_halts_series(self) -> None:
        self.ledger.record_reconcile_mismatch(
            ticker="KXBTC15M-MISMATCH",
            series="KXBTC15M",
            ts="2026-05-06T00:00:00Z",
            raw={"ledger_yes": 1, "kalshi_yes": 0},
        )

        self.assertIn("KXBTC15M", self.ledger.reconcile_halted_series)


if __name__ == "__main__":
    unittest.main()
