# Stopping Rule

This document defines the time budget and stopping criteria for the current
Kalshi trading-system project.

The goal is to build a profitable trading system that can eventually scale. The
current codebase is valuable infrastructure, but the blind market-making strategy
must earn its place with data.

## Phase 1: Current Blind Market-Making Branch

Time budget from May 15, 2026: at most 2 weeks.

Scope:

- Finish the 95c crypto test on `KXETH15M` and `KXSOL15M`.
- Apply the Plan A gate in `PLAN_B_VIABILITY_CRITERIA.md` strictly.
- If Plan A fails, optionally run NBA totals observation for 3-5 game nights.
- Do not quote NBA totals unless observation passes the Plan B gate.
- Do not continue tuning blind Kalshi market making after both Plan A and Plan B
  fail without writing a new thesis first.

Decision:

- If Plan A passes, continue validating the crypto configuration before any live
  capital.
- If Plan A fails and Plan B observation passes, adapt the bot only after writing
  the required sports-specific design changes.
- If Plan A fails and Plan B observation fails, stop the blind market-making
  branch.

## Phase 2: Latency/Event-Driven Feasibility

Only begin this phase if Phase 1 fails or if the blind market-making branch is
explicitly paused.

Time budget: at most 4 weeks for feasibility, not a full production build.

The purpose is to test whether an external signal reliably leads Kalshi prices
after fees and realistic execution assumptions.

Required measurements:

- Coinbase WebSocket move timestamps versus Kalshi crypto order book repricing
- Kalshi WebSocket latency and REST order-placement latency
- Frequency and duration of stale Kalshi quotes after external crypto moves
- Paper-simulated taker entries against live observed books, including fees and
  slippage assumptions
- If sports observation is active, public score-feed timestamps versus Kalshi
  sports market repricing

Pass condition:

- Repeated, measurable external-signal lead over Kalshi repricing
- Signal persists after fees, slippage, and realistic latency assumptions
- Enough opportunity frequency to justify a 2-3 month production build

Fail condition:

- No repeated external lead
- Edge disappears after fees/slippage/latency
- Opportunity frequency is too low to scale
- Results remain ambiguous after the 4-week feasibility budget

If Phase 2 fails, stop this project as a trading system.

## Writeup Commitment

If the project stops after Phase 1 or Phase 2, preserve the value of the work in a
technical writeup before starting another trading project.

The writeup should include:

- Summary of Plan A, Plan B, and any Plan C feasibility work
- Bugs found and how each was diagnosed
- Fee and market-structure findings
- What the infrastructure now supports
- What would be required for profitability
- What should be reused or avoided in the next project

The expected output is a clean repository plus a technical post-mortem. That is a
valid project outcome if the trading strategy does not pass its gates.

## Hard Constraint

Do not let this project become an open-ended sequence of parameter tweaks.

If a new idea appears after a fail condition, write a new thesis with:

- Hypothesis
- Required data
- Pass/fail gate
- Time budget
- What existing code can be reused

No new blind-quoting market is accepted without passing the blind-quoting gate in
`PLAN_B_VIABILITY_CRITERIA.md`.
