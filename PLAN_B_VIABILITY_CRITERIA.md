# Plan B Viability Criteria

This document is a pre-commitment gate for any pivot from the current crypto
market-making experiment to NBA totals or other maker-fee sports markets.

It exists to prevent post-hoc rationalization. Do not relax these criteria after
looking at the data unless the change is documented as a new experiment.

## Current Plan A

Plan A is the 95c crypto market-making test on `KXETH15M` and `KXSOL15M`.

Clean test window begins after the 95c cap deploy:

- Cutoff: `2026-05-14T21:37:00Z`
- Enabled series: `KXETH15M,KXSOL15M`
- Paired quote cap: `YES_lim + NO_lim <= 95c`

Plan A only passes if the clean window shows all of the following:

- At least 25 true quote-window paired fills in 24 hours, or 50 true pairs total
- True paired gross spread averages at least 4c
- Net realized P&L after estimated fees and FORCE_CLOSE/unpaired costs is at
  least +1c per true pair
- Paired/unpaired ratio is at least 4:1

If Plan A fails or remains ambiguous after sufficient observation, Plan B may be
activated as observation only.

## Plan B Candidate

Primary candidate:

- `KXNBATOTAL`

Secondary candidate:

- `KXNBASPREAD`

Rejected for this bot unless a separate thesis is written:

- Macro series such as `KXFED`, `KXCPI`, `KXPAYROLLS`
- Long-duration or low-flow sports series such as `KXNCAAFPLAYOFF`
- High-flow 1c-spread winner markets unless a specific queue or latency edge is
  demonstrated

## Required Sports Time Field

Sports markets can have administrative `close_time` and `expiration_time` values
that are days or weeks after the actual event.

For sports observation and any future hard-close logic, use:

- `expected_expiration_time`, if present
- otherwise `occurrence_datetime`, if present
- only fall back to `close_time` if neither event-time field exists

Example observed on `KXNBATOTAL`:

- `expected_expiration_time`: `2026-05-16T02:00:00Z`
- `close_time`: `2026-05-29T23:00:00Z`

Using `close_time` for sports would make hard-close and time-to-event logic
wrong.

## Observation-First Rule

Do not adapt the market maker to quote `KXNBATOTAL` directly.

If Plan B activates, first build and run a read-only NBA totals observation
collector for 3-5 game nights. The collector should snapshot order book depth at
least every 30 seconds during active games and answer these questions:

- Does depth asymmetry persist, or does it flip during the game?
- Which time buckets have both spread and volume?
- How large and frequent are price jumps around game events?
- Does inside depth actually turn over enough for passive orders to fill?

## Plan B Pass Gate

Plan B is viable only if observation finds at least one repeatable game-time
bucket where all of the following are true:

- Average spread is at least 4c, or average spread is at least 3c with unusually
  strong queue turnover and low jump risk
- Top-5 depth is meaningfully present on both sides, not one-sided decoration
- Depth ratio is usually between 0.5x and 2.0x, or flips often enough to be a
  measurable signal
- Inside turnover is high enough that small passive orders could plausibly fill
- Price jumps larger than 5c are rare enough that stale quotes are not routinely
  farmed

If Plan B fails this gate after 3-5 game nights, do not adapt the market maker to
NBA totals.

## Blind Quoting Acceptance Gate

No new blind-quoting market should be accepted unless it meets all of these
conditions before quoting begins:

- Spreads are at least 5c in the windows where the bot would actually be active,
  or at least 3c with maker fees and strong queue turnover evidence
- Depth on both sides is within 2x during the bot's intended quote duration, or
  the asymmetry is proven to flip in a measurable way
- Inside turnover indicates passive fills are plausible
- Paper or observation-derived simulation has positive expected value after fees
  and inventory/flattening costs over at least 200 paired fills or equivalent
  replay opportunities

## Plan C

If Plan A fails and Plan B observation fails, stop searching for blind quoting
targets on Kalshi without a new written thesis.

The preferred Plan C is an event-driven or latency-driven strategy:

- Use external feeds such as Coinbase WebSocket prices or sports score feeds
- Compare external moves against Kalshi order book repricing
- Trade only when an external signal reliably leads Kalshi prices

This is a different strategy from blind market making and should be treated as a
new build, not a parameter tweak.
