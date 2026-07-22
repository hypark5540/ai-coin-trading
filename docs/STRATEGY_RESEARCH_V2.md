# Strategy Research V2 Protocol

Status: frozen development protocol, 2026-07-21 KST.

This protocol was written after the existing 72-hour model and the 2025-07-19
through 2026-07-19 results had already been inspected.  Those results are
therefore contaminated diagnostics, not a fresh holdout.  Nothing in this
document is a profit promise.

## Decision

The active champion is cash/observe-only.  The HFT
`diagnostic-imbalance-flow-v0` policy is a plumbing diagnostic and is not
eligible for promotion.  It must not be retuned or restarted as an alpha model.

The next cycle evaluates at most three new challengers plus one fixed control.
The same parameters apply to every market; results may not be used to add,
remove, or customize a market during this cycle.

## Frozen universe and capital

- Markets: `KRW-BTC`, `KRW-ETH`, `KRW-XRP`, `KRW-SOL`.
- Bar interval: 60 minutes.
- Portfolio: four independent sleeves at 25% of initial capital each.
- Costs: 5 bp fee plus 5 bp slippage per side; a 2x-cost run must reuse the
  exact same prediction/signal manifest.
- Missing candle intervals start a new feature segment.  Rolling features may
  not cross a gap.

## Candidate registry

- `C0_CONTROL_ER72`: the already-seen 72-hour expected-return ridge model with
  SMA168 regime gate.  Comparison only; never promotable in this cycle.
- `C1_SLOW_ER168`: 168-hour expected-return ridge model, SMA336 regime gate,
  minimum net edge 0.30%, weekly retraining, and identical parameters across
  all four markets.
- `C2_TREND_336_168`: enter after a completed bar closes strictly above the
  prior 336-bar high and SMA336; exit after a completed bar closes strictly
  below the prior 168-bar low, with a 336-bar maximum holding horizon.  Orders
  execute at the next bar open and retain the common risk stops.
- `C3_ENSEMBLE_50_50`: fixed 50/50 capital split between C1 and C2 within every
  market sleeve.  No result-dependent weighting.

No fifth candidate or parameter sweep is allowed in V2.  If every challenger
fails, cash remains champion and a later hypothesis starts a new version.

## Historical development windows

All windows are half-open UTC intervals and each begins from cash:

- `D1`: `[2024-07-19T07:00:00Z, 2025-01-19T07:00:00Z)`
- `D2`: `[2025-01-19T07:00:00Z, 2025-07-19T07:00:00Z)`
- `D3_CONTAMINATED`: `[2025-07-19T07:00:00Z, 2026-07-19T07:00:00Z)`

D3 is reported for transparency but cannot affect selection, ranking, or
promotion.  Walk-forward fitting must purge at least the candidate horizon and
use only labels observable at prediction time.

## Development gate

A challenger passes development only if all conditions hold:

- D1 and D2 portfolio total return are each greater than zero at base cost.
- D1 and D2 portfolio total return are each greater than zero at 2x cost.
- Combined profit factor is at least 1.15.
- Worst-fold portfolio drawdown is no more than 10%.
- At least three of four market sleeves have non-negative cumulative return.
- At least 30 portfolio round trips occur; fewer is
  `INSUFFICIENT_EVIDENCE`, not a pass.
- Base and 2x-cost executions share the same immutable signal hash.

If more than one challenger passes, select the largest minimum D1/D2 2x-cost
return.  Break a tie with lower worst-fold drawdown.  D3 is never a tiebreaker.

## Frozen run result

The 2026-07-21 run used all four markets, found no invalid OHLCV rows, reused
the same signal manifests for base and 2x-cost execution, and recorded
calculation-code fingerprint
`09f09b04e5724d3ddc40b6eb3d4c10676bc927ac89326a239324ec29027067e5`.

| Candidate | D1 base / 2x | D2 base / 2x | D3 contaminated base / 2x | Status |
|---|---:|---:|---:|---|
| C0 control | -1.879% / -2.154% | +0.517% / +0.202% | +0.370% / -0.262% | comparison only |
| C1 slow ER168 | +0.488% / +0.095% | -1.528% / -1.934% | +0.444% / -0.048% | FAIL |
| C2 trend 336/168 | +2.869% / +2.192% | +3.594% / +3.084% | -3.914% / -4.308% | PASS |
| C3 fixed ensemble | +1.679% / +1.144% | +1.033% / +0.575% | -1.735% / -2.178% | PASS |

The frozen D1/D2 selection rule ranks C2 first and C3 second.  This is a
development selection only.  The latest diagnostic year is negative for both,
so the result does not activate either candidate and does not authorize
promotion.  Cash/observe-only remains the active champion.

Reproduce the suite with:

```bash
./scripts/run-strategy-research-v2 \
  --output-dir artifacts/strategy-v2/review-$(date +%F)
```

The manifest hashes every JSON/CSV output.  The suite is read-only with respect
to cached candles and optional shadow ledgers; it has no exchange client and
reports `orders_sent=0`.

## Untouched forward clock

Historical development cannot promote a strategy.  After code, configuration,
candidate registry, data hashes, and source fingerprint are frozen, start a new
paper ledger and record its first successful initialization timestamp as `T0`.
The original midnight-only convention was amended before the first C2 forward
ledger existed, following the 2026-07-21 request to start immediately.  An
exact timestamp is analytically equivalent and avoids mixing any preflight
ledger with the formal forward clock.

- The first 90 days are an operational/early-stop checkpoint, not promotion.
- Final confirmation requires 365 unchanged days, positive 2x-cost return,
  annualized return at least 20%, profit factor at least 1.15, drawdown no more
  than 15%, at least 50 portfolio round trips, no risk halt, and a non-negative
  lower confidence bound from weekly block resampling.
- Any code, parameter, universe, or allocation change demotes accumulated
  forward evidence to development and starts a new T0.

For a future HFT challenger, additionally require at least 30 continuous days,
99.9% capture availability, 1,000 non-overlapping round trips, and positive
results under 2x costs and p95/2x latency before it can enter forward paper.

Live routing remains unsupported and `orders_sent` must remain zero.
