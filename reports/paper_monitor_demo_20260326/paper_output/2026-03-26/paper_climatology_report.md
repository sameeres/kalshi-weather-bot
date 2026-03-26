# Paper Climatology Monitor

- Generated at (UTC): `2026-03-26T20:14:19.093052+00:00`
- Strategy: `climatology_or_below_yes_cheap_v1`
- Paper only: `True`
- Evaluations: `3`
- Gate passed: `2`
- Paper trades: `1`

## Rules

- Contract type: `or_below`
- Side: `yes`
- Max entry price: `25.0` cents
- Min net edge: `0.05`
- Max spread: `2.0` cents
- Fee model: `kalshi_standard_taker`

## By City

- `chicago` evaluations=2 gate_passed=1 paper_trades=0
- `nyc` evaluations=1 gate_passed=1 paper_trades=1

## Paper Trades

| snapshot_ts | city | market | event_date | entry | fair_yes | net_edge |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 2026-03-26T18:00:00+00:00 | nyc | KXHIGHNY-26MAR27-T50 | 2026-03-27 | 20.0 | 1.0000 | 0.7800 |

## Best Skips

| snapshot_ts | city | market | entry | fair_yes | net_edge | rejection |
| --- | --- | --- | ---: | ---: | ---: | --- |
| 2026-03-26T18:00:00+00:00 | chicago | KXHIGHCHI-26MAR27-T30 | 20.0 | 1.0000 | 0.7800 | spread_too_wide |

## Rejections

- `net_edge_below_threshold`: 1
- `not_or_below_contract`: 1
- `spread_too_wide`: 1
