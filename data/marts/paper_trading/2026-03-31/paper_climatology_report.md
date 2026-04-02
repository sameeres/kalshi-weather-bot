# Paper Climatology Monitor

- Generated at (UTC): `2026-04-01T01:48:16.957697+00:00`
- Strategy: `climatology_or_below_yes_cheap_v1`
- Paper only: `True`
- Evaluations: `2880`
- Gate passed: `360`
- Paper trades: `0`

## Rules

- Contract type: `or_below`
- Side: `yes`
- Max entry price: `25.0` cents
- Min net edge: `0.05`
- Max spread: `2.0` cents
- Fee model: `kalshi_standard_taker`

## By City

- `chicago` evaluations=1440 gate_passed=120 paper_trades=0
- `nyc` evaluations=1440 gate_passed=240 paper_trades=0

## Paper Trades

- None

## Best Skips

| snapshot_ts | city | market | entry | fair_yes | net_edge | rejection |
| --- | --- | --- | ---: | ---: | ---: | --- |
| 2026-04-01T01:15:39.290163+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:16:42.105914+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:17:44.936573+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:18:47.732999+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:19:50.670216+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:20:53.918838+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:21:56.909760+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:22:59.979036+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:24:02.733868+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:25:05.792577+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:26:08.797200+00:00 | nyc | KXHIGHNY-26APR01-T72 | 1.0 | 0.9091 | 0.8891 | market_not_open |
| 2026-04-01T01:14:35.902579+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |
| 2026-04-01T01:27:11.828899+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |
| 2026-04-01T01:28:15.237010+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |
| 2026-04-01T01:29:18.041479+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |
| 2026-04-01T01:30:21.097400+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |
| 2026-04-01T01:31:23.886362+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |
| 2026-04-01T01:32:26.896133+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |
| 2026-04-01T01:33:30.213523+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |
| 2026-04-01T01:34:33.055669+00:00 | nyc | KXHIGHNY-26APR01-T72 | 2.0 | 0.9091 | 0.8791 | market_not_open |

## Rejections

- `market_not_open`: 2880
- `net_edge_below_threshold`: 2507
- `not_or_below_contract`: 2400
- `entry_price_above_gate`: 723
- `entry_price_not_in_0_25_bucket`: 723
- `spread_too_wide`: 59
