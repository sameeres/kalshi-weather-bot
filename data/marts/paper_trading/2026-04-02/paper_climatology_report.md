# Paper Climatology Monitor

- Generated at (UTC): `2026-04-02T17:11:07.580493+00:00`
- Strategy: `climatology_or_below_yes_cheap_v1`
- Paper only: `True`
- Evaluations: `2880`
- Gate passed: `316`
- Paper trades: `0`

## Rules

- Contract type: `or_below`
- Side: `yes`
- Max entry price: `25.0` cents
- Min net edge: `0.05`
- Max spread: `2.0` cents
- Fee model: `kalshi_standard_taker`

## By City

- `chicago` evaluations=1440 gate_passed=240 paper_trades=0
- `nyc` evaluations=1440 gate_passed=76 paper_trades=0

## Paper Trades

- None

## Best Skips

| snapshot_ts | city | market | entry | fair_yes | net_edge | rejection |
| --- | --- | --- | ---: | ---: | ---: | --- |
| 2026-04-02T15:09:13.999085+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:10:16.796212+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:11:19.653732+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:12:22.564909+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:13:25.509347+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:14:28.297359+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:15:31.472716+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:16:34.415534+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:17:37.103457+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:18:39.968584+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:19:42.719900+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:20:45.649174+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:21:48.659571+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:22:51.367500+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:23:54.375955+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:24:57.637788+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:26:00.754467+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:27:03.623277+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T15:28:06.572289+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 2.0 | 0.8182 | 0.7882 | market_not_open |
| 2026-04-02T16:59:21.532733+00:00 | chicago | KXHIGHCHI-26APR02-T66 | 3.0 | 0.8182 | 0.7782 | market_not_open |

## Rejections

- `market_not_open`: 2880
- `not_or_below_contract`: 2400
- `net_edge_below_threshold`: 2160
- `spread_too_wide`: 477
- `entry_price_above_gate`: 444
- `entry_price_not_in_0_25_bucket`: 444
