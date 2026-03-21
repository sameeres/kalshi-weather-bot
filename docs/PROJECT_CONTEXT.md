# Project Context

## What this project is

This project is a research system for finding potentially positive expected-value trades in Kalshi weather markets, starting with **daily high temperature contracts**.

The central idea is:
- Kalshi offers bucketed markets on daily highs in specific cities.
- Each market implies a probability distribution through prices across buckets.
- We want to estimate our own probability distribution for the exact settlement outcome.
- If our fair probabilities differ materially from the market and the gap survives fees/spread/slippage, the trade may be attractive.

## What makes this harder than it first appears

A naive version of the strategy would do this:
- look at the current Kalshi line,
- compare it to historical highs for that city on nearby dates,
- make a bet based on whether the line looks high or low.

That is too simplistic.

Why:
1. the market already incorporates current forecast information,
2. the exact settlement source matters,
3. bucket pricing requires a probability distribution, not a point estimate,
4. poor execution assumptions can destroy apparent edge.

So the correct framing is:

> Build a calibrated probability model for the exact settlement temperature, using station-level historical weather plus current forecast information, then compare it with live market prices.

## The MVP definition

The MVP should remain intentionally narrow.

### Scope
- 3 cities maximum
- daily high temperature markets only
- backtesting only at first
- one decision timestamp per day
- conservative fill assumptions

### Why keep it narrow
Because most of the risk is in data integrity and evaluation discipline, not in code volume.
A narrow MVP gives us a chance to prove that the basic research loop works.

## The research loop

1. select enabled cities from config
2. discover Kalshi series / events / market metadata
3. extract settlement-source metadata
4. map each city / event to the correct weather station
5. ingest historical realized daily highs for that station
6. ingest relevant forecast data / forecast-derived features
7. build a model for the distribution of the next settlement high
8. convert the modeled distribution into bucket probabilities
9. compare fair values with executable market prices
10. simulate trades conservatively
11. evaluate calibration, not just PnL

## The most important concept: settlement truth

This project should always distinguish between:
- **forecast data**,
- **observed weather data**,
- **settlement data**.

These are related, but they are not interchangeable.

The system should always preserve the exact source used for settlement and never silently replace it with a nearby proxy.
If a proxy is used for exploratory work, that should be explicit in naming and logs.

## Data architecture

We are using a simple analytics-friendly layout:

- `data/raw/` for unmodified downloads
- `data/staging/` for normalized intermediate tables
- `data/marts/` for model-ready and backtest-ready datasets

We are using **DuckDB + Parquet** because it is lightweight, fast, and easy to inspect in a local research workflow.

## Expected tables / entities

At a minimum, the system will need:

### City / market mapping
A table that connects:
- city key
- Kalshi series ticker
- event ticker
- market ticker
- settlement source
- settlement station id
- station coordinates / metadata

### Weather history
Daily observations at the settlement station, especially:
- date
- max temp
- min temp
- quality flags

### Climate normals
For seasonal context and anomalies.

### Kalshi market data
- event metadata
- market definitions / bucket bounds
- quotes / snapshots
- candles or other historical price series

### Model outputs
- predicted mean / uncertainty or full distribution representation
- bucket probabilities
- fair yes / no values
- edge estimates at each decision time

### Backtest trade logs
- timestamp
- market
- side
- fill assumption
- contracts
- fees
- realized outcome
- PnL

## Modeling roadmap

We should move from simple to complex.

### Stage 1: Climatology-only baseline
Question:
If we only look at historical realized highs for the same station and season, how informative is that?

Purpose:
This is the baseline we must beat.

### Stage 2: Forecast-shifted climatology
Question:
Can we improve the baseline by shifting or reweighting historical outcomes using the current forecast?

Purpose:
This is likely the first realistic model worth backtesting.

### Stage 3: Forecast error calibration
Question:
Can we estimate forecast uncertainty and improve bucket probabilities?

Purpose:
This gets us closer to a genuine edge model.

### Stage 4: Market-aware refinements
Possible additions later:
- price drift around forecast updates
- liquidity / spread filters
- edge persistence tests
- threshold-specific mispricing analysis

These are explicitly **later-stage** items.

## Backtesting principles

The backtest should be designed to avoid self-deception.

### Rule 1: point-in-time correctness
At each decision time, only use information that would have been available then.

### Rule 2: executable prices
Prefer bid/ask-based assumptions over midpoint marks.

### Rule 3: include frictions
Include fees, spreads, and slippage buffers.

### Rule 4: evaluate calibration
A model that makes money in-sample for the wrong reasons is not useful.
We should measure:
- hit rates
- Brier score / log loss where appropriate
- calibration by bucket or confidence band
- stability across seasons and cities

### Rule 5: do not optimize too early
Do not over-tune thresholds or model parameters on a tiny initial sample.

## Execution roadmap

Execution is not the current goal.
But when the time comes, the sequence should be:
1. signal generation,
2. trade filters,
3. max exposure rules,
4. order placement,
5. monitoring and kill switch.

No live auto-trading should be added before research outputs are trustworthy.

## First milestone

The first serious milestone is not “place a trade.”
It is:

> Build a reliable ingestion and mapping layer that can discover enabled city markets, pull event metadata, extract settlement-source information, and save a clean staging table for downstream research.

That is why `src/kwb/ingestion/kalshi_events.py` is the first major implementation target.

## Immediate next engineering task

Implement an end-to-end version of:
- `src/kwb/ingestion/kalshi_events.py`

It should:
1. read enabled cities from config,
2. query Kalshi for relevant series / events,
3. fetch event metadata,
4. extract settlement-source data,
5. write a clean staging dataset,
6. produce output that can later be joined to station mapping.

## How the assistant should help in this repo

Good help looks like:
- making concrete, minimal edits,
- preserving the MVP scope,
- improving correctness and reproducibility,
- calling out assumptions clearly,
- preventing premature complexity.

Bad help looks like:
- adding lots of production infrastructure too early,
- introducing live trading logic before the research base is sound,
- glossing over settlement-source ambiguity,
- using unrealistic backtest assumptions.

