## Kalshi Execution Realism Audit

### What current staged data contains

- `kalshi_markets.parquet`
  - One row per market ticker
  - Contract metadata only
  - No historical bid/ask snapshots
  - No historical depth
- `kalshi_candles.parquet`
  - Columns: `market_ticker`, `city_key`, `candle_ts`, `open`, `high`, `low`, `close`, `volume`, `interval`, `ingested_at`
  - Current file has `20544` rows across `546` market tickers
  - All rows are `1h` candles
  - This is OHLCV over time windows, not archived L1 quotes

### What current executable backtests are actually using

- `decision_price` mode uses the candle close as a point-in-time price proxy.
- `candle_proxy` mode derives executable quotes from the last completed candle:
  - `yes_bid = low`
  - `yes_ask = high`
  - `no_bid = 100 - yes_ask`
  - `no_ask = 100 - yes_bid`
- Those are conservative proxies, but they are not truly observed historical best bid/ask snapshots.

### What is not currently stored

- No historical top-of-book snapshot table
- No historical orderbook depth table
- No tick-by-tick trade history
- No archived resting-order queue/fill evidence

### Can historical execution realism be improved right now from existing data?

No, not materially.

The existing staged history is good enough for candle-based proxy bounds, but it cannot support credible claims about:

- true historical best bid/ask at the decision timestamp
- displayed depth available at that quote
- walking the book
- resting-order fill probability

### Highest-value next step implemented

A forward microstructure capture pipeline was added:

- `kwb kalshi capture-microstructure`

It writes two append-only research datasets:

- `kalshi_market_microstructure_snapshots.parquet`
  - timestamped top-of-book and market metadata
  - stores both markets-endpoint quote fields and orderbook-derived best levels when available
- `kalshi_orderbook_levels.parquet`
  - timestamped per-level bid depth by side

The pipeline also stores:

- reciprocal implied asks
- size fields
- tick size
- price-level structure
- market status and timing metadata
- capture summary JSON

### Remaining limitation after this implementation

We still cannot retroactively fix past execution realism for the already completed backtests.

This new pipeline makes future execution-aware research possible, but true L1/depth history must be collected going forward.
