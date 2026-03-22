from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class CityConfig:
    city_key: str
    city_name: str
    timezone: str
    kalshi_series_ticker: Optional[str]
    settlement_source_name: Optional[str]
    settlement_station_id: Optional[str]
    settlement_station_name: Optional[str]
    settlement_source_url: Optional[str]
    station_lat: Optional[float]
    station_lon: Optional[float]
    enabled: bool = True


@dataclass
class EventMetadata:
    event_ticker: str
    series_ticker: str
    title: str
    strike_date: datetime
    settlement_source_name: Optional[str]
    settlement_source_url: Optional[str]


@dataclass
class WeatherDaily:
    station_id: str
    city_key: str
    obs_date: date
    tmax_c: Optional[float]
    tmin_c: Optional[float]
    tmax_f: Optional[float]
    tmin_f: Optional[float]
    source_dataset: str
    ingested_at: datetime


@dataclass
class ClimateNormalsDaily:
    station_id: str
    city_key: str
    month_day: str
    normal_tmax_c: Optional[float]
    normal_tmin_c: Optional[float]
    normal_tmax_f: Optional[float]
    normal_tmin_f: Optional[float]
    normals_period: str
    normals_source: str
    ingested_at: datetime


@dataclass
class MarketQuote:
    snapshot_ts: datetime
    market_ticker: str
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    last_price: Optional[float]
    volume: Optional[float]


@dataclass
class KalshiMarket:
    city_key: str
    series_ticker: str
    event_ticker: str
    market_ticker: str
    strike_date: Optional[datetime]
    market_title: Optional[str]
    market_subtitle: Optional[str]
    status: Optional[str]
    floor_strike: Optional[float]
    cap_strike: Optional[float]
    strike_type: Optional[str]
    expiration_ts: Optional[datetime]
    close_time: Optional[datetime]
    ingested_at: datetime


@dataclass
class KalshiCandle:
    market_ticker: str
    city_key: str
    candle_ts: datetime
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]
    interval: str
    ingested_at: datetime


@dataclass
class BucketProbability:
    market_ticker: str
    decision_ts: datetime
    prob_yes: float
    fair_yes: float
    fair_no: float


@dataclass
class BacktestDatasetRow:
    city_key: str
    city_name: str
    timezone: str
    series_ticker: str
    event_ticker: str
    market_ticker: str
    strike_date: datetime
    event_date: date
    month_day: str
    market_title: Optional[str]
    market_subtitle: Optional[str]
    status: Optional[str]
    floor_strike: Optional[float]
    cap_strike: Optional[float]
    strike_type: str
    decision_time_local: str
    decision_ts: datetime
    decision_candle_ts: datetime
    decision_price: Optional[float]
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    candle_interval: str
    actual_tmax_f: Optional[float]
    normal_tmax_f: Optional[float]
    tmax_anomaly_f: Optional[float]
    weather_station_id: str
    normals_station_id: str
    normals_period: str
    normals_source: str
    resolved_yes: Optional[bool]


@dataclass
class ClimatologyScoredRow:
    city_key: str
    market_ticker: str
    event_date: date
    decision_ts: datetime
    decision_price: Optional[float]
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    actual_tmax_f: Optional[float]
    normal_tmax_f: Optional[float]
    tmax_anomaly_f: Optional[float]
    resolved_yes: Optional[bool]
    model_prob_yes: float
    model_prob_no: float
    fair_yes: float
    fair_no: float
    edge_yes: float
    lookback_sample_size: int
    model_name: str


@dataclass
class ClimatologyTradeRow:
    city_key: str
    market_ticker: str
    event_date: date
    decision_ts: datetime
    decision_price: float
    resolved_yes: bool
    model_prob_yes: float
    model_prob_no: float
    edge_yes: float
    chosen_side: str
    entry_price: float
    edge_at_entry: float
    pricing_mode: str
    contracts: int
    gross_pnl: float
    net_pnl: float
    lookback_sample_size: int
    model_name: str


@dataclass
class ClimatologyExecutableTradeRow:
    city_key: str
    market_ticker: str
    event_date: date
    decision_ts: datetime
    decision_price: float
    resolved_yes: bool
    model_prob_yes: float
    model_prob_no: float
    fair_yes: float
    fair_no: float
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    chosen_side: str
    entry_price: float
    entry_price_source: str
    pricing_mode: str
    quote_source: str
    uses_true_quotes: bool
    quote_spread: Optional[float]
    exec_edge_yes: Optional[float]
    exec_edge_no: Optional[float]
    edge_at_entry: float
    contracts: int
    gross_pnl: float
    net_pnl: float
    lookback_sample_size: int
    model_name: str


@dataclass
class WalkforwardFoldResultRow:
    fold_number: int
    pricing_mode: str
    train_start: str
    train_end: str
    validation_start: str
    validation_end: str
    test_start: str
    test_end: str
    selected_min_edge: Optional[float]
    selected_min_samples: Optional[int]
    selected_min_price: Optional[float]
    selected_max_price: Optional[float]
    selected_allow_no: Optional[bool]
    selected_max_spread: Optional[float]
    validation_trades: int
    validation_total_net_pnl: float
    validation_average_net_pnl_per_trade: float
    test_rows_evaluated: int
    test_trades: int
    test_yes_trades: int
    test_no_trades: int
    test_hit_rate: float
    test_average_edge_at_entry: float
    test_total_gross_pnl: float
    test_total_net_pnl: float
    test_brier_score: float
    skip_reason: str


@dataclass
class WalkforwardDiagnosticRow:
    pricing_mode: str
    subset: str
    breakdown: str
    bucket: str
    trades_taken: int
    yes_trades_taken: int
    no_trades_taken: int
    hit_rate: float
    average_edge_at_entry: float
    average_net_pnl_per_trade: float
    total_net_pnl: float


@dataclass
class ClimatologyResearchManifest:
    model_name: str
    run_timestamp_utc: str
    run_directory: str
    pricing_modes_requested: list[str]
    decision_time_local: str
    parquet_engine_available: bool
    parquet_engine_limitations_affected_execution: bool
    real_local_data_available: bool


@dataclass
class ClimatologyBaselineReport:
    model_name: str
    run_timestamp_utc: str
    baseline_status: str
    baseline_status_reason: str
