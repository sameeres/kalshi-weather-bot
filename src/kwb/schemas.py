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
    contracts: int
    gross_pnl: float
    net_pnl: float
    lookback_sample_size: int
    model_name: str
