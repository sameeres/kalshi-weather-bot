from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from kwb.cli import app
from kwb.ingestion.kalshi_microstructure import (
    capture_kalshi_microstructure_for_enabled_cities,
)
from kwb.ingestion.validate_staging import validate_staging_datasets


class FakeKalshiMicrostructureClient:
    def __init__(self) -> None:
        self.market_calls: list[tuple[str | None, str | None, str | None]] = []
        self.orderbook_calls: list[tuple[str, int]] = []

    def list_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        status: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict:
        self.market_calls.append((series_ticker, status, cursor))
        return {
            "markets": [
                {
                    "ticker": "KXHIGHNY-26MAR27-T50",
                    "event_ticker": "KXHIGHNY-26MAR27",
                    "strike_date": "2026-03-27T00:00:00Z",
                    "title": "50 or below",
                    "subtitle": "49° or below",
                    "status": "open",
                    "floor_strike": None,
                    "cap_strike": 49,
                    "strike_type": "less",
                    "yes_bid_dollars": "0.4100",
                    "yes_bid_size_fp": "12.00",
                    "yes_ask_dollars": "0.4300",
                    "yes_ask_size_fp": "8.00",
                    "no_bid_dollars": "0.5700",
                    "no_bid_size_fp": "8.00",
                    "no_ask_dollars": "0.5900",
                    "no_ask_size_fp": "12.00",
                    "last_price_dollars": "0.4200",
                    "volume_fp": "102.00",
                    "open_interest_fp": "88.00",
                    "liquidity_dollars": "150.0000",
                    "tick_size": 1,
                    "response_price_units": "usd_cent",
                    "price_level_structure": "linear_cent",
                    "price_ranges": [{"start": "1", "end": "99", "step": "1"}],
                    "close_time": "2026-03-27T20:59:00Z",
                    "expiration_time": "2026-03-27T20:59:00Z",
                    "fractional_trading_enabled": True,
                    "can_close_early": True,
                }
            ]
        }

    def get_market_orderbook(self, market_ticker: str, depth: int = 10) -> dict:
        self.orderbook_calls.append((market_ticker, depth))
        return {
            "orderbook": {
                "yes": [[41, 12], [40, 7]],
                "no": [[58, 9], [57, 3]],
            }
        }


class NoOrderbookKalshiMicrostructureClient(FakeKalshiMicrostructureClient):
    def get_market_orderbook(self, market_ticker: str, depth: int = 10) -> dict:
        raise RuntimeError("401 Unauthorized")


def _write_single_enabled_city(tmp_path: Path) -> Path:
    config_path = tmp_path / "cities.yml"
    config_path.write_text(
        """
cities:
  - city_key: nyc
    city_name: New York
    timezone: America/New_York
    kalshi_series_ticker: KXHIGHNY
    enabled: true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config_path


def test_capture_kalshi_microstructure_writes_snapshot_and_depth_tables(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiMicrostructureClient()

    snapshots_path, levels_path, summary_path, summary = capture_kalshi_microstructure_for_enabled_cities(
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
        status="open",
        include_orderbook=True,
        orderbook_depth=2,
        return_summary=True,
    )

    snapshots_df = pd.read_parquet(snapshots_path)
    levels_df = pd.read_parquet(levels_path)

    assert summary_path.exists()
    assert len(snapshots_df) == 1
    assert len(levels_df) == 4
    assert snapshots_df.loc[0, "quote_source"] == "orderbook"
    assert snapshots_df.loc[0, "strike_type"] == "less"
    assert snapshots_df.loc[0, "cap_strike"] == 49.0
    assert snapshots_df.loc[0, "best_yes_bid_cents"] == 41.0
    assert snapshots_df.loc[0, "best_yes_ask_cents"] == 42.0
    assert snapshots_df.loc[0, "best_no_bid_cents"] == 58.0
    assert snapshots_df.loc[0, "best_no_ask_cents"] == 59.0
    assert snapshots_df.loc[0, "best_yes_ask_size"] == 9.0
    assert snapshots_df.loc[0, "tick_size"] == 1.0
    assert list(levels_df["side"]) == ["no", "no", "yes", "yes"] or list(levels_df["side"]) == ["yes", "yes", "no", "no"]
    assert summary["snapshot_rows_captured"] == 1
    assert summary["orderbook_levels_captured"] == 4


def test_capture_kalshi_microstructure_falls_back_to_market_endpoint_quotes(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = NoOrderbookKalshiMicrostructureClient()

    snapshots_path, levels_path, _, _ = capture_kalshi_microstructure_for_enabled_cities(
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
        include_orderbook=True,
        return_summary=True,
    )

    snapshots_df = pd.read_parquet(snapshots_path)
    levels_df = pd.read_parquet(levels_path)

    assert len(levels_df) == 0
    assert bool(snapshots_df.loc[0, "orderbook_available"]) is False
    assert snapshots_df.loc[0, "quote_source"] == "markets_endpoint"
    assert snapshots_df.loc[0, "best_yes_bid_cents"] == 41.0
    assert snapshots_df.loc[0, "best_yes_ask_cents"] == 43.0
    assert "401" in snapshots_df.loc[0, "orderbook_capture_error"]


def test_validate_staging_accepts_microstructure_datasets(tmp_path: Path) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    client = FakeKalshiMicrostructureClient()
    capture_kalshi_microstructure_for_enabled_cities(
        config_path=config_path,
        output_dir=tmp_path,
        client=client,
        include_orderbook=True,
        return_summary=False,
    )

    summary = validate_staging_datasets(
        datasets=("kalshi_market_microstructure_snapshots", "kalshi_orderbook_levels"),
        staging_dir=tmp_path,
        config_path=config_path,
    )

    assert summary["ready"] is True
    assert summary["datasets"]["kalshi_market_microstructure_snapshots"]["row_count"] == 1
    assert summary["datasets"]["kalshi_orderbook_levels"]["row_count"] == 4


def test_kalshi_microstructure_cli_smoke(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_single_enabled_city(tmp_path)
    runner = CliRunner()

    def fake_capture(**kwargs):
        snapshots_path = tmp_path / "kalshi_market_microstructure_snapshots.parquet"
        levels_path = tmp_path / "kalshi_orderbook_levels.parquet"
        summary_path = tmp_path / "kalshi_microstructure_capture_summary.json"
        pd.DataFrame(
            [
                {
                    "snapshot_ts": "2026-03-26T18:00:00+00:00",
                    "city_key": "nyc",
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26MAR27",
                    "market_ticker": "KXHIGHNY-26MAR27-T50",
                    "strike_date": "2026-03-27T00:00:00Z",
                    "floor_strike": None,
                    "cap_strike": 49.0,
                    "strike_type": "less",
                    "market_status": "open",
                    "best_yes_bid_cents": 41.0,
                    "best_yes_ask_cents": 42.0,
                    "best_no_bid_cents": 58.0,
                    "best_no_ask_cents": 59.0,
                    "orderbook_available": True,
                    "quote_source": "orderbook",
                    "ingested_at": "2026-03-26T18:00:00+00:00",
                }
            ]
        ).to_parquet(snapshots_path, index=False)
        pd.DataFrame(
            [
                {
                    "snapshot_ts": "2026-03-26T18:00:00+00:00",
                    "city_key": "nyc",
                    "series_ticker": "KXHIGHNY",
                    "market_ticker": "KXHIGHNY-26MAR27-T50",
                    "side": "yes",
                    "level_rank": 1,
                    "price_cents": 41.0,
                    "quantity": 12.0,
                    "ingested_at": "2026-03-26T18:00:00+00:00",
                }
            ]
        ).to_parquet(levels_path, index=False)
        summary_path.write_text("{}", encoding="utf-8")
        return snapshots_path, levels_path, summary_path, {
            "snapshot_rows_captured": 1,
            "orderbook_levels_captured": 1,
        }

    monkeypatch.setattr("kwb.cli.capture_kalshi_microstructure_for_enabled_cities", fake_capture)
    result = runner.invoke(
        app,
        [
            "kalshi",
            "capture-microstructure",
            "--config-path",
            str(config_path),
            "--output-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert "snapshots captured: 1" in result.output
