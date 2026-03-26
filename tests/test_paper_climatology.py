from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from kwb.cli import app
from kwb.execution.paper_climatology import run_paper_climatology_monitor


def _write_city_config(tmp_path: Path) -> Path:
    path = tmp_path / "cities.yml"
    path.write_text(
        """
cities:
  - city_key: nyc
    city_name: New York
    timezone: America/New_York
    kalshi_series_ticker: KXHIGHNY
    enabled: true
  - city_key: chicago
    city_name: Chicago
    timezone: America/Chicago
    kalshi_series_ticker: KXHIGHCHI
    enabled: true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _write_paper_config(tmp_path: Path) -> Path:
    path = tmp_path / "paper_trading.yml"
    path.write_text(
        """
paper_climatology_monitor:
  strategy_name: climatology_or_below_yes_cheap_v1
  paper_only: true
  gate:
    contract_type: or_below
    chosen_side: yes
    max_entry_price_cents: 25.0
  decision:
    day_window: 1
    min_lookback_samples: 30
    contracts: 1
    fee_model: kalshi_standard_taker
    fee_per_contract: 0.0
    min_net_edge: 0.05
    max_spread_cents: 2.0
  capture:
    status: open
    include_orderbook: true
    orderbook_depth: 10
    iterations: 1
    poll_interval_seconds: 60
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _write_history(tmp_path: Path) -> Path:
    rows = []
    for year in range(2015, 2026):
        for month_day in ["03-26", "03-27", "03-28"]:
            obs_date = f"{year}-{month_day}"
            rows.append(
                {
                    "station_id": "KNYC",
                    "city_key": "nyc",
                    "obs_date": obs_date,
                    "tmax_f": 40.0,
                }
            )
            rows.append(
                {
                    "station_id": "KMDW",
                    "city_key": "chicago",
                    "obs_date": obs_date,
                    "tmax_f": 25.0,
                }
            )
    for extra_year in [2014, 2013]:
        rows.append(
            {
                "station_id": "KNYC",
                "city_key": "nyc",
                "obs_date": f"{extra_year}-03-27",
                "tmax_f": 40.0,
            }
        )
        rows.append(
            {
                "station_id": "KMDW",
                "city_key": "chicago",
                "obs_date": f"{extra_year}-03-27",
                "tmax_f": 25.0,
            }
        )
    path = tmp_path / "weather_daily.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _fake_capture_factory(tmp_path: Path):
    def fake_capture(**kwargs):
        snapshots_path = tmp_path / "kalshi_market_microstructure_snapshots.parquet"
        levels_path = tmp_path / "kalshi_orderbook_levels.parquet"
        summary_path = tmp_path / "kalshi_microstructure_capture_summary.json"
        snapshot_ts = "2026-03-26T18:00:00+00:00"
        pd.DataFrame(
            [
                {
                    "snapshot_ts": snapshot_ts,
                    "city_key": "nyc",
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26MAR27",
                    "market_ticker": "KXHIGHNY-26MAR27-T50",
                    "strike_date": "2026-03-27T00:00:00Z",
                    "floor_strike": None,
                    "cap_strike": 49.0,
                    "strike_type": "less",
                    "market_title": "50 or below",
                    "market_subtitle": "49 or below",
                    "market_status": "open",
                    "best_yes_bid_cents": 19.0,
                    "best_yes_ask_cents": 20.0,
                    "best_no_bid_cents": 80.0,
                    "best_no_ask_cents": 81.0,
                    "best_yes_bid_size": 10.0,
                    "best_yes_ask_size": 12.0,
                    "best_no_bid_size": 11.0,
                    "best_no_ask_size": 8.0,
                    "yes_spread_cents": 1.0,
                    "no_spread_cents": 1.0,
                    "orderbook_available": True,
                    "quote_source": "orderbook",
                    "tick_size": 1.0,
                    "price_level_structure": "linear_cent",
                    "ingested_at": snapshot_ts,
                },
                {
                    "snapshot_ts": snapshot_ts,
                    "city_key": "chicago",
                    "series_ticker": "KXHIGHCHI",
                    "event_ticker": "KXHIGHCHI-26MAR27",
                    "market_ticker": "KXHIGHCHI-26MAR27-B60",
                    "strike_date": "2026-03-27T00:00:00Z",
                    "floor_strike": 60.0,
                    "cap_strike": None,
                    "strike_type": "greater",
                    "market_title": "60 or above",
                    "market_subtitle": "60 or above",
                    "market_status": "open",
                    "best_yes_bid_cents": 18.0,
                    "best_yes_ask_cents": 19.0,
                    "best_no_bid_cents": 81.0,
                    "best_no_ask_cents": 82.0,
                    "best_yes_bid_size": 10.0,
                    "best_yes_ask_size": 12.0,
                    "best_no_bid_size": 11.0,
                    "best_no_ask_size": 8.0,
                    "yes_spread_cents": 1.0,
                    "no_spread_cents": 1.0,
                    "orderbook_available": True,
                    "quote_source": "orderbook",
                    "tick_size": 1.0,
                    "price_level_structure": "linear_cent",
                    "ingested_at": snapshot_ts,
                },
                {
                    "snapshot_ts": snapshot_ts,
                    "city_key": "chicago",
                    "series_ticker": "KXHIGHCHI",
                    "event_ticker": "KXHIGHCHI-26MAR27",
                    "market_ticker": "KXHIGHCHI-26MAR27-T30",
                    "strike_date": "2026-03-27T00:00:00Z",
                    "floor_strike": None,
                    "cap_strike": 29.0,
                    "strike_type": "less",
                    "market_title": "30 or below",
                    "market_subtitle": "29 or below",
                    "market_status": "open",
                    "best_yes_bid_cents": 16.0,
                    "best_yes_ask_cents": 20.0,
                    "best_no_bid_cents": 80.0,
                    "best_no_ask_cents": 84.0,
                    "best_yes_bid_size": 10.0,
                    "best_yes_ask_size": 12.0,
                    "best_no_bid_size": 11.0,
                    "best_no_ask_size": 8.0,
                    "yes_spread_cents": 4.0,
                    "no_spread_cents": 4.0,
                    "orderbook_available": True,
                    "quote_source": "orderbook",
                    "tick_size": 1.0,
                    "price_level_structure": "linear_cent",
                    "ingested_at": snapshot_ts,
                },
            ]
        ).to_parquet(snapshots_path, index=False)
        pd.DataFrame(
            [
                {
                    "snapshot_ts": snapshot_ts,
                    "city_key": "nyc",
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26MAR27",
                    "market_ticker": "KXHIGHNY-26MAR27-T50",
                    "market_status": "open",
                    "side": "yes",
                    "level_rank": 1,
                    "price_cents": 19.0,
                    "quantity": 10.0,
                    "orderbook_depth_requested": 10,
                    "tick_size": 1.0,
                    "price_level_structure": "linear_cent",
                    "ingested_at": snapshot_ts,
                }
            ]
        ).to_parquet(levels_path, index=False)
        summary_path.write_text("{}", encoding="utf-8")
        return snapshots_path, levels_path, summary_path, {
            "snapshot_rows_captured": 3,
            "orderbook_levels_captured": 1,
            "iteration_summaries": [{"snapshot_ts": snapshot_ts}],
        }

    return fake_capture


def test_run_paper_climatology_monitor_writes_logs_and_report(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_city_config(tmp_path)
    paper_config_path = _write_paper_config(tmp_path)
    history_path = _write_history(tmp_path)
    monkeypatch.setattr(
        "kwb.execution.paper_climatology.capture_kalshi_microstructure_for_enabled_cities",
        _fake_capture_factory(tmp_path),
    )

    evaluations_path, trades_path, summary_path, report_path, summary = run_paper_climatology_monitor(
        config_path=config_path,
        paper_config_path=paper_config_path,
        history_path=history_path,
        output_root=tmp_path / "paper",
        microstructure_dir=tmp_path / "staging",
    )

    evaluations_df = pd.read_parquet(evaluations_path)
    trades_df = pd.read_parquet(trades_path)

    assert evaluations_path.exists()
    assert trades_path.exists()
    assert summary_path.exists()
    assert report_path.exists()
    assert len(evaluations_df) == 3
    assert len(trades_df) == 1
    assert summary["totals"]["evaluations"] == 3
    assert summary["totals"]["gate_passed"] == 2
    assert summary["totals"]["paper_trades"] == 1
    assert trades_df.loc[0, "city_key"] == "nyc"
    assert bool(trades_df.loc[0, "take_paper_trade"]) is True
    rejected = evaluations_df.set_index("market_ticker")["rejection_reasons"].to_dict()
    assert "not_or_below_contract" in rejected["KXHIGHCHI-26MAR27-B60"]
    assert "spread_too_wide" in rejected["KXHIGHCHI-26MAR27-T30"]
    report_text = report_path.read_text(encoding="utf-8")
    assert "Paper Trades" in report_text
    assert "Best Skips" in report_text


def test_paper_monitor_cli_smoke(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()

    def fake_run(**kwargs):
        evaluations = tmp_path / "paper_climatology_evaluations.parquet"
        trades = tmp_path / "paper_climatology_trades.parquet"
        summary = tmp_path / "paper_climatology_summary.json"
        report = tmp_path / "paper_climatology_report.md"
        pd.DataFrame([{"snapshot_ts": "2026-03-26T18:00:00+00:00"}]).to_parquet(evaluations, index=False)
        pd.DataFrame([{"snapshot_ts": "2026-03-26T18:00:00+00:00"}]).to_parquet(trades, index=False)
        summary.write_text("{}", encoding="utf-8")
        report.write_text("# report\n", encoding="utf-8")
        return evaluations, trades, summary, report, {"totals": {"evaluations": 1, "paper_trades": 1}}

    monkeypatch.setattr("kwb.cli.run_paper_climatology_monitor", fake_run)

    result = runner.invoke(app, ["research", "paper-monitor-climatology"])

    assert result.exit_code == 0
    assert "Saved paper-only climatology monitor" in result.output
