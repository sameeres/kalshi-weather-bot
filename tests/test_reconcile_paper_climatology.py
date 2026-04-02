from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from kwb.cli import app
from kwb.research.reconcile_paper_climatology import reconcile_paper_climatology


def _write_history(tmp_path: Path) -> Path:
    history = pd.DataFrame(
        [
            {"station_id": "KNYC", "city_key": "nyc", "obs_date": "2026-03-31", "tmax_f": 48.0},
            {"station_id": "KMDW", "city_key": "chicago", "obs_date": "2026-03-31", "tmax_f": 35.0},
            {"station_id": "KNYC", "city_key": "nyc", "obs_date": "2026-04-01", "tmax_f": 41.0},
        ]
    )
    path = tmp_path / "weather_daily.parquet"
    history.to_parquet(path, index=False)
    return path


def _write_trade_day(root: Path, day: str, rows: list[dict]) -> Path:
    daily = root / day
    daily.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(daily / "paper_climatology_trades.parquet", index=False)
    return daily


def _trade_row(
    city_key: str,
    market_ticker: str,
    event_date: str,
    cap_strike: float,
    entry_price_cents: float,
) -> dict:
    return {
        "snapshot_ts": f"{event_date}T15:00:00+00:00",
        "evaluation_ts": f"{event_date}T15:00:00+00:00",
        "strategy_name": "climatology_or_below_yes_cheap_v1",
        "paper_only_mode": True,
        "city_key": city_key,
        "series_ticker": "KXHIGHNY" if city_key == "nyc" else "KXHIGHCHI",
        "event_ticker": f"{market_ticker.rsplit('-', 1)[0]}",
        "market_ticker": market_ticker,
        "event_date": event_date,
        "market_status": "open",
        "market_title": "demo",
        "market_subtitle": "demo",
        "strike_date": f"{event_date}T00:00:00Z",
        "floor_strike": None,
        "cap_strike": cap_strike,
        "strike_type": "less",
        "contract_type": "or_below",
        "chosen_side": "yes",
        "gate_passed": True,
        "take_paper_trade": True,
        "rejection_reasons": "",
        "entry_price_cents": entry_price_cents,
        "entry_price_bucket": "0-25",
        "best_yes_bid_cents": max(entry_price_cents - 1.0, 0.0),
        "best_yes_ask_cents": entry_price_cents,
        "best_no_bid_cents": 100.0 - entry_price_cents,
        "best_no_ask_cents": 100.0 - max(entry_price_cents - 1.0, 0.0),
        "best_yes_bid_size": 10.0,
        "best_yes_ask_size": 10.0,
        "best_no_bid_size": 10.0,
        "best_no_ask_size": 10.0,
        "quote_source": "orderbook",
        "orderbook_available": True,
        "yes_spread_cents": 1.0,
        "no_spread_cents": 1.0,
        "tick_size": 1.0,
        "price_level_structure": "linear_cent",
        "lookback_sample_size": 30,
        "fair_yes": 0.50,
        "fair_no": 0.50,
        "model_prob_yes": 0.50,
        "model_prob_no": 0.50,
        "gross_edge_yes": 0.30,
        "net_edge_yes": 0.29,
        "estimated_fees_dollars": 0.01,
        "gate_max_entry_price_cents": 25.0,
        "decision_min_net_edge": 0.05,
        "decision_max_spread_cents": 2.0,
        "decision_fee_model": "kalshi_standard_taker",
        "day_window": 1,
        "min_lookback_samples": 30,
        "model_name": "baseline_climatology_v1",
    }


def test_reconcile_paper_climatology_handles_win_loss_and_unresolved(tmp_path: Path) -> None:
    history_path = _write_history(tmp_path)
    paper_root = tmp_path / "paper"
    _write_trade_day(
        paper_root,
        "2026-03-31",
        [
            _trade_row("nyc", "KXHIGHNY-26MAR31-T49", "2026-03-31", 49.0, 20.0),
            _trade_row("chicago", "KXHIGHCHI-26MAR31-T30", "2026-03-31", 30.0, 18.0),
            _trade_row("nyc", "KXHIGHNY-26APR02-T39", "2026-04-02", 39.0, 12.0),
        ],
    )

    reconciled_path, summary_path, report_path, scoreboard_path, cumulative_summary_path, cumulative_report_path, payload = reconcile_paper_climatology(
        trade_date="2026-03-31",
        paper_output_root=paper_root,
        history_path=history_path,
    )

    reconciled = pd.read_parquet(reconciled_path)
    assert reconciled_path.exists()
    assert summary_path.exists()
    assert report_path.exists()
    assert scoreboard_path.exists()
    assert cumulative_summary_path.exists()
    assert cumulative_report_path.exists()
    assert len(reconciled) == 3

    by_ticker = reconciled.set_index("market_ticker")
    assert bool(by_ticker.loc["KXHIGHNY-26MAR31-T49", "won_trade"]) is True
    assert round(float(by_ticker.loc["KXHIGHNY-26MAR31-T49", "realized_net_pnl_dollars"]), 6) == 0.79
    assert bool(by_ticker.loc["KXHIGHCHI-26MAR31-T30", "lost_trade"]) is True
    assert round(float(by_ticker.loc["KXHIGHCHI-26MAR31-T30", "realized_net_pnl_dollars"]), 6) == -0.19
    assert by_ticker.loc["KXHIGHNY-26APR02-T39", "resolved_status"] == "unresolved"

    daily = payload["daily_summary"]["totals"]
    assert daily["total_paper_trades"] == 3
    assert daily["resolved_trades"] == 2
    assert daily["unresolved_trades"] == 1
    assert daily["win_count"] == 1
    assert daily["loss_count"] == 1
    assert round(float(daily["realized_net_pnl_dollars"]), 6) == 0.60


def test_reconcile_paper_climatology_rebuilds_cumulative_scoreboard(tmp_path: Path) -> None:
    history_path = _write_history(tmp_path)
    paper_root = tmp_path / "paper"
    _write_trade_day(
        paper_root,
        "2026-03-30",
        [_trade_row("nyc", "KXHIGHNY-26MAR30-T42", "2026-04-01", 42.0, 10.0)],
    )
    _write_trade_day(
        paper_root,
        "2026-03-31",
        [_trade_row("nyc", "KXHIGHNY-26MAR31-T49", "2026-03-31", 49.0, 20.0)],
    )

    reconcile_paper_climatology(trade_date="2026-03-30", paper_output_root=paper_root, history_path=history_path)
    _, _, _, scoreboard_path, cumulative_summary_path, _, payload = reconcile_paper_climatology(
        trade_date="2026-03-31",
        paper_output_root=paper_root,
        history_path=history_path,
    )

    scoreboard = pd.read_csv(scoreboard_path)
    assert list(scoreboard["trade_date"]) == ["2026-03-30", "2026-03-31"]
    cumulative = payload["cumulative_summary"]["totals"]
    assert cumulative["paper_monitor_days_reconciled"] == 2
    assert cumulative["resolved_trades"] == 2
    assert round(float(cumulative["cumulative_realized_net_pnl_dollars"]), 6) == 1.68
    assert cumulative_summary_path.exists()


def test_reconcile_paper_climatology_cli_happy_path(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()

    def fake_reconcile(**kwargs):
        daily = tmp_path / "paper_climatology_reconciled_trades.parquet"
        summary = tmp_path / "paper_climatology_reconciliation_summary.json"
        report = tmp_path / "paper_climatology_reconciliation_report.md"
        scoreboard = tmp_path / "paper_climatology_cumulative_scoreboard.csv"
        cumulative_summary = tmp_path / "paper_climatology_cumulative_summary.json"
        cumulative_report = tmp_path / "paper_climatology_cumulative_report.md"
        pd.DataFrame([{"trade_date": "2026-03-31"}]).to_parquet(daily, index=False)
        summary.write_text("{}", encoding="utf-8")
        report.write_text("# report\n", encoding="utf-8")
        pd.DataFrame([{"trade_date": "2026-03-31"}]).to_csv(scoreboard, index=False)
        cumulative_summary.write_text("{}", encoding="utf-8")
        cumulative_report.write_text("# cumulative\n", encoding="utf-8")
        return daily, summary, report, scoreboard, cumulative_summary, cumulative_report, {
            "trade_date": "2026-03-31",
            "daily_summary": {"totals": {"resolved_trades": 1, "unresolved_trades": 0, "realized_net_pnl_dollars": 0.5}},
        }

    monkeypatch.setattr("kwb.cli.reconcile_paper_climatology", fake_reconcile)

    result = runner.invoke(app, ["research", "reconcile-paper-climatology", "--trade-date", "2026-03-31"])

    assert result.exit_code == 0
    assert "Saved paper climatology reconciliation" in result.output
