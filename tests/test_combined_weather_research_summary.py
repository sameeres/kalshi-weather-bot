from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.research.combined_weather_research_summary import build_latest_combined_weather_research_summary


def test_build_latest_combined_weather_research_summary_writes_markdown(tmp_path: Path) -> None:
    paper_dir = tmp_path / "paper" / "2026-04-02"
    forecast_dir = tmp_path / "forecast" / "2026-04-02"
    paper_dir.mkdir(parents=True)
    forecast_dir.mkdir(parents=True)

    (paper_dir / "paper_climatology_summary.json").write_text(
        json.dumps({"totals": {"evaluations": 10, "gate_passed": 2, "paper_trades": 1}}),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "snapshot_ts": "2026-04-02T14:00:00+00:00",
                "city_key": "nyc",
                "market_ticker": "KXHIGHNY-26APR02-T70",
                "gate_passed": True,
                "entry_price_cents": 3.0,
                "fair_yes": 0.65,
                "net_edge_yes": 0.60,
                "take_paper_trade": False,
            }
        ]
    ).to_parquet(paper_dir / "paper_climatology_evaluations.parquet", index=False)
    (paper_dir / "paper_climatology_reconciliation_summary.json").write_text(
        json.dumps({"totals": {"resolved_trades": 1, "win_count": 1, "loss_count": 0, "realized_net_pnl_dollars": 0.75}}),
        encoding="utf-8",
    )

    (forecast_dir / "backtest_summary_forecast_distribution.json").write_text(
        json.dumps(
            {
                "strategies": {
                    "climatology_only": {"trade_count": 2, "total_net_pnl": 0.1, "average_net_pnl_per_trade": 0.05},
                    "forecast_only": {"trade_count": 3, "total_net_pnl": 0.2, "average_net_pnl_per_trade": 0.066},
                    "intersection": {"trade_count": 1, "total_net_pnl": 0.08, "average_net_pnl_per_trade": 0.08},
                }
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "strategy_name": "forecast_only",
                "city_key": "nyc",
                "market_ticker": "KXHIGHNY-26APR02-T70",
                "event_date": "2026-04-02",
                "chosen_side": "yes",
                "entry_price": 3.0,
                "edge_at_entry": 0.4,
            },
            {
                "strategy_name": "intersection",
                "city_key": "chicago",
                "market_ticker": "KXHIGHCHI-26APR02-T45",
                "event_date": "2026-04-02",
                "chosen_side": "yes",
                "entry_price": 4.0,
                "edge_at_entry": 0.3,
            },
        ]
    ).to_parquet(forecast_dir / "backtest_trades_forecast_distribution.parquet", index=False)
    (forecast_dir / "forecast_snapshot_coverage.json").write_text(
        json.dumps(
            {
                "snapshot_archive": {
                    "rows": 8,
                    "earliest_snapshot_ts": "2026-04-02T12:00:00+00:00",
                    "latest_snapshot_ts": "2026-04-02T15:00:00+00:00",
                },
                "matching_coverage": {
                    "backtest_rows_eligible": 10,
                    "backtest_rows_matched": 8,
                    "matched_share": 0.8,
                },
                "warnings": [],
            }
        ),
        encoding="utf-8",
    )

    output_path, _ = build_latest_combined_weather_research_summary(
        paper_root=tmp_path / "paper",
        forecast_root=tmp_path / "forecast",
        output_path=tmp_path / "combined.md",
    )

    text = output_path.read_text(encoding="utf-8")
    assert "Combined Weather Research Summary" in text
    assert "Latest Paper Candidates" in text
    assert "Latest Forecast Candidates" in text
    assert "Overlap / Intersection Candidates" in text
    assert "Forecast Coverage" in text


def test_combined_weather_summary_cli_smoke(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()

    def fake_build(**kwargs):
        out = tmp_path / "combined.md"
        out.write_text("# Combined", encoding="utf-8")
        return out, {"paper": {"latest_date": "2026-04-02"}, "forecast": {"latest_run": "2026-04-02"}}

    monkeypatch.setattr(cli_module, "build_latest_combined_weather_research_summary", fake_build)
    result = runner.invoke(app, ["research", "build-combined-weather-summary", "--output-path", str(tmp_path / "combined.md")])

    assert result.exit_code == 0
    assert "Saved combined weather research summary" in result.stdout
