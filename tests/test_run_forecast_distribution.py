from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

import kwb.cli as cli_module
from kwb.cli import app
from kwb.research.run_forecast_distribution import (
    _build_forecast_snapshot_coverage_summary_from_frames,
    run_forecast_distribution_research,
)


def test_run_forecast_distribution_research_writes_manifest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "kwb.research.run_forecast_distribution.build_backtest_dataset",
        lambda **kwargs: (tmp_path / "backtest_dataset.parquet", {"rows_written": 10}),
    )
    monkeypatch.setattr(
        "kwb.research.run_forecast_distribution.score_climatology_baseline",
        lambda **kwargs: (tmp_path / "climatology.parquet", {"rows_scored": 8}),
    )
    monkeypatch.setattr(
        "kwb.research.run_forecast_distribution.score_forecast_distribution",
        lambda **kwargs: (tmp_path / "forecast.parquet", {"rows_scored": 6}),
    )
    monkeypatch.setattr(
        "kwb.research.run_forecast_distribution.evaluate_forecast_distribution_signals",
        lambda **kwargs: (
            tmp_path / "trades.parquet",
            tmp_path / "summary.json",
            tmp_path / "report.md",
            {"rows_with_both_models": 5, "strategies": {}},
        ),
    )
    monkeypatch.setattr(
        "kwb.research.run_forecast_distribution.build_forecast_snapshot_coverage_summary",
        lambda **kwargs: {
            "generated_at_utc": "2026-04-02T12:00:00+00:00",
            "max_snapshot_age_hours": 18.0,
            "snapshot_archive": {
                "rows": 4,
                "distinct_snapshots": 2,
                "cities_covered": ["nyc", "chicago"],
                "earliest_snapshot_ts": "2026-04-02T12:00:00+00:00",
                "latest_snapshot_ts": "2026-04-02T13:00:00+00:00",
                "by_city": [],
                "by_city_date": [],
            },
            "matching_coverage": {
                "backtest_rows_eligible": 10,
                "backtest_rows_matched": 8,
                "matched_share": 0.8,
                "by_city": [],
            },
            "warnings": [],
        },
    )

    run_dir, manifest_path, manifest = run_forecast_distribution_research(output_dir=tmp_path, overwrite=True)

    assert run_dir == tmp_path
    assert manifest_path.exists()
    assert manifest["row_counts"]["forecast_rows_scored"] == 6
    assert "forecast_coverage_report" in manifest["output_paths"]


def test_run_forecast_distribution_cli_smoke(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()

    def fake_run(**kwargs):
        manifest = {
            "row_counts": {"forecast_rows_scored": 7},
            "output_paths": {
                "forecast_coverage_report": str(tmp_path / "coverage.md"),
                "comparison_report": str(tmp_path / "report.md"),
            },
        }
        path = tmp_path / "manifest.json"
        path.write_text("{}", encoding="utf-8")
        return tmp_path, path, manifest

    monkeypatch.setattr(cli_module, "run_forecast_distribution_research", fake_run)
    result = runner.invoke(app, ["research", "run-forecast-distribution", "--output-dir", str(tmp_path)])

    assert result.exit_code == 0
    assert "forecast rows" in result.stdout
    assert "scored:" in result.stdout
    assert "7" in result.stdout


def test_forecast_coverage_summary_reports_matches_and_warnings() -> None:
    backtest = pd.DataFrame(
        [
            {
                "city_key": "nyc",
                "event_date": "2026-04-02",
                "decision_ts": "2026-04-02T14:00:00+00:00",
            },
            {
                "city_key": "chicago",
                "event_date": "2026-04-02",
                "decision_ts": "2026-04-02T15:00:00+00:00",
            },
        ]
    )
    forecast = pd.DataFrame(
        [
            {
                "snapshot_ts": "2026-04-02T13:30:00+00:00",
                "city_key": "nyc",
                "period_start_ts": "2026-04-02T15:00:00+00:00",
                "period_end_ts": "2026-04-02T16:00:00+00:00",
                "period_date_local": "2026-04-02",
            }
        ]
    )

    summary = _build_forecast_snapshot_coverage_summary_from_frames(
        backtest_df=backtest,
        forecast_df=forecast,
        max_snapshot_age_hours=18.0,
    )

    assert summary["matching_coverage"]["backtest_rows_eligible"] == 2
    assert summary["matching_coverage"]["backtest_rows_matched"] == 1
    assert summary["matching_coverage"]["matched_share"] == 0.5
    assert any("below 0.80" in warning for warning in summary["warnings"])
