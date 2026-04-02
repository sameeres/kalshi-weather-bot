from __future__ import annotations

from pathlib import Path


def test_manual_forecast_scripts_reference_expected_commands() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    fetch_script = (repo_root / "scripts" / "fetch_nws_forecast_snapshots_manual.sh").read_text(encoding="utf-8")
    run_script = (repo_root / "scripts" / "run_forecast_distribution_manual.sh").read_text(encoding="utf-8")
    show_script = (repo_root / "scripts" / "show_latest_forecast_distribution_reports.sh").read_text(encoding="utf-8")
    combined_script = (repo_root / "scripts" / "show_latest_combined_weather_research_summary.sh").read_text(encoding="utf-8")

    assert "python3 -m kwb data fetch-nws-forecast-snapshots" in fetch_script
    assert "python3 -m kwb research run-forecast-distribution" in run_script
    assert "forecast_snapshot_coverage.md" in run_script
    assert "backtest_report_forecast_distribution.md" in show_script
    assert "forecast_snapshot_coverage.md" in show_script
    assert "python3 -m kwb research build-combined-weather-summary" in combined_script
