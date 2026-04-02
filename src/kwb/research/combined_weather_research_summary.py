from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.settings import MARTS_DIR

DEFAULT_PAPER_ROOT = MARTS_DIR / "paper_trading"
DEFAULT_FORECAST_ROOT = MARTS_DIR / "forecast_distribution_manual"
DEFAULT_SUMMARY_PATH = MARTS_DIR / "combined_weather_research_summary_latest.md"


class CombinedWeatherResearchSummaryError(ValueError):
    """Raised when the combined daily research summary cannot be built safely."""


def build_latest_combined_weather_research_summary(
    paper_root: Path | None = None,
    forecast_root: Path | None = None,
    output_path: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    paper_root = (paper_root or DEFAULT_PAPER_ROOT).expanduser().resolve()
    forecast_root = (forecast_root or DEFAULT_FORECAST_ROOT).expanduser().resolve()
    output_path = (output_path or DEFAULT_SUMMARY_PATH).expanduser().resolve()

    paper_date, paper_dir = _latest_dated_dir(paper_root)
    forecast_run, forecast_dir = _latest_dated_dir(forecast_root)

    paper_section = _build_paper_section(paper_date=paper_date, paper_dir=paper_dir) if paper_dir else _empty_paper_section()
    forecast_section = _build_forecast_section(forecast_run=forecast_run, forecast_dir=forecast_dir) if forecast_dir else _empty_forecast_section()

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "paper": paper_section,
        "forecast": forecast_section,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(_render_markdown(payload), encoding="utf-8")
    return output_path, payload


def _latest_dated_dir(root: Path) -> tuple[str | None, Path | None]:
    if not root.exists():
        return None, None
    candidates = sorted(path for path in root.iterdir() if path.is_dir())
    if not candidates:
        return None, None
    latest = candidates[-1]
    return latest.name, latest


def _build_paper_section(paper_date: str | None, paper_dir: Path) -> dict[str, Any]:
    summary_path = paper_dir / "paper_climatology_summary.json"
    evaluations_path = paper_dir / "paper_climatology_evaluations.parquet"
    reconciliation_summary_path = paper_dir / "paper_climatology_reconciliation_summary.json"

    summary = _read_json(summary_path)
    evaluations = pd.read_parquet(evaluations_path) if evaluations_path.exists() else pd.DataFrame()
    gate_passed = evaluations.loc[evaluations.get("gate_passed", False)].copy() if not evaluations.empty else pd.DataFrame()
    latest_candidates = (
        gate_passed.sort_values(["snapshot_ts", "net_edge_yes"], ascending=[False, False], kind="stable")
        .head(8)
        .to_dict("records")
        if not gate_passed.empty
        else []
    )
    reconciliation = _read_json(reconciliation_summary_path) if reconciliation_summary_path.exists() else None

    return {
        "latest_date": paper_date,
        "daily_dir": str(paper_dir),
        "totals": summary.get("totals", {}),
        "latest_candidates": latest_candidates,
        "reconciliation": None if reconciliation is None else reconciliation.get("totals", {}),
    }


def _build_forecast_section(forecast_run: str | None, forecast_dir: Path) -> dict[str, Any]:
    summary_path = forecast_dir / "backtest_summary_forecast_distribution.json"
    trades_path = forecast_dir / "backtest_trades_forecast_distribution.parquet"
    coverage_path = forecast_dir / "forecast_snapshot_coverage.json"

    summary = _read_json(summary_path)
    coverage = _read_json(coverage_path)
    trades = pd.read_parquet(trades_path) if trades_path.exists() else pd.DataFrame()

    latest_candidates = []
    overlap_candidates = []
    if not trades.empty:
        latest_candidates = (
            trades.loc[trades["strategy_name"].isin(["climatology_only", "forecast_only"])]
            .sort_values(["event_date", "strategy_name", "edge_at_entry"], ascending=[False, True, False], kind="stable")
            .head(10)
            .to_dict("records")
        )
        overlap_candidates = (
            trades.loc[trades["strategy_name"] == "intersection"]
            .sort_values(["event_date", "edge_at_entry"], ascending=[False, False], kind="stable")
            .head(8)
            .to_dict("records")
        )

    return {
        "latest_run": forecast_run,
        "run_dir": str(forecast_dir),
        "strategy_summary": summary.get("strategies", {}),
        "latest_candidates": latest_candidates,
        "overlap_candidates": overlap_candidates,
        "coverage": coverage,
    }


def _empty_paper_section() -> dict[str, Any]:
    return {
        "latest_date": None,
        "daily_dir": None,
        "totals": {},
        "latest_candidates": [],
        "reconciliation": None,
    }


def _empty_forecast_section() -> dict[str, Any]:
    return {
        "latest_run": None,
        "run_dir": None,
        "strategy_summary": {},
        "latest_candidates": [],
        "overlap_candidates": [],
        "coverage": {},
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _render_markdown(payload: dict[str, Any]) -> str:
    paper = payload["paper"]
    forecast = payload["forecast"]
    lines = [
        "# Combined Weather Research Summary",
        "",
        f"- Generated at (UTC): `{payload['generated_at_utc']}`",
        f"- Latest paper date: `{paper['latest_date']}`",
        f"- Latest forecast run: `{forecast['latest_run']}`",
        "",
        "## Climatology Paper",
        "",
    ]
    if not paper["totals"]:
        lines.append("- No paper climatology artifacts found.")
    else:
        lines.extend(
            [
                f"- Evaluations: `{paper['totals'].get('evaluations', 0)}`",
                f"- Gate passed: `{paper['totals'].get('gate_passed', 0)}`",
                f"- Paper trades: `{paper['totals'].get('paper_trades', 0)}`",
            ]
        )
    lines.append("")
    lines.extend(["### Latest Paper Candidates", ""])
    if not paper["latest_candidates"]:
        lines.append("- None")
    else:
        for row in paper["latest_candidates"]:
            lines.append(
                f"- `{row.get('snapshot_ts')}` `{row.get('city_key')}` `{row.get('market_ticker')}` "
                f"entry={row.get('entry_price_cents')} fair_yes={row.get('fair_yes')} "
                f"net_edge={row.get('net_edge_yes')} take_paper_trade={row.get('take_paper_trade')}"
            )
    lines.append("")
    lines.extend(["### Most Recent Reconciled Paper Results", ""])
    if not paper["reconciliation"]:
        lines.append("- None")
    else:
        lines.extend(
            [
                f"- Resolved trades: `{paper['reconciliation'].get('resolved_trades', 0)}`",
                f"- Win count: `{paper['reconciliation'].get('win_count', 0)}`",
                f"- Loss count: `{paper['reconciliation'].get('loss_count', 0)}`",
                f"- Realized net PnL: `{paper['reconciliation'].get('realized_net_pnl_dollars', 0.0)}`",
            ]
        )
    lines.append("")

    lines.extend(["## Forecast Sidecar", ""])
    if not forecast["strategy_summary"]:
        lines.append("- No forecast-distribution artifacts found.")
    else:
        for name in ["climatology_only", "forecast_only", "intersection"]:
            summary = forecast["strategy_summary"].get(name)
            if not summary:
                continue
            lines.append(
                f"- `{name}` trades={summary.get('trade_count', 0)} total_net_pnl={summary.get('total_net_pnl', 0.0)} "
                f"avg_net_pnl={summary.get('average_net_pnl_per_trade', 0.0)}"
            )
    lines.append("")
    lines.extend(["### Latest Forecast Candidates", ""])
    if not forecast["latest_candidates"]:
        lines.append("- None")
    else:
        for row in forecast["latest_candidates"]:
            lines.append(
                f"- `{row.get('strategy_name')}` `{row.get('event_date')}` `{row.get('city_key')}` `{row.get('market_ticker')}` "
                f"side={row.get('chosen_side')} entry={row.get('entry_price')} edge={row.get('edge_at_entry')}"
            )
    lines.append("")
    lines.extend(["### Overlap / Intersection Candidates", ""])
    if not forecast["overlap_candidates"]:
        lines.append("- None")
    else:
        for row in forecast["overlap_candidates"]:
            lines.append(
                f"- `{row.get('event_date')}` `{row.get('city_key')}` `{row.get('market_ticker')}` "
                f"side={row.get('chosen_side')} entry={row.get('entry_price')} edge={row.get('edge_at_entry')}"
            )
    lines.append("")

    coverage = forecast.get("coverage", {})
    matching = coverage.get("matching_coverage", {})
    lines.extend(["## Forecast Coverage", ""])
    if not coverage:
        lines.append("- None")
    else:
        archive = coverage.get("snapshot_archive", {})
        lines.extend(
            [
                f"- Snapshot rows: `{archive.get('rows', 0)}`",
                f"- Snapshot range: `{archive.get('earliest_snapshot_ts')}` to `{archive.get('latest_snapshot_ts')}`",
                f"- Backtest rows eligible: `{matching.get('backtest_rows_eligible', 0)}`",
                f"- Backtest rows matched: `{matching.get('backtest_rows_matched', 0)}`",
                f"- Matched share: `{matching.get('matched_share', 0.0)}`",
            ]
        )
        warnings = coverage.get("warnings", [])
        if warnings:
            lines.append("- Warnings:")
            for warning in warnings[:5]:
                lines.append(f"  - {warning}")
    lines.append("")
    return "\n".join(lines)
