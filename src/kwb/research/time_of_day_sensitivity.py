from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from kwb.research.run_climatology_baseline import run_climatology_baseline_research
from kwb.research.stress_test_climatology_frictions import stress_test_climatology_frictions
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_REPORT_JSON_FILENAME = "time_of_day_sensitivity.json"
DEFAULT_REPORT_CSV_FILENAME = "time_of_day_sensitivity_summary.csv"
DEFAULT_FOLD_CSV_FILENAME = "time_of_day_sensitivity_fold_metrics.csv"
DEFAULT_REPORT_MARKDOWN_BASENAME = "time_of_day_sensitivity"
DEFAULT_TEST_TIMES = ("08:00", "09:00", "10:00", "11:00", "12:00", "13:00", "14:00")
VALIDATED_GATE = {
    "contract_type": "or_below",
    "chosen_side": "yes",
    "entry_price_bucket": "0-25",
}
SCENARIO_CODES = ("A", "B", "C", "D")
CITY_SCOPES = ("pooled", "nyc", "chicago")


class TimeOfDaySensitivityError(ValueError):
    """Raised when the time-of-day sensitivity study cannot complete safely."""


def run_time_of_day_sensitivity_study(
    decision_times_local: tuple[str, ...] = DEFAULT_TEST_TIMES,
    output_dir: Path | None = None,
    config_path: Path | None = None,
    weather_path: Path | None = None,
    normals_path: Path | None = None,
    markets_path: Path | None = None,
    candles_path: Path | None = None,
    history_path: Path | None = None,
    walkforward_profile: str = "research_short",
    selection_metric: str = "total_net_pnl",
    min_trades_for_selection: int = 1,
) -> tuple[Path, Path, Path, Path, dict[str, object]]:
    if not decision_times_local:
        raise TimeOfDaySensitivityError("At least one decision time is required.")

    for value in decision_times_local:
        _parse_local_time(value)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = output_dir or (Path("reports") / f"time_of_day_sensitivity_{timestamp}")
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_rows: list[dict[str, object]] = []
    fold_rows: list[dict[str, object]] = []
    run_records: list[dict[str, object]] = []

    for decision_time_local in decision_times_local:
        safe_time = decision_time_local.replace(":", "")
        run_dir = output_dir / "runs" / safe_time
        friction_dir = output_dir / "friction" / safe_time

        logger.info("Running time-of-day sensitivity hour=%s", decision_time_local)
        run_dir, _, _, _, manifest = run_climatology_baseline_research(
            decision_time_local=decision_time_local,
            output_dir=run_dir,
            overwrite=True,
            pricing_mode="candle_proxy",
            config_path=config_path,
            weather_path=weather_path,
            normals_path=normals_path,
            markets_path=markets_path,
            candles_path=candles_path,
            history_path=history_path,
            day_window=1,
            min_lookback_samples=30,
            walkforward_profile=walkforward_profile,
            min_trades_for_selection=min_trades_for_selection,
            selection_metric=selection_metric,
            allow_no=False,
        )
        _, _, _, friction_report = stress_test_climatology_frictions(
            run_dir=run_dir,
            output_dir=friction_dir,
            walkforward_profile=walkforward_profile,
            selection_metric=selection_metric,
            min_trades_for_selection=min_trades_for_selection,
        )

        time_summary_rows, time_fold_rows, time_record = _summarize_time_slice(
            decision_time_local=decision_time_local,
            run_dir=run_dir,
            friction_dir=friction_dir,
            manifest=manifest,
            friction_report=friction_report,
        )
        summary_rows.extend(time_summary_rows)
        fold_rows.extend(time_fold_rows)
        run_records.append(time_record)

    summary_df = pd.DataFrame(summary_rows)
    fold_df = pd.DataFrame(fold_rows)
    ranked_df = _rank_hours(summary_df)
    baseline_10 = _baseline_comparison(summary_df)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "times_tested": list(decision_times_local),
        "validated_gate": dict(VALIDATED_GATE),
        "methodology": {
            "cities": ["nyc", "chicago"],
            "pricing_basis": "fee-aware executable candle proxy",
            "walkforward_profile": walkforward_profile,
            "selection_metric": selection_metric,
            "min_trades_for_selection": min_trades_for_selection,
            "climatology_day_window": 1,
            "climatology_min_lookback_samples": 30,
            "decision_time_interpretation": "Local market time per city using existing backtest mart logic.",
            "guardrails": [
                "No minute-level sweep.",
                "No threshold tuning per hour beyond the existing friction scenarios.",
                "The validated gate was held fixed for every hour.",
            ],
        },
        "runs": run_records,
        "ranking": ranked_df.to_dict("records"),
        "baseline_10_comparison": baseline_10.to_dict("records"),
        "operational_note_amsterdam": _build_amsterdam_note(decision_times_local),
    }

    json_path = output_dir / DEFAULT_REPORT_JSON_FILENAME
    csv_path = output_dir / DEFAULT_REPORT_CSV_FILENAME
    fold_csv_path = output_dir / DEFAULT_FOLD_CSV_FILENAME
    markdown_path = output_dir / f"{DEFAULT_REPORT_MARKDOWN_BASENAME}_{date.today().strftime('%Y%m%d')}.md"

    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_df.to_csv(csv_path, index=False)
    fold_df.to_csv(fold_csv_path, index=False)
    markdown_path.write_text(_render_markdown(report, summary_df, fold_df, ranked_df, baseline_10), encoding="utf-8")

    return json_path, csv_path, fold_csv_path, markdown_path, report


def _summarize_time_slice(
    decision_time_local: str,
    run_dir: Path,
    friction_dir: Path,
    manifest: dict[str, object],
    friction_report: dict[str, object],
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    summary_rows: list[dict[str, object]] = []
    fold_rows: list[dict[str, object]] = []
    scenario_records: list[dict[str, object]] = []

    for scenario_code in SCENARIO_CODES:
        scenario_path = friction_dir / f"scenario_{scenario_code.lower()}" / "scenario_trades_rebuilt.csv"
        trades_df = pd.read_csv(scenario_path)
        trades_df = _apply_validated_gate(trades_df)
        scenario_records.append(
            {
                "scenario_code": scenario_code,
                "scenario_path": str(scenario_path),
                "gated_trade_count": int(len(trades_df)),
            }
        )
        summary_rows.extend(_aggregate_scope_rows(decision_time_local, scenario_code, trades_df))
        fold_rows.extend(_aggregate_scope_fold_rows(decision_time_local, scenario_code, trades_df))

    return (
        summary_rows,
        fold_rows,
        {
            "decision_time_local": decision_time_local,
            "run_dir": str(run_dir),
            "friction_dir": str(friction_dir),
            "backtest_rows": manifest.get("row_counts", {}).get("backtest_dataset_rows"),
            "scored_rows": manifest.get("row_counts", {}).get("scored_rows"),
            "scenario_records": scenario_records,
        },
    )


def _apply_validated_gate(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return trades_df.copy()
    df = trades_df.copy()
    df["chosen_side"] = df["chosen_side"].astype(str).str.lower()
    gate = (
        (df["contract_type"] == VALIDATED_GATE["contract_type"])
        & (df["chosen_side"] == VALIDATED_GATE["chosen_side"])
        & (df["entry_price_bucket"] == VALIDATED_GATE["entry_price_bucket"])
    )
    df = df.loc[gate].copy()
    if df.empty:
        return df
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["won"] = df["net_pnl"] > 0
    return df


def _aggregate_scope_rows(
    decision_time_local: str,
    scenario_code: str,
    trades_df: pd.DataFrame,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for city_scope in CITY_SCOPES:
        scoped = trades_df if city_scope == "pooled" else trades_df.loc[trades_df["city_key"] == city_scope].copy()
        rows.append(
            {
                "decision_time_local": decision_time_local,
                "scenario_code": scenario_code,
                "city_scope": city_scope,
                "trades_taken": int(len(scoped)),
                "total_net_pnl": round(float(scoped["net_pnl"].sum()), 6) if not scoped.empty else 0.0,
                "win_rate": round(float(scoped["won"].mean()), 6) if not scoped.empty else 0.0,
                "average_entry_price": round(float(scoped["entry_price"].mean()), 6) if not scoped.empty else 0.0,
                "average_spread": round(float(scoped["quote_spread"].mean()), 6) if not scoped.empty else 0.0,
                "average_edge": round(float(scoped["edge_at_entry"].mean()), 6) if not scoped.empty else 0.0,
                "positive_fold_count": int(_fold_positive_count(scoped)),
                "fold_count": int(scoped["fold_number"].nunique()) if not scoped.empty else 0,
                "all_folds_positive": bool(_all_folds_positive(scoped)),
            }
        )
    return rows


def _aggregate_scope_fold_rows(
    decision_time_local: str,
    scenario_code: str,
    trades_df: pd.DataFrame,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for city_scope in CITY_SCOPES:
        scoped = trades_df if city_scope == "pooled" else trades_df.loc[trades_df["city_key"] == city_scope].copy()
        if scoped.empty:
            continue
        grouped = (
            scoped.groupby("fold_number")
            .agg(
                trades_taken=("market_ticker", "count"),
                total_net_pnl=("net_pnl", "sum"),
                win_rate=("won", "mean"),
                average_entry_price=("entry_price", "mean"),
                average_spread=("quote_spread", "mean"),
                average_edge=("edge_at_entry", "mean"),
            )
            .reset_index()
        )
        for row in grouped.to_dict("records"):
            rows.append(
                {
                    "decision_time_local": decision_time_local,
                    "scenario_code": scenario_code,
                    "city_scope": city_scope,
                    "fold_number": int(row["fold_number"]),
                    "trades_taken": int(row["trades_taken"]),
                    "total_net_pnl": round(float(row["total_net_pnl"]), 6),
                    "win_rate": round(float(row["win_rate"]), 6),
                    "average_entry_price": round(float(row["average_entry_price"]), 6),
                    "average_spread": round(float(row["average_spread"]), 6),
                    "average_edge": round(float(row["average_edge"]), 6),
                    "positive_fold": bool(float(row["total_net_pnl"]) > 0),
                }
            )
    return rows


def _fold_positive_count(trades_df: pd.DataFrame) -> int:
    if trades_df.empty:
        return 0
    return int((trades_df.groupby("fold_number")["net_pnl"].sum() > 0).sum())


def _all_folds_positive(trades_df: pd.DataFrame) -> bool:
    if trades_df.empty:
        return False
    pnl = trades_df.groupby("fold_number")["net_pnl"].sum()
    return bool((pnl > 0).all())


def _rank_hours(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame()
    pooled_b = _scenario_scope(summary_df, "B", "pooled").rename(
        columns={
            "total_net_pnl": "pooled_b_pnl",
            "trades_taken": "pooled_b_trades",
            "positive_fold_count": "pooled_b_positive_folds",
        }
    )
    pooled_d = _scenario_scope(summary_df, "D", "pooled").rename(
        columns={
            "total_net_pnl": "pooled_d_pnl",
            "trades_taken": "pooled_d_trades",
            "positive_fold_count": "pooled_d_positive_folds",
            "all_folds_positive": "pooled_d_all_folds_positive",
        }
    )
    nyc_d = _scenario_scope(summary_df, "D", "nyc").rename(columns={"total_net_pnl": "nyc_d_pnl"})
    chi_d = _scenario_scope(summary_df, "D", "chicago").rename(columns={"total_net_pnl": "chicago_d_pnl"})

    ranked = pooled_b.merge(pooled_d, on="decision_time_local", how="outer")
    ranked = ranked.merge(nyc_d[["decision_time_local", "nyc_d_pnl"]], on="decision_time_local", how="left")
    ranked = ranked.merge(chi_d[["decision_time_local", "chicago_d_pnl"]], on="decision_time_local", how="left")
    ranked["both_cities_positive_d"] = (ranked["nyc_d_pnl"] > 0) & (ranked["chicago_d_pnl"] > 0)
    ranked = ranked.sort_values(
        [
            "both_cities_positive_d",
            "pooled_d_all_folds_positive",
            "pooled_d_positive_folds",
            "pooled_d_pnl",
            "pooled_d_trades",
            "pooled_b_pnl",
        ],
        ascending=[False, False, False, False, False, False],
        kind="stable",
    ).reset_index(drop=True)
    ranked["credibility_rank"] = range(1, len(ranked) + 1)
    return ranked[
        [
            "credibility_rank",
            "decision_time_local",
            "both_cities_positive_d",
            "pooled_d_all_folds_positive",
            "pooled_d_positive_folds",
            "pooled_d_pnl",
            "pooled_d_trades",
            "pooled_b_pnl",
            "pooled_b_trades",
            "nyc_d_pnl",
            "chicago_d_pnl",
        ]
    ]


def _scenario_scope(summary_df: pd.DataFrame, scenario_code: str, city_scope: str) -> pd.DataFrame:
    return summary_df.loc[
        (summary_df["scenario_code"] == scenario_code) & (summary_df["city_scope"] == city_scope)
    ].copy()


def _baseline_comparison(summary_df: pd.DataFrame) -> pd.DataFrame:
    base = summary_df.loc[summary_df["decision_time_local"] == "10:00"][
        ["scenario_code", "city_scope", "total_net_pnl", "trades_taken"]
    ].rename(
        columns={
            "total_net_pnl": "baseline_1000_total_net_pnl",
            "trades_taken": "baseline_1000_trades_taken",
        }
    )
    comparison = summary_df.merge(base, on=["scenario_code", "city_scope"], how="left")
    comparison["delta_vs_1000_pnl"] = (
        comparison["total_net_pnl"] - comparison["baseline_1000_total_net_pnl"]
    ).round(6)
    comparison["delta_vs_1000_trades"] = comparison["trades_taken"] - comparison["baseline_1000_trades_taken"]
    return comparison


def _build_amsterdam_note(decision_times_local: tuple[str, ...]) -> dict[str, object]:
    reference_date = date.today()
    nyc_map = {value: _convert_hour(value, "America/New_York", "Europe/Amsterdam", reference_date) for value in decision_times_local}
    chi_map = {value: _convert_hour(value, "America/Chicago", "Europe/Amsterdam", reference_date) for value in decision_times_local}
    return {
        "reference_date": reference_date.isoformat(),
        "nyc_local_to_amsterdam": nyc_map,
        "chicago_local_to_amsterdam": chi_map,
    }


def _convert_hour(value: str, from_tz: str, to_tz: str, reference_date: date) -> str:
    local_time = _parse_local_time(value)
    local_dt = datetime.combine(reference_date, local_time, tzinfo=ZoneInfo(from_tz))
    return local_dt.astimezone(ZoneInfo(to_tz)).strftime("%H:%M")


def _parse_local_time(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise TimeOfDaySensitivityError(f"Invalid decision time {value!r}. Expected HH:MM.") from exc


def _render_markdown(
    report: dict[str, object],
    summary_df: pd.DataFrame,
    fold_df: pd.DataFrame,
    ranked_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
) -> str:
    lines = [
        "# Time-of-Day Sensitivity",
        "",
        f"- Generated at (UTC): `{report['generated_at_utc']}`",
        f"- Times tested: `{', '.join(report['times_tested'])}`",
        f"- Gate fixed: `{VALIDATED_GATE['contract_type']}` / `{VALIDATED_GATE['chosen_side']}` / `{VALIDATED_GATE['entry_price_bucket']}`",
        f"- Walk-forward profile: `{report['methodology']['walkforward_profile']}`",
        "",
        "## Methodology",
        "",
        "- Reused the existing backtest mart builder with different `decision_time_local` values.",
        "- Held the climatology model fixed: `day_window=1`, `min_lookback_samples=30`.",
        "- Held the validated narrow gate fixed for every hour.",
        "- Reused the existing executable-proxy friction scenarios A-D with no hour-specific tuning.",
        "",
        "## Ranking By Credibility",
        "",
    ]
    if ranked_df.empty:
        lines.append("- No results.")
    else:
        lines.append("| Rank | Time | Both Cities Positive D | D Positive Folds | Pooled D PnL | Pooled D Trades | Pooled B PnL |")
        lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: |")
        for row in ranked_df.to_dict("records"):
            lines.append(
                f"| {row['credibility_rank']} | {row['decision_time_local']} | {row['both_cities_positive_d']} | "
                f"{row['pooled_d_positive_folds']} | {row['pooled_d_pnl']:.3f} | {row['pooled_d_trades']} | {row['pooled_b_pnl']:.3f} |"
            )
    lines.append("")

    for city_scope in ["pooled", "nyc", "chicago"]:
        lines.extend([f"## {city_scope.upper()} Results", ""])
        subset = summary_df.loc[summary_df["city_scope"] == city_scope].copy()
        if subset.empty:
            lines.append("- No results.")
            lines.append("")
            continue
        lines.append("| Time | Scenario | Trades | Net PnL | Win Rate | Avg Entry | Avg Spread | Positive Folds |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for row in subset.sort_values(["decision_time_local", "scenario_code"]).to_dict("records"):
            lines.append(
                f"| {row['decision_time_local']} | {row['scenario_code']} | {row['trades_taken']} | "
                f"{row['total_net_pnl']:.3f} | {row['win_rate']:.3f} | {row['average_entry_price']:.3f} | "
                f"{row['average_spread']:.3f} | {row['positive_fold_count']} |"
            )
        lines.append("")

    lines.extend(["## Fold Detail", ""])
    if fold_df.empty:
        lines.append("- No fold-level results.")
    else:
        lines.append("| Time | Scenario | Scope | Fold | Trades | Net PnL | Win Rate | Avg Entry | Avg Spread |")
        lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for row in fold_df.sort_values(["decision_time_local", "scenario_code", "city_scope", "fold_number"]).to_dict("records"):
            lines.append(
                f"| {row['decision_time_local']} | {row['scenario_code']} | {row['city_scope']} | {row['fold_number']} | "
                f"{row['trades_taken']} | {row['total_net_pnl']:.3f} | {row['win_rate']:.3f} | "
                f"{row['average_entry_price']:.3f} | {row['average_spread']:.3f} |"
            )
    lines.append("")

    lines.extend(["## 10:00 Comparison", ""])
    for city_scope in ["pooled", "nyc", "chicago"]:
        subset = comparison_df.loc[
            (comparison_df["city_scope"] == city_scope) & (comparison_df["scenario_code"].isin(["B", "D"]))
        ].copy()
        lines.append(f"### {city_scope.upper()}")
        lines.append("")
        lines.append("| Time | Scenario | PnL Delta vs 10:00 | Trade Delta vs 10:00 |")
        lines.append("| --- | --- | ---: | ---: |")
        for row in subset.sort_values(["decision_time_local", "scenario_code"]).to_dict("records"):
            lines.append(
                f"| {row['decision_time_local']} | {row['scenario_code']} | "
                f"{row['delta_vs_1000_pnl']:.3f} | {int(row['delta_vs_1000_trades'])} |"
            )
        lines.append("")

    note = report["operational_note_amsterdam"]
    lines.extend(
        [
            "## Amsterdam Note",
            "",
            f"- Reference date: `{note['reference_date']}`",
            f"- NYC local to Amsterdam: `{note['nyc_local_to_amsterdam']}`",
            f"- Chicago local to Amsterdam: `{note['chicago_local_to_amsterdam']}`",
            "",
            "## What Could Make This Wrong",
            "",
            "- Multiple-testing risk: this is still a seven-hour sweep, so the best-looking hour may win by noise.",
            "- Hourly candle limitation: the execution proxy only moves in hourly steps, so sub-hour timing claims would be fake precision.",
            "- Sample size: the gated slice is small, and fold stability still matters more than raw PnL.",
            "- Seasonal concentration: the sample remains winter-heavy, so an apparent hour effect may partly be a winter-slice artifact.",
            "",
        ]
    )

    recommendation = _recommendation_text(ranked_df, summary_df)
    lines.extend(["## Recommendation", "", recommendation, ""])
    return "\n".join(lines)


def _recommendation_text(ranked_df: pd.DataFrame, summary_df: pd.DataFrame) -> str:
    if ranked_df.empty:
        return "No recommendation could be made because the sweep produced no gated trades."
    top = ranked_df.iloc[0]
    baseline = ranked_df.loc[ranked_df["decision_time_local"] == "10:00"]
    if baseline.empty:
        return f"`{top['decision_time_local']}` ranked highest, but the 10:00 baseline row was unavailable."
    baseline_row = baseline.iloc[0]
    if top["decision_time_local"] == "10:00":
        return "10:00 still ranks as the most credible coarse hour under this sweep."
    if (
        bool(top["both_cities_positive_d"])
        and float(top["pooled_d_pnl"]) > float(baseline_row["pooled_d_pnl"])
        and int(top["pooled_d_positive_folds"]) >= int(baseline_row["pooled_d_positive_folds"])
    ):
        return (
            f"`{top['decision_time_local']}` looks more promising than 10:00 on this coarse sweep, but it should be "
            "treated as a paper-trading candidate rather than a proven improvement because the sweep itself adds "
            "multiple-testing risk."
        )
    return "The tested hours look too similar or too fragile to treat any non-10:00 hour as a trusted improvement."
