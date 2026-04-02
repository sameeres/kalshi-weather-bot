from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.execution.paper_climatology import DEFAULT_PAPER_ROOT_DIR, DEFAULT_TRADES_FILENAME
from kwb.marts.backtest_dataset import resolve_bucket
from kwb.models.baseline_climatology import DEFAULT_HISTORY_PATH
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_RECONCILED_TRADES_FILENAME = "paper_climatology_reconciled_trades.parquet"
DEFAULT_RECONCILIATION_SUMMARY_FILENAME = "paper_climatology_reconciliation_summary.json"
DEFAULT_RECONCILIATION_REPORT_FILENAME = "paper_climatology_reconciliation_report.md"
DEFAULT_CUMULATIVE_SCOREBOARD_FILENAME = "paper_climatology_cumulative_scoreboard.csv"
DEFAULT_CUMULATIVE_SUMMARY_FILENAME = "paper_climatology_cumulative_summary.json"
DEFAULT_CUMULATIVE_REPORT_FILENAME = "paper_climatology_cumulative_report.md"


class PaperClimatologyReconciliationError(ValueError):
    """Raised when paper-trade reconciliation cannot complete safely."""


def reconcile_paper_climatology(
    trade_date: str | None = None,
    paper_output_root: Path | None = DEFAULT_PAPER_ROOT_DIR,
    history_path: Path | None = DEFAULT_HISTORY_PATH,
) -> tuple[Path, Path, Path, Path, Path, Path, dict[str, Any]]:
    paper_output_root = (paper_output_root or DEFAULT_PAPER_ROOT_DIR).expanduser().resolve()
    history_path = (history_path or DEFAULT_HISTORY_PATH).expanduser().resolve()
    resolved_trade_date = trade_date or _latest_reconcilable_trade_date(paper_output_root)
    daily_dir = paper_output_root / resolved_trade_date
    if not daily_dir.exists():
        raise PaperClimatologyReconciliationError(f"Paper-trading date directory does not exist: {daily_dir}")

    trades_path = daily_dir / DEFAULT_TRADES_FILENAME
    if not trades_path.exists():
        raise PaperClimatologyReconciliationError(f"Paper trades file does not exist: {trades_path}")

    trades_df = pd.read_parquet(trades_path).copy()
    history_df = pd.read_parquet(history_path).copy()
    _prepare_inputs(trades_df=trades_df, history_df=history_df, trades_path=trades_path, history_path=history_path)
    reconciled_df = _reconcile_trades_frame(trades_df=trades_df, history_df=history_df, trade_date=resolved_trade_date)

    reconciled_path = daily_dir / DEFAULT_RECONCILED_TRADES_FILENAME
    summary_path = daily_dir / DEFAULT_RECONCILIATION_SUMMARY_FILENAME
    report_path = daily_dir / DEFAULT_RECONCILIATION_REPORT_FILENAME
    reconciled_df.to_parquet(reconciled_path, index=False)

    daily_summary = _build_daily_summary(
        reconciled_df=reconciled_df,
        trade_date=resolved_trade_date,
        daily_dir=daily_dir,
        trades_path=trades_path,
        history_path=history_path,
    )
    summary_path.write_text(json.dumps(daily_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_daily_report(daily_summary, reconciled_df), encoding="utf-8")

    cumulative_scoreboard_path = paper_output_root / DEFAULT_CUMULATIVE_SCOREBOARD_FILENAME
    cumulative_summary_path = paper_output_root / DEFAULT_CUMULATIVE_SUMMARY_FILENAME
    cumulative_report_path = paper_output_root / DEFAULT_CUMULATIVE_REPORT_FILENAME
    scoreboard_df, cumulative_summary = _rebuild_cumulative_outputs(paper_output_root=paper_output_root)
    scoreboard_df.to_csv(cumulative_scoreboard_path, index=False)
    cumulative_summary_path.write_text(json.dumps(cumulative_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    cumulative_report_path.write_text(_render_cumulative_report(cumulative_summary, scoreboard_df), encoding="utf-8")

    logger.info(
        "Saved paper reconciliation for %s to %s and updated cumulative outputs in %s",
        resolved_trade_date,
        daily_dir,
        paper_output_root,
    )
    return (
        reconciled_path,
        summary_path,
        report_path,
        cumulative_scoreboard_path,
        cumulative_summary_path,
        cumulative_report_path,
        {
            "trade_date": resolved_trade_date,
            "daily_summary": daily_summary,
            "cumulative_summary": cumulative_summary,
        },
    )


def _latest_reconcilable_trade_date(paper_output_root: Path) -> str:
    if not paper_output_root.exists():
        raise PaperClimatologyReconciliationError(f"Paper output root does not exist: {paper_output_root}")
    candidates = sorted(path.name for path in paper_output_root.iterdir() if path.is_dir() and _looks_like_date(path.name))
    if not candidates:
        raise PaperClimatologyReconciliationError(f"No dated paper-trading directories found under {paper_output_root}")
    return candidates[-1]


def _looks_like_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _prepare_inputs(
    trades_df: pd.DataFrame,
    history_df: pd.DataFrame,
    trades_path: Path,
    history_path: Path,
) -> None:
    required_trade_columns = {
        "snapshot_ts",
        "city_key",
        "market_ticker",
        "event_date",
        "chosen_side",
        "entry_price_cents",
        "estimated_fees_dollars",
        "strike_type",
        "floor_strike",
        "cap_strike",
    }
    missing_trade = sorted(required_trade_columns - set(trades_df.columns))
    if missing_trade:
        raise PaperClimatologyReconciliationError(
            f"Paper trades file is missing required columns in {trades_path}: {', '.join(missing_trade)}"
        )

    required_history = {"city_key", "obs_date", "tmax_f"}
    missing_history = sorted(required_history - set(history_df.columns))
    if missing_history:
        raise PaperClimatologyReconciliationError(
            f"Weather history is missing required columns in {history_path}: {', '.join(missing_history)}"
        )


def _reconcile_trades_frame(
    trades_df: pd.DataFrame,
    history_df: pd.DataFrame,
    trade_date: str,
) -> pd.DataFrame:
    history = history_df.copy()
    history["obs_date"] = pd.to_datetime(history["obs_date"], errors="coerce")
    history = history.loc[history["obs_date"].notna()].copy()
    history["event_date"] = history["obs_date"].dt.strftime("%Y-%m-%d")
    weather_lookup = {
        (str(row["city_key"]), str(row["event_date"])): row
        for row in history[["city_key", "event_date", "tmax_f", "station_id"]].to_dict("records")
    }

    rows: list[dict[str, Any]] = []
    for row in trades_df.to_dict("records"):
        event_date = str(row["event_date"])
        city_key = str(row["city_key"])
        weather_row = weather_lookup.get((city_key, event_date))
        actual_tmax_f = None
        settlement_station_id = None
        resolved_yes = None
        settlement_outcome = "pending"
        notes = ""
        won_trade = None
        gross_payout = None
        gross_pnl = None
        realized_net_pnl = None
        total_entry_cost = None

        if weather_row is None:
            notes = "missing_settlement_weather_observation"
        else:
            actual_tmax_f = float(weather_row["tmax_f"]) if weather_row["tmax_f"] is not None else None
            settlement_station_id = weather_row.get("station_id")
            try:
                resolved_yes = resolve_bucket(
                    actual_tmax_f=actual_tmax_f,
                    floor_strike=row.get("floor_strike"),
                    cap_strike=row.get("cap_strike"),
                    strike_type=row.get("strike_type"),
                )
            except Exception as exc:
                notes = f"bucket_resolution_error:{exc}"
            else:
                if resolved_yes is None:
                    notes = "unresolved_bucket"
                else:
                    settlement_outcome = "yes" if bool(resolved_yes) else "no"
                    won_trade = bool(resolved_yes) if str(row["chosen_side"]).lower() == "yes" else not bool(resolved_yes)
                    gross_payout = 1.0 if won_trade else 0.0
                    total_entry_cost = round(float(row["entry_price_cents"]) / 100.0, 6)
                    gross_pnl = round(gross_payout - total_entry_cost, 6)
                    realized_net_pnl = round(gross_pnl - float(row.get("estimated_fees_dollars") or 0.0), 6)

        rows.append(
            {
                "trade_date": trade_date,
                "snapshot_ts": row["snapshot_ts"],
                "city_key": city_key,
                "market_ticker": row["market_ticker"],
                "event_date": event_date,
                "entry_side": str(row["chosen_side"]).lower(),
                "entry_price_cents": float(row["entry_price_cents"]),
                "contracts": 1,
                "estimated_entry_fee_dollars": round(float(row.get("estimated_fees_dollars") or 0.0), 6),
                "actual_tmax_f": actual_tmax_f,
                "settlement_station_id": settlement_station_id,
                "resolved_yes": resolved_yes,
                "settlement_outcome": settlement_outcome,
                "won_trade": won_trade,
                "lost_trade": None if won_trade is None else not won_trade,
                "gross_payout_dollars": gross_payout,
                "total_entry_cost_dollars": total_entry_cost,
                "gross_pnl_dollars": gross_pnl,
                "realized_net_pnl_dollars": realized_net_pnl,
                "resolved_status": "resolved" if won_trade is not None else "unresolved",
                "notes": notes,
                "market_title": row.get("market_title"),
                "market_subtitle": row.get("market_subtitle"),
                "contract_type": row.get("contract_type"),
                "strike_type": row.get("strike_type"),
                "floor_strike": row.get("floor_strike"),
                "cap_strike": row.get("cap_strike"),
            }
        )

    return pd.DataFrame(rows).sort_values(["trade_date", "city_key", "event_date", "market_ticker"], kind="stable").reset_index(drop=True)


def _build_daily_summary(
    reconciled_df: pd.DataFrame,
    trade_date: str,
    daily_dir: Path,
    trades_path: Path,
    history_path: Path,
) -> dict[str, Any]:
    resolved = reconciled_df.loc[reconciled_df["resolved_status"] == "resolved"].copy()
    unresolved = reconciled_df.loc[reconciled_df["resolved_status"] != "resolved"].copy()

    by_city = []
    for city_key, city_df in reconciled_df.groupby("city_key", sort=True):
        city_resolved = city_df.loc[city_df["resolved_status"] == "resolved"].copy()
        by_city.append(
            {
                "city_key": city_key,
                "total_paper_trades": int(len(city_df)),
                "resolved_trades": int(len(city_resolved)),
                "unresolved_trades": int(len(city_df) - len(city_resolved)),
                "win_count": int(city_resolved["won_trade"].fillna(False).sum()),
                "loss_count": int(city_resolved["lost_trade"].fillna(False).sum()),
                "realized_net_pnl_dollars": round(
                    float(city_resolved["realized_net_pnl_dollars"].sum()) if not city_resolved.empty else 0.0, 6
                ),
            }
        )

    failures = [
        {"market_ticker": row["market_ticker"], "notes": row["notes"]}
        for row in reconciled_df.loc[reconciled_df["notes"].astype(str) != ""].to_dict("records")
    ]

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "trade_date": trade_date,
        "daily_dir": str(daily_dir),
        "source_paths": {
            "paper_trades_path": str(trades_path),
            "history_path": str(history_path),
        },
        "settlement_assumption": (
            "Reconciled from settlement-aligned staged weather observations in data/staging/weather_daily.parquet "
            "using the existing market bucket-resolution logic from kwb.marts.backtest_dataset.resolve_bucket."
        ),
        "totals": {
            "total_paper_trades": int(len(reconciled_df)),
            "resolved_trades": int(len(resolved)),
            "unresolved_trades": int(len(unresolved)),
            "win_count": int(resolved["won_trade"].fillna(False).sum()) if not resolved.empty else 0,
            "loss_count": int(resolved["lost_trade"].fillna(False).sum()) if not resolved.empty else 0,
            "win_rate_resolved": round(float(resolved["won_trade"].mean()), 6) if not resolved.empty else 0.0,
            "gross_payout_dollars": round(float(resolved["gross_payout_dollars"].sum()) if not resolved.empty else 0.0, 6),
            "total_entry_cost_dollars": round(float(resolved["total_entry_cost_dollars"].sum()) if not resolved.empty else 0.0, 6),
            "total_fees_dollars": round(float(resolved["estimated_entry_fee_dollars"].sum()) if not resolved.empty else 0.0, 6),
            "realized_net_pnl_dollars": round(float(resolved["realized_net_pnl_dollars"].sum()) if not resolved.empty else 0.0, 6),
        },
        "by_city": by_city,
        "reconciliation_failures_or_ambiguities": failures,
    }


def _rebuild_cumulative_outputs(paper_output_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    records: list[dict[str, Any]] = []
    unresolved_records: list[dict[str, Any]] = []

    for daily_dir in sorted(path for path in paper_output_root.iterdir() if path.is_dir() and _looks_like_date(path.name)):
        reconciled_path = daily_dir / DEFAULT_RECONCILED_TRADES_FILENAME
        if not reconciled_path.exists():
            continue
        df = pd.read_parquet(reconciled_path).copy()
        resolved = df.loc[df["resolved_status"] == "resolved"].copy()
        unresolved = df.loc[df["resolved_status"] != "resolved"].copy()
        records.append(
            {
                "trade_date": daily_dir.name,
                "total_paper_trades": int(len(df)),
                "resolved_trades": int(len(resolved)),
                "unresolved_trades": int(len(unresolved)),
                "win_count": int(resolved["won_trade"].fillna(False).sum()) if not resolved.empty else 0,
                "loss_count": int(resolved["lost_trade"].fillna(False).sum()) if not resolved.empty else 0,
                "gross_payout_dollars": round(float(resolved["gross_payout_dollars"].sum()) if not resolved.empty else 0.0, 6),
                "total_entry_cost_dollars": round(float(resolved["total_entry_cost_dollars"].sum()) if not resolved.empty else 0.0, 6),
                "total_fees_dollars": round(float(resolved["estimated_entry_fee_dollars"].sum()) if not resolved.empty else 0.0, 6),
                "realized_net_pnl_dollars": round(float(resolved["realized_net_pnl_dollars"].sum()) if not resolved.empty else 0.0, 6),
                "win_rate_resolved": round(float(resolved["won_trade"].mean()), 6) if not resolved.empty else 0.0,
            }
        )
        unresolved_records.extend(
            {"trade_date": daily_dir.name, "market_ticker": row["market_ticker"], "notes": row["notes"]}
            for row in unresolved.to_dict("records")
        )

    scoreboard_df = pd.DataFrame(records).sort_values(["trade_date"], kind="stable").reset_index(drop=True) if records else pd.DataFrame(
        columns=[
            "trade_date",
            "total_paper_trades",
            "resolved_trades",
            "unresolved_trades",
            "win_count",
            "loss_count",
            "gross_payout_dollars",
            "total_entry_cost_dollars",
            "total_fees_dollars",
            "realized_net_pnl_dollars",
            "win_rate_resolved",
        ]
    )

    city_breakdown = []
    resolved_all = pd.DataFrame()
    if records:
        reconciled_frames = [
            pd.read_parquet(path / DEFAULT_RECONCILED_TRADES_FILENAME)
            for path in sorted(paper_output_root.iterdir())
            if path.is_dir() and _looks_like_date(path.name) and (path / DEFAULT_RECONCILED_TRADES_FILENAME).exists()
        ]
        if reconciled_frames:
            resolved_all = pd.concat(reconciled_frames, ignore_index=True)
            resolved_all = resolved_all.loc[resolved_all["resolved_status"] == "resolved"].copy()
            for city_key, city_df in resolved_all.groupby("city_key", sort=True):
                city_breakdown.append(
                    {
                        "city_key": city_key,
                        "resolved_trades": int(len(city_df)),
                        "win_rate_resolved": round(float(city_df["won_trade"].mean()), 6) if not city_df.empty else 0.0,
                        "realized_net_pnl_dollars": round(float(city_df["realized_net_pnl_dollars"].sum()), 6),
                    }
                )

    cumulative_summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "paper_output_root": str(paper_output_root),
        "totals": {
            "paper_monitor_days_reconciled": int(len(scoreboard_df)),
            "resolved_trades": int(scoreboard_df["resolved_trades"].sum()) if not scoreboard_df.empty else 0,
            "unresolved_trades": int(scoreboard_df["unresolved_trades"].sum()) if not scoreboard_df.empty else 0,
            "cumulative_realized_net_pnl_dollars": round(float(scoreboard_df["realized_net_pnl_dollars"].sum()) if not scoreboard_df.empty else 0.0, 6),
            "cumulative_win_rate_resolved": round(float(resolved_all["won_trade"].mean()), 6) if not resolved_all.empty else 0.0,
            "average_pnl_per_resolved_trade_dollars": round(float(resolved_all["realized_net_pnl_dollars"].mean()), 6) if not resolved_all.empty else 0.0,
        },
        "city_level_breakdown": city_breakdown,
        "date_level_breakdown": scoreboard_df.to_dict("records"),
        "pending_unresolved_trades": unresolved_records,
        "notes": [
            "Cumulative outputs are rebuilt from daily reconciled trade files on every run.",
            "Unresolved trades remain excluded from realized PnL until settlement-aligned weather observations are available.",
        ],
    }
    return scoreboard_df, cumulative_summary


def _render_daily_report(summary: dict[str, Any], reconciled_df: pd.DataFrame) -> str:
    lines = [
        "# Paper Climatology Reconciliation",
        "",
        f"- Generated at (UTC): `{summary['generated_at_utc']}`",
        f"- Trade date: `{summary['trade_date']}`",
        f"- Total paper trades: `{summary['totals']['total_paper_trades']}`",
        f"- Resolved trades: `{summary['totals']['resolved_trades']}`",
        f"- Unresolved trades: `{summary['totals']['unresolved_trades']}`",
        f"- Win count: `{summary['totals']['win_count']}`",
        f"- Loss count: `{summary['totals']['loss_count']}`",
        f"- Win rate among resolved: `{summary['totals']['win_rate_resolved']}`",
        f"- Gross payout: `{summary['totals']['gross_payout_dollars']}`",
        f"- Total entry cost: `{summary['totals']['total_entry_cost_dollars']}`",
        f"- Total fees: `{summary['totals']['total_fees_dollars']}`",
        f"- Realized net PnL: `{summary['totals']['realized_net_pnl_dollars']}`",
        "",
        "## By City",
        "",
    ]
    for row in summary["by_city"]:
        lines.append(
            f"- `{row['city_key']}` trades={row['total_paper_trades']} resolved={row['resolved_trades']} "
            f"unresolved={row['unresolved_trades']} net_pnl={row['realized_net_pnl_dollars']}"
        )
    lines.append("")
    lines.extend(["## Reconciled Trades", ""])
    if reconciled_df.empty:
        lines.append("- None")
    else:
        lines.append("| market | city | event_date | entry | outcome | status | net_pnl | notes |")
        lines.append("| --- | --- | --- | ---: | --- | --- | ---: | --- |")
        for row in reconciled_df.to_dict("records"):
            entry = row["entry_price_cents"] if row["entry_price_cents"] is not None else 0.0
            pnl = row["realized_net_pnl_dollars"] if row["realized_net_pnl_dollars"] is not None else 0.0
            lines.append(
                f"| {row['market_ticker']} | {row['city_key']} | {row['event_date']} | {entry:.1f} | "
                f"{row['settlement_outcome']} | {row['resolved_status']} | {pnl:.4f} | {row['notes']} |"
            )
    lines.append("")
    if summary["reconciliation_failures_or_ambiguities"]:
        lines.extend(["## Failures / Ambiguities", ""])
        for row in summary["reconciliation_failures_or_ambiguities"]:
            lines.append(f"- `{row['market_ticker']}`: {row['notes']}")
        lines.append("")
    return "\n".join(lines)


def _render_cumulative_report(summary: dict[str, Any], scoreboard_df: pd.DataFrame) -> str:
    lines = [
        "# Paper Climatology Cumulative Summary",
        "",
        f"- Generated at (UTC): `{summary['generated_at_utc']}`",
        f"- Paper monitor days reconciled: `{summary['totals']['paper_monitor_days_reconciled']}`",
        f"- Total resolved trades: `{summary['totals']['resolved_trades']}`",
        f"- Total unresolved trades: `{summary['totals']['unresolved_trades']}`",
        f"- Cumulative realized net PnL: `{summary['totals']['cumulative_realized_net_pnl_dollars']}`",
        f"- Cumulative win rate: `{summary['totals']['cumulative_win_rate_resolved']}`",
        f"- Average PnL per resolved trade: `{summary['totals']['average_pnl_per_resolved_trade_dollars']}`",
        "",
        "## By City",
        "",
    ]
    for row in summary["city_level_breakdown"]:
        lines.append(
            f"- `{row['city_key']}` resolved_trades={row['resolved_trades']} win_rate={row['win_rate_resolved']} "
            f"net_pnl={row['realized_net_pnl_dollars']}"
        )
    lines.append("")
    lines.extend(["## By Date", ""])
    if scoreboard_df.empty:
        lines.append("- None")
    else:
        lines.append("| trade_date | resolved | unresolved | wins | losses | net_pnl |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        for row in scoreboard_df.to_dict("records"):
            lines.append(
                f"| {row['trade_date']} | {row['resolved_trades']} | {row['unresolved_trades']} | "
                f"{row['win_count']} | {row['loss_count']} | {row['realized_net_pnl_dollars']:.4f} |"
            )
    lines.append("")
    if summary["pending_unresolved_trades"]:
        lines.extend(["## Pending Unresolved Trades", ""])
        for row in summary["pending_unresolved_trades"]:
            lines.append(f"- `{row['trade_date']}` `{row['market_ticker']}`: {row['notes']}")
        lines.append("")
    return "\n".join(lines)
