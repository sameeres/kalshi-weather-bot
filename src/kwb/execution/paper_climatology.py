from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from kwb.backtest.fees import modeled_trade_fee
from kwb.features.market_microstructure import spread
from kwb.ingestion.kalshi_events import DEFAULT_CITIES_CONFIG_PATH
from kwb.ingestion.kalshi_microstructure import capture_kalshi_microstructure_for_enabled_cities
from kwb.models.baseline_climatology import (
    DEFAULT_MODEL_NAME,
    DEFAULT_HISTORY_PATH,
    estimate_climatology_prob_yes,
    select_climatology_lookback,
)
from kwb.settings import CONFIG_DIR, MARTS_DIR, STAGING_DIR
from kwb.utils.io import read_yaml
from kwb.utils.logging import get_logger

logger = get_logger(__name__)

DEFAULT_PAPER_CONFIG_PATH = CONFIG_DIR / "paper_trading.yml"
DEFAULT_PAPER_ROOT_DIR = MARTS_DIR / "paper_trading"
DEFAULT_EVALUATIONS_FILENAME = "paper_climatology_evaluations.parquet"
DEFAULT_TRADES_FILENAME = "paper_climatology_trades.parquet"
DEFAULT_SUMMARY_FILENAME = "paper_climatology_summary.json"
DEFAULT_REPORT_FILENAME = "paper_climatology_report.md"

REQUIRED_EVALUATION_COLUMNS = {
    "snapshot_ts",
    "evaluation_ts",
    "city_key",
    "market_ticker",
    "contract_type",
    "chosen_side",
    "gate_passed",
    "take_paper_trade",
    "rejection_reasons",
    "entry_price_cents",
    "entry_price_bucket",
    "best_yes_bid_cents",
    "best_yes_ask_cents",
    "best_no_bid_cents",
    "best_no_ask_cents",
    "quote_source",
    "orderbook_available",
    "yes_spread_cents",
    "fair_yes",
    "model_prob_yes",
    "gross_edge_yes",
    "net_edge_yes",
    "estimated_fees_dollars",
    "lookback_sample_size",
    "model_name",
}

EVALUATION_COLUMNS = [
    "snapshot_ts",
    "evaluation_ts",
    "strategy_name",
    "paper_only_mode",
    "city_key",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "event_date",
    "market_status",
    "market_title",
    "market_subtitle",
    "strike_date",
    "floor_strike",
    "cap_strike",
    "strike_type",
    "contract_type",
    "chosen_side",
    "gate_passed",
    "take_paper_trade",
    "rejection_reasons",
    "entry_price_cents",
    "entry_price_bucket",
    "best_yes_bid_cents",
    "best_yes_ask_cents",
    "best_no_bid_cents",
    "best_no_ask_cents",
    "best_yes_bid_size",
    "best_yes_ask_size",
    "best_no_bid_size",
    "best_no_ask_size",
    "quote_source",
    "orderbook_available",
    "yes_spread_cents",
    "no_spread_cents",
    "tick_size",
    "price_level_structure",
    "lookback_sample_size",
    "fair_yes",
    "fair_no",
    "model_prob_yes",
    "model_prob_no",
    "gross_edge_yes",
    "net_edge_yes",
    "estimated_fees_dollars",
    "gate_max_entry_price_cents",
    "decision_min_net_edge",
    "decision_max_spread_cents",
    "decision_fee_model",
    "day_window",
    "min_lookback_samples",
    "model_name",
]


class PaperClimatologyMonitorError(ValueError):
    """Raised when the paper-only climatology monitor cannot complete safely."""


def run_paper_climatology_monitor(
    config_path: Path = DEFAULT_CITIES_CONFIG_PATH,
    paper_config_path: Path = DEFAULT_PAPER_CONFIG_PATH,
    history_path: Path | None = DEFAULT_HISTORY_PATH,
    output_root: Path | None = None,
    microstructure_dir: Path | None = None,
    iterations: int | None = None,
    poll_interval_seconds: float | None = None,
    status: str | None = None,
    include_orderbook: bool | None = None,
    orderbook_depth: int | None = None,
    min_net_edge: float | None = None,
    max_spread_cents: float | None = None,
    max_entry_price_cents: float | None = None,
) -> tuple[Path, Path, Path, Path, dict[str, Any]]:
    history_path = history_path or DEFAULT_HISTORY_PATH
    config = _load_monitor_config(paper_config_path)
    strategy_name = str(config["strategy_name"])
    gate = dict(config["gate"])
    decision = dict(config["decision"])
    capture = dict(config["capture"])

    if iterations is not None:
        capture["iterations"] = int(iterations)
    if poll_interval_seconds is not None:
        capture["poll_interval_seconds"] = float(poll_interval_seconds)
    if status is not None:
        capture["status"] = status
    if include_orderbook is not None:
        capture["include_orderbook"] = bool(include_orderbook)
    if orderbook_depth is not None:
        capture["orderbook_depth"] = int(orderbook_depth)
    if min_net_edge is not None:
        decision["min_net_edge"] = float(min_net_edge)
    if max_spread_cents is not None:
        decision["max_spread_cents"] = float(max_spread_cents)
    if max_entry_price_cents is not None:
        gate["max_entry_price_cents"] = float(max_entry_price_cents)

    history_df = pd.read_parquet(history_path).copy()
    _validate_history_frame(history_df=history_df, history_path=history_path)
    history_df["obs_date"] = pd.to_datetime(history_df["obs_date"], errors="coerce")
    history_df["month_day"] = history_df["obs_date"].dt.strftime("%m-%d")
    grouped_history = {
        city_key: frame.reset_index(drop=True)
        for city_key, frame in history_df.groupby("city_key", sort=False)
    }

    microstructure_dir = microstructure_dir or STAGING_DIR
    snapshots_path, levels_path, capture_summary_path, capture_summary = capture_kalshi_microstructure_for_enabled_cities(
        config_path=config_path,
        output_dir=microstructure_dir,
        status=capture.get("status"),
        include_orderbook=bool(capture["include_orderbook"]),
        orderbook_depth=int(capture["orderbook_depth"]),
        iterations=int(capture["iterations"]),
        poll_interval_seconds=capture.get("poll_interval_seconds"),
        return_summary=True,
    )

    snapshots_df = pd.read_parquet(snapshots_path).copy()
    if snapshots_df.empty:
        raise PaperClimatologyMonitorError(f"No microstructure snapshots available in {snapshots_path}.")

    snapshot_ts_values = [
        str(summary.get("snapshot_ts"))
        for summary in capture_summary.get("iteration_summaries", [])
        if summary.get("snapshot_ts")
    ]
    session_snapshots = snapshots_df.loc[snapshots_df["snapshot_ts"].astype(str).isin(snapshot_ts_values)].copy()
    if session_snapshots.empty:
        raise PaperClimatologyMonitorError("Microstructure capture completed, but no session snapshots were found.")

    evaluation_ts = datetime.now(timezone.utc).isoformat()
    evaluation_rows = [
        _evaluate_snapshot_row(
            row=row,
            grouped_history=grouped_history,
            strategy_name=strategy_name,
            evaluation_ts=evaluation_ts,
            gate=gate,
            decision=decision,
            paper_only_mode=bool(config["paper_only"]),
        )
        for row in session_snapshots.to_dict("records")
    ]

    evaluations_df = _build_evaluations_frame(evaluation_rows)
    trades_df = evaluations_df.loc[evaluations_df["take_paper_trade"]].copy()
    _validate_evaluations_frame(evaluations_df)

    output_root = output_root or DEFAULT_PAPER_ROOT_DIR
    session_date = str(pd.to_datetime(snapshot_ts_values[0], utc=True).date())
    daily_dir = output_root / session_date
    daily_dir.mkdir(parents=True, exist_ok=True)

    evaluations_out = daily_dir / DEFAULT_EVALUATIONS_FILENAME
    trades_out = daily_dir / DEFAULT_TRADES_FILENAME
    summary_out = daily_dir / DEFAULT_SUMMARY_FILENAME
    report_out = daily_dir / DEFAULT_REPORT_FILENAME

    all_evaluations = _append_or_create_parquet(
        path=evaluations_out,
        frame=evaluations_df,
        unique_keys=["snapshot_ts", "market_ticker"],
        sort_keys=["snapshot_ts", "city_key", "market_ticker"],
    )
    all_trades = _append_or_create_parquet(
        path=trades_out,
        frame=trades_df,
        unique_keys=["snapshot_ts", "market_ticker"],
        sort_keys=["snapshot_ts", "city_key", "market_ticker"],
    )
    full_evaluations = pd.read_parquet(evaluations_out).copy()
    full_trades = pd.read_parquet(trades_out).copy() if trades_out.exists() else trades_df.copy()

    summary = _build_summary(
        evaluations_df=full_evaluations,
        trades_df=full_trades,
        strategy_name=strategy_name,
        config_path=config_path,
        paper_config_path=paper_config_path,
        history_path=history_path,
        snapshots_path=snapshots_path,
        levels_path=levels_path,
        capture_summary_path=capture_summary_path,
        capture_summary=capture_summary,
        gate=gate,
        decision=decision,
        daily_dir=daily_dir,
        evaluation_rows_written=all_evaluations,
        trade_rows_written=all_trades,
    )
    summary_out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_out.write_text(_render_report(summary, full_evaluations, full_trades), encoding="utf-8")

    logger.info(
        "Saved paper-only climatology monitor outputs to %s (evaluations=%s trades=%s)",
        daily_dir,
        len(evaluations_df),
        len(trades_df),
    )
    return evaluations_out, trades_out, summary_out, report_out, summary


def _load_monitor_config(path: Path) -> dict[str, Any]:
    payload = read_yaml(path)
    if not isinstance(payload, dict) or "paper_climatology_monitor" not in payload:
        raise PaperClimatologyMonitorError(f"Expected paper_climatology_monitor config in {path}.")
    monitor = payload["paper_climatology_monitor"]
    required_sections = {"gate", "decision", "capture"}
    missing = sorted(required_sections - set(monitor))
    if missing:
        raise PaperClimatologyMonitorError(f"Paper monitor config is missing sections: {', '.join(missing)}")
    return {
        "strategy_name": monitor.get("strategy_name", "climatology_or_below_yes_cheap_v1"),
        "paper_only": bool(monitor.get("paper_only", True)),
        "gate": {
            "contract_type": str(monitor["gate"].get("contract_type", "or_below")),
            "chosen_side": _normalize_side_value(monitor["gate"].get("chosen_side", "yes")),
            "max_entry_price_cents": float(monitor["gate"].get("max_entry_price_cents", 25.0)),
        },
        "decision": {
            "day_window": int(monitor["decision"].get("day_window", 1)),
            "min_lookback_samples": int(monitor["decision"].get("min_lookback_samples", 30)),
            "contracts": int(monitor["decision"].get("contracts", 1)),
            "fee_model": str(monitor["decision"].get("fee_model", "kalshi_standard_taker")),
            "fee_per_contract": float(monitor["decision"].get("fee_per_contract", 0.0)),
            "min_net_edge": float(monitor["decision"].get("min_net_edge", 0.05)),
            "max_spread_cents": float(monitor["decision"].get("max_spread_cents", 2.0)),
        },
        "capture": {
            "status": monitor["capture"].get("status", "open"),
            "include_orderbook": bool(monitor["capture"].get("include_orderbook", True)),
            "orderbook_depth": int(monitor["capture"].get("orderbook_depth", 10)),
            "iterations": int(monitor["capture"].get("iterations", 1)),
            "poll_interval_seconds": float(monitor["capture"].get("poll_interval_seconds", 60)),
        },
    }


def _validate_history_frame(history_df: pd.DataFrame, history_path: Path) -> None:
    required = {"city_key", "obs_date", "tmax_f"}
    missing = sorted(required - set(history_df.columns))
    if missing:
        raise PaperClimatologyMonitorError(
            f"Weather history is missing required columns in {history_path}: {', '.join(missing)}"
        )


def _evaluate_snapshot_row(
    row: dict[str, Any],
    grouped_history: dict[str, pd.DataFrame],
    strategy_name: str,
    evaluation_ts: str,
    gate: dict[str, Any],
    decision: dict[str, Any],
    paper_only_mode: bool,
) -> dict[str, Any]:
    city_key = str(row.get("city_key") or "")
    strike_type = str(row.get("strike_type") or "")
    contract_type = _contract_type_from_strike_type(strike_type)
    chosen_side = "yes"

    best_yes_bid = _optional_float(row.get("best_yes_bid_cents"))
    best_yes_ask = _optional_float(row.get("best_yes_ask_cents"))
    best_no_bid = _optional_float(row.get("best_no_bid_cents"))
    best_no_ask = _optional_float(row.get("best_no_ask_cents"))
    yes_spread = spread(best_yes_bid, best_yes_ask)
    no_spread = spread(best_no_bid, best_no_ask)
    entry_bucket = _entry_price_bucket(best_yes_ask)
    event_date = _resolve_event_date(row)

    reasons: list[str] = []
    gate_passed = (
        contract_type == gate["contract_type"]
        and chosen_side == gate["chosen_side"]
        and entry_bucket == "0-25"
    )
    if contract_type != gate["contract_type"]:
        reasons.append("not_or_below_contract")
    if entry_bucket != "0-25":
        reasons.append("entry_price_not_in_0_25_bucket")
    if str(row.get("market_status") or "").lower() != "open":
        reasons.append("market_not_open")
    if best_yes_ask is None:
        reasons.append("missing_yes_ask")
    if event_date is None:
        reasons.append("missing_event_date")

    lookback_sample_size = None
    fair_yes = None
    fair_no = None
    gross_edge_yes = None
    net_edge_yes = None
    estimated_fees_dollars = None

    city_history = grouped_history.get(city_key)
    if city_history is None:
        reasons.append("missing_city_history")
    elif event_date is not None and contract_type is not None:
        lookback = select_climatology_lookback(
            history_df=city_history,
            city_key=city_key,
            event_date=event_date,
            month_day=pd.Timestamp(event_date).strftime("%m-%d"),
            day_window=int(decision["day_window"]),
        )
        lookback_sample_size = int(len(lookback))
        if lookback_sample_size < int(decision["min_lookback_samples"]):
            reasons.append("insufficient_history")
        else:
            try:
                fair_yes = estimate_climatology_prob_yes(
                    history_df=lookback,
                    floor_strike=row.get("floor_strike"),
                    cap_strike=row.get("cap_strike"),
                    strike_type=row.get("strike_type"),
                )
            except Exception:
                reasons.append("climatology_scoring_error")
            else:
                fair_no = round(1.0 - float(fair_yes), 6)

    if best_yes_ask is not None and fair_yes is not None:
        estimated_fees_dollars = modeled_trade_fee(
            fill_price=best_yes_ask / 100.0,
            contracts=int(decision["contracts"]),
            fee_model=str(decision["fee_model"]),
            fee_per_contract=float(decision["fee_per_contract"]),
        )
        gross_edge_yes = round(float(fair_yes) - (best_yes_ask / 100.0), 6)
        net_edge_yes = round(gross_edge_yes - (estimated_fees_dollars / int(decision["contracts"])), 6)
        if best_yes_ask > float(gate["max_entry_price_cents"]):
            reasons.append("entry_price_above_gate")
        if yes_spread is not None and yes_spread > float(decision["max_spread_cents"]):
            reasons.append("spread_too_wide")
        if net_edge_yes < float(decision["min_net_edge"]):
            reasons.append("net_edge_below_threshold")

    take_paper_trade = gate_passed and not reasons

    return {
        "snapshot_ts": str(row.get("snapshot_ts")),
        "evaluation_ts": evaluation_ts,
        "strategy_name": strategy_name,
        "paper_only_mode": bool(paper_only_mode),
        "city_key": city_key,
        "series_ticker": row.get("series_ticker"),
        "event_ticker": row.get("event_ticker"),
        "market_ticker": row.get("market_ticker"),
        "event_date": event_date,
        "market_status": row.get("market_status"),
        "market_title": row.get("market_title"),
        "market_subtitle": row.get("market_subtitle"),
        "strike_date": row.get("strike_date"),
        "floor_strike": _optional_float(row.get("floor_strike")),
        "cap_strike": _optional_float(row.get("cap_strike")),
        "strike_type": row.get("strike_type"),
        "contract_type": contract_type,
        "chosen_side": chosen_side,
        "gate_passed": gate_passed,
        "take_paper_trade": take_paper_trade,
        "rejection_reasons": "" if take_paper_trade else "|".join(dict.fromkeys(reasons)),
        "entry_price_cents": best_yes_ask,
        "entry_price_bucket": entry_bucket,
        "best_yes_bid_cents": best_yes_bid,
        "best_yes_ask_cents": best_yes_ask,
        "best_no_bid_cents": best_no_bid,
        "best_no_ask_cents": best_no_ask,
        "best_yes_bid_size": _optional_float(row.get("best_yes_bid_size")),
        "best_yes_ask_size": _optional_float(row.get("best_yes_ask_size")),
        "best_no_bid_size": _optional_float(row.get("best_no_bid_size")),
        "best_no_ask_size": _optional_float(row.get("best_no_ask_size")),
        "quote_source": row.get("quote_source"),
        "orderbook_available": bool(row.get("orderbook_available")),
        "yes_spread_cents": yes_spread,
        "no_spread_cents": no_spread,
        "tick_size": _optional_float(row.get("tick_size")),
        "price_level_structure": row.get("price_level_structure"),
        "lookback_sample_size": lookback_sample_size,
        "fair_yes": fair_yes,
        "fair_no": fair_no,
        "model_prob_yes": fair_yes,
        "model_prob_no": fair_no,
        "gross_edge_yes": gross_edge_yes,
        "net_edge_yes": net_edge_yes,
        "estimated_fees_dollars": estimated_fees_dollars,
        "gate_max_entry_price_cents": float(gate["max_entry_price_cents"]),
        "decision_min_net_edge": float(decision["min_net_edge"]),
        "decision_max_spread_cents": float(decision["max_spread_cents"]),
        "decision_fee_model": str(decision["fee_model"]),
        "day_window": int(decision["day_window"]),
        "min_lookback_samples": int(decision["min_lookback_samples"]),
        "model_name": DEFAULT_MODEL_NAME,
    }


def _resolve_event_date(row: dict[str, Any]) -> str | None:
    raw = row.get("strike_date")
    if raw not in (None, ""):
        timestamp = pd.to_datetime(raw, errors="coerce", utc=True)
        if pd.notna(timestamp):
            return str(timestamp.date())
    event_ticker = str(row.get("event_ticker") or "")
    if event_ticker:
        parsed = _parse_kalshi_date_token(event_ticker)
        if parsed is not None:
            return parsed
    market_ticker = str(row.get("market_ticker") or "")
    if market_ticker:
        parsed = _parse_kalshi_date_token(market_ticker)
        if parsed is not None:
            return parsed
    return None


def _parse_kalshi_date_token(token: str) -> str | None:
    parts = token.split("-")
    if len(parts) < 2:
        return None
    date_token = parts[1]
    if len(date_token) != 7:
        return None
    try:
        parsed = datetime.strptime(date_token, "%y%b%d")
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def _contract_type_from_strike_type(strike_type: str | None) -> str | None:
    normalized = str(strike_type or "").lower()
    if normalized in {"less", "below"}:
        return "or_below"
    if normalized in {"greater", "above"}:
        return "or_above"
    if normalized == "between":
        return "between"
    return None


def _entry_price_bucket(entry_price_cents: float | None) -> str | None:
    if entry_price_cents is None:
        return None
    if entry_price_cents <= 25.0:
        return "0-25"
    if entry_price_cents <= 50.0:
        return "25-50"
    if entry_price_cents <= 75.0:
        return "50-75"
    return "75-100"


def _build_evaluations_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=EVALUATION_COLUMNS)
    if df.empty:
        return df
    return df.sort_values(["snapshot_ts", "city_key", "market_ticker"], kind="stable").reset_index(drop=True)


def _validate_evaluations_frame(frame: pd.DataFrame) -> None:
    missing = sorted(REQUIRED_EVALUATION_COLUMNS - set(frame.columns))
    if missing:
        raise PaperClimatologyMonitorError(
            f"Paper evaluation log is missing required columns: {', '.join(missing)}"
        )


def _append_or_create_parquet(
    path: Path,
    frame: pd.DataFrame,
    unique_keys: list[str],
    sort_keys: list[str],
) -> int:
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, frame], ignore_index=True)
    else:
        combined = frame.copy()
    if combined.empty:
        combined = combined.reindex(columns=frame.columns)
    combined = combined.drop_duplicates(subset=unique_keys, keep="last")
    if not combined.empty:
        combined = combined.sort_values(sort_keys, kind="stable").reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path, index=False)
    return int(len(combined))


def _build_summary(
    evaluations_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    strategy_name: str,
    config_path: Path,
    paper_config_path: Path,
    history_path: Path,
    snapshots_path: Path,
    levels_path: Path,
    capture_summary_path: Path,
    capture_summary: dict[str, Any],
    gate: dict[str, Any],
    decision: dict[str, Any],
    daily_dir: Path,
    evaluation_rows_written: int,
    trade_rows_written: int,
) -> dict[str, Any]:
    rejected_counts = _rejection_counts(evaluations_df)
    by_city = (
        evaluations_df.groupby("city_key")
        .agg(
            evaluations=("market_ticker", "count"),
            gate_passed=("gate_passed", "sum"),
            paper_trades=("take_paper_trade", "sum"),
        )
        .reset_index()
        .to_dict("records")
        if not evaluations_df.empty
        else []
    )
    trade_rows = (
        trades_df[
            ["snapshot_ts", "city_key", "market_ticker", "event_date", "entry_price_cents", "fair_yes", "net_edge_yes"]
        ]
        .sort_values(["snapshot_ts", "city_key", "net_edge_yes"], ascending=[True, True, False], kind="stable")
        .head(20)
        .to_dict("records")
        if not trades_df.empty
        else []
    )
    skipped = evaluations_df.loc[
        evaluations_df["gate_passed"] & ~evaluations_df["take_paper_trade"]
    ].copy()
    skipped_rows = (
        skipped[
            ["snapshot_ts", "city_key", "market_ticker", "event_date", "entry_price_cents", "fair_yes", "net_edge_yes", "rejection_reasons"]
        ]
        .sort_values(["net_edge_yes", "snapshot_ts"], ascending=[False, True], kind="stable")
        .head(20)
        .to_dict("records")
        if not skipped.empty
        else []
    )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategy_name": strategy_name,
        "paper_only_mode": True,
        "config_path": str(config_path),
        "paper_config_path": str(paper_config_path),
        "history_path": str(history_path),
        "daily_dir": str(daily_dir),
        "microstructure_paths": {
            "snapshots_path": str(snapshots_path),
            "levels_path": str(levels_path),
            "capture_summary_path": str(capture_summary_path),
        },
        "capture_summary": capture_summary,
        "gate": gate,
        "decision": decision,
        "totals": {
            "evaluations": int(len(evaluations_df)),
            "gate_passed": int(evaluations_df["gate_passed"].sum()) if not evaluations_df.empty else 0,
            "paper_trades": int(evaluations_df["take_paper_trade"].sum()) if not evaluations_df.empty else 0,
            "evaluation_rows_written": evaluation_rows_written,
            "trade_rows_written": trade_rows_written,
        },
        "by_city": by_city,
        "rejected_counts": rejected_counts,
        "paper_trades_top": trade_rows,
        "best_skips_top": skipped_rows,
    }


def _rejection_counts(frame: pd.DataFrame) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for value in frame.loc[~frame["take_paper_trade"], "rejection_reasons"].fillna(""):
        for reason in [token for token in str(value).split("|") if token]:
            counts[reason] = counts.get(reason, 0) + 1
    return [
        {"reason": reason, "count": counts[reason]}
        for reason in sorted(counts, key=lambda key: (-counts[key], key))
    ]


def _render_report(summary: dict[str, Any], evaluations_df: pd.DataFrame, trades_df: pd.DataFrame) -> str:
    lines = [
        "# Paper Climatology Monitor",
        "",
        f"- Generated at (UTC): `{summary['generated_at_utc']}`",
        f"- Strategy: `{summary['strategy_name']}`",
        f"- Paper only: `{summary['paper_only_mode']}`",
        f"- Evaluations: `{summary['totals']['evaluations']}`",
        f"- Gate passed: `{summary['totals']['gate_passed']}`",
        f"- Paper trades: `{summary['totals']['paper_trades']}`",
        "",
        "## Rules",
        "",
        f"- Contract type: `{summary['gate']['contract_type']}`",
        f"- Side: `{summary['gate']['chosen_side']}`",
        f"- Max entry price: `{summary['gate']['max_entry_price_cents']}` cents",
        f"- Min net edge: `{summary['decision']['min_net_edge']}`",
        f"- Max spread: `{summary['decision']['max_spread_cents']}` cents",
        f"- Fee model: `{summary['decision']['fee_model']}`",
        "",
        "## By City",
        "",
    ]
    for row in summary["by_city"]:
        lines.append(
            f"- `{row['city_key']}` evaluations={row['evaluations']} gate_passed={row['gate_passed']} paper_trades={row['paper_trades']}"
        )
    lines.append("")

    lines.extend(["## Paper Trades", ""])
    if trades_df.empty:
        lines.append("- None")
        lines.append("")
    else:
        lines.append("| snapshot_ts | city | market | event_date | entry | fair_yes | net_edge |")
        lines.append("| --- | --- | --- | --- | ---: | ---: | ---: |")
        for row in summary["paper_trades_top"]:
            lines.append(
                f"| {row['snapshot_ts']} | {row['city_key']} | {row['market_ticker']} | {row['event_date']} | "
                f"{row['entry_price_cents']:.1f} | {row['fair_yes']:.4f} | {row['net_edge_yes']:.4f} |"
            )
        lines.append("")

    lines.extend(["## Best Skips", ""])
    if not summary["best_skips_top"]:
        lines.append("- None")
        lines.append("")
    else:
        lines.append("| snapshot_ts | city | market | entry | fair_yes | net_edge | rejection |")
        lines.append("| --- | --- | --- | ---: | ---: | ---: | --- |")
        for row in summary["best_skips_top"]:
            fair_yes = row["fair_yes"] if row["fair_yes"] is not None else 0.0
            net_edge = row["net_edge_yes"] if row["net_edge_yes"] is not None else 0.0
            entry = row["entry_price_cents"] if row["entry_price_cents"] is not None else 0.0
            lines.append(
                f"| {row['snapshot_ts']} | {row['city_key']} | {row['market_ticker']} | {entry:.1f} | "
                f"{fair_yes:.4f} | {net_edge:.4f} | {row['rejection_reasons']} |"
            )
        lines.append("")

    lines.extend(["## Rejections", ""])
    if not summary["rejected_counts"]:
        lines.append("- None")
    else:
        for row in summary["rejected_counts"]:
            lines.append(f"- `{row['reason']}`: {row['count']}")
    lines.append("")
    return "\n".join(lines)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_side_value(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value).strip().lower()
