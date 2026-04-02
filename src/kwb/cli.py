from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from kwb.ingestion.kalshi_events import ingest_enabled_city_events, ingest_events_for_series
from kwb.ingestion.kalshi_market_history import ingest_kalshi_market_history_for_enabled_cities
from kwb.ingestion.kalshi_microstructure import capture_kalshi_microstructure_for_enabled_cities
from kwb.ingestion.climate_normals import ingest_climate_normals_for_enabled_cities
from kwb.ingestion.build_staging import build_staging_datasets
from kwb.ingestion.validate_staging import check_climatology_baseline_readiness, validate_staging_datasets
from kwb.ingestion.weather_history import ingest_weather_history_for_enabled_cities
from kwb.backtest.evaluate_climatology import ClimatologyEvaluationError, evaluate_climatology_strategy
from kwb.backtest.evaluate_climatology_executable import (
    ClimatologyExecutableEvaluationError,
    evaluate_climatology_executable_strategy,
)
from kwb.backtest.compare_climatology_pricing import (
    ClimatologyPricingComparisonError,
    compare_climatology_pricing_modes,
)
from kwb.backtest.walkforward_climatology import (
    WalkforwardClimatologyError,
    run_walkforward_climatology,
)
from kwb.marts.backtest_dataset import BacktestDatasetBuildError, build_backtest_dataset
from kwb.models.baseline_climatology import ClimatologyModelError, score_climatology_baseline
from kwb.research.run_climatology_baseline import (
    ClimatologyResearchRunError,
    run_climatology_baseline_research,
)
from kwb.research.stress_test_climatology_frictions import (
    ClimatologyFrictionStressTestError,
    stress_test_climatology_frictions,
)
from kwb.research.time_of_day_sensitivity import (
    TimeOfDaySensitivityError,
    run_time_of_day_sensitivity_study,
)
from kwb.research.reconcile_paper_climatology import (
    PaperClimatologyReconciliationError,
    reconcile_paper_climatology,
)
from kwb.execution.paper_climatology import (
    DEFAULT_PAPER_CONFIG_PATH,
    PaperClimatologyMonitorError,
    run_paper_climatology_monitor,
)
from kwb.mapping.station_candidates import (
    apply_station_mapping_recommendations,
    build_station_mapping_report,
    write_station_mapping_recommendations,
    write_station_mapping_report,
)
from kwb.mapping.station_mapping import (
    StationMappingValidationError,
    validate_enabled_city_mappings,
)
from kwb.settings import CONFIG_DIR
from kwb.utils.io import read_yaml

app = typer.Typer(help="Kalshi Weather Bot research CLI")
cities_app = typer.Typer(help="City configuration helpers")
ingest_app = typer.Typer(help="Data ingestion commands")
station_app = typer.Typer(help="Settlement station mapping commands")
weather_app = typer.Typer(help="Historical weather ingestion commands")
kalshi_app = typer.Typer(help="Historical Kalshi market ingestion commands")
data_app = typer.Typer(help="Bootstrap and validate staged baseline inputs")
mart_app = typer.Typer(help="Mart builders for backtesting datasets")
model_app = typer.Typer(help="Baseline research models")
backtest_app = typer.Typer(help="Research backtests and paper-trading evaluation")
research_app = typer.Typer(help="Reproducible research pipeline commands")
app.add_typer(cities_app, name="cities")
app.add_typer(ingest_app, name="ingest")
app.add_typer(station_app, name="station")
app.add_typer(weather_app, name="weather")
app.add_typer(kalshi_app, name="kalshi")
app.add_typer(data_app, name="data")
app.add_typer(mart_app, name="mart")
app.add_typer(model_app, name="model")
app.add_typer(backtest_app, name="backtest")
app.add_typer(research_app, name="research")
console = Console()


def load_cities() -> list[dict]:
    payload = read_yaml(CONFIG_DIR / "cities.yml")
    return payload.get("cities", [])


@cities_app.command("list")
def list_cities(enabled_only: bool = typer.Option(False, help="Show only enabled cities.")) -> None:
    cities = load_cities()
    if enabled_only:
        cities = [c for c in cities if c.get("enabled")]

    table = Table(title="Configured Cities")
    table.add_column("city_key")
    table.add_column("city_name")
    table.add_column("series")
    table.add_column("enabled")

    for city in cities:
        table.add_row(
            str(city.get("city_key")),
            str(city.get("city_name")),
            str(city.get("kalshi_series_ticker")),
            str(city.get("enabled")),
        )

    console.print(table)


@ingest_app.command("kalshi-events")
def ingest_kalshi_events(
    series_ticker: str = typer.Option(
        "",
        "--series-ticker",
        "-s",
        help="Kalshi series ticker, e.g. KXHIGHNY. Omit to ingest all enabled cities from config.",
    ),
    output_dir: str = typer.Option("", help="Optional output directory override."),
    config_path: str = typer.Option("", help="Optional city config path override."),
    update_city_config: bool = typer.Option(
        True,
        help="Update settlement-source fields in the city config when discovery is unambiguous.",
    ),
) -> None:
    outdir = Path(output_dir) if output_dir else None
    if series_ticker:
        outpath = ingest_events_for_series(series_ticker=series_ticker, output_dir=outdir)
    else:
        cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
        outpath = ingest_enabled_city_events(
            config_path=cfg_path,
            output_dir=outdir,
            update_city_config=update_city_config,
        )
    console.print(f"Saved: {outpath}")


@station_app.command("validate")
def validate_station_mapping(
    config_path: str = typer.Option("", help="Optional city config path override."),
    events_path: str = typer.Option(
        "",
        help="Optional staged Kalshi events parquet override for source-consistency validation.",
    ),
) -> None:
    cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
    evt_path = Path(events_path) if events_path else None

    try:
        cities = validate_enabled_city_mappings(config_path=cfg_path, events_path=evt_path)
    except StationMappingValidationError as exc:
        console.print(f"[red]Station mapping validation failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(f"Station mapping validation passed for {len(cities)} enabled cities.")


@station_app.command("report")
def station_mapping_report(
    config_path: str = typer.Option("", help="Optional city config path override."),
    events_path: str = typer.Option(
        "",
        help="Optional staged Kalshi events parquet override for settlement-source context.",
    ),
    output_path: str = typer.Option(
        "",
        help="Optional CSV output path override. Defaults to data/staging/station_mapping_report.csv.",
    ),
) -> None:
    cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
    evt_path = Path(events_path) if events_path else None
    out_path = Path(output_path) if output_path else None

    report = build_station_mapping_report(config_path=cfg_path, events_path=evt_path)
    saved_path = write_station_mapping_report(config_path=cfg_path, events_path=evt_path, output_path=out_path)

    table = Table(title="Station Mapping Report")
    table.add_column("city_key")
    table.add_column("staged_source")
    table.add_column("station_id")
    table.add_column("complete")
    table.add_column("ready")
    table.add_column("missing_fields")

    for row in report.to_dict("records"):
        staged_source = row.get("staged_settlement_source_url") or row.get("settlement_source_url") or ""
        table.add_row(
            str(row.get("city_key")),
            str(staged_source),
            str(row.get("settlement_station_id")),
            str(row.get("mapping_complete")),
            str(row.get("validation_ready")),
            str(row.get("missing_fields")),
        )

    console.print(table)
    console.print(f"Saved report: {saved_path}")


@station_app.command("recommend")
def station_mapping_recommend(
    config_path: str = typer.Option("", help="Optional city config path override."),
    events_path: str = typer.Option(
        "",
        help="Optional staged Kalshi events parquet override for settlement-source evidence.",
    ),
    city_key: str = typer.Option("", help="Optional single city_key override."),
    output_dir: str = typer.Option("", help="Optional output directory override."),
    min_confidence: float = typer.Option(0.85, help="Minimum confidence threshold for auto-selection."),
    write_config: bool = typer.Option(
        False,
        help="Opt in to writing high-confidence recommendations back to configs/cities.yml.",
    ),
) -> None:
    cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
    evt_path = Path(events_path) if events_path else None
    out_dir = Path(output_dir) if output_dir else None

    if write_config:
        _, updates, resolution = apply_station_mapping_recommendations(
            config_path=cfg_path,
            events_path=evt_path,
            city_key=city_key or None,
            min_confidence=min_confidence,
        )
        json_path, md_path, _ = write_station_mapping_recommendations(
            config_path=cfg_path,
            events_path=evt_path,
            city_key=city_key or None,
            output_dir=out_dir,
            min_confidence=min_confidence,
        )
        console.print(
            f"Saved station recommendations: json={json_path} md={md_path} "
            f"(config updates applied: {len(updates)})"
        )
        if updates:
            for update in updates:
                console.print(
                    f"Applied {update['city_key']}: {', '.join(sorted(update['changed_fields']))} "
                    f"(confidence: {update['confidence']})"
                )
        else:
            unresolved = [result["city_key"] for result in resolution["results"] if not result["selected_automatically"]]
            if unresolved:
                console.print(f"No config updates applied. Manual review required for: {', '.join(unresolved)}")
        return

    json_path, md_path, resolution = write_station_mapping_recommendations(
        config_path=cfg_path,
        events_path=evt_path,
        city_key=city_key or None,
        output_dir=out_dir,
        min_confidence=min_confidence,
    )
    auto_selected = sum(1 for result in resolution["results"] if result["selected_automatically"])
    console.print(
        f"Saved station recommendations: json={json_path} md={md_path} "
        f"(auto-selectable cities: {auto_selected}/{len(resolution['results'])})"
    )


@weather_app.command("history")
def ingest_weather_history(
    start_date: str = typer.Option(..., help="Observation start date in YYYY-MM-DD format."),
    end_date: str = typer.Option(..., help="Observation end date in YYYY-MM-DD format."),
    config_path: str = typer.Option("", help="Optional city config path override."),
    events_path: str = typer.Option(
        "",
        help="Optional staged Kalshi events parquet override for station validation.",
    ),
    output_dir: str = typer.Option("", help="Optional output directory override."),
) -> None:
    cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
    evt_path = Path(events_path) if events_path else None
    out_dir = Path(output_dir) if output_dir else None

    try:
        outpath = ingest_weather_history_for_enabled_cities(
            start_date=start_date,
            end_date=end_date,
            config_path=cfg_path,
            events_path=evt_path,
            output_dir=out_dir,
        )
    except StationMappingValidationError as exc:
        console.print(f"[red]Weather history ingestion failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved weather history: {outpath} "
        f"(date range: {start_date} to {end_date})"
    )


@weather_app.command("normals")
def ingest_weather_normals(
    config_path: str = typer.Option("", help="Optional city config path override."),
    events_path: str = typer.Option(
        "",
        help="Optional staged Kalshi events parquet override for station validation.",
    ),
    output_dir: str = typer.Option("", help="Optional output directory override."),
) -> None:
    cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
    evt_path = Path(events_path) if events_path else None
    out_dir = Path(output_dir) if output_dir else None

    try:
        outpath, row_count, station_count = ingest_climate_normals_for_enabled_cities(
            config_path=cfg_path,
            events_path=evt_path,
            output_dir=out_dir,
        )
    except StationMappingValidationError as exc:
        console.print(f"[red]Climate normals ingestion failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved climate normals: {outpath} "
        f"(rows: {row_count}, stations: {station_count})"
    )


@kalshi_app.command("history")
def ingest_kalshi_history(
    start_date: str = typer.Option(..., help="Candle start date in YYYY-MM-DD format."),
    end_date: str = typer.Option(..., help="Candle end date in YYYY-MM-DD format."),
    interval: str = typer.Option("1h", help="Candle interval. Supported: 1m, 5m, 15m, 1h, 1d."),
    config_path: str = typer.Option("", help="Optional city config path override."),
    output_dir: str = typer.Option("", help="Optional output directory override."),
    resume: bool = typer.Option(False, help="Resume from saved Kalshi chunk progress instead of restarting."),
    max_retries: int = typer.Option(4, help="Maximum Kalshi retries for 429/transient failures."),
    initial_backoff_seconds: float = typer.Option(1.0, help="Initial Kalshi retry backoff in seconds."),
    max_backoff_seconds: float = typer.Option(30.0, help="Maximum Kalshi retry backoff in seconds."),
) -> None:
    cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
    out_dir = Path(output_dir) if output_dir else None

    try:
        markets_path, candles_path, details = ingest_kalshi_market_history_for_enabled_cities(
            start_date=start_date,
            end_date=end_date,
            interval=interval,
            config_path=cfg_path,
            output_dir=out_dir,
            resume=resume,
            max_retries=max_retries,
            initial_backoff_seconds=initial_backoff_seconds,
            max_backoff_seconds=max_backoff_seconds,
            return_details=True,
        )
    except Exception as exc:
        console.print(f"[red]Kalshi history ingestion failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved Kalshi history: markets={markets_path} candles={candles_path} "
        f"(date range: {start_date} to {end_date}, interval: {interval}, "
        f"retries={details['retry_summary']['total_retries']}, "
        f"resume_supported={details['resume_supported']})"
    )


@kalshi_app.command("capture-microstructure")
def capture_kalshi_microstructure_command(
    config_path: str = typer.Option("", help="Optional city config path override."),
    output_dir: str = typer.Option("", help="Optional staging output directory override."),
    status: str = typer.Option("open", help="Optional market status filter passed to Kalshi market listing."),
    include_orderbook: bool = typer.Option(
        True,
        "--include-orderbook/--skip-orderbook",
        help="Attempt to capture orderbook depth in addition to market top-of-book fields.",
    ),
    orderbook_depth: int = typer.Option(10, help="Requested orderbook depth per market."),
    iterations: int = typer.Option(1, help="Number of repeated snapshot captures to run."),
    poll_interval_seconds: float = typer.Option(
        None,
        help="Sleep interval between snapshot iterations when iterations > 1.",
    ),
) -> None:
    try:
        snapshots_path, levels_path, summary_path, summary = capture_kalshi_microstructure_for_enabled_cities(
            config_path=Path(config_path) if config_path else CONFIG_DIR / "cities.yml",
            output_dir=Path(output_dir) if output_dir else None,
            status=status or None,
            include_orderbook=include_orderbook,
            orderbook_depth=orderbook_depth,
            iterations=iterations,
            poll_interval_seconds=poll_interval_seconds,
            return_summary=True,
        )
    except Exception as exc:
        console.print(f"[red]Kalshi microstructure capture failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved Kalshi microstructure: snapshots={snapshots_path} levels={levels_path} summary={summary_path} "
        f"(snapshots captured: {summary['snapshot_rows_captured']}, "
        f"orderbook levels captured: {summary['orderbook_levels_captured']})"
    )


@data_app.command("build-staging")
def build_staging_command(
    dataset: list[str] | None = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Dataset(s) to build. Repeat for subsets. Defaults to all required baseline datasets.",
    ),
    start_date: str = typer.Option("", help="Required for weather_daily and Kalshi history datasets."),
    end_date: str = typer.Option("", help="Required for weather_daily and Kalshi history datasets."),
    weather_start_date: str = typer.Option("", help="Optional weather_daily-specific start date override."),
    weather_end_date: str = typer.Option("", help="Optional weather_daily-specific end date override."),
    interval: str = typer.Option("1h", help="Kalshi candle interval for kalshi_candles."),
    config_path: str = typer.Option("", help="Optional city config path override."),
    events_path: str = typer.Option("", help="Optional staged Kalshi events parquet override for station validation."),
    output_dir: str = typer.Option("", help="Optional staging output directory override."),
    summary_output: str = typer.Option("", help="Optional JSON validation-summary output override."),
    report_output: str = typer.Option("", help="Optional markdown bootstrap-report output override."),
    overwrite: bool = typer.Option(False, help="Allow overwriting existing staged parquet files."),
    resume: bool = typer.Option(False, help="Resume Kalshi chunked ingestion from prior partial progress."),
    max_retries: int = typer.Option(4, help="Maximum Kalshi retries for 429/transient failures."),
    initial_backoff_seconds: float = typer.Option(1.0, help="Initial Kalshi retry backoff in seconds."),
    max_backoff_seconds: float = typer.Option(30.0, help="Maximum Kalshi retry backoff in seconds."),
) -> None:
    try:
        summary = build_staging_datasets(
            datasets=tuple(dataset) if dataset else ("weather_daily", "weather_normals_daily", "kalshi_markets", "kalshi_candles"),
            config_path=Path(config_path) if config_path else CONFIG_DIR / "cities.yml",
            staging_dir=Path(output_dir) if output_dir else None,
            summary_output_path=Path(summary_output) if summary_output else None,
            report_output_path=Path(report_output) if report_output else None,
            start_date=start_date or None,
            end_date=end_date or None,
            weather_start_date=weather_start_date or None,
            weather_end_date=weather_end_date or None,
            interval=interval,
            overwrite=overwrite,
            resume=resume,
            max_retries=max_retries,
            initial_backoff_seconds=initial_backoff_seconds,
            max_backoff_seconds=max_backoff_seconds,
            events_path=Path(events_path) if events_path else None,
        )
    except Exception as exc:
        console.print(f"[red]Staging build failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    if not summary["success"]:
        details = "\n".join(summary.get("errors", []))
        console.print(
            f"[red]Staging build incomplete[/red]\n"
            f"validation={summary['validation_summary_path']}\n"
            f"report={summary['bootstrap_report_path']}\n"
            f"{details}\n"
            f"{summary['recommendation']}"
        )
        raise typer.Exit(code=1)

    console.print(
        f"Built staging datasets successfully: validation={summary['validation_summary_path']} "
        f"report={summary['bootstrap_report_path']}"
    )


@data_app.command("validate-staging")
def validate_staging_command(
    dataset: list[str] | None = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Dataset(s) to validate. Repeat for subsets. Defaults to all required baseline datasets.",
    ),
    config_path: str = typer.Option("", help="Optional city config path override."),
    staging_dir: str = typer.Option("", help="Optional staging directory override."),
    summary_output: str = typer.Option("", help="Optional JSON validation-summary output override."),
) -> None:
    summary = validate_staging_datasets(
        datasets=tuple(dataset) if dataset else ("weather_daily", "weather_normals_daily", "kalshi_markets", "kalshi_candles"),
        staging_dir=Path(staging_dir) if staging_dir else None,
        config_path=Path(config_path) if config_path else CONFIG_DIR / "cities.yml",
        summary_output_path=Path(summary_output) if summary_output else None,
    )

    status = "ready" if summary["ready"] else "not ready"
    console.print(
        f"Staging validation: {status} "
        f"(station mapping ready: {summary['station_mapping']['ready']}, "
        f"missing: {len(summary['missing_datasets'])}, invalid: {len(summary['invalid_datasets'])}) "
        f"summary={summary['summary_output_path']}"
    )
    if not summary["ready"]:
        raise typer.Exit(code=1)


@mart_app.command("backtest-dataset")
def build_backtest_dataset_command(
    decision_time_local: str = typer.Option(..., help="Local decision time in HH:MM format."),
    config_path: str = typer.Option("", help="Optional city config path override."),
    weather_path: str = typer.Option("", help="Optional staged weather_daily parquet override."),
    normals_path: str = typer.Option("", help="Optional staged weather_normals_daily parquet override."),
    markets_path: str = typer.Option("", help="Optional staged kalshi_markets parquet override."),
    candles_path: str = typer.Option("", help="Optional staged kalshi_candles parquet override."),
    output_dir: str = typer.Option("", help="Optional marts output directory override."),
) -> None:
    cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
    out_dir = Path(output_dir) if output_dir else None

    try:
        outpath, stats = build_backtest_dataset(
            decision_time_local=decision_time_local,
            config_path=cfg_path,
            weather_path=Path(weather_path) if weather_path else None,
            normals_path=Path(normals_path) if normals_path else None,
            markets_path=Path(markets_path) if markets_path else None,
            candles_path=Path(candles_path) if candles_path else None,
            output_dir=out_dir,
        )
    except BacktestDatasetBuildError as exc:
        console.print(f"[red]Backtest dataset build failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved backtest dataset: {outpath} "
        f"(rows: {stats['rows_written']}, cities: {stats['cities_covered']}, "
        f"decision time local: {stats['decision_time_local']})"
    )


@model_app.command("climatology-baseline")
def run_climatology_baseline(
    backtest_dataset_path: str = typer.Option("", help="Optional backtest_dataset parquet override."),
    history_path: str = typer.Option("", help="Optional staged weather_daily parquet override."),
    output_dir: str = typer.Option("", help="Optional scored marts output directory override."),
    day_window: int = typer.Option(0, help="Day-of-year half-window around month_day for the climatology sample."),
    min_lookback_samples: int = typer.Option(1, help="Minimum required historical samples to score a row."),
) -> None:
    out_dir = Path(output_dir) if output_dir else None

    try:
        outpath, summary = score_climatology_baseline(
            backtest_dataset_path=Path(backtest_dataset_path) if backtest_dataset_path else None,
            history_path=Path(history_path) if history_path else None,
            output_dir=out_dir,
            day_window=day_window,
            min_lookback_samples=min_lookback_samples,
        )
    except ClimatologyModelError as exc:
        console.print(f"[red]Climatology baseline scoring failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved climatology baseline: {outpath} "
        f"(rows scored: {summary['rows_scored']}, "
        f"avg lookback: {summary['average_lookback_sample_size']}, "
        f"Brier: {summary['brier_score']}, "
        f"avg edge yes: {summary['average_edge_yes']})"
    )


@backtest_app.command("evaluate-climatology")
def run_climatology_backtest(
    scored_dataset_path: str = typer.Option("", help="Optional scored climatology parquet override."),
    output_dir: str = typer.Option("", help="Optional backtest output directory override."),
    min_edge: float = typer.Option(0.0, help="Minimum model edge required to take a trade."),
    min_samples: int = typer.Option(1, help="Minimum lookback sample size required."),
    min_price: float = typer.Option(0.0, help="Minimum entry price in cents."),
    max_price: float = typer.Option(100.0, help="Maximum entry price in cents."),
    contracts: int = typer.Option(1, help="Contracts per selected trade."),
    fee_per_contract: float = typer.Option(0.0, help="Flat fee per contract in dollars."),
    allow_no: bool = typer.Option(False, help="Also allow NO-side trades when the NO edge qualifies."),
) -> None:
    out_dir = Path(output_dir) if output_dir else None

    try:
        trades_path, summary_path, summary = evaluate_climatology_strategy(
            scored_dataset_path=Path(scored_dataset_path) if scored_dataset_path else None,
            output_dir=out_dir,
            min_edge=min_edge,
            min_samples=min_samples,
            min_price=min_price,
            max_price=max_price,
            contracts=contracts,
            fee_per_contract=fee_per_contract,
            allow_no=allow_no,
        )
    except ClimatologyEvaluationError as exc:
        console.print(f"[red]Climatology evaluation failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved climatology evaluation: trades={trades_path} summary={summary_path} "
        f"(trades: {summary['trades_taken']}, hit rate: {summary['hit_rate']}, "
        f"avg pnl/trade: {summary['average_pnl_per_trade']}, total net pnl: {summary['total_net_pnl']})"
    )


@backtest_app.command("evaluate-climatology-executable")
def run_climatology_executable_backtest(
    input: str = typer.Option("", help="Optional scored climatology parquet override."),
    output: str = typer.Option("", help="Optional executable trades parquet output override."),
    summary_output: str = typer.Option("", help="Optional executable summary JSON output override."),
    min_edge: float = typer.Option(0.0, help="Minimum executable edge required to take a trade."),
    min_samples: int = typer.Option(1, help="Minimum lookback sample size required."),
    min_price: float = typer.Option(0.0, help="Minimum executable entry price in cents."),
    max_price: float = typer.Option(100.0, help="Maximum executable entry price in cents."),
    allow_no: bool = typer.Option(False, help="Also allow NO-side executable trades."),
    contracts: int = typer.Option(1, help="Contracts per selected trade."),
    fee_per_contract: float = typer.Option(0.0, help="Flat fee per contract in dollars."),
    max_spread: float = typer.Option(
        None,
        help="Optional maximum bid/ask spread in cents for the selected side.",
    ),
) -> None:
    try:
        trades_path, summary_path, summary = evaluate_climatology_executable_strategy(
            scored_dataset_path=Path(input) if input else None,
            output_path=Path(output) if output else None,
            summary_output_path=Path(summary_output) if summary_output else None,
            min_edge=min_edge,
            min_samples=min_samples,
            min_price=min_price,
            max_price=max_price,
            contracts=contracts,
            fee_per_contract=fee_per_contract,
            allow_no=allow_no,
            max_spread=max_spread,
        )
    except ClimatologyExecutableEvaluationError as exc:
        console.print(f"[red]Executable climatology evaluation failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved executable climatology evaluation: trades={trades_path} summary={summary_path} "
        f"(trades: {summary['trades_taken']}, yes trades: {summary['yes_trades_taken']}, "
        f"no trades: {summary['no_trades_taken']}, total net pnl: {summary['total_net_pnl']})"
    )


@backtest_app.command("compare-climatology-pricing")
def run_climatology_pricing_comparison(
    input: str = typer.Option("", help="Optional scored climatology parquet override."),
    output_dir: str = typer.Option("", help="Optional comparison output directory override."),
    min_edge: float = typer.Option(0.0, help="Minimum edge required to take a trade."),
    min_samples: int = typer.Option(1, help="Minimum lookback sample size required."),
    min_price: float = typer.Option(0.0, help="Minimum entry price in cents."),
    max_price: float = typer.Option(100.0, help="Maximum entry price in cents."),
    allow_no: bool = typer.Option(False, help="Also allow NO-side trades when supported by the mode."),
    contracts: int = typer.Option(1, help="Contracts per selected trade."),
    fee_per_contract: float = typer.Option(0.0, help="Flat fee per contract in dollars."),
    max_spread: float = typer.Option(
        None,
        help="Optional maximum bid/ask spread in cents for executable candle-proxy mode.",
    ),
) -> None:
    try:
        json_path, csv_path, comparison = compare_climatology_pricing_modes(
            scored_dataset_path=Path(input) if input else None,
            output_dir=Path(output_dir) if output_dir else None,
            min_edge=min_edge,
            min_samples=min_samples,
            min_price=min_price,
            max_price=max_price,
            contracts=contracts,
            fee_per_contract=fee_per_contract,
            allow_no=allow_no,
            max_spread=max_spread,
        )
    except (ClimatologyPricingComparisonError, ClimatologyEvaluationError, ClimatologyExecutableEvaluationError) as exc:
        console.print(f"[red]Climatology pricing comparison failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    delta = comparison["delta_executable_minus_decision_price"]
    console.print(
        f"Saved pricing comparison: json={json_path} csv={csv_path} "
        f"(mode count: {len(comparison['modes'])}, "
        f"trade delta exec-vs-decision: {delta.get('trades_taken_delta', 0)}, "
        f"net pnl delta exec-vs-decision: {delta.get('total_net_pnl_delta', 0.0)})"
    )


@backtest_app.command("walkforward-climatology")
def run_walkforward_climatology_command(
    input: str = typer.Option("", help="Optional scored climatology parquet override."),
    output_dir: str = typer.Option("", help="Optional walk-forward output directory override."),
    pricing_mode: str = typer.Option("both", help="Pricing mode: decision_price, candle_proxy, or both."),
    window_profile: str = typer.Option(
        "custom",
        help="Walk-forward window profile: custom, standard, research_short, or auto.",
    ),
    train_window: int = typer.Option(60, help="Training window size in ordered unique event dates."),
    validation_window: int = typer.Option(30, help="Validation window size in ordered unique event dates."),
    test_window: int = typer.Option(30, help="Test window size in ordered unique event dates."),
    step_window: int = typer.Option(0, help="Step size in unique event dates. Defaults to test_window."),
    min_trades_for_selection: int = typer.Option(1, help="Minimum validation trades required to select a threshold set."),
    min_edge_grid: str = typer.Option("0.0,0.02,0.05", help="Comma-separated min_edge grid."),
    min_samples_grid: str = typer.Option("1,5", help="Comma-separated min_samples grid."),
    min_price_grid: str = typer.Option("0", help="Comma-separated min_price grid."),
    max_price_grid: str = typer.Option("100", help="Comma-separated max_price grid."),
    max_spread_grid: str = typer.Option("none,5", help="Comma-separated max_spread grid for candle_proxy mode."),
    allow_no_grid: str = typer.Option("false", help="Comma-separated allow_no grid values (true/false)."),
    expanding: bool = typer.Option(True, "--expanding/--rolling", help="Use an expanding training window."),
    selection_metric: str = typer.Option(
        "total_net_pnl",
        help="Validation objective: total_net_pnl or average_net_pnl_per_trade.",
    ),
) -> None:
    try:
        results_path, summary_path, diagnostics_path, summary = run_walkforward_climatology(
            scored_dataset_path=Path(input) if input else None,
            output_dir=Path(output_dir) if output_dir else None,
            pricing_mode=pricing_mode,
            window_profile=window_profile,
            train_window=train_window,
            validation_window=validation_window,
            test_window=test_window,
            step_window=step_window or None,
            min_trades_for_selection=min_trades_for_selection,
            min_edge_grid=_parse_float_grid(min_edge_grid),
            min_samples_grid=_parse_int_grid(min_samples_grid),
            min_price_grid=_parse_float_grid(min_price_grid),
            max_price_grid=_parse_float_grid(max_price_grid),
            max_spread_grid=_parse_optional_float_grid(max_spread_grid),
            allow_no_grid=_parse_bool_grid(allow_no_grid),
            expanding=expanding,
            selection_metric=selection_metric,
        )
    except WalkforwardClimatologyError as exc:
        console.print(f"[red]Walk-forward climatology evaluation failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved walk-forward climatology: results={results_path} summary={summary_path} diagnostics={diagnostics_path} "
        f"(folds: {summary['fold_count']}, pricing mode: {summary['pricing_mode']})"
    )


@research_app.command("run-climatology-baseline")
def run_climatology_baseline_research_command(
    decision_time_local: str = typer.Option("10:00", help="Local decision time in HH:MM format."),
    output_dir: str = typer.Option("", help="Optional research run output directory override."),
    overwrite: bool = typer.Option(False, help="Allow writing into an existing non-empty output directory."),
    pricing_mode: str = typer.Option("both", help="Pricing mode: decision_price, candle_proxy, or both."),
    config_path: str = typer.Option("", help="Optional city config path override."),
    weather_path: str = typer.Option("", help="Optional staged weather_daily parquet override."),
    normals_path: str = typer.Option("", help="Optional staged weather_normals_daily parquet override."),
    markets_path: str = typer.Option("", help="Optional staged kalshi_markets parquet override."),
    candles_path: str = typer.Option("", help="Optional staged kalshi_candles parquet override."),
    history_path: str = typer.Option("", help="Optional climatology history parquet override."),
    day_window: int = typer.Option(1, help="Day-of-year half-window around month_day for the climatology sample."),
    min_lookback_samples: int = typer.Option(30, help="Minimum required historical samples to score a row."),
    min_edge: float = typer.Option(0.0, help="Minimum one-shot trade edge."),
    min_samples: int = typer.Option(1, help="Minimum one-shot lookback sample size."),
    min_price: float = typer.Option(0.0, help="Minimum one-shot entry price in cents."),
    max_price: float = typer.Option(100.0, help="Maximum one-shot entry price in cents."),
    allow_no: bool = typer.Option(False, help="Also allow NO-side trades when supported."),
    contracts: int = typer.Option(1, help="Contracts per selected trade."),
    fee_per_contract: float = typer.Option(0.0, help="Flat fee per contract in dollars."),
    max_spread: float = typer.Option(None, help="Optional max executable spread for candle_proxy mode."),
    walkforward_profile: str = typer.Option(
        "custom",
        help="Walk-forward window profile: custom, standard, research_short, or auto.",
    ),
    train_window: int = typer.Option(60, help="Walk-forward train window in ordered unique event dates."),
    validation_window: int = typer.Option(30, help="Walk-forward validation window in ordered unique event dates."),
    test_window: int = typer.Option(30, help="Walk-forward test window in ordered unique event dates."),
    step_window: int = typer.Option(0, help="Walk-forward step size. Defaults to test_window."),
    min_trades_for_selection: int = typer.Option(1, help="Minimum validation trades required to select thresholds."),
    min_edge_grid: str = typer.Option("0.0,0.02,0.05", help="Comma-separated min_edge grid."),
    min_samples_grid: str = typer.Option("1,5", help="Comma-separated min_samples grid."),
    min_price_grid: str = typer.Option("0", help="Comma-separated min_price grid."),
    max_price_grid: str = typer.Option("100", help="Comma-separated max_price grid."),
    max_spread_grid: str = typer.Option("none,5", help="Comma-separated max_spread grid for candle_proxy mode."),
    allow_no_grid: str = typer.Option("false", help="Comma-separated allow_no grid values."),
    expanding: bool = typer.Option(True, "--expanding/--rolling", help="Use an expanding training window."),
    validate_staging: bool = typer.Option(
        False,
        "--validate-staging/--no-validate-staging",
        help="Validate required staged inputs before running the baseline bundle.",
    ),
    fail_fast_on_unready_staging: bool = typer.Option(
        False,
        "--fail-fast-on-unready-staging/--allow-unready-staging",
        help="Stop before research execution if staged inputs are missing or malformed.",
    ),
    build_staging_first: bool = typer.Option(
        False,
        help="Explicitly build the required staged datasets before running research.",
    ),
    staging_start_date: str = typer.Option(
        "",
        help="Required with --build-staging-first for weather_daily and Kalshi history builds.",
    ),
    staging_end_date: str = typer.Option(
        "",
        help="Required with --build-staging-first for weather_daily and Kalshi history builds.",
    ),
    staging_weather_start_date: str = typer.Option(
        "",
        help="Optional weather_daily-specific start date when --build-staging-first is enabled. Defaults to 10 years before staging_end_date.",
    ),
    staging_weather_end_date: str = typer.Option(
        "",
        help="Optional weather_daily-specific end date when --build-staging-first is enabled. Defaults to staging_end_date.",
    ),
    staging_interval: str = typer.Option(
        "1h",
        help="Kalshi candle interval to use when --build-staging-first is enabled.",
    ),
    selection_metric: str = typer.Option(
        "total_net_pnl",
        help="Walk-forward validation objective: total_net_pnl or average_net_pnl_per_trade.",
    ),
) -> None:
    try:
        run_dir, manifest_path, report_json_path, report_markdown_path, manifest = run_climatology_baseline_research(
            decision_time_local=decision_time_local,
            output_dir=Path(output_dir) if output_dir else None,
            overwrite=overwrite,
            pricing_mode=pricing_mode,
            config_path=Path(config_path) if config_path else None,
            weather_path=Path(weather_path) if weather_path else None,
            normals_path=Path(normals_path) if normals_path else None,
            markets_path=Path(markets_path) if markets_path else None,
            candles_path=Path(candles_path) if candles_path else None,
            history_path=Path(history_path) if history_path else None,
            day_window=day_window,
            min_lookback_samples=min_lookback_samples,
            min_edge=min_edge,
            min_samples=min_samples,
            min_price=min_price,
            max_price=max_price,
            allow_no=allow_no,
            contracts=contracts,
            fee_per_contract=fee_per_contract,
            max_spread=max_spread,
            walkforward_profile=walkforward_profile,
            train_window=train_window,
            validation_window=validation_window,
            test_window=test_window,
            step_window=step_window or None,
            min_trades_for_selection=min_trades_for_selection,
            min_edge_grid=_parse_float_grid(min_edge_grid),
            min_samples_grid=_parse_int_grid(min_samples_grid),
            min_price_grid=_parse_float_grid(min_price_grid),
            max_price_grid=_parse_float_grid(max_price_grid),
            max_spread_grid=_parse_optional_float_grid(max_spread_grid),
            allow_no_grid=_parse_bool_grid(allow_no_grid),
            expanding=expanding,
            validate_staging_before_run=validate_staging,
            fail_fast_on_unready_staging=fail_fast_on_unready_staging,
            build_staging_first=build_staging_first,
            staging_start_date=staging_start_date or None,
            staging_end_date=staging_end_date or None,
            staging_weather_start_date=staging_weather_start_date or None,
            staging_weather_end_date=staging_weather_end_date or None,
            staging_interval=staging_interval,
            selection_metric=selection_metric,
        )
    except ClimatologyResearchRunError as exc:
        console.print(f"[red]Climatology baseline research run failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved climatology baseline research bundle: run_dir={run_dir} manifest={manifest_path} "
        f"report_json={report_json_path} report_md={report_markdown_path} "
        f"(skipped steps: {len(manifest['skipped_steps'])})"
    )


@research_app.command("check-baseline-readiness")
def check_baseline_readiness_command(
    config_path: str = typer.Option("", help="Optional city config path override."),
    staging_dir: str = typer.Option("", help="Optional staging directory override."),
    summary_output: str = typer.Option("", help="Optional JSON summary output override."),
) -> None:
    readiness = check_climatology_baseline_readiness(
        staging_dir=Path(staging_dir) if staging_dir else None,
        config_path=Path(config_path) if config_path else CONFIG_DIR / "cities.yml",
        summary_output_path=Path(summary_output) if summary_output else None,
    )

    status = "ready" if readiness["ready"] else "not ready"
    console.print(
        f"Baseline readiness: {status} "
        f"(station mapping ready: {readiness['station_mapping']['ready']}, "
        f"missing: {len(readiness['missing_datasets'])}, invalid: {len(readiness['invalid_datasets'])}) "
        f"summary={readiness['validation_summary_path']}\n"
        f"{readiness['recommendation']}"
    )
    if not readiness["ready"]:
        raise typer.Exit(code=1)


@research_app.command("stress-test-climatology-frictions")
def stress_test_climatology_frictions_command(
    run_dir: str = typer.Option(
        "",
        help="Existing climatology baseline run directory containing backtest/scored artifacts.",
    ),
    output_dir: str = typer.Option("", help="Optional output directory for stress-test artifacts."),
    walkforward_profile: str = typer.Option(
        "research_short",
        help="Walk-forward profile to use for executable friction stress testing.",
    ),
    selection_metric: str = typer.Option(
        "total_net_pnl",
        help="Walk-forward validation objective: total_net_pnl or average_net_pnl_per_trade.",
    ),
    min_trades_for_selection: int = typer.Option(
        1,
        help="Minimum validation trades required to select thresholds in each scenario.",
    ),
) -> None:
    if not run_dir:
        console.print("[red]A run directory is required.[/red]")
        raise typer.Exit(code=1)

    try:
        json_path, csv_path, markdown_path, report = stress_test_climatology_frictions(
            run_dir=Path(run_dir),
            output_dir=Path(output_dir) if output_dir else None,
            walkforward_profile=walkforward_profile,
            selection_metric=selection_metric,
            min_trades_for_selection=min_trades_for_selection,
        )
    except ClimatologyFrictionStressTestError as exc:
        console.print(f"[red]Climatology friction stress test failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved climatology friction stress test: json={json_path} csv={csv_path} markdown={markdown_path} "
        f"(scenario count: {len(report['scenario_reports'])})"
    )


@research_app.command("paper-monitor-climatology")
def paper_monitor_climatology_command(
    config_path: str = typer.Option("", help="Optional city config path override."),
    paper_config_path: str = typer.Option("", help="Optional paper-trading config path override."),
    history_path: str = typer.Option("", help="Optional settlement-aligned weather history parquet override."),
    output_root: str = typer.Option("", help="Optional paper-trading output root override."),
    microstructure_dir: str = typer.Option("", help="Optional live microstructure capture directory override."),
    iterations: int = typer.Option(0, help="Repeated scan count. Defaults to the paper config."),
    poll_interval_seconds: float = typer.Option(
        None,
        help="Sleep interval between scans when iterations > 1. Defaults to the paper config.",
    ),
    status: str = typer.Option("", help="Optional market status filter override."),
    include_orderbook: bool = typer.Option(
        True,
        "--include-orderbook/--skip-orderbook",
        help="Capture orderbook depth when available for quote context.",
    ),
    orderbook_depth: int = typer.Option(0, help="Optional orderbook depth override."),
    min_net_edge: float = typer.Option(None, help="Optional paper-trade minimum net edge override."),
    max_spread_cents: float = typer.Option(None, help="Optional maximum YES spread override."),
    max_entry_price_cents: float = typer.Option(None, help="Optional gate max entry price override."),
) -> None:
    try:
        evaluations_path, trades_path, summary_path, report_path, summary = run_paper_climatology_monitor(
            config_path=Path(config_path) if config_path else CONFIG_DIR / "cities.yml",
            paper_config_path=Path(paper_config_path) if paper_config_path else DEFAULT_PAPER_CONFIG_PATH,
            history_path=Path(history_path) if history_path else None,
            output_root=Path(output_root) if output_root else None,
            microstructure_dir=Path(microstructure_dir) if microstructure_dir else None,
            iterations=iterations or None,
            poll_interval_seconds=poll_interval_seconds,
            status=status or None,
            include_orderbook=include_orderbook,
            orderbook_depth=orderbook_depth or None,
            min_net_edge=min_net_edge,
            max_spread_cents=max_spread_cents,
            max_entry_price_cents=max_entry_price_cents,
        )
    except PaperClimatologyMonitorError as exc:
        console.print(f"[red]Paper-only climatology monitor failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        console.print(f"[red]Paper-only climatology monitor failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved paper-only climatology monitor: evaluations={evaluations_path} trades={trades_path} "
        f"summary={summary_path} report={report_path} "
        f"(evaluations: {summary['totals']['evaluations']}, paper trades: {summary['totals']['paper_trades']})"
    )


@research_app.command("reconcile-paper-climatology")
def reconcile_paper_climatology_command(
    trade_date: str = typer.Option(
        "",
        help="Specific paper-trading date to reconcile in YYYY-MM-DD format. Defaults to the latest available date.",
    ),
    paper_output_root: str = typer.Option(
        "",
        help="Optional paper-trading output root override. Defaults to data/marts/paper_trading.",
    ),
    history_path: str = typer.Option(
        "",
        help="Optional settlement-aligned weather history parquet override.",
    ),
) -> None:
    try:
        (
            reconciled_path,
            summary_path,
            report_path,
            cumulative_scoreboard_path,
            cumulative_summary_path,
            cumulative_report_path,
            payload,
        ) = reconcile_paper_climatology(
            trade_date=trade_date or None,
            paper_output_root=Path(paper_output_root) if paper_output_root else None,
            history_path=Path(history_path) if history_path else None,
        )
    except PaperClimatologyReconciliationError as exc:
        console.print(f"[red]Paper climatology reconciliation failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    daily = payload["daily_summary"]["totals"]
    console.print(
        f"Saved paper climatology reconciliation: reconciled={reconciled_path} summary={summary_path} "
        f"report={report_path} cumulative_scoreboard={cumulative_scoreboard_path} "
        f"cumulative_summary={cumulative_summary_path} cumulative_report={cumulative_report_path} "
        f"(trade_date: {payload['trade_date']}, resolved: {daily['resolved_trades']}, "
        f"unresolved: {daily['unresolved_trades']}, net_pnl: {daily['realized_net_pnl_dollars']})"
    )


@research_app.command("time-of-day-sensitivity-climatology")
def time_of_day_sensitivity_climatology_command(
    times: str = typer.Option(
        "08:00,09:00,10:00,11:00,12:00,13:00,14:00",
        help="Comma-separated local decision times to test.",
    ),
    output_dir: str = typer.Option("", help="Optional output directory for the sweep artifacts."),
    config_path: str = typer.Option("", help="Optional city config path override."),
    weather_path: str = typer.Option("", help="Optional staged weather_daily parquet override."),
    normals_path: str = typer.Option("", help="Optional staged weather_normals_daily parquet override."),
    markets_path: str = typer.Option("", help="Optional staged kalshi_markets parquet override."),
    candles_path: str = typer.Option("", help="Optional staged kalshi_candles parquet override."),
    history_path: str = typer.Option("", help="Optional climatology history parquet override."),
    walkforward_profile: str = typer.Option(
        "research_short",
        help="Walk-forward profile to use for all tested hours.",
    ),
    selection_metric: str = typer.Option(
        "total_net_pnl",
        help="Walk-forward validation objective: total_net_pnl or average_net_pnl_per_trade.",
    ),
    min_trades_for_selection: int = typer.Option(
        1,
        help="Minimum validation trades required to select thresholds for each hour.",
    ),
) -> None:
    try:
        parsed_times = tuple(chunk.strip() for chunk in times.split(",") if chunk.strip())
        json_path, csv_path, fold_csv_path, markdown_path, report = run_time_of_day_sensitivity_study(
            decision_times_local=parsed_times,
            output_dir=Path(output_dir) if output_dir else None,
            config_path=Path(config_path) if config_path else None,
            weather_path=Path(weather_path) if weather_path else None,
            normals_path=Path(normals_path) if normals_path else None,
            markets_path=Path(markets_path) if markets_path else None,
            candles_path=Path(candles_path) if candles_path else None,
            history_path=Path(history_path) if history_path else None,
            walkforward_profile=walkforward_profile,
            selection_metric=selection_metric,
            min_trades_for_selection=min_trades_for_selection,
        )
    except TimeOfDaySensitivityError as exc:
        console.print(f"[red]Time-of-day sensitivity study failed[/red]\n{exc}")
        raise typer.Exit(code=1) from exc

    console.print(
        f"Saved time-of-day sensitivity study: json={json_path} csv={csv_path} fold_csv={fold_csv_path} "
        f"report={markdown_path} (times tested: {len(report['times_tested'])})"
    )


def _parse_float_grid(raw: str) -> tuple[float, ...]:
    values = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    return tuple(float(value) for value in values)


def _parse_int_grid(raw: str) -> tuple[int, ...]:
    values = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    return tuple(int(value) for value in values)


def _parse_optional_float_grid(raw: str) -> tuple[float | None, ...]:
    values = [chunk.strip().lower() for chunk in raw.split(",") if chunk.strip()]
    parsed: list[float | None] = []
    for value in values:
        if value in {"none", "null"}:
            parsed.append(None)
        else:
            parsed.append(float(value))
    return tuple(parsed)


def _parse_bool_grid(raw: str) -> tuple[bool, ...]:
    values = [chunk.strip().lower() for chunk in raw.split(",") if chunk.strip()]
    parsed: list[bool] = []
    for value in values:
        if value in {"true", "1", "yes"}:
            parsed.append(True)
        elif value in {"false", "0", "no"}:
            parsed.append(False)
        else:
            raise WalkforwardClimatologyError(f"Unsupported boolean grid value {value!r}.")
    return tuple(parsed)


if __name__ == "__main__":
    app()
