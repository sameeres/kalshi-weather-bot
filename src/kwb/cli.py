from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from kwb.ingestion.kalshi_events import ingest_enabled_city_events, ingest_events_for_series
from kwb.ingestion.kalshi_market_history import ingest_kalshi_market_history_for_enabled_cities
from kwb.ingestion.weather_history import ingest_weather_history_for_enabled_cities
from kwb.mapping.station_candidates import build_station_mapping_report, write_station_mapping_report
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
app.add_typer(cities_app, name="cities")
app.add_typer(ingest_app, name="ingest")
app.add_typer(station_app, name="station")
app.add_typer(weather_app, name="weather")
app.add_typer(kalshi_app, name="kalshi")
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


@kalshi_app.command("history")
def ingest_kalshi_history(
    start_date: str = typer.Option(..., help="Candle start date in YYYY-MM-DD format."),
    end_date: str = typer.Option(..., help="Candle end date in YYYY-MM-DD format."),
    interval: str = typer.Option("1h", help="Candle interval. Supported: 1m, 5m, 15m, 1h, 1d."),
    config_path: str = typer.Option("", help="Optional city config path override."),
    output_dir: str = typer.Option("", help="Optional output directory override."),
) -> None:
    cfg_path = Path(config_path) if config_path else CONFIG_DIR / "cities.yml"
    out_dir = Path(output_dir) if output_dir else None

    markets_path, candles_path = ingest_kalshi_market_history_for_enabled_cities(
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        config_path=cfg_path,
        output_dir=out_dir,
    )

    console.print(
        f"Saved Kalshi history: markets={markets_path} candles={candles_path} "
        f"(date range: {start_date} to {end_date}, interval: {interval})"
    )


if __name__ == "__main__":
    app()
