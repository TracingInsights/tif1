"""CLI tool for tif1."""

import logging
from typing import cast

import pandas as pd
import typer
from rich import print as rprint
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

import tif1

app = typer.Typer(help="tif1 - Fast F1 data access CLI")
console = Console()


@app.command()
def events(
    year: int = typer.Argument(..., help="Year (2018-current)"),
) -> None:
    """List all events for a year."""
    events_schedule = tif1.get_events(year)

    table = Table(title=f"F1 Events {year}")
    table.add_column("#", style="cyan")
    table.add_column("Event", style="green")

    for idx, event_name in enumerate(events_schedule["EventName"].tolist(), 1):
        table.add_row(str(idx), event_name)

    console.print(table)
    console.print(f"\n[bold]Total:[/bold] {len(events_schedule)} events")


@app.command()
def sessions(
    year: int = typer.Argument(..., help="Year (2018-current)"),
    event: str = typer.Argument(..., help="Event name"),
) -> None:
    """List all sessions for an event."""
    sessions_list = tif1.get_sessions(year, event)

    table = Table(title=f"{event} {year}")
    table.add_column("#", style="cyan")
    table.add_column("Session", style="green")

    for idx, session in enumerate(sessions_list, 1):
        table.add_row(str(idx), session)

    console.print(table)


@app.command()
def drivers(
    year: int = typer.Argument(..., help="Year (2018-current)"),
    event: str = typer.Argument(..., help="Event name"),
    session: str = typer.Argument(..., help="Session name"),
) -> None:
    """List all drivers in a session."""
    with Progress() as progress:
        task = progress.add_task("[cyan]Loading session...", total=1)
        sess = tif1.get_session(year, event, session)
        progress.update(task, advance=1)

    table = Table(title=f"{event} {year} - {session}")
    table.add_column("Driver", style="cyan")
    table.add_column("Team", style="green")

    for driver_info in sess.drivers:
        table.add_row(driver_info["driver"], driver_info["team"])

    console.print(table)
    console.print(f"\n[bold]Total:[/bold] {len(sess.drivers)} drivers")


@app.command()
def fastest(
    year: int = typer.Argument(..., help="Year (2018-current)"),
    event: str = typer.Argument(..., help="Event name"),
    session: str = typer.Argument(..., help="Session name"),
    driver: str | None = typer.Option(None, "--driver", "-d", help="Specific driver"),
) -> None:
    """Show fastest laps."""
    with Progress() as progress:
        task = progress.add_task("[cyan]Loading session...", total=1)
        sess = tif1.get_session(year, event, session)
        progress.update(task, advance=1)

    if driver:
        drv = sess.get_driver(driver)
        fastest_lap = drv.get_fastest_lap()
        time_val = None
        if sess.lib == "pandas":
            fastest_lap_pd = cast(pd.DataFrame, fastest_lap)
            if not fastest_lap_pd.empty:
                time_col = "LapTime" if "LapTime" in fastest_lap_pd.columns else "time"
                time_val = fastest_lap_pd[time_col].iloc[0]
        else:
            fastest_lap_pl = fastest_lap
            if not fastest_lap_pl.is_empty():
                time_col = "LapTime" if "LapTime" in fastest_lap_pl.columns else "time"
                time_val = fastest_lap_pl[time_col][0]

        if time_val is not None:
            rprint(f"[bold]{driver}[/bold] fastest lap: [green]{time_val:.3f}s[/green]")
        else:
            rprint(f"[red]No valid laps for {driver}[/red]")
    else:
        fastest_laps = sess.get_fastest_laps(by_driver=True)

        table = Table(title=f"Fastest Laps - {event} {year} - {session}")
        table.add_column("Pos", style="cyan")
        table.add_column("Driver", style="yellow")
        table.add_column("Team", style="green")
        table.add_column("Time", style="magenta")

        # Sort by time
        if sess.lib == "pandas":
            fastest_laps_pd = cast(pd.DataFrame, fastest_laps)
            time_col = "LapTime" if "LapTime" in fastest_laps_pd.columns else "time"
            fastest_laps_pd = fastest_laps_pd.sort_values(time_col)
            for idx, row in enumerate(fastest_laps_pd.itertuples(), 1):
                time_val = getattr(row, time_col)
                table.add_row(str(idx), str(row.Driver), str(row.Team), f"{time_val:.3f}s")
        else:
            fastest_laps_pl = fastest_laps
            time_col = "LapTime" if "LapTime" in fastest_laps_pl.columns else "time"
            fastest_laps_pl = fastest_laps_pl.sort(time_col)
            for idx, row in enumerate(fastest_laps_pl.iter_rows(named=True), 1):
                table.add_row(str(idx), row["Driver"], row["Team"], f"{row[time_col]:.3f}s")

        console.print(table)


@app.command()
def cache_info() -> None:
    """Show cache information."""
    cache = tif1.get_cache()
    rprint(f"[bold]Cache location:[/bold] {cache.cache_dir}")

    cache_files = [p for p in cache.cache_dir.glob("cache.sqlite*") if p.is_file()]
    if not cache_files:
        cache_files = [p for p in cache.cache_dir.iterdir() if p.is_file()]
    rprint(f"[bold]Cache files:[/bold] {len(cache_files)}")

    total_size = sum(f.stat().st_size for f in cache_files)
    rprint(f"[bold]Total size:[/bold] {total_size / 1024 / 1024:.2f} MB")


@app.command()
def cache_clear(
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Clear cache."""
    if not confirm:
        confirm = typer.confirm("Are you sure you want to clear the cache?")

    if confirm:
        cache = tif1.get_cache()
        cache.clear()
        rprint("[green]Cache cleared successfully![/green]")
    else:
        rprint("[yellow]Cache clear cancelled[/yellow]")


@app.command()
def version() -> None:
    """Show tif1 version."""
    rprint(f"[bold]tif1[/bold] version [green]{tif1.__version__}[/green]")


@app.command()
def debug(
    year: int = typer.Argument(..., help="Year (2018-current)"),
    event: str = typer.Argument(..., help="Event name"),
    session: str = typer.Argument(..., help="Session name"),
) -> None:
    """Enable debug logging and load session."""
    tif1.setup_logging(logging.DEBUG)

    with Progress() as progress:
        task = progress.add_task("[cyan]Loading session with debug logging...", total=1)
        sess = tif1.get_session(year, event, session)
        progress.update(task, advance=1)

    rprint("[green]Session loaded successfully![/green]")
    rprint(f"[bold]Drivers:[/bold] {len(sess.drivers)}")
    rprint(f"[bold]Laps:[/bold] {len(sess.laps)}")


if __name__ == "__main__":
    app()
