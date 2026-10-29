"""CLI tool for tif1."""

import logging
from typing import Any, cast

import typer

import tif1

# pandas and rich are deliberately imported inside the command bodies:
# module-level imports cost ~500 ms of CLI startup for every invocation,
# while only data-rendering commands actually need them.


def _rprint(*args, **kwargs):
    from rich import print as rprint

    rprint(*args, **kwargs)


_console_obj = None


def _console():
    global _console_obj
    if _console_obj is None:
        from rich.console import Console

        _console_obj = Console()
    return _console_obj


app = typer.Typer(help="tif1 - Fast F1 data access CLI")


@app.callback()
def _configure_output() -> None:
    """Replace characters a legacy (cp1252) console cannot show instead of failing."""
    import sys

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(errors="replace")


@app.command()
def events(
    year: int = typer.Argument(..., help="Year (1950-current; timing and telemetry from 2018)"),
) -> None:
    """List all events for a year."""
    from rich.table import Table

    from tif1.events import _build_events_for_year

    # The season's event names, in schedule order: the same column
    # ``tif1.get_events`` exposes, without building the pandas schedule (the
    # pandas import alone is ~340 ms of a ~0.6 s command). A year without a
    # schedule (before 2018, or not published yet) has no events.
    event_names = _build_events_for_year(year)
    if not event_names:
        _rprint(f"[red]No events found for {year}[/red]")
        raise typer.Exit(code=1)

    table = Table(title=f"F1 Events {year}")
    table.add_column("#", style="cyan")
    table.add_column("Event", style="green")

    for idx, event_name in enumerate(event_names, 1):
        table.add_row(str(idx), event_name)

    _console().print(table)
    _console().print(f"\n[bold]Total:[/bold] {len(event_names)} events")


@app.command()
def sessions(
    year: int = typer.Argument(..., help="Year (1950-current; timing and telemetry from 2018)"),
    event: str = typer.Argument(..., help="Event name"),
) -> None:
    """List all sessions for an event."""
    from rich.table import Table

    sessions_list = tif1.get_sessions(year, event)

    table = Table(title=f"{event} {year}")
    table.add_column("#", style="cyan")
    table.add_column("Session", style="green")

    for idx, session in enumerate(sessions_list, 1):
        table.add_row(str(idx), session)

    _console().print(table)


@app.command()
def drivers(
    year: int = typer.Argument(..., help="Year (1950-current; timing and telemetry from 2018)"),
    event: str = typer.Argument(..., help="Event name"),
    session: str = typer.Argument(..., help="Session name"),
) -> None:
    """List all drivers in a session."""
    from rich.progress import Progress

    with Progress() as progress:
        task = progress.add_task("[cyan]Loading session...", total=1)
        # The table reads pandas rows (a polars default backend would hand
        # back a polars drivers_df): tables always load the pandas view.
        sess = tif1.get_session(year, event, session, lib="pandas")
        progress.update(task, advance=1)

    from rich.table import Table

    table = Table(title=f"{event} {year} - {session}")
    table.add_column("Driver", style="cyan")
    table.add_column("Team", style="green")

    # drivers_df is one row per driver in classification order.
    drivers_df = sess.drivers_df
    table.add_column("No.", style="magenta")
    for row in drivers_df.itertuples(index=False):
        table.add_row(str(row.Driver), str(row.Team), str(row.DriverNumber))

    _console().print(table)
    _console().print(f"\n[bold]Total:[/bold] {len(drivers_df)} drivers")


def _seconds(value: Any) -> float:
    """Lap time as float seconds (pandas keeps ``LapTime`` as a Timedelta)."""
    total_seconds = getattr(value, "total_seconds", None)
    if callable(total_seconds):
        return float(total_seconds())
    return float(value)


def _adopt_warm_laps(sess: Any) -> None:
    """Bind the session's laps from the laps frame tier when it holds them.

    With laps unloaded, ``get_fastest_laps`` and ``Driver.get_fastest_lap``
    rebuild lap rows from the raw laptime payloads (~80 ms warm, mostly dtype
    processing); a frame-tier hit costs ~2 ms and both then select from the
    in-memory table, printing the same laps. A miss (cold cache) changes
    nothing: no fetch is started here.
    """
    if sess._laps is None:
        laps = sess._laps_from_frame_tier()
        if laps is not None:
            sess._laps = laps


@app.command()
def fastest(
    year: int = typer.Argument(..., help="Year (1950-current; timing and telemetry from 2018)"),
    event: str = typer.Argument(..., help="Event name"),
    session: str = typer.Argument(..., help="Session name"),
    driver: str | None = typer.Option(None, "--driver", "-d", help="Specific driver"),
) -> None:
    """Show fastest laps."""
    import pandas as pd
    from rich.progress import Progress

    with Progress() as progress:
        task = progress.add_task("[cyan]Loading session...", total=1)
        sess = tif1.get_session(year, event, session)
        _adopt_warm_laps(sess)
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
            _rprint(f"[bold]{driver}[/bold] fastest lap: [green]{_seconds(time_val):.3f}s[/green]")
        else:
            _rprint(f"[red]No valid laps for {driver}[/red]")
    else:
        fastest_laps = sess.get_fastest_laps(by_driver=True)

        from rich.table import Table

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
                table.add_row(
                    str(idx), str(row.Driver), str(row.Team), f"{_seconds(time_val):.3f}s"
                )
        else:
            fastest_laps_pl = fastest_laps
            time_col = "LapTime" if "LapTime" in fastest_laps_pl.columns else "time"
            fastest_laps_pl = fastest_laps_pl.sort(time_col)
            for idx, row in enumerate(fastest_laps_pl.iter_rows(named=True), 1):
                table.add_row(str(idx), row["Driver"], row["Team"], f"{row[time_col]:.3f}s")

        _console().print(table)


def _fmt_td(value: Any) -> str:
    """``m:ss.mmm`` / ``h:mm:ss.mmm`` for a Timedelta, blank for missing values."""
    import pandas as pd

    if value is None or pd.isna(value):
        return ""
    total = float(pd.Timedelta(value).total_seconds())
    sign = "-" if total < 0 else ""
    total = abs(total)
    hours, rest = divmod(total, 3600)
    minutes, seconds = divmod(rest, 60)
    if hours >= 1:
        return f"{sign}{int(hours)}:{int(minutes):02d}:{seconds:06.3f}"
    return f"{sign}{int(minutes)}:{seconds:06.3f}"


def _fmt_num(value: Any, digits: int = 0) -> str:
    import pandas as pd

    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.{digits}f}" if digits else str(int(float(value)))


@app.command()
def results(
    year: int = typer.Argument(..., help="Year (1950-current)"),
    event: str = typer.Argument(..., help="Event name or round number"),
    session: str = typer.Argument(..., help="Session name (Race, Qualifying, Sprint, ...)"),
    no_ergast: bool = typer.Option(
        False, "--no-ergast", help="Skip the official Ergast classification merge"
    ),
) -> None:
    """Show the session classification (official Ergast data when available)."""
    from rich.progress import Progress
    from rich.table import Table

    with Progress() as progress:
        task = progress.add_task("[cyan]Loading session...", total=1)
        gp: int | str = int(event) if event.isdigit() else event
        sess = tif1.get_session(year, gp, session, lib="pandas", ergast=not no_ergast)
        frame = sess.results
        progress.update(task, advance=1)

    if frame.empty:
        _rprint(f"[red]No results for {year} {event} {session}[/red]")
        raise typer.Exit(code=1)

    quali = "Q1" in frame.columns and frame["Q1"].notna().any()
    table = Table(title=f"{sess.event_name} {year} - {sess.session_name}")
    table.add_column("Pos", style="cyan")
    table.add_column("No.", style="magenta")
    table.add_column("Driver", style="yellow")
    table.add_column("Team", style="green")
    if quali:
        for segment in ("Q1", "Q2", "Q3"):
            table.add_column(segment)
    else:
        table.add_column("Grid")
        table.add_column("Status")
        table.add_column("Points")
        table.add_column("Time / Gap", no_wrap=True)  # never truncate a race time
    for row in frame.itertuples(index=False):
        position = getattr(row, "ClassifiedPosition", "") or _fmt_num(
            getattr(row, "Position", None)
        )
        cells = [str(position), str(row.DriverNumber), str(row.Abbreviation), str(row.TeamName)]
        if quali:
            cells.extend(_fmt_td(getattr(row, segment)) for segment in ("Q1", "Q2", "Q3"))
        else:
            gap_text = _fmt_td(getattr(row, "TimeGap", None))
            time_text = _fmt_td(getattr(row, "Time", None))
            winner = _fmt_num(getattr(row, "Position", None)) == "1"
            shown = (
                time_text if winner or not gap_text or gap_text == "0:00.000" else f"+{gap_text}"
            )
            cells.extend(
                [
                    _fmt_num(getattr(row, "GridPosition", None)),
                    str(getattr(row, "Status", "") or ""),
                    _fmt_num(getattr(row, "Points", None), 1),
                    shown,
                ]
            )
        table.add_row(*cells)
    _console().print(table)


@app.command()
def standings(
    year: int = typer.Argument(..., help="Year (1950-current)"),
    round_number: int | None = typer.Argument(
        None, help="Round number (default: last completed round)"
    ),
    constructors: bool = typer.Option(
        False, "--constructors", "-c", help="Constructors' instead of drivers' standings"
    ),
) -> None:
    """Show the championship standings after a round (Ergast data)."""
    from rich.table import Table

    if constructors:
        frame = tif1.get_constructor_standings(year, round_number, lib="pandas")
        title = f"Constructors' standings {year}"
    else:
        frame = tif1.get_driver_standings(year, round_number, lib="pandas")
        title = f"Drivers' standings {year}"
    if round_number is not None:
        title += f" after round {round_number}"
    if frame.empty:
        _rprint(f"[red]No standings for {year}[/red]")
        raise typer.Exit(code=1)

    table = Table(title=title)
    table.add_column("Pos", style="cyan")
    if constructors:
        table.add_column("Team", style="green")
    else:
        table.add_column("Driver", style="yellow")
        table.add_column("Team", style="green")
    table.add_column("Points", style="magenta")
    table.add_column("Wins")
    for row in frame.itertuples(index=False):
        position = str(getattr(row, "PositionText", "") or _fmt_num(row.Position))
        if constructors:
            cells = [position, str(row.TeamName)]
        else:
            name = str(row.Abbreviation) or str(row.FullName)
            cells = [position, name, ", ".join(row.TeamNames)]
        cells.extend([_fmt_num(row.Points, 1), _fmt_num(row.Wins)])
        table.add_row(*cells)
    _console().print(table)


@app.command()
def cache_info() -> None:
    """Show cache information."""
    cache = tif1.get_cache()
    _rprint(f"[bold]Cache location:[/bold] {cache.cache_dir}")

    cache_files = [p for p in cache.cache_dir.glob("cache.sqlite*") if p.is_file()]
    if not cache_files:
        cache_files = [p for p in cache.cache_dir.iterdir() if p.is_file()]
    _rprint(f"[bold]Cache files:[/bold] {len(cache_files)}")

    total_size = sum(f.stat().st_size for f in cache_files)
    _rprint(f"[bold]Total size:[/bold] {total_size / 1024 / 1024:.2f} MB")


@app.command()
def cache_clear(
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    ergast: bool = typer.Option(
        False, "--ergast", help="Clear only the Ergast tier (Stats files and Jolpica answers)"
    ),
) -> None:
    """Clear cache."""
    what = "the Ergast cache tier" if ergast else "the cache"
    if not confirm:
        confirm = typer.confirm(f"Are you sure you want to clear {what}?")

    if confirm:
        cache = tif1.get_cache()
        if ergast:
            cache.invalidate("ergast")
            _rprint("[green]Ergast cache tier cleared successfully![/green]")
        else:
            cache.clear()
            _rprint("[green]Cache cleared successfully![/green]")
    else:
        _rprint("[yellow]Cache clear cancelled[/yellow]")


@app.command()
def version() -> None:
    """Show tif1 version."""
    _rprint(f"[bold]tif1[/bold] version [green]{tif1.__version__}[/green]")


@app.command()
def debug(
    year: int = typer.Argument(..., help="Year (1950-current; timing and telemetry from 2018)"),
    event: str = typer.Argument(..., help="Event name"),
    session: str = typer.Argument(..., help="Session name"),
) -> None:
    """Enable debug logging and load session."""
    tif1.setup_logging(logging.DEBUG)

    from rich.progress import Progress

    with Progress() as progress:
        task = progress.add_task("[cyan]Loading session with debug logging...", total=1)
        sess = tif1.get_session(year, event, session)
        progress.update(task, advance=1)

    _rprint("[green]Session loaded successfully![/green]")
    _rprint(f"[bold]Drivers:[/bold] {len(sess.drivers)}")
    _rprint(f"[bold]Laps:[/bold] {len(sess.laps)}")


if __name__ == "__main__":
    app()
