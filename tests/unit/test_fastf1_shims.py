"""fastf1 shims: ``tif1.logger``, the exception aliases, ``Cache`` keywords, event keywords."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

import tif1
from tif1 import exceptions as exc
from tif1 import fastf1_compat
from tif1.events import EventSchedule, get_events
from tif1.logger import LoggingManager, get_logger, set_log_level, soft_exceptions

# --------------------------------------------------------------------------- logger


@pytest.fixture
def _restore_log_levels(monkeypatch):
    """Put the ``tif1`` logger and its console handler back where they were."""
    root = logging.getLogger("tif1")
    handler = LoggingManager._console_handler
    root_level, handler_level = root.level, handler.level
    monkeypatch.setattr(LoggingManager, "debug", False)
    yield
    root.setLevel(root_level)
    handler.setLevel(handler_level)


def test_logger_names_are_tif1_never_fastf1():
    assert LoggingManager._root_logger is logging.getLogger("tif1")
    assert get_logger("core") is logging.getLogger("tif1.core")
    assert get_logger("core").name == "tif1.core"
    assert LoggingManager.get_child("ergast.jolpica").name == "tif1.ergast.jolpica"
    assert "fastf1" not in get_logger("anything").name
    # fastf1 attaches its console handler to the package logger at import; so does tif1.
    assert LoggingManager._console_handler in logging.getLogger("tif1").handlers


def test_console_handler_follows_the_current_stderr(monkeypatch):
    sink = MagicMock()
    monkeypatch.setattr(sys, "stderr", sink)
    assert LoggingManager._console_handler.stream is sink
    LoggingManager._console_handler.setStream(MagicMock())
    assert LoggingManager._console_handler.stream is sink


@pytest.mark.usefixtures("_restore_log_levels")
@pytest.mark.parametrize(
    ("level", "expected"),
    [
        ("WARNING", logging.WARNING),
        ("warning", logging.WARNING),
        ("Debug", logging.DEBUG),
        (logging.ERROR, logging.ERROR),
        (logging.INFO, logging.INFO),
    ],
)
def test_set_log_level_accepts_names_and_ints(level, expected):
    set_log_level(level)
    assert logging.getLogger("tif1").level == expected
    assert LoggingManager._console_handler.level == expected


@pytest.mark.usefixtures("_restore_log_levels")
def test_set_log_level_rejects_unknown_names():
    with pytest.raises(ValueError, match="Unknown log level 'LOUD'"):
        set_log_level("LOUD")


@pytest.mark.usefixtures("_restore_log_levels")
def test_set_log_level_default_is_warning():
    set_log_level(logging.DEBUG)
    set_log_level()
    assert logging.getLogger("tif1").level == logging.WARNING


@pytest.mark.usefixtures("_restore_log_levels")
def test_compat_and_top_level_set_log_level_share_the_implementation():
    fastf1_compat.set_log_level("ERROR")
    assert logging.getLogger("tif1").level == logging.ERROR
    assert LoggingManager._console_handler.level == logging.ERROR

    tif1.set_log_level(logging.INFO)
    assert logging.getLogger("tif1").level == logging.INFO

    fastf1_compat.set_log_level()
    assert logging.getLogger("tif1").level == logging.WARNING


@pytest.mark.usefixtures("_restore_log_levels")
def test_soft_exceptions_logs_and_returns_none(caplog):
    log = get_logger("shimtest")
    caplog.set_level(logging.DEBUG, logger="tif1.shimtest")

    @soft_exceptions("lap data", "Failed to load lap data", log)
    def load(value):
        raise RuntimeError(f"boom {value}")

    assert load(3) is None
    warnings_ = [r for r in caplog.records if r.levelno == logging.WARNING]
    debugs = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert [r.getMessage() for r in warnings_] == ["Failed to load lap data"]
    assert len(debugs) == 1
    assert debugs[0].getMessage() == "Traceback for failure in lap data"
    assert debugs[0].exc_info is not None
    assert isinstance(debugs[0].exc_info[1], RuntimeError)
    assert str(debugs[0].exc_info[1]) == "boom 3"


@pytest.mark.usefixtures("_restore_log_levels")
def test_soft_exceptions_passes_results_and_metadata_through():
    @soft_exceptions("x", "never shown", get_logger("shimtest"))
    def add(a, b=1):
        """Add two numbers."""
        return a + b

    assert add(2, b=3) == 5
    assert add.__name__ == "add"
    assert add.__doc__ == "Add two numbers."


@pytest.mark.usefixtures("_restore_log_levels")
@pytest.mark.parametrize(
    "error",
    [
        exc.FastF1CriticalError("stop"),
        exc.RateLimitExceededError(url="https://api.jolpi.ca/ergast/f1/2024.json"),
    ],
)
def test_soft_exceptions_always_reraises_critical_errors(caplog, error):
    caplog.set_level(logging.DEBUG, logger="tif1.shimtest")

    @soft_exceptions("x", "swallowed?", get_logger("shimtest"))
    def load():
        raise error

    with pytest.raises(exc.FastF1CriticalError):
        load()
    assert not caplog.records


@pytest.mark.usefixtures("_restore_log_levels")
def test_soft_exceptions_debug_mode_reraises_everything(caplog, monkeypatch):
    caplog.set_level(logging.DEBUG, logger="tif1.shimtest")
    monkeypatch.setattr(LoggingManager, "debug", True)

    @soft_exceptions("x", "swallowed?", get_logger("shimtest"))
    def load():
        raise KeyError("missing")

    with pytest.raises(KeyError, match="missing"):
        load()
    assert not caplog.records


@pytest.mark.parametrize(("env_value", "expected"), [("1", "True"), (None, "False")])
def test_tif1_debug_env_var_sets_debug_mode(env_value, expected):
    env = {k: v for k, v in os.environ.items() if k != "TIF1_DEBUG"}
    if env_value is not None:
        env["TIF1_DEBUG"] = env_value
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from tif1.logger import LoggingManager; print(LoggingManager.debug)",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )
    assert result.stdout.strip() == expected
    assert ("Debug Mode enabled for Logger!" in result.stderr) == (env_value == "1")


# ----------------------------------------------------------------------- exceptions


def test_data_not_loaded_error_is_session_not_loaded_error():
    assert exc.DataNotLoadedError is exc.SessionNotLoadedError
    with pytest.raises(exc.DataNotLoadedError, match="Access 'laps' requires loading"):
        raise exc.SessionNotLoadedError("laps")


def test_no_lap_data_error_mro_and_message():
    err = exc.NoLapDataError()
    assert isinstance(err, exc.LapNotFoundError)
    assert isinstance(err, exc.DataNotFoundError)
    assert isinstance(err, exc.TIF1Error)
    assert str(err) == "Failed to load session because the API did not provide any usable data."
    assert exc.NoLapDataError("ignored", "args").message == str(err)
    assert exc.NoLapDataError(event="Monaco%20Grand%20Prix").context == {
        "event": "Monaco Grand Prix"
    }


def test_fuzzy_match_error_mro():
    assert issubclass(exc.FuzzyMatchError, exc.EventNotFoundError)
    assert issubclass(exc.FuzzyMatchError, ValueError)
    assert not issubclass(exc.FuzzyMatchError, exc.TIF1Error)
    with pytest.raises(exc.EventNotFoundError):
        raise exc.FuzzyMatchError("no event matching 'xyz'")


def test_fastf1_critical_error_is_a_tif1_error_with_context():
    err = exc.FastF1CriticalError("stop", reason="limit")
    assert isinstance(err, exc.TIF1Error)
    assert err.message == "stop"
    assert err.context == {"reason": "limit"}


def test_rate_limit_error_has_both_parents():
    assert issubclass(exc.RateLimitExceededError, exc.NetworkError)
    assert issubclass(exc.RateLimitExceededError, exc.FastF1CriticalError)
    mro = exc.RateLimitExceededError.__mro__
    assert mro.index(exc.NetworkError) < mro.index(exc.FastF1CriticalError)
    assert mro.index(exc.FastF1CriticalError) < mro.index(exc.TIF1Error)

    err = exc.RateLimitExceededError(url="https://api.jolpi.ca/x", retry_after=2.0)
    assert isinstance(err, exc.NetworkError)
    assert isinstance(err, exc.FastF1CriticalError)
    assert isinstance(err, exc.TIF1Error)
    assert str(err) == "Jolpica rate limit exceeded (retry after 2s): url='https://api.jolpi.ca/x'"
    assert err.context["status_code"] == 429
    assert err.context["retry_after"] == 2.0
    assert err.context["url"] == "https://api.jolpi.ca/x"

    with pytest.raises(exc.NetworkError):
        raise err
    with pytest.raises(exc.FastF1CriticalError):
        raise err


def test_other_exceptions_keep_their_parents():
    assert exc.NetworkError.__bases__ == (exc.TIF1Error,)
    assert exc.CircuitBreakerOpenError.__bases__ == (exc.NetworkError,)
    assert not issubclass(exc.NetworkError, exc.FastF1CriticalError)
    assert not issubclass(exc.ErgastError, exc.FastF1CriticalError)


# ---------------------------------------------------------------------------- Cache


@pytest.fixture
def cache_config(monkeypatch):
    """Cache enabled, no disable depth, every change undone at teardown."""
    from tif1.config import get_config

    config = get_config()
    monkeypatch.setitem(config._config, "enable_cache", True)
    monkeypatch.setattr(fastf1_compat.Cache, "_disable_depth", 0)
    monkeypatch.setattr(fastf1_compat.Cache, "_enable_cache_before_disable", None)
    return config


def test_configure_maps_onto_enable_cache(monkeypatch, tmp_path, cache_config):
    import tif1.cache as cache_module

    fake = MagicMock()
    fake.cache_dir = tmp_path
    monkeypatch.setattr(cache_module, "_cache", fake, raising=False)
    monkeypatch.setattr(cache_module, "get_cache", lambda: fake)
    monkeypatch.setitem(cache_config._config, "cache_dir", cache_config.get("cache_dir"))

    assert fastf1_compat.Cache.configure(cache_dir=tmp_path, force_renew=True) is None
    fake.clear.assert_called_once()
    assert cache_config.get("enable_cache") is True
    assert cache_config.get("cache_dir") == str(tmp_path)
    assert fastf1_compat.Cache._cache_dir == tmp_path


def test_configure_forwards_every_keyword(monkeypatch, tmp_path):
    seen: dict = {}
    monkeypatch.setattr(
        fastf1_compat.Cache, "enable_cache", classmethod(lambda _cls, **kw: seen.update(kw))
    )
    fastf1_compat.Cache.configure(
        cache_dir=tmp_path, force_renew=True, ignore_version=True, use_requests_cache=False
    )
    assert seen == {
        "cache_dir": tmp_path,
        "force_renew": True,
        "ignore_version": True,
        "use_requests_cache": False,
    }
    seen.clear()
    fastf1_compat.Cache.configure()
    assert seen == {
        "cache_dir": None,
        "force_renew": False,
        "ignore_version": False,
        "use_requests_cache": True,
    }


def test_configure_is_keyword_only(tmp_path):
    with pytest.raises(TypeError):
        fastf1_compat.Cache.configure(tmp_path)  # type: ignore[ty:too-many-positional-arguments]


@pytest.mark.parametrize(
    ("http", "func"),
    [(True, True), (True, False), (False, True)],
)
def test_disabled_either_stage_disables_the_single_toggle(cache_config, http, func):
    with fastf1_compat.Cache.disabled(disable_http_cache=http, disable_func_cache=func):
        assert cache_config.get("enable_cache") is False
        assert fastf1_compat.Cache._disable_depth == 1
    assert cache_config.get("enable_cache") is True
    assert fastf1_compat.Cache._disable_depth == 0


def test_disabled_both_false_is_a_noop(cache_config):
    with fastf1_compat.Cache.disabled(disable_http_cache=False, disable_func_cache=False):
        assert cache_config.get("enable_cache") is True
        assert fastf1_compat.Cache._disable_depth == 0
    assert cache_config.get("enable_cache") is True
    assert fastf1_compat.Cache._disable_depth == 0


def test_set_disabled_both_false_is_a_noop_and_set_enabled_stays_safe(cache_config):
    fastf1_compat.Cache.set_disabled(disable_http_cache=False, disable_func_cache=False)
    assert cache_config.get("enable_cache") is True
    assert fastf1_compat.Cache._disable_depth == 0
    fastf1_compat.Cache.set_enabled()
    assert cache_config.get("enable_cache") is True
    assert fastf1_compat.Cache._disable_depth == 0


def test_disabled_noop_inside_a_disabled_block_keeps_it_disabled(cache_config):
    with fastf1_compat.Cache.disabled():
        with fastf1_compat.Cache.disabled(disable_http_cache=False, disable_func_cache=False):
            assert cache_config.get("enable_cache") is False
        assert cache_config.get("enable_cache") is False
        assert fastf1_compat.Cache._disable_depth == 1
    assert cache_config.get("enable_cache") is True


def test_disabled_blocks_nest(cache_config):
    with fastf1_compat.Cache.disabled():
        with fastf1_compat.Cache.disabled(disable_func_cache=False):
            assert fastf1_compat.Cache._disable_depth == 2
            assert cache_config.get("enable_cache") is False
        assert cache_config.get("enable_cache") is False
        assert fastf1_compat.Cache._disable_depth == 1
    assert cache_config.get("enable_cache") is True
    assert fastf1_compat.Cache._disable_depth == 0


def test_set_disabled_depth_and_previous_state(cache_config):
    cache_config.set("enable_cache", False)
    fastf1_compat.Cache.set_disabled()
    fastf1_compat.Cache.set_disabled(disable_http_cache=False)
    assert fastf1_compat.Cache._disable_depth == 2
    fastf1_compat.Cache.set_enabled()
    assert cache_config.get("enable_cache") is False
    fastf1_compat.Cache.set_enabled()
    # The value from before the first set_disabled comes back, not a blanket True.
    assert cache_config.get("enable_cache") is False
    assert fastf1_compat.Cache._disable_depth == 0


# --------------------------------------------------------------------------- events


@pytest.fixture(scope="module")
def schedule() -> EventSchedule:
    """The vendored 2024 schedule (no network: past seasons are packaged)."""
    return get_events(2024)


def test_get_event_by_name_accepts_both_exact_keywords(schedule):
    by_exact = schedule.get_event_by_name("British Grand Prix", exact_match=True)
    by_strict = schedule.get_event_by_name("British Grand Prix", strict_search=True)
    positional = schedule.get_event_by_name("british grand prix", True)
    assert by_exact is not None
    assert by_strict is not None
    assert positional is not None
    assert by_exact["EventName"] == by_strict["EventName"] == positional["EventName"]
    assert by_exact["EventName"] == "British Grand Prix"
    assert by_exact["RoundNumber"] == 12

    fuzzy = schedule.get_event_by_name("Silverstone")
    assert fuzzy is not None
    assert fuzzy["EventName"] == "British Grand Prix"
    assert schedule.get_event_by_name("British", exact_match=True) is None
    assert schedule.get_event_by_name("British", strict_search=True) is None


def test_get_event_accepts_exact_match(schedule):
    event = schedule.get_event("British Grand Prix", exact_match=True)
    assert event is not None
    assert event["EventName"] == "British Grand Prix"
    assert schedule.get_event("British", exact_match=True) is None


def test_get_event_by_round_accepts_round_keyword(schedule):
    positional = schedule.get_event_by_round(1)
    by_number = schedule.get_event_by_round(round_number=1)
    by_round = schedule.get_event_by_round(round=1)
    assert positional["EventName"] == by_number["EventName"] == by_round["EventName"]
    assert by_round["EventName"] == "Bahrain Grand Prix"
    assert by_round["RoundNumber"] == 1
    assert by_round.year == 2024


def test_get_event_by_round_requires_exactly_one_keyword(schedule):
    with pytest.raises(ValueError, match="Cannot pass both"):
        schedule.get_event_by_round(1, round=1)
    with pytest.raises(ValueError, match="missing 1 required argument: 'round_number'"):
        schedule.get_event_by_round()
    with pytest.raises(ValueError, match="Cannot get testing event by round number"):
        schedule.get_event_by_round(round=0)
    with pytest.raises(ValueError, match="Invalid round: 99"):
        schedule.get_event_by_round(round=99)


def test_event_get_session_accepts_identifier_and_session_name(schedule):
    event = schedule.get_event_by_round(1)
    # ``session_name`` is the decoded form (``session`` stays URL-encoded for CDN paths).
    assert event.get_session("Race").session_name == "Race"
    assert event.get_session(identifier="Q").session_name == "Qualifying"
    assert event.get_session(session_name="Race").session_name == "Race"
    assert event.get_session(5).session_name == "Race"
    assert event.get_session(session_name=1).session_name == "Practice 1"
    assert event.get_session(session_name=1).session == "Practice%201"


def test_event_get_session_requires_exactly_one_identifier(schedule):
    event = schedule.get_event_by_round(1)
    with pytest.raises(ValueError, match="missing 1 required argument: 'identifier'"):
        event.get_session()
    with pytest.raises(ValueError, match="Cannot pass both"):
        event.get_session("Race", session_name="Race")
    with pytest.raises(ValueError, match="does not exist for this event"):
        event.get_session(session_name="Sprint")


def test_base_class_view_is_a_plain_dataframe(schedule):
    view = schedule.base_class_view
    assert type(view) is pd.DataFrame
    assert not isinstance(view, EventSchedule)
    assert list(view.columns) == list(schedule.columns)
    assert len(view) == len(schedule)
    assert view["EventName"].tolist() == schedule["EventName"].tolist()
    # A view of a sliced schedule is a plain frame as well.
    assert type(schedule.head(2).base_class_view) is pd.DataFrame
