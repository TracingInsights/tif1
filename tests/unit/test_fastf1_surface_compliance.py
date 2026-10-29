"""fastf1 surface lock: every public fastf1 name, parameter and column exists in tif1.

fastf1 is compared by introspection only (classes, ``inspect.signature``, the
``_COLUMNS``/``_CHANNELS`` tables); it never loads a session here (that needs
an F1 account). The tif1 frames come from an offline session served from
in-memory payloads (``InMemoryTransport``, the ``ergast_offline`` fixture and
a fake ``fetch_multiple_async``), so the whole file runs without a socket.

Runs only when fastf1 is installed (``pytest.importorskip``; install it by hand
as for ``test_ergast_fastf1_compliance.py``: ``uv pip install fastf1 --no-deps``).
The installed version is pinned: a fastf1 bump fails
``test_installed_fastf1_is_the_pinned_version`` and the other tests then list
what the new release added.

Rules:

- Members: the public names (no leading ``_``) of a fastf1 class, plus the
  public instance attributes its ``__init__`` assigns, its pandas ``_metadata``
  and its dataclass fields. Names a fastf1 class inherits from a non-fastf1
  base (pandas ``DataFrame``/``Series``, ``list``, ``object``) are excluded on
  both backends: tif1's pandas classes inherit them too, and on polars they
  have no meaning. Modules: ``__all__`` when defined, else the public names
  defined in fastf1 (imported stdlib/third-party names are skipped), plus the
  public submodules (``pkgutil``, so the result does not depend on what else
  the process imported).
- Parameters: every fastf1 parameter name (positional-or-keyword or
  keyword-only, no leading ``_``) must be a named parameter of the tif1
  callable; ``*args``/``**kwargs`` do not count. Constructors compare
  ``__init__``.
- Columns: fastf1's columns must be present, in fastf1's relative order (tif1
  extras may sit anywhere) and in the same dtype family; the documented tif1
  deviations (``DEVIATIONS``) are spelled out next to the tables below.
- ``OUT_OF_SCOPE`` is the only name allow-list; ``DEVIATIONS`` only widens
  dtype families, never excuses a missing column.
"""

from __future__ import annotations

import ast
import dataclasses
import importlib
import importlib.util
import inspect
import pkgutil
import textwrap
import types
from collections.abc import Callable
from typing import Any, NamedTuple

import pandas as pd
import pytest

pytest.importorskip("fastf1")

import fastf1
import fastf1.core
import fastf1.events

import tif1

pytestmark = pytest.mark.compliance_offline

FASTF1_PINNED_VERSION = "3.9.0"

HAS_POLARS = importlib.util.find_spec("polars") is not None
needs_polars = pytest.mark.skipif(not HAS_POLARS, reason="polars is not installed")

#: fastf1 names tif1 deliberately does not provide (qualified fastf1 name -> reason).
OUT_OF_SCOPE: dict[str, str] = {
    # Plan section 8: live timing and fastf1's own plumbing.
    "fastf1.set_auth_method": "F1 account sign-in for live timing; tif1 serves historical data",
    "fastf1.exceptions.F1AuthError": "raised only by the F1 sign-in flow tif1 does not have",
    "fastf1.livetiming": "live timing recorder/client; tif1 serves historical data only",
    "fastf1.req": "fastf1's HTTP/cache layer; tif1's is http_session.py, cdn.py and cache.py",
    "fastf1.api": "fastf1's livetiming-API parsers, slated to become private upstream",
    "fastf1.internals": "fastf1-private helpers (pandas base classes, fuzzy matcher)",
    # fastf1-private implementation details.
    "fastf1.mvapi.internals": "fastf1-private MultiViewer helpers (same rule as fastf1.internals)",
    "fastf1.ergast.sphinx": "Sphinx extension that renders fastf1's own documentation",
    # Plan section 2.1: documented constructor difference; users go through get_session.
    "fastf1.core.Session.__init__": "Session(event, session_name, ...) vs Session(year, gp, session)",
}


# --------------------------------------------------------------------------- #
# Introspection helpers
# --------------------------------------------------------------------------- #


def _resolve(path: str) -> Any:
    """Import the longest module prefix of a dotted ``path`` and walk the rest."""
    parts = path.split(".")
    for split in range(len(parts), 0, -1):
        try:
            obj = importlib.import_module(".".join(parts[:split]))
        except ImportError:
            continue
        for attribute in parts[split:]:
            obj = getattr(obj, attribute)
        return obj
    raise ImportError(path)


def _package(cls: type) -> str:
    return cls.__module__.split(".", 1)[0]


def _assigned_self_attributes(function: Any) -> set[str]:
    """Names assigned as ``self.<name>`` in the body of ``function``."""
    try:
        source = textwrap.dedent(inspect.getsource(function))
    except (OSError, TypeError):  # generated (dataclass) or builtin __init__
        return set()
    names: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign | ast.AugAssign):
            targets = [node.target]
        else:
            continue
        for target in targets:
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
            ):
                names.add(target.attr)
    return names


def _declared_attributes(cls: type) -> set[str]:
    """Instance attributes ``cls`` declares without a class attribute.

    ``self.<name>`` assignments in ``__init__``, pandas ``_metadata``,
    ``__slots__`` and dataclass fields, over the classes of ``cls``'s MRO that
    belong to the same package.
    """
    package = _package(cls)
    names: set[str] = set()
    for klass in cls.__mro__:
        if _package(klass) != package:
            continue
        namespace = vars(klass)
        if "__init__" in namespace:
            names |= _assigned_self_attributes(namespace["__init__"])
        names.update(namespace.get("_metadata", ()))
        slots = namespace.get("__slots__", ())
        names.update((slots,) if isinstance(slots, str) else slots)
    if dataclasses.is_dataclass(cls):
        names.update(field.name for field in dataclasses.fields(cls))
    return {name for name in names if not name.startswith("_")}


def _foreign_base_members(cls: type) -> set[str]:
    """Members ``cls`` inherits from bases outside its own package (pandas, list, object)."""
    package = _package(cls)
    members: set[str] = set()
    for klass in cls.__mro__:
        if _package(klass) != package:
            members.update(dir(klass))
    return members


def _class_members(cls: type) -> set[str]:
    public = {name for name in dir(cls) if not name.startswith("_")}
    return (public | _declared_attributes(cls)) - _foreign_base_members(cls)


def _class_has(cls: type, name: str) -> bool:
    return hasattr(cls, name) or name in _declared_attributes(cls)


def _module_members(module: types.ModuleType) -> set[str]:
    package = module.__name__.split(".", 1)[0]
    exported = getattr(module, "__all__", None)
    if exported is not None:
        names = set(exported)
    else:
        names = set()
        for name in dir(module):
            if name.startswith("_"):
                continue
            value = getattr(module, name)
            if isinstance(value, types.ModuleType):
                continue  # submodules come from pkgutil below, imports are not members
            origin = getattr(value, "__module__", None)
            if isinstance(origin, str) and origin.split(".", 1)[0] != package:
                continue  # imported from the stdlib or a third-party package
            names.add(name)
    if hasattr(module, "__path__"):
        names.update(
            info.name
            for info in pkgutil.iter_modules(module.__path__)
            if not info.name.startswith("_")
        )
    return names


def _module_has(module: types.ModuleType, name: str) -> bool:
    if hasattr(module, name):
        return True
    if not hasattr(module, "__path__"):
        return False
    return importlib.util.find_spec(f"{module.__name__}.{name}") is not None


def _signature(obj: Any) -> inspect.Signature | None:
    target = obj.__init__ if isinstance(obj, type) else obj
    try:
        return inspect.signature(target)
    except (TypeError, ValueError):  # builtin slot wrappers (object/Exception __init__)
        return None


_NAMED = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)


def _wanted_parameters(obj: Any) -> list[str]:
    signature = _signature(obj)
    if signature is None:
        return []
    return [
        parameter.name
        for parameter in signature.parameters.values()
        if parameter.kind in _NAMED
        and not parameter.name.startswith("_")
        and parameter.name not in {"self", "cls"}
    ]


def _accepted_parameters(obj: Any) -> set[str]:
    signature = _signature(obj)
    if signature is None:
        return set()
    return {
        parameter.name for parameter in signature.parameters.values() if parameter.kind in _NAMED
    }


def _parameter_gaps(reference: Any, target: Any) -> list[str]:
    accepted = _accepted_parameters(target)
    return [name for name in _wanted_parameters(reference) if name not in accepted]


def _is_routine(obj: Any) -> bool:
    return callable(obj) and not isinstance(obj, type)


# --------------------------------------------------------------------------- #
# Surface pairs
# --------------------------------------------------------------------------- #


class Pair(NamedTuple):
    """A reference surface (fastf1, or tif1's pandas class) and its tif1 counterpart."""

    reference: str
    target: str

    @property
    def id(self) -> str:
        return f"{self.reference}->{self.target}"


_ERGAST_RESPONSES = (
    "ErgastResponseMixin",
    "ErgastSimpleResponse",
    "ErgastMultiResponse",
    "ErgastRawResponse",
    "ErgastResultFrame",
    "ErgastResultSeries",
)

CLASS_PAIRS = [
    Pair("fastf1.core.Session", "tif1.core.Session"),
    Pair("fastf1.core.Laps", "tif1.models.Laps"),
    Pair("fastf1.core.Lap", "tif1.models.Lap"),
    Pair("fastf1.core.Telemetry", "tif1.models.Telemetry"),
    Pair("fastf1.core.SessionResults", "tif1.models.SessionResults"),
    Pair("fastf1.core.DriverResult", "tif1.models.DriverResult"),
    Pair("fastf1.mvapi.CircuitInfo", "tif1.models.CircuitInfo"),
    Pair("fastf1.events.Event", "tif1.events.Event"),
    Pair("fastf1.events.EventSchedule", "tif1.events.EventSchedule"),
    Pair("fastf1.ergast.Ergast", "tif1.ergast.Ergast"),
    *(
        Pair(f"fastf1.ergast.interface.{name}", f"tif1.ergast.interface.{name}")
        for name in _ERGAST_RESPONSES
    ),
    Pair("fastf1.Cache", "tif1.Cache"),
]

POLARS_CLASS_PAIRS = [
    Pair("fastf1.core.Laps", "tif1.models_polars.PolarsLaps"),
    Pair("fastf1.core.Lap", "tif1.models_polars.PolarsLap"),
    Pair("fastf1.core.Telemetry", "tif1.models_polars.PolarsTelemetry"),
    Pair("fastf1.core.SessionResults", "tif1.models_polars.PolarsSessionResults"),
    Pair("fastf1.core.DriverResult", "tif1.models_polars.PolarsDriverResult"),
    Pair("fastf1.core.DriverResult", "tif1.models_polars.PolarsDriver"),
    # fastf1 has no Driver class; PolarsDriver mirrors tif1's pandas Driver (plan 9.1).
    Pair("tif1.models.Driver", "tif1.models_polars.PolarsDriver"),
]

MODULE_PAIRS = [
    Pair("fastf1", "tif1"),
    Pair("fastf1.plotting", "tif1.plotting"),
    Pair("fastf1.utils", "tif1.utils"),
    Pair("fastf1.logger", "tif1.logger"),
    Pair("fastf1.mvapi", "tif1.mvapi"),
    Pair("fastf1.exceptions", "tif1.exceptions"),
    Pair("fastf1.ergast", "tif1.ergast"),
]

ALL_CLASS_PAIRS = [
    *(pytest.param(pair, id=pair.id) for pair in CLASS_PAIRS),
    *(pytest.param(pair, id=pair.id, marks=needs_polars) for pair in POLARS_CLASS_PAIRS),
]


def _check_class_members(pair: Pair) -> None:
    reference, target = _resolve(pair.reference), _resolve(pair.target)
    missing = sorted(
        name
        for name in _class_members(reference)
        if f"{pair.reference}.{name}" not in OUT_OF_SCOPE and not _class_has(target, name)
    )
    assert not missing, f"{pair.target} lacks {pair.reference} members: {', '.join(missing)}"


def _check_class_parameters(pair: Pair) -> None:
    reference, target = _resolve(pair.reference), _resolve(pair.target)
    names = ["__init__", *sorted(_class_members(reference))]
    gaps: list[str] = []
    for name in names:
        if f"{pair.reference}.{name}" in OUT_OF_SCOPE or not hasattr(target, name):
            continue  # missing members are reported by the member test
        ours, theirs = getattr(target, name), getattr(reference, name, None)
        if theirs is None or not _is_routine(theirs):
            continue
        if not _is_routine(ours):
            gaps.append(f"{name} (not callable in tif1)")
            continue
        missing = _parameter_gaps(theirs, ours)
        if missing:
            gaps.append(f"{name}({', '.join(missing)})")
    assert not gaps, f"{pair.target} does not accept {pair.reference} parameters: " + "; ".join(
        gaps
    )


# --------------------------------------------------------------------------- #
# Member and parameter parity
# --------------------------------------------------------------------------- #


def test_installed_fastf1_is_the_pinned_version():
    assert fastf1.__version__ == FASTF1_PINNED_VERSION, (
        f"fastf1 {fastf1.__version__} is installed but this lock was taken against "
        f"{FASTF1_PINNED_VERSION}: review the surface diff below, close the gaps (or extend "
        "OUT_OF_SCOPE with a reason), then bump FASTF1_PINNED_VERSION"
    )


@pytest.mark.parametrize("name", sorted(OUT_OF_SCOPE))
def test_out_of_scope_entries_still_exist_in_fastf1(name):
    """An allow-list entry fastf1 no longer has must be deleted, not carried."""
    module_path = name
    while module_path:
        try:
            if importlib.util.find_spec(module_path) is not None:
                break
        except ModuleNotFoundError:
            pass
        module_path = module_path.rpartition(".")[0]
    if module_path == name:
        return  # a (sub)module: found without importing it (fastf1.ergast.sphinx needs sphinx)
    obj: Any = importlib.import_module(module_path)
    for attribute in name[len(module_path) :].split(".")[1:]:
        assert hasattr(obj, attribute), f"{name} no longer exists in fastf1"
        obj = getattr(obj, attribute)


@pytest.mark.parametrize("pair", ALL_CLASS_PAIRS)
def test_class_members(pair):
    _check_class_members(pair)


@pytest.mark.parametrize("pair", ALL_CLASS_PAIRS)
def test_class_parameters(pair):
    _check_class_parameters(pair)


@pytest.mark.parametrize("pair", [pytest.param(pair, id=pair.id) for pair in MODULE_PAIRS])
def test_module_members(pair):
    reference, target = _resolve(pair.reference), _resolve(pair.target)
    missing = sorted(
        name
        for name in _module_members(reference)
        if f"{pair.reference}.{name}" not in OUT_OF_SCOPE and not _module_has(target, name)
    )
    assert not missing, f"{pair.target} lacks {pair.reference} members: {', '.join(missing)}"


@pytest.mark.parametrize("pair", [pytest.param(pair, id=pair.id) for pair in MODULE_PAIRS])
def test_module_parameters(pair):
    reference, target = _resolve(pair.reference), _resolve(pair.target)
    gaps: list[str] = []
    for name in sorted(_module_members(reference)):
        if f"{pair.reference}.{name}" in OUT_OF_SCOPE or not hasattr(target, name):
            continue
        theirs, ours = getattr(reference, name, None), getattr(target, name)
        if theirs is None or isinstance(theirs, types.ModuleType) or not callable(theirs):
            continue
        if not callable(ours):
            gaps.append(f"{name} (not callable in tif1)")
            continue
        missing = _parameter_gaps(theirs, ours)
        if missing:
            gaps.append(f"{name}({', '.join(missing)})")
    assert not gaps, f"{pair.target} does not accept {pair.reference} parameters: " + "; ".join(
        gaps
    )


# --------------------------------------------------------------------------- #
# Column parity
# --------------------------------------------------------------------------- #

#: fastf1 3.9.0 ``Lap.get_telemetry()`` columns, in order, with the dtype
#: family of fastf1's car/position streams (``fastf1._api.car_data`` and
#: ``position_data`` cast RPM/Speed/nGear/Throttle/DRS/X/Y/Z to int64;
#: ``merge_channels`` restores those dtypes after merging). This is
#: ``Telemetry._CHANNELS`` minus ``DifferentialDistance``, which only
#: ``add_differential_distance()`` adds; the channels fastf1 marks
#: ``"excluded"`` (``Date``, ``Time``, ``SessionTime``, ``Source``) are excluded
#: from interpolation, not from the frame.
FASTF1_TELEMETRY_COLUMNS: dict[str, str] = {
    "Date": "datetime",
    "SessionTime": "timedelta",
    "DriverAhead": "str",
    "DistanceToDriverAhead": "float",
    "Time": "timedelta",
    "RPM": "int",
    "Speed": "int",
    "nGear": "int",
    "Throttle": "int",
    "Brake": "bool",
    "DRS": "int",
    "Source": "str",
    "Distance": "float",
    "RelativeDistance": "float",
    "Status": "str",
    "X": "int",
    "Y": "int",
    "Z": "int",
}
TELEMETRY_CHANNELS_NOT_EMITTED = {"DifferentialDistance"}

#: fastf1 family -> the tif1 families that satisfy it. ``object`` is pandas'
#: classic string storage (fastf1's own str columns are object); fastf1's
#: ``bool | None`` (``Deleted``) is an object column of bools and ``None``; its
#: ``object`` columns (``Session*Date``, tz-aware timestamps of mixed offsets)
#: also accept a datetime dtype.
_ACCEPTED: dict[str, set[str]] = {
    "timedelta": {"timedelta"},
    "datetime": {"datetime"},
    "float": {"float"},
    "int": {"int"},
    "bool": {"bool"},
    "nullable-bool": {"bool", "object"},
    "str": {"str", "object"},
    "object": {"object", "datetime"},
}

#: Documented tif1 deviations: (backend, frame) -> column -> extra accepted families.
#: Categorical Driver/Team/Compound/TrackStatus count as str-like and nullable
#: ``Int64``/``boolean`` as int/bool-like through the family rules themselves.
_INTERPOLATED_AS_FLOAT = {"RPM", "Speed", "Throttle", "X", "Y", "Z"}
_TELEMETRY_DEVIATIONS = {column: {"float"} for column in _INTERPOLATED_AS_FLOAT}
DEVIATIONS: dict[tuple[str, str], dict[str, set[str]]] = {
    # Plan 5.1: the payload values are interpolated, so RPM/Speed/Throttle stay
    # float64; X/Y/Z (int64 in fastf1's position stream) follow the same rule.
    ("pandas", "telemetry"): _TELEMETRY_DEVIATIONS,
    ("pandas", "car_data"): _TELEMETRY_DEVIATIONS,
    ("polars", "telemetry"): _TELEMETRY_DEVIATIONS,
    ("polars", "car_data"): _TELEMETRY_DEVIATIONS,
    # fastf1 stores these counts as float64 only to hold NaN; polars integers
    # are nullable, so the polars laps contract keeps them Int64.
    ("polars", "laps"): {
        column: {"int"} for column in ("LapNumber", "Stint", "TyreLife", "Position")
    },
}


def _fastf1_family(spec: Any) -> str:
    """The dtype family of one fastf1 ``_COLUMNS`` entry."""
    if isinstance(spec, types.UnionType):
        members = [arg for arg in spec.__args__ if arg is not type(None)]
        if members == [bool]:
            return "nullable-bool"
    elif isinstance(spec, str):
        kind = pd.api.types.pandas_dtype(spec).kind
        family = {"m": "timedelta", "M": "datetime", "f": "float", "i": "int", "b": "bool"}
        if kind in family:
            return family[kind]
    elif spec in (str, bool, int, float, object):
        return spec.__name__
    raise AssertionError(f"unknown fastf1 column type {spec!r}: extend _fastf1_family")


def _pandas_family(dtype: Any) -> str:
    if isinstance(dtype, pd.CategoricalDtype):
        categories = dtype.categories.dtype
        string_like = pd.api.types.is_object_dtype(categories) or isinstance(
            categories, pd.StringDtype
        )
        return "str" if string_like else f"categorical[{categories}]"
    if pd.api.types.is_bool_dtype(dtype):
        return "bool"
    if pd.api.types.is_timedelta64_dtype(dtype):
        return "timedelta"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    if pd.api.types.is_integer_dtype(dtype):
        return "int"
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if isinstance(dtype, pd.StringDtype):
        return "str"
    if pd.api.types.is_object_dtype(dtype):
        return "object"
    return str(dtype)


def _polars_family(dtype: Any) -> str:
    import polars as pl

    if isinstance(dtype, pl.Duration):
        return "timedelta"
    if isinstance(dtype, pl.Datetime):
        return "datetime"
    if isinstance(dtype, pl.Boolean):
        return "bool"
    if isinstance(dtype, pl.String | pl.Categorical | pl.Enum):
        return "str"
    if dtype.is_integer():
        return "int"
    if dtype.is_float():
        return "float"
    return str(dtype)


def _expected_columns(frame: str) -> dict[str, str]:
    if frame in {"telemetry", "car_data"}:
        return FASTF1_TELEMETRY_COLUMNS
    table = {
        "laps": fastf1.core.Laps,
        "results": fastf1.core.SessionResults,
        "schedule": fastf1.events.EventSchedule,
    }[frame]._COLUMNS
    return {column: _fastf1_family(spec) for column, spec in table.items()}


class _BuildFailure(NamedTuple):
    error: BaseException


#: Session surfaces built from the offline session, by key.
SURFACES: dict[str, str] = {
    "laps": "session.laps",
    "fastest_lap": 'session.laps.pick_drivers("PIA").pick_fastest()',
    "telemetry": 'session.laps.pick_drivers("PIA").pick_fastest().get_telemetry()',
    "car_data": 'session.car_data["81"]',
    "results": "session.results",
    "driver": 'session.get_driver("PIA")',
    "schedule": "tif1.get_event_schedule(2024)",
}

#: The tif1 class each surface must be, per backend (fastf1's class on pandas).
SURFACE_CLASSES: dict[str, dict[str, str]] = {
    "pandas": {
        "laps": "tif1.models.Laps",
        "fastest_lap": "tif1.models.Lap",
        "telemetry": "tif1.models.Telemetry",
        "car_data": "tif1.models.Telemetry",
        "results": "tif1.models.SessionResults",
        "driver": "tif1.models.DriverResult",
        "schedule": "tif1.events.EventSchedule",
    },
    "polars": {
        "laps": "tif1.models_polars.PolarsLaps",
        "fastest_lap": "tif1.models_polars.PolarsLap",
        "telemetry": "tif1.models_polars.PolarsTelemetry",
        "car_data": "tif1.models_polars.PolarsTelemetry",
        "results": "tif1.models_polars.PolarsSessionResults",
        "driver": "tif1.models_polars.PolarsDriver",
    },
}

#: (backend, frame) pairs checked against fastf1's column tables.
FRAMES = [
    *(("pandas", frame) for frame in ("laps", "telemetry", "car_data", "results", "schedule")),
    *(("polars", frame) for frame in ("laps", "telemetry", "car_data", "results")),
]


def _laptimes(code: str, number: str, offset: float) -> dict[str, list[Any]]:
    """Three laps of a ``{driver}/laptimes.json`` payload (raw CDN keys)."""
    start = 3600.0 + offset
    return {
        "time": [None, 95.2, 94.8],
        "lap": [1, 2, 3],
        "compound": ["MEDIUM", "MEDIUM", "MEDIUM"],
        "stint": [1, 1, 1],
        "s1": [None, 30.1, 29.9],
        "s2": [35.0, 35.1, 34.9],
        "s3": [30.0, 30.0, 30.0],
        "life": [1, 2, 3],
        "pos": [2, 1, 1],
        "status": ["1", "1", "12"],
        "pb": [False, True, True],
        "sesT": [start + 95.0, start + 190.2, start + 285.0],
        "drv": [code] * 3,
        "dNum": [number] * 3,
        "pout": [start - 600.0, None, None],
        "pin": [None, None, None],
        "s1T": [None, start + 125.3, start + 220.0],
        "s2T": [start + 65.0, start + 160.2, start + 255.0],
        "s3T": [start + 95.0, start + 190.2, start + 285.0],
        "vi1": [262.0, 294.0, 290.0],
        "vi2": [253.0, 322.0, 320.0],
        "vfl": [219.0, 212.0, 218.0],
        "vst": [293.0, 329.0, 330.0],
        "fresh": [True, True, True],
        "team": ["McLaren"] * 3,
        "lST": [start, start + 95.0, start + 190.2],
        "lSD": [
            "2025-03-23T07:00:00.000000000",
            "2025-03-23T07:01:35.000000000",
            "2025-03-23T07:03:10.200000000",
        ],
        "del": [False, False, False],
        # No deleted lap: the CDN stores null reasons, so DeletedReason is an
        # all-null column and must still come out str-typed.
        "delR": [None, None, None],
        "ff1G": [False, False, False],
        "iacc": [False, True, True],
        "wT": [3590.0, 3650.0, 3710.0],
        "wAT": [27.7, 27.9, 27.9],
        "wH": [45.0, 46.0, 45.0],
        "wP": [1017.1, 1017.2, 1017.2],
        "wR": [False, False, False],
        "wTT": [34.9, 34.0, 34.0],
        "wWD": [222, 231, 239],
        "wWS": [1.5, 1.1, 1.1],
    }


_TELEMETRY_PAYLOAD = {
    "tel": {
        "time": [0.0, 0.25, 0.5, 0.75, 1.0],
        "rpm": [10000.0, 10500.0, 11000.0, 11500.0, 12000.0],
        "speed": [200.0, 210.0, 220.0, 230.0, 240.0],
        "gear": [6, 6, 7, 7, 8],
        "throttle": [100.0] * 5,
        "brake": [False] * 5,
        "drs": [0, 0, 1, 1, 1],
        "distance": [0.0, 14.0, 29.0, 45.0, 62.0],
        "rel_distance": [0.0, 0.002, 0.005, 0.008, 0.011],
        "DriverAhead": ["4"] * 5,
        "DistanceToDriverAhead": [10.0, 11.0, 12.0, 13.0, 14.0],
        "x": [1.0, 2.0, 3.0, 4.0, 5.0],
        "y": [1.0] * 5,
        "z": [0.0] * 5,
        "acc_x": [0.1] * 5,
        "acc_y": [0.0] * 5,
        "acc_z": [0.0] * 5,
    }
}

_DRIVERS_PAYLOAD = {
    "drivers": [
        {
            "driver": code,
            "team": "McLaren",
            "dn": number,
            "fn": first,
            "ln": last,
            "tc": "FF8000",
            "url": "",
        }
        for code, number, first, last in (
            ("PIA", "81", "Oscar", "Piastri"),
            ("NOR", "4", "Lando", "Norris"),
        )
    ]
}


def _session_payloads() -> dict[str, Any]:
    return {
        "drivers.json": _DRIVERS_PAYLOAD,
        "PIA/laptimes.json": _laptimes("PIA", "81", 0.0),
        "NOR/laptimes.json": _laptimes("NOR", "4", 1.0),
        "_tel.json": _TELEMETRY_PAYLOAD,
    }


def _build_surfaces(backend: str, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Every surface of an offline 2025 Chinese GP Race session (results from the fixtures)."""
    from tif1 import async_fetch
    from tif1 import core as tif1_core
    from tif1.payload_loader import InMemoryTransport

    payloads = _session_payloads()

    def resolve(path: str) -> Any:
        return payloads["_tel.json"] if path.endswith("_tel.json") else payloads.get(path)

    async def fetch_multiple(requests: list[tuple[int, str, str, str]], **_: Any) -> list[Any]:
        return [resolve(path) for _year, _gp, _session, path in requests]

    monkeypatch.setattr(tif1_core, "fetch_multiple_async", fetch_multiple)
    monkeypatch.setattr(async_fetch, "fetch_multiple_async", fetch_multiple)

    built: dict[str, Any] = {}

    def attempt(key: str, build: Callable[[], Any]) -> None:
        try:
            built[key] = build()
        except Exception as error:  # reported by the test that reads the surface
            built[key] = _BuildFailure(error)

    def need(key: str) -> Any:
        value = built[key]
        if isinstance(value, _BuildFailure):
            raise RuntimeError(f"{SURFACES[key]} failed: {value.error!r}")
        return value

    session = tif1.get_session(2025, "Chinese Grand Prix", "Race", enable_cache=False, lib=backend)
    session._payload_loader.transport = InMemoryTransport(payloads)
    attempt("laps", lambda: session.laps)
    attempt("fastest_lap", lambda: need("laps").pick_drivers("PIA").pick_fastest())
    attempt("telemetry", lambda: need("fastest_lap").get_telemetry())
    attempt("car_data", lambda: session.car_data["81"])
    attempt("results", lambda: session.results)
    attempt("driver", lambda: session.get_driver("PIA"))
    if backend == "pandas":
        attempt("schedule", lambda: tif1.get_event_schedule(2024))
    return built


_SURFACE_CACHE: dict[str, dict[str, Any]] = {}


@pytest.fixture
def surface(request: pytest.FixtureRequest) -> Callable[[str, str], Any]:
    """``surface(backend, key)``: the built session surface (built once per backend)."""

    def get(backend: str, key: str) -> Any:
        if backend not in _SURFACE_CACHE:
            request.getfixturevalue("ergast_offline")
            _SURFACE_CACHE[backend] = _build_surfaces(
                backend, request.getfixturevalue("monkeypatch")
            )
        value = _SURFACE_CACHE[backend][key]
        if isinstance(value, _BuildFailure):
            pytest.fail(f"{backend}: {SURFACES[key]} raised {value.error!r}")
        return value

    return get


def _backend_param(backend: str, *values: str) -> Any:
    marks = needs_polars if backend == "polars" else ()
    return pytest.param(backend, *values, id="-".join((backend, *values)), marks=marks)


def test_telemetry_table_matches_fastf1_channels():
    channels = set(fastf1.core.Telemetry._CHANNELS) - TELEMETRY_CHANNELS_NOT_EMITTED
    assert set(FASTF1_TELEMETRY_COLUMNS) == channels, (
        "fastf1's Telemetry._CHANNELS changed: update FASTF1_TELEMETRY_COLUMNS"
    )


@pytest.mark.parametrize(
    ("backend", "key"),
    [
        _backend_param(backend, key)
        for backend, classes in SURFACE_CLASSES.items()
        for key in classes
    ],
)
def test_session_surface_class(surface, backend, key):
    value = surface(backend, key)
    expected = _resolve(SURFACE_CLASSES[backend][key])
    assert isinstance(value, expected), (
        f"{backend}: {SURFACES[key]} is {type(value).__module__}.{type(value).__qualname__}, "
        f"expected {expected.__module__}.{expected.__qualname__}"
    )


@pytest.mark.parametrize(
    ("backend", "frame"), [_backend_param(backend, frame) for backend, frame in FRAMES]
)
def test_frame_has_fastf1_columns(surface, backend, frame):
    columns = list(surface(backend, frame).columns)
    missing = [column for column in _expected_columns(frame) if column not in columns]
    assert not missing, f"{backend}: {SURFACES[frame]} lacks fastf1 columns: {missing}"


@pytest.mark.parametrize(
    ("backend", "frame"), [_backend_param(backend, frame) for backend, frame in FRAMES]
)
def test_frame_columns_follow_fastf1_order(surface, backend, frame):
    expected = _expected_columns(frame)
    columns = list(surface(backend, frame).columns)
    ours = [column for column in columns if column in expected]
    theirs = [column for column in expected if column in columns]
    assert ours == theirs, (
        f"{backend}: {SURFACES[frame]} orders fastf1's columns as {ours}, fastf1 as {theirs}"
    )


@pytest.mark.parametrize(
    ("backend", "frame"), [_backend_param(backend, frame) for backend, frame in FRAMES]
)
def test_frame_dtype_families(surface, backend, frame):
    value = surface(backend, frame)
    if backend == "polars":
        import polars as pl

        assert isinstance(value, pl.DataFrame), (
            f"polars: {SURFACES[frame]} is a {type(value).__module__}.{type(value).__name__}, "
            "not a polars frame"
        )
        families = {name: _polars_family(dtype) for name, dtype in value.schema.items()}
    else:
        families = {name: _pandas_family(dtype) for name, dtype in value.dtypes.items()}
    deviations = DEVIATIONS.get((backend, frame), {})
    mismatches = []
    for column, family in _expected_columns(frame).items():
        if column not in families:
            continue  # reported by test_frame_has_fastf1_columns
        accepted = _ACCEPTED[family] | deviations.get(column, set())
        if families[column] not in accepted:
            mismatches.append(f"{column}: {families[column]} (fastf1: {family})")
    assert not mismatches, f"{backend}: {SURFACES[frame]} dtype families differ: " + "; ".join(
        mismatches
    )
