"""Every private upstream API tif1's fast paths rely on, and proof each path is active.

tif1 reaches into pandas, polars, niquests, qh3 and CPython internals where the
public API costs measurable time (each seam names the round that added it).
Every one of them probes or guards the private surface and falls back to the
public route when it drifted, so an upstream release cannot break results;
it can silently put a hot path back on the slow route instead. These tests
assert, seam by seam, that the fast route is the one taken on the installed
versions, so a drift fails here under the seam's name rather than as a
vague perf regression or a user report.

``SEAMS`` records the upstream version each seam was verified against (the
version ``uv.lock`` pins; ``test_seam_table_follows_the_lock`` fails on a lock
bump until the table is re-verified and updated). The weekly upstream canary
(``.github/workflows/upstream-canary.yml``) runs this file against the newest
releases and pre-releases of the packages named here.
"""

from __future__ import annotations

import importlib.machinery
import importlib.metadata
import importlib.util
import subprocess
import sys
import threading
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tif1 import cache as cache_mod
from tif1 import models, ocsp_cache
from tif1.core_utils import frame_codec, pandas_blocks

_REPO = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Seam:
    """One private upstream surface used by a tif1 fast path."""

    name: str
    #: Distribution owning the surface (``python`` for CPython itself).
    package: str
    #: Version the fast path was verified against (``uv.lock``'s pin).
    verified: str
    #: Where tif1 uses it.
    where: str
    #: The private names involved.
    surface: str
    #: What runs when the surface drifts.
    fallback: str
    #: The test below that proves the fast path is active.
    test: str


SEAMS: tuple[Seam, ...] = (
    Seam(
        name="pandas-direct-blocks",
        package="pandas",
        verified="3.1.0",
        where="core_utils/pandas_blocks.py (Thebes TH2, Pergamon P1), frame_codec.encode_frame",
        surface=(
            "pandas.core.internals.BlockManager, blocks.new_block and the block classes, "
            "pandas._libs.internals.BlockPlacement, construction.ensure_wrapped_if_datetimelike, "
            "DataFrame._from_mgr, IntegerArray/TimedeltaArray/DatetimeArray._simple_new, "
            "DataFrame._from_arrays"
        ),
        fallback=(
            "DataFrame._from_arrays for every typed frame build (the public constructor "
            "without it), and no pandas frame tier (encode_frame returns None)"
        ),
        test="test_pandas_direct_blocks",
    ),
    Seam(
        name="pandas-lap-frames",
        package="pandas",
        verified="3.1.0",
        where="core_utils/frame_codec.py _LapBuilder (Pergamon Q18)",
        surface=(
            "ArrowStringArray._pa_array/_dtype/_readonly built by object.__new__, "
            "NDArrayBacked._simple_new + TimedeltaArray._freq"
        ),
        fallback="ExtensionArray.__getitem__ / TimedeltaArray._simple_new per lap",
        test="test_pandas_lap_frames",
    ),
    Seam(
        name="pandas-arrow-strings",
        package="pandas",
        verified="3.1.0",
        where="core_utils/frame_codec.py _str_dictionary and the span concatenation",
        surface=(
            "ArrowStringArray._pa_array, ArrowExtensionArray._from_pyarrow_array, "
            "ExtensionArray._concat_same_type"
        ),
        fallback="object-array dictionary encoding (astype(object))",
        test="test_pandas_arrow_strings",
    ),
    Seam(
        name="pandas-lap-rows",
        package="pandas",
        verified="3.1.0",
        where=(
            "models._interleaved_row/_lap_from_row/_object_row_placement/Laps._ixs "
            "(Pergamon Q7, Elis E7, Naxos N6)"
        ),
        surface=(
            "DataFrame._ixs, Series._from_mgr, SingleBlockManager.from_array, "
            "SingleBlockManager(block, index), NumpyBlock(values, placement, 1), "
            "construction.sanitize_array, _libs.lib.maybe_convert_objects (keeps a "
            "row holding a number or bool as object), managers.interleaved_dtype, "
            "blocks.ExtensionBlock, _libs.tslibs.timedeltas.ints_to_pytimedelta"
        ),
        fallback=(
            "pandas' fast_xs per Laps.iloc[i] / results row and the Series constructor; "
            "iterlaps infers every row (_probe_object_rows False)"
        ),
        test="test_pandas_lap_rows",
    ),
    Seam(
        name="pandas-constructor-hooks",
        package="pandas",
        verified="3.1.0",
        where="models.Laps/Telemetry/SessionResults._constructor(_sliced)_from_mgr (Q4)",
        surface="DataFrame._constructor_from_mgr / _constructor_sliced_from_mgr hooks",
        fallback="Laps(df)/Telemetry(df)/Lap(ser): a shallow manager copy per selection",
        test="test_pandas_constructor_hooks",
    ),
    Seam(
        name="polars-ipc-stream-reader",
        package="polars",
        verified="2.1.0",
        where="cache._read_polars_ipc_row (Elis E6)",
        surface="polars._plr.PyDataFrame.read_ipc_stream (positional signature)",
        fallback="pl.read_ipc (a query plan per row, ~2x slower, the first ~7 ms)",
        test="test_polars_ipc_stream_reader",
    ),
    Seam(
        name="polars-pydf-wrap",
        package="polars",
        verified="2.1.0",
        where="models_polars (cloned _df gathers), cache, ergast.interface ErgastPolarsResponse",
        surface="DataFrame._df, DataFrame._from_pydf (and polars calling cls._from_pydf)",
        fallback="none (AttributeError): the polars hot paths depend on it",
        test="test_polars_pydf_wrap",
    ),
    Seam(
        name="niquests-ocsp-pickling",
        package="niquests",
        verified="3.21.2",
        where="ocsp_cache.seed/persist (Elis E3)",
        surface=(
            "niquests.extensions.revocation._ocsp (InMemoryRevocationStatus pickling state, "
            "OCSPResponse, Certificate), Session._ocsp_cache and the adapters' _ocsp_cache"
        ),
        fallback="no persisted verdicts: a responder round trip (~100 ms) per cold process",
        test="test_niquests_ocsp_pickling",
    ),
    Seam(
        name="qh3-hazmat-preload",
        package="qh3",
        verified="2.1.0",
        where="ocsp_cache.preload_hazmat (Elis E3)",
        surface="the qh3/_hazmat extension module loaded without the qh3 package __init__",
        fallback="niquests imports the whole QUIC stack for the OCSP check (~30 ms)",
        test="test_qh3_hazmat_preload",
    ),
    Seam(
        name="threading-register-atexit",
        package="python",
        verified="3.11-3.15",
        where="cache._register_prefetch_exit_hook, core_utils/telemetry_stream (Elis E10)",
        surface="threading._register_atexit (runs before concurrent.futures joins its pools)",
        fallback="atexit.register: queued reads and requests finish before the process exits",
        test="test_threading_register_atexit",
    ),
)


def _note(package: str) -> str:
    """Which version a failure ran against, next to the one the seam was verified on."""
    verified = sorted({seam.verified for seam in SEAMS if seam.package == package})
    if package == "python":
        installed = ".".join(map(str, sys.version_info[:3]))
    else:
        try:
            installed = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            installed = "not installed"
    return f"{package} {installed} installed, seam verified against {', '.join(verified)}"


def _locked_versions() -> dict[str, set[str]]:
    lock = tomllib.loads((_REPO / "uv.lock").read_text(encoding="utf-8"))
    versions: dict[str, set[str]] = {}
    for package in lock.get("package", []):
        versions.setdefault(package["name"], set()).add(package["version"])
    return versions


# -- the table ---------------------------------------------------------------------


def test_every_seam_has_its_test():
    names = [seam.name for seam in SEAMS]
    assert len(names) == len(set(names))
    module = sys.modules[__name__]
    for seam in SEAMS:
        assert callable(getattr(module, seam.test, None)), seam.name


def test_seam_table_follows_the_lock():
    """A lock bump of a seam's package must come with a re-verified table entry."""
    locked = _locked_versions()
    for seam in SEAMS:
        if seam.package == "python":
            continue
        assert seam.verified in locked.get(seam.package, set()), (
            f"{seam.name}: verified against {seam.package} {seam.verified}, uv.lock pins "
            f"{sorted(locked.get(seam.package, set()))}; run this file on the new lock and "
            "update SEAMS"
        )


def test_the_canary_installs_every_seam_package():
    workflow = (_REPO / ".github" / "workflows" / "upstream-canary.yml").read_text(encoding="utf-8")
    for package in {seam.package for seam in SEAMS} - {"python"}:
        assert package in workflow, package


# -- pandas ------------------------------------------------------------------------


def _typed_arrays() -> tuple[list[Any], list[str]]:
    """A small telemetry-shaped frame's columns: every block kind the builders lay out."""
    columns = ["Time", "SessionTime", "RPM", "Speed", "nGear", "Brake", "DriverAhead", "Source"]
    arrays: list[Any] = [
        np.array([0, 16_000_000, 256_000_000], dtype="m8[ns]"),
        np.array([1_000_000_000, 1_016_000_000, 1_256_000_000], dtype="m8[ns]"),
        np.array([10000.0, 10500.0, np.nan]),
        np.array([250.0, 251.5, 252.0]),
        pd.arrays.IntegerArray(np.array([7, 8, 8]), np.zeros(3, dtype=bool)),
        np.array([False, True, False]),
        pd.Series(["1", None, "44"]).array,
        np.full(3, "car", dtype=object),
    ]
    return arrays, columns


def test_pandas_direct_blocks():
    note = _note("pandas")
    assert callable(getattr(pd.DataFrame, "_from_arrays", None)), f"_from_arrays gone ({note})"
    assert pandas_blocks._DIRECT_BLOCKS_AVAILABLE, f"private block API missing ({note})"
    assert pandas_blocks._DIRECT_BLOCK_CLASSES_OK, f"block classes/_simple_new drifted ({note})"
    assert pandas_blocks._probe_direct_blocks(), f"direct block route != public route ({note})"
    arrays, columns = _typed_arrays()
    direct = pandas_blocks._direct_frame_from_columns(arrays, columns, 3)
    public = pandas_blocks._frame_from_arrays(arrays, columns, 3)
    pd.testing.assert_frame_equal(direct, public)
    # The pandas frame tier encodes through the same probe; ``None`` means no tier.
    blob = frame_codec.encode_frame(public)
    assert blob is not None, f"encode_frame fell back: no pandas frame tier ({note})"
    pd.testing.assert_frame_equal(frame_codec.decode_frame(blob), public)


def test_pandas_lap_frames():
    note = _note("pandas")
    assert frame_codec._probe_str_slice(), f"ArrowStringArray slice internals drifted ({note})"
    assert frame_codec._probe_timedelta(), f"TimedeltaArray._simple_new drifted ({note})"
    assert frame_codec._fast_lap_frames(), f"lap frame block route off ({note})"
    arrays, columns = _typed_arrays()
    laps = []
    for lap in (1, 2):
        frame = pd.DataFrame(dict(zip(columns, arrays, strict=True)))
        frame["LapNumber"] = pd.array([lap] * 3, dtype="Int64")
        laps.append((lap, frame))
    blob, leftovers = frame_codec.encode_driver_frames(laps)
    assert blob is not None, note
    assert leftovers == [], note
    decoded = frame_codec.decode_driver_frames(bytearray(blob))
    for lap, frame in laps:
        pd.testing.assert_frame_equal(decoded[lap], frame)


def test_pandas_arrow_strings():
    note = _note("pandas")
    ea = pd.Series(["a", None, "bb", "a"]).array
    assert isinstance(ea, pd.arrays.ArrowStringArray), note
    chunked = getattr(ea, "_pa_array", None)
    assert hasattr(chunked, "num_chunks"), f"_pa_array gone ({note})"
    assert callable(getattr(ea, "_from_pyarrow_array", None)), note
    assert callable(getattr(type(ea), "_concat_same_type", None)), note
    codes, cats = frame_codec._str_dictionary(ea)
    assert cats == ["a", "bb"], note
    assert codes.tolist() == [0, -1, 1, 0], note
    rebuilt = ea._from_pyarrow_array(chunked)
    assert type(rebuilt) is type(ea), note
    assert rebuilt.dtype == ea.dtype, note
    assert rebuilt.tolist() == ea.tolist(), note


def _laps_frame() -> pd.DataFrame:
    """Several blocks interleaving to object, a 2-column timedelta block among them."""
    return models.Laps(
        {
            "Time": pd.to_timedelta([90.0, 181.5, np.nan], unit="s"),
            "LapTime": pd.to_timedelta([90.0, 91.5, 92.0], unit="s"),
            "Driver": ["VER", "VER", "VER"],
            "LapNumber": [1.0, 2.0, 3.0],
            "Position": pd.array([1, 1, None], dtype="Int64"),
            "IsPersonalBest": [True, False, False],
        }
    )


def test_pandas_lap_rows(monkeypatch):
    note = _note("pandas")
    assert models._FAST_LAP_ROWS, f"private row API missing ({note})"
    laps = _laps_frame()
    row = models._interleaved_row(laps._mgr, 1)
    assert row is not None, f"_interleaved_row fell back to pandas ({note})"
    public = pd.DataFrame(laps).iloc[1]
    assert row.tolist() == public.tolist(), note

    calls: list[int] = []
    real_ixs = models.Laps._ixs

    def spy(self: Any, i: int, axis: int = 0) -> Any:
        calls.append(i)
        return real_ixs(self, i, axis)

    monkeypatch.setattr(models.Laps, "_ixs", spy)
    lap = laps.iloc[2]
    assert calls == [2], f"Laps.iloc[int] no longer goes through _ixs ({note})"
    assert type(lap) is models.Lap
    pd.testing.assert_series_equal(lap, pd.DataFrame(laps).iloc[2], check_series_type=False)

    values = np.array([pd.Timedelta(seconds=90), "VER", 1.0], dtype=object)
    index = pd.Index(["LapTime", "Driver", "LapNumber"])
    fast = models._lap_from_row(values.copy(), index, 7, None)
    reference = models.Lap(values.copy(), index=index, name=7)
    pd.testing.assert_series_equal(fast, reference)

    # Naxos N6: rows holding a number or bool skip the inference.
    assert models._probe_object_rows(), f"object-row inference rule drifted ({note})"
    placement = models._object_row_placement(laps)
    assert placement is not None, f"iterlaps fell back to the row inference ({note})"
    row_placement = models._BlockPlacement(slice(0, len(index)))
    direct = models._lap_from_row(values.copy(), index, 7, None, row_placement)
    pd.testing.assert_series_equal(direct, reference)


def test_pandas_constructor_hooks(monkeypatch):
    note = _note("pandas")
    calls: list[str] = []

    def spy(cls: type, hook: str) -> None:
        real = getattr(cls, hook)

        def wrapper(self: Any, mgr: Any, axes: Any) -> Any:
            calls.append(f"{cls.__name__}.{hook}")
            return real(self, mgr, axes)

        monkeypatch.setattr(cls, hook, wrapper)

    for cls in (models.Laps, models.Telemetry, models.SessionResults):
        spy(cls, "_constructor_from_mgr")
        spy(cls, "_constructor_sliced_from_mgr")

    laps = _laps_frame()
    assert type(laps[laps["LapNumber"] > 1.0]) is models.Laps
    assert type(laps["LapTime"]) is models.Lap
    telemetry = models.Telemetry({"Speed": [1.0, 2.0, 3.0], "RPM": [1.0, 2.0, 3.0]})
    assert type(telemetry.iloc[:2]) is models.Telemetry
    assert telemetry["Speed"].tolist() == [1.0, 2.0, 3.0]
    results = models.SessionResults({"Abbreviation": ["VER", "NOR"], "Points": [25.0, 18.0]})
    assert type(results.iloc[:1]) is models.SessionResults
    assert type(results["Points"]) is models.DriverResult
    expected = {
        f"{cls}.{hook}"
        for cls in ("Laps", "Telemetry", "SessionResults")
        for hook in ("_constructor_from_mgr", "_constructor_sliced_from_mgr")
    }
    assert expected <= set(calls), f"pandas skipped {sorted(expected - set(calls))} ({note})"


# -- polars ------------------------------------------------------------------------


def test_polars_ipc_stream_reader(monkeypatch):
    pl = pytest.importorskip("polars")
    note = _note("polars")
    assert callable(cache_mod._ipc_stream_reader()), f"PyDataFrame.read_ipc_stream gone ({note})"
    frame = pl.DataFrame(
        {
            "Speed": [200.0, None, 201.5],
            "nGear": [7, 7, 8],
            "DriverAhead": ["81", None, "4"],
            "Source": pl.Series(["car"] * 3, dtype=pl.Categorical),
        }
    )
    blob = cache_mod._write_polars_ipc(frame)

    def no_fallback(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError(f"_read_polars_ipc_row fell back to pl.read_ipc ({note})")

    monkeypatch.setattr(pl, "read_ipc", no_fallback)
    got = cache_mod._read_polars_ipc_row(blob)
    assert got.equals(frame), note
    assert got.schema == frame.schema, note


def test_polars_pydf_wrap():
    pl = pytest.importorskip("polars")
    note = _note("polars")
    frame = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", None]})
    clone = pl.DataFrame._from_pydf(frame._df.clone())
    assert type(clone) is pl.DataFrame, note
    assert clone.equals(frame), note
    clone = clone.with_columns(pl.Series("a", [9, 9, 9]))
    assert frame["a"].to_list() == [1, 2, 3], note
    from tif1.ergast.interface import _polars_response_type

    response_type: Any = _polars_response_type()
    response = response_type._from_pydf(frame._df)
    assert type(response) is pl.DataFrame, f"derived frames keep the response class ({note})"


# -- niquests / qh3 ----------------------------------------------------------------


def test_niquests_ocsp_pickling():
    pytest.importorskip("niquests")
    note = _note("niquests")
    module = ocsp_cache._ocsp_module()
    assert module is not None, f"niquests' OCSP module or its pickling support is gone ({note})"
    for name in ("OCSPResponse", "Certificate", "OCSPCertStatus", "OCSPResponseStatus"):
        assert hasattr(module, name), f"{name} ({note})"
    assert callable(getattr(module.OCSPResponse, "deserialize", None)), note
    assert callable(getattr(module.OCSPResponse, "authenticate_for", None)), note
    assert callable(getattr(module.Certificate, "deserialize", None)), note
    state = module.InMemoryRevocationStatus().__getstate__()
    assert {"_max_size", "_store", "_issuers_map", "_failure_count"} <= set(state), note

    import niquests

    session = niquests.Session()
    try:
        assert hasattr(session, "_ocsp_cache"), f"Session._ocsp_cache renamed ({note})"
        adapters = [a for a in session.adapters.values() if hasattr(a, "_ocsp_cache")]
        assert adapters, f"adapters no longer carry _ocsp_cache ({note})"
    finally:
        session.close()


def test_qh3_hazmat_preload():
    pytest.importorskip("niquests")
    note = _note("qh3")
    spec = importlib.util.find_spec("qh3")
    assert spec is not None, note
    assert spec.submodule_search_locations, note
    extensions = [
        Path(location) / f"_hazmat{suffix}"
        for location in spec.submodule_search_locations
        for suffix in importlib.machinery.EXTENSION_SUFFIXES
    ]
    assert any(path.is_file() for path in extensions), f"qh3/_hazmat extension moved ({note})"
    # niquests must read its OCSP types from that module, or the preload saves nothing.
    module = ocsp_cache._ocsp_module()
    assert module is not None, note
    assert module.OCSPResponse.__module__ == "qh3._hazmat", note
    # (test_ocsp_cache.test_preload_hazmat_skips_the_quic_stack checks, in a fresh
    # interpreter, that the preload leaves the qh3 package unimported.)


# -- CPython -----------------------------------------------------------------------


def test_threading_register_atexit():
    note = _note("python")
    assert callable(getattr(threading, "_register_atexit", None)), note
    # The hooks run before ``concurrent.futures`` joins its workers: a hook
    # registered after the pool exists releases a worker that would otherwise
    # block the join (the exit would hang until the timeout).
    code = (
        "import threading\n"
        "from concurrent.futures import ThreadPoolExecutor\n"
        "release = threading.Event()\n"
        "pool = ThreadPoolExecutor(1)\n"
        "pool.submit(release.wait)\n"
        "threading._register_atexit(release.set)\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True, timeout=30)
