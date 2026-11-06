# N10 intended commits (`git commit` is denied in this sandbox; everything is staged)

Two commits, in this order. The split is by file, so the integrator can take
commit 1 alone.

## Commit 1: `perf(naxos-N10): promote the survivors of GC-suspended frame bursts to the oldest generation`

Files:

- `src/tif1/core_utils/gc_control.py` (new)
- `src/tif1/cache.py`
- `src/tif1/core.py`
- `tests/unit/test_naxos_n10_gc_promote.py` (new)
- `tools/perf/naxos_n10_profile.py`, `naxos_n10_stages.py`, `naxos_n10_steps.py`,
  `naxos_n10_gc.py`, `naxos_n10_gc_ab.py`, `naxos_n10_gc_promote_micro.py`

Message body:

```
The warm frame-tier reads build a race's 1128 lap frames under _suspend_gc.
The first young collection afterwards traversed all ~55 000 of their
containers and freed nothing: a 9-10 ms gen-1 pass inside
fetch_all_laps_telemetry().

suspend_gc(promote=True) now ends the read-side bursts with
gc.freeze(); gc.unfreeze(). On GIL builds of CPython 3.11-3.13 that is an
O(1) splice into the oldest generation. It is skipped:
- below the young-collection threshold;
- when the process froze objects itself;
- on 3.14+ and on free-threaded builds.

The GC helpers move to core_utils/gc_control.py. cache._suspend_gc is the
same function.
```

## Commit 2: `perf(naxos-N10): import pandas, polars and tif1.core without young GC collections`

Files:

- `src/tif1/core_utils/lazy_import.py` (`import_promoted`; `LazyModule` loads through it)
- `src/tif1/core_utils/runtime.py` (`import_supported_polars`)
- `src/tif1/__init__.py` (`__getattr__` exports)
- `tests/unit/test_naxos_n10_import_gc.py` (new)
- `tests/unit/test_exit_gc_freeze.py`: the spy counts only `gc.freeze` calls
  made after exit began, because tif1's imports now call it at runtime.
- `tools/perf/naxos_n10_gc_phases.py`, `naxos_n10_import_gc.py`, `naxos_n10_ab_table.py`,
  plus the remaining `naxos_n10_*` probes
- `.agents/perf-naxos/N10*` (report, A/B JSONs, progress; the parity digests stay in `/tmp/naxos/`)

Message body:

```
import pandas creates ~150 000 long-lived module objects. With cyclic GC
on, the import ran ~50 young collections that freed nothing, and it left
the objects for the collections after it to traverse again: 26 ms of GC
inside get_session() on pandas, 11 ms on polars.

The first import of a session's library now runs under
suspend_gc(promote=True):
- LazyModule (pandas, numpy, tif1.models), through import_promoted;
- import_supported_polars;
- tif1.__getattr__.

GC inside get_session() drops to 0 ms. car_data_loop GC drops 4.4 -> 0.6 ms,
and the notebook's iterlaps loop GC 11.3 -> 5.7 ms.
```
