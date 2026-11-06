# C6 intended commits (git commit was denied in this sandbox; everything is staged)

## 1. perf(corinth-C6): tif1.plotting imports no matplotlib; plot_driver_laptimes without seaborn

Files: src/tif1/plotting.py, src/tif1/charts/lap_times.py,
tests/unit/test_chart_import_contracts.py, tests/unit/test_charts.py,
tests/unit/test_chart_scatter_parity.py

tif1.plotting no longer imports matplotlib/pyplot (or rapidfuzz) at import:
the color/name/style lookups need neither; setup_mpl and the other helpers
that touch rcParams or legends import matplotlib themselves, and the old
mpl/plt/cycler module attributes resolve lazily. The session mapping reads
whole columns instead of iterrows().

plot_driver_laptimes draws seaborn's compound scatter with the matplotlib
calls seaborn 0.13 makes (same order: axis units, inversion, scatter,
facecolors, labels, legend artists, despine), so it no longer imports
seaborn and scipy.stats (~0.85 s). A unit test compares seaborn's figure
and the new one (artists, axis state, Agg pixels) on seven frames with and
without timple's converter.

## 2. perf(corinth-C6): plotting A/B harness, report

Files: tools/perf/corinth_plot_bench.py, tools/perf/corinth_plot_ab.py,
tools/perf/corinth_plot_phases.py, .agents/perf-corinth/C6*.md,
.agents/perf-corinth/C6_ab_*.json
