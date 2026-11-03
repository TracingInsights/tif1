#!/usr/bin/env bash
# Elis E10: the cold A/B campaign for one backend (one cold bench at a time).
# Usage (from the repo root): bash tools/perf/elis_e10_campaign.sh pandas|polars [reps]
set -u
LIB=${1:-pandas}
REPS=${2:-5}
PY=/home/harish/Documents/tif1/.venv/bin/python
BENCH=tools/perf/elis_e10_cold_bench.py
OUT=.agents/perf-elis
CONTROL=/tmp/tif1-elis-control-e10/src
CANDIDATE=$(pwd)/src

# E1 (control tree) vs E10 (candidate), same workload in both arms.
$PY $BENCH --lib "$LIB" --reps "$REPS" --control-src $CONTROL --candidate-src "$CANDIDATE" \
    --workloads iterlaps_all_lazy,notebook_lazy,notebook --out $OUT/E10_cold_${LIB}_vs_e1.json
# The lazy worst case against the eager load (control tree, eager).
$PY $BENCH --lib "$LIB" --reps "$REPS" --control-src $CONTROL --candidate-src "$CANDIDATE" \
    --workloads iterlaps_all_lazy --control-workload iterlaps_all \
    --out $OUT/E10_cold_${LIB}_lazy_vs_eager_iterlaps.json
# The lazy win against the eager load (control tree, eager).
$PY $BENCH --lib "$LIB" --reps "$REPS" --control-src $CONTROL --candidate-src "$CANDIDATE" \
    --workloads notebook_lazy --control-workload notebook_4drivers \
    --out $OUT/E10_cold_${LIB}_lazy_vs_eager_notebook4.json
