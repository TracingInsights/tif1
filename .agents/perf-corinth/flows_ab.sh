#!/usr/bin/env bash
# Interleaved cold A/B of the survey flows: control vs integ, fresh empty cache per run.
PY=/home/harish/Documents/tif1/.venv/bin/python
export HOME=/tmp/corinth/home-integ
for rep in 1 2 3; do
  for flow in "season pandas" "season polars" "old pandas" "old polars" "ergast pandas" "schedule pandas"; do
    for arm in control integ; do
      d=$(mktemp -d /tmp/corinth/coldcache.XXXX)
      line=$(TIF1_CACHE_DIR=$d PYTHONPATH=/tmp/corinth/$arm/src $PY /tmp/corinth/scripts/survey.py $flow 2>/dev/null | tail -1)
      echo "$rep $arm $line"
      rm -rf "$d"
    done
  done
  for lib in pandas polars; do
    for arm in control integ; do
      d=$(mktemp -d /tmp/corinth/coldcache.XXXX)
      echo "$rep $arm $(TIF1_CACHE_DIR=$d PYTHONPATH=/tmp/corinth/$arm/src $PY /tmp/corinth/scripts/current.py $lib cold 2>/dev/null | tail -1)"
      rm -rf "$d"
    done
  done
done
