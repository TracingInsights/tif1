#!/usr/bin/env bash
# Generic interleaved A/B harness: each arm runs in a fresh subprocess.
#   ab.sh <workload.py> <rounds>
# Arm A (baseline): PYTHONPATH=/tmp/kroton-baseline/src
# Arm B (candidate): main workspace src (uncommitted changes)
set -u
WORKLOAD="$1"
ROUNDS="${2:-5}"
MAIN=/tmp/hoplite/workspace
BASE=/tmp/kroton-baseline
PY="$MAIN/.venv/bin/python"

run_arm() {
  local srcdir="$1"
  PYTHONPATH="$srcdir" "$PY" "$MAIN/$WORKLOAD" 2>/dev/null | grep -oE '[A-Z_]+ [0-9.]+' | tr '\n' ' '
  echo
}

echo "round A B"
for i in $(seq "$ROUNDS"); do
  a=$(run_arm "$BASE/src")
  b=$(run_arm "$MAIN/src")
  echo "$i $a $b"
done
