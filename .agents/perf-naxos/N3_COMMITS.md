# N3: intended commits (`git commit` was denied by the sandbox; everything is staged)

1. `perf(naxos-N3): polars telemetry arithmetic without numpy and without query plans`
   - src/tif1/models_polars.py, tests/unit/test_naxos_n3_polars_arithmetic.py,
     tests/unit/test_polars_without_pandas.py, AGENTS.md
   - Body: Distances on Series arithmetic (full-length divisors) and an
     itertools.accumulate cumsum; fill_missing, slices and merge_channels on
     Series masks, bisect and list kernels (numpy above 200k rows, or 4096 once
     numpy.ma is loaded); resample_channels(rule=) on Series/lists for frames up
     to 20k rows (Kahan bin means, polars' interpolate formula, join_asof's
     last-of-equal rule). Warm polars notebook 716 -> 596 ms (13/16 pairs),
     analysis phase 162 -> 18 ms; parity 0/131.
2. `perf(naxos-N3): harness copies, arithmetic digest and resample ulp probe`
   - tools/perf/naxos_n3_warm_bench.py, naxos_n3_warm_ab.py,
     naxos_n3_arith_digest.py, naxos_n3_resample_ulps.py
3. `perf(naxos-N3): report, A/B results and progress notes`
   - .agents/perf-naxos/N3.md, N3_PROGRESS.md, N3_COMMITS.md, N3_ab_polars*.json
