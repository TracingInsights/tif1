# D8: intended commits (git commit was denied by the agent sandbox)

Everything is staged on `perf/delphi-D8`. Commit in two steps from
`/tmp/tif1-delphi/D8`:

```bash
git commit src/tif1/cache.py src/tif1/core_utils/frame_codec.py \
    src/tif1/models.py tests/unit/test_pergamon_contracts.py \
    -m "perf(delphi-D8): driver merged telemetry from the memoized decoded row" \
    -m "Laps.telemetry for one driver gathers the merged frame from the DriverFrameSource the session memo already holds (load() or an earlier lap access put it there) instead of reading, decompressing and decoding the frame-tier row again; rows spanning two storage layouts are joined column by column instead of through pd.concat. Without a memoized row the path is the previous throwaway decode with in-place moves (holding the row would cost the merged frame a copy: ~1000 page faults per 4 MB block in a fresh process). A single driver row read by a per-lap access decompresses inline instead of through the pool. Notebook benchmark: driver_merged_x4 76 -> 40 ms (8/8), wall 982 -> 935 ms (7/8); driver_tel without load() unchanged; parity identical."

git commit .agents/perf-delphi \
    -m "perf(delphi-D8): report, A/B results and probes"
```
