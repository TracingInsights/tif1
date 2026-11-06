# C7 intended commit (git commit was denied; everything is staged)

```
perf(corinth-C7): CLI session tables without pandas; dist-info version lookup

results/drivers/fastest load polars sessions when polars is installed and
print the same tables (int-nanosecond durations summed as
Timedelta.total_seconds, pandas' quicksort order for tied fastest laps);
standings prints the standings rows without a frame; tif1.__version__ and
the User-Agent read the dist-info Version header instead of importing
importlib.metadata. Warm session commands 0.31-0.56x, cold 0.56-0.70x,
tif1 version 0.85x; CLI output identical on 209 command lines, session
digests 0/133 and 0/131. Fixes tif1 fastest crashing on polars configs.
```
