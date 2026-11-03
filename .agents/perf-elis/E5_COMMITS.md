# E5: intended commit (git commit was denied in this session; everything is staged)

```
perf(elis-E5): the second lazy driver row reads the session's other rows ahead (pandas)

After a warm load() the first driver a script touches reads its row alone
(Delphi D5). The second driver read every remaining lazy row in one batch
(~45-60 ms for a race's 19 rows, all of it waited for); it now reads its
own row alone too and starts reading the others ahead in the pool, in the
laps' driver order, on two serial lanes (Cache.prefetch_telemetry_frame_rows,
_PrefetchedRow): their first laps take the rows read meanwhile, and a row no
lane has started is read by the caller instead of waiting behind the queue.
A second driver that follows the first in the laps' order (a loop over the
laps or the drivers) keeps the D5 batch, which beats reading ahead beside
the lap builds. Queued reads are cancelled by Cache.close() and by a
threading exit hook that runs before the pool joins its workers, so the
exit waits for at most the rows in flight. One-driver scripts run the
control's code.

Tests: tests/unit/test_elis_e5_prefetch.py. Probes and the A/B copy with the
load_then_N_drivers workloads: tools/perf/elis_e5_*.py. Report:
.agents/perf-elis/E5.md.
```

Commit with `git commit -F` after removing the fence lines, or paste the
message body above.
