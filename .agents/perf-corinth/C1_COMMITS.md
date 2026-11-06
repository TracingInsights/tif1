# C1 intended commit (git commit was denied in this sandbox; everything is staged)

```
perf(corinth-C1): next-round lookahead for season loops

get_session() for round r right after round r-1 (same season, session and
schedule backend) fetches round r+1's session files (the ones round r-1
fetched over the network) and the Stats classification into first-wave
slots over the kept-alive stdlib connections; the next load takes them
through first_wave.pending/take. One round ahead, nothing for cached or
future sessions, never telemetry; session_lookahead /
TIF1_SESSION_LOOKAHEAD turns it off.

Cold 24-race loop (laps + results): pandas wall 4236 -> 2519 ms (5/5),
polars 4679 -> 3034 ms (5/5); parity 0/133, 0/131.
```
