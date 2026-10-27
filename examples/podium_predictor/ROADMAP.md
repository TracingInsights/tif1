# Podium Predictor Roadmap

The objective is a reproducible example with measured forecast performance.
More features do not establish better forecasts.

## Included Methods

- [x] Race-grouped ranking and consistent ordered-podium probabilities.
- [x] Chronological feature and recency selection, followed by temperature calibration.
- [x] Practice, sprint, and archived forecast inputs with cutoff audits.
- [x] Separate historical evaluation from an upcoming Austin forecast.
- [x] Synthetic regression tests for chronology, probabilities, caching, and reports.
- [x] Preserve the original 17-race development benchmark and unsuccessful comparisons.

The original benchmark predates the `tif1` adapter. It is not evidence that
the integrated version produces identical results.

## Integration Validation

- [ ] Run the complete chronological benchmark through the `tif1` session adapter.
- [ ] Compare session feature values with the previous adapter on the same inputs.
- [ ] Record library and model versions, source hashes, missing inputs, and data cutoffs.
- [ ] Publish new measurements separately from `benchmarks_2026.json`.
- [ ] Expand the optional model dependency matrix beyond the tested configurations.

## Forecast Development

- [ ] Record predictions before future sessions and score them without later changes.
- [ ] Evaluate calibration and podium accuracy over more unseen races.
- [ ] Select any circuit or round for live predictions instead of only Austin.
- [ ] Split shared data and feature code into smaller modules.
- [ ] Improve long-run comparisons with traffic and stint context where available.
- [ ] Test shared retirement, weather, and strategy risks in race simulations.

## Evidence for Each Change

Record the data cutoff, features, training window, calibration period, and
evaluation races. Compare against the same baseline on the same races.
Report winner accuracy, unordered and ordered podium hit rates, mean correct
podium drivers, Brier scores, and uncertainty.

Keep tuning results separate from new prospective results. Repeated tuning
on the same small evaluation set does not provide new independent evidence.
