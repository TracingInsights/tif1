# Experimental Podium Predictor

This research example estimates Formula 1 podium probabilities. It combines
`tif1` session laps, Jolpica race history, and archived Open-Meteo forecasts.
The contribution comes from @fortikday and includes AI-assisted development.

The published benchmark does **not** establish an accuracy improvement over
the previous model or simple baselines. It predates the `tif1` adapter and
has not been rerun through that adapter. This example is not an official
TracingInsights prediction service.

## Scope

- Fit race-grouped XGBoost rankings using strictly earlier races.
- Compare uniform history, 24/48-race half-lives, and a 72-race training window.
- Select features and calibrate probabilities using earlier validation races.
- Produce consistent win, second, third, and podium probabilities through a
  Plackett-Luce distribution.
- Compare practice pace adjusted for tyre compound, tyre age, and session time.
- Use archived weather forecasts available before each prediction cutoff.
- Evaluate three historical stages across completed races.

Live entry construction targets Austin / Circuit of the Americas only.
The 2026 Austin race has ended. Use historical evaluation for that season.
Support for other live targets remains a [roadmap item](ROADMAP.md).

## Install

Use a native Python 3.12 or newer environment. The base `tif1` library supports
Python 3.11, but the optional model dependencies require Python 3.12 or newer.
The example does not support Pyodide. Its dependencies do not change the
installation requirements of the main library.

Create an isolated environment from the repository root:

```sh
python -m venv .venv-podium
```

Activate it on Linux or macOS:

```sh
source .venv-podium/bin/activate
```

Activate it in Windows PowerShell:

```powershell
.venv-podium\Scripts\Activate.ps1
```

On Windows, install the [Microsoft Visual C++ v14 Redistributable](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)
matching your Python architecture before installing XGBoost.
[XGBoost 3.4.1](https://github.com/dmlc/xgboost/blob/v3.4.1/doc/install.rst#python)
requires its runtime DLLs; skip this step if your Visual Studio installation already supplies them.

Install the checkout and the optional dependencies:

```sh
python -m pip install -e ".[dev]"
python -m pip install -r examples/podium_predictor/requirements.txt
```

The requirements file pins the model environment. Dependency pins alone do
not freeze upstream datasets or reproduce historical publication times.

## Test

Run the example tests from the repository root:

```sh
python -m unittest discover -s examples/podium_predictor -p "test_*.py" -v
```

Run the repository integration check in the same activated environment:

```sh
python -m pytest -o addopts='' tests/unit/test_podium_predictor_example.py -v -n 0 --no-cov
```

The optional CI job tests Python 3.12 and 3.13. It installs the model
requirements into the repository's uv environment, then runs:

```sh
uv run --no-sync pytest -o addopts='' tests/unit/test_podium_predictor_example.py -v -n 0 --no-cov
```

Keep `--no-sync` when using that environment. A lockfile sync can remove
the separately installed model dependencies.

The optional job also checks lint and formatting. The explicit configuration
includes this example despite the repository's default example-directory exclusion:

```sh
uv run --no-sync ruff check --config 'extend-exclude=[]' examples/podium_predictor tests/unit/test_podium_predictor_example.py
uv run --no-sync ruff format --check --config 'extend-exclude=[]' examples/podium_predictor tests/unit/test_podium_predictor_example.py
```

The integration check runs the model suite when its optional dependencies are
installed. It also checks the session adapter without the model dependencies.
The tests use synthetic or mocked data and require no downloads. They verify
probability arithmetic, chronology, cutoff handling, caching, and report generation.
They do not demonstrate predictive accuracy.

## Evaluate Completed Races

Run the following commands from `examples/podium_predictor`:

```sh
cd examples/podium_predictor
```

Start with historical evaluation using results through 21 October 2026:

```sh
python cota_predictor_v2.py --season 2026 --eval-year 2026 --mode all --evaluate-only --through-date 2026-10-21
```

The first run downloads race history and eligible session and forecast inputs.
Downloads can be substantial. Later runs reuse local caches.
For a results-only comparison, add `--no-weekend-data`.

Repeat a completed download without network access:

```sh
python cota_predictor_v2.py --season 2026 --eval-year 2026 --mode all --evaluate-only --through-date 2026-10-21 --offline --reuse-validation
```

Offline mode needs the example's cached inputs. Missing race-history caches
fail explicitly. Missing optional session or forecast inputs appear in the
data audit. Validation reuse requires matching code, historical features,
settings, and recorded dependency versions. Version changes invalidate reuse.
Run manifests and forecast metadata record Python and dependency versions.

`--through-date` includes completed results on its boundary date. It requires
`--evaluate-only`. It does not restore older upstream revisions or recreate
the original benchmark environment. A new run after the adapter change is
a new evaluation, even with the same cutoff.

## Session Data and Prediction Cutoffs

The session adapter calls `tif1.get_session(..., lib="pandas")` and reads
`session.laps`. It converts the canonical columns to the model's input format.
The model needs lap timing and stint information, so it does not request
full telemetry. `tif1` handles the session data transport and CDN fallback.
Jolpica supplies race results and calendars. Open-Meteo supplies forecast runs.

| Mode | Information cutoff |
| --- | --- |
| `pre` | 108 hours before the race |
| `practice` | One hour before qualifying; 26 hours before the race when qualifying time is missing |
| `post` | Two hours before the race |
| `auto` | Latest stage whose cutoff has passed |

The model admits practice or sprint sessions three hours after their scheduled
start. It also checks lap-end timestamps against the prediction cutoff.
An eight-hour buffer separates weather initialization from assumed availability.
These buffers are conservative assumptions, not verified publication logs.

For a future Austin race, set `--season` to its year and use `--mode auto`.
Live prediction requires an available calendar and enough training and validation
results. It fails if Austin has already finished.
`--grid` accepts a complete, confirmed driver-code order in `post` mode.
Without that override, the model uses qualifying order.

## Original Development Benchmark

The table below records the evaluation completed before this integration.
Race history ends on 21 October 2026. Selection and calibration use the last
18 races of 2025. The evaluation walks forward over 17 completed 2026 races,
refitting every three races on earlier results.

| Selected model stage | Winner correct | All podium drivers, any order | Exact podium order | Mean correct podium drivers |
| --- | ---: | ---: | ---: | ---: |
| Before practice | 5/17 (29.4%) | 0/17 | 0/17 | 1.24 / 3 |
| Before qualifying | 5/17 (29.4%) | 0/17 | 0/17 | 1.24 / 3 |
| Before race | 12/17 (70.6%) | 3/17 (17.6%) | 1/17 (5.9%) | 1.88 / 3 |

Before the race, the qualifying-order baseline averages 1.82 correct podium
drivers. The previous results-only model averages 1.88. Before practice,
the previous model averages 1.29. Paired race-bootstrap intervals do not
establish an improvement over the simple baselines.

In the original benchmark, calibration improved probability scores relative to
the new model's raw scores.
It does not establish perfectly calibrated probabilities or a predictive edge.
The pre and practice stages select the same core features. The post stage
selects weekend features. This comparison does not isolate their independent benefit.

See [benchmarks_2026.json](benchmarks_2026.json) for the saved comparisons.
This small development benchmark is not a blind prospective experiment.
Source revisions, the session adapter, and dependency changes can affect results.

## Outputs and Development

Runtime files use `.f1_cache/` and `v2_outputs/` beneath the working directory.
Both paths are ignored. The `tif1` session loader also uses its configured cache.
Outputs include holdout predictions, calibration tables, selection reports,
source audits, metadata, and readable results and charts.

`cota_predictor_v2.py` is the main entry point. The shared historical features
and the previous results-only baseline remain in `cota_predictor_improved.py`.
That shared module is not a separate entry point.
`compare_v1.py` compares the previous model on the same evaluation races.
Run `summarize_v2.py` afterward to include a matching comparison in the report.

Use the [roadmap](ROADMAP.md) for further work. Record a baseline, isolate each
change, and compare the same races before claiming an improvement.

## Limits and Attribution

Practice fuel loads and traffic are unobserved. Pace and degradation estimates
remain noisy proxies. Historical grids use qualifying order to exclude later
penalty announcements. Entry-list revisions and result-publication revisions
are not reconstructed. Race strengths do not explicitly model shared accidents
or strategy. Position intervals are nominal and lack guaranteed empirical coverage.

Data and software sources:

- [tif1](https://github.com/TracingInsights/tif1) and
  [TracingInsights session data](https://github.com/TracingInsights/2026).
- [Jolpica race-history API](https://github.com/jolpica/jolpica-f1).
- [Open-Meteo archived forecast runs](https://open-meteo.com/en/docs/single-runs-api).
- [XGBoost ranking documentation](https://xgboost.readthedocs.io/en/stable/tutorials/learning_to_rank.html).

The authored contribution follows the repository's [MIT License](../../LICENSE).
That license does not relicense external datasets, forecasts, dependencies,
or trademarks. Consult each provider's terms and attribution requirements.
Formula 1 and team marks belong to their respective owners. No affiliation is implied.
