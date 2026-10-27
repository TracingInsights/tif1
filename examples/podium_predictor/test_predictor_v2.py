"""Offline regression tests for chronological podium predictions."""

# ruff: noqa: PT009, PT027

from __future__ import annotations

import argparse
import datetime as dt
import io
import itertools
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import cota_predictor_v2 as model
import numpy as np
import pandas as pd
import weekend_data as data
from test_predictor import history


class ArchiveEvaluationTests(unittest.TestCase):
    """Cover dated result snapshots and historical evaluation without live entries."""

    def dated_history(self) -> pd.DataFrame:
        """Build fixture results with races on both sides of a snapshot cutoff."""
        frame = history()
        frame["race_date"] = frame.rnd.map(
            {1: "2026-10-11", 2: "2026-10-21", 3: "2026-10-25", 4: "2026-10-30"}
        )
        return frame

    def test_snapshot_filter_includes_boundary_date(self) -> None:
        """Include the cutoff date and preserve history when no cutoff is given."""
        frame = self.dated_history()
        filtered = model.history_through_date(frame, dt.date(2026, 10, 21))
        self.assertEqual(sorted(filtered.rnd.unique()), [1, 2])
        self.assertEqual(len(filtered), 24)
        pd.testing.assert_frame_equal(model.history_through_date(frame), frame)

    def test_snapshot_cutoff_requires_evaluation_only(self) -> None:
        """Require historical-only mode whenever a result cutoff is supplied."""
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            model.parse_arguments(["--through-date", "2026-10-21"])
        args = model.parse_arguments(["--evaluate-only", "--through-date", "2026-10-21"])
        self.assertTrue(args.evaluate_only)
        self.assertEqual(args.through_date, dt.date(2026, 10, 21))

    def test_invalid_or_empty_snapshot_fails_explicitly(self) -> None:
        """Reject snapshots with no eligible races or invalid race dates."""
        frame = self.dated_history()
        with self.assertRaisesRegex(ValueError, "No completed races"):
            model.history_through_date(frame, dt.date(2026, 1, 1))
        frame.loc[0, "race_date"] = None
        with self.assertRaisesRegex(ValueError, "invalid race dates"):
            model.history_through_date(frame, dt.date(2026, 10, 21))

    def test_completed_austin_can_be_evaluated_without_building_live_entries(self) -> None:
        """Evaluate each stage without requesting entries or a live prediction."""
        frame = self.dated_history()
        race = {
            "season": "2026",
            "round": "4",
            "date": "2026-10-30",
            "time": "20:00:00Z",
            "raceName": "United States Grand Prix",
            "Circuit": {"circuitId": "americas", "Location": {"lat": "30", "long": "-97"}},
        }
        schedule = {"MRData": {"RaceTable": {"Races": [race]}}}
        report = {"selected": {}, "selection_last_date": "2025-12-07", "tuning_race_count": 18}
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            output = Path(folder)
            with (
                patch.object(model, "OUT", output),
                patch(
                    "sys.argv",
                    [
                        "cota_predictor_v2.py",
                        "--evaluate-only",
                        "--mode",
                        "all",
                        "--through-date",
                        "2026-10-21",
                        "--no-weekend-data",
                    ],
                ),
                patch.object(model.base, "load_history", return_value=frame),
                patch.object(model.base, "jget", return_value=schedule),
                patch.object(model.base, "cota_race", return_value=race),
                patch.object(
                    model.base,
                    "build_upcoming",
                    side_effect=AssertionError("Live entries requested"),
                ),
                patch.object(
                    model, "auto_mode", side_effect=AssertionError("Live stage requested")
                ),
                patch.object(
                    model, "final_prediction", side_effect=AssertionError("Live forecast requested")
                ),
                patch.object(
                    model.weekend, "attach_weekends", side_effect=lambda f, *_a, **_k: (f, [])
                ),
                patch.object(model, "evaluate", return_value=(report.copy(), {}, 1.0)) as evaluate,
                patch("summarize_v2.main") as summarize,
                redirect_stdout(io.StringIO()),
            ):
                model.main()
            self.assertEqual(evaluate.call_count, 3)
            for call in evaluate.call_args_list:
                self.assertEqual(sorted(call.args[0].rnd.unique()), [1, 2])
                self.assertTrue(call.args[0].position.notna().all())
            manifest = json.loads((output / "run_manifest.json").read_text())
            self.assertEqual(manifest["prediction_modes"], [])
            self.assertTrue(manifest["evaluation_only"])
            self.assertEqual(manifest["through_date"], "2026-10-21")
            self.assertFalse(list(output.glob("*_prediction.csv")))
            summarize.assert_called_once_with()


class OfflineConfigurationTests(unittest.TestCase):
    """Keep inherited offline settings effective and scoped to a single run."""

    def assert_offline_scope(
        self, *, cli: bool, configured: bool | None, local: bool, fail: bool = False
    ) -> None:
        """Inspect both provider flags during a mocked run and after its cleanup."""
        settings = {"offline_mode": configured}
        config = SimpleNamespace(get=settings.get, set=settings.__setitem__)
        arguments = argparse.Namespace(offline=cli)
        expected = bool(cli or configured or local)

        def inspect_run(args: argparse.Namespace) -> None:
            """Verify the effective setting passed to feature and history providers."""
            assert args.offline is expected
            assert settings["offline_mode"] is expected
            assert model.base.OFFLINE is expected
            if fail:
                raise RuntimeError("forecast failed")

        with (
            patch("tif1.get_config", return_value=config),
            patch.object(model, "parse_arguments", return_value=arguments),
            patch.object(model.base, "OFFLINE", local),
            patch.object(model, "run", side_effect=inspect_run) as run,
            redirect_stdout(io.StringIO()),
        ):
            if fail:
                with self.assertRaisesRegex(RuntimeError, "forecast failed"):
                    model.main()
            else:
                model.main()
            self.assertIs(settings["offline_mode"], configured)
            self.assertIs(model.base.OFFLINE, local)
            run.assert_called_once_with(arguments)

    def test_inherited_offline_settings_reach_run(self) -> None:
        """Honor either existing tif1 or history-provider offline configuration."""
        for configured, local in [(True, False), (False, True)]:
            with self.subTest(configured=configured, local=local):
                self.assert_offline_scope(cli=False, configured=configured, local=local)

    def test_cli_offline_temporarily_sets_both_providers(self) -> None:
        """Make the explicit offline switch effective without persisting it."""
        self.assert_offline_scope(cli=True, configured=False, local=False)

    def test_offline_settings_are_restored_after_failure(self) -> None:
        """Restore exact prior configuration even when model execution raises."""
        self.assert_offline_scope(cli=True, configured=None, local=False, fail=True)


class OrderedPodiumTests(unittest.TestCase):
    """Check joint podium probabilities and calibration against known outcomes."""

    def test_matches_complete_order_enumeration(self) -> None:
        """Match an exhaustive race-order calculation and normalized marginals."""
        scores = np.array([0.1, 1.0, -0.5, 0.7])
        joint, marginal = model.podium_distribution(scores, 1.3)
        expected = np.zeros_like(joint)
        w = model.strengths(scores, 1.3)
        for order in itertools.permutations(range(4)):
            p, total = 1.0, w.sum()
            for i in order:
                p *= w[i] / total
                total -= w[i]
            expected[order[:3]] += p
        np.testing.assert_allclose(joint, expected, atol=1e-12)
        np.testing.assert_allclose(marginal.sum(axis=0), [1, 1, 1])
        self.assertAlmostEqual(marginal.sum(), 3)
        self.assertTrue((marginal.sum(axis=1) <= 1 + 1e-12).all())

    def test_no_duplicate_drivers_and_score_shift_invariance(self) -> None:
        """Exclude repeated podium drivers and ignore a common score offset."""
        scores = np.linspace(-3, 3, 22)
        joint, marginal = model.podium_distribution(scores)
        shifted, _ = model.podium_distribution(scores + 100)
        np.testing.assert_allclose(joint, shifted, atol=1e-12)
        i, j, k = np.indices(joint.shape)
        self.assertEqual(joint[(i == j) | (j == k) | (i == k)].sum(), 0.0)
        self.assertTrue((marginal[:, 0] <= marginal.sum(axis=1) + 1e-12).all())

    def test_temperature_learns_from_out_of_sample_races(self) -> None:
        """Recover synthetic dispersion and improve later-race log loss."""
        rng = np.random.default_rng(13)
        pieces = []
        for rnd in range(350):
            score = rng.normal(size=8) * 2
            order = np.argsort(-(score / 2.5 + rng.gumbel(size=8)))
            pos = np.argsort(order) + 1
            pieces.append(pd.DataFrame({"race_idx": rnd, "score": score, "position": pos}))
        frame = pd.concat(pieces, ignore_index=True)
        temp = model.calibration_temperature(frame[frame.race_idx < 250])
        self.assertGreater(temp, 1.8)
        self.assertLess(temp, 3.5)
        test = [g for _, g in frame[frame.race_idx >= 250].groupby("race_idx")]
        self.assertLess(
            np.mean([model.order_logloss(g, temp) for g in test]),
            np.mean([model.order_logloss(g, 1) for g in test]),
        )


class ChronologyTests(unittest.TestCase):
    """Guard feature chronology and race-group weighting in model fitting."""

    def test_historical_grid_does_not_use_later_penalties(self) -> None:
        """Replace final result grids with qualifying order for historical rows."""
        raw = history()
        frame = raw.copy()
        frame["race_idx"] = frame.rnd - 1
        frame.loc[frame.driverId == "d0", "grid"] = 12
        corrected = model.historical_qualifying_grid(frame, raw)
        self.assertTrue((corrected.grid == corrected.quali_pos).all())
        self.assertTrue((corrected.loc[corrected.driverId == "d0", "grid"] == 1).all())

    def test_walk_forward_never_fits_future_races(self) -> None:
        """Ensure every training block ends before its prediction block begins."""
        frame = history()
        frame["race_idx"] = frame.rnd - 1
        seen = []

        def fake_fit(
            train: pd.DataFrame, test: pd.DataFrame, mode: str, config: dict[str, Any]
        ) -> tuple[pd.DataFrame, dict[str, Any]]:
            """Record prediction blocks while asserting strict training chronology."""
            self.assertLess(train.race_idx.max(), test.race_idx.min())
            seen.append(test.race_idx.unique().tolist())
            out = test.copy()
            out["score"] = -out.grid
            return out, {}

        config = model.configs(False)[:1]
        with patch.object(model, "fit_scores", side_effect=fake_fit):
            pred = model.walk_forward(frame, [1, 2, 3], "pre", config, block=2)
        self.assertEqual(seen, [[1, 2], [3]])
        self.assertEqual(len(next(iter(pred.values()))), 36)

    def test_current_team_form_excludes_current_result(self) -> None:
        """Keep a race's form features independent of its finishing results."""
        frame = history()
        frame["race_idx"] = frame.rnd - 1
        first = model.extra_form(frame)
        frame.loc[frame.rnd == 3, "position"] = 12
        changed = model.extra_form(frame)
        cols = model.CORE_EXTRA
        pd.testing.assert_frame_equal(
            first.loc[first.rnd <= 3, cols], changed.loc[changed.rnd <= 3, cols]
        )

    def test_ranker_has_query_weights_not_driver_weights(self) -> None:
        """Pass one recency weight per race rather than per driver row."""
        frame = history()
        frame["race_idx"] = frame.rnd - 1
        frame = model.extra_form(frame)
        for c in model.TRACK:
            frame[c] = 0.5
        for c in model.feature_names("pre", False):
            if c not in frame:
                frame[c] = 1.0
        captured = {}

        class FakeRanker:
            """Capture fit options while returning deterministic test scores."""

            def __init__(self, **kwargs: Any) -> None:
                """Accept production constructor options without fitting a model."""

            def fit(self, x: pd.DataFrame, _y: pd.Series, **kwargs: Any) -> None:
                """Capture race weights and provide a feature-importance placeholder."""
                captured.update(kwargs)
                self.feature_importances_ = np.ones(x.shape[1]) / x.shape[1]

            def predict(self, x: pd.DataFrame) -> np.ndarray:
                """Return one neutral score for each requested driver."""
                return np.zeros(len(x))

        with patch.object(model, "XGBRanker", FakeRanker):
            model.fit_scores(
                frame[frame.rnd < 4],
                frame[frame.rnd == 4],
                "pre",
                {"half_life": 1, "window": 0, "extended": False},
            )
        self.assertEqual(len(captured["sample_weight"]), 3)
        self.assertAlmostEqual(captured["sample_weight"][-1] / captured["sample_weight"][0], 4)


class WeekendDataTests(unittest.TestCase):
    """Verify forecast publication cutoffs and session data availability."""

    def test_forecast_run_allows_publication_time(self) -> None:
        """Select a weather initialization with enough time for publication."""
        cutoff = dt.datetime(2026, 10, 21, 7, 59, tzinfo=data.UTC)
        run = data.forecast_run(cutoff)
        self.assertEqual(run.hour, 18)
        self.assertLessEqual(run + dt.timedelta(hours=8), cutoff)

    def test_forecast_uses_explicit_archive_run_not_realized_weather(self) -> None:
        """Use a forecast archive run and the race-window weather values."""
        race = {
            "date": "2026-10-25",
            "time": "20:00:00Z",
            "Circuit": {"Location": {"lat": "30", "long": "-97"}},
        }
        cutoff = dt.datetime(2026, 10, 21, 8, tzinfo=data.UTC)
        payload = {
            "hourly": {
                "time": ["2026-10-25T20:00", "2026-10-25T21:00", "2026-10-25T22:00"],
                "temperature_2m": [25, 26, 27],
                "precipitation": [99, 0.2, 0.3],
                "wind_speed_10m": [10, 11, 12],
                "relative_humidity_2m": [60, 65, 70],
            }
        }
        with patch.object(data, "cached_get", return_value=(payload, "ok")) as get:
            values, _audit = data.weather_for(race, cutoff)
        self.assertIn("single-runs-api", get.call_args.args[0])
        self.assertEqual(get.call_args.args[1]["run"], "2026-10-21T00:00")
        self.assertNotIn("start_date", get.call_args.args[1])
        self.assertAlmostEqual(values["forecast_rain"], 0.5)
        self.assertEqual(values["forecast_temp"], 26.0)

    def test_rain_overlaps_half_hour_race_start(self) -> None:
        """Weight precipitation intervals by overlap with a half-hour race start."""
        race = {
            "date": "2026-10-25",
            "time": "20:30:00Z",
            "Circuit": {"Location": {"lat": "30", "long": "-97"}},
        }
        payload = {
            "hourly": {
                "time": [f"2026-10-25T{h}:00" for h in [20, 21, 22, 23]],
                "temperature_2m": [20, 21, 22, 23],
                "precipitation": [99, 2, 4, 6],
                "wind_speed_10m": [10, 10, 10, 10],
                "relative_humidity_2m": [60, 60, 60, 60],
            }
        }
        with patch.object(data, "cached_get", return_value=(payload, "ok")):
            values, _ = data.weather_for(race, dt.datetime(2026, 10, 21, 8, tzinfo=data.UTC))
        self.assertAlmostEqual(values["forecast_rain"], 8.0)
        self.assertAlmostEqual(values["forecast_temp"], 21.5)

    def test_practice_excludes_future_laps(self) -> None:
        """Ignore post-cutoff laps without changing eligible practice metrics."""
        cutoff = dt.datetime(2026, 10, 23, 20, tzinfo=data.UTC)
        rows = [
            {
                "drv": f"D{d}",
                "lap": lap,
                "time": 90 + d * 0.2 + lap * 0.02,
                "compound": "MEDIUM",
                "life": lap,
                "stint": 1,
                "status": "1",
                "pin": None,
                "pout": None,
                "sesT": lap * 100,
                "iacc": True,
                "wR": False,
                "lSD": f"2026-10-23T19:{lap * 2:02d}:00",
            }
            for d in range(4)
            for lap in range(1, 9)
        ]
        raw = pd.DataFrame(rows).to_dict("list")
        before, _ = data.practice_summary(raw, cutoff)
        for col, value in {
            "drv": "FUTURE",
            "lap": 1,
            "time": 45.0,
            "compound": "SOFT",
            "life": 1,
            "stint": 1,
            "status": "1",
            "pin": None,
            "pout": None,
            "sesT": 9999,
            "iacc": True,
            "wR": False,
            "lSD": "2026-10-24T19:00:00",
        }.items():
            raw[col].append(value)
        after, audit = data.practice_summary(raw, cutoff)
        self.assertEqual(before, after)
        self.assertNotIn("FUTURE", after)
        self.assertEqual(audit["drivers"], 4)
        self.assertFalse(audit["fuel_load_known"])

    def test_missing_weather_run_falls_back_only_to_earlier_initialization(self) -> None:
        """Fall back to an earlier forecast when the preferred archive is missing."""
        race = {
            "date": "2026-10-25",
            "time": "20:00:00Z",
            "Circuit": {"Location": {"lat": "30", "long": "-97"}},
        }
        cutoff = dt.datetime(2026, 10, 21, 8, tzinfo=data.UTC)
        payload = {
            "hourly": {
                "time": ["2026-10-25T20:00", "2026-10-25T21:00", "2026-10-25T22:00"],
                "temperature_2m": [25, 26, 27],
                "precipitation": [0, 0, 0],
                "wind_speed_10m": [10, 11, 12],
                "relative_humidity_2m": [60, 65, 70],
            }
        }
        with patch.object(data, "cached_get", side_effect=[(None, "HTTP 400"), (payload, "ok")]):
            _, audit = data.weather_for(race, cutoff)
        self.assertEqual(audit["run"], "2026-10-20T18:00:00+00:00")
        self.assertEqual(len(audit["attempted_runs"]), 2)

    def test_unavailable_practice_is_explicit(self) -> None:
        """Return empty metrics and a zero-lap audit for missing practice data."""
        metrics, audit = data.practice_summary({}, dt.datetime.now(data.UTC))
        self.assertEqual(metrics, {})
        self.assertEqual(audit["clean_laps"], 0)

    def test_sprint_positions_exclude_laps_after_cutoff(self) -> None:
        """Determine sprint positions using only laps available by the cutoff."""
        raw = {
            "drv": ["A", "A", "B"],
            "lap": [1, 2, 1],
            "pos": [1, 2, 2],
            "lSD": ["2026-10-23T13:00:00", "2026-10-23T14:30:00", "2026-10-23T13:00:00"],
            "time": [90, 90, 91],
        }
        positions = data.session_positions(raw, dt.datetime(2026, 10, 23, 14, tzinfo=data.UTC))
        self.assertEqual(positions, {"A": 1.0, "B": 2.0})

    def test_offline_is_forwarded_to_weekend_loading(self) -> None:
        """Pass the offline flag through to each isolated weekend provider."""
        frame = pd.DataFrame(
            {
                "season": [2026],
                "rnd": [16],
                "race_idx": [0],
                "constructorId": ["team"],
                "code": ["A"],
            }
        )
        race = {
            "season": "2026",
            "round": "16",
            "raceName": "Bahrain Grand Prix in Malaysia",
            "date": "2026-10-04",
            "time": "13:00:00Z",
            "Circuit": {"Location": {}},
        }
        with patch.object(
            data, "weekend_for", return_value=({"weather": {}, "drivers": {}}, {})
        ) as fetch:
            data.attach_weekends(frame, {(2026, 16): race}, "practice", offline=True)
        self.assertIs(fetch.call_args.args[-1], True)
        self.assertEqual(fetch.call_args.args[0], race)


if __name__ == "__main__":
    unittest.main()
