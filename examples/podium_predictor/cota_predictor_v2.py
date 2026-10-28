#!/usr/bin/env python3
"""Chronological F1 model selection, temperature calibration, and ordered podiums.

Run: python cota_predictor_v2.py --mode all
Offline repeat: python cota_predictor_v2.py --mode all --offline
No new data: python cota_predictor_v2.py --no-weekend-data --offline
Historical evaluation only: python cota_predictor_v2.py --evaluate-only --mode all

Pre: Wednesday, 108 h before race. Practice: 1 h before qualifying.
Post: 2 h before race. Auto uses the latest stage whose cutoff has passed.
Historical evaluation models are refit every block on strictly earlier races.
Model/feature/recency/temperature selection uses 2025; 2026 is held out from that
selection. Earlier 2026 results can train later 2026 forecasts (walk-forward).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any

import cota_predictor_improved as base
import numpy as np
import orjson
import pandas as pd
import weekend_data as weekend
from numpy.typing import ArrayLike, NDArray
from scipy.optimize import minimize_scalar
from xgboost import XGBRanker

VERSION = "2.1-tif1"
OUT = Path("v2_outputs")
CORE_EXTRA = ["drv_season_team_form", "team_season_form"]
TRACK = [c for c in base.FEATURES_TRACK if c not in ["elevation", "circuit_sc_rate"]]
RECENCY = [
    ("all_history", 0, 0),
    ("half_life_24", 24, 0),
    ("half_life_48", 48, 0),
    ("last_72_races", 0, 72),
]


def dependency_versions() -> dict[str, str]:
    """Record the installed data and model packages that affect reproducibility."""
    packages = ("tif1", "numpy", "pandas", "scipy", "scikit-learn", "xgboost", "orjson")
    return {"python": sys.version.split()[0], **{name: version(name) for name in packages}}


def save_json(path: Path, obj: Any) -> None:
    """Write JSON, converting NumPy scalars and nonfinite floats for portability."""

    def clean(x: Any) -> Any:
        """Recursively normalize containers and numeric values for JSON encoding."""
        if isinstance(x, dict):
            return {str(k): clean(v) for k, v in x.items()}
        if isinstance(x, (list, tuple)):
            return [clean(v) for v in x]
        if isinstance(x, np.generic):
            return clean(x.item())
        if isinstance(x, float) and not np.isfinite(x):
            return None
        return x

    path.write_bytes(orjson.dumps(clean(obj), option=orjson.OPT_INDENT_2, default=str) + b"\n")


def strengths(scores: ArrayLike, temperature: float = 1) -> NDArray[np.float64]:
    """Convert finite scores into positive temperature-scaled race strengths."""
    scores = np.asarray(scores, dtype=float)
    if not np.isfinite(scores).all() or temperature <= 0:
        raise ValueError("Finite scores and a positive temperature are required")
    return np.exp(np.clip((scores - scores.max()) / temperature, -20, 0))


def podium_distribution(
    scores: ArrayLike, temperature: float = 1
) -> tuple[NDArray[np.float64], NDArray[np.float64]]:
    """Exact ordered top-three probabilities from a Plackett-Luce race model.

    All drivers share one distribution of race orders: three distinct drivers,
    with consistent P1, P2, P3 and podium marginals. This model assumes conditional
    independent strengths; it does not simulate shared crashes/weather/strategy.
    """
    w = strengths(scores, temperature)
    n, total = len(w), w.sum()
    if n < 3:
        raise ValueError("A podium requires at least three drivers")
    i, j, k = np.indices((n, n, n))
    valid = (i != j) & (j != k) & (i != k)
    second_denom = np.maximum(total - w[i], 1e-15)
    third_denom = np.maximum(total - w[i] - w[j], 1e-15)
    joint = np.where(valid, (w[i] / total) * (w[j] / second_denom) * (w[k] / third_denom), 0.0)
    joint /= joint.sum()
    p1, p2, p3 = joint.sum(axis=(1, 2)), joint.sum(axis=(0, 2)), joint.sum(axis=(0, 1))
    return joint, np.column_stack([p1, p2, p3])


def order_logloss(race: pd.DataFrame, temperature: float) -> float:
    """Return negative log probability of a race's observed ordered podium."""
    w = strengths(race.score.to_numpy(), temperature)
    order = np.argsort(race.position.to_numpy(), kind="stable")[:3]
    remaining, loss = w.sum(), 0.0
    for driver in order:
        loss -= np.log(max(w[driver] / remaining, 1e-15))
        remaining -= w[driver]
    return float(loss)


def calibration_temperature(predictions: pd.DataFrame) -> float:
    """Fit one temperature using chronological out-of-sample podium outcomes."""
    races = [g for _, g in predictions.groupby("race_idx", sort=True)]
    if len(races) < 8:
        raise ValueError(
            "At least eight chronological out-of-sample races are required for calibration"
        )

    def objective(log_t: float) -> float:
        """Score a candidate temperature against chronological held-out races."""
        return float(np.mean([order_logloss(g, np.exp(log_t)) for g in races]))

    fit = minimize_scalar(objective, bounds=(np.log(0.25), np.log(8)), method="bounded")
    return float(np.exp(fit.x))


def scored_metrics(
    predictions: pd.DataFrame, temperature: float = 1
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return per-race accuracy scores and consistent driver probabilities."""
    metrics, probabilities = [], []
    for _, source in predictions.groupby("race_idx", sort=True):
        race = source.copy()
        joint, marginal = podium_distribution(race.score, temperature)
        race["p_win"], race["p_second"], race["p_third"] = marginal.T
        race["p_podium"] = marginal.sum(axis=1)
        choice = np.array(np.unravel_index(joint.argmax(), joint.shape))
        actual = np.argsort(race.position.to_numpy(), kind="stable")[:3]
        selected = np.argsort(-race.p_podium.to_numpy(), kind="stable")[:3]
        hits = len(set(selected) & set(actual))
        ywin, ypod = race.position.eq(1).to_numpy(float), race.position.le(3).to_numpy(float)
        ranks = race.score.rank(ascending=False, method="first").to_numpy()
        metrics.append(
            {
                "race_idx": int(race.race_idx.iloc[0]),
                "season": int(race.season.iloc[0]),
                "rnd": int(race.rnd.iloc[0]),
                "race_name": race.race_name.iloc[0],
                "winner_hit": float(choice[0] == actual[0]),
                "podium_hits": hits,
                "podium_set_hit": float(hits == 3),
                "ordered_podium_hit": float(np.array_equal(choice, actual)),
                "win_brier": float(np.mean((race.p_win - ywin) ** 2)),
                "podium_brier": float(np.mean((race.p_podium - ypod) ** 2)),
                "ordered_top3_logloss": order_logloss(race, temperature),
                "position_mae": float(np.mean(np.abs(ranks - race.position.to_numpy()))),
            }
        )
        probabilities.append(race)
    return pd.DataFrame(metrics), pd.concat(probabilities, ignore_index=True)


def mean_metrics(metrics: pd.DataFrame) -> dict[str, float]:
    """Average evaluation metrics with equal weight for each race."""
    columns = [
        "winner_hit",
        "podium_hits",
        "podium_set_hit",
        "ordered_podium_hit",
        "win_brier",
        "podium_brier",
        "ordered_top3_logloss",
        "position_mae",
    ]
    return {c: float(metrics[c].mean()) for c in columns}


def configs(allow_weekend: bool = True) -> list[dict[str, Any]]:
    """Enumerate recency settings and optionally include weekend features."""
    out = []
    for name, half_life, window in RECENCY:
        out.extend(
            {
                "name": name + ("_weekend" if extended else "_core"),
                "half_life": half_life,
                "window": window,
                "extended": extended,
            }
            for extended in ([False, True] if allow_weekend else [False])
        )
    return out


def feature_names(mode: str, extended: bool) -> list[str]:
    """List the core and optional features available at a prediction stage."""
    features = (
        base.get_features("post" if mode == "post" else "pre", False, False) + CORE_EXTRA + TRACK
    )
    if extended:
        features += weekend.WEATHER_FEATURES
        if mode != "pre":
            features += weekend.PRACTICE_FEATURES
    return features


def fit_scores(
    train: pd.DataFrame, test: pd.DataFrame, mode: str, config: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Fit a race-grouped ranker on earlier results and score later entries."""
    if train.race_idx.max() >= test.race_idx.min():
        raise ValueError("Training races must strictly precede prediction races")
    train = train.sort_values(["race_idx", "driverId"]).copy()
    if config["window"]:
        ids = train.race_idx.unique()[-config["window"] :]
        train = train[train.race_idx.isin(ids)]
    features = [c for c in feature_names(mode, config["extended"]) if train[c].notna().any()]
    if not features:
        raise ValueError("No training features available")
    group = train.groupby("race_idx", sort=True).size()
    age = train.race_idx.max() - group.index.to_numpy()
    weights = np.exp2(-age / config["half_life"]) if config["half_life"] else np.ones(len(group))
    weights /= weights.mean()
    relevance = train.groupby("race_idx").driverId.transform("size") - train.position
    ranker = XGBRanker(
        objective="rank:pairwise",
        n_estimators=160,
        max_depth=3,
        learning_rate=0.045,
        subsample=0.85,
        colsample_bytree=0.85,
        min_child_weight=5,
        reg_lambda=3,
        random_state=42,
        n_jobs=2,
        tree_method="hist",
        lambdarank_pair_method="mean",
        lambdarank_num_pair_per_sample=4,
    )
    ranker.fit(train[features], relevance, group=group.to_numpy(), sample_weight=weights)
    out = test.copy()
    out["score"] = ranker.predict(test[features]).astype(float)
    return out, {
        "training_races": len(group),
        "training_rows": len(train),
        "cutoff_race_idx": int(train.race_idx.max()),
        "features": features,
        "importance": dict(zip(features, ranker.feature_importances_.astype(float))),
    }


def walk_forward(
    frame: pd.DataFrame,
    races: list[int],
    mode: str,
    candidates: list[dict[str, Any]],
    block: int = 3,
) -> dict[str, pd.DataFrame]:
    """Score race blocks using only results preceding each block's first race."""
    done = frame[frame.position.notna()]
    pieces = {c["name"]: [] for c in candidates}
    for start in range(0, len(races), block):
        ids = races[start : start + block]
        train = done[done.race_idx < min(ids)]
        test = done[done.race_idx.isin(ids)]
        for config in candidates:
            pred, _ = fit_scores(train, test, mode, config)
            pieces[config["name"]].append(pred)
        print(
            f"  {mode} chronological fits: {min(start + block, len(races))}/{len(races)} races",
            flush=True,
        )
    return {name: pd.concat(parts, ignore_index=True) for name, parts in pieces.items()}


def baseline_scores(frame: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Score drivers by qualifying grid or recent driver and team form."""
    out = frame.copy()
    if mode == "post":
        out["score"] = -out.grid.astype(float)
    else:
        out["score"] = -(out.drv_form_pos.fillna(12) + out.team_form_pos.fillna(12)) / 2
    return out


def reliability(predictions: pd.DataFrame) -> pd.DataFrame:
    """Compare mean forecast probabilities with observed outcomes in bins."""
    rows = []
    for col, target in [("p_win", 1), ("p_podium", 3)]:
        bins = pd.cut(
            predictions[col], [0, 0.05, 0.15, 0.3, 0.5, 0.7, 0.85, 1.00001], include_lowest=True
        )
        table = pd.DataFrame(
            {
                "probability": predictions[col],
                "occurred": predictions.position.le(target),
                "bin": bins,
            }
        )
        for label, g in table.groupby("bin", observed=True):
            rows.append(
                {
                    "event": col,
                    "bin": str(label),
                    "drivers": len(g),
                    "predicted": float(g.probability.mean()),
                    "observed": float(g.occurred.mean()),
                }
            )
    return pd.DataFrame(rows)


def evaluate(
    frame: pd.DataFrame,
    mode: str,
    allow_weekend: bool,
    eval_year: int = 2026,
    tuning_races: int = 18,
    block: int = 3,
) -> tuple[dict[str, Any], dict[str, Any], float]:
    """Select and calibrate on earlier races, then report held-out-year results."""
    done = frame[frame.position.notna()]
    selection_ids = sorted(done.loc[done.season < eval_year, "race_idx"].unique())[-tuning_races:]
    test_ids = sorted(done.loc[done.season == eval_year, "race_idx"].unique())
    if len(selection_ids) < 8 or len(test_ids) < 5:
        raise ValueError("Need >=8 selection races and >=5 evaluation races")
    candidates = configs(allow_weekend)
    selection = walk_forward(frame, selection_ids, mode, candidates, block)
    rows = []
    for config in candidates:
        pred = selection[config["name"]]
        temperature = calibration_temperature(pred)
        metrics, _ = scored_metrics(pred, temperature)
        rows.append(dict(**config, temperature=temperature, **mean_metrics(metrics)))
    # Lock every choice before loading/evaluating the holdout predictions.
    winner = min(rows, key=lambda row: (row["podium_brier"], row["ordered_top3_logloss"]))
    chosen = next(c for c in candidates if c["name"] == winner["name"])
    uniform = next(c for c in candidates if c["name"] == "all_history_core")
    chosen_test = walk_forward(
        frame,
        test_ids,
        mode,
        [chosen] if chosen["name"] == uniform["name"] else [chosen, uniform],
        block,
    )
    baseline_selection = baseline_scores(done[done.race_idx.isin(selection_ids)], mode)
    baseline_temp = calibration_temperature(baseline_selection)
    baseline_test = baseline_scores(done[done.race_idx.isin(test_ids)], mode)
    uniform_temp = next(row["temperature"] for row in rows if row["name"] == uniform["name"])
    comparisons = {
        "form_or_grid_baseline": (baseline_test, baseline_temp),
        "uniform_core_ranker": (chosen_test[uniform["name"]], uniform_temp),
        "selected_raw": (chosen_test[chosen["name"]], 1.0),
        "selected_calibrated": (chosen_test[chosen["name"]], winner["temperature"]),
    }
    report = {
        "mode": mode,
        "eval_year": eval_year,
        "tuning_race_count": len(selection_ids),
        "eval_race_count": len(test_ids),
        "refit_every_races": block,
        "selection_last_date": str(done.loc[done.race_idx.isin(selection_ids), "race_date"].max()),
        "evaluation_first_date": str(done.loc[done.race_idx.isin(test_ids), "race_date"].min()),
        "selected": winner,
        "candidates": rows,
        "comparisons": {},
    }
    tables = {}
    for name, (pred, temperature) in comparisons.items():
        metrics, probabilities = scored_metrics(pred, temperature)
        tables[name] = metrics
        report["comparisons"][name] = mean_metrics(metrics)
        metrics.to_csv(OUT / f"{mode}_{name}_races.csv", index=False)
        if name == "selected_calibrated":
            probabilities.to_csv(OUT / f"{mode}_holdout_predictions.csv", index=False)
            reliability(probabilities).to_csv(OUT / f"{mode}_reliability.csv", index=False)
    # Paired race bootstrap, not driver-level resampling.
    best = tables["selected_calibrated"].set_index("race_idx")
    baseline = tables["form_or_grid_baseline"].set_index("race_idx").reindex(best.index)
    rng = np.random.default_rng(42)
    indices = rng.integers(0, len(best), size=(2000, len(best)))
    report["paired_vs_baseline"] = {}
    for col in ["podium_hits", "podium_brier"]:
        difference = (best[col] - baseline[col]).to_numpy()
        interval = np.percentile(difference[indices].mean(axis=1), [2.5, 97.5])
        report["paired_vs_baseline"][col] = {
            "mean": float(difference.mean()),
            "ci95": interval.tolist(),
        }
    save_json(OUT / f"{mode}_validation.json", report)
    print(
        f"  {mode}: selected {winner['name']}, temperature {winner['temperature']:.3f}", flush=True
    )
    print(
        pd.DataFrame(report["comparisons"])
        .T[["winner_hit", "podium_hits", "podium_set_hit", "podium_brier"]]
        .to_string(),
        flush=True,
    )
    return report, chosen, winner["temperature"]


def extra_form(frame: pd.DataFrame) -> pd.DataFrame:
    """Add driver and team season form using only preceding race results."""
    out = frame.copy()
    out["drv_season_team_form"] = out.groupby(
        ["driverId", "constructorId", "season"]
    ).position.transform(lambda s: s.shift().rolling(3, min_periods=1).mean())
    team = out.groupby(["race_idx", "season", "constructorId"], as_index=False).position.mean()
    team = team.sort_values("race_idx")
    team["team_season_form"] = team.groupby(["season", "constructorId"]).position.transform(
        lambda s: s.shift().rolling(3, min_periods=1).mean()
    )
    return out.merge(
        team[["race_idx", "constructorId", "team_season_form"]],
        on=["race_idx", "constructorId"],
        how="left",
    )


def historical_qualifying_grid(frame: pd.DataFrame, raw_history: pd.DataFrame) -> pd.DataFrame:
    """Apply the historical post-stage grid policy before feature construction.

    Qualifying order matches the default live forecast. All grid-derived
    statistics must use these inputs consistently, including shifted circuit
    statistics. A confirmed live --grid override remains untouched. Final result
    grids do not establish when historical penalties were announced.
    """
    out = frame.copy()
    keys = ["season", "rnd", "driverId"]
    qualifying = raw_history.set_index(keys).quali_pos
    raw = qualifying.reindex(pd.MultiIndex.from_frame(out[keys])).to_numpy()
    size = out.groupby(["season", "rnd"]).driverId.transform("size").to_numpy()
    historical = out.position.notna().to_numpy()
    known = np.where(np.isfinite(raw), raw, size)
    out.loc[historical, "grid"] = known[historical]
    out.loc[historical, "quali_pos"] = known[historical]
    return out


def final_prediction(
    frame: pd.DataFrame,
    mode: str,
    chosen: dict[str, Any],
    temperature: float,
    metadata: dict[str, Any],
) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    """Fit the selected model and save podium forecasts for unfinished entries."""
    test = frame[frame.position.isna()].copy()
    train = frame[frame.position.notna() & (frame.race_idx < test.race_idx.min())]
    pred, training = fit_scores(train, test, mode, chosen)
    joint, marginal = podium_distribution(pred.score, temperature)
    pred["p_win"], pred["p_second"], pred["p_third"] = marginal.T
    pred["p_podium"] = marginal.sum(axis=1)
    w = strengths(pred.score, temperature)
    pair = w[None, :] / (w[:, None] + w[None, :])
    pred["expected_position"] = 1 + pair.sum(axis=1) - 0.5
    # Sample complete orders from exactly the same Plackett-Luce distribution.
    rng = np.random.default_rng(42)
    simulated = np.argsort(-(np.log(w)[None, :] + rng.gumbel(size=(50000, len(w)))), axis=1)
    ranks = np.argsort(simulated, axis=1) + 1
    pred["nominal_pos_p10"], pred["nominal_pos_p90"] = np.quantile(ranks, [0.1, 0.9], axis=0)
    order = np.argsort(-joint.ravel(), kind="stable")[:10]
    codes = pred.code.to_numpy()
    ordered = []
    for idx in order:
        i, j, k = np.unravel_index(idx, joint.shape)
        ordered.append(
            {
                "first": codes[i],
                "second": codes[j],
                "third": codes[k],
                "probability": float(joint[i, j, k]),
            }
        )
    display = [
        "code",
        "constructorId",
        "p_win",
        "p_second",
        "p_third",
        "p_podium",
        "expected_position",
    ]
    pred.sort_values("p_podium", ascending=False).to_csv(
        OUT / f"{mode}_prediction.csv", index=False
    )
    save_json(OUT / f"{mode}_ordered_podiums.json", ordered)
    latest = train.sort_values(["season", "rnd"]).iloc[-1]
    meta = dict(
        version=VERSION,
        mode=mode,
        generated_utc=dt.datetime.now(weekend.UTC).isoformat(),
        selected=chosen,
        temperature=temperature,
        training=training,
        last_training_date=latest.race_date,
        last_training_race=latest.race_name,
        ranking_distribution="Plackett-Luce",
        simulation_samples=50000,
        intervals_empirically_calibrated=False,
        **metadata,
    )
    save_json(OUT / f"{mode}_prediction_metadata.json", meta)
    print(f"\n{mode.upper()} AUSTIN PREDICTION", flush=True)
    print(
        pred.sort_values("p_podium", ascending=False)[display].head(10).to_string(index=False),
        flush=True,
    )
    print("Most likely ordered podium:", ordered[0], flush=True)
    return pred, ordered, meta


def auto_mode(race: dict[str, Any], now: dt.datetime) -> str:
    """Choose the latest prediction stage whose information cutoff has passed."""
    if now >= cutoff_for_live(race, "post"):
        return "post"
    if now >= cutoff_for_live(race, "practice"):
        return "practice"
    return "pre"


def cutoff_for_live(race: dict[str, Any], mode: str) -> dt.datetime:
    """Return the scheduled information cutoff for a live prediction stage."""
    return weekend.cutoff_for(race, mode)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI options and reject inconsistent evaluation and training ranges."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=dt.datetime.now(weekend.UTC).year)
    ap.add_argument("--first-season", type=int, default=2014)
    ap.add_argument("--mode", choices=["auto", "pre", "practice", "post", "all"], default="auto")
    ap.add_argument("--eval-year", type=int, default=2026)
    ap.add_argument("--tuning-races", type=int, default=18)
    ap.add_argument("--refit-every", type=int, default=3)
    ap.add_argument("--grid")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--no-weekend-data", action="store_true")
    ap.add_argument(
        "--reuse-validation",
        action="store_true",
        help="reuse saved selection with identical data/config fingerprints",
    )
    ap.add_argument(
        "--evaluate-only",
        action="store_true",
        help="evaluate completed races without constructing a live Austin forecast",
    )
    ap.add_argument(
        "--through-date",
        type=dt.date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="include results dated on or before this date; requires --evaluate-only",
    )
    args = ap.parse_args(argv)
    if args.refit_every < 1 or args.tuning_races < 8 or args.first_season > args.season:
        ap.error("Invalid training/evaluation range or refit interval")
    if args.through_date is not None and not args.evaluate_only:
        ap.error("--through-date requires --evaluate-only")
    return args


def history_through_date(
    history: pd.DataFrame, through_date: dt.date | None = None
) -> pd.DataFrame:
    """Reproduce a completed-results snapshot, including the boundary date.

    This filters final race-result records by race date. It does not reconstruct
    historical publication times or later amendments to those records.
    """
    if through_date is None:
        return history.copy()
    dates = pd.to_datetime(history["race_date"], errors="coerce", utc=True)
    if dates.isna().any():
        raise ValueError("History contains missing or invalid race dates")
    filtered = history.loc[dates.dt.date <= through_date].copy()
    if filtered.empty:
        raise ValueError("No completed races on or before --through-date")
    return filtered


def main() -> None:
    """Prepare features, validate models, and optionally produce a live forecast."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = parse_arguments()
    import tif1

    config = tif1.get_config()
    previous_offline = config.get("offline_mode")
    previous_local_offline = base.OFFLINE
    effective_offline = bool(args.offline or previous_local_offline or previous_offline)
    config.set("offline_mode", effective_offline)
    base.OFFLINE = effective_offline
    args.offline = base.OFFLINE
    try:
        run(args)
    finally:
        config.set("offline_mode", previous_offline)
        base.OFFLINE = previous_local_offline


def run(args: argparse.Namespace) -> None:
    """Execute a forecast with the caller's temporary offline configuration."""
    OUT.mkdir(exist_ok=True)
    environment = dependency_versions()
    print("Loading race results...", flush=True)
    hist = history_through_date(
        base.load_history(args.first_season, args.season), args.through_date
    )
    calendars = {}
    for season in range(2024, args.season + 1):
        schedule = base.jget(f"{season}.json", ttl=3600)["MRData"]["RaceTable"]["Races"]
        for race in schedule:
            calendars[(int(race["season"]), int(race["round"]))] = race
    race = base.cota_race(args.season)
    if not race:
        raise ValueError("Austin is absent from this calendar")
    now = dt.datetime.now(weekend.UTC)
    run_id = now.isoformat()
    current_mode = "pre" if args.evaluate_only else auto_mode(race, now)
    modes = (
        ["pre", "practice", "post"]
        if args.mode == "all"
        else [current_mode if args.mode == "auto" else args.mode]
    )
    final_mode = None if args.evaluate_only else (current_mode if args.mode == "all" else modes[0])
    if (
        final_mode is not None
        and final_mode != "pre"
        and now < weekend.cutoff_for(race, final_mode)
    ):
        raise ValueError(
            f"{final_mode} cutoff has not arrived; use --mode auto or --mode all for historical evaluation"
        )
    rows = hist
    if not args.evaluate_only:
        up, rnd, name, actual_mode = base.build_upcoming(
            hist, args.season, "post" if final_mode == "post" else "pre", args.grid
        )
        if final_mode == "post" and actual_mode != "post":
            raise ValueError("Post-qualifying requested but qualifying/grid data are unavailable")
        loc = race["Circuit"]["Location"]
        up["race_date"], up["race_time"], up["lat"], up["lon"] = (
            race["date"],
            race.get("time"),
            loc.get("lat"),
            loc.get("long"),
        )
        rows = pd.concat([hist, up], ignore_index=True)
    frame = extra_form(base.add_features(rows))
    summaries = {}
    for mode in modes:
        print(f"\nPreparing {mode} features...", flush=True)
        # Normalize raw post-stage grids before deriving direct or shifted features.
        stage_frame = (
            extra_form(base.add_features(historical_qualifying_grid(rows, hist)))
            if mode == "post"
            else frame
        )
        augmented, audit = weekend.attach_weekends(
            stage_frame,
            calendars,
            mode,
            now=now,
            weather=not args.no_weekend_data,
            practice=not args.no_weekend_data,
            offline=args.offline,
        )
        save_json(OUT / f"{mode}_data_audit.json", audit)
        augmented.to_csv(OUT / f"{mode}_features.csv", index=False)
        # Reuse only exact feature/label/config matches, never across refreshed data.

        source_hash = hashlib.sha256(
            Path(__file__).read_bytes()
            + Path(weekend.__file__).read_bytes()
            + Path(base.__file__).read_bytes()
        ).hexdigest()
        fingerprint = hashlib.sha256(
            (
                VERSION
                + source_hash
                + mode
                + str(args.eval_year)
                + str(args.tuning_races)
                + str(args.refit_every)
                + str(not args.no_weekend_data)
                + str(args.through_date)
                + orjson.dumps(environment, option=orjson.OPT_SORT_KEYS).decode()
            ).encode()
            + pd.util.hash_pandas_object(
                augmented[augmented.position.notna()], index=True
            ).values.tobytes()
        ).hexdigest()
        validation_path = OUT / f"{mode}_validation.json"
        old = (
            orjson.loads(validation_path.read_bytes())
            if args.reuse_validation and validation_path.exists()
            else None
        )
        if old and old.get("fingerprint") == fingerprint:
            report = old
            chosen = {k: old["selected"][k] for k in ["name", "half_life", "window", "extended"]}
            temperature = old["selected"]["temperature"]
            print("Reusing matching chronological validation.", flush=True)
        else:
            report, chosen, temperature = evaluate(
                augmented,
                mode,
                not args.no_weekend_data,
                args.eval_year,
                args.tuning_races,
                args.refit_every,
            )
            report["fingerprint"] = fingerprint
            save_json(validation_path, report)
        summaries[mode] = report
        if mode == final_mode:
            live_audit = next(
                (a for a in audit if a["season"] == args.season and a["rnd"] == rnd), {}
            )
            final_prediction(
                augmented,
                mode,
                chosen,
                temperature,
                {
                    "race": name,
                    "race_date": race["date"],
                    "data_audit": live_audit,
                    "run_id": run_id,
                    "dependencies": environment,
                    "calibration_last_date": report["selection_last_date"],
                    "calibration_races": report["tuning_race_count"],
                },
            )
    save_json(OUT / "validation_summary.json", summaries)
    save_json(
        OUT / "run_manifest.json",
        {
            "run_id": run_id,
            "data_adapter": "tif1.get_session().laps",
            "dependencies": environment,
            "race_date": race["date"],
            "prediction_modes": [] if args.evaluate_only else [final_mode],
            "evaluation_only": args.evaluate_only,
            "through_date": args.through_date.isoformat()
            if args.through_date is not None
            else None,
            "eval_year": args.eval_year,
            "tuning_races": args.tuning_races,
            "refit_every": args.refit_every,
        },
    )
    from summarize_v2 import main as make_report

    make_report()
    print(f"\nSaved reviewable results in {OUT.resolve()}", flush=True)


if __name__ == "__main__":
    main()
