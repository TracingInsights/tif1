"""Shared chronological features and the previous results-only comparison model.

Use cota_predictor_v2.py for evaluation and forecasts. Race results and qualifying
come from Jolpica. Session laps are loaded by weekend_data through tif1.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import orjson
import pandas as pd
import requests
from numpy.typing import ArrayLike, NDArray
from requests.adapters import HTTPAdapter
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from urllib3.util.retry import Retry
from xgboost import XGBClassifier, XGBRegressor

BASE = "https://api.jolpi.ca/ergast/f1"

CIRCUIT_ID = "americas"

CACHE = Path(".f1_cache")

OFFLINE = False

BLEND = 0.5

HTTP = requests.Session()

HTTP.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
    ),
)

FEATURES_PRE = [
    "drv_form_pos",
    "drv_form_pts",
    "drv_dnf_rate",
    "team_form_pos",
    "team_form_pts",
    "drv_circuit_avg",
    "circuit_chaos",
    "drv_inc_rate",
    "team_mech_rate",
    "drv_circuit_dnf",  # retirement risk: crashes / reliability
]

FEATURES_QUALI = [
    "grid",
    "quali_pos",
    "quali_gap_pct",
    "teammate_grid_gap",
    "q3_gap_pct",
    "last_seg",
    "last_seg_gap_pct",
]

FEATURES_PACE = [
    "drv_rpace_form",
    "drv_qpace_form",
    "drv_lap1_form",
    "team_rpace_form",
    "team_qpace_form",
    "team_ts_form",
    "team_rts_form",
    "circuit_vmax",
    "ts_x_circuit",  # straight-line speed x track
    "team_pit_form",
    "team_slow_form",
]

FEATURES_PACE_NOW = ["q_gap_pct", "team_q_now"]

FEATURES_TRACK = [
    "circuit_pole_win",
    "circuit_dnf",
    "circuit_grid_corr",
    "circuit_sc_rate",
    "team_circuit_avg",
    "elevation",
]

FEATURES_WEATHER = ["air_temp", "humidity", "wind", "precip_mm", "wet", "drv_wet_gain", "drv_wet_x"]

XGB_REG = {
    "n_estimators": 300,
    "max_depth": 3,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "reg_lambda": 2.0,
    "random_state": 42,
}

XGB_CLF = {
    "n_estimators": 200,
    "max_depth": 3,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "reg_lambda": 2.0,
    "random_state": 42,
    "eval_metric": "logloss",
}


def get_features(mode: str, pace: bool, env: bool = False) -> list[str]:
    """Return feature names for the prediction mode and available optional data."""
    f = list(FEATURES_PRE)
    if pace:
        f += FEATURES_PACE
    if env:
        f += FEATURES_TRACK + FEATURES_WEATHER
    if mode == "post":
        f += FEATURES_QUALI
        if pace:
            f += FEATURES_PACE_NOW
    return f


def jget(
    path: str, params: dict[str, Any] | None = None, ttl: float | None = None
) -> dict[str, Any]:
    """GET from Jolpica with on-disk caching. ttl=None -> cache forever."""
    params = params or {}
    key = hashlib.sha256(
        path.encode() + orjson.dumps(params, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()
    f = CACHE / f"{key}.json"
    if f.exists() and (OFFLINE or ttl is None or time.time() - f.stat().st_mtime < ttl):
        return orjson.loads(f.read_bytes())
    if OFFLINE:
        raise RuntimeError(
            f"Offline Jolpica cache is missing {path}; run once with internet access"
        )
    for attempt in range(6):
        r = HTTP.get(f"{BASE}/{path}", params=params, timeout=30)
        if r.status_code == 429:  # rate limited -> back off
            time.sleep(2**attempt)
            continue
        r.raise_for_status()
        data = orjson.loads(r.content)
        CACHE.mkdir(exist_ok=True)
        f.write_bytes(orjson.dumps(data))
        time.sleep(0.3)
        return data
    raise RuntimeError(f"Jolpica API kept rate-limiting: {path}")


def fetch_races(season: int, endpoint: str, ttl: float | None) -> dict[int, dict[str, Any]]:
    """All races of a season for 'results' or 'qualifying' (handles pagination)."""
    key = "Results" if endpoint == "results" else "QualifyingResults"
    races, offset = {}, 0
    while True:
        mr = jget(f"{season}/{endpoint}.json", {"limit": 100, "offset": offset}, ttl)["MRData"]
        for race in mr["RaceTable"]["Races"]:
            rnd = int(race["round"])
            if rnd not in races:
                races[rnd] = race
                races[rnd][key] = list(race.get(key, []))
            else:  # a race can be split across pages
                races[rnd][key].extend(race.get(key, []))
        offset += 100
        if offset >= int(mr["total"]):
            break
    return races


def parse_time(s: str | None) -> float:
    """'1:32.608' -> 92.608 seconds."""
    if not s:
        return np.nan
    try:
        parts = [float(p) for p in s.split(":")]
    except ValueError:
        return np.nan
    return sum(p * 60**i for i, p in enumerate(reversed(parts)))


INCIDENT_WORDS = ("accident", "collision", "spun", "crash")


def dnf_cause(status: str | None) -> tuple[float, float]:
    """-> (incident, mechanical) flags. Finished / lapped (+1 Lap, Lapped) / disqualified -> (0, 0)."""
    st = (status or "").lower()
    if (
        st == "finished"
        or st.startswith("+")
        or "lapped" in st
        or "disqualif" in st
        or "exclu" in st
    ):
        return 0.0, 0.0
    if any(w in st for w in INCIDENT_WORDS):
        return 1.0, 0.0
    return 0.0, 1.0


def load_history(first_season: int, last_season: int) -> pd.DataFrame:
    """Load Jolpica results and qualifying for an inclusive range of seasons."""
    rows = []
    this_year = dt.datetime.now(dt.UTC).year
    for season in range(first_season, last_season + 1):
        ttl = None if season < this_year else 3600  # refresh the live season hourly
        print(f"  loading {season} ...", end="\r", flush=True)
        res = fetch_races(season, "results", ttl)
        qual = fetch_races(season, "qualifying", ttl)
        for rnd, race in sorted(res.items()):
            q = {x["Driver"]["driverId"]: x for x in qual.get(rnd, {}).get("QualifyingResults", [])}
            for r in race["Results"]:
                d = r["Driver"]["driverId"]
                qq = q.get(d)
                status = r.get("status", "")
                d_inc, d_mech = dnf_cause(status)
                loc = race["Circuit"].get("Location", {})
                rows.append(
                    {
                        "season": season,
                        "rnd": rnd,
                        "race_name": race["raceName"],
                        "race_date": race.get("date"),
                        "race_time": race.get("time"),
                        "lat": loc.get("lat"),
                        "lon": loc.get("long"),
                        "circuitId": race["Circuit"]["circuitId"],
                        "driverId": d,
                        "code": r["Driver"].get("code", d[:3].upper()),
                        "constructorId": r["Constructor"]["constructorId"],
                        "grid": int(r["grid"]),
                        "position": int(r["position"]),
                        "points": float(r["points"]),
                        "dnf": float(d_inc + d_mech > 0),
                        "status": status,
                        "dnf_inc": d_inc,
                        "dnf_mech": d_mech,
                        "quali_pos": float(qq["position"]) if qq else np.nan,
                        "q1": parse_time(qq.get("Q1")) if qq else np.nan,
                        "q2": parse_time(qq.get("Q2")) if qq else np.nan,
                        "q3": parse_time(qq.get("Q3")) if qq else np.nan,
                    }
                )
    print(" " * 40, end="\r")
    return pd.DataFrame(rows)


def cota_race(season: int) -> dict[str, Any] | None:
    """Return the season's scheduled COTA race, or None when absent."""
    sched = jget(f"{season}.json", ttl=3600)["MRData"]["RaceTable"]["Races"]
    return next((r for r in sched if r["Circuit"]["circuitId"] == CIRCUIT_ID), None)


def _roll(s: pd.Series, w: int) -> pd.Series:
    """Calculate a rolling mean of prior values, excluding the current row."""
    return s.shift().rolling(w, min_periods=1).mean()


WX_COLS = ["air_temp", "humidity", "wind", "cloud", "precip_mm", "rain_prob", "elevation", "wet"]


def add_features(
    df: pd.DataFrame, tr: pd.DataFrame | None = None, wx: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Merge optional data and derive grid features and prior-race form statistics."""
    df = df.copy()
    TR_COLS = (
        "q_gap_pct",
        "r_gap_pct",
        "ts_gap",
        "lap1_pos",
        "sc",
        "rts_gap",
        "vmax",
        "pit_rel",
        "pit_slow",
    )
    for c in TR_COLS + tuple(WX_COLS):
        if c in df.columns:
            df = df.drop(columns=c)
    for c in ("dnf_inc", "dnf_mech"):
        if c not in df.columns:
            df[c] = np.nan
    if tr is not None:
        df = df.merge(tr, on=["season", "rnd", "code"], how="left")
    else:
        for c in TR_COLS:
            df[c] = np.nan
    if wx is not None:
        df = df.merge(wx, on=["season", "rnd"], how="left")
    else:
        for c in WX_COLS:
            df[c] = np.nan
    df = df.sort_values(["season", "rnd", "driverId"]).reset_index(drop=True)
    df["race_idx"] = df.groupby(["season", "rnd"]).ngroup()

    # --- qualifying / grid features (known at race start) ---
    field_size = df.groupby("race_idx")["driverId"].transform("size")
    df["grid"] = df["grid"].replace(0, np.nan).fillna(field_size)
    df["quali_pos"] = df["quali_pos"].fillna(df["grid"])
    df["quali_gap_pct"] = (df["q1"] / df.groupby("race_idx")["q1"].transform("min") - 1) * 100
    df["teammate_grid_gap"] = df["grid"] - df.groupby(["race_idx", "constructorId"])[
        "grid"
    ].transform("mean")
    # full qualifying picture: Q3 gap to pole + gap inside the LAST segment each driver reached
    df["q3_gap_pct"] = (df["q3"] / df.groupby("race_idx")["q3"].transform("min") - 1) * 100
    df["last_seg"] = np.where(
        df["q3"].notna(), 3, np.where(df["q2"].notna(), 2, np.where(df["q1"].notna(), 1, np.nan))
    )
    last_t = df["q3"].fillna(df["q2"]).fillna(df["q1"])
    seg_best = last_t.groupby([df["race_idx"], df["last_seg"]]).transform("min")
    df["last_seg_gap_pct"] = (last_t / seg_best - 1) * 100
    df["lap1_gain"] = df["grid"] - df["lap1_pos"]  # places gained on lap 1 (race-day only)

    # --- driver form (results) ---
    g = df.groupby("driverId")
    df["drv_form_pos"] = g["position"].transform(
        lambda s: s.shift().rolling(5, min_periods=2).mean()
    )
    df["drv_form_pts"] = g["points"].transform(lambda s: s.shift().rolling(5, min_periods=2).mean())
    df["drv_dnf_rate"] = g["dnf"].transform(lambda s: s.shift().rolling(10, min_periods=3).mean())
    df["drv_inc_rate"] = g["dnf_inc"].transform(
        lambda s: s.shift().rolling(15, min_periods=3).mean()
    )
    df["drv_circuit_dnf"] = df.groupby(["driverId", "circuitId"])["dnf"].transform(
        lambda s: s.shift().expanding().mean()
    )

    # --- team form (results; car pace proxy) ---
    tm = df.groupby(["race_idx", "constructorId"], as_index=False).agg(
        season=("season", "first"),
        circuitId=("circuitId", "first"),
        team_pos=("position", "mean"),
        team_pts=("points", "sum"),
        team_r=("r_gap_pct", "mean"),
        team_q=("q_gap_pct", "mean"),
        team_ts=("ts_gap", "mean"),
        team_rts=("rts_gap", "mean"),
        team_pit=("pit_rel", "mean"),
        team_slow=("pit_slow", "mean"),
        team_mech=("dnf_mech", "mean"),
    )
    tm = tm.sort_values(["constructorId", "race_idx"])
    gt = tm.groupby("constructorId")
    tm["team_form_pos"] = gt["team_pos"].transform(
        lambda s: s.shift().rolling(5, min_periods=2).mean()
    )
    tm["team_form_pts"] = gt["team_pts"].transform(
        lambda s: s.shift().rolling(5, min_periods=2).mean()
    )
    tm["team_pit_form"] = gt["team_pit"].transform(
        lambda s: s.shift().rolling(6, min_periods=2).mean()
    )
    tm["team_slow_form"] = gt["team_slow"].transform(
        lambda s: s.shift().rolling(6, min_periods=2).mean()
    )
    tm["team_mech_rate"] = gt["team_mech"].transform(
        lambda s: s.shift().rolling(10, min_periods=3).mean()
    )

    # --- TracingInsights pace form: rolling over the last 3 races of the SAME season,
    #     so new-regulation cars are judged only on this year's data ---
    gts = tm.groupby(["constructorId", "season"])
    tm["team_rpace_form"] = gts["team_r"].transform(lambda s: _roll(s, 3))
    tm["team_qpace_form"] = gts["team_q"].transform(lambda s: _roll(s, 3))
    tm["team_ts_form"] = gts["team_ts"].transform(lambda s: _roll(s, 3))
    tm["team_rts_form"] = gts["team_rts"].transform(lambda s: _roll(s, 3))
    tm["team_q_now"] = tm["team_q"]  # same-weekend quali pace (known)
    tm["team_circuit_avg"] = tm.groupby(["constructorId", "circuitId"])["team_pos"].transform(
        lambda s: s.shift().expanding().mean()
    )
    df = df.merge(
        tm[
            [
                "race_idx",
                "constructorId",
                "team_form_pos",
                "team_form_pts",
                "team_rpace_form",
                "team_qpace_form",
                "team_ts_form",
                "team_q_now",
                "team_circuit_avg",
                "team_pit_form",
                "team_slow_form",
                "team_mech_rate",
                "team_rts_form",
            ]
        ],
        on=["race_idx", "constructorId"],
        how="left",
    )
    gds = df.groupby(["driverId", "season"])
    df["drv_rpace_form"] = gds["r_gap_pct"].transform(lambda s: _roll(s, 3))
    df["drv_qpace_form"] = gds["q_gap_pct"].transform(lambda s: _roll(s, 3))
    df["drv_lap1_form"] = df.groupby("driverId")["lap1_gain"].transform(
        lambda s: s.shift().rolling(8, min_periods=2).mean()
    )

    # --- circuit-specific: driver's history at this track + how chaotic the track is ---
    df["drv_circuit_avg"] = df.groupby(["driverId", "circuitId"])["position"].transform(
        lambda s: s.shift().expanding().mean()
    )
    df["abs_change"] = (df["position"] - df["grid"]).abs()
    rc = df.groupby(["race_idx", "circuitId"], as_index=False)["abs_change"].mean()
    rc["circuit_chaos"] = rc.groupby("circuitId")["abs_change"].transform(
        lambda s: s.shift().expanding().mean()
    )
    df = df.merge(rc[["race_idx", "circuit_chaos"]], on="race_idx", how="left")

    # --- the TRACK: what each circuit has historically done to the results (prior races only) ---
    stats = []
    for idx, g in df.groupby("race_idx"):
        row = {
            "race_idx": idx,
            "circuitId": g["circuitId"].iloc[0],
            "pole_win": np.nan,
            "dnf": np.nan,
            "grid_corr": np.nan,
            "sc": g["sc"].max(),
            "vmax": g["vmax"].max(),
        }
        if g["position"].notna().sum() >= 10:
            pole = g[g["grid"] == 1]
            row.update(
                pole_win=float((pole["position"] == 1).any()) if len(pole) else np.nan,
                dnf=g["dnf"].mean(),
                grid_corr=g["grid"].rank().corr(g["position"].rank()),
            )
        stats.append(row)
    cs = pd.DataFrame(stats).sort_values("race_idx")
    for col, name in (
        ("pole_win", "circuit_pole_win"),
        ("dnf", "circuit_dnf"),
        ("grid_corr", "circuit_grid_corr"),
        ("sc", "circuit_sc_rate"),
        ("vmax", "circuit_vmax"),
    ):
        cs[name] = cs.groupby("circuitId")[col].transform(lambda s: s.shift().expanding().mean())
    df = df.merge(
        cs[
            [
                "race_idx",
                "circuit_pole_win",
                "circuit_dnf",
                "circuit_grid_corr",
                "circuit_sc_rate",
                "circuit_vmax",
            ]
        ],
        on="race_idx",
        how="left",
    )
    df["ts_x_circuit"] = (
        df["team_ts_form"] * df["circuit_vmax"] / 330.0
    )  # top-speed deficit x track speed

    # --- the WEATHER: who gains places in wet races (prior wet races only) ---
    gain_wet = (df["grid"] - df["position"]).where(df["wet"] == 1)
    df["drv_wet_gain"] = gain_wet.groupby(df["driverId"]).transform(
        lambda s: s.shift().expanding(min_periods=1).mean()
    )
    x = np.where(df["wet"] == 1, df["drv_wet_gain"], 0.0)  # only active on a wet day
    x[df["wet"].isna().to_numpy()] = np.nan
    df["drv_wet_x"] = x
    return df


def bounded_probabilities(
    raw: ArrayLike, total: float, lower: ArrayLike | None = None
) -> NDArray[np.float64]:
    """Rescale marginal probabilities within [lower, 1] to an exact slot total.

    This enforces probability arithmetic, not empirical calibration or a joint
    distribution of finishing orders. Fit calibration on separate past races.
    """
    raw = np.asarray(raw, dtype=float)
    lo = np.zeros_like(raw) if lower is None else np.asarray(lower, dtype=float)
    if (
        not np.isfinite(raw).all()
        or not np.isfinite(lo).all()
        or np.any(lo < 0)
        or np.any(lo > 1)
        or not lo.sum() <= total <= len(raw)
    ):
        raise ValueError("Invalid probability bounds or slot total")
    left, right = 0.0, 1.0
    raw = np.clip(raw, 1e-12, 1)
    while np.clip(raw * right, lo, 1).sum() < total:
        right *= 2
    for _ in range(80):
        mid = (left + right) / 2
        if np.clip(raw * mid, lo, 1).sum() < total:
            left = mid
        else:
            right = mid
    return np.clip(raw * ((left + right) / 2), lo, 1)


def reconcile_probabilities(out: pd.DataFrame) -> pd.DataFrame:
    """Ensure one winner, three podium places, and nested top-k marginals."""
    out = out.copy()
    for col in ("p_win", "p_podium", "p_top10"):
        if col in out:
            out[col] = out[col].astype("float64")
    for _, race in out.groupby("race_idx"):
        ix, n = race.index, len(race)
        win = bounded_probabilities(race["p_win"], 1)
        podium = bounded_probabilities(race["p_podium"], min(3, n), win)
        out.loc[ix, "p_win"] = win
        out.loc[ix, "p_podium"] = podium
        if "p_top10" in out:
            out.loc[ix, "p_top10"] = bounded_probabilities(race["p_top10"], min(10, n), podium)
    return out


def fit_predict(
    train: pd.DataFrame, test: pd.DataFrame, feats: list[str], extras: bool = False
) -> pd.DataFrame:
    """Fit on `train`, return `test` with pred_pos, p_win, p_podium."""
    if train.empty or test.empty:
        raise ValueError("Training and prediction data must both contain drivers")
    if train["race_idx"].max() >= test["race_idx"].min():
        raise ValueError("Training races must strictly precede prediction races")
    Xtr, Xte = train[feats], test[feats]
    xgb = XGBRegressor(**XGB_REG).fit(Xtr, train["position"])
    rdg = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=5.0)).fit(
        Xtr, train["position"]
    )
    out = test.copy()
    out["pred_xgb"], out["pred_ridge"] = xgb.predict(Xte), rdg.predict(Xte)
    out["pred_pos"] = BLEND * out["pred_xgb"] + (1 - BLEND) * out["pred_ridge"]

    for name, y in (("p_win", train["position"] == 1), ("p_podium", train["position"] <= 3)):
        clf = XGBClassifier(**XGB_CLF).fit(Xtr, y.astype(int))
        out[name] = clf.predict_proba(Xte)[:, 1]
    dn = XGBClassifier(**XGB_CLF).fit(Xtr, train["dnf"].astype(int))  # retirement risk
    out["p_dnf"] = dn.predict_proba(Xte)[:, 1]
    if extras:
        t10 = XGBClassifier(**XGB_CLF).fit(Xtr, (train["position"] <= 10).astype(int))
        out["p_top10"] = t10.predict_proba(Xte)[:, 1]
        for name, a in (("pos_lo", 0.1), ("pos_hi", 0.9)):  # 10th / 90th percentile of position
            q = XGBRegressor(**{**XGB_REG, "objective": "reg:quantileerror", "quantile_alpha": a})
            out[name] = q.fit(Xtr, train["position"]).predict(Xte)
        n_cars = out.groupby("race_idx")["code"].transform("size")
        out["pos_lo"] = np.clip(np.minimum(out["pos_lo"], out["pred_pos"]), 1, n_cars).round()
        out["pos_hi"] = np.clip(np.maximum(out["pos_hi"], out["pred_pos"]), 1, n_cars).round()
    return reconcile_probabilities(out)


def build_upcoming(
    hist: pd.DataFrame, season: int, mode: str, grid_override: str | None
) -> tuple[pd.DataFrame, int, str, str]:
    """Build COTA entries and qualifying inputs, validating any grid override."""
    race = cota_race(season)
    if race is None:
        sys.exit(f"No COTA race found on the {season} calendar.")
    rnd = int(race["round"])
    if ((hist["season"] == season) & (hist["rnd"] == rnd)).any():
        sys.exit(f"The {season} {race['raceName']} has already been run - see the backtest above.")

    # driver / team line-up = latest completed race of this season (else last season's finale)
    cur = hist[hist["season"] == season]
    base = cur if len(cur) else hist[hist["season"] == season - 1]
    last = base[(base["season"] == base["season"].max()) & (base["rnd"] == base["rnd"].max())]
    entries = last[["driverId", "code", "constructorId"]].drop_duplicates("driverId")

    qd = jget(f"{season}/{rnd}/qualifying.json", ttl=600)["MRData"]["RaceTable"]["Races"]
    qres = {x["Driver"]["driverId"]: x for x in qd[0]["QualifyingResults"]} if qd else {}
    if qres:
        # This weekend's qualifying is a better entry list than the last race.
        entries = pd.DataFrame(
            [
                {
                    "driverId": d,
                    "code": q["Driver"].get("code", d[:3].upper()),
                    "constructorId": q["Constructor"]["constructorId"],
                }
                for d, q in qres.items()
            ]
        )

    if mode == "auto":
        mode = "post" if (qres or grid_override) else "pre"
    if mode == "post" and not (qres or grid_override):
        print("! No qualifying results yet - falling back to pre-qualifying mode.")
        mode = "pre"

    rows = []
    for _, e in entries.iterrows():
        q = qres.get(e.driverId)
        rows.append(
            {
                "season": season,
                "rnd": rnd,
                "race_name": race["raceName"],
                "circuitId": CIRCUIT_ID,
                "driverId": e.driverId,
                "code": e.code,
                "constructorId": e.constructorId,
                "grid": 0,
                "position": np.nan,
                "points": np.nan,
                "dnf": np.nan,
                "quali_pos": float(q["position"]) if q else np.nan,
                "q1": parse_time(q.get("Q1")) if q else np.nan,
                "q2": parse_time(q.get("Q2")) if q else np.nan,
                "q3": parse_time(q.get("Q3")) if q else np.nan,
            }
        )
    up = pd.DataFrame(rows)
    if mode == "post":
        if grid_override:
            codes = [c.strip().upper() for c in grid_override.split(",")]
            if any(not c for c in codes) or len(codes) != len(set(codes)):
                raise ValueError("Grid codes must be nonempty and unique")
            unknown = set(codes) - set(up["code"])
            if unknown:
                raise ValueError(f"Unknown grid driver codes: {', '.join(sorted(unknown))}")
            if not qres and len(codes) != len(up):
                raise ValueError(
                    "Without qualifying results, --grid must specify every driver in order"
                )
            order = {c: i + 1 for i, c in enumerate(codes)}
            up["grid"] = up["code"].map(order)
            rest = up["grid"].isna()  # unlisted drivers line up behind, in quali order
            up.loc[rest, "grid"] = up.loc[rest, "quali_pos"].fillna(20).rank(method="first") + len(
                order
            )
        else:
            up["grid"] = up["quali_pos"]
            print("! Grid uses qualifying order. Apply confirmed grid penalties with --grid.")
    return up, rnd, race["raceName"], mode
