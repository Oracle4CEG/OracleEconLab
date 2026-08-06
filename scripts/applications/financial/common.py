#!/usr/bin/env python3
"""Economic realization and capital-friction benchmark.

The module deliberately keeps protocol-native raw amounts as strings. Numeric
conversion is only used for within-asset descriptive statistics.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/applications/financial_benchmark"
FIG = ROOT / "figures"
CUTOFF = 1782863999  # 2026-06-30 23:59:59 UTC
SEED = 20260729


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def build_episodes() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    oata = ROOT / "data/applications/oata/accountability_episodes.parquet"
    uma_events = ROOT / "data/curated/parquet/polygon_oov2_events.parquet"
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW landmarks AS
        SELECT oo_request_id,
               min(block_time) FILTER (event='ProposePrice') AS proposal_time,
               min(block_time) FILTER (event='DisputePrice') AS challenge_time,
               min(block_time) FILTER (event='Settle') AS settlement_time,
               max(try_cast(expiration_time AS BIGINT)) FILTER (event='ProposePrice')
                   AS proposal_expiration_time
        FROM read_parquet('{_sql_path(uma_events)}')
        GROUP BY 1
        """
    )
    target = OUT / "financial_episodes.parquet"
    con.execute(
        f"""
        COPY (
          WITH e AS (
            SELECT a.*,
              coalesce(try_cast(principal_locked_raw AS HUGEINT), 0) AS pl,
              coalesce(try_cast(principal_returned_raw AS HUGEINT), 0) AS pr,
              coalesce(try_cast(reward_accrued_raw AS HUGEINT), 0) AS ra,
              coalesce(try_cast(reward_claimable_raw AS HUGEINT), 0) AS rc,
              coalesce(try_cast(reward_paid_raw AS HUGEINT), 0) AS rp,
              coalesce(try_cast(reward_forfeited_raw AS HUGEINT), 0) AS rf,
              coalesce(try_cast(bond_forfeited_raw AS HUGEINT), 0) AS bf,
              coalesce(try_cast(principal_slashed_raw AS HUGEINT), 0) AS ps
            FROM read_parquet('{_sql_path(oata)}') a
            WHERE start_time IS NULL OR start_time <= {CUTOFF}
          ), classified AS (
            SELECT e.*, l.proposal_time, l.challenge_time, l.settlement_time,
                   l.proposal_expiration_time,
              CASE
                WHEN ps > 0 THEN 'principal_slashed'
                WHEN bf > 0 THEN 'bond_forfeited'
                WHEN rf > 0 THEN 'reward_forfeited'
                WHEN terminal_status IN ('nonmonetary_restriction','chilled','jailed')
                  OR list_contains(state_sequence, 'NONMONETARY_RESTRICTION')
                  THEN 'nonmonetary_restriction'
                WHEN rp > 0 THEN 'reward_paid'
                WHEN pr > 0 THEN 'principal_returned'
                WHEN right_censored THEN 'right_censored'
                ELSE 'unresolved_coverage'
              END AS reconstructed_terminal_event,
              CASE
                WHEN episode_type='uma_oov2_request' AND proposal_time IS NOT NULL
                     AND proposal_expiration_time >= proposal_time
                THEN proposal_expiration_time - proposal_time
                ELSE NULL
              END AS minimum_wait_seconds
            FROM e LEFT JOIN landmarks l ON e.unit_id=l.oo_request_id
          )
          SELECT
            episode_id, protocol, episode_type AS native_unit_type, unit_id,
            actor, actor_role, asset, asset_decimals, contract_version,
            start_time,
            CASE
              WHEN challenge_time IS NOT NULL THEN challenge_time
              WHEN proposal_time IS NOT NULL THEN proposal_time
              ELSE NULL
            END AS landmark_time,
            CASE WHEN reconstructed_terminal_event NOT IN
                  ('right_censored','unresolved_coverage')
                 THEN coalesce(settlement_time, end_time) ELSE NULL END AS terminal_time,
            reconstructed_terminal_event AS terminal_event,
            right_censored OR reconstructed_terminal_event='right_censored'
              AS right_censored,
            principal_locked_raw AS principal_locked,
            principal_returned_raw AS principal_returned,
            reward_accrued_raw AS reward_accrued,
            reward_claimable_raw AS reward_claimable,
            reward_paid_raw AS reward_paid,
            reward_forfeited_raw AS reward_forfeited,
            bond_forfeited_raw AS bond_forfeited,
            principal_slashed_raw AS principal_slashed,
            reconstructed_terminal_event='nonmonetary_restriction'
              AS nonmonetary_penalty,
            minimum_wait_seconds AS protocol_minimum_wait,
            coverage_status,
            evidence_class AS evidence_status,
            cross_chain_flag, source_table, state_sequence,
            proposal_time, challenge_time, settlement_time,
            CASE WHEN start_time IS NULL THEN NULL
                 WHEN terminal_time IS NOT NULL THEN terminal_time-start_time
                 ELSE {CUTOFF}-start_time END AS observed_duration_seconds,
            CASE WHEN terminal_time IS NOT NULL AND minimum_wait_seconds IS NOT NULL
                 THEN greatest(0, terminal_time-start_time-minimum_wait_seconds)
                 ELSE NULL END AS excess_delay_seconds,
            CASE WHEN challenge_time IS NOT NULL AND proposal_time IS NOT NULL
                 THEN challenge_time-proposal_time ELSE NULL END AS adjudication_delay_seconds,
            CASE WHEN settlement_time IS NOT NULL AND challenge_time IS NOT NULL
                 THEN settlement_time-challenge_time ELSE NULL END AS post_resolution_delay_seconds,
            CASE WHEN pl > 0 AND asset IS NOT NULL
                       AND (coalesce(terminal_time,{CUTOFF}) >= start_time)
                 THEN cast(pl AS DECIMAL(38,0))
                      * ((coalesce(terminal_time,{CUTOFF})-start_time)/86400.0)
                 ELSE NULL END AS capital_days_locked_raw
          FROM classified
        ) TO '{_sql_path(target)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    tracks = OUT / "track_episodes.parquet"
    con.execute(
        f"""
        COPY (
          WITH e AS (SELECT * FROM read_parquet('{_sql_path(target)}')),
          t AS (
            SELECT *, 'F1_reward_realization' AS track FROM e
             WHERE coalesce(try_cast(reward_accrued AS HUGEINT),0)>0
                OR coalesce(try_cast(reward_claimable AS HUGEINT),0)>0
                OR coalesce(try_cast(reward_paid AS HUGEINT),0)>0
                OR coalesce(try_cast(reward_forfeited AS HUGEINT),0)>0
            UNION ALL
            SELECT *, 'F2_penalty_enforcement' AS track FROM e
             WHERE native_unit_type IN ('uma_dvm_voter','flare_provider_epoch',
                                        'tellor_dispute','chainlink_service_window')
                OR (native_unit_type='uma_oov2_request'
                    AND (challenge_time IS NOT NULL
                         OR coalesce(try_cast(bond_forfeited AS HUGEINT),0)>0))
                OR coalesce(try_cast(principal_slashed AS HUGEINT),0)>0
                OR coalesce(try_cast(bond_forfeited AS HUGEINT),0)>0
                OR coalesce(try_cast(reward_forfeited AS HUGEINT),0)>0
                OR nonmonetary_penalty
            UNION ALL
            SELECT *, 'F3_capital_settlement' AS track FROM e
             WHERE coalesce(try_cast(principal_locked AS HUGEINT),0)>0
          )
          SELECT *,
            CASE
              WHEN track='F1_reward_realization'
                   AND coalesce(try_cast(reward_forfeited AS HUGEINT),0)>0
                THEN 'reward_forfeited'
              WHEN track='F1_reward_realization'
                   AND coalesce(try_cast(reward_paid AS HUGEINT),0)>0
                THEN 'reward_paid'
              WHEN track='F1_reward_realization' AND right_censored
                THEN 'right_censored'
              WHEN track='F2_penalty_enforcement'
                   AND coalesce(try_cast(principal_slashed AS HUGEINT),0)>0
                THEN 'principal_slashed'
              WHEN track='F2_penalty_enforcement'
                   AND coalesce(try_cast(bond_forfeited AS HUGEINT),0)>0
                THEN 'bond_forfeited'
              WHEN track='F2_penalty_enforcement'
                   AND coalesce(try_cast(reward_forfeited AS HUGEINT),0)>0
                THEN 'reward_forfeited'
              WHEN track='F2_penalty_enforcement' AND nonmonetary_penalty
                THEN 'nonmonetary_restriction'
              WHEN track='F2_penalty_enforcement' AND right_censored
                THEN 'right_censored'
              WHEN track='F2_penalty_enforcement'
                   AND native_unit_type='chainlink_service_window'
                   AND coverage_status='complete'
                THEN 'window_closed_without_penalty'
              WHEN track='F3_capital_settlement'
                   AND coalesce(try_cast(principal_slashed AS HUGEINT),0)>0
                THEN 'principal_slashed'
              WHEN track='F3_capital_settlement'
                   AND coalesce(try_cast(bond_forfeited AS HUGEINT),0)>0
                THEN 'bond_forfeited'
              WHEN track='F3_capital_settlement'
                   AND coalesce(try_cast(principal_returned AS HUGEINT),0)>0
                THEN 'principal_returned'
              WHEN track='F3_capital_settlement' AND right_censored
                THEN 'right_censored'
              ELSE NULL
            END AS track_terminal_event,
            CASE
              WHEN track_terminal_event IS NULL THEN NULL
              WHEN track_terminal_event='right_censored' THEN false
              ELSE true
            END AS event_observed
          FROM t
        ) TO '{_sql_path(tracks)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    prefixes = OUT / "landmark_prefixes.parquet"
    con.execute(
        f"""
        COPY (
          WITH e AS (SELECT * FROM read_parquet('{_sql_path(tracks)}'))
          SELECT episode_id, track, protocol, 'T0' AS landmark,
                 start_time AS prediction_time, native_unit_type, actor_role,
                 asset, contract_version, cross_chain_flag,
                 false AS challenge_visible, false AS adjudication_visible,
                 track_terminal_event, event_observed, observed_duration_seconds
          FROM e WHERE track_terminal_event IS NOT NULL
          UNION ALL
          SELECT episode_id, track, protocol, 'T1', landmark_time,
                 native_unit_type, actor_role, asset, contract_version,
                 cross_chain_flag, challenge_time IS NOT NULL,
                 false, track_terminal_event, event_observed,
                 greatest(0, coalesce(terminal_time,{CUTOFF})-landmark_time)
          FROM e
          WHERE track_terminal_event IS NOT NULL AND landmark_time IS NOT NULL
                AND landmark_time < coalesce(terminal_time,{CUTOFF})
          UNION ALL
          SELECT episode_id, track, protocol, 'T2', settlement_time,
                 native_unit_type, actor_role, asset, contract_version,
                 cross_chain_flag, challenge_time IS NOT NULL,
                 true, track_terminal_event, event_observed,
                 greatest(0, coalesce(terminal_time,{CUTOFF})-settlement_time)
          FROM e
          WHERE track_terminal_event IS NOT NULL AND settlement_time IS NOT NULL
                AND settlement_time < coalesce(terminal_time,{CUTOFF})
        ) TO '{_sql_path(prefixes)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.close()


def build_splits() -> None:
    src = OUT / "landmark_prefixes.parquet"
    con = duckdb.connect()
    target = OUT / "splits.parquet"
    con.execute(
        f"""
        COPY (
          WITH base AS (
            SELECT p.*, e.actor
            FROM read_parquet('{_sql_path(src)}') p
            LEFT JOIN read_parquet('{_sql_path(OUT / "financial_episodes.parquet")}') e
            USING (episode_id)
          ), versioned AS (
            SELECT *,
              max(prediction_time) OVER (
                PARTITION BY track,landmark,protocol,contract_version
              ) version_last_time
            FROM base
          ), x AS (
            SELECT *,
              row_number() OVER (PARTITION BY track,landmark ORDER BY prediction_time,episode_id) rn,
              count(prediction_time) OVER (PARTITION BY track,landmark) n,
              abs(hash(episode_id || ':' || track || ':' || landmark || ':{SEED}')) % 10000 h,
              abs(hash(coalesce(actor,'') || ':{SEED}')) % 10 actor_fold,
              dense_rank() OVER (
                PARTITION BY track,landmark,protocol
                ORDER BY version_last_time DESC NULLS LAST
              ) version_recency_rank
            FROM versioned
          )
          SELECT episode_id, track, landmark, protocol, prediction_time,
            CASE WHEN h<7000 THEN 'train' WHEN h<8500 THEN 'validation' ELSE 'test' END
              AS random_split,
            CASE WHEN prediction_time IS NULL THEN 'unavailable'
                 WHEN rn <= floor(n*0.70) THEN 'train'
                 WHEN rn <= floor(n*0.85) THEN 'validation' ELSE 'test' END
              AS chronological_split,
            CASE WHEN actor_fold<=6 THEN 'train'
                 WHEN actor_fold<=8 THEN 'validation' ELSE 'test' END
              AS actor_disjoint_split,
            contract_version,
            CASE WHEN contract_version IS NULL THEN 'unavailable'
                 WHEN version_recency_rank=1 THEN 'test_latest_version'
                 ELSE 'train_earlier_versions' END AS version_holdout_split,
            actor_fold,
            'protocol:' || protocol AS lopo_group
          FROM x
        ) TO '{_sql_path(target)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.close()


def build_descriptives() -> None:
    con = duckdb.connect()
    t = _sql_path(OUT / "track_episodes.parquet")
    con.execute(
        f"""
        COPY (
          SELECT track, protocol, coalesce(track_terminal_event,'excluded_coverage') outcome,
                 count(*) episodes,
                 sum(CASE WHEN right_censored THEN 1 ELSE 0 END) censored,
                 median(observed_duration_seconds/86400.0)
                   FILTER (event_observed) AS median_observed_days
          FROM read_parquet('{t}') GROUP BY 1,2,3
        ) TO '{_sql_path(OUT / "episode_summary.parquet")}'
          (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.execute(
        f"""
        COPY (
          SELECT protocol, asset, native_unit_type,
                 count(*) episodes,
                 quantile_cont(try_cast(capital_days_locked_raw AS DOUBLE),0.25) p25,
                 median(try_cast(capital_days_locked_raw AS DOUBLE)) median,
                 quantile_cont(try_cast(capital_days_locked_raw AS DOUBLE),0.75) p75,
                 quantile_cont(try_cast(capital_days_locked_raw AS DOUBLE),0.95) p95
          FROM read_parquet('{t}')
          WHERE track='F3_capital_settlement' AND capital_days_locked_raw IS NOT NULL
          GROUP BY 1,2,3
        ) TO '{_sql_path(OUT / "capital_lock_distribution.parquet")}'
          (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.execute(
        f"""
        COPY (
          SELECT protocol, native_unit_type,
                 count(*) FILTER (protocol_minimum_wait IS NOT NULL) n_min_wait,
                 median(protocol_minimum_wait/86400.0)
                   FILTER (protocol_minimum_wait IS NOT NULL) mandatory_wait_days,
                 median(adjudication_delay_seconds/86400.0)
                   FILTER (adjudication_delay_seconds IS NOT NULL) adjudication_days,
                 median(post_resolution_delay_seconds/86400.0)
                   FILTER (post_resolution_delay_seconds IS NOT NULL) post_resolution_days,
                 median(excess_delay_seconds/86400.0)
                   FILTER (excess_delay_seconds IS NOT NULL) excess_days
          FROM read_parquet('{t}') GROUP BY 1,2
        ) TO '{_sql_path(OUT / "delay_decomposition.parquet")}'
          (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    # Reuse the release-QC concentration table without changing its semantics.
    old = ROOT / "data/applications/accountability_economics/reward_concentration_metrics.parquet"
    if old.exists():
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{_sql_path(old)}')) "
            f"TO '{_sql_path(OUT / 'reward_concentration.parquet')}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    con.close()


def build_competing_risk_incidence(max_rows_per_track: int = 200000) -> None:
    """Compute an Aalen--Johansen CIF on a fixed, outcome-stratified sample."""
    con = duckdb.connect()
    frame = con.execute(
        f"""
        SELECT track, track_terminal_event, event_observed,
               observed_duration_seconds/86400.0 AS duration_days,
               episode_id
        FROM read_parquet('{_sql_path(OUT / "track_episodes.parquet")}')
        WHERE track_terminal_event IS NOT NULL
              AND observed_duration_seconds > 0
        """
    ).fetchdf()
    con.close()
    sampled = []
    for track, group in frame.groupby("track", sort=False):
        if len(group) > max_rows_per_track:
            parts = []
            outcomes = list(group.track_terminal_event.unique())
            quota = max(100, max_rows_per_track // max(1, len(outcomes)))
            for _, outcome_group in group.groupby("track_terminal_event", sort=False):
                parts.append(outcome_group.sample(
                    min(len(outcome_group), quota), random_state=SEED
                ))
            selected = pd.concat(parts)
            if len(selected) < max_rows_per_track:
                rest = group.drop(index=selected.index, errors="ignore")
                selected = pd.concat([
                    selected,
                    rest.sample(min(len(rest), max_rows_per_track-len(selected)),
                                random_state=SEED),
                ])
            group = selected.head(max_rows_per_track)
        sampled.append(group)
    frame = pd.concat(sampled, ignore_index=True)
    rows = []
    for track, group in frame.groupby("track", sort=False):
        agg = group.groupby(
            ["duration_days", "track_terminal_event"], as_index=False
        ).size().sort_values("duration_days")
        at_risk = len(group)
        survival = 1.0
        cif = {o: 0.0 for o in group.track_terminal_event.unique()
               if o != "right_censored"}
        for duration, time_group in agg.groupby("duration_days", sort=True):
            censor = int(time_group.loc[
                time_group.track_terminal_event.eq("right_censored"), "size"
            ].sum())
            event_rows = time_group[
                ~time_group.track_terminal_event.eq("right_censored")
            ]
            total_events = int(event_rows["size"].sum())
            if at_risk <= 0:
                break
            for _, event_row in event_rows.iterrows():
                cause = event_row.track_terminal_event
                cif[cause] += survival * int(event_row["size"]) / at_risk
            survival *= 1.0 - total_events / at_risk
            for cause, value in cif.items():
                rows.append({
                    "track": track, "duration_days": float(duration),
                    "cause": cause, "cumulative_incidence": float(value),
                    "survival_any_event": float(survival),
                    "risk_set_before": int(at_risk),
                    "sampled_estimate": len(group) < len(frame),
                })
            at_risk -= total_events + censor
    pd.DataFrame(rows).to_parquet(OUT / "aalen_johansen_incidence.parquet",
                                  index=False)


def _km_survival(times: np.ndarray, events: np.ndarray, grid: np.ndarray) -> np.ndarray:
    order = np.argsort(times)
    t, e = times[order], events[order]
    surv, at_risk, pos = 1.0, len(t), 0
    values = []
    for g in grid:
        while pos < len(t) and t[pos] <= g:
            tt = t[pos]
            end = np.searchsorted(t, tt, side="right")
            d = int(e[pos:end].sum())
            if at_risk and d:
                surv *= 1.0 - d / at_risk
            at_risk -= end - pos
            pos = end
        values.append(surv)
    return np.asarray(values)


def run_benchmark(max_rows_per_track: int = 6000) -> None:
    """Run seven executable T0 survival families plus an AJ incidence baseline."""
    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sksurv.ensemble import (
        ExtraSurvivalTrees,
        GradientBoostingSurvivalAnalysis,
        RandomSurvivalForest,
    )
    from sksurv.linear_model import CoxPHSurvivalAnalysis, CoxnetSurvivalAnalysis
    from sksurv.metrics import (
        concordance_index_censored,
        concordance_index_ipcw,
        cumulative_dynamic_auc,
        integrated_brier_score,
    )
    from sksurv.svm import FastSurvivalSVM
    from sksurv.util import Surv
    import xgboost as xgb
    from scipy.stats import norm

    con = duckdb.connect()
    df = con.execute(
        f"""
        SELECT p.*, s.chronological_split, s.random_split,
               s.actor_disjoint_split, s.lopo_group
        FROM read_parquet('{_sql_path(OUT / "landmark_prefixes.parquet")}') p
        JOIN read_parquet('{_sql_path(OUT / "splits.parquet")}') s
        USING (episode_id,track,landmark,protocol,prediction_time)
        WHERE p.landmark='T0' AND p.event_observed IS NOT NULL
              AND p.observed_duration_seconds > 0
              AND p.track_terminal_event <> 'unresolved_coverage'
        """
    ).fetchdf()
    con.close()
    if df.empty:
        raise RuntimeError("No eligible financial prefix observations")
    rng = np.random.default_rng(SEED)
    kept = []
    for _, g in df.groupby("track", sort=False):
        if len(g) > max_rows_per_track:
            # Preserve outcome support inside each chronological partition.
            rare_parts = []
            for _, z in g.groupby(
                ["chronological_split", "track_terminal_event"], sort=False
            ):
                rare_parts.append(z.sample(min(len(z), 500), random_state=SEED))
            rare = pd.concat(rare_parts).drop_duplicates("episode_id")
            remaining = g.drop(index=rare.index, errors="ignore")
            fill_n = max(0, max_rows_per_track - len(rare))
            fill = remaining.sample(min(fill_n, len(remaining)), random_state=SEED)
            g = pd.concat([rare, fill], ignore_index=True)
        kept.append(g)
    df = pd.concat(kept, ignore_index=True)
    df["duration_days"] = df["observed_duration_seconds"].astype(float) / 86400.0
    df["start_year"] = pd.to_datetime(df["prediction_time"], unit="s", utc=True).dt.year
    df["start_month"] = pd.to_datetime(df["prediction_time"], unit="s", utc=True).dt.month

    cat = ["protocol", "native_unit_type", "actor_role", "asset", "contract_version"]
    num = ["start_year", "start_month", "cross_chain_flag"]
    for c in cat:
        df[c] = df[c].fillna("missing").astype(str)
    df["cross_chain_flag"] = df["cross_chain_flag"].fillna(False).astype(int)

    models = {
        "CoxPH": CoxPHSurvivalAnalysis(alpha=1e-3),
        "Coxnet": CoxnetSurvivalAnalysis(
            l1_ratio=0.2, alpha_min_ratio=0.05, fit_baseline_model=True
        ),
        "RandomSurvivalForest": RandomSurvivalForest(
            n_estimators=60, min_samples_leaf=20, max_features="sqrt",
            n_jobs=min(8, os.cpu_count() or 1), random_state=SEED,
        ),
        "ExtraSurvivalTrees": ExtraSurvivalTrees(
            n_estimators=60, min_samples_leaf=20, max_features="sqrt",
            n_jobs=min(8, os.cpu_count() or 1), random_state=SEED,
        ),
        "GradientBoostingSurvival": GradientBoostingSurvivalAnalysis(
            n_estimators=50, learning_rate=0.05, max_depth=2,
            random_state=SEED,
        ),
        "FastSurvivalSVM": FastSurvivalSVM(
            alpha=1.0, rank_ratio=1.0, max_iter=100, random_state=SEED,
        ),
    }
    metric_rows, pred_rows, cal_rows = [], [], []
    model_registry = [
        ("KaplanMeier", "executed", "nonparametric marginal baseline"),
        ("AalenJohansen", "executed", "nonparametric competing-risk incidence"),
        *[(k, "executed", "individualized T0 survival") for k in models],
        ("FineGray", "not_executed", "no stable compatible implementation in environment"),
        ("XGBoostAFT", "executed", "log-normal AFT with censoring bounds"),
        ("DeepSurv", "not_executed", "PyTorch training stack unavailable"),
        ("DeepHit", "not_executed", "PyTorch training stack unavailable"),
        ("SequenceSurvival", "not_executed", "requires preregistered sequence architecture"),
    ]

    for track, d in df.groupby("track", sort=False):
        chrono_train = d[d.chronological_split == "train"]
        chrono_test = d[d.chronological_split == "test"]
        if (chrono_train.event_observed.nunique() >= 2
                and chrono_test.event_observed.nunique() >= 2):
            train, test = chrono_train.copy(), chrono_test.copy()
            evaluation_name = "pooled_chronological"
        else:
            # Chronological infeasibility is itself retained in split diagnostics;
            # random evaluation is a secondary, explicitly weaker benchmark.
            train = d[d.random_split == "train"].copy() if "random_split" in d else d.iloc[0:0]
            test = d[d.random_split == "test"].copy() if "random_split" in d else d.iloc[0:0]
            evaluation_name = "pooled_random_fallback"
        if len(train) < 100 or len(test) < 30 or train.event_observed.nunique() < 2:
            continue
        prep = ColumnTransformer(
            [("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat),
             ("num", StandardScaler(), num)],
            sparse_threshold=0,
        )
        Xtr = prep.fit_transform(train[cat + num])
        Xte = prep.transform(test[cat + num])
        ytr = Surv.from_arrays(train.event_observed.astype(bool), train.duration_days)
        yte = Surv.from_arrays(test.event_observed.astype(bool), test.duration_days)
        upper = min(float(np.quantile(train.duration_days, 0.90)),
                    float(np.quantile(test.duration_days, 0.90)))
        lower = max(float(np.quantile(train.duration_days, 0.10)), 1e-5)
        grid = np.linspace(lower, upper, 24)
        km = _km_survival(train.duration_days.to_numpy(),
                          train.event_observed.to_numpy(bool), grid)
        km_pred = np.tile(km, (len(test), 1))
        try:
            uno = concordance_index_ipcw(ytr, yte, np.zeros(len(test)), tau=upper)[0]
            ibs = integrated_brier_score(ytr, yte, km_pred, grid)
        except Exception:
            uno, ibs = np.nan, np.nan
        metric_rows.append(dict(track=track, evaluation=evaluation_name,
                                model="KaplanMeier", n_train=len(train), n_test=len(test),
                                uno_c_index=uno, integrated_brier_score=ibs,
                                time_dependent_auc=np.nan, calibration_error=float("nan"),
                                median_time_mae=np.nan, interval_coverage=np.nan))
        for name, model in models.items():
            try:
                model.fit(Xtr, ytr)
                risk = np.asarray(model.predict(Xte), dtype=float)
                harrell = concordance_index_censored(
                    test.event_observed.to_numpy(bool),
                    test.duration_days.to_numpy(float), risk
                )[0]
                try:
                    uno = concordance_index_ipcw(ytr, yte, risk, tau=upper)[0]
                    auc = float(np.mean(cumulative_dynamic_auc(ytr, yte, risk, grid)[0]))
                except Exception:
                    uno, auc = np.nan, np.nan
                surv_pred = None
                if hasattr(model, "predict_survival_function"):
                    sf = model.predict_survival_function(Xte)
                    surv_pred = np.vstack([[float(f(g)) for g in grid] for f in sf])
                if surv_pred is not None:
                    try:
                        ibs = integrated_brier_score(ytr, yte, surv_pred, grid)
                    except Exception:
                        ibs = np.nan
                    hidx = len(grid) // 2
                    observed_event = (
                        test.event_observed.to_numpy(bool)
                        & (test.duration_days.to_numpy(float) <= grid[hidx])
                    ).astype(float)
                    cal = abs(float(np.mean(1-surv_pred[:, hidx]))
                              - float(observed_event.mean()))
                    med = np.array([
                        grid[np.where(row <= 0.5)[0][0]]
                        if np.any(row <= 0.5) else np.nan for row in surv_pred
                    ])
                    completed = test.event_observed.to_numpy(bool) & np.isfinite(med)
                    med_mae = (float(np.mean(np.abs(
                        med[completed]-test.duration_days.to_numpy(float)[completed]
                    ))) if completed.any() else np.nan)
                    for j, eid in enumerate(test.episode_id.astype(str)):
                        pred_rows.append(dict(episode_id=eid, track=track, model=name,
                                              risk_score=float(risk[j]),
                                              horizon_days=float(grid[hidx]),
                                              event_probability=float(1-surv_pred[j,hidx])))
                    for j, g in enumerate(grid):
                        cal_rows.append(dict(track=track, model=name, horizon_days=float(g),
                                             mean_predicted_event=float(np.mean(1-surv_pred[:,j]))))
                else:
                    ibs = cal = med_mae = np.nan
                metric_rows.append(dict(
                    track=track, evaluation=evaluation_name, model=name,
                    n_train=len(train), n_test=len(test), harrell_c_index=harrell,
                    uno_c_index=uno, integrated_brier_score=ibs,
                    time_dependent_auc=auc, calibration_error=cal,
                    median_time_mae=med_mae, interval_coverage=np.nan,
                ))
            except Exception as exc:
                metric_rows.append(dict(
                    track=track, evaluation=evaluation_name, model=name,
                    n_train=len(train), n_test=len(test), status="failed",
                    error=str(exc)[:300],
                ))
        try:
            dtrain = xgb.DMatrix(Xtr)
            dtest = xgb.DMatrix(Xte)
            lower_bound = train.duration_days.to_numpy(float)
            upper_bound = lower_bound.copy()
            upper_bound[~train.event_observed.to_numpy(bool)] = np.inf
            dtrain.set_float_info("label_lower_bound", lower_bound)
            dtrain.set_float_info("label_upper_bound", upper_bound)
            booster = xgb.train({
                "objective": "survival:aft",
                "eval_metric": "aft-nloglik",
                "aft_loss_distribution": "normal",
                "aft_loss_distribution_scale": 1.0,
                "tree_method": "hist",
                "max_depth": 3,
                "eta": 0.05,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "seed": SEED,
                "nthread": min(8, os.cpu_count() or 1),
            }, dtrain, num_boost_round=120, verbose_eval=False)
            predicted_time = np.maximum(booster.predict(dtest), 1e-6)
            risk = -np.log(predicted_time)
            surv_pred = np.vstack([
                norm.sf((np.log(np.maximum(grid,1e-6))-np.log(pt))/1.0)
                for pt in predicted_time
            ])
            uno = concordance_index_ipcw(ytr, yte, risk, tau=upper)[0]
            auc = float(np.mean(cumulative_dynamic_auc(ytr,yte,risk,grid)[0]))
            ibs = integrated_brier_score(ytr,yte,surv_pred,grid)
            hidx=len(grid)//2
            observed_event=(test.event_observed.to_numpy(bool)
                            &(test.duration_days.to_numpy(float)<=grid[hidx])).astype(float)
            cal=abs(float(np.mean(1-surv_pred[:,hidx]))-float(observed_event.mean()))
            completed=test.event_observed.to_numpy(bool)
            med_mae=float(np.mean(np.abs(
                predicted_time[completed]-test.duration_days.to_numpy(float)[completed]
            ))) if completed.any() else np.nan
            metric_rows.append(dict(
                track=track,evaluation=evaluation_name,model="XGBoostAFT",
                n_train=len(train),n_test=len(test),
                harrell_c_index=concordance_index_censored(
                    test.event_observed.to_numpy(bool),
                    test.duration_days.to_numpy(float),risk)[0],
                uno_c_index=uno,integrated_brier_score=ibs,
                time_dependent_auc=auc,calibration_error=cal,
                median_time_mae=med_mae,interval_coverage=np.nan,
            ))
            for j,eid in enumerate(test.episode_id.astype(str)):
                pred_rows.append(dict(
                    episode_id=eid,track=track,model="XGBoostAFT",
                    risk_score=float(risk[j]),horizon_days=float(grid[hidx]),
                    event_probability=float(1-surv_pred[j,hidx]),
                ))
            for j,g in enumerate(grid):
                cal_rows.append(dict(
                    track=track,model="XGBoostAFT",horizon_days=float(g),
                    mean_predicted_event=float(np.mean(1-surv_pred[:,j])),
                ))
        except Exception as exc:
            metric_rows.append(dict(
                track=track,evaluation=evaluation_name,model="XGBoostAFT",
                n_train=len(train),n_test=len(test),status="failed",
                error=str(exc)[:300],
            ))

    pd.DataFrame(metric_rows).to_parquet(OUT / "metrics.parquet", index=False)
    pd.DataFrame(pred_rows).to_parquet(OUT / "predictions.parquet", index=False)
    pd.DataFrame(cal_rows).to_parquet(OUT / "calibration.parquet", index=False)
    pd.DataFrame(model_registry, columns=["model", "status", "note"]).to_parquet(
        OUT / "model_registry.parquet", index=False
    )
    _write_json(OUT / "benchmark_config.json", {
        "cutoff_unix": CUTOFF,
        "random_seed": SEED,
        "primary_landmark": "T0",
        "split": "chronological 70/15/15",
        "maximum_rows_per_track": max_rows_per_track,
        "feature_policy": "only static fields visible at episode initiation",
        "unknown_unavailable_are_negative": False,
        "asset_aggregation": "none",
    })


def _save_figure(fig, stem: str, frame: pd.DataFrame) -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / f"{stem}.pdf", bbox_inches="tight")
    fig.savefig(FIG / f"{stem}.png", dpi=300, bbox_inches="tight")
    frame.to_csv(FIG / f"{stem}.csv", index=False)
    frame.to_parquet(FIG / f"{stem}.parquet", index=False)


def render_figures() -> None:
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_theme(style="whitegrid", context="paper")
    colors = ["#2F6BFF", "#F28E2B", "#2A9D8F", "#8E5BD9", "#D1495B", "#5B6770"]

    metrics = pd.read_parquet(OUT / "metrics.parquet")
    ok = metrics.dropna(subset=["uno_c_index"]).copy()
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    if len(ok):
        pivot = ok.pivot_table(index="model", columns="track",
                               values="uno_c_index", aggfunc="mean")
        pivot.plot.bar(ax=ax, color=colors[:len(pivot.columns)], width=.78)
    ax.set_ylabel("Uno C-index"); ax.set_xlabel("")
    ax.set_title("T0 survival discrimination (chronological holdout)")
    ax.legend(frameon=False, fontsize=7)
    _save_figure(fig, "fig_financial_model_benchmark", ok)
    plt.close(fig)

    cr = pd.read_parquet(OUT / "aalen_johansen_incidence.parquet")
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    if len(cr):
        for i, ((track,cause), g) in enumerate(cr.groupby(["track","cause"])):
            ax.plot(g.duration_days, g.cumulative_incidence,
                    label=f"{track[:2]} · {cause}",
                    color=colors[i % len(colors)])
    ax.set_xscale("symlog", linthresh=1)
    ax.set_xlabel("Days since T0"); ax.set_ylabel("Aalen–Johansen cumulative incidence")
    ax.set_title("Competing outcomes with right-censoring")
    ax.legend(frameon=False, fontsize=6, ncol=2)
    _save_figure(fig, "fig_competing_risk_incidence", cr)
    plt.close(fig)

    cal = pd.read_parquet(OUT / "calibration.parquet")
    fig, ax = plt.subplots(figsize=(6.2, 3.5))
    for i, (name, g) in enumerate(cal.groupby("model")):
        ax.plot(g.horizon_days, g.mean_predicted_event, label=name,
                color=colors[i % len(colors)], alpha=.9)
    ax.set_xlabel("Horizon (days)"); ax.set_ylabel("Mean predicted event probability")
    ax.set_title("Realization probability across registered horizons")
    ax.legend(frameon=False, fontsize=6, ncol=2)
    _save_figure(fig, "fig_survival_calibration", cal)
    plt.close(fig)

    delay = pd.read_parquet(OUT / "delay_decomposition.parquet")
    cols = ["mandatory_wait_days","adjudication_days","post_resolution_days","excess_days"]
    dplot = delay.melt(id_vars=["protocol","native_unit_type"], value_vars=cols,
                       var_name="component", value_name="median_days").dropna()
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    if len(dplot):
        sns.barplot(data=dplot, x="native_unit_type", y="median_days",
                    hue="component", palette=colors[:4], ax=ax)
        ax.tick_params(axis="x", rotation=25)
    ax.set_yscale("symlog", linthresh=1); ax.set_xlabel(""); ax.set_ylabel("Median days")
    ax.set_title("Protocol-native delay decomposition")
    ax.legend(frameon=False, fontsize=6)
    _save_figure(fig, "fig_delay_decomposition", dplot)
    plt.close(fig)

    cap = pd.read_parquet(OUT / "capital_lock_distribution.parquet")
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    if len(cap):
        x = np.arange(len(cap))
        ax.scatter(x, cap["median"], color=colors[0], label="median")
        ax.vlines(x, cap["p25"], cap["p75"], color=colors[0], lw=2)
        ax.set_xticks(x, cap.protocol + " · " + cap.asset.fillna("unknown"),
                      rotation=30, ha="right")
    ax.set_yscale("symlog", linthresh=1); ax.set_ylabel("Raw asset-units × days")
    ax.set_title("Capital-days locked, never aggregated across assets")
    _save_figure(fig, "fig_capital_days_locked", cap)
    plt.close(fig)

    conc = pd.read_parquet(OUT / "reward_concentration.parquet")
    fig, ax = plt.subplots(figsize=(6.8, 3.5))
    numeric = [c for c in ["top1_share","top5_share","top10_share","hhi","gini"]
               if c in conc.columns]
    if numeric:
        cplot = conc.melt(id_vars=[c for c in ["protocol"] if c in conc.columns],
                          value_vars=numeric, var_name="metric", value_name="value")
        sns.barplot(data=cplot, x="metric", y="value",
                    hue="protocol" if "protocol" in cplot else None, ax=ax)
    else:
        cplot = conc
        ax.text(.5,.5,"Released concentration schema has no standard metric columns",
                ha="center",va="center",transform=ax.transAxes)
    ax.set_title("Reward concentration (descriptive, non-causal)")
    _save_figure(fig, "fig_reward_concentration", cplot)
    plt.close(fig)


def write_manifest() -> None:
    files = sorted(
        p for p in OUT.iterdir()
        if p.is_file() and p.name not in {"manifest.json", "checksums.csv"}
    )
    rows = []
    for p in files:
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        rows.append({"file": p.name, "bytes": p.stat().st_size, "sha256": h.hexdigest()})
    pd.DataFrame(rows).to_csv(OUT / "checksums.csv", index=False)
    _write_json(OUT / "manifest.json", {
        "application": "Oracle Economic Realization and Capital Friction Benchmark",
        "cutoff_unix": CUTOFF, "random_seed": SEED, "files": rows,
    })


def run_all() -> None:
    random.seed(SEED)
    np.random.seed(SEED)
    build_episodes()
    build_splits()
    build_descriptives()
    build_competing_risk_incidence()
    run_benchmark()
    render_figures()
    write_manifest()


if __name__ == "__main__":
    run_all()
