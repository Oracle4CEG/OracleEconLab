#!/usr/bin/env python3
"""Evaluate zero-shot coverage fairness from UMA to smaller-chain Tellor.

The shared estimand is conditional challenge success under each protocol's own
adjudication.  Only dimensionless and timing features with qualified common
economic meaning are used; native assets are never pooled or converted by raw
amount.  Tellor is held out entirely for a genuine protocol-transfer audit.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "data/applications/trustworthy_ai_challenge"
CURATED = ROOT / "data/curated/parquet"
OUT = ROOT / "data/applications/trustworthy_ai_cross_protocol_fairness"
REPORT = ROOT / "reports/trustworthy_ai_cross_protocol_fairness.md"
FEATURES = ["penalty_to_challenge_capital", "report_age_hours", "challenge_window_hours"]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def action(probability: float) -> str:
    if probability <= 0.20:
        return "Accept"
    if probability < 0.40:
        return "Investigate"
    if probability < 0.60:
        return "Abstain"
    if probability < 0.80:
        return "Investigate"
    return "Challenge"


def timestamp(value: str) -> float:
    return pd.Timestamp(value).timestamp()


def build_common_samples() -> pd.DataFrame:
    uma = pd.read_parquet(BASE / "decision_samples.parquet").merge(
        pd.read_parquet(BASE / "splits.parquet")[["sample_id", "split"]], on="sample_id", validate="one_to_one"
    )
    uma_rows = pd.DataFrame({
        "sample_id": uma.sample_id, "protocol": "UMA", "chain": "Polygon+Ethereum",
        "asset": "USDC/USDC.e", "decision_time_unix": uma.decision_time_unix,
        "penalty_to_challenge_capital": 1.0,
        "report_age_hours": uma.proposal_latency_hours,
        "challenge_window_hours": uma.liveness_hours,
        "challenge_succeeds_under_protocol": uma.proposal_rejected_by_protocol.astype(int),
        "source_split": uma.split, "ground_truth_status": "protocol_resolution_only",
    })
    tellor = pd.read_parquet(CURATED / "tellor_disputes.parquet").sort_values(["dispute_start_time", "dispute_id"])
    tellor_rows = []
    for row in tellor.itertuples(index=False):
        capital = int(row.dispute_fee_raw)
        success = str(row.vote_result) in {"SUPPORT", "NO_QUORUM_MAJORITY_SUPPORT"}
        start = timestamp(row.dispute_start_time)
        end = timestamp(row.dispute_end_time)
        tellor_rows.append({
            "sample_id": f"tellor:{row.dispute_id}", "protocol": "Tellor", "chain": "tellor-1",
            "asset": row.asset, "decision_time_unix": int(start),
            "penalty_to_challenge_capital": int(row.slash_amount_raw) / capital if capital else None,
            "report_age_hours": (start - int(row.report_timestamp_ms) / 1000) / 3600,
            "challenge_window_hours": (end - start) / 3600,
            "challenge_succeeds_under_protocol": int(success), "source_split": "heldout_protocol",
            "ground_truth_status": "protocol_resolution_only",
        })
    frame = pd.concat([uma_rows, pd.DataFrame(tellor_rows)], ignore_index=True)
    return frame


def evaluate(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train = frame[(frame.protocol == "UMA") & frame.source_split.isin(["train", "validation"])]
    tests = {
        "UMA_chronological_test": frame[(frame.protocol == "UMA") & (frame.source_split == "test")],
        "Tellor_zero_shot": frame[frame.protocol == "Tellor"],
    }
    model = Pipeline([
        ("scale", StandardScaler()),
        ("model", LogisticRegression(C=1.0, class_weight="balanced", max_iter=2000, random_state=20260806)),
    ])
    model.fit(train[FEATURES], train.challenge_succeeds_under_protocol)
    prediction_rows, metrics = [], []
    for evaluation, part in tests.items():
        probabilities = model.predict_proba(part[FEATURES])[:, 1]
        actions = [action(value) for value in probabilities]
        y = part.challenge_succeeds_under_protocol.to_numpy()
        automated = np.isin(actions, ["Accept", "Challenge"])
        errors = np.array([
            (a == "Accept" and outcome == 1) or (a == "Challenge" and outcome == 0)
            for a, outcome in zip(actions, y)
        ])
        for sample, probability, chosen, error in zip(part.itertuples(index=False), probabilities, actions, errors):
            prediction_rows.append({
                "sample_id": sample.sample_id, "protocol": sample.protocol, "chain": sample.chain,
                "evaluation": evaluation, "probability_challenge_succeeds": float(probability),
                "action": chosen, "protocol_outcome": int(sample.challenge_succeeds_under_protocol),
                "automated_error": bool(error) if chosen in {"Accept", "Challenge"} else None,
                "ground_truth_status": "protocol_resolution_only",
            })
        metrics.append({
            "evaluation": evaluation, "protocol": part.protocol.iloc[0], "observations": len(part),
            "positive_rate": float(y.mean()), "roc_auc": float(roc_auc_score(y, probabilities)),
            "brier_score": float(brier_score_loss(y, probabilities)),
            "log_loss": float(log_loss(y, probabilities)),
            "automated_coverage": float(automated.mean()),
            "automated_selective_error": float(errors[automated].mean()) if automated.any() else None,
            "review_burden": float(np.isin(actions, ["Investigate", "Abstain"]).mean()),
        })
    predictions = pd.DataFrame(prediction_rows)
    metric_frame = pd.DataFrame(metrics)
    scaler: StandardScaler = model.named_steps["scale"]
    shift_rows = []
    for feature, mean, scale in zip(FEATURES, scaler.mean_, scaler.scale_):
        for protocol, part in frame.groupby("protocol"):
            standardized = (part[feature] - mean) / scale
            shift_rows.append({
                "feature": feature, "protocol": protocol, "observations": len(part),
                "mean_in_uma_train_sd": float(standardized.mean()),
                "p95_abs_in_uma_train_sd": float(np.quantile(np.abs(standardized), 0.95)),
            })
    return predictions, metric_frame, pd.DataFrame(shift_rows)


def validate(samples: pd.DataFrame, predictions: pd.DataFrame, metrics: pd.DataFrame) -> dict[str, bool]:
    tellor = metrics[metrics.protocol.eq("Tellor")].iloc[0]
    checks = {
        "all_823_samples_present": len(samples) == 823,
        "all_13_tellor_held_out": len(samples[samples.protocol.eq("Tellor")]) == 13,
        "both_tellor_outcomes_present": samples[samples.protocol.eq("Tellor")].challenge_succeeds_under_protocol.nunique() == 2,
        "native_assets_not_model_features": "asset" not in FEATURES,
        "protocol_identity_not_model_feature": "protocol" not in FEATURES,
        "all_predictions_protocol_only": predictions.ground_truth_status.eq("protocol_resolution_only").all(),
        "tellor_zero_shot_metrics_present": int(tellor.observations) == 13,
    }
    if not all(checks.values()):
        raise RuntimeError(f"cross-protocol fairness QC failed: {checks}")
    return {key: bool(value) for key, value in checks.items()}


def main() -> None:
    samples = build_common_samples()
    predictions, metrics, shift = evaluate(samples)
    checks = validate(samples, predictions, metrics)
    OUT.mkdir(parents=True, exist_ok=True)
    objects = {
        "common_decision_samples": samples, "cross_protocol_predictions": predictions,
        "cross_protocol_metrics": metrics, "feature_shift": shift,
    }
    for name, value in objects.items():
        value.to_parquet(OUT / f"{name}.parquet", index=False)
        value.to_csv(OUT / f"{name}.csv", index=False)
    REPORT.write_text(
        "# Cross-protocol and smaller-chain coverage-fairness experiment\n\n"
        "A model is trained only on UMA train+validation observations and transferred without Tellor labels to all 13 resolved tellor-1 disputes. Native amounts and protocol identity are excluded; only a dimensionless penalty/capital ratio and two timing variables are shared.\n\n"
        + metrics.to_markdown(index=False) + "\n\n"
        "This is a coverage and transfer-stress experiment, not a protocol ranking. Tellor has only 13 observations, institutional labels differ, and all endpoints are protocol resolutions rather than independent truth.\n",
        encoding="utf-8",
    )
    files = []
    for path in sorted(OUT.glob("*")):
        if path.is_file() and path.name != "manifest.json":
            files.append({"path": str(path), "bytes": path.stat().st_size, "sha256": sha256(path)})
    files.append({"path": str(REPORT), "bytes": REPORT.stat().st_size, "sha256": sha256(REPORT)})
    (OUT / "manifest.json").write_text(json.dumps({
        "dataset": "Cross-protocol smaller-chain fairness experiment", "version": "1.0.0",
        "generated_at_utc": datetime.now(UTC).isoformat(), "features": FEATURES,
        "all_required_assertions_pass": True, "checks": checks,
        "rows": {name: len(value) for name, value in objects.items()}, "files": files,
    }, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"checks": checks, "metrics": metrics.to_dict("records")}, indent=2))


if __name__ == "__main__":
    main()
