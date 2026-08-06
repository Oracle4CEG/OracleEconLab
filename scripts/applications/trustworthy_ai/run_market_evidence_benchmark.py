#!/usr/bin/env python3
"""Evaluate proposal-time Polymarket price evidence on the fixed UMA task."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from scripts.applications.trustworthy_ai.run_challenge_benchmark import (  # noqa: E402
    CATEGORICAL_FEATURES, MODEL_FEATURES, NUMERIC_FEATURES, SEEDS,
    canonical_hash, ece, four_action, nominal_cost,
)


BASE = ROOT / "data/applications/trustworthy_ai_challenge"
PRICES = ROOT / "data/curated/parquet/polymarket_decision_time_prices.parquet"
PRICE_PROVENANCE = ROOT / "data/curated/parquet/polymarket_decision_time_price_provenance.parquet"
OUT = ROOT / "data/applications/trustworthy_ai_market_evidence"
REPORT = ROOT / "reports/trustworthy_ai_market_evidence.md"
BOOTSTRAP_SEED = 20260806
MARKET_FEATURES = [
    "aligned_probability", "opposing_probability", "price_sum", "market_confidence",
    "aligned_price_age_seconds", "aligned_momentum_1h", "aligned_momentum_6h",
    "aligned_momentum_24h", "aligned_volatility_24h", "aligned_range_24h",
    "total_points_1h", "total_points_6h", "total_points_24h", "total_points_168h",
]


def load_frame() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    samples = pd.read_parquet(BASE / "decision_samples.parquet")
    splits = pd.read_parquet(BASE / "splits.parquet")
    base_predictions = pd.read_parquet(BASE / "predictions.parquet")
    prices = pd.read_parquet(PRICES)
    frame = samples.merge(splits, on=["sample_id", "decision_time_unix"], validate="one_to_one")
    frame = frame.merge(prices, on=["sample_id", "decision_time_unix"], validate="one_to_one")
    yes = frame.proposed_price_class.eq("binary_one")
    no = frame.proposed_price_class.eq("binary_zero")
    frame["aligned_probability"] = np.where(yes, frame.yes_last_price, np.where(no, frame.no_last_price, np.nan))
    frame["opposing_probability"] = np.where(yes, frame.no_last_price, np.where(no, frame.yes_last_price, np.nan))
    for suffix in ["price_age_seconds", "momentum_1h", "momentum_6h", "momentum_24h", "volatility_24h", "range_24h"]:
        frame[f"aligned_{suffix}"] = np.where(yes, frame[f"yes_{suffix}"], np.where(no, frame[f"no_{suffix}"], np.nan))
    for hours in [1, 6, 24, 168]:
        frame[f"total_points_{hours}h"] = frame[f"yes_points_{hours}h"] + frame[f"no_points_{hours}h"]
    return frame, base_predictions, pd.read_parquet(PRICE_PROVENANCE)


def preprocess(mode: str) -> ColumnTransformer:
    transformers: list[tuple[str, Any, Any]] = [
        ("market", Pipeline([
            ("impute", SimpleImputer(strategy="median", add_indicator=True)),
            ("scale", StandardScaler()),
        ]), MARKET_FEATURES),
    ]
    if mode == "structured_market":
        transformers.extend([
            ("numeric", StandardScaler(), NUMERIC_FEATURES),
            ("categorical", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ])
    return ColumnTransformer(transformers)


def fit(frame: pd.DataFrame, mode: str) -> tuple[np.ndarray, np.ndarray, pd.DataFrame]:
    train = frame.split.eq("train").to_numpy()
    valid = frame.split.eq("validation").to_numpy()
    test = frame.split.eq("test").to_numpy()
    y = frame.proposal_rejected_by_protocol.to_numpy()
    features = MARKET_FEATURES if mode == "market_only" else MARKET_FEATURES + MODEL_FEATURES
    X = frame[features]
    train_indices = np.flatnonzero(train)
    runs = []
    coefficients = []
    for seed in SEEDS:
        rng = np.random.default_rng(seed + BOOTSTRAP_SEED)
        bootstrap = rng.choice(train_indices, len(train_indices), replace=True)
        pipe = Pipeline([
            ("preprocess", preprocess(mode)),
            ("model", LogisticRegression(max_iter=4000, class_weight="balanced", random_state=seed)),
        ])
        pipe.fit(X.iloc[bootstrap], y[bootstrap])
        valid_p = np.clip(pipe.predict_proba(X.loc[valid])[:, 1], 1e-6, 1 - 1e-6)
        calibrator = LogisticRegression(random_state=seed).fit(
            np.log(valid_p / (1 - valid_p)).reshape(-1, 1), y[valid]
        )
        test_p = np.clip(pipe.predict_proba(X.loc[test])[:, 1], 1e-6, 1 - 1e-6)
        runs.append(calibrator.predict_proba(np.log(test_p / (1 - test_p)).reshape(-1, 1))[:, 1])
        for name, value in zip(pipe.named_steps["preprocess"].get_feature_names_out(), pipe.named_steps["model"].coef_[0]):
            coefficients.append({"model": mode, "seed": seed, "feature": name, "coefficient": float(value)})
    matrix = np.vstack(runs)
    return matrix.mean(axis=0), matrix.std(axis=0), pd.DataFrame(coefficients)


def metric(model: str, y: np.ndarray, p: np.ndarray, actions: list[str]) -> dict[str, Any]:
    automated = np.isin(actions, ["Accept", "Challenge"])
    errors = np.array([
        (action == "Accept" and outcome == 1) or (action == "Challenge" and outcome == 0)
        for action, outcome in zip(actions, y)
    ])
    return {
        "model": model, "n_test": len(y), "roc_auc": roc_auc_score(y, p),
        "brier_score": brier_score_loss(y, p), "log_loss": log_loss(y, np.clip(p, 1e-6, 1 - 1e-6)),
        "ece_10": ece(y, p), "automated_coverage": automated.mean(),
        "automated_selective_error": errors[automated].mean() if automated.any() else None,
        "investigate_rate": np.mean(np.asarray(actions) == "Investigate"),
        "abstain_rate": np.mean(np.asarray(actions) == "Abstain"),
        "stylized_normalized_cost": np.mean([nominal_cost(a, int(o)) for a, o in zip(actions, y)]),
    }


def paired_bootstrap(y: np.ndarray, base: np.ndarray, alternatives: dict[str, np.ndarray]) -> pd.DataFrame:
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    distributions = {(name, metric): [] for name in alternatives for metric in ["auc_delta", "brier_delta"]}
    for _ in range(2000):
        index = rng.integers(0, len(y), len(y))
        if len(set(y[index])) < 2:
            continue
        for name, p in alternatives.items():
            distributions[(name, "auc_delta")].append(roc_auc_score(y[index], p[index]) - roc_auc_score(y[index], base[index]))
            distributions[(name, "brier_delta")].append(brier_score_loss(y[index], p[index]) - brier_score_loss(y[index], base[index]))
    rows = []
    for name, p in alternatives.items():
        observed = {
            "auc_delta": roc_auc_score(y, p) - roc_auc_score(y, base),
            "brier_delta": brier_score_loss(y, p) - brier_score_loss(y, base),
        }
        for metric_name, estimate in observed.items():
            values = distributions[(name, metric_name)]
            rows.append({
                "comparison": f"{name}_minus_structured_base", "metric": metric_name,
                "estimate": estimate, "ci_2_5": float(np.quantile(values, .025)),
                "ci_97_5": float(np.quantile(values, .975)), "bootstrap_replicates": len(values),
                "seed": BOOTSTRAP_SEED,
            })
    return pd.DataFrame(rows)


def price_evidence_provenance(frame: pd.DataFrame, raw_provenance: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in frame[["sample_id", "decision_time_unix", "coverage_status"]].merge(
        raw_provenance, on=["sample_id", "decision_time_unix"], validate="one_to_one"
    ).itertuples(index=False):
        payload = {
            "sample_id": row.sample_id, "evidence_kind": "official_clob_historical_price_index",
            "decision_time_unix": int(row.decision_time_unix), "request_end_ts": int(row.request_end_ts),
            "raw_sha256": row.raw_sha256, "source": row.source,
        }
        rows.append({
            **payload, "evidence_id": canonical_hash(payload), "evidence_grade": row.evidence_grade,
            "available_at_decision": int(row.request_end_ts) <= int(row.decision_time_unix),
            "coverage_status": row.coverage_status,
        })
    return pd.DataFrame(rows)


def predictions(test: pd.DataFrame, provenance: pd.DataFrame, model: str, p: np.ndarray, sd: np.ndarray) -> pd.DataFrame:
    pids = provenance.set_index("sample_id").evidence_id
    rows = []
    for index, row in test.reset_index(drop=True).iterrows():
        action = four_action(float(p[index]))
        rows.append({
            "sample_id": row.sample_id, "model": model,
            "probability_proposal_rejected": float(p[index]), "probability_std_across_runs": float(sd[index]),
            "action": action, "confidence": float(max(p[index], 1 - p[index])),
            "protocol_outcome": int(row.proposal_rejected_by_protocol),
            "evidence_ids": json.dumps(json.loads(row.evidence_ids) + [pids[row.sample_id]]),
            "price_evidence_coverage": row.coverage_status,
            "ground_truth_status": "protocol_resolution_only",
            "explanation": "Official CLOB price history ending at proposal time; Grade-B indexed evidence, with missing values explicitly imputed.",
        })
    return pd.DataFrame(rows)


def validate(frame: pd.DataFrame, provenance: pd.DataFrame, output_predictions: pd.DataFrame, comparison: pd.DataFrame) -> pd.DataFrame:
    checks = {
        "same_fixed_sample": len(frame) == 810 and frame.sample_id.nunique() == 810,
        "exact_market_coverage_795": frame.coverage_status.eq("complete").sum() == 795,
        "fifteen_unavailable_not_zero": frame.coverage_status.ne("complete").sum() == 15,
        "all_price_requests_end_by_decision": provenance.available_at_decision.all(),
        "probabilities_bounded": output_predictions.probability_proposal_rejected.between(0, 1).all(),
        "outputs_protocol_only": output_predictions.ground_truth_status.eq("protocol_resolution_only").all(),
        "paired_bootstrap_complete": len(comparison) == 6 and comparison.bootstrap_replicates.ge(1900).all(),
    }
    result = pd.DataFrame([{"check": name, "passed": bool(value)} for name, value in checks.items()])
    failed = result.loc[~result.passed, "check"].tolist()
    if failed:
        raise RuntimeError(f"market evidence benchmark QC failed: {failed}")
    return result


def write_report(metrics: pd.DataFrame, comparison: pd.DataFrame, output_predictions: pd.DataFrame, qc: pd.DataFrame) -> None:
    rows = [
        "# Decision-time market evidence benchmark", "",
        "## Evidence", "",
        "Official Polymarket CLOB price histories were requested with `endTs` exactly equal to each OOV2 proposal timestamp. Primary/secondary token orientation was resolved by the official token-to-market endpoint. The frozen index covers 795/810 cases; 15 missing links remain null and are never encoded as zero.", "",
        "The evidence is Grade B: historical API responses are checksum-frozen and time-bounded, but this release has not decoded the complete Polygon `OrderFilled` ledger.", "",
        "## Test result", "",
        "| Model | AUC | Brier | ECE | Automated coverage | Selective error | Abstain |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in metrics.itertuples(index=False):
        selective = "n/a" if pd.isna(row.automated_selective_error) else f"{row.automated_selective_error:.3f}"
        rows.append(f"| {row.model} | {row.roc_auc:.3f} | {row.brier_score:.3f} | {row.ece_10:.3f} | {row.automated_coverage:.3f} | {selective} | {row.abstain_rate:.3f} |")
    rows += ["", "## Paired comparison", ""]
    for row in comparison.query("metric == 'auc_delta'").itertuples(index=False):
        rows.append(f"- `{row.comparison}`: AUC delta {row.estimate:+.3f}, 95% bootstrap CI [{row.ci_2_5:+.3f}, {row.ci_97_5:+.3f}].")
    actions = output_predictions.groupby(["model", "action"]).size()
    rows += ["", "## Four-action outputs", ""]
    for (model, action), count in actions.items():
        rows.append(f"- `{model}` / `{action}`: {count}")
    rows += [
        "", "The endpoint remains UMA protocol adjudication among actually challenged requests. Market prices may reflect collective information, but the analysis does not establish independent truth or causal value of challenging.",
        "", "## QC", "",
    ]
    rows += [f"- `{row.check}`: PASS" for row in qc.itertuples(index=False)]
    rows += ["", "## Reproduction", "", "```bash", "python scripts/applications/trustworthy_ai/run_market_evidence_benchmark.py", "```", ""]
    REPORT.write_text("\n".join(rows), encoding="utf-8")


def write_outputs(objects: dict[str, pd.DataFrame]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, frame in objects.items():
        frame.to_parquet(OUT / f"{name}.parquet", index=False)
        if name in {"metrics", "paired_comparison", "qc_results"}:
            frame.to_csv(OUT / f"{name}.csv", index=False)
    files = sorted(path for path in OUT.iterdir() if path.is_file() and path.name != "manifest.json")
    manifest = {
        "dataset": "UMA decision-time market evidence benchmark",
        "generated_at_utc": datetime.now(UTC).isoformat(), "samples": 810,
        "source_manifest": str(ROOT / "data/manifests/polymarket_decision_time_prices.json"),
        "all_required_assertions_pass": bool(objects["qc_results"].passed.all()),
        "files": [{"path": str(path), "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in files],
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main() -> None:
    frame, base_predictions, raw_provenance = load_frame()
    test = frame.query("split == 'test'").copy().reset_index(drop=True)
    y = test.proposal_rejected_by_protocol.to_numpy()
    market_p, market_sd, market_coef = fit(frame, "market_only")
    combined_p, combined_sd, combined_coef = fit(frame, "structured_market")
    base = base_predictions[base_predictions.model.eq("calibrated_logit_ensemble")].set_index("sample_id").loc[test.sample_id]
    base_p = base.probability_proposal_rejected.to_numpy()
    train_prevalence = float(frame.query("split == 'train'").proposal_rejected_by_protocol.mean())
    alignment_p = (1 - test.aligned_probability).fillna(train_prevalence).to_numpy()
    metrics = pd.DataFrame([
        metric("structured_base", y, base_p, [four_action(value) for value in base_p]),
        metric("market_alignment_rule", y, alignment_p, [four_action(value) for value in alignment_p]),
        metric("market_only", y, market_p, [four_action(value) for value in market_p]),
        metric("structured_market", y, combined_p, [four_action(value) for value in combined_p]),
    ])
    comparison = paired_bootstrap(y, base_p, {
        "market_alignment_rule": alignment_p,
        "market_only": market_p,
        "structured_market": combined_p,
    })
    provenance = price_evidence_provenance(frame, raw_provenance)
    output_predictions = pd.concat([
        predictions(test, provenance, "market_alignment_rule", alignment_p, np.zeros(len(test))),
        predictions(test, provenance, "market_only", market_p, market_sd),
        predictions(test, provenance, "structured_market", combined_p, combined_sd),
    ], ignore_index=True)
    coefficients = pd.concat([market_coef, combined_coef], ignore_index=True)
    qc = validate(frame, provenance, output_predictions, comparison)
    objects = {
        "market_feature_snapshot": frame[["sample_id", "decision_time_unix", "split", "coverage_status", *MARKET_FEATURES]],
        "evidence_provenance": provenance, "metrics": metrics, "paired_comparison": comparison,
        "predictions": output_predictions, "model_coefficients": coefficients, "qc_results": qc,
    }
    write_outputs(objects)
    write_report(metrics, comparison, output_predictions, qc)
    print(json.dumps({"samples": len(frame), "test": len(test), "output": str(OUT), "report": str(REPORT)}, indent=2))


if __name__ == "__main__":
    main()
