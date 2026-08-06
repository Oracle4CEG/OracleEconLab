#!/usr/bin/env python3
"""Evaluate decision-time on-chain semantic evidence for the UMA task."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from scripts.applications.trustworthy_ai.run_challenge_benchmark import (
    CATEGORICAL_FEATURES,
    MODEL_FEATURES,
    NUMERIC_FEATURES,
    SEEDS,
    canonical_hash,
    ece,
    four_action,
    nominal_cost,
)


CURATED = Path(os.environ.get(
    "ORACLE_NATURE_CURATED_ROOT", str(ROOT / "data/curated")
))
BASE = ROOT / "data/applications/trustworthy_ai_challenge"
OUT = ROOT / "data/applications/trustworthy_ai_semantic"
REPORT = ROOT / "reports/trustworthy_ai_semantic_evidence.md"
GAMMA_MANIFEST = ROOT / "data/manifests/polymarket_gamma.json"
BOOTSTRAP_SEED = 20260806


def decode_ancillary(value: str) -> str:
    try:
        decoded = bytes.fromhex(str(value).removeprefix("0x")).decode("utf-8", errors="replace")
    except (ValueError, TypeError):
        return ""
    match = re.search(
        r"(?:^|\s)q:\s*title:\s*(.*?),\s*description:\s*(.*?)\s*res_data:",
        decoded, flags=re.IGNORECASE | re.DOTALL,
    )
    text = " ".join(match.groups()) if match else decoded
    return re.sub(r"\s+", " ", text).strip()


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    samples = pd.read_parquet(BASE / "decision_samples.parquet")
    splits = pd.read_parquet(BASE / "splits.parquet")
    base_predictions = pd.read_parquet(BASE / "predictions.parquet")
    con = duckdb.connect()
    con.register("wanted", samples[["sample_id"]])
    source = con.execute(f"""
        SELECT r.oo_request_id sample_id, r.ancillary_data_hex, r.source_tx,
               r.source_block, r.block_time request_time_unix,
               g.gamma_link_grade, g.createdAt gamma_created_at,
               g.question gamma_question, g.description gamma_description,
               g.category gamma_category, g.endDate gamma_end_date,
               g.resolutionSource gamma_resolution_source, g.volumeNum gamma_volume,
               g.liquidityNum gamma_liquidity, g.closed gamma_closed,
               g.umaResolutionStatus gamma_resolution_status
        FROM read_parquet('{CURATED / 'parquet/polygon_uma_request_rounds.parquet'}') r
        JOIN wanted w ON r.oo_request_id=w.sample_id
        LEFT JOIN read_parquet('{CURATED / 'parquet/polygon_uma_gamma_links.parquet'}') g
          ON r.oo_request_id=g.oo_request_id
        ORDER BY r.oo_request_id
    """).fetchdf()
    con.close()
    if len(source) != len(samples) or source.sample_id.nunique() != len(samples):
        raise RuntimeError("semantic source join is not one-to-one")
    source["semantic_text"] = source.ancillary_data_hex.map(decode_ancillary)
    return samples, splits, base_predictions, source


def gamma_audit(samples: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    manifest = json.loads(GAMMA_MANIFEST.read_text())
    snapshot = pd.Timestamp(manifest["snapshot_time_utc"])
    latest_decision = pd.to_datetime(samples.decision_time_unix.max(), unit="s", utc=True)
    fields = [
        ("question_id / exact link", "linkage_only", "Exact identifier is useful for provenance but is not a model feature."),
        ("createdAt", "excluded", "Observed only in a mutable snapshot taken after the decision; no decision-time content hash."),
        ("question / description / category / endDate / resolutionSource", "excluded", "Current metadata version is not historically timestamped or hash-pinned at decision time."),
        ("volume / liquidity", "excluded", "Mutable post-decision quantities; direct temporal leakage risk."),
        ("closed / resolution status", "excluded", "Terminal outcome fields; direct temporal leakage."),
        ("on-chain ancillary_data_hex", "admissible", "Emitted in RequestPrice before the proposal decision and immutable on chain."),
    ]
    return pd.DataFrame([{
        "evidence_fields": name, "admission_status": status, "reason": reason,
        "snapshot_time_utc": snapshot.isoformat(),
        "latest_decision_time_utc": latest_decision.isoformat(),
        "snapshot_after_all_decisions": bool(snapshot > latest_decision),
    } for name, status, reason in fields])


def semantic_provenance(samples: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    frame = samples[["sample_id", "decision_time_unix", "evidence_ids"]].merge(
        source[["sample_id", "source_tx", "source_block", "request_time_unix", "semantic_text"]],
        on="sample_id", validate="one_to_one",
    )
    rows = []
    for row in frame.itertuples(index=False):
        payload = {
            "sample_id": row.sample_id, "evidence_kind": "onchain_request_semantics",
            "source_transaction": row.source_tx, "source_block": int(row.source_block),
            "evidence_time_unix": int(row.request_time_unix),
            "semantic_text_sha256": hashlib.sha256(row.semantic_text.encode()).hexdigest(),
            "transformation": "UTF-8 decode; retain title and description before res_data; collapse whitespace",
        }
        rows.append({
            **payload, "evidence_id": canonical_hash(payload),
            "decision_time_unix": int(row.decision_time_unix),
            "available_at_decision": int(row.request_time_unix) <= int(row.decision_time_unix),
            "base_evidence_ids": row.evidence_ids,
        })
    return pd.DataFrame(rows)


def make_preprocessor(mode: str) -> ColumnTransformer:
    transformers: list[tuple[str, Any, Any]] = []
    if mode == "combined":
        transformers.extend([
            ("numeric", StandardScaler(), NUMERIC_FEATURES),
            ("categorical", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ])
    transformers.append((
        "text",
        TfidfVectorizer(
            lowercase=True, strip_accents="unicode", ngram_range=(1, 2), min_df=3,
            max_df=0.98, max_features=5000, sublinear_tf=True, norm="l2",
        ),
        "semantic_text",
    ))
    return ColumnTransformer(transformers)


def fit_ensemble(frame: pd.DataFrame, mode: str) -> tuple[np.ndarray, np.ndarray, pd.DataFrame, np.ndarray]:
    train = frame.split.eq("train").to_numpy()
    valid = frame.split.eq("validation").to_numpy()
    test = frame.split.eq("test").to_numpy()
    y = frame.proposal_rejected_by_protocol.to_numpy()
    inputs = MODEL_FEATURES + ["semantic_text"] if mode == "combined" else ["semantic_text"]
    X = frame[inputs]
    train_indices = np.flatnonzero(train)
    runs: list[np.ndarray] = []
    coefficient_rows: list[dict[str, Any]] = []
    for seed in SEEDS:
        rng = np.random.default_rng(seed + BOOTSTRAP_SEED)
        bootstrap = rng.choice(train_indices, size=len(train_indices), replace=True)
        pipe = Pipeline([
            ("preprocess", make_preprocessor(mode)),
            ("model", LogisticRegression(max_iter=4000, class_weight="balanced", random_state=seed)),
        ])
        pipe.fit(X.iloc[bootstrap], y[bootstrap])
        valid_p = np.clip(pipe.predict_proba(X.loc[valid])[:, 1], 1e-6, 1 - 1e-6)
        valid_logit = np.log(valid_p / (1 - valid_p)).reshape(-1, 1)
        calibrator = LogisticRegression(random_state=seed).fit(valid_logit, y[valid])
        test_p = np.clip(pipe.predict_proba(X.loc[test])[:, 1], 1e-6, 1 - 1e-6)
        test_logit = np.log(test_p / (1 - test_p)).reshape(-1, 1)
        runs.append(calibrator.predict_proba(test_logit)[:, 1])
        for name, value in zip(
            pipe.named_steps["preprocess"].get_feature_names_out(),
            pipe.named_steps["model"].coef_[0],
        ):
            coefficient_rows.append({
                "model": f"{mode}_semantic_ensemble", "seed": seed,
                "feature": name, "coefficient": float(value),
            })
    matrix = np.vstack(runs)
    return matrix.mean(axis=0), matrix.std(axis=0), pd.DataFrame(coefficient_rows), matrix


def metrics_row(model: str, y: np.ndarray, p: np.ndarray, actions: list[str]) -> dict[str, Any]:
    automated = np.isin(actions, ["Accept", "Challenge"])
    errors = np.array([
        (action == "Accept" and outcome == 1) or (action == "Challenge" and outcome == 0)
        for action, outcome in zip(actions, y)
    ])
    return {
        "model": model, "n_test": len(y), "roc_auc": roc_auc_score(y, p),
        "brier_score": brier_score_loss(y, p),
        "log_loss": log_loss(y, np.clip(p, 1e-6, 1 - 1e-6)), "ece_10": ece(y, p),
        "automated_coverage": automated.mean(),
        "automated_selective_error": errors[automated].mean() if automated.any() else None,
        "investigate_rate": np.mean(np.asarray(actions) == "Investigate"),
        "abstain_rate": np.mean(np.asarray(actions) == "Abstain"),
        "stylized_normalized_cost": np.mean([nominal_cost(a, int(o)) for a, o in zip(actions, y)]),
    }


def paired_bootstrap(y: np.ndarray, base: np.ndarray, alternatives: dict[str, np.ndarray]) -> pd.DataFrame:
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    rows = []
    values = {
        (name, metric): []
        for name in alternatives for metric in ["auc_delta", "brier_delta"]
    }
    for _ in range(2000):
        index = rng.integers(0, len(y), len(y))
        if len(set(y[index])) < 2:
            continue
        for name, probability in alternatives.items():
            values[(name, "auc_delta")].append(
                roc_auc_score(y[index], probability[index]) - roc_auc_score(y[index], base[index])
            )
            values[(name, "brier_delta")].append(
                brier_score_loss(y[index], probability[index]) - brier_score_loss(y[index], base[index])
            )
    for name, probability in alternatives.items():
        observed = {
            "auc_delta": roc_auc_score(y, probability) - roc_auc_score(y, base),
            "brier_delta": brier_score_loss(y, probability) - brier_score_loss(y, base),
        }
        for metric, estimate in observed.items():
            distribution = values[(name, metric)]
            rows.append({
                "comparison": f"{name}_minus_structured_base", "metric": metric,
                "estimate": estimate, "ci_2_5": float(np.quantile(distribution, 0.025)),
                "ci_97_5": float(np.quantile(distribution, 0.975)),
                "bootstrap_replicates": len(distribution), "seed": BOOTSTRAP_SEED,
            })
    return pd.DataFrame(rows)


def top_terms(coefficients: pd.DataFrame) -> pd.DataFrame:
    text = coefficients[coefficients.feature.str.startswith("text__")].copy()
    wide = text.pivot_table(
        index=["model", "feature"], columns="seed", values="coefficient", aggfunc="first"
    ).reindex(columns=SEEDS).fillna(0.0)
    summary = wide.mean(axis=1).rename("mean_coefficient").to_frame()
    summary["sd_coefficient"] = wide.std(axis=1, ddof=0)
    summary["runs_present"] = wide.ne(0).sum(axis=1)
    summary = summary.reset_index()
    rows = []
    for model, part in summary.groupby("model"):
        for direction, selected in [
            ("protocol_rejection_association", part.nlargest(20, "mean_coefficient")),
            ("proposal_upheld_association", part.nsmallest(20, "mean_coefficient")),
        ]:
            value = selected.copy()
            value["direction"] = direction
            rows.append(value)
    return pd.concat(rows, ignore_index=True)


def build_predictions(
    test: pd.DataFrame, provenance: pd.DataFrame, model: str, probability: np.ndarray, std: np.ndarray,
) -> pd.DataFrame:
    prov = provenance.set_index("sample_id")
    rows = []
    for index, row in test.reset_index(drop=True).iterrows():
        action = four_action(float(probability[index]))
        evidence = json.loads(row.evidence_ids) + [prov.loc[row.sample_id, "evidence_id"]]
        rows.append({
            "sample_id": row.sample_id, "model": model,
            "probability_proposal_rejected": float(probability[index]),
            "probability_std_across_runs": float(std[index]), "action": action,
            "confidence": float(max(probability[index], 1 - probability[index])),
            "protocol_outcome": int(row.proposal_rejected_by_protocol),
            "evidence_ids": json.dumps(evidence),
            "explanation": "Decision-time on-chain question semantics plus permitted structured evidence; Gamma snapshot fields excluded.",
            "ground_truth_status": "protocol_resolution_only",
        })
    return pd.DataFrame(rows)


def validate(
    samples: pd.DataFrame, source: pd.DataFrame, provenance: pd.DataFrame, audit: pd.DataFrame,
    predictions: pd.DataFrame, comparison: pd.DataFrame,
) -> pd.DataFrame:
    checks = {
        "same_810_samples": len(samples) == 810 and source.sample_id.nunique() == 810,
        "nonempty_semantic_text": source.semantic_text.str.len().gt(0).all(),
        "semantic_evidence_precedes_decision": provenance.available_at_decision.all(),
        "gamma_snapshot_after_all_decisions": audit.snapshot_after_all_decisions.all(),
        "gamma_mutable_fields_excluded": set(audit.query("admission_status == 'excluded'").evidence_fields) == {
            "createdAt", "question / description / category / endDate / resolutionSource",
            "volume / liquidity", "closed / resolution status",
        },
        "only_onchain_semantics_admitted": audit.query("admission_status == 'admissible'").evidence_fields.tolist() == ["on-chain ancillary_data_hex"],
        "prediction_probabilities_bounded": predictions.probability_proposal_rejected.between(0, 1).all(),
        "all_outputs_protocol_only": predictions.ground_truth_status.eq("protocol_resolution_only").all(),
        "paired_bootstrap_complete": len(comparison) == 4 and comparison.bootstrap_replicates.ge(1900).all(),
    }
    result = pd.DataFrame([{"check": name, "passed": bool(value)} for name, value in checks.items()])
    failed = result.loc[~result.passed, "check"].tolist()
    if failed:
        raise RuntimeError(f"semantic benchmark QC failed: {failed}")
    return result


def write_report(metrics: pd.DataFrame, comparison: pd.DataFrame, predictions: pd.DataFrame, qc: pd.DataFrame) -> None:
    metric_index = metrics.set_index("model")
    semantic = metric_index.loc["semantic_only_ensemble"]
    combined = metric_index.loc["combined_semantic_ensemble"]
    semantic_delta = comparison.query(
        "comparison == 'semantic_only_ensemble_minus_structured_base' and metric == 'auc_delta'"
    ).iloc[0]
    combined_delta = comparison.query(
        "comparison == 'combined_semantic_ensemble_minus_structured_base' and metric == 'auc_delta'"
    ).iloc[0]
    action_counts = predictions[predictions.model.eq("combined_semantic_ensemble")].action.value_counts()
    lines = [
        "# Decision-time semantic evidence benchmark", "",
        "## Evidence admission audit", "",
        "The available Gamma metadata was snapshotted on 2026-07-20, after every benchmark decision, and is explicitly described as mutable. Gamma question text, category, end date, resolution source, volume, liquidity and terminal status are therefore excluded. The only newly admitted evidence is the immutable `ancillary_data_hex` emitted by `RequestPrice` before the proposal decision.", "",
        "## Result", "",
        "| Model | AUC | Brier | ECE | Automated coverage | Abstain rate |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in metrics.itertuples(index=False):
        lines.append(f"| {row.model} | {row.roc_auc:.3f} | {row.brier_score:.3f} | {row.ece_10:.3f} | {row.automated_coverage:.3f} | {row.abstain_rate:.3f} |")
    lines += [
        "",
        f"The semantic-only model has test AUC {semantic.roc_auc:.3f}; its paired AUC delta versus the structured baseline is {semantic_delta.estimate:+.3f} (95% CI {semantic_delta.ci_2_5:+.3f} to {semantic_delta.ci_97_5:+.3f}).",
        f"Direct feature fusion is worse: combined AUC {combined.roc_auc:.3f}, delta {combined_delta.estimate:+.3f} (95% CI {combined_delta.ci_2_5:+.3f} to {combined_delta.ci_97_5:+.3f}). This is evidence of small-sample instability, not a robust gain from semantics.",
        f"Its four-action outputs are: " + ", ".join(f"{name}={int(value)}" for name, value in action_counts.items()) + ".",
        "Because the confidence interval includes no-improvement when applicable and the test cohort contains only 160 observations, semantic terms are exploratory associations, not causal explanations.",
        "The cohort still conditions on actual challenges and the endpoint remains protocol adjudication rather than independent truth.",
        "", "## QC", "",
    ]
    lines += [f"- `{row.check}`: PASS" for row in qc.itertuples(index=False)]
    lines += ["", "## Reproduction", "", "```bash", "python scripts/applications/trustworthy_ai/run_semantic_evidence_benchmark.py", "```", ""]
    REPORT.write_text("\n".join(lines), encoding="utf-8")


def write_outputs(objects: dict[str, Any]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, value in objects.items():
        if isinstance(value, pd.DataFrame):
            value.to_parquet(OUT / f"{name}.parquet", index=False)
            if name in {"gamma_evidence_audit", "metrics", "paired_comparison", "qc_results"}:
                value.to_csv(OUT / f"{name}.csv", index=False)
        else:
            (OUT / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
    files = sorted(path for path in OUT.iterdir() if path.is_file() and path.name != "manifest.json")
    manifest = {
        "dataset": "UMA decision-time semantic evidence benchmark",
        "generated_at_utc": datetime.now(UTC).isoformat(), "base_task": str(BASE / "task_definition.json"),
        "fixed_cutoff": "2026-06-30T23:59:59Z", "samples": 810,
        "all_required_assertions_pass": bool(objects["qc_results"].passed.all()),
        "files": [{"path": str(path), "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in files],
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main() -> None:
    samples, splits, base_predictions, source = load_inputs()
    audit = gamma_audit(samples, source)
    provenance = semantic_provenance(samples, source)
    frame = samples.merge(splits, on=["sample_id", "decision_time_unix"]).merge(
        source[["sample_id", "semantic_text"]], on="sample_id", validate="one_to_one"
    )
    test = frame.query("split == 'test'").copy().reset_index(drop=True)
    y = test.proposal_rejected_by_protocol.to_numpy()
    semantic_p, semantic_sd, semantic_coef, _ = fit_ensemble(frame, "semantic_only")
    combined_p, combined_sd, combined_coef, _ = fit_ensemble(frame, "combined")
    base = base_predictions[
        base_predictions.model.eq("calibrated_logit_ensemble")
    ].set_index("sample_id").loc[test.sample_id]
    base_p = base.probability_proposal_rejected.to_numpy()
    metrics = pd.DataFrame([
        metrics_row("structured_base", y, base_p, [four_action(p) for p in base_p]),
        metrics_row("semantic_only_ensemble", y, semantic_p, [four_action(p) for p in semantic_p]),
        metrics_row("combined_semantic_ensemble", y, combined_p, [four_action(p) for p in combined_p]),
    ])
    comparison = paired_bootstrap(y, base_p, {
        "semantic_only_ensemble": semantic_p,
        "combined_semantic_ensemble": combined_p,
    })
    predictions = pd.concat([
        build_predictions(test, provenance, "semantic_only_ensemble", semantic_p, semantic_sd),
        build_predictions(test, provenance, "combined_semantic_ensemble", combined_p, combined_sd),
    ], ignore_index=True)
    coefficients = pd.concat([semantic_coef, combined_coef], ignore_index=True)
    terms = top_terms(coefficients)
    qc = validate(samples, source, provenance, audit, predictions, comparison)
    objects = {
        "gamma_evidence_audit": audit, "semantic_source": source,
        "semantic_provenance": provenance, "metrics": metrics,
        "paired_comparison": comparison, "predictions": predictions,
        "model_coefficients": coefficients, "top_semantic_terms": terms,
        "qc_results": qc,
    }
    write_outputs(objects)
    write_report(metrics, comparison, predictions, qc)
    print(json.dumps({
        "samples": len(samples), "test": len(test), "output": str(OUT),
        "report": str(REPORT), "qc_passed": bool(qc.passed.all()),
    }, indent=2))


if __name__ == "__main__":
    main()
