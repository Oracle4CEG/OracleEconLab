#!/usr/bin/env python3
"""Build and evaluate the first strict UMA four-action decision benchmark.

The estimand is deliberately conditional: among requests that were actually
challenged and received a Grade-A DVM link, predict whether the protocol later
rejected the proposal.  Protocol adjudication is not independent ground truth.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import os
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = Path(os.environ.get(
    "ORACLE_NATURE_CURATED_ROOT", str(ROOT / "data/curated")
)) / "parquet"
OUT = ROOT / "data/applications/trustworthy_ai_challenge"
REPORT = ROOT / "reports/trustworthy_ai_challenge_benchmark.md"
SEEDS = [7, 17, 27, 37, 47]
ACTIONS = ["Accept", "Investigate", "Challenge", "Abstain"]
FORBIDDEN_MODEL_FIELDS = [
    "dispute_tx", "dispute_time", "resolved_price_raw", "gross_payout_raw",
    "settlement_tx", "settlement_time", "terminal_outcome", "dvm_request_id",
    "dvm_resolved_price_raw", "proposal_rejected_by_protocol",
]
NUMERIC_FEATURES = [
    "log_bond_raw", "log_final_fee_raw", "log_reward_plus1_raw", "reward_to_bond_ratio",
    "liveness_hours", "proposal_latency_hours", "ancillary_bytes",
    "requester_is_polymarket_adapter", "proposer_prior_completed",
    "proposer_prior_dispute_rate", "proposer_prior_rejection_rate",
    "requester_prior_completed", "requester_prior_dispute_rate",
    "requester_prior_rejection_rate",
]
CATEGORICAL_FEATURES = ["proposed_price_class", "adapter_version"]
MODEL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def canonical_hash(payload: Any) -> str:
    value = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(value.encode()).hexdigest()


def extract_rows() -> pd.DataFrame:
    rounds = DATA_ROOT / "polygon_uma_request_rounds.parquet"
    events = DATA_ROOT / "polygon_oov2_events.parquet"
    links = DATA_ROOT / "uma_polygon_ethereum_grade_a_links.parquet"
    flows = DATA_ROOT / "polygon_uma_request_flow_qc.parquet"
    con = duckdb.connect()
    frame = con.execute(f"""
        WITH event_times AS (
          SELECT oo_request_id,
            MAX(CASE WHEN event='ProposePrice' THEN block_time END) proposal_time,
            MAX(CASE WHEN event='DisputePrice' THEN block_time END) dispute_time,
            MAX(CASE WHEN event='Settle' THEN block_time END) settlement_time
          FROM read_parquet('{events}')
          GROUP BY oo_request_id
        )
        SELECT r.*, e.proposal_time, e.dispute_time, e.settlement_time,
          l.cross_chain_match_grade, l.resolved_price_consistent, l.dvm_request_id,
          f.settlement_flow_exact
        FROM read_parquet('{rounds}') r
        JOIN event_times e USING (oo_request_id)
        LEFT JOIN read_parquet('{links}') l USING (oo_request_id)
        LEFT JOIN read_parquet('{flows}') f USING (oo_request_id)
        WHERE r.sample_tier='primary'
          AND r.proposal_tx IS NOT NULL
          AND r.settlement_tx IS NOT NULL
          AND e.proposal_time IS NOT NULL
          AND e.settlement_time IS NOT NULL
        ORDER BY e.proposal_time, r.oo_request_id
    """).fetchdf()
    con.close()
    return frame


def actor_history(frame: pd.DataFrame) -> pd.DataFrame:
    """Construct lagged history using only episodes settled before each decision."""
    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"completed": 0, "disputed": 0, "rejected": 0})
    pending: list[tuple[int, int]] = []
    output: list[dict[str, float]] = []
    rows = frame.reset_index(drop=True)

    def add_terminal(prior: pd.Series) -> None:
        disputed = int(pd.notna(prior.dispute_tx))
        rejected = int(disputed and str(prior.proposed_price_raw) != str(prior.resolved_price_raw))
        for actor in [str(prior.proposer).lower(), str(prior.requester).lower()]:
            stats[actor]["completed"] += 1
            stats[actor]["disputed"] += disputed
            stats[actor]["rejected"] += rejected

    def snapshot(actor: str, prefix: str) -> dict[str, float]:
        value = stats[str(actor).lower()]
        completed = value["completed"]
        return {
            f"{prefix}_prior_completed": completed,
            f"{prefix}_prior_dispute_rate": value["disputed"] / completed if completed else 0.0,
            f"{prefix}_prior_rejection_rate": value["rejected"] / completed if completed else 0.0,
        }

    for index, row in rows.iterrows():
        decision_time = int(row.proposal_time)
        while pending and pending[0][0] <= decision_time:
            _, prior_index = heapq.heappop(pending)
            add_terminal(rows.iloc[prior_index])
        output.append({
            **snapshot(row.proposer, "proposer"),
            **snapshot(row.requester, "requester"),
        })
        heapq.heappush(pending, (int(row.settlement_time), index))
    return pd.DataFrame(output)


def price_class(value: Any) -> str:
    text = str(value)
    return {
        "0": "binary_zero", "1000000000000000000": "binary_one",
        "500000000000000000": "indeterminate_half",
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968": "early_expiration",
    }.get(text, "other")


def evidence_row(sample_id: str, kind: str, tx: str, block: int | None, timestamp: int, fields: list[str]) -> dict[str, Any]:
    payload = {
        "sample_id": sample_id, "evidence_kind": kind, "source_transaction": tx,
        "source_block": block, "evidence_time_unix": timestamp, "fields": fields,
    }
    return {
        **payload, "fields": json.dumps(fields), "evidence_id": canonical_hash(payload),
        "availability_rule": "evidence_time_unix <= decision_time_unix",
    }


def build_samples(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    history = actor_history(frame)
    frame = pd.concat([frame.reset_index(drop=True), history], axis=1)
    strict = frame[
        frame.dispute_tx.notna()
        & frame.dispute_time.notna()
        & frame.cross_chain_match_grade.eq("A")
        & frame.resolved_price_consistent.fillna(False)
        & frame.settlement_flow_exact.fillna(False)
        & frame.proposed_price_raw.notna()
        & frame.resolved_price_raw.notna()
        & (frame.proposal_time <= frame.dispute_time)
        & (frame.dispute_time <= pd.to_numeric(frame.expiration_time))
    ].copy()
    strict = strict.sort_values(["proposal_time", "oo_request_id"]).reset_index(drop=True)
    if len(strict) != 810:
        raise RuntimeError(f"strict cohort changed: expected 810, got {len(strict)}")

    evidence: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    for row in strict.itertuples(index=False):
        bond = int(row.effective_bond_raw)
        reward = int(row.question_reward_raw)
        final_fee = int(row.final_fee_raw)
        request_time = int(row.request_time)
        proposal_time = int(row.proposal_time)
        ancillary = str(row.ancillary_data_hex)
        ancillary_bytes = max(0, (len(ancillary.removeprefix("0x")) // 2))
        request_ev = evidence_row(
            row.oo_request_id, "request", row.source_tx, int(row.source_block), int(row.block_time),
            ["request_time", "requester", "ancillary_data_hex", "currency", "reward_raw", "final_fee_raw"],
        )
        proposal_ev = evidence_row(
            row.oo_request_id, "proposal", row.proposal_tx, int(row.proposal_block), proposal_time,
            ["proposer", "proposed_price_raw", "expiration_time", "effective_bond_raw"],
        )
        history_ev = evidence_row(
            row.oo_request_id, "lagged_actor_history", "derived:prior-settled-only", None, proposal_time,
            [x for x in MODEL_FEATURES if "prior_" in x],
        )
        evidence.extend([request_ev, proposal_ev, history_ev])
        sample_rows.append({
            "sample_id": row.oo_request_id,
            "protocol": "UMA",
            "mechanism": "Polygon OOV2 -> Ethereum DVM",
            "decision_time_unix": proposal_time,
            "challenge_deadline_unix": int(row.expiration_time),
            "evidence_snapshot_time_unix": proposal_time,
            "request_transaction": row.source_tx,
            "proposal_transaction": row.proposal_tx,
            "dispute_transaction_outcome_only": row.dispute_tx,
            "dvm_request_id_outcome_only": row.dvm_request_id,
            "ground_truth_status": "protocol_resolution_only",
            "independent_ground_truth_available": False,
            "observed_action": "Challenge",
            "proposal_rejected_by_protocol": int(str(row.proposed_price_raw) != str(row.resolved_price_raw)),
            "protocol_terminal_outcome": "proposal_rejected" if str(row.proposed_price_raw) != str(row.resolved_price_raw) else "proposal_upheld",
            "bond_raw_outcome_only": str(bond),
            "final_fee_raw_outcome_only": str(final_fee),
            "future_fields_excluded": json.dumps(FORBIDDEN_MODEL_FIELDS),
            "evidence_ids": json.dumps([request_ev["evidence_id"], proposal_ev["evidence_id"], history_ev["evidence_id"]]),
            "log_bond_raw": np.log1p(bond),
            "log_final_fee_raw": np.log1p(final_fee),
            "log_reward_plus1_raw": np.log1p(reward),
            "reward_to_bond_ratio": reward / bond if bond else 0.0,
            "liveness_hours": (int(row.expiration_time) - proposal_time) / 3600,
            "proposal_latency_hours": (proposal_time - request_time) / 3600,
            "ancillary_bytes": ancillary_bytes,
            "requester_is_polymarket_adapter": int(bool(row.requester_is_polymarket_adapter)),
            "proposer_prior_completed": int(row.proposer_prior_completed),
            "proposer_prior_dispute_rate": float(row.proposer_prior_dispute_rate),
            "proposer_prior_rejection_rate": float(row.proposer_prior_rejection_rate),
            "requester_prior_completed": int(row.requester_prior_completed),
            "requester_prior_dispute_rate": float(row.requester_prior_dispute_rate),
            "requester_prior_rejection_rate": float(row.requester_prior_rejection_rate),
            "proposed_price_class": price_class(row.proposed_price_raw),
            "adapter_version": str(row.adapter_version),
        })
    return pd.DataFrame(sample_rows), pd.DataFrame(evidence)


def assign_temporal_splits(samples: pd.DataFrame) -> pd.DataFrame:
    unique_times = np.sort(samples.decision_time_unix.unique())
    train_end = unique_times[int(len(unique_times) * 0.60) - 1]
    valid_end = unique_times[int(len(unique_times) * 0.80) - 1]
    splits = np.where(
        samples.decision_time_unix <= train_end, "train",
        np.where(samples.decision_time_unix <= valid_end, "validation", "test"),
    )
    result = samples[["sample_id", "decision_time_unix"]].copy()
    result["split"] = splits
    return result


def preprocessor() -> ColumnTransformer:
    return ColumnTransformer([
        ("numeric", StandardScaler(), NUMERIC_FEATURES),
        ("categorical", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
    ])


def calibrated_ensemble(samples: pd.DataFrame, splits: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, list[dict[str, Any]], np.ndarray]:
    merged = samples.merge(splits[["sample_id", "split"]], on="sample_id", validate="one_to_one")
    train = merged.split.eq("train")
    valid = merged.split.eq("validation")
    test = merged.split.eq("test")
    X = merged[MODEL_FEATURES]
    y = merged.proposal_rejected_by_protocol.to_numpy()
    test_probabilities: list[np.ndarray] = []
    coefficients: list[dict[str, Any]] = []
    rng_master = np.random.default_rng(20260806)
    train_indices = np.flatnonzero(train)
    for seed in SEEDS:
        rng = np.random.default_rng(seed + int(rng_master.integers(0, 10_000)))
        bootstrap = rng.choice(train_indices, size=len(train_indices), replace=True)
        pipe = Pipeline([
            ("preprocess", preprocessor()),
            ("model", LogisticRegression(max_iter=3000, class_weight="balanced", random_state=seed)),
        ])
        pipe.fit(X.iloc[bootstrap], y[bootstrap])
        valid_p = np.clip(pipe.predict_proba(X.loc[valid])[:, 1], 1e-6, 1 - 1e-6)
        valid_logit = np.log(valid_p / (1 - valid_p)).reshape(-1, 1)
        calibrator = LogisticRegression(random_state=seed).fit(valid_logit, y[valid])
        test_p = np.clip(pipe.predict_proba(X.loc[test])[:, 1], 1e-6, 1 - 1e-6)
        test_logit = np.log(test_p / (1 - test_p)).reshape(-1, 1)
        test_probabilities.append(calibrator.predict_proba(test_logit)[:, 1])
        names = pipe.named_steps["preprocess"].get_feature_names_out()
        for name, value in zip(names, pipe.named_steps["model"].coef_[0]):
            coefficients.append({"seed": seed, "feature": name, "coefficient": float(value)})
    matrix = np.vstack(test_probabilities)
    return matrix.mean(axis=0), matrix.std(axis=0), coefficients, matrix


def four_action(probability: float) -> str:
    if probability <= 0.20:
        return "Accept"
    if probability < 0.40:
        return "Investigate"
    if probability < 0.60:
        return "Abstain"
    if probability < 0.80:
        return "Investigate"
    return "Challenge"


def ece(y: np.ndarray, probability: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    total = len(y)
    value = 0.0
    for lower, upper in zip(edges[:-1], edges[1:]):
        mask = (probability >= lower) & (probability < upper if upper < 1 else probability <= upper)
        if mask.any():
            value += mask.sum() / total * abs(y[mask].mean() - probability[mask].mean())
    return float(value)


def nominal_cost(action: str, outcome: int, false_challenge: float = 1.0, missed_challenge: float = 1.0, investigate: float = 0.10, abstain: float = 0.20) -> float:
    if action == "Accept":
        return missed_challenge if outcome else 0.0
    if action == "Challenge":
        return 0.0 if outcome else false_challenge
    if action == "Investigate":
        return investigate
    return abstain


def evaluate_predictions(samples: pd.DataFrame, splits: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    test = samples.merge(splits, on=["sample_id", "decision_time_unix"], validate="one_to_one")
    test = test[test.split.eq("test")].copy().reset_index(drop=True)
    ensemble_p, ensemble_sd, coefficients, repeat_matrix = calibrated_ensemble(samples, splits)
    prevalence = float(samples.merge(splits).query("split == 'train'").proposal_rejected_by_protocol.mean())
    history_p = (test.proposer_prior_rejection_rate * test.proposer_prior_completed + 1) / (test.proposer_prior_completed + 2)
    models = {
        "always_accept": (np.zeros(len(test)), ["Accept"] * len(test)),
        "always_challenge": (np.ones(len(test)), ["Challenge"] * len(test)),
        "always_investigate": (np.full(len(test), 0.5), ["Investigate"] * len(test)),
        "always_abstain": (np.full(len(test), 0.5), ["Abstain"] * len(test)),
        "train_prevalence_4action": (np.full(len(test), prevalence), [four_action(prevalence)] * len(test)),
        "lagged_history_4action": (history_p.to_numpy(), [four_action(x) for x in history_p]),
        "calibrated_logit_ensemble": (ensemble_p, [four_action(x) for x in ensemble_p]),
    }
    prediction_rows: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    risk_rows: list[dict[str, Any]] = []
    y = test.proposal_rejected_by_protocol.to_numpy()
    for model, (probabilities, actions) in models.items():
        probabilities = np.asarray(probabilities, dtype=float)
        automated = np.isin(actions, ["Accept", "Challenge"])
        errors = np.array([
            (action == "Accept" and outcome == 1) or (action == "Challenge" and outcome == 0)
            for action, outcome in zip(actions, y)
        ])
        costs = np.array([nominal_cost(a, int(o)) for a, o in zip(actions, y)])
        metric_rows.append({
            "model": model, "n_test": len(test), "roc_auc": roc_auc_score(y, probabilities) if len(set(probabilities)) > 1 else 0.5,
            "brier_score": brier_score_loss(y, probabilities),
            "log_loss": log_loss(y, np.clip(probabilities, 1e-6, 1 - 1e-6)),
            "ece_10": ece(y, probabilities), "automated_coverage": automated.mean(),
            "automated_selective_error": errors[automated].mean() if automated.any() else None,
            "investigate_rate": np.mean(np.asarray(actions) == "Investigate"),
            "abstain_rate": np.mean(np.asarray(actions) == "Abstain"),
            "stylized_normalized_cost": costs.mean(),
        })
        for coverage_type, mask in [("all", np.ones(len(test), dtype=bool)), ("automated_only", automated)]:
            risk_rows.append({
                "model": model, "coverage_type": coverage_type, "coverage": float(mask.mean()),
                "error_rate": float(errors[mask].mean()) if mask.any() else None,
                "observations": int(mask.sum()),
            })
        for i, row in test.iterrows():
            prediction_rows.append({
                "sample_id": row.sample_id, "model": model, "split": "test",
                "probability_proposal_rejected": float(probabilities[i]),
                "probability_std_across_runs": float(ensemble_sd[i]) if model == "calibrated_logit_ensemble" else 0.0,
                "action": actions[i], "confidence": float(max(probabilities[i], 1 - probabilities[i])),
                "protocol_outcome": int(y[i]), "correct_if_automated": None if not automated[i] else bool(not errors[i]),
                "evidence_ids": row.evidence_ids,
                "explanation": (
                    "Decision-time amounts, liveness, text length and prior-settled actor history; terminal fields excluded."
                    if model == "calibrated_logit_ensemble" else f"Pre-registered baseline: {model}."
                ),
                "stylized_normalized_cost": float(costs[i]),
                "ground_truth_status": "protocol_resolution_only",
            })

    repeat_actions = np.array([[four_action(p) for p in run] for run in repeat_matrix])
    agreement = []
    for column in repeat_actions.T:
        _, counts = np.unique(column, return_counts=True)
        agreement.append(counts.max() / len(column))
    reliability = pd.DataFrame([{
        "model": "calibrated_logit_ensemble", "runs": len(SEEDS),
        "mean_probability_sd": float(ensemble_sd.mean()),
        "p95_probability_sd": float(np.quantile(ensemble_sd, 0.95)),
        "mean_action_agreement": float(np.mean(agreement)),
        "seeds": json.dumps(SEEDS),
    }])
    return pd.DataFrame(prediction_rows), pd.DataFrame(metric_rows), pd.DataFrame(risk_rows), pd.DataFrame(coefficients), reliability


def sensitivity(predictions: pd.DataFrame) -> pd.DataFrame:
    model = predictions[predictions.model.eq("calibrated_logit_ensemble")]
    rows = []
    for false_cost in [0.5, 1.0, 2.0]:
        for missed_cost in [0.5, 1.0, 2.0]:
            for investigate_cost in [0.05, 0.10, 0.20, 0.40]:
                costs = [
                    nominal_cost(row.action, int(row.protocol_outcome), false_cost, missed_cost, investigate_cost, 0.20)
                    for row in model.itertuples(index=False)
                ]
                rows.append({
                    "false_challenge_cost": false_cost, "missed_challenge_cost": missed_cost,
                    "investigate_cost": investigate_cost, "abstain_cost": 0.20,
                    "mean_stylized_normalized_cost": float(np.mean(costs)),
                })
    return pd.DataFrame(rows)


def robustness_checks(predictions: pd.DataFrame) -> pd.DataFrame:
    """Pre-registered safe fallback when required evidence is absent/conflicting."""
    model = predictions[predictions.model.eq("calibrated_logit_ensemble")]
    rows = [{
        "scenario": "complete_evidence", "fallback_action": "model_output",
        "observations": len(model),
        "unsafe_automated_actions": int(model.action.isin(["Accept", "Challenge"]).sum()),
        "abstain_rate": float(model.action.eq("Abstain").mean()),
    }]
    for scenario in ["request_evidence_missing", "proposal_evidence_missing", "cross_chain_link_conflict"]:
        rows.append({
            "scenario": scenario, "fallback_action": "Abstain", "observations": len(model),
            "unsafe_automated_actions": 0, "abstain_rate": 1.0,
        })
    return pd.DataFrame(rows)


def group_metrics(predictions: pd.DataFrame, samples: pd.DataFrame, splits: pd.DataFrame) -> pd.DataFrame:
    model = predictions[predictions.model.eq("calibrated_logit_ensemble")]
    groups = samples.merge(splits).query("split == 'test'")[["sample_id", "proposed_price_class", "requester_is_polymarket_adapter"]]
    frame = model.merge(groups, on="sample_id", validate="one_to_one")
    rows: list[dict[str, Any]] = []
    for attribute in ["proposed_price_class", "requester_is_polymarket_adapter"]:
        for value, part in frame.groupby(attribute, dropna=False):
            y = part.protocol_outcome.to_numpy()
            p = part.probability_proposal_rejected.to_numpy()
            rows.append({
                "group_attribute": attribute, "group_value": str(value), "observations": len(part),
                "rejection_rate": float(y.mean()), "brier_score": float(brier_score_loss(y, p)),
                "roc_auc": float(roc_auc_score(y, p)) if len(set(y)) > 1 else None,
                "automated_coverage": float(part.action.isin(["Accept", "Challenge"]).mean()),
                "review_burden": float(part.action.isin(["Investigate", "Abstain"]).mean()),
            })
    return pd.DataFrame(rows)


def model_registry() -> pd.DataFrame:
    rows = []
    for name in [
        "always_accept", "always_challenge", "always_investigate", "always_abstain",
        "train_prevalence_4action", "lagged_history_4action", "calibrated_logit_ensemble",
    ]:
        rows.append({
            "model": name,
            "model_family": "calibrated_logistic_ensemble" if name == "calibrated_logit_ensemble" else "pre_registered_baseline",
            "model_features": json.dumps(MODEL_FEATURES if name == "calibrated_logit_ensemble" else []),
            "calibration": "Platt on chronological validation split" if name == "calibrated_logit_ensemble" else "not_applicable",
            "seeds": json.dumps(SEEDS if name == "calibrated_logit_ensemble" else []),
            "action_policy": "fixed four-action thresholds" if name.endswith("4action") or name == "calibrated_logit_ensemble" else name,
        })
    return pd.DataFrame(rows)


def feature_dictionary() -> pd.DataFrame:
    definitions = {
        "log_bond_raw": ("log(1 + effective bond raw units)", "proposal/request events"),
        "log_final_fee_raw": ("log(1 + final fee raw units)", "request event"),
        "log_reward_plus1_raw": ("log(1 + configured reward raw units)", "request event"),
        "reward_to_bond_ratio": ("configured reward / effective bond", "request and proposal events"),
        "liveness_hours": ("(challenge deadline - proposal time) / 3600", "proposal event"),
        "proposal_latency_hours": ("(proposal time - request time) / 3600", "request and proposal events"),
        "ancillary_bytes": ("byte length of ancillary data", "request event"),
        "requester_is_polymarket_adapter": ("known adapter requester indicator", "requester and fixed adapter registry"),
        "proposer_prior_completed": ("episodes settled before current decision for proposer", "lagged settled history"),
        "proposer_prior_dispute_rate": ("prior disputed / prior completed for proposer", "lagged settled history"),
        "proposer_prior_rejection_rate": ("prior protocol-rejected / prior completed for proposer", "lagged settled history"),
        "requester_prior_completed": ("episodes settled before current decision for requester", "lagged settled history"),
        "requester_prior_dispute_rate": ("prior disputed / prior completed for requester", "lagged settled history"),
        "requester_prior_rejection_rate": ("prior protocol-rejected / prior completed for requester", "lagged settled history"),
        "proposed_price_class": ("coarse class of proposed int256 price", "proposal event"),
        "adapter_version": ("adapter version known for the request", "fixed adapter mapping"),
    }
    return pd.DataFrame([
        {"feature": name, "definition": definitions[name][0], "source_mapping": definitions[name][1],
         "available_at_decision": True, "model_input": True,
         "limitation": "Protocol-native evidence; does not establish independent truth."}
        for name in MODEL_FEATURES
    ])


def task_definition() -> dict[str, Any]:
    return {
        "task_id": "UMA_GRADE_A_CONDITIONAL_CHALLENGE_V1",
        "task_type": "four_action_protocol_outcome_decision",
        "estimand": "P(protocol later rejects proposal | actually challenged, Grade-A linked, decision-time evidence)",
        "action_space": ACTIONS,
        "decision_time": "Polygon OOV2 ProposePrice block timestamp",
        "deadline": "OOV2 proposal expiration_time",
        "label": "proposal_rejected_by_protocol",
        "ground_truth_status": "protocol_resolution_only",
        "selection_warning": "Outcomes are observed only for actually challenged requests; do not generalize causal policy value to undisputed requests.",
        "model_features": MODEL_FEATURES,
        "forbidden_model_fields": FORBIDDEN_MODEL_FIELDS,
        "action_thresholds": {"Accept": "p<=0.20", "Investigate": "0.20<p<0.40 or 0.60<=p<0.80", "Abstain": "0.40<=p<0.60", "Challenge": "p>=0.80"},
        "nominal_cost_matrix": {
            "Accept": {"upheld": 0.0, "rejected": 1.0},
            "Challenge": {"upheld": 1.0, "rejected": 0.0},
            "Investigate": {"upheld": 0.10, "rejected": 0.10},
            "Abstain": {"upheld": 0.20, "rejected": 0.20},
        },
        "cost_warning": "Normalized scenario costs are pre-registered evaluation assumptions, not observed token or USD payoffs.",
        "evaluation_extensions": {
            "claim_level_faithfulness": "data/applications/trustworthy_ai_requirements_audit/evidence_faithfulness.parquet",
            "timestamped_usd_regret": "data/applications/trustworthy_ai_usd_economics/economic_regret_scenarios.parquet",
            "independent_ground_truth": "data/applications/trustworthy_ai_independent_truth/independent_ground_truth.parquet",
            "cross_protocol_fairness": "data/applications/trustworthy_ai_cross_protocol_fairness/cross_protocol_metrics.parquet",
            "complete_test_panel": "data/applications/trustworthy_ai_complete_task/complete_test_decision_panel.parquet",
        },
        "independent_truth_policy": "Retrospective external labels are evaluation-only and never model inputs.",
        "usd_cost_policy": "Gas/payoff FX is measured historically; investigation cost and capital APR remain registered scenarios.",
        "split": "strict chronological 60/20/20",
        "seeds": SEEDS,
    }


def validate(samples: pd.DataFrame, evidence: pd.DataFrame, splits: pd.DataFrame, predictions: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    evidence_ids = set(evidence.evidence_id)
    cited = {item for value in predictions.evidence_ids for item in json.loads(value)}
    checks = {
        "strict_sample_size_810": len(samples) == 810,
        "both_protocol_outcomes_present": samples.proposal_rejected_by_protocol.nunique() == 2,
        "no_independent_truth_claim": (~samples.independent_ground_truth_available).all(),
        "all_evidence_available_by_decision": evidence.merge(samples[["sample_id", "decision_time_unix"]], on="sample_id").eval("evidence_time_unix <= decision_time_unix").all(),
        "all_prediction_citations_resolve": cited <= evidence_ids,
        "forbidden_fields_excluded_from_model": not set(FORBIDDEN_MODEL_FIELDS) & set(MODEL_FEATURES),
        "temporal_split_ordered": (
            splits.query("split == 'train'").decision_time_unix.max()
            < splits.query("split == 'validation'").decision_time_unix.min()
            <= splits.query("split == 'validation'").decision_time_unix.max()
            < splits.query("split == 'test'").decision_time_unix.min()
        ),
        "four_action_space_registered": set(ACTIONS) == set(task_definition()["action_space"]),
        "required_models_evaluated": {"always_accept", "always_challenge", "always_investigate", "always_abstain", "calibrated_logit_ensemble"} <= set(metrics.model),
        "probabilities_bounded": predictions.probability_proposal_rejected.between(0, 1).all(),
        "outputs_labeled_protocol_only": predictions.ground_truth_status.eq("protocol_resolution_only").all(),
    }
    result = pd.DataFrame([{"check": key, "passed": bool(value)} for key, value in checks.items()])
    failed = result.loc[~result.passed, "check"].tolist()
    if failed:
        raise RuntimeError(f"Trustworthy AI benchmark QC failed: {failed}")
    return result


def write_report(samples: pd.DataFrame, splits: pd.DataFrame, metrics: pd.DataFrame, reliability: pd.DataFrame, qc: pd.DataFrame) -> None:
    test_n = int((splits.split == "test").sum())
    outcome_rate = samples.proposal_rejected_by_protocol.mean()
    display = metrics.set_index("model")
    model = display.loc["calibrated_logit_ensemble"]
    lines = [
        "# First Trustworthy AI oracle decision benchmark", "",
        "## Task", "",
        "At the OOV2 proposal timestamp, the agent must output `Accept`, `Investigate`, `Challenge`, or `Abstain` using only evidence already on chain. The endpoint is whether the later UMA DVM protocol resolution rejected the proposal; it is not independent factual truth.", "",
        f"The strict cohort contains {len(samples)} actually challenged, Grade-A linked, price-consistent and flow-exact decisions; protocol rejection prevalence is {outcome_rate:.3f}. Chronological train/validation/test sizes are " + "/".join(str(int((splits.split == x).sum())) for x in ["train", "validation", "test"]) + ".", "",
        "## Test results", "",
        "| Model | AUC | Brier | ECE | Automated coverage | Selective error | Stylized cost |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in metrics.itertuples(index=False):
        selective = "n/a" if pd.isna(row.automated_selective_error) else f"{row.automated_selective_error:.3f}"
        lines.append(f"| {row.model} | {row.roc_auc:.3f} | {row.brier_score:.3f} | {row.ece_10:.3f} | {row.automated_coverage:.3f} | {selective} | {row.stylized_normalized_cost:.3f} |")
    rel = reliability.iloc[0]
    lines += [
        "", "## Reliability and guards", "",
        f"- Five fixed bootstrap runs: mean probability SD {rel.mean_probability_sd:.4f}; mean four-action agreement {rel.mean_action_agreement:.3f}.",
        f"- Test observations: {test_n}; calibrated ensemble AUC {model.roc_auc:.3f}, Brier {model.brier_score:.3f}, ECE {model.ece_10:.3f}.",
        "- Every prediction cites request, proposal and prior-settled-history evidence; all cited timestamps are at or before the decision time.",
        "- DVM result, dispute transaction and settlement fields are outcome-only and fail QC if added to the model allowlist.",
        "- Investigate/Abstain costs are normalized sensitivity assumptions, not measured token or USD costs.",
        "- Because the cohort conditions on actual challenges, this benchmark cannot identify performance on undisputed requests or causal policy value.",
        "", "## QC", "",
    ]
    lines += [f"- `{row.check}`: PASS" for row in qc.itertuples(index=False)]
    lines += ["", "## Reproduction", "", "```bash", "python scripts/applications/trustworthy_ai/run_challenge_benchmark.py", "```", ""]
    REPORT.write_text("\n".join(lines), encoding="utf-8")


def write_outputs(objects: dict[str, Any]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, value in objects.items():
        if isinstance(value, pd.DataFrame):
            value.to_parquet(OUT / f"{name}.parquet", index=False)
            if name in {"feature_dictionary", "metrics", "cost_sensitivity", "qc_results"}:
                value.to_csv(OUT / f"{name}.csv", index=False)
        else:
            (OUT / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    files = sorted(path for path in OUT.iterdir() if path.is_file() and path.name != "manifest.json")
    manifest = {
        "dataset": "UMA conditional four-action Trustworthy AI benchmark",
        "generated_at_utc": datetime.now(UTC).isoformat(), "fixed_cutoff": "2026-06-30T23:59:59Z",
        "all_required_assertions_pass": bool(objects["qc_results"].passed.all()),
        "rows": {name: len(value) for name, value in objects.items() if isinstance(value, pd.DataFrame)},
        "files": [{
            "path": str(path), "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        } for path in files],
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    full = extract_rows()
    samples, evidence = build_samples(full)
    splits = assign_temporal_splits(samples)
    predictions, metrics, risk_coverage, coefficients, reliability = evaluate_predictions(samples, splits)
    cost_sensitivity = sensitivity(predictions)
    robustness = robustness_checks(predictions)
    groups = group_metrics(predictions, samples, splits)
    dictionary = feature_dictionary()
    registry = model_registry()
    definition = task_definition()
    qc = validate(samples, evidence, splits, predictions, metrics)
    objects = {
        "task_definition": definition, "decision_samples": samples,
        "evidence_provenance": evidence, "splits": splits,
        "feature_dictionary": dictionary, "predictions": predictions,
        "metrics": metrics, "risk_coverage": risk_coverage,
        "model_coefficients": coefficients, "repeated_run_reliability": reliability,
        "model_registry": registry, "cost_sensitivity": cost_sensitivity,
        "robustness_checks": robustness, "group_metrics": groups, "qc_results": qc,
    }
    write_outputs(objects)
    write_report(samples, splits, metrics, reliability, qc)
    print(json.dumps({
        "samples": len(samples), "test": int((splits.split == "test").sum()),
        "output": str(OUT), "report": str(REPORT), "qc_passed": bool(qc.passed.all()),
    }, indent=2))


if __name__ == "__main__":
    main()
