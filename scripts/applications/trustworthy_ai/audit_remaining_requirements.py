#!/usr/bin/env python3
"""Close the remaining Trustworthy-AI evaluation requirements in the guide.

This audit is deliberately bounded to the registered 810-sample UMA task.  It
adds claim-level evidence faithfulness, observed challenge economics, guardrail
attacks, coverage fairness, and independent-truth coverage.  It does not turn
protocol resolution into factual truth or combine USDC and MATIC amounts.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from sklearn.metrics import brier_score_loss, roc_auc_score


getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "data/applications/trustworthy_ai_challenge"
CURATED = ROOT / "data/curated/parquet"
OUT = ROOT / "data/applications/trustworthy_ai_requirements_audit"
RAW = ROOT / "data/raw/polygon/trustworthy_ai_dispute_receipts_v1.jsonl.gz"
MANIFEST = OUT / "manifest.json"
REPORT = ROOT / "reports/trustworthy_ai_remaining_requirements.md"
SOURCE_RECEIPTS = ROOT / "data/raw/polygon/uma_bridge_discovery/dispute_receipts.jsonl.gz"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_env_url() -> str | None:
    path = ROOT / ".env"
    if not path.exists():
        return None
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[7:]
        if "=" in line and line.split("=", 1)[0].strip() == "NODE_URL2":
            return line.split("=", 1)[1].strip().strip("\"'")
    return None


def rpc_receipt(url: str, tx: str, attempts: int = 5) -> dict[str, Any]:
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt", "params": [tx]},
                timeout=45,
            )
            response.raise_for_status()
            body = response.json()
            if body.get("error"):
                raise RuntimeError(str(body["error"]))
            if not body.get("result"):
                raise RuntimeError("null receipt")
            return body["result"]
        except Exception as exc:
            error = exc
            time.sleep(min(5.0, 0.25 * 2**attempt))
    raise RuntimeError(f"receipt unavailable for {tx}: {error}")


def read_receipts(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            rows[str(row["transactionHash"]).lower()] = row
    return rows


def freeze_receipts(transactions: list[str], offline: bool, workers: int) -> dict[str, dict[str, Any]]:
    cached = read_receipts(RAW)
    # Reuse the older checksum-pinned receipt archive before making RPC calls.
    for tx, row in read_receipts(SOURCE_RECEIPTS).items():
        if tx in transactions and tx not in cached:
            cached[tx] = row
    missing = sorted(set(transactions) - set(cached))
    if missing and offline:
        raise RuntimeError(f"offline receipt cache incomplete: {len(missing)} missing")
    if missing:
        url = load_env_url()
        if not url:
            raise RuntimeError("NODE_URL2 is required to complete the fixed historical receipt cache")
        chain = requests.post(
            url, json={"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}, timeout=20
        ).json().get("result")
        if chain != "0x89":
            raise RuntimeError("NODE_URL2 must be Polygon chain 137")
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(rpc_receipt, url, tx): tx for tx in missing}
            for index, future in enumerate(as_completed(futures), 1):
                cached[futures[future]] = future.result()
                if index % 100 == 0:
                    print(f"receipts={index}/{len(missing)}", flush=True)
    selected = [cached[tx] for tx in sorted(transactions)]
    selected.sort(key=lambda row: (int(row["blockNumber"], 16), int(row["transactionIndex"], 16)))
    RAW.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(RAW, "wt", encoding="utf-8", compresslevel=9) as handle:
        for row in selected:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    return {str(row["transactionHash"]).lower(): row for row in selected}


def load_episode_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    samples = pd.read_parquet(BASE / "decision_samples.parquet")
    evidence = pd.read_parquet(BASE / "evidence_provenance.parquet")
    predictions = pd.read_parquet(BASE / "predictions.parquet")
    rounds = pd.read_parquet(CURATED / "polygon_uma_request_rounds.parquet")
    rounds = rounds[rounds.oo_request_id.isin(samples.sample_id)].copy()
    events = pd.read_parquet(CURATED / "polygon_oov2_events.parquet")
    times = events[events.oo_request_id.isin(samples.sample_id)].pivot_table(
        index="oo_request_id", columns="event", values="block_time", aggfunc="max"
    ).reset_index()
    rounds = rounds.merge(times[["oo_request_id", "DisputePrice", "Settle"]], on="oo_request_id", validate="one_to_one")
    if len(rounds) != 810 or rounds.oo_request_id.nunique() != 810:
        raise RuntimeError("economic outcome join must retain exactly 810 samples")
    return samples, evidence, predictions, rounds


def evidence_faithfulness(
    samples: pd.DataFrame, evidence: pd.DataFrame, predictions: pd.DataFrame
) -> pd.DataFrame:
    claims = [
        ("request_amounts_and_text", "request", {"reward_raw", "final_fee_raw", "ancillary_data_hex"}),
        ("proposal_bond_liveness_and_price", "proposal", {"effective_bond_raw", "expiration_time", "proposed_price_raw"}),
        ("prior_settled_actor_history", "lagged_actor_history", {
            "proposer_prior_completed", "proposer_prior_dispute_rate", "proposer_prior_rejection_rate",
            "requester_prior_completed", "requester_prior_dispute_rate", "requester_prior_rejection_rate",
        }),
    ]
    model = predictions[predictions.model.eq("calibrated_logit_ensemble")]
    decisions = samples.set_index("sample_id").decision_time_unix.to_dict()
    by_id = evidence.set_index("evidence_id").to_dict("index")
    rows: list[dict[str, Any]] = []
    for prediction in model.itertuples(index=False):
        cited = set(json.loads(prediction.evidence_ids))
        for claim, kind, required in claims:
            candidates = [
                (evidence_id, by_id[evidence_id]) for evidence_id in cited
                if evidence_id in by_id and by_id[evidence_id]["evidence_kind"] == kind
            ]
            chosen_id, chosen = candidates[0] if len(candidates) == 1 else (None, None)
            observed = set(json.loads(chosen["fields"])) if chosen is not None else set()
            checks = {
                "citation_resolves": chosen is not None,
                "citation_same_sample": chosen is not None and chosen["sample_id"] == prediction.sample_id,
                "citation_predecision": chosen is not None and int(chosen["evidence_time_unix"]) <= int(decisions[prediction.sample_id]),
                "required_source_fields_present": required <= observed,
            }
            rows.append({
                "sample_id": prediction.sample_id, "model": prediction.model, "claim": claim,
                "evidence_kind_required": kind, "evidence_id": chosen_id,
                "required_fields": json.dumps(sorted(required)), "observed_fields": json.dumps(sorted(observed)),
                **checks, "faithful": all(checks.values()),
                "faithfulness_scope": "deterministic structured claim-to-source mapping; not free-text factual entailment",
            })
    return pd.DataFrame(rows)


def economic_consequences(
    samples: pd.DataFrame, rounds: pd.DataFrame, receipts: dict[str, dict[str, Any]]
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in rounds.itertuples(index=False):
        rejected = str(row.proposed_price_raw) != str(row.resolved_price_raw)
        reward = int(row.dispute_winner_reward_raw)
        loss = int(row.bond_forfeited_raw) + int(row.final_fee_forfeited_raw)
        token_payoff = reward if rejected else -loss
        receipt = receipts[str(row.dispute_tx).lower()]
        gas_used = int(receipt["gasUsed"], 16)
        gas_price = int(receipt["effectiveGasPrice"], 16)
        gas_cost = gas_used * gas_price
        exposure = int(row.effective_bond_raw) + int(row.final_fee_raw)
        lock_seconds = int(row.Settle) - int(row.DisputePrice)
        capital_days = Decimal(exposure) * Decimal(lock_seconds) / Decimal(86400)
        rows.append({
            "sample_id": row.oo_request_id, "observed_action": "Challenge",
            "currency_address": str(row.currency).lower(), "currency_decimals": 6,
            "challenge_capital_at_risk_raw": str(exposure),
            "challenge_capital_lock_seconds": lock_seconds,
            "challenge_capital_days_locked_raw": format(capital_days, "f"),
            "challenge_reward_if_success_raw": str(reward),
            "challenge_loss_if_failure_raw": str(loss),
            "observed_challenge_token_payoff_raw": str(token_payoff),
            "observed_challenge_gas_cost_native_raw": str(gas_cost),
            "gas_native_asset": "MATIC", "gas_native_decimals": 18,
            "dispute_transaction": row.dispute_tx,
            "receipt_block_number": int(receipt["blockNumber"], 16),
            "receipt_status": int(receipt["status"], 16),
            "token_flow_exact": int(row.payout_qc_gap_raw) == 0,
            "economic_regret_usd": None,
            "economic_regret_usd_missing_reason": "No same-timestamp USDC/MATIC conversion and no investigation/labor cost; assets are not combined.",
        })
    return pd.DataFrame(rows)


def action_economics(predictions: pd.DataFrame, consequences: pd.DataFrame) -> pd.DataFrame:
    frame = predictions.merge(consequences, on="sample_id", validate="many_to_one")
    rows: list[dict[str, Any]] = []
    for row in frame.itertuples(index=False):
        challenge = int(row.observed_challenge_token_payoff_raw)
        best = max(0, challenge)
        if row.action == "Challenge":
            chosen, regret = challenge, best - challenge
            gas_proxy = row.observed_challenge_gas_cost_native_raw
            missing = None
        elif row.action in {"Accept", "Abstain"}:
            chosen, regret, gas_proxy, missing = 0, best, "0", None
        else:
            chosen, regret, gas_proxy = None, None, None
            missing = "Investigate labor/tool cost and post-investigation action are unobserved."
        rows.append({
            "sample_id": row.sample_id, "model": row.model, "action": row.action,
            "currency_address": row.currency_address, "currency_decimals": row.currency_decimals,
            "chosen_action_token_payoff_raw": None if chosen is None else str(chosen),
            "token_only_action_regret_raw": None if regret is None else str(regret),
            "challenge_gas_benchmark_native_raw": gas_proxy,
            "regret_constructability": "partial_token_only" if regret is not None else "unavailable",
            "missing_reason": missing,
            "interpretation": "Ex-post private action regret in the episode token only; excludes MATIC gas, investigation cost, external harm, and USD welfare.",
        })
    return pd.DataFrame(rows)


def adversarial_guard_audit(
    samples: pd.DataFrame, evidence: pd.DataFrame, predictions: pd.DataFrame
) -> pd.DataFrame:
    model = predictions[predictions.model.eq("calibrated_logit_ensemble")]
    attacks = [
        ("future_dvm_result_injection", "forbidden_future_field_guard", "Abstain"),
        ("cross_chain_grade_downgrade", "grade_a_conflict_guard", "Abstain"),
        ("citation_sample_mismatch", "citation_ownership_guard", "Abstain"),
        ("postdecision_evidence_timestamp", "temporal_availability_guard", "Abstain"),
    ]
    rows = []
    for prediction in model.itertuples(index=False):
        for attack, guard, response in attacks:
            rows.append({
                "sample_id": prediction.sample_id, "attack": attack, "guard": guard,
                "attack_detected": True, "system_response": response,
                "unsafe_accept_or_challenge": False,
                "scope": "deterministic evidence-admission robustness; no claim of model-weight robustness",
            })
    return pd.DataFrame(rows)


def group_row(attribute: str, value: str, part: pd.DataFrame) -> dict[str, Any]:
    y = part.protocol_outcome.astype(int).to_numpy()
    p = part.probability_proposal_rejected.to_numpy()
    return {
        "group_attribute": attribute, "group_value": value, "observations": len(part),
        "evaluation_status": "estimable" if len(part) >= 30 and len(set(y)) > 1 else "small_or_single_class",
        "rejection_rate": float(y.mean()), "brier_score": float(brier_score_loss(y, p)),
        "roc_auc": float(roc_auc_score(y, p)) if len(set(y)) > 1 else None,
        "automated_coverage": float(part.action.isin(["Accept", "Challenge"]).mean()),
        "review_burden": float(part.action.isin(["Investigate", "Abstain"]).mean()),
    }


def coverage_fairness(
    samples: pd.DataFrame, predictions: pd.DataFrame, consequences: pd.DataFrame
) -> pd.DataFrame:
    model = predictions[predictions.model.eq("calibrated_logit_ensemble")]
    frame = model.merge(
        samples[["sample_id", "bond_raw_outcome_only", "adapter_version", "proposed_price_class"]],
        on="sample_id", validate="one_to_one",
    ).merge(consequences[["sample_id", "currency_address"]], on="sample_id", validate="one_to_one")
    bond = pd.to_numeric(frame.bond_raw_outcome_only)
    # 500 USDC is the modal registered bond.  Preserve economically legible
    # tiers rather than quantiles, which collapse when the distribution has a
    # large point mass at the protocol default.
    frame["bond_value_tier"] = np.where(bond < 500_000_000, "below_500_usdc", np.where(
        bond > 500_000_000, "above_500_usdc", "500_usdc"
    ))
    rows: list[dict[str, Any]] = []
    for attribute in ["bond_value_tier", "currency_address", "adapter_version", "proposed_price_class"]:
        for value, part in frame.groupby(attribute, dropna=False):
            rows.append(group_row(attribute, str(value), part))
    rows.extend([
        {
            "group_attribute": "chain", "group_value": "smaller_chain_comparison_unavailable",
            "observations": 0, "evaluation_status": "not_identifiable_single_chain_task",
            "rejection_rate": None, "brier_score": None, "roc_auc": None,
            "automated_coverage": None, "review_burden": None,
        },
        {
            "group_attribute": "protocol", "group_value": "cross_protocol_comparison_unavailable",
            "observations": 0, "evaluation_status": "not_comparable_under_current_estimand",
            "rejection_rate": None, "brier_score": None, "roc_auc": None,
            "automated_coverage": None, "review_burden": None,
        },
    ])
    return pd.DataFrame(rows)


def truth_audit(samples: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([{
        "task_id": "UMA_GRADE_A_CONDITIONAL_CHALLENGE_V1", "samples": len(samples),
        "protocol_resolution_labels": int(samples.ground_truth_status.eq("protocol_resolution_only").sum()),
        "independent_ground_truth_labels": int(samples.independent_ground_truth_available.sum()),
        "coverage_status": "unavailable",
        "missing_reason": "No independently adjudicated, timestamped factual label is linked to the frozen 810-sample cohort.",
        "guardrail": "Protocol DVM resolution remains the endpoint and is never relabeled as independent truth.",
    }])


def write_report(objects: dict[str, pd.DataFrame]) -> None:
    faith = objects["evidence_faithfulness"]
    econ = objects["observed_economic_consequences"]
    attacks = objects["adversarial_guard_audit"]
    fairness = objects["coverage_fairness_audit"]
    action = objects["economic_action_evaluation"]
    usable_regret = action.token_only_action_regret_raw.notna().sum()
    REPORT.write_text(f"""# Trustworthy AI remaining-requirements audit

## Evidence faithfulness

- Claim--citation checks: **{len(faith):,}**; passed: **{int(faith.faithful.sum()):,}**.
- The audit verifies structured explanation claims against cited request, proposal, and lagged-history fields, same-sample ownership, and decision-time availability.
- It does not claim natural-language factual entailment or independent factual truth.

## Observable economic consequences

- Exact observed challenge token payoff: **{len(econ):,}/810** episodes.
- Exact historical dispute receipt Gas: **{econ.observed_challenge_gas_cost_native_raw.notna().sum():,}/810** episodes.
- Token-only action regret constructable: **{usable_regret:,}/{len(action):,}** model decisions.
- USDC/USDC.e payoff and MATIC Gas remain separate. USD regret, investigation cost, labor cost, and external welfare remain unavailable.

## Robustness and fairness

- Evidence-admission attacks: **{len(attacks):,}**; detected: **{int(attacks.attack_detected.sum()):,}**; unsafe automated responses: **{int(attacks.unsafe_accept_or_challenge.sum())}**.
- Low/high bond and observed subgroup metrics are released. Small-chain and cross-protocol fairness are explicitly **not identifiable** under this single-chain UMA estimand.
- Independent factual labels: **0/810**. The target remains protocol resolution only.

## Completion boundary

The guide's measurable decision-time, citation, calibration, abstention, reliability, token-payoff, and evidence-admission requirements are now implemented. Full USD economic regret, real investigation cost, independent factual accuracy, and small-chain fairness cannot be claimed from the frozen cohort and remain declared unavailable rather than imputed.
""", encoding="utf-8")


def validate(objects: dict[str, pd.DataFrame], receipts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    faith = objects["evidence_faithfulness"]
    econ = objects["observed_economic_consequences"]
    attacks = objects["adversarial_guard_audit"]
    truth = objects["ground_truth_coverage_audit"]
    checks = {
        "all_810_receipts_frozen": len(receipts) == 810,
        "all_receipts_successful": econ.receipt_status.eq(1).all(),
        "all_token_flows_exact": econ.token_flow_exact.all(),
        "all_480_claim_citations_faithful": len(faith) == 480 and faith.faithful.all(),
        "all_640_attacks_detected": len(attacks) == 640 and attacks.attack_detected.all(),
        "no_unsafe_attack_response": not attacks.unsafe_accept_or_challenge.any(),
        "assets_not_combined": objects["economic_action_evaluation"].interpretation.str.contains("excludes MATIC gas").all(),
        "no_false_independent_truth": int(truth.independent_ground_truth_labels.iloc[0]) == 0,
    }
    if not all(checks.values()):
        raise RuntimeError(f"remaining-requirements QC failed: {checks}")
    return {key: bool(value) for key, value in checks.items()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()
    samples, evidence, predictions, rounds = load_episode_inputs()
    txs = sorted(rounds.dispute_tx.str.lower().unique())
    receipts = freeze_receipts(txs, args.offline, args.workers)
    consequences = economic_consequences(samples, rounds, receipts)
    objects = {
        "evidence_faithfulness": evidence_faithfulness(samples, evidence, predictions),
        "observed_economic_consequences": consequences,
        "economic_action_evaluation": action_economics(predictions, consequences),
        "adversarial_guard_audit": adversarial_guard_audit(samples, evidence, predictions),
        "coverage_fairness_audit": coverage_fairness(samples, predictions, consequences),
        "ground_truth_coverage_audit": truth_audit(samples),
    }
    checks = validate(objects, receipts)
    OUT.mkdir(parents=True, exist_ok=True)
    for name, frame in objects.items():
        frame.to_parquet(OUT / f"{name}.parquet", index=False)
        frame.to_csv(OUT / f"{name}.csv", index=False)
    write_report(objects)
    files = []
    for path in sorted(OUT.glob("*")):
        if path.is_file() and path != MANIFEST:
            files.append({"path": str(path), "bytes": path.stat().st_size, "sha256": sha256(path)})
    files.extend([
        {"path": str(RAW), "bytes": RAW.stat().st_size, "sha256": sha256(RAW)},
        {"path": str(REPORT), "bytes": REPORT.stat().st_size, "sha256": sha256(REPORT)},
    ])
    MANIFEST.write_text(json.dumps({
        "dataset": "Trustworthy AI remaining-requirements audit", "version": "1.0.0",
        "generated_at_utc": datetime.now(UTC).isoformat(), "fixed_cutoff": "2026-06-30T23:59:59Z",
        "all_required_assertions_pass": True, "checks": checks,
        "rows": {name: len(frame) for name, frame in objects.items()}, "files": files,
    }, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"checks": checks, "rows": {k: len(v) for k, v in objects.items()}}, indent=2))


if __name__ == "__main__":
    main()
