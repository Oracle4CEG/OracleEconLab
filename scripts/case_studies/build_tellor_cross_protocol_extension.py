#!/usr/bin/env python3
"""Map every resolved Tellor Layer dispute into the shared economic schema.

Usage:
    python scripts/case_studies/build_tellor_cross_protocol_extension.py

The mapping deliberately separates protocol-state exposure/slash from observed
payments.  A MsgWithdrawFeeRefund is retained as a gross receipt because its
principal/reward composition is not identifiable from the receipt alone.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.environ.get(
    "ORACLE_NATURE_CURATED_ROOT", str(ROOT / "data/curated")
)) / "parquet"
OUT = ROOT / "data/case_studies/tellor_cross_protocol_extension"
REPORT = ROOT / "reports/tellor_cross_protocol_extension.md"
SCHEMA_PATH = ROOT / "schemas/cross_chain_economic_observation.schema.json"
SOURCE_MANIFEST = ROOT / "data/manifests/tellor_layer_disputes.json"
FIXED_CUTOFF = "2026-06-30T23:59:59+00:00"
SUPPORT_RESULTS = {"SUPPORT", "NO_QUORUM_MAJORITY_SUPPORT"}


def canonical_hash(payload: Any) -> str:
    value = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(value.encode()).hexdigest()


def unix(value: Any) -> int:
    return int(pd.Timestamp(value).timestamp())


def decimal_string(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(int(value))


def read_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    manifest = json.loads(SOURCE_MANIFEST.read_text(encoding="utf-8"))
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError("Tellor source ledger did not pass release QC")
    con = duckdb.connect()
    disputes = con.execute(
        f"SELECT * FROM read_parquet('{DATA_ROOT / 'tellor_disputes.parquet'}') "
        "ORDER BY CAST(dispute_id AS INTEGER)"
    ).fetchdf()
    payments = con.execute(
        f"SELECT * FROM read_parquet('{DATA_ROOT / 'tellor_dispute_payments.parquet'}') "
        "ORDER BY CAST(dispute_id AS INTEGER), block_time, source_tx"
    ).fetchdf()
    votes = con.execute(
        f"SELECT * FROM read_parquet('{DATA_ROOT / 'tellor_dispute_votes.parquet'}') "
        "ORDER BY CAST(dispute_id AS INTEGER), block_time, source_tx"
    ).fetchdf()
    con.close()
    return disputes, payments, votes, manifest


def build_episode(row: pd.Series, receipts: pd.DataFrame, vote_count: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    dispute_id = str(row.dispute_id)
    start = unix(row.dispute_start_time)
    end = unix(row.dispute_end_time)
    support = row.vote_result in SUPPORT_RESULTS
    bond = int(row.dispute_fee_raw)
    slash = int(row.slash_amount_raw) if support else 0
    forfeiture = 0 if support else int(row.fee_total_raw)
    actor_receipts = receipts[
        receipts.actor.eq(row.disputer) & receipts.event.eq("MsgWithdrawFeeRefund")
    ]
    gross_receipt = (
        sum(int(v) for v in actor_receipts.received_loya_raw) if len(actor_receipts) else None
    )

    variable_sources = {
        "bond_raw": [row.source_tx],
        "bond_forfeited_raw": [row.source_tx],
        "principal_slashed_raw": [row.source_tx],
        "gross_payout_raw": actor_receipts.source_tx.tolist(),
        "resolution_delay_seconds": [row.source_tx],
        "terminal_outcome": [row.source_tx],
    }
    variable_values = {
        "bond_raw": str(bond),
        "bond_forfeited_raw": str(forfeiture),
        "principal_slashed_raw": str(slash),
        "gross_payout_raw": None if gross_receipt is None else str(gross_receipt),
        "resolution_delay_seconds": str(end - start),
        "terminal_outcome": str(row.vote_result),
    }
    provenance: list[dict[str, Any]] = []
    for variable, value in variable_values.items():
        payload = {
            "episode_id": f"tellor:tellor-1:dispute:{dispute_id}",
            "variable_name": variable,
            "value": value,
            "source_transactions": variable_sources[variable],
            "source_table": "tellor_disputes" if variable != "gross_payout_raw" else "tellor_dispute_payments",
            "fixed_cutoff": FIXED_CUTOFF,
            "transformation_rule_id": f"TELLOR_CROSS_PROTOCOL_{variable.upper()}_V1",
        }
        provenance.append({
            **payload,
            "source_transactions": json.dumps(payload["source_transactions"]),
            "evidence_grade": "B",
            "validation_status": "passed",
            "provenance_id": canonical_hash(payload),
        })
    aggregate_pid = canonical_hash(sorted(p["provenance_id"] for p in provenance))

    note = (
        "Tellor protocol vote adjudication, not independent truth. dispute_fee_raw is capital at "
        "risk for the challenge. bond_forfeited_raw is populated only for an unsuccessful challenge; "
        "principal_slashed_raw is populated only when the report is rejected. "
        "Observed fee-refund receipts remain gross and are not decomposed into returned principal or reward."
    )
    episode = {
        "schema_version": "1.0.0",
        "episode_id": f"tellor:tellor-1:dispute:{dispute_id}",
        "protocol": "Tellor",
        "mechanism": "Tellor Layer paid dispute and group vote",
        "native_unit_type": "dispute",
        "observation_unit": "dispute",
        "security_chain_namespace": "cosmos",
        "security_chain_id": "tellor-1",
        "delivery_chain_namespace": "cosmos",
        "delivery_chain_id": "tellor-1",
        "actor": str(row.disputer),
        "actor_role": "disputer",
        "counterparty": str(row.reporter),
        "counterparty_role": "reporter",
        "decision_time_unix": start,
        "proposal_time_unix": None,
        "dispute_time_unix": start,
        "settlement_time_unix": None,
        "challenge_deadline_unix": end,
        "terminal_time_unix": end,
        "action": "challenge",
        "terminal_outcome": str(row.vote_result),
        "right_censored": False,
        "independent_ground_truth": None,
        "ground_truth_status": "protocol_resolution_only",
        "asset_address": None,
        "asset_symbol": "loya",
        "asset_decimals": int(row.asset_decimals),
        "bond_raw": str(bond),
        "reward_configured_raw": None,
        "reward_paid_raw": None,
        "reward_forfeited_raw": None,
        "principal_returned_raw": None,
        "bond_forfeited_raw": str(forfeiture),
        "final_fee_forfeited_raw": None,
        "principal_slashed_raw": str(slash),
        "protocol_fee_raw": None,
        "gross_payout_raw": None if gross_receipt is None else str(gross_receipt),
        "realized_payoff_raw": None,
        "gas_cost_native_raw": None,
        "investigation_cost_usd": None,
        "delay_cost_usd": None,
        "capital_cost_usd": None,
        "usd_value": None,
        "usd_conversion_source": None,
        "usd_conversion_time_unix": None,
        "reward_to_bond_ratio": None,
        "dispute_decision": True,
        "proposal_upheld": not support,
        "mandatory_wait_seconds": end - start,
        "resolution_delay_seconds": end - start,
        "settlement_delay_seconds": None,
        "excess_delay_seconds": None,
        "capital_days_locked_raw": str(bond * (end - start) / 86400),
        "verification_cost_usd": None,
        "economic_regret_usd": None,
        "independent_ground_truth_available": False,
        "dvm_positive_redistribution_raw": None,
        "dvm_negative_slash_raw": None,
        "decision_evidence_ids": [provenance[0]["provenance_id"]],
        "evidence_snapshot_time_unix": start,
        "future_fields_excluded": [
            "vote_result", "terminal_outcome", "principal_slashed_raw",
            "bond_forfeited_raw", "gross_payout_raw", "dispute_votes",
        ],
        "source_chain_namespace": "cosmos",
        "source_chain_id": "tellor-1",
        "source_contract": "layer/dispute",
        "source_transaction": str(row.source_tx).lower(),
        "source_log_index": None,
        "source_block_number": int(row.source_block),
        "source_block_timestamp_unix": start,
        "source_event": "MsgProposeDispute / new_dispute",
        "source_table": "tellor_disputes + tellor_dispute_payments + tellor_dispute_votes",
        "source_finality_rule": (
            "Successful canonical tellor-1 transactions through height 19,890,860 at the fixed cutoff; "
            "resolved protocol state required."
        ),
        "cross_chain_link_grade": "not_applicable",
        "provenance_id": aggregate_pid,
        "prov_entity_id": f"prov:entity:tellor:dispute:{dispute_id}",
        "prov_activity_id": "prov:activity:build_tellor_cross_protocol_extension_v1",
        "prov_agent_id": "prov:softwareAgent:oracle-nature",
        "transformation_rule_id": "TELLOR_CROSS_PROTOCOL_DISPUTE_V1",
        "contract_semantics_rule_id": "TELLOR_LAYER_DISPUTE_STATE_AND_TX_V1",
        "coverage_status": "partial",
        "missing_reason": (
            "Independent truth, gas, investigation/USD costs and exact disputer settlement decomposition "
            "are unavailable; only observed gross refund receipts are retained."
        ),
        "evidence_grade": "B",
        "validation_status": "passed",
        "validation_rule_ids": [
            "TELLOR_DISPUTE_RESOLVED_V1", "TELLOR_OUTCOME_CONTINGENT_EXPOSURE_V1",
            "TELLOR_PAYMENT_NONDECOMPOSITION_V1", "TELLOR_DECISION_TIME_LEAKAGE_GUARD_V1",
        ],
        "interpretation_note": f"{note} Vote-message rows linked to this dispute: {vote_count}.",
    }
    return episode, provenance


def validate(episodes: list[dict[str, Any]], disputes: pd.DataFrame) -> pd.DataFrame:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    schema_errors = [
        f"{e['episode_id']}: {error.message}"
        for e in episodes for error in validator.iter_errors(e)
    ]
    support = disputes.vote_result.isin(SUPPORT_RESULTS)
    checks = {
        "source_release_qc_passed": True,
        "all_13_resolved_disputes_mapped": len(episodes) == 13 and len(disputes) == 13,
        "shared_schema_valid": not schema_errors,
        "unique_episode_ids": len({e["episode_id"] for e in episodes}) == len(episodes),
        "three_reporter_slash_outcomes": int(support.sum()) == 3,
        "ten_challenger_forfeiture_outcomes": int((~support).sum()) == 10,
        "principal_slash_total_exact": sum(int(e["principal_slashed_raw"]) for e in episodes) == 79_920_000,
        "bond_forfeiture_total_exact": sum(int(e["bond_forfeited_raw"]) for e in episodes) == 70_260_000,
        "no_inferred_paid_rewards": all(e["reward_paid_raw"] is None for e in episodes),
        "no_inferred_realized_payoffs": all(e["realized_payoff_raw"] is None for e in episodes),
        "three_observed_gross_refunds": sum(e["gross_payout_raw"] is not None for e in episodes) == 3,
        "decision_time_future_fields_declared": all("vote_result" in e["future_fields_excluded"] for e in episodes),
        "single_chain_link_not_applicable": all(e["cross_chain_link_grade"] == "not_applicable" for e in episodes),
    }
    if schema_errors:
        print("\n".join(schema_errors))
    result = pd.DataFrame([{"check": key, "passed": bool(value)} for key, value in checks.items()])
    failed = result.loc[~result.passed, "check"].tolist()
    if failed:
        raise RuntimeError(f"Tellor cross-protocol QC failed: {failed}")
    return result


def comparability_table() -> pd.DataFrame:
    return pd.DataFrame([
        ["challenge capital at risk", "effective bond + final-fee exposure", "dispute fee", "comparable_with_qualification", "Both are action-contingent locked capital; components and refund rules differ."],
        ["failed challenge forfeiture", "disputer escrow loss", "dispute fee forfeiture", "comparable_with_qualification", "Same direction of incentive; payment paths and fee sinks differ."],
        ["incorrect report penalty", "proposer escrow redistribution", "reporter principal slash", "protocol_internal_only", "Both punish rejected reports, but UMA transfers escrow while Tellor slashes reporter stake."],
        ["resolution delay", "Polygon dispute to Ethereum DVM result/settlement", "fixed Tellor voting window", "comparable_with_qualification", "Elapsed time is comparable; adjudication institution and finality are not."],
        ["paid reward", "exact OOV2 settlement flow", "claims/refunds may occur later and aggregate components", "not_directly_comparable", "Tellor configured pools, voter claims and disputer refunds must not be merged."],
        ["realized actor payoff", "exact for selected UMA case", "not decomposable from current receipts", "unavailable_cross_protocol", "Do not replace missing Tellor payoff with designed slash or reward parameters."],
        ["truth", "protocol DVM resolution", "protocol group-vote resolution", "comparable_as_protocol_outcome_only", "Neither is independent factual ground truth."],
        ["cross-chain evidence", "Grade-A Polygon-to-Ethereum link", "single tellor-1 chain", "not_applicable", "Absence of a cross-chain link is structural, not missing data."],
    ], columns=["economic_concept", "uma_mapping", "tellor_mapping", "comparability", "guardrail"])


def write_report(episodes: list[dict[str, Any]], qc: pd.DataFrame, comparison: pd.DataFrame) -> None:
    support = sum(e["principal_slashed_raw"] != "0" for e in episodes)
    refunds = sum(e["gross_payout_raw"] is not None for e in episodes)
    lines = [
        "# Tellor cross-protocol economic-schema extension", "",
        f"Fixed cutoff: `{FIXED_CUTOFF}`. All {len(episodes)} resolved Tellor Layer disputes are mapped to the same 85-field schema used by the UMA benchmark.", "",
        "## Result", "",
        f"- Reporter-slash outcomes: {support}; exact designed principal slash: 79.920000 TRB.",
        f"- Failed-challenge outcomes: {len(episodes) - support}; exact designed dispute-fee forfeiture: 70.260000 TRB.",
        f"- Observed disputer gross refund receipts: {refunds}/13. These receipts are not relabeled as reward or realized payoff.",
        "- Independent truth, gas, off-chain investigation cost and exact settlement decomposition remain unavailable.", "",
        "## UMA--Tellor comparability", "",
        "| Economic concept | UMA | Tellor | Status | Guardrail |", "|---|---|---|---|---|",
    ]
    for row in comparison.itertuples(index=False):
        lines.append(f"| {row.economic_concept} | {row.uma_mapping} | {row.tellor_mapping} | {row.comparability} | {row.guardrail} |")
    lines += ["", "## QC", ""]
    lines += [f"- `{row.check}`: PASS" for row in qc.itertuples(index=False)]
    lines += ["", "## Reproduction", "", "```bash", "python scripts/case_studies/build_tellor_cross_protocol_extension.py", "```", ""]
    REPORT.write_text("\n".join(lines), encoding="utf-8")


def write_outputs(
    episodes: list[dict[str, Any]], provenance: list[dict[str, Any]], disputes: pd.DataFrame,
    payments: pd.DataFrame, votes: pd.DataFrame, qc: pd.DataFrame, comparison: pd.DataFrame,
) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(episodes).to_parquet(OUT / "economic_episodes.parquet", index=False)
    with (OUT / "economic_episodes.jsonl").open("w", encoding="utf-8") as handle:
        for episode in episodes:
            handle.write(json.dumps(episode, sort_keys=True) + "\n")
    pd.DataFrame(provenance).to_parquet(OUT / "variable_provenance.parquet", index=False)
    disputes.to_parquet(OUT / "source_disputes.parquet", index=False)
    payments.to_parquet(OUT / "source_payments.parquet", index=False)
    votes.to_parquet(OUT / "source_votes.parquet", index=False)
    qc.to_parquet(OUT / "qc_results.parquet", index=False)
    comparison.to_parquet(OUT / "uma_tellor_comparability.parquet", index=False)
    comparison.to_csv(OUT / "uma_tellor_comparability.csv", index=False)

    files = sorted(path for path in OUT.iterdir() if path.is_file() and path.name != "manifest.json")
    manifest = {
        "dataset": "Tellor cross-protocol economic-schema extension",
        "schema_version": "1.0.0",
        "fixed_cutoff": FIXED_CUTOFF,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_manifest": str(SOURCE_MANIFEST),
        "episodes": len(episodes),
        "all_required_assertions_pass": bool(qc.passed.all()),
        "files": [
            {"path": str(path), "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in files
        ],
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    disputes, payments, votes, _ = read_inputs()
    episodes: list[dict[str, Any]] = []
    provenance: list[dict[str, Any]] = []
    for _, row in disputes.iterrows():
        dispute_receipts = payments[payments.dispute_id.eq(str(row.dispute_id))]
        vote_count = int(votes.dispute_id.eq(str(row.dispute_id)).sum())
        episode, rows = build_episode(row, dispute_receipts, vote_count)
        episodes.append(episode)
        provenance.extend(rows)
    qc = validate(episodes, disputes)
    comparison = comparability_table()
    write_outputs(episodes, provenance, disputes, payments, votes, qc, comparison)
    write_report(episodes, qc, comparison)
    print(json.dumps({
        "episodes": len(episodes), "output": str(OUT), "report": str(REPORT),
        "qc_passed": bool(qc.passed.all()),
    }, indent=2))


if __name__ == "__main__":
    main()
