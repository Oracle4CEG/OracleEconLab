"""Create the release-level completeness and consistency proof for the dataset."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
MANIFEST_DIR = ROOT / "data/manifests"
CURATED_DIR = (ROOT / "data/curated").resolve()

STANDARD_ASSERTION_MANIFESTS = (
    "tellor_layer_disputes",
    "tellor_tips_withdrawals",
    "tellor_aggregate_index",
    "tellor_rewards_full",
    "tellor_micro_reports",
    "tellor_jail_lifecycle",
    "dia_staking",
    "flare_fsp_rewards",
    "flare_claims_chill",
    "pyth_ois_rolling_state",
    "pyth_ois_history",
    "pyth_historical_observability",
    "flare_reward_attribution",
    "phase4_oracle_economic_interfaces",
    "chronicle_redstone_ethereum_events",
    "chronicle_redstone_settlement_audit",
    "accountability_events",
    "research_samples",
)

CUTOFF_MANIFESTS = (
    "tellor_layer_disputes",
    "tellor_tips_withdrawals",
    "tellor_aggregate_index",
    "tellor_rewards_full",
    "tellor_micro_reports",
    "tellor_jail_lifecycle",
    "dia_staking",
    "flare_fsp_rewards",
    "flare_claims_chill",
    "pyth_ois_rolling_state",
    "pyth_ois_history",
    "pyth_historical_observability",
    "flare_reward_attribution",
    "phase4_oracle_economic_interfaces",
    "chronicle_redstone_ethereum_events",
    "chronicle_redstone_settlement_audit",
    "realized_reward_slash_events",
    "accountability_events",
    "research_samples",
)

REQUIRED_MANIFESTS = (
    "chainlink_evidence_ledger",
    "chainlink_staking_v02_ledger",
    "polygon_uma_ledger",
    "polygon_uma_token_flow_ledger",
    "uma_crosschain_links",
    "uma_dvm_ledger",
    *STANDARD_ASSERTION_MANIFESTS,
    "contract_semantics_audit",
    "curated_parquet",
    "realized_reward_slash_events",
    "oracle_universe_registry",
    "oracle_observability_scores",
)

REQUIRED_TABLES = (
    "chainlink_eth_usd_reports",
    "chainlink_event_link_flow_qc",
    "chainlink_link_flows",
    "chainlink_staking_v02_events",
    "polygon_oov2_events",
    "polygon_adapter_events",
    "polygon_child_tunnel_events",
    "polygon_uma_gamma_links",
    "polygon_uma_request_rounds",
    "polygon_uma_token_flows",
    "uma_polygon_ethereum_grade_a_links",
    "uma_dvm_requests",
    "uma_dvm_staking_events",
    "uma_dvm_voter_payoffs",
    "uma_dvm_votes_events",
    "tellor_disputes",
    "tellor_dispute_votes",
    "tellor_dispute_payments",
    "tellor_query_tip_funding",
    "tellor_tip_withdrawals_realized",
    "tellor_aggregate_height_index",
    "tellor_liveness_reward_distributions_full",
    "tellor_reporter_reward_accruals_full",
    "tellor_legacy_selector_reward_accruals",
    "tellor_micro_reports",
    "tellor_jail_events",
    "tellor_jail_lifecycles",
    "dia_staking_events",
    "dia_staking_withdrawals",
    "flare_reward_claims",
    "flare_reward_epochs",
    "flare_voter_registrations",
    "flare_provider_conditions",
    "flare_provider_feed_performance",
    "flare_reward_claim_events",
    "flare_beneficiary_chill_events",
    "flare_reward_claim_epoch_qc",
    "pyth_ois_instructions",
    "pyth_ois_stake_events",
    "pyth_ois_economic_events",
    "pyth_ois_historical_observability",
    "flare_reward_component_attribution",
    "phase4_oracle_economic_interfaces",
    "chronicle_ethereum_events",
    "chronicle_redstone_settlement_interfaces",
    "redstone_ethereum_push_events",
    "ecosystem_observability_evidence",
)

REQUIRED_PARQUET_ONLY = (
    "economic_semantics_events",
    "realized_reward_slash_events",
    "accountability_events",
    "sample_b_observable_accountability",
    "sample_c_strict_honesty_events",
)


def load_manifest(name: str) -> dict[str, Any]:
    path = MANIFEST_DIR / f"{name}.json"
    if not path.is_file():
        raise RuntimeError(f"missing required manifest: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def count_jsonl(path: Path) -> int:
    count = 0
    with path.open("rb") as handle:
        while chunk := handle.read(64 * 1024 * 1024):
            count += chunk.count(b"\n")
    return count


def find_parquet(stem: str) -> Path:
    candidates = (
        CURATED_DIR / f"{stem}.parquet",
        CURATED_DIR / "parquet" / f"{stem}.parquet",
    )
    for path in candidates:
        if path.is_file():
            return path
    raise RuntimeError(f"missing required Parquet table: {stem}")


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def main() -> None:
    manifests = {name: load_manifest(name) for name in REQUIRED_MANIFESTS}
    assertion_failures = [
        name
        for name in STANDARD_ASSERTION_MANIFESTS
        if manifests[name].get("all_required_assertions_pass") is not True
    ]
    cutoff_failures = [
        name
        for name in CUTOFF_MANIFESTS
        if parse_time(str(manifests[name]["fixed_cutoff"])) != CUTOFF
    ]

    table_qc: dict[str, dict[str, Any]] = {}
    for stem in REQUIRED_TABLES:
        jsonl_path = CURATED_DIR / f"{stem}.jsonl"
        if not jsonl_path.is_file():
            raise RuntimeError(f"missing required JSONL table: {jsonl_path}")
        parquet_path = find_parquet(stem)
        jsonl_rows = count_jsonl(jsonl_path)
        parquet_rows = int(pq.ParquetFile(parquet_path).metadata.num_rows)
        table_qc[stem] = {
            "jsonl": str(jsonl_path),
            "parquet": str(parquet_path),
            "jsonl_rows": jsonl_rows,
            "parquet_rows": parquet_rows,
            "row_counts_match": jsonl_rows == parquet_rows,
        }
    for stem in REQUIRED_PARQUET_ONLY:
        parquet_path = find_parquet(stem)
        parquet_rows = int(pq.ParquetFile(parquet_path).metadata.num_rows)
        table_qc[stem] = {
            "jsonl": None,
            "parquet": str(parquet_path),
            "jsonl_rows": None,
            "parquet_rows": parquet_rows,
            "row_counts_match": parquet_rows > 0,
        }

    semantic_failures: list[str] = []
    chainlink = manifests["chainlink_evidence_ledger"]
    if not chainlink.get("feed_events") or int(chainlink.get("link_flows", 0)) <= 0:
        semantic_failures.append("chainlink_evidence_ledger_empty")
    polygon = manifests["polygon_uma_ledger"]
    if int(polygon.get("duplicate_source_logs", -1)) != 0:
        semantic_failures.append("polygon_uma_duplicate_source_logs")
    if int(polygon.get("payout_qc_nonzero_gaps", -1)) != 0:
        semantic_failures.append("polygon_uma_payout_flow_gap")
    crosschain = manifests["uma_crosschain_links"]
    if int(crosschain.get("ambiguous_matches", -1)) != 0:
        semantic_failures.append("uma_crosschain_ambiguous_match")
    realized = manifests["realized_reward_slash_events"]
    realized_qc = realized.get("qc") or {}
    for key in (
        "voter_slashed_rows_in_realized",
        "uma_dispute_penalty_rows_not_flow_exact",
        "chainlink_claims_not_flow_exact",
        "realized_rows_without_payment_or_state_delta",
        "duplicate_evidence_ids",
    ):
        if int(realized_qc.get(key, -1)) != 0:
            semantic_failures.append(f"realized_reward_slash_events:{key}")
    if realized_qc.get("dvm_accrued_net_equals_applied_net") is not True:
        semantic_failures.append("dvm_accrued_net_not_equal_applied_net")
    semantics = manifests["contract_semantics_audit"]
    if len(semantics.get("rules") or []) < 20:
        semantic_failures.append("contract_semantics_rules_incomplete")
    scores = manifests["oracle_observability_scores"]
    if int(scores.get("ecosystem_audit_complete_rows", 0)) != int(scores.get("rows", -1)):
        semantic_failures.append("oracle_registry_audit_incomplete")
    if any(int(value) != int(scores.get("rows", -1)) for value in (scores.get("non_null_scores") or {}).values()):
        semantic_failures.append("oracle_registry_score_nulls_remain")
    samples = manifests["research_samples"]
    if parse_time(samples["generated_at_utc"]) < parse_time(manifests["accountability_events"]["generated_at_utc"]):
        semantic_failures.append("research_samples_older_than_accountability_ledger")
    dia = manifests["dia_staking"]
    if int(dia.get("realized_withdrawals", 0)) <= 0:
        semantic_failures.append("dia_realized_withdrawals_empty")
    if int(dia.get("exact_principal_reward_payment_decompositions", -1)) != int(dia.get("realized_withdrawals", -2)):
        semantic_failures.append("dia_principal_reward_payment_not_exact")
    if dia.get("slashing_amount_imputed_as_zero") is not False:
        semantic_failures.append("dia_slashing_improperly_imputed_as_zero")

    table_count_failures = [
        stem for stem, row in table_qc.items() if not row["row_counts_match"]
    ]
    all_pass = not (
        assertion_failures
        or cutoff_failures
        or semantic_failures
        or table_count_failures
    )
    release = {
        "dataset": "Oracle accountability atlas fixed-cutoff release QC",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff": CUTOFF.isoformat(),
        "required_manifests": list(REQUIRED_MANIFESTS),
        "required_tables": list(REQUIRED_TABLES),
        "required_parquet_only_tables": list(REQUIRED_PARQUET_ONLY),
        "standard_assertion_failures": assertion_failures,
        "cutoff_failures": cutoff_failures,
        "semantic_failures": semantic_failures,
        "table_count_failures": table_count_failures,
        "table_qc": table_qc,
        "total_required_rows": sum(
            int(row["parquet_rows"]) for row in table_qc.values()
        ),
        "all_required_assertions_pass": all_pass,
        "scope_guard": (
            "Complete means all protocol-observable rows inside the declared "
            "security/delivery-chain scope and fixed cutoff. A protocol that "
            "does not expose a publisher reward/slash settlement interface is "
            "recorded as structurally unobservable, never silently encoded as zero."
        ),
    }
    if not all_pass:
        raise RuntimeError(f"release QC failed: {release}")
    manifest_path = MANIFEST_DIR / "oracle_dataset_release.json"
    atomic_json(manifest_path, release)
    report_path = ROOT / "reports/oracle_dataset_release_qc.md"
    report_path.write_text(
        "\n".join(
            (
                "# Oracle dataset release QC",
                "",
                f"Generated: {release['generated_at_utc']}  ",
                f"Fixed cutoff: {release['fixed_cutoff']}  ",
                "",
                f"- Required manifests: {len(REQUIRED_MANIFESTS):,}.",
                f"- Required JSONL/Parquet table pairs: {len(REQUIRED_TABLES):,}.",
                f"- Required derived Parquet-only tables: {len(REQUIRED_PARQUET_ONLY):,}.",
                f"- Total rows across required tables: {release['total_required_rows']:,}.",
                "- Manifest assertions, cutoff equality, semantic guards, and row-count parity all pass.",
                "",
                release["scope_guard"],
                "",
            )
        ),
        encoding="utf-8",
    )
    print(manifest_path)


if __name__ == "__main__":
    main()
