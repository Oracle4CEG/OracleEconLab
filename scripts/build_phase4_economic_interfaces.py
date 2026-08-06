"""Freeze event-interface evidence for the six phase-four Oracle protocols.

This is deliberately an interface ledger, not a token-transfer heuristic. An
empty oracle-accountability event interface is a verified scope result and is
never represented as a zero slash amount.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = (ROOT / "data/curated/phase4_oracle_economic_interfaces.jsonl").resolve()

ROWS = [
    {
        "oracle_network": "Band", "security_chain": "BandChain", "adapter_status": "node_required",
        "economic_unit": "validator_consensus_not_individual_oracle_report",
        "reward_interface": "Cosmos distribution validator rewards",
        "penalty_interface": "Cosmos slashing validator jail/slash",
        "truth_linkability": "not_report_level",
        "source_evidence": "https://docs.bandchain.org/",
        "required_external_data": "BandChain archive RPC",
    },
    {
        "oracle_network": "Switchboard", "security_chain": "Solana_Jito_NCN", "adapter_status": "interface_identified",
        "economic_unit": "NCN_epoch_operator_and_vault",
        "reward_interface": "Jito NCN vault epoch distributions and SWTCH subsidy",
        "penalty_interface": "NCN operator slashing mechanism",
        "truth_linkability": "performance_documented_component_settlement_not_publicly_itemized",
        "source_evidence": "https://docs.switchboard.xyz/governance-and-tokenomics/governance-and-tokenomics",
        "required_external_data": "Solana archive plus Jito NCN account/program registry",
    },
    {
        "oracle_network": "API3", "security_chain": "multi_EVM", "adapter_status": "scoped_mechanism_absent",
        "economic_unit": "dapp_month_OEV_revenue_not_oracle_reporter_honesty",
        "reward_interface": "monthly OEV reward to consuming dApp",
        "penalty_interface": "no public report-level publisher slash interface in current dAPI contracts",
        "truth_linkability": "none_for_oracle_reporter_reward",
        "source_evidence": "https://docs.api3.org/dapps/oev-rewards/",
        "required_external_data": "none_for_negative_scope_result",
    },
    {
        "oracle_network": "DIA", "security_chain": "DIA_Lasernet", "adapter_status": "event_ledger_qc_complete",
        "economic_unit": "staking_position_realized_withdrawal",
        "reward_interface": "historical stakingStores reward fields reconciled to realized wDIA transfers",
        "penalty_interface": "official staking FAQ says slashing not implemented at cutoff",
        "truth_linkability": "base_staking_not_individual_report_correctness",
        "source_evidence": "https://docs.diadata.org/",
        "required_external_data": "complete_local_dia_lasernet_ledger",
        "realized_event_rows": 129,
        "realized_amount_raw": "22235907087809645291878",
    },
    {
        "oracle_network": "Stork", "security_chain": "offchain_signed_data_with_delivery_verification", "adapter_status": "scoped_mechanism_absent",
        "economic_unit": "signed_update_delivery",
        "reward_interface": "no unified public publisher reward settlement interface verified",
        "penalty_interface": "no unified public publisher slash settlement interface verified",
        "truth_linkability": "signed_report_delivery_only",
        "source_evidence": "https://docs.stork.network/",
        "required_external_data": "none_for_negative_scope_result",
    },
    {
        "oracle_network": "Supra", "security_chain": "Supra_Mainnet", "adapter_status": "node_required",
        "economic_unit": "L1_validator_epoch_not_individual_oracle_report",
        "reward_interface": "validator staking reward claim",
        "penalty_interface": "validator consensus penalty if exposed by chain state",
        "truth_linkability": "not_report_level",
        "source_evidence": "https://docs.supra.com/network/node/node-operator-faq",
        "required_external_data": "Supra Mainnet archive REST/RPC",
    },
]


def main() -> None:
    now = datetime.now(UTC).isoformat()
    rows = []
    for row in ROWS:
        copy = dict(row)
        copy.update({
            "audit_time_utc": now,
            "fixed_cutoff": "2026-06-30T23:59:59Z",
            "realized_event_rows": copy.get("realized_event_rows"),
            "realized_amount_raw": copy.get("realized_amount_raw"),
            "zero_amount_asserted": False,
            "rule_id": "PHASE4_ORACLE_ECONOMIC_INTERFACE_AUDIT_V1",
        })
        rows.append(copy)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".jsonl.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(OUTPUT)
    manifest = {
        "dataset": "Phase-four Oracle economic event-interface ledger",
        "generated_at_utc": now,
        "fixed_cutoff": "2026-06-30T23:59:59Z",
        "protocols": len(rows),
        "protocol_names": [row["oracle_network"] for row in rows],
        "node_required": [row["oracle_network"] for row in rows if row["adapter_status"] == "node_required"],
        "verified_scoped_absence": [row["oracle_network"] for row in rows if row["adapter_status"] == "scoped_mechanism_absent"],
        "amounts_guessed_from_token_flows": 0,
        "output": str(OUTPUT),
        "all_required_assertions_pass": len(rows) == 6 and all(not row["zero_amount_asserted"] for row in rows),
    }
    path = ROOT / "data/manifests/phase4_oracle_economic_interfaces.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(path)


if __name__ == "__main__":
    main()
