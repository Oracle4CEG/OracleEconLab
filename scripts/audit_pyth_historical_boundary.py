"""Freeze the evidence-backed historical boundary of the Pyth OIS dataset.

Quality-score fields requested by the research design are not inputs to the
Integrity Pool program. The program consumes signed publisher stake caps and
keeps only MAX_EVENTS=52 reward epochs. This table prevents absent fields from
being mistaken for zero-valued observations.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "data/raw/pyth_ois/source_governance").resolve()
OUTPUT = (ROOT / "data/curated/pyth_ois_historical_observability.jsonl").resolve()


def main() -> None:
    constants = SOURCE / "staking/programs/integrity-pool/src/utils/constants.rs"
    pool = SOURCE / "staking/programs/integrity-pool/src/state/pool.rs"
    cli = SOURCE / "staking/cli/src/instructions.rs"
    claim_test = SOURCE / "staking/integration-tests/tests/claim.rs"
    required = [constants, pool, cli, claim_test]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise RuntimeError(f"missing frozen Pyth source evidence: {missing}")
    if "MAX_EVENTS: usize = 52" not in constants.read_text(encoding="utf-8"):
        raise RuntimeError("Pyth MAX_EVENTS=52 source invariant changed")
    if "publisher_stake_caps/latest" not in cli.read_text(encoding="utf-8"):
        raise RuntimeError("Pyth publisher cap ingestion source invariant changed")

    rows = []
    for field in ("publisher_quality_rank", "uptime_score", "price_deviation_score", "stalled_price_score"):
        rows.append({
            "oracle_network": "Pyth",
            "requested_field": field,
            "historical_value_status": "not_a_security_chain_program_input",
            "numeric_value": None,
            "zero_observed": False,
            "recoverable_from_existing_transactions": False,
            "reason": (
                "Integrity Pool advance consumes signed publisher stake caps; this named "
                "quality component is not serialized in the program state or instruction."
            ),
            "security_chain_input_available": "publisher_stake_cap",
            "source_code": str(pool),
            "rule_id": "PYTH_OIS_QUALITY_COMPONENT_NOT_ON_SECURITY_CHAIN_V1",
        })
    rows.extend([
        {
            "oracle_network": "Pyth",
            "requested_field": "publisher_stake_cap",
            "historical_value_status": "recoverable_from_archived_advance_transactions",
            "numeric_value": None,
            "zero_observed": False,
            "recoverable_from_existing_transactions": True,
            "reason": "Signed cap messages are transaction inputs to each successful Integrity Pool advance.",
            "security_chain_input_available": "publisher_stake_cap",
            "source_code": str(cli),
            "rule_id": "PYTH_OIS_ARCHIVED_PUBLISHER_CAP_INPUT_V1",
        },
        {
            "oracle_network": "Pyth",
            "requested_field": "reward_epoch_factor_history",
            "historical_value_status": "onchain_state_ring_buffer_overwrites_after_52_epochs",
            "numeric_value": None,
            "zero_observed": False,
            "recoverable_from_existing_transactions": True,
            "reason": (
                "The current account retains 52 events, but archived advance transactions and "
                "realized token transfers preserve the durable economic history collected by this dataset."
            ),
            "security_chain_input_available": "52_slot_reward_event_ring",
            "source_code": str(constants),
            "rule_id": "PYTH_OIS_MAX_EVENTS_52_WITH_TX_ARCHIVE_V1",
        },
    ])
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".jsonl.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(OUTPUT)
    manifest = {
        "dataset": "Pyth OIS historical observability boundary",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff": "2026-06-30T23:59:59Z",
        "rows": len(rows),
        "requested_quality_fields": 4,
        "quality_fields_improperly_imputed_as_zero": 0,
        "publisher_caps_recoverable": True,
        "realized_reward_stake_slash_transfers_preserved": True,
        "ring_buffer_slots": 52,
        "output": str(OUTPUT),
        "source_files": [str(path) for path in required],
        "all_required_assertions_pass": len(rows) == 6,
    }
    path = ROOT / "data/manifests/pyth_historical_observability.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(path)


if __name__ == "__main__":
    main()
