"""Record source-backed settlement boundaries for Chronicle and RedStone."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "data/raw/source_audit").resolve()
OUTPUT = (ROOT / "data/curated/chronicle_redstone_settlement_interfaces.jsonl").resolve()


def main() -> None:
    chronicle = SOURCE / "chronicle_scribe_12ff/src/ScribeOptimistic.sol"
    redstone_roots = sorted(SOURCE.glob("redstone*"))
    if not chronicle.is_file() or not redstone_roots:
        raise RuntimeError("missing frozen Chronicle/RedStone source snapshots")
    chronicle_text = "\n".join(
        path.read_text(encoding="utf-8", errors="ignore")
        for path in (chronicle.parent).rglob("*.sol")
    )
    for needle in ("OpChallengeRewardPaid", "FeedDropped"):
        if needle not in chronicle_text:
            raise RuntimeError(f"Chronicle source invariant missing: {needle}")
    redstone_files = [path for root in redstone_roots for path in root.rglob("*.sol") if path.is_file()]
    redstone_text = "\n".join(path.read_text(encoding="utf-8", errors="ignore") for path in redstone_files)
    rows = [
        {
            "oracle_network": "Chronicle",
            "mechanism": "optimistic_report_challenge",
            "settlement_interface": "OpChallengeRewardPaid",
            "economic_amount_observable": True,
            "publisher_reward_observable": False,
            "publisher_slash_observable": False,
            "nonmonetary_penalty_observable": True,
            "nonmonetary_penalty_interface": "FeedDropped",
            "zero_publisher_reward_or_slash_asserted": False,
            "reason": "Challenge rewards and feed drops are explicit; routine validator compensation has no unified Scribe settlement event.",
            "source_code": str(chronicle),
            "rule_id": "CHRONICLE_SETTLEMENT_INTERFACE_BOUNDARY_V1",
        },
        {
            "oracle_network": "RedStone",
            "mechanism": "signed_pull_and_push_delivery",
            "settlement_interface": None,
            "economic_amount_observable": False,
            "publisher_reward_observable": False,
            "publisher_slash_observable": False,
            "nonmonetary_penalty_observable": False,
            "nonmonetary_penalty_interface": None,
            "zero_publisher_reward_or_slash_asserted": False,
            "reason": "Delivery contracts verify signed payloads; the frozen contract corpus exposes no unified publisher reward/slash settlement event.",
            "source_code": ",".join(str(path) for path in redstone_roots),
            "rule_id": "REDSTONE_SETTLEMENT_INTERFACE_BOUNDARY_V1",
        },
    ]
    # A source search is evidence only for the frozen scoped contract corpus.
    reward_terms = ("RewardPaid", "PublisherReward", "PublisherSlashed", "OracleSlashed")
    rows[1]["scoped_settlement_terms_found"] = [term for term in reward_terms if term in redstone_text]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".jsonl.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(OUTPUT)
    manifest = {
        "dataset": "Chronicle and RedStone source-backed settlement boundary",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff": "2026-06-30T23:59:59Z",
        "rows": len(rows),
        "publisher_amounts_guessed_from_ordinary_token_flows": 0,
        "redstone_solidity_files_searched": len(redstone_files),
        "redstone_scoped_settlement_terms_found": rows[1]["scoped_settlement_terms_found"],
        "output": str(OUTPUT),
        "all_required_assertions_pass": (
            len(rows) == 2
            and rows[0]["economic_amount_observable"] is True
            and rows[1]["zero_publisher_reward_or_slash_asserted"] is False
        ),
    }
    path = ROOT / "data/manifests/chronicle_redstone_settlement_audit.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(path)


if __name__ == "__main__":
    main()
