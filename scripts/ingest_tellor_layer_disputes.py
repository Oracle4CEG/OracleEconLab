"""Collect the complete Tellor Layer dispute panel through the fixed cutoff.

The official public API is sufficient for the small dispute panel. Full report,
tip, and end-block reward history remains a separate archive-node collection.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from oracle_ledger.tellor_layer import TellorClient, event_attributes, loya_received_by


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_RPC = "https://mainnet.tellorlayer.com/rpc"
DEFAULT_API = "https://mainnet.tellorlayer.com"
ACTIONS = [
    "/layer.dispute.MsgProposeDispute",
    "/layer.dispute.MsgAddFeeToDispute",
    "/layer.dispute.MsgVote",
    "/layer.dispute.MsgClaimReward",
    "/layer.dispute.MsgWithdrawFeeRefund",
    "/layer.dispute.MsgAddEvidence",
]


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)


def write_gzip_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=9) as handle:
        json.dump(value, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
    temporary.replace(path)


def paginate_disputes(client: TellorClient) -> list[dict[str, Any]]:
    key: str | None = None
    disputes: list[dict[str, Any]] = []
    while True:
        params = {"pagination.limit": "100"}
        if key:
            params["pagination.key"] = key
        body = client.api("/tellor-io/layer/dispute/disputes", params)
        disputes.extend(body.get("disputes") or [])
        key = (body.get("pagination") or {}).get("next_key")
        if not key:
            return disputes


def normalized_tx(client: TellorClient, tx: dict[str, Any]) -> dict[str, Any]:
    decoded = client.decoded_tx(tx["hash"])
    response = decoded["tx_response"]
    messages = decoded["tx"]["body"]["messages"]
    return {
        "tx_hash": tx["hash"],
        "height": int(tx["height"]),
        "timestamp": response.get("timestamp"),
        "code": int(response.get("code") or 0),
        "messages": messages,
        "events": response.get("events") or [],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Tellor Layer dispute accountability evidence")
    parser.add_argument("--rpc-url", default=os.getenv("TELLOR_RPC_URL", DEFAULT_RPC))
    parser.add_argument("--api-url", default=os.getenv("TELLOR_API_URL", DEFAULT_API))
    args = parser.parse_args()
    client = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = client.height_at_or_before(CUTOFF)
    cutoff_block_time = client.block_time(cutoff_height).isoformat()
    raw_dir = (ROOT / "data/raw/tellor_layer").resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True); curated_dir.mkdir(parents=True, exist_ok=True)

    disputes_state = paginate_disputes(client)
    disputes_state = [row for row in disputes_state if int(row["metadata"]["dispute_start_block"]) <= cutoff_height]
    enriched: list[dict[str, Any]] = []
    for row in disputes_state:
        dispute_id = row["disputeId"]
        copy = dict(row)
        copy["tally"] = client.api(f"/tellor-io/layer/dispute/tally/{dispute_id}")
        copy["vote_result"] = client.api(f"/tellor-io/layer/dispute/vote-result/{dispute_id}").get("vote_result")
        enriched.append(copy)

    tx_by_hash: dict[str, dict[str, Any]] = {}
    for action in ACTIONS:
        query = f"message.action='{action}' AND tx.height >= 1 AND tx.height <= {cutoff_height}"
        for tx in client.tx_search(query):
            tx_by_hash.setdefault(tx["hash"], normalized_tx(client, tx))
    transactions = sorted(tx_by_hash.values(), key=lambda row: (row["height"], row["tx_hash"]))
    write_gzip_json(raw_dir / "disputes_state.json.gz", enriched)
    write_gzip_json(raw_dir / "dispute_transactions.json.gz", transactions)

    proposal_by_id: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    votes: list[dict[str, Any]] = []
    payments: list[dict[str, Any]] = []
    actions_seen: Counter[str] = Counter()
    for tx in transactions:
        response = {"events": tx["events"]}
        new_dispute_events = event_attributes(response, "new_dispute")
        for message in tx["messages"]:
            action = message.get("@type")
            actions_seen[str(action)] += 1
            if action == "/layer.dispute.MsgProposeDispute" and new_dispute_events:
                proposal_by_id[new_dispute_events[0]["dispute_id"]] = (tx, message)
            elif action == "/layer.dispute.MsgVote":
                votes.append({
                    "dispute_id": str(message["id"]), "voter": message["voter"], "choice": message["vote"],
                    "source_tx": tx["tx_hash"], "source_block": tx["height"], "block_time": tx["timestamp"],
                    "event_attributes": (event_attributes(response, "voted_on_dispute") or [None])[0],
                })
            elif action in {"/layer.dispute.MsgClaimReward", "/layer.dispute.MsgWithdrawFeeRefund", "/layer.dispute.MsgAddFeeToDispute"}:
                recipient = message.get("caller_address") or message.get("payer_address") or message.get("creator")
                payments.append({
                    "event": action.rsplit(".", 1)[-1], "dispute_id": str(message.get("dispute_id") or message.get("id")),
                    "actor": recipient, "payer_address": message.get("payer_address"),
                    "declared_amount_raw": (message.get("amount") or {}).get("amount"),
                    "declared_asset": (message.get("amount") or {}).get("denom"),
                    "received_loya_raw": loya_received_by(response, str(recipient)) if recipient else None,
                    "source_tx": tx["tx_hash"], "source_block": tx["height"], "block_time": tx["timestamp"],
                })

    disputes: list[dict[str, Any]] = []
    for row in enriched:
        dispute_id = str(row["disputeId"]); metadata = row["metadata"]
        proposal = proposal_by_id.get(dispute_id)
        proposal_tx, proposal_message = proposal if proposal else (None, None)
        evidence = metadata["initial_evidence"]
        disputes.append({
            "dispute_id": dispute_id,
            "status": metadata["dispute_status"],
            "category": metadata["dispute_category"],
            "vote_result": row["vote_result"],
            "disputer": proposal_message.get("creator") if proposal_message else None,
            "reporter": evidence["reporter"],
            "query_id": evidence["query_id"],
            "query_type": evidence["query_type"],
            "report_meta_id": evidence["meta_id"],
            "report_value": evidence["value"],
            "report_timestamp_ms": evidence["timestamp"],
            "report_block": int(evidence["block_number"]),
            "report_power": evidence["power"],
            "dispute_fee_raw": metadata["dispute_fee"],
            "fee_total_raw": metadata["fee_total"],
            "slash_amount_raw": metadata["slash_amount"],
            "burn_amount_raw": metadata["burn_amount"],
            "voter_reward_pool_raw": metadata["voter_reward"],
            "asset": "loya",
            "asset_decimals": 6,
            "dispute_start_time": metadata["dispute_start_time"],
            "dispute_end_time": metadata["dispute_end_time"],
            "dispute_start_block": int(metadata["dispute_start_block"]),
            "dispute_round": int(metadata["dispute_round"]),
            "open": bool(metadata["open"]),
            "tally": row["tally"],
            "source_tx": proposal_tx["tx_hash"] if proposal_tx else None,
            "source_block": proposal_tx["height"] if proposal_tx else int(metadata["dispute_start_block"]),
            "truth_basis": "protocol_vote_adjudication",
            "confidence_grade": "A" if proposal else "B",
            "rule_id": "TELLOR_LAYER_DISPUTE_STATE_AND_TX_V1",
        })

    write_jsonl(curated_dir / "tellor_disputes.jsonl", disputes)
    write_jsonl(curated_dir / "tellor_dispute_votes.jsonl", votes)
    write_jsonl(curated_dir / "tellor_dispute_payments.jsonl", payments)

    integer_fields = ["dispute_fee_raw", "fee_total_raw", "slash_amount_raw", "burn_amount_raw", "voter_reward_pool_raw", "report_power"]
    malformed = sum(not str(row[field]).isdigit() for row in disputes for field in integer_fields)
    ids = [row["dispute_id"] for row in disputes]
    manifest = {
        "dataset": "Tellor Layer dispute accountability ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "cutoff_block_time": cutoff_block_time,
        "rpc_host": args.rpc_url.split("//", 1)[-1].split("/", 1)[0],
        "api_host": args.api_url.split("//", 1)[-1].split("/", 1)[0],
        "disputes": len(disputes),
        "disputes_by_result": dict(Counter(row["vote_result"] for row in disputes)),
        "votes": len(votes),
        "payments": len(payments),
        "messages_by_action": dict(actions_seen),
        "proposal_tx_links": len(proposal_by_id),
        "malformed_integer_fields": malformed,
        "duplicate_dispute_ids": len(ids) - len(set(ids)),
        "raw_state": str(raw_dir / "disputes_state.json.gz"),
        "raw_transactions": str(raw_dir / "dispute_transactions.json.gz"),
        "curated_disputes": str(curated_dir / "tellor_disputes.jsonl"),
        "curated_votes": str(curated_dir / "tellor_dispute_votes.jsonl"),
        "curated_payments": str(curated_dir / "tellor_dispute_payments.jsonl"),
        "all_required_assertions_pass": len(proposal_by_id) == len(disputes) and malformed == 0 and len(ids) == len(set(ids)),
        "scope_limit": "Complete dispute panel through cutoff; full report/tip/end-block reward history still requires a dedicated tellor-1 archive collection.",
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor dispute QC failed: {manifest}")
    manifest_path = ROOT / "data/manifests/tellor_layer_disputes.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    support_results = {"SUPPORT", "NO_QUORUM_MAJORITY_SUPPORT"}
    against_results = {"AGAINST", "NO_QUORUM_MAJORITY_AGAINST"}
    support_rows = [row for row in disputes if row["vote_result"] in support_results]
    against_rows = [row for row in disputes if row["vote_result"] in against_results]
    support_slash = sum(int(row["slash_amount_raw"]) for row in support_rows)
    against_fee = sum(int(row["dispute_fee_raw"]) for row in against_rows)
    voter_pool = sum(int(row["voter_reward_pool_raw"]) for row in disputes)
    voter_claimed = sum(int(row["received_loya_raw"] or 0) for row in payments if row["event"] == "MsgClaimReward")
    report = f"""# Tellor Layer dispute accountability QC

Generated: {manifest['generated_at_utc']}  
Chain: `tellor-1`  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff height: {cutoff_height} ({cutoff_block_time})

## Result

- Complete on-chain dispute state through cutoff: {len(disputes)} resolved disputes; proposal transaction links {len(proposal_by_id)}/{len(disputes)}.
- Vote messages: {len(votes)}; reward claims: {actions_seen['/layer.dispute.MsgClaimReward']}; settlement withdrawals: {actions_seen['/layer.dispute.MsgWithdrawFeeRefund']}.
- Support outcomes: {len(support_rows)}; designed reporter principal slash: {support_slash / 1_000_000:.6f} TRB.
- Against outcomes: {len(against_rows)}; designed unsuccessful-dispute fee exposure: {against_fee / 1_000_000:.6f} TRB.
- Voter reward pools: {voter_pool / 1_000_000:.6f} TRB; actually observed voter reward receipts through cutoff: {voter_claimed / 1_000_000:.6f} TRB.
- Malformed integer fields: {malformed}; duplicate dispute IDs: {manifest['duplicate_dispute_ids']}.

## Interpretation guards

- `SUPPORT` and `NO_QUORUM_MAJORITY_SUPPORT` are protocol vote adjudication, not external objective truth.
- `slash_amount_raw` and `voter_reward_pool_raw` are finalized protocol-state amounts. `MsgClaimReward.received_loya_raw` is an observed payment.
- `MsgWithdrawFeeRefund` receipts may combine returned fee/principal and dispute settlement gains, so they are retained as gross settlement receipts and are not labeled entirely as reward.
- No-stake reports are excluded from the staked-report accountability sample.
- This release completes the dispute-resolved strict-honesty subset. Full report, tip, selector-reward, and end-block reward history still requires a dedicated `tellor-1` archive collection.

Official evidence: Tellor dispute documentation and the `tellor-io/layer` source event definitions.
"""
    (ROOT / "reports/tellor_layer_dispute_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
