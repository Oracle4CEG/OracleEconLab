"""Build Chainlink LINK-flow and ETH/USD service-window evidence ledgers."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from .chainlink_evidence import ANSWER_UPDATED_TOPIC, LINK_TOKEN, NEW_TRANSMISSION_TOPIC, TRANSFER_TOPIC
from .chainlink_staking import CONTRACTS
from .rpc import write_json


def curated_dir(root: Path) -> Path:
    configured = os.environ.get("ORACLE_LEDGER_CURATED_DIR", "data/curated")
    path = Path(configured); return path if path.is_absolute() else root / path


def logs(folder: Path) -> Iterable[dict[str, Any]]:
    for path in sorted(folder.glob("*.jsonl.gz")):
        digest = path.with_suffix(path.suffix + ".sha256")
        if not digest.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest.read_text(encoding="utf-8").strip():
            raise RuntimeError(f"checksum mismatch: {path}")
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def signed(value: str) -> int:
    number = int(value, 16)
    return number - (1 << 256) if number >= (1 << 255) else number


def build(root: Path) -> Path:
    root = root.resolve(); curated = curated_dir(root)
    raw_manifest = json.loads((root / "data/manifests/chainlink_evidence_raw.json").read_text(encoding="utf-8"))
    phase_intervals = raw_manifest.get("eth_usd_feed", {}).get("phase_intervals", [])
    roles = {address.lower(): role for role, address in CONTRACTS.items()}
    flow_output = curated / "chainlink_link_flows.jsonl"; by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    flow_counts: Counter[str] = Counter(); seen: set[tuple[str, str]] = set()
    with flow_output.open("w", encoding="utf-8") as handle:
        for log in logs(root / "data/raw/ethereum/chainlink_link_flows"):
            key = (log["transactionHash"].lower(), log["logIndex"].lower())
            if key in seen or log["address"].lower() != LINK_TOKEN:
                continue
            seen.add(key); sender = "0x" + log["topics"][1][-40:].lower(); receiver = "0x" + log["topics"][2][-40:].lower()
            row = {
                "token": LINK_TOKEN, "sender": sender, "receiver": receiver,
                "sender_role": roles.get(sender), "receiver_role": roles.get(receiver),
                "amount_raw": str(int(log["data"], 16)), "source_tx": log["transactionHash"].lower(),
                "source_block": int(log["blockNumber"], 16), "log_index": int(log["logIndex"], 16),
            }
            direction = "internal_protocol" if row["sender_role"] and row["receiver_role"] else "outgoing" if row["sender_role"] else "incoming"
            row["direction"] = direction; flow_counts[direction] += 1; by_tx[row["source_tx"]].append(row)
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    event_qc_output = curated / "chainlink_event_link_flow_qc.jsonl"; qc_counts: Counter[str] = Counter(); route_counts: Counter[str] = Counter()
    with (curated / "chainlink_staking_v02_events.jsonl").open(encoding="utf-8") as source, event_qc_output.open("w", encoding="utf-8") as handle:
        for line in source:
            event = json.loads(line); kind = event["event"]
            if kind == "ForfeitedRewardDistributed":
                result = {
                    "event": kind,
                    "source_tx": event["source_tx"],
                    "vested_reward_raw": event["vested_reward_raw"],
                    "reclaimed_reward_raw": event["reclaimed_reward_raw"],
                    "operator_reward": event["operator_reward"],
                    "flow_expected": False,
                    "flow_exact": None,
                    "reconciliation_status": "accounting_only_no_transfer_required",
                }
                qc_counts["ForfeitedRewardDistributed_accounting_only"] += 1
                handle.write(json.dumps(result, separators=(",", ":")) + "\n")
                continue
            if kind not in {"Staked", "Unstaked", "RewardClaimed", "AlertingRewardPaid", "Slashed"}:
                continue
            amount_field = {"Staked": "amount_raw", "Unstaked": "amount_raw", "RewardClaimed": "reward_claimed_raw", "AlertingRewardPaid": "alert_reward_actual_raw", "Slashed": "principal_slashed_raw"}[kind]
            expected = int(event[amount_field]); actor = event.get("staker") or event.get("alerter") or event.get("operator")
            candidates = by_tx.get(event["source_tx"], [])
            if kind == "Staked":
                direct = [row for row in candidates if row["sender"] == actor and row["receiver"] == event["source_contract"]]
                mediated = [row for row in candidates if row["receiver"] == event["source_contract"] and int(row["amount_raw"]) == expected]
                if sum(int(row["amount_raw"]) for row in direct) == expected:
                    matched = direct; route = "direct_staker_to_pool"
                elif mediated:
                    matched = mediated[:1]; route = "contract_mediated_to_pool"
                else:
                    matched = [row for row in candidates if row["receiver"] == event["source_contract"]]; route = "unresolved_incoming"
            elif kind in {"Unstaked", "RewardClaimed", "AlertingRewardPaid"}:
                sourced = [row for row in candidates if row["sender"] == event["source_contract"] and row["receiver"] == actor]
                exact_sourced = [row for row in sourced if int(row["amount_raw"]) == expected]
                matched = exact_sourced[:1] if exact_sourced else sourced
                route = "source_contract_to_actor"
            else:
                matched = [row for row in candidates if row["sender"] == event["source_contract"] or row["receiver"] == event["source_contract"]]
                route = "slash_contract_flow"
            observed = sum(int(row["amount_raw"]) for row in matched)
            exact = observed == expected
            result = {
                "event": kind, "source_tx": event["source_tx"], "actor": actor,
                "expected_amount_raw": str(expected), "observed_link_flow_raw": str(observed),
                "flow_expected": True, "flow_exact": exact, "flow_route": route,
                "matched_transfer_count": len(matched),
                "matched_senders": sorted({row["sender"] for row in matched}),
                "matched_receivers": sorted({row["receiver"] for row in matched}),
            }
            qc_counts[f"{kind}_exact" if exact else f"{kind}_mismatch"] += 1
            route_counts[f"{kind}:{route}"] += 1
            handle.write(json.dumps(result, separators=(",", ":")) + "\n")

    report_output = curated / "chainlink_eth_usd_reports.jsonl"; reports: list[dict[str, Any]] = []; report_counts: Counter[str] = Counter()
    excluded_outside_active_phase = 0
    with report_output.open("w", encoding="utf-8") as handle:
        for log in logs(root / "data/raw/ethereum/chainlink_eth_usd"):
            if not log.get("topics"):
                continue
            topic = log["topics"][0].lower(); row = None
            block = int(log["blockNumber"], 16); address = log["address"].lower()
            if topic in {ANSWER_UPDATED_TOPIC, NEW_TRANSMISSION_TOPIC} and phase_intervals:
                active = any(
                    address == interval["aggregator"]
                    and int(interval["valid_from_block"]) <= block <= int(interval["valid_to_block"])
                    for interval in phase_intervals
                )
                if not active:
                    excluded_outside_active_phase += 1
                    continue
            if topic == ANSWER_UPDATED_TOPIC and len(log["topics"]) >= 3:
                row = {
                    "event": "AnswerUpdated", "aggregator": address,
                    "answer_raw": str(signed(log["topics"][1])), "round_id": str(int(log["topics"][2], 16)),
                    "updated_at": str(int(log["data"], 16)),
                }
            elif topic == NEW_TRANSMISSION_TOPIC:
                row = {"event": "NewTransmission", "aggregator": address, "raw_topics": log["topics"], "raw_data": log["data"]}
            if row is None:
                continue
            row.update(source_tx=log["transactionHash"].lower(), source_block=int(log["blockNumber"], 16), log_index=int(log["logIndex"], 16))
            reports.append(row); report_counts[row["event"]] += 1; handle.write(json.dumps(row, separators=(",", ":")) + "\n")
    answer_times = sorted({int(row["updated_at"]) for row in reports if row["event"] == "AnswerUpdated" and int(row["updated_at"]) > 0})
    gaps = [right - left for left, right in zip(answer_times, answer_times[1:])]
    manifest = {
        "protocol": "Chainlink Staking v0.2 LINK and ETH/USD evidence",
        "link_flows": sum(flow_counts.values()), "link_flows_by_direction": dict(flow_counts),
        "event_link_flow_qc": dict(qc_counts), "event_link_flow_routes": dict(route_counts), "feed_events": dict(report_counts),
        "feed_phase_intervals": phase_intervals, "feed_events_excluded_outside_active_phase": excluded_outside_active_phase,
        "answer_updated_unique_times": len(answer_times), "max_answer_update_gap_seconds": max(gaps) if gaps else None,
        "answer_update_gaps_over_3h": sum(gap > 10800 for gap in gaps),
        "outputs": {"flows": str(flow_output), "event_qc": str(event_qc_output), "feed_reports": str(report_output)},
        "interpretation_guard": "Accepted feed update timing is necessary service evidence; alert validity still requires the contemporaneous controller configuration and alert transaction.",
    }
    output = root / "data/manifests/chainlink_evidence_ledger.json"; write_json(output, manifest); return output
