"""Build Ethereum-only accountability ledgers from already preserved raw logs."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from eth_utils import keccak

from .chainlink_staking import CONTRACTS as CHAINLINK_CONTRACTS, TOPIC_TO_SIGNATURE as CHAINLINK_TOPICS
from .ethereum_audit import TOPIC_TO_SIGNATURE as UMA_TOPICS
from .rpc import write_json


def _curated_dir(root: Path) -> Path:
    """Keep large derived ledgers on the designated high-capacity volume."""
    configured = os.environ.get("ORACLE_LEDGER_CURATED_DIR", "data/curated")
    path = Path(configured)
    if not path.is_absolute():
        path = root / path
    path.mkdir(parents=True, exist_ok=True)
    return path


def _files(folder: Path) -> list[Path]:
    def start(path: Path) -> int:
        match = re.search(r"_(\d+)_\d+\.jsonl\.gz$", path.name)
        return int(match.group(1)) if match else -1
    return sorted(folder.glob("*.jsonl.gz"), key=start)


def _logs(folder: Path) -> Iterable[dict[str, Any]]:
    for path in _files(folder):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def _words(data: str) -> list[int]:
    body = data[2:]
    return [int(body[index : index + 64], 16) for index in range(0, len(body), 64)]


def _signed(word: int) -> int:
    return word - (1 << 256) if word >= (1 << 255) else word


def _address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def _base(log: dict[str, Any], event: str) -> dict[str, Any]:
    return {
        "event": event,
        "source_contract": log["address"].lower(),
        "source_tx": log["transactionHash"].lower(),
        "source_block": int(log["blockNumber"], 16),
        "transaction_index": int(log["transactionIndex"], 16),
        "log_index": int(log["logIndex"], 16),
    }


def build_chainlink(root: Path) -> Path:
    """Emit v0.2 staking, reward, alert and slash rows with integer-string amounts."""
    roles = {address.lower(): role for role, address in CHAINLINK_CONTRACTS.items()}
    output = _curated_dir(root) / "chainlink_staking_v02_events.jsonl"
    temporary = output.with_suffix(output.suffix + ".tmp")
    counts: Counter[str] = Counter()
    with temporary.open("w", encoding="utf-8") as handle:
        for log in _logs(root / "data/raw/ethereum/chainlink_staking_v02"):
            # Preserve the earlier router probe as raw evidence, but exclude it
            # from the formal v0.2 ledger after version-boundary verification.
            if log["address"].lower() not in roles:
                continue
            signature = CHAINLINK_TOPICS.get(log["topics"][0].lower())
            if signature is None:
                continue
            event = signature.split("(")[0]
            row = _base(log, event)
            row["contract_role"] = roles[log["address"].lower()]
            words = _words(log["data"])
            topics = log["topics"]
            if event in {"Staked", "Unstaked"}:
                row.update(staker=_address(topics[1]), amount_raw=str(words[0]), principal_after_raw=str(words[1]))
                if len(words) > 2:
                    row["additional_amount_raw"] = str(words[2])
            elif event == "Slashed":
                row.update(operator=_address(topics[1]), principal_slashed_raw=str(words[0]), principal_after_raw=str(words[1]), total_principal_raw=str(words[2]), penalty_class="principal_slash")
            elif event == "RewardClaimed":
                row.update(staker=_address(topics[1]), reward_claimed_raw=str(words[0]), reward_class="base_or_delegation_staking_reward")
            elif event == "RewardFinalized":
                row.update(staker=_address(topics[1]), reward_forfeited=bool(words[0]), penalty_class="reward_forfeiture" if words[0] else None)
            elif event == "StakerRewardUpdated":
                row.update(staker=_address(topics[1]), vested_base_reward_raw=str(words[0]), vested_delegated_reward_raw=str(words[1]), base_reward_per_token_raw=str(words[2]), delegated_reward_per_token_raw=str(words[3]), staker_principal_raw=str(words[4]))
            elif event == "ForfeitedRewardDistributed":
                row.update(vested_reward_raw=str(words[0]), vested_reward_per_token_raw=str(words[1]), reclaimed_reward_raw=str(words[2]), operator_reward=bool(words[3]), penalty_class="reward_forfeiture")
            elif event == "AlertingRewardPaid":
                row.update(alerter=_address(topics[1]), alert_reward_actual_raw=str(words[0]), alert_reward_expected_raw=str(words[1]), reward_class="alert_reward")
            elif event == "AlertRaised":
                row.update(alerter="0x" + words[0].to_bytes(32, "big")[-20:].hex(), round_id=str(words[1]), alert_reward_raw=str(words[2]))
            elif event == "FeedConfigSet":
                row.update(
                    feed=_address(topics[1]), threshold_1_seconds=str(words[0]), threshold_2_seconds=str(words[1]),
                    operator_slash_amount_raw=str(words[2]), alerter_reward_amount_raw=str(words[3]),
                    configuration_version=f"{row['source_block']}:{row['log_index']}",
                    interpretation_note="threshold field names remain neutral until the verified controller source is archived",
                )
            elif event == "RewardAdded":
                row.update(pool=_address(topics[1]), reward_amount_raw=str(words[0]), emission_rate_raw=str(words[1]))
            elif event in {"CommunityPoolRewardUpdated", "OperatorPoolRewardUpdated"}:
                row.update(reward_per_token_raw=[str(word) for word in words])
            else:
                row["raw_data_words"] = [str(word) for word in words]
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
            counts[event] += 1
    temporary.replace(output)
    manifest = {"protocol": "Chainlink Staking v0.2", "rows": sum(counts.values()), "by_event": dict(sorted(counts.items())), "source": str(output), "amount_policy": "integer strings only"}
    manifest_path = root / "data/manifests/chainlink_staking_v02_ledger.json"
    write_json(manifest_path, manifest)
    return manifest_path


def _dynamic_bytes(data: str, offset_word: int) -> str:
    raw = bytes.fromhex(data[2:])
    length = int.from_bytes(raw[offset_word : offset_word + 32], "big")
    return "0x" + raw[offset_word + 32 : offset_word + 32 + length].hex()


def _request_id(identifier_topic: str, timestamp: int, ancillary_hex: str) -> str:
    ancillary = bytes.fromhex(ancillary_hex[2:])
    encoded = bytes.fromhex(identifier_topic[2:]) + timestamp.to_bytes(32, "big") + (96).to_bytes(32, "big") + len(ancillary).to_bytes(32, "big") + ancillary + b"\0" * ((32 - len(ancillary) % 32) % 32)
    return "0x" + keccak(encoded).hex()


def build_uma(root: Path) -> Path:
    """Emit DVM-only requests, vote events, signed slashes, and staking events."""
    curated = _curated_dir(root)
    files = {
        "votes": (curated / "uma_dvm_vote_events.jsonl").with_suffix(".jsonl.tmp"),
        "payoffs": (curated / "uma_dvm_voter_payoffs.jsonl").with_suffix(".jsonl.tmp"),
        "staking": (curated / "uma_dvm_staking_events.jsonl").with_suffix(".jsonl.tmp"),
    }
    handles = {name: path.open("w", encoding="utf-8") for name, path in files.items()}
    requests: dict[str, dict[str, Any]] = {}
    resolved_index: dict[int, str] = {}
    counts: Counter[str] = Counter()
    try:
        for log in _logs(root / "data/raw/ethereum/logs"):
            signature = UMA_TOPICS.get(log["topics"][0].lower())
            if signature is None:
                continue
            event = signature.split("(")[0]
            topics, words = log["topics"], _words(log["data"])
            if event == "RequestAdded":
                ancillary = _dynamic_bytes(log["data"], words[1])
                request_id = _request_id(topics[3], words[0], ancillary)
                requests[request_id] = _base(log, event) | {"dvm_request_id": request_id, "requester": _address(topics[1]), "round_id": str(int(topics[2], 16)), "identifier": topics[3].lower(), "request_time": str(words[0]), "ancillary_data_hex": ancillary, "is_governance": bool(words[2]), "status": "added", "cross_chain_match_grade": "U"}
            elif event == "RequestResolved":
                ancillary = _dynamic_bytes(log["data"], words[1])
                request_id = _request_id(topics[3], words[0], ancillary)
                row = requests.setdefault(request_id, {"dvm_request_id": request_id, "identifier": topics[3].lower(), "request_time": str(words[0]), "ancillary_data_hex": ancillary, "cross_chain_match_grade": "U"})
                row.update(_base(log, event), round_id=str(int(topics[1], 16)), request_index=str(int(topics[2], 16)), resolved_price_raw=str(_signed(words[2])), status="resolved")
                resolved_index[int(topics[2], 16)] = request_id
            elif event in {"VoteCommitted", "VoteRevealed"}:
                ancillary = _dynamic_bytes(log["data"], words[2])
                request_id = _request_id(topics[3], words[1], ancillary)
                row = _base(log, event) | {"dvm_request_id": request_id, "voter": _address(topics[1]), "caller_or_delegate": _address(topics[2]), "round_id": str(words[0]), "identifier": topics[3].lower(), "request_time": str(words[1]), "ancillary_data_hex": ancillary, "committed": event == "VoteCommitted", "revealed": event == "VoteRevealed"}
                if event == "VoteRevealed": row.update(revealed_price_raw=str(_signed(words[3])), tokens_at_reveal_raw=str(words[4]))
                handles["votes"].write(json.dumps(row, separators=(",", ":")) + "\n")
            elif event == "VoterSlashed":
                delta = _signed(words[0]); request_index = int(topics[2], 16)
                row = _base(log, event) | {"dvm_request_id": resolved_index.get(request_index), "request_index": str(request_index), "voter": _address(topics[1]), "signed_slash_delta_raw": str(delta), "correct_vote_redistribution_raw": str(delta) if delta > 0 else "0", "wrong_or_no_vote_slash_raw": str(-delta) if delta < 0 else "0", "classification_rule_id": "DVM_CORRECT_VOTE_REDISTRIBUTION" if delta > 0 else "DVM_NEGATIVE_SLASH" if delta < 0 else "DVM_ZERO_SLASH", "confidence_grade": "A" if request_index in resolved_index else "U"}
                handles["payoffs"].write(json.dumps(row, separators=(",", ":")) + "\n")
            elif event in {"Staked", "RequestedUnstake", "ExecutedUnstake", "UpdatedReward", "WithdrawnRewards", "SetNewEmissionRate", "SetNewUnstakeCoolDown", "VoterSlashApplied"}:
                row = _base(log, event) | {"raw_data_words": [str(_signed(word) if event == "VoterSlashApplied" and index == 0 else word) for index, word in enumerate(words)]}
                if len(topics) > 1: row["actor"] = _address(topics[1])
                if event == "VoterSlashApplied": row["classification"] = "stake_balance_reconciliation_only"
                elif event in {"UpdatedReward", "WithdrawnRewards"}: row["classification"] = "base_staking_emission_not_honesty_reward"
                handles["staking"].write(json.dumps(row, separators=(",", ":")) + "\n")
            counts[event] += 1
    finally:
        for handle in handles.values(): handle.close()
    requests_output = curated / "uma_dvm_requests.jsonl"
    with requests_output.with_suffix(".jsonl.tmp").open("w", encoding="utf-8") as handle:
        for row in sorted(requests.values(), key=lambda value: (value.get("source_block", 0), value["dvm_request_id"])):
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
    requests_output.with_suffix(".jsonl.tmp").replace(requests_output)
    for name, temporary in files.items(): temporary.replace(curated / f"uma_dvm_{'voter_payoffs' if name == 'payoffs' else name + '_events'}.jsonl")
    manifest = {"protocol": "UMA VotingV2", "requests": len(requests), "events_seen": dict(sorted(counts.items())), "cross_chain_match_policy": "all requests remain U until Polygon evidence is present", "amount_policy": "integer strings only"}
    manifest_path = root / "data/manifests/uma_dvm_ledger.json"
    write_json(manifest_path, manifest)
    return manifest_path
