"""Decode Polygon UMA evidence and build Polymarket request-round lifecycles."""
from __future__ import annotations

import gzip
import json
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from eth_abi import decode
from eth_utils import keccak

from .polygon_uma import CONTRACTS, HISTORICAL_CHILD_TUNNELS, TOPIC_TO_SIGNATURE
from .rpc import write_json


ZERO_ADDRESS = "0x" + "00" * 20
ADAPTER_ROLES = {role for role in CONTRACTS if role.startswith("adapter_")}
PRIMARY_ADAPTER_ROLES = {"adapter_v2_0", "adapter_v3_current"}
PRIMARY_START_TIMESTAMP = 1680307200  # 2023-04-01T00:00:00Z
ROLE_BY_ADDRESS = {address.lower(): role for role, address in (CONTRACTS | HISTORICAL_CHILD_TUNNELS).items()}


def curated_dir(root: Path) -> Path:
    configured = os.environ.get("ORACLE_LEDGER_CURATED_DIR", "data/curated")
    path = Path(configured)
    if not path.is_absolute():
        path = root / path
    path.mkdir(parents=True, exist_ok=True)
    return path


def raw_logs(folder: Path) -> Iterable[dict[str, Any]]:
    def start(path: Path) -> int:
        match = re.search(r"_(\d+)_\d+\.jsonl\.gz$", path.name)
        return int(match.group(1)) if match else -1
    for path in sorted(folder.glob("polygon_uma_*.jsonl.gz"), key=start):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def bytes32(value: bytes | str) -> str:
    return value.lower() if isinstance(value, str) else "0x" + value.hex()


def bytes_hex(value: bytes) -> str:
    return "0x" + value.hex()


def signed_topic(topic: str) -> int:
    value = int(topic, 16)
    return value - (1 << 256) if value >= (1 << 255) else value


def base(log: dict[str, Any], event: str) -> dict[str, Any]:
    row = {
        "event": event,
        "source_contract": log["address"].lower(),
        "contract_role": ROLE_BY_ADDRESS[log["address"].lower()],
        "source_tx": log["transactionHash"].lower(),
        "source_block": int(log["blockNumber"], 16),
        "transaction_index": int(log["transactionIndex"], 16),
        "log_index": int(log["logIndex"], 16),
    }
    if log.get("blockTimestamp") is not None:
        row["block_time"] = int(log["blockTimestamp"], 16)
    return row


def request_id(requester: str, identifier: str, timestamp: int, ancillary: bytes) -> str:
    # OptimisticOracleV2._getId uses abi.encodePacked(address,bytes32,uint256,bytes).
    payload = bytes.fromhex(requester[2:]) + bytes.fromhex(identifier[2:]) + timestamp.to_bytes(32, "big") + ancillary
    return "0x" + keccak(payload).hex()


def decode_log(log: dict[str, Any]) -> dict[str, Any] | None:
    signature = TOPIC_TO_SIGNATURE.get(log["topics"][0].lower())
    if signature is None:
        return None
    event = signature.split("(", 1)[0]
    topics = log["topics"]
    data = bytes.fromhex(log["data"][2:])
    row = base(log, event)
    role = row["contract_role"]
    legacy_adapter = role in {"adapter_v1_0", "adapter_v1_0_1"}
    if event == "QuestionInitialized" and not legacy_adapter:
        ancillary, reward_token, reward, proposal_bond = decode(["bytes", "address", "uint256", "uint256"], data)
        row.update(
            question_id=topics[1].lower(), request_timestamp=str(int(topics[2], 16)), creator=address(topics[3]),
            ancillary_data_hex=bytes_hex(ancillary), reward_token=reward_token.lower(), reward_raw=str(reward),
            proposal_bond_raw=str(proposal_bond),
        )
    elif event in {"QuestionInitialized", "QuestionUpdated"} and legacy_adapter:
        ancillary, resolution_time, reward_token, reward, proposal_bond, early = decode(
            ["bytes", "uint256", "address", "uint256", "uint256", "bool"], data
        )
        row.update(
            question_id=topics[1].lower(), ancillary_data_hex=bytes_hex(ancillary),
            resolution_time=str(resolution_time), reward_token=reward_token.lower(), reward_raw=str(reward),
            proposal_bond_raw=str(proposal_bond), early_resolution_enabled=early,
        )
    elif event in {"QuestionPaused", "QuestionUnpaused"}:
        if legacy_adapter:
            (question_id,) = decode(["bytes32"], data); row["question_id"] = bytes32(question_id)
        else:
            row["question_id"] = topics[1].lower()
    elif event in {"QuestionFlagged", "QuestionUnflagged", "QuestionReset"}:
        row["question_id"] = topics[1].lower()
    elif event == "QuestionFlaggedForAdminResolution":
        (question_id,) = decode(["bytes32"], data); row["question_id"] = bytes32(question_id)
    elif event == "QuestionResolved" and not legacy_adapter:
        (payouts,) = decode(["uint256[]"], data)
        row.update(question_id=topics[1].lower(), settled_price_raw=str(signed_topic(topics[2])), payouts_raw=[str(x) for x in payouts])
    elif event == "QuestionResolved" and legacy_adapter:
        row.update(question_id=topics[1].lower(), emergency_report=bool(int(topics[2], 16)))
    elif event in {"QuestionManuallyResolved", "QuestionEmergencyResolved"}:
        (payouts,) = decode(["uint256[]"], data)
        row.update(question_id=topics[1].lower(), payouts_raw=[str(x) for x in payouts])
    elif event == "ResolutionDataRequested":
        identifier, ancillary, reward_token, reward, proposal_bond, early = decode(
            ["bytes32", "bytes", "address", "uint256", "uint256", "bool"], data
        )
        requester = row["source_contract"]; timestamp = int(topics[2], 16); identifier_hex = bytes32(identifier)
        row.update(
            requestor=address(topics[1]), request_timestamp=str(timestamp), question_id=topics[3].lower(),
            identifier=identifier_hex, ancillary_data_hex=bytes_hex(ancillary), reward_token=reward_token.lower(),
            reward_raw=str(reward), proposal_bond_raw=str(proposal_bond), early_resolution=early,
            oo_request_id=request_id(requester, identifier_hex, timestamp, ancillary),
        )
    elif event == "QuestionSettled":
        row.update(
            question_id=topics[1].lower(), settled_price_raw=str(signed_topic(topics[2])),
            early_resolution=bool(int(topics[3], 16)),
        )
    elif event == "NewFinderAddress":
        old_finder, new_finder = decode(["address", "address"], data)
        row.update(old_finder=old_finder.lower(), new_finder=new_finder.lower())
    elif event in {"AuthorizedUser", "DeauthorizedUser", "UnauthorizedUser"}:
        if len(topics) > 1:
            row["user"] = address(topics[1])
        else:
            (user,) = decode(["address"], data); row["user"] = user.lower()
    elif event == "AncillaryDataUpdated":
        (update,) = decode(["bytes"], data)
        row.update(question_id=topics[1].lower(), owner=address(topics[2]), update_hex=bytes_hex(update))
    elif event == "RequestPrice":
        identifier, timestamp, ancillary, currency, reward, final_fee = decode(
            ["bytes32", "uint256", "bytes", "address", "uint256", "uint256"], data
        )
        requester = address(topics[1]); identifier_hex = bytes32(identifier)
        row.update(
            oo_request_id=request_id(requester, identifier_hex, timestamp, ancillary), requester=requester,
            identifier=identifier_hex, request_time=str(timestamp), ancillary_data_hex=bytes_hex(ancillary),
            currency=currency.lower(), reward_raw=str(reward), final_fee_raw=str(final_fee),
        )
    elif event == "ProposePrice":
        identifier, timestamp, ancillary, price, expiration, currency = decode(
            ["bytes32", "uint256", "bytes", "int256", "uint256", "address"], data
        )
        requester = address(topics[1]); identifier_hex = bytes32(identifier)
        row.update(
            oo_request_id=request_id(requester, identifier_hex, timestamp, ancillary), requester=requester,
            proposer=address(topics[2]), identifier=identifier_hex, request_time=str(timestamp),
            ancillary_data_hex=bytes_hex(ancillary), proposed_price_raw=str(price),
            expiration_time=str(expiration), currency=currency.lower(),
        )
    elif event == "DisputePrice":
        identifier, timestamp, ancillary, price = decode(["bytes32", "uint256", "bytes", "int256"], data)
        requester = address(topics[1]); identifier_hex = bytes32(identifier)
        row.update(
            oo_request_id=request_id(requester, identifier_hex, timestamp, ancillary), requester=requester,
            proposer=address(topics[2]), disputer=address(topics[3]), identifier=identifier_hex,
            request_time=str(timestamp), ancillary_data_hex=bytes_hex(ancillary), proposed_price_raw=str(price),
        )
    elif event == "Settle":
        identifier, timestamp, ancillary, price, payout = decode(["bytes32", "uint256", "bytes", "int256", "uint256"], data)
        requester = address(topics[1]); identifier_hex = bytes32(identifier)
        row.update(
            oo_request_id=request_id(requester, identifier_hex, timestamp, ancillary), requester=requester,
            proposer=address(topics[2]), disputer=address(topics[3]), identifier=identifier_hex,
            request_time=str(timestamp), ancillary_data_hex=bytes_hex(ancillary), resolved_price_raw=str(price),
            gross_payout_raw=str(payout),
        )
    elif event == "PriceRequestAdded":
        timestamp, ancillary = decode(["uint256", "bytes"], data)
        row.update(
            identifier=topics[1].lower(), request_hash=topics[2].lower(), dvm_time=str(timestamp),
            stamped_ancillary_data_hex=bytes_hex(ancillary),
        )
    elif event == "PushedPrice":
        timestamp, ancillary, price = decode(["uint256", "bytes", "int256"], data)
        row.update(
            identifier=topics[1].lower(), request_hash=topics[2].lower(), dvm_time=str(timestamp),
            stamped_ancillary_data_hex=bytes_hex(ancillary), resolved_price_raw=str(price),
        )
    elif event == "PriceRequestBridged":
        identifier, timestamp, ancillary = decode(["bytes32", "uint256", "bytes"], data)
        row.update(
            requester=address(topics[1]), identifier=bytes32(identifier), dvm_time=str(timestamp),
            child_ancillary_data_hex=bytes_hex(ancillary), child_request_hash=topics[2].lower(),
            parent_request_hash=topics[3].lower(),
        )
    elif event == "ResolvedLegacyRequest":
        timestamp, ancillary, price = decode(["uint256", "bytes", "int256"], data)
        row.update(
            identifier=topics[1].lower(), dvm_time=str(timestamp), child_ancillary_data_hex=bytes_hex(ancillary),
            resolved_price_raw=str(price), request_hash=topics[2].lower(), legacy_request_hash=topics[3].lower(),
        )
    elif event == "MessageSent":
        (message,) = decode(["bytes"], data)
        row.update(message_hex=bytes_hex(message), message_hash="0x" + keccak(message).hex())
    return row


def economic_fields(round_row: dict[str, Any]) -> dict[str, Any]:
    if round_row.get("gross_payout_raw") is None:
        return {"economic_status": "not_settled"}
    if round_row.get("question_link_grade") != "A" or round_row.get("proposal_bond_raw") is None:
        return {"economic_status": "insufficient_adapter_parameters"}
    reward = int(round_row["reward_raw"])
    final_fee = int(round_row["final_fee_raw"])
    configured_bond = int(round_row.get("proposal_bond_raw") or 0)
    bond = configured_bond if configured_bond > 0 else final_fee
    payout = int(round_row["gross_payout_raw"])
    disputer = round_row.get("disputer", ZERO_ADDRESS)
    result: dict[str, Any] = {
        "effective_bond_raw": str(bond), "principal_returned_raw": str(bond + final_fee),
    }
    if disputer == ZERO_ADDRESS:
        expected = bond + final_fee + reward
        result.update(
            rule_id="OO_UNDISPUTED_PROPOSER_REWARD", economic_status="settled_undisputed",
            explicit_report_reward_raw=str(reward), reward_refunded_or_rolled_raw="0",
            reward_remaining_at_settlement_raw=str(reward),
            dispute_winner_reward_raw="0", bond_forfeited_raw="0", final_fee_forfeited_raw="0",
            protocol_fee_raw="0", expected_gross_payout_raw=str(expected), payout_qc_gap_raw=str(payout - expected),
        )
    else:
        # v2+ calls setEventBased(), which automatically enables
        # refundOnDispute. Legacy v1 adapters do not.
        refund_on_dispute = round_row.get("adapter_version") not in {"adapter_v1_0", "adapter_v1_0_1"}
        reward_at_settlement = 0 if refund_on_dispute else reward
        proposer_wins = round_row.get("resolved_price_raw") == round_row.get("proposed_price_raw")
        winner_reward = bond - bond // 2
        expected = bond + final_fee + winner_reward + reward_at_settlement
        result.update(
            rule_id="OO_DISPUTED_PROPOSER_WINS" if proposer_wins else "OO_DISPUTED_DISPUTER_WINS",
            economic_status="settled_disputed_proposer_wins" if proposer_wins else "settled_disputed_disputer_wins",
            explicit_report_reward_raw=str(reward_at_settlement),
            reward_refunded_or_rolled_raw=str(reward if refund_on_dispute else 0),
            reward_remaining_at_settlement_raw=str(reward_at_settlement),
            refund_on_dispute=refund_on_dispute,
            dispute_winner_reward_raw=str(winner_reward), bond_forfeited_raw=str(bond),
            final_fee_forfeited_raw=str(final_fee), protocol_fee_raw=str(bond // 2 + final_fee),
            expected_gross_payout_raw=str(expected), payout_qc_gap_raw=str(payout - expected),
        )
    return result


def build(root: Path) -> Path:
    root = root.resolve(); curated = curated_dir(root)
    outputs = {
        "adapter": curated / "polygon_adapter_events.jsonl",
        "oov2": curated / "polygon_oov2_events.jsonl",
        "child": curated / "polygon_child_tunnel_events.jsonl",
    }
    temps = {name: path.with_suffix(path.suffix + ".tmp") for name, path in outputs.items()}
    handles = {name: path.open("w", encoding="utf-8") for name, path in temps.items()}
    counts: Counter[str] = Counter(); decode_errors: Counter[str] = Counter()
    adapter_questions: dict[str, dict[str, Any]] = {}
    adapter_request_links: dict[str, dict[str, Any]] = {}
    oov2_events: list[dict[str, Any]] = []
    source_keys: set[tuple[str, int]] = set(); duplicates = 0
    try:
        raw_folders = [root / "data/raw/polygon/uma", root / "data/raw/polygon/uma_legacy_child_tunnel"]
        for log in (item for folder in raw_folders if folder.is_dir() for item in raw_logs(folder)):
            key = (log["transactionHash"].lower(), int(log["logIndex"], 16))
            if key in source_keys:
                duplicates += 1; continue
            source_keys.add(key)
            try:
                row = decode_log(log)
            except Exception:
                topic = log["topics"][0].lower(); decode_errors[topic] += 1; continue
            if row is None:
                continue
            role = row["contract_role"]
            group = "adapter" if role in ADAPTER_ROLES else "oov2" if role == "optimistic_oracle_v2" else "child"
            handles[group].write(json.dumps(row, separators=(",", ":")) + "\n")
            counts[row["event"]] += 1
            if row["event"] == "QuestionInitialized":
                adapter_questions[row["question_id"]] = row
            elif row["event"] == "QuestionUpdated":
                adapter_questions[row["question_id"]] = row
            elif row["event"] == "ResolutionDataRequested":
                adapter_request_links[row["oo_request_id"]] = row
            if group == "oov2":
                oov2_events.append(row)
    finally:
        for handle in handles.values(): handle.close()
    for name, temp in temps.items(): temp.replace(outputs[name])

    rounds: dict[str, dict[str, Any]] = {}
    adapter_addresses = {CONTRACTS[role].lower() for role in ADAPTER_ROLES}
    for event in oov2_events:
        if event["event"] == "RequestPrice" and event["requester"] in adapter_addresses:
            rounds[event["oo_request_id"]] = dict(event)
            rounds[event["oo_request_id"]]["status"] = "requested"
            rounds[event["oo_request_id"]]["requester_is_polymarket_adapter"] = True
    for event in oov2_events:
        request = rounds.get(event.get("oo_request_id"))
        if request is None or event["event"] == "RequestPrice":
            continue
        if event["event"] == "ProposePrice":
            request.update(
                proposer=event["proposer"], proposed_price_raw=event["proposed_price_raw"],
                expiration_time=event["expiration_time"], proposal_tx=event["source_tx"],
                proposal_block=event["source_block"], status="proposed",
            )
        elif event["event"] == "DisputePrice":
            request.update(proposer=event["proposer"], disputer=event["disputer"], proposed_price_raw=event["proposed_price_raw"], dispute_tx=event["source_tx"], status="disputed")
        elif event["event"] == "Settle":
            request.update(
                proposer=event["proposer"], disputer=event["disputer"], resolved_price_raw=event["resolved_price_raw"],
                gross_payout_raw=event["gross_payout_raw"], settlement_tx=event["source_tx"], settlement_block=event["source_block"], status="settled",
            )

    round_output = curated / "polygon_uma_request_rounds.jsonl"
    round_temp = round_output.with_suffix(round_output.suffix + ".tmp")
    round_counts: Counter[str] = Counter(); sample_counts: Counter[str] = Counter(); exact_question_links = payout_qc_failures = 0
    with round_temp.open("w", encoding="utf-8") as handle:
        for row in sorted(rounds.values(), key=lambda item: (item["source_block"], item["oo_request_id"])):
            ancillary = bytes.fromhex(row["ancillary_data_hex"][2:])
            candidate_question = "0x" + keccak(ancillary).hex()
            question = adapter_questions.get(candidate_question)
            if question is None:
                question = adapter_request_links.get(row["oo_request_id"])
                candidate_question = question["question_id"] if question is not None else candidate_question
            if question is not None and question["source_contract"] == row["requester"]:
                exact_question_links += 1
                row.update(
                    question_id=candidate_question, adapter_version=question["contract_role"],
                    proposal_bond_raw=question["proposal_bond_raw"], question_reward_raw=question["reward_raw"],
                    question_link_grade="A",
                )
            else:
                row["question_link_grade"] = "U"
            if row["question_link_grade"] != "A":
                row["sample_tier"] = "unresolved"
                row["sample_tier_reason"] = "adapter_question_link_not_exact"
            elif row.get("adapter_version") in PRIMARY_ADAPTER_ROLES and int(row["request_time"]) >= PRIMARY_START_TIMESTAMP:
                row["sample_tier"] = "primary"
                row["sample_tier_reason"] = "primary_adapter_and_main_analysis_window"
            else:
                row["sample_tier"] = "supplementary"
                row["sample_tier_reason"] = "legacy_or_historical_adapter_or_pre_main_window"
            row.update(economic_fields(row))
            if row.get("payout_qc_gap_raw") not in {None, "0"}:
                payout_qc_failures += 1
            round_counts[row["status"]] += 1
            sample_counts[row["sample_tier"]] += 1
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
    round_temp.replace(round_output)
    manifest = {
        "protocol": "Polygon Polymarket UMA / OOV2",
        "decoded_events": sum(counts.values()), "by_event": dict(sorted(counts.items())),
        "decode_errors_by_topic": dict(sorted(decode_errors.items())), "duplicate_source_logs": duplicates,
        "request_rounds": len(rounds), "request_rounds_by_status": dict(sorted(round_counts.items())),
        "request_rounds_by_sample_tier": dict(sorted(sample_counts.items())),
        "grade_a_adapter_question_links": exact_question_links, "payout_qc_nonzero_gaps": payout_qc_failures,
        "outputs": {name: str(path) for name, path in outputs.items()} | {"request_rounds": str(round_output)},
        "amount_policy": "integer strings only; Settle payout is never labelled as reward",
    }
    output = root / "data/manifests/polygon_uma_ledger.json"
    write_json(output, manifest)
    return output
