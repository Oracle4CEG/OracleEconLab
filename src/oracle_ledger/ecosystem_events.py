"""Event signatures and strict decoders for Chronicle and RedStone EVM evidence."""
from __future__ import annotations

from typing import Any

from eth_abi import decode
from eth_utils import keccak


def topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


CHRONICLE_SIGNATURES = {
    "Poked": "Poked(address,uint128,uint32)",
    "FeedLifted": "FeedLifted(address,address)",
    "FeedDropped": "FeedDropped(address,address)",
    "BarUpdated": "BarUpdated(address,uint8,uint8)",
    "OpPoked": "OpPoked(address,address,(bytes32,address,bytes),(uint128,uint32))",
    "OpPokeChallengedSuccessfully": (
        "OpPokeChallengedSuccessfully(address,(bytes32,address,bytes),bytes)"
    ),
    "OpPokeChallengedUnsuccessfully": (
        "OpPokeChallengedUnsuccessfully(address,(bytes32,address,bytes))"
    ),
    "OpChallengeRewardPaid": (
        "OpChallengeRewardPaid(address,(bytes32,address,bytes),uint256)"
    ),
    "OpPokeDataDropped": "OpPokeDataDropped(address,(uint128,uint32))",
    "OpChallengePeriodUpdated": "OpChallengePeriodUpdated(address,uint16,uint16)",
    "MaxChallengeRewardUpdated": "MaxChallengeRewardUpdated(address,uint256,uint256)",
}
CHRONICLE_TOPICS = {topic(signature): name for name, signature in CHRONICLE_SIGNATURES.items()}

REDSTONE_SIGNATURES = {
    "ValueUpdate": "ValueUpdate(uint256,bytes32,uint256)",
    "AnswerUpdated": "AnswerUpdated(int256,uint256,uint256)",
    "UpdateSkipDueToBlockTimestamp": "UpdateSkipDueToBlockTimestamp(bytes32)",
    "UpdateSkipDueToDataTimestamp": "UpdateSkipDueToDataTimestamp(bytes32)",
    "UpdateSkipDueToInvalidValue": "UpdateSkipDueToInvalidValue(bytes32)",
}
REDSTONE_TOPICS = {topic(signature): name for name, signature in REDSTONE_SIGNATURES.items()}


def _bytes(value: str) -> bytes:
    return bytes.fromhex(value.removeprefix("0x"))


def _address_from_topic(value: str) -> str:
    raw = value.removeprefix("0x")
    if len(raw) != 64 or any(character != "0" for character in raw[:24]):
        raise ValueError(f"invalid indexed address: {value}")
    return "0x" + raw[-40:].lower()


def _bytes32_text(value: bytes | str) -> str:
    raw = _bytes(value) if isinstance(value, str) else value
    return raw.rstrip(b"\x00").decode("utf-8", errors="replace")


def _schnorr(value: tuple[bytes, str, bytes]) -> dict[str, Any]:
    signature, commitment, feed_ids = value
    return {
        "schnorr_signature": "0x" + signature.hex(),
        "schnorr_commitment": str(commitment).lower(),
        "feed_ids_hex": "0x" + feed_ids.hex(),
        "feed_ids_count": len(feed_ids),
    }


def decode_chronicle_log(log: dict[str, Any]) -> dict[str, Any]:
    topics = log["topics"]
    name = CHRONICLE_TOPICS.get(str(topics[0]).lower())
    if name is None:
        raise ValueError(f"unknown Chronicle topic: {topics[0]}")
    data = _bytes(log["data"])
    row: dict[str, Any] = {
        "protocol": "Chronicle",
        "event_name": name,
        "contract_address": str(log["address"]).lower(),
        "transaction_hash": str(log["transactionHash"]).lower(),
        "block_number": int(log["blockNumber"], 16),
        "transaction_index": int(log["transactionIndex"], 16),
        "log_index": int(log["logIndex"], 16),
        "removed": bool(log.get("removed", False)),
    }

    if name == "Poked":
        if len(topics) != 2:
            raise ValueError("Poked topic count mismatch")
        val, age = decode(["uint128", "uint32"], data)
        row.update(
            {
                "caller": _address_from_topic(topics[1]),
                "value": str(val),
                "oracle_timestamp": int(age),
                "semantic_class": "report_delivery",
            }
        )
    elif name in {"FeedLifted", "FeedDropped"}:
        if len(topics) != 3 or data:
            raise ValueError(f"{name} ABI mismatch")
        caller = _address_from_topic(topics[1])
        row.update(
            {
                "caller": caller,
                "feed": _address_from_topic(topics[2]),
                "semantic_class": (
                    "validator_configuration"
                    if name == "FeedLifted"
                    else "non_monetary_penalty_or_governance_removal"
                ),
                "self_governed_drop": name == "FeedDropped"
                and caller == str(log["address"]).lower(),
            }
        )
    elif name == "BarUpdated":
        if len(topics) != 2:
            raise ValueError("BarUpdated topic count mismatch")
        old_bar, new_bar = decode(["uint8", "uint8"], data)
        row.update(
            {
                "caller": _address_from_topic(topics[1]),
                "old_bar": int(old_bar),
                "new_bar": int(new_bar),
                "semantic_class": "security_configuration",
            }
        )
    elif name == "OpPoked":
        if len(topics) != 3:
            raise ValueError("OpPoked topic count mismatch")
        schnorr, poke_data = decode(
            ["(bytes32,address,bytes)", "(uint128,uint32)"], data
        )
        row.update(
            {
                "caller": _address_from_topic(topics[1]),
                "op_feed": _address_from_topic(topics[2]),
                **_schnorr(schnorr),
                "value": str(poke_data[0]),
                "oracle_timestamp": int(poke_data[1]),
                "semantic_class": "optimistic_report_delivery",
            }
        )
    elif name == "OpPokeChallengedSuccessfully":
        if len(topics) != 2:
            raise ValueError(f"{name} topic count mismatch")
        schnorr, error_data = decode(["(bytes32,address,bytes)", "bytes"], data)
        row.update(
            {
                "caller": _address_from_topic(topics[1]),
                **_schnorr(schnorr),
                "schnorr_error_hex": "0x" + error_data.hex(),
                "semantic_class": "successful_invalid_report_challenge",
            }
        )
    elif name == "OpPokeChallengedUnsuccessfully":
        if len(topics) != 2:
            raise ValueError(f"{name} topic count mismatch")
        (schnorr,) = decode(["(bytes32,address,bytes)"], data)
        row.update(
            {
                "caller": _address_from_topic(topics[1]),
                **_schnorr(schnorr),
                "semantic_class": "unsuccessful_report_challenge",
            }
        )
    elif name == "OpChallengeRewardPaid":
        if len(topics) != 2:
            raise ValueError(f"{name} topic count mismatch")
        schnorr, reward = decode(["(bytes32,address,bytes)", "uint256"], data)
        row.update(
            {
                "challenger": _address_from_topic(topics[1]),
                **_schnorr(schnorr),
                "reward_amount_raw": str(reward),
                "reward_asset": "ETH",
                "reward_asset_decimals": 18,
                "semantic_class": "realized_challenge_reward",
            }
        )
    elif name == "OpPokeDataDropped":
        if len(topics) != 2:
            raise ValueError(f"{name} topic count mismatch")
        (poke_data,) = decode(["(uint128,uint32)"], data)
        row.update(
            {
                "caller": _address_from_topic(topics[1]),
                "value": str(poke_data[0]),
                "oracle_timestamp": int(poke_data[1]),
                "semantic_class": "optimistic_report_removed",
            }
        )
    elif name == "OpChallengePeriodUpdated":
        if len(topics) != 2:
            raise ValueError(f"{name} topic count mismatch")
        old_value, new_value = decode(["uint16", "uint16"], data)
        row.update(
            {
                "caller": _address_from_topic(topics[1]),
                "old_challenge_period_seconds": int(old_value),
                "new_challenge_period_seconds": int(new_value),
                "semantic_class": "security_configuration",
            }
        )
    elif name == "MaxChallengeRewardUpdated":
        if len(topics) != 2:
            raise ValueError(f"{name} topic count mismatch")
        old_value, new_value = decode(["uint256", "uint256"], data)
        row.update(
            {
                "caller": _address_from_topic(topics[1]),
                "old_max_challenge_reward_raw": str(old_value),
                "new_max_challenge_reward_raw": str(new_value),
                "reward_asset": "ETH",
                "reward_asset_decimals": 18,
                "semantic_class": "economic_configuration",
            }
        )
    return row


def decode_redstone_log(
    log: dict[str, Any],
    labels_by_address: dict[str, list[str]],
) -> dict[str, Any]:
    topics = log["topics"]
    name = REDSTONE_TOPICS.get(str(topics[0]).lower())
    if name is None:
        raise ValueError(f"unknown RedStone topic: {topics[0]}")
    address = str(log["address"]).lower()
    data = _bytes(log["data"])
    row: dict[str, Any] = {
        "protocol": "RedStone",
        "event_name": name,
        "contract_address": address,
        "feed_labels": sorted(labels_by_address.get(address, [])),
        "transaction_hash": str(log["transactionHash"]).lower(),
        "block_number": int(log["blockNumber"], 16),
        "transaction_index": int(log["transactionIndex"], 16),
        "log_index": int(log["logIndex"], 16),
        "removed": bool(log.get("removed", False)),
        "semantic_class": (
            "report_delivery"
            if name in {"ValueUpdate", "AnswerUpdated"}
            else "rejected_report_update"
        ),
    }
    if name == "ValueUpdate":
        if len(topics) != 1:
            raise ValueError("ValueUpdate topic count mismatch")
        value, data_feed_id, updated_at = decode(["uint256", "bytes32", "uint256"], data)
        row.update(
            {
                "value": str(value),
                "data_feed_id_hex": "0x" + data_feed_id.hex(),
                "data_feed_id_text": _bytes32_text(data_feed_id),
                "oracle_timestamp": int(updated_at),
            }
        )
    elif name == "AnswerUpdated":
        if len(topics) != 3:
            raise ValueError("AnswerUpdated topic count mismatch")
        (updated_at,) = decode(["uint256"], data)
        current_raw = int(str(topics[1]), 16)
        if current_raw >= 1 << 255:
            current_raw -= 1 << 256
        row.update(
            {
                "value": str(current_raw),
                "round_id": str(int(str(topics[2]), 16)),
                "oracle_timestamp": int(updated_at),
            }
        )
    else:
        if len(topics) != 1:
            raise ValueError(f"{name} topic count mismatch")
        (data_feed_id,) = decode(["bytes32"], data)
        row.update(
            {
                "data_feed_id_hex": "0x" + data_feed_id.hex(),
                "data_feed_id_text": _bytes32_text(data_feed_id),
            }
        )
    return row
