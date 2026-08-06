"""Decode and validate Flare RewardManager/VoterRegistry event logs."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


REWARD_CLAIMED_TOPIC = "0x06f77960d1401cc7d724b5c2b5ad672b9dbf08d8b11516a38c21697c23fbb0d2"
BENEFICIARY_CHILLED_UINT256_TOPIC = (
    "0x0a5e087b026d8f1c57e75d9d0cb0394c2ad3535e7a15d97d553be80476274cd0"
)
BURN_ADDRESS = "0x000000000000000000000000000000000000dead"
CLAIM_TYPES = {0: "DIRECT", 1: "FEE", 2: "WNAT", 3: "MIRROR", 4: "CCHAIN"}


def _hex_int(value: str) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"expected hex string, got {value!r}")
    return int(value, 16)


def _topic_address(value: str) -> str:
    raw = value.removeprefix("0x")
    if len(raw) != 64:
        raise ValueError(f"expected 32-byte address topic, got {value}")
    if int(raw[:24], 16):
        raise ValueError(f"nonzero address topic padding: {value}")
    return "0x" + raw[-40:].lower()


def _topic_bytes20(value: str) -> str:
    raw = value.removeprefix("0x")
    if len(raw) != 64:
        raise ValueError(f"expected 32-byte bytes20 topic, got {value}")
    if int(raw[40:], 16):
        raise ValueError(f"nonzero bytes20 topic padding: {value}")
    return "0x" + raw[:40].lower()


def _data_words(value: str, expected: int) -> list[int]:
    raw = value.removeprefix("0x")
    if len(raw) != expected * 64:
        raise ValueError(f"expected {expected} ABI words, got {len(raw) // 64}")
    return [int(raw[offset : offset + 64], 16) for offset in range(0, len(raw), 64)]


def _timestamp(value: str) -> tuple[int, str]:
    timestamp = _hex_int(value)
    return timestamp, datetime.fromtimestamp(timestamp, UTC).isoformat().replace("+00:00", "Z")


def log_key(log: dict[str, Any]) -> tuple[int, int, int, str]:
    """Return a deterministic source-order/deduplication key."""
    return (
        _hex_int(log["blockNumber"]),
        _hex_int(log.get("transactionIndex") or "0x0"),
        _hex_int(log["logIndex"]),
        str(log["transactionHash"]).lower(),
    )


def decode_reward_claimed(log: dict[str, Any]) -> dict[str, Any]:
    topics = log["topics"]
    if len(topics) < 4 or str(topics[0]).lower() != REWARD_CLAIMED_TOPIC:
        raise ValueError("not a RewardClaimed log")
    epoch, claim_type_id, amount = _data_words(log["data"], 3)
    if epoch >= 2**24 or claim_type_id >= 2**8 or amount >= 2**120:
        raise ValueError("RewardClaimed ABI width violation")
    timestamp, block_time = _timestamp(log["timeStamp"])
    recipient = _topic_address(topics[3])
    return {
        "beneficiary": _topic_address(topics[1]),
        "reward_owner": _topic_address(topics[2]),
        "recipient": recipient,
        "reward_epoch_id": epoch,
        "claim_type_id": claim_type_id,
        "claim_type": CLAIM_TYPES.get(claim_type_id, f"UNKNOWN_{claim_type_id}"),
        "amount_raw": str(amount),
        "asset": "FLR",
        "asset_decimals": 18,
        "is_fee_burn": recipient == BURN_ADDRESS,
        "source_contract": str(log["address"]).lower(),
        "source_tx": str(log["transactionHash"]).lower(),
        "source_block": _hex_int(log["blockNumber"]),
        "source_tx_index": _hex_int(log.get("transactionIndex") or "0x0"),
        "source_log_index": _hex_int(log["logIndex"]),
        "block_time_unix": timestamp,
        "block_time": block_time,
        "event_signature": "RewardClaimed(address,address,address,uint24,uint8,uint120)",
        "rule_id": "FLARE_REWARD_MANAGER_REWARD_CLAIMED_V1",
    }


def decode_beneficiary_chilled(log: dict[str, Any]) -> dict[str, Any]:
    topics = log["topics"]
    if len(topics) < 2 or str(topics[0]).lower() != BENEFICIARY_CHILLED_UINT256_TOPIC:
        raise ValueError("not a deployed-version BeneficiaryChilled log")
    (until_epoch,) = _data_words(log["data"], 1)
    timestamp, block_time = _timestamp(log["timeStamp"])
    return {
        "beneficiary": _topic_bytes20(topics[1]),
        "chilled_until_reward_epoch_id": until_epoch,
        "source_contract": str(log["address"]).lower(),
        "source_tx": str(log["transactionHash"]).lower(),
        "source_block": _hex_int(log["blockNumber"]),
        "source_tx_index": _hex_int(log.get("transactionIndex") or "0x0"),
        "source_log_index": _hex_int(log["logIndex"]),
        "block_time_unix": timestamp,
        "block_time": block_time,
        "event_signature": "BeneficiaryChilled(bytes20,uint256)",
        "rule_id": "FLARE_VOTER_REGISTRY_BENEFICIARY_CHILLED_V1",
    }
