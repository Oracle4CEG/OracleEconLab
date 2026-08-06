"""Decoders for durable Pyth OIS integrity-pool state on Solana mainnet."""
from __future__ import annotations

import hashlib
import struct
from typing import Any


MAX_PUBLISHERS = 1_024
MAX_EVENTS = 52
FRAC64_MULTIPLIER = 1_000_000
EPOCH_SECONDS = 7 * 24 * 60 * 60
EVENT_SIZE = 8 + 8 + 7 * 8 + MAX_PUBLISHERS * 3 * 8
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def base58_encode(value: bytes) -> str:
    leading = len(value) - len(value.lstrip(b"\0"))
    number = int.from_bytes(value, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = ALPHABET[remainder] + encoded
    return "1" * leading + encoded


def base58_decode(value: str) -> bytes:
    number = 0
    for character in value:
        try:
            digit = ALPHABET.index(character)
        except ValueError as exc:
            raise ValueError(f"invalid base58 character: {character}") from exc
        number = number * 58 + digit
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    return b"\0" * (len(value) - len(value.lstrip("1"))) + raw


INTEGRITY_POOL_INSTRUCTIONS = (
    "initialize_pool",
    "update_y",
    "update_reward_program_authority",
    "update_delegation_fee",
    "delegate",
    "merge_delegation_positions",
    "undelegate",
    "set_publisher_stake_account",
    "advance",
    "advance_delegation_record",
    "withdraw",
    "create_slash_event",
    "slash",
)
INSTRUCTION_BY_DISCRIMINATOR = {
    hashlib.sha256(f"global:{name}".encode()).digest()[:8]: name
    for name in INTEGRITY_POOL_INSTRUCTIONS
}
# Anchor's legacy IDL-management entrypoint is dispatched by the deployed
# program before the business-instruction table.  These writes publish the
# program IDL and never mutate stake, rewards, or slash state.  The label is
# independently confirmed by the successful transaction logs ("IdlWrite").
ANCHOR_IDL_WRITE_DISCRIMINATOR = bytes.fromhex("40f4bc78a7e9690a")
INSTRUCTION_BY_DISCRIMINATOR[ANCHOR_IDL_WRITE_DISCRIMINATOR] = "anchor_idl_write"


def decode_integrity_pool_instruction(data_base58: str) -> dict[str, Any]:
    raw = base58_decode(data_base58)
    if len(raw) < 8:
        raise ValueError("integrity-pool instruction is shorter than its discriminator")
    name = INSTRUCTION_BY_DISCRIMINATOR.get(raw[:8])
    if name is None:
        raise ValueError(f"unknown integrity-pool discriminator: {raw[:8].hex()}")
    args: dict[str, Any] = {}
    body = raw[8:]
    if name == "initialize_pool":
        if len(body) != 40:
            raise ValueError("unexpected initialize_pool argument width")
        args["reward_program_authority"] = base58_encode(body[:32])
        args["y_raw"] = struct.unpack_from("<Q", body, 32)[0]
    elif name in {"delegate", "withdraw", "slash"}:
        if len(body) != 8:
            raise ValueError(f"unexpected {name} argument width")
        args["amount" if name != "slash" else "index"] = struct.unpack("<Q", body)[0]
    elif name == "undelegate":
        if len(body) != 9:
            raise ValueError("unexpected undelegate argument width")
        args["position_index"] = body[0]
        args["amount"] = struct.unpack_from("<Q", body, 1)[0]
    elif name == "create_slash_event":
        if len(body) != 16:
            raise ValueError("unexpected create_slash_event argument width")
        args["index"], args["slash_ratio_raw"] = struct.unpack("<QQ", body)
    elif name in {"update_y", "update_delegation_fee"}:
        if len(body) != 8:
            raise ValueError(f"unexpected {name} argument width")
        args["value_raw"] = struct.unpack("<Q", body)[0]
    elif name == "update_reward_program_authority":
        if len(body) != 32:
            raise ValueError("unexpected update_reward_program_authority argument width")
        args["reward_program_authority"] = base58_encode(body)
    elif name == "anchor_idl_write":
        # The legacy Anchor IDL payload is variable-width and has no economic
        # semantics. Preserve it byte-for-byte rather than pretending it is an
        # integrity-pool argument.
        args["payload_hex"] = body.hex()
    elif body:
        raise ValueError(f"unexpected argument bytes for {name}: {len(body)}")
    return {"instruction": name, "arguments": args, "raw_hex": raw.hex()}


def decode_pool_data(data: bytes) -> dict[str, Any]:
    if len(data) != 2 * 1024 * 1024:
        raise ValueError(f"unexpected PoolData length: {len(data)}")
    offset = 8
    last_updated_epoch, claimable_rewards_raw = struct.unpack_from("<QQ", data, offset)
    offset += 16
    publisher_bytes = [data[offset + index * 32 : offset + (index + 1) * 32] for index in range(MAX_PUBLISHERS)]
    publishers = [base58_encode(value) for value in publisher_bytes if value != bytes(32)]
    offset += MAX_PUBLISHERS * 32
    delegation_states = [struct.unpack_from("<Qq", data, offset + index * 16) for index in range(MAX_PUBLISHERS)]
    offset += MAX_PUBLISHERS * 16
    self_delegation_states = [struct.unpack_from("<Qq", data, offset + index * 16) for index in range(MAX_PUBLISHERS)]
    offset += MAX_PUBLISHERS * 16
    publisher_stake_accounts = [
        base58_encode(data[offset + index * 32 : offset + (index + 1) * 32])
        for index in range(len(publishers))
    ]
    offset += MAX_PUBLISHERS * 32
    events = []
    for storage_index in range(MAX_EVENTS):
        event_offset = offset + storage_index * EVENT_SIZE
        epoch, y = struct.unpack_from("<QQ", data, event_offset)
        publisher_factors = []
        factor_offset = event_offset + 8 + 8 + 7 * 8
        for publisher_index in range(len(publishers)):
            self_ratio, other_ratio, fee = struct.unpack_from("<QQQ", data, factor_offset + publisher_index * 24)
            publisher_factors.append((self_ratio, other_ratio, fee))
        events.append({"storage_index": storage_index, "epoch": epoch, "y": y, "publisher_factors": publisher_factors})
    offset += MAX_EVENTS * EVENT_SIZE
    num_events = struct.unpack_from("<Q", data, offset)[0]
    offset += 8
    slash_counters = list(struct.unpack_from(f"<{MAX_PUBLISHERS}Q", data, offset))[: len(publishers)]
    offset += MAX_PUBLISHERS * 8
    delegation_fees = list(struct.unpack_from(f"<{MAX_PUBLISHERS}Q", data, offset))[: len(publishers)]
    return {
        "last_updated_epoch": last_updated_epoch,
        "claimable_rewards_raw": claimable_rewards_raw,
        "publishers": publishers,
        "publisher_stake_accounts": publisher_stake_accounts,
        "delegation_states": delegation_states[: len(publishers)],
        "self_delegation_states": self_delegation_states[: len(publishers)],
        "events": events,
        "num_events": num_events,
        "slash_counters": slash_counters,
        "delegation_fees": delegation_fees,
    }


def decode_pool_config(data: bytes) -> dict[str, Any]:
    if len(data) != 1_000:
        raise ValueError(f"unexpected PoolConfig length: {len(data)}")
    offset = 8
    values = []
    for _ in range(3):
        values.append(base58_encode(data[offset : offset + 32])); offset += 32
    y = struct.unpack_from("<Q", data, offset)[0]; offset += 8
    slash_custody = base58_encode(data[offset : offset + 32])
    return {
        "pool_data": values[0],
        "reward_program_authority": values[1],
        "pyth_token_mint": values[2],
        "y": y,
        "slash_custody": slash_custody,
    }
