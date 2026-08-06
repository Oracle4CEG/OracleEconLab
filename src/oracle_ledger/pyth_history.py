"""Parse Pyth OIS transaction instructions and realized token movements."""
from __future__ import annotations

from datetime import UTC, datetime
import json
from typing import Any

from oracle_ledger.pyth_ois import (
    base58_decode,
    decode_integrity_pool_instruction,
)


PROGRAM_ID = "pyti8TM4zRVBjmarcgAPmTNNAXYKJv7WVHrkrm6woLN"
PYTH_MINT = "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3"


def account_pubkeys(message: dict[str, Any]) -> list[str]:
    values = []
    for account in message.get("accountKeys") or []:
        values.append(str(account["pubkey"] if isinstance(account, dict) else account))
    return values


def _inner_token_transfers(meta: dict[str, Any], outer_index: int) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for group in meta.get("innerInstructions") or []:
        if int(group["index"]) != outer_index:
            continue
        for instruction in group.get("instructions") or []:
            if instruction.get("program") != "spl-token":
                continue
            parsed = instruction.get("parsed") or {}
            if parsed.get("type") not in {"transfer", "transferChecked"}:
                continue
            info = parsed.get("info") or {}
            token_amount = info.get("tokenAmount") or {}
            amount = info.get("amount") or token_amount.get("amount")
            if amount is None:
                continue
            output.append({
                "source": str(info["source"]),
                "destination": str(info["destination"]),
                "authority": str(info.get("authority") or ""),
                "amount_raw": str(amount),
            })
    return output


def parse_integrity_pool_transaction(
    signature: str,
    transaction: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Return instruction, stake-mutation, and token-economic rows."""
    message = transaction["transaction"]["message"]
    meta = transaction["meta"]
    accounts = account_pubkeys(message)
    block_time_unix = int(transaction["blockTime"])
    block_time = datetime.fromtimestamp(block_time_unix, UTC).isoformat().replace("+00:00", "Z")
    success = meta.get("err") is None
    instructions: list[dict[str, Any]] = []
    stake_events: list[dict[str, Any]] = []
    economic_events: list[dict[str, Any]] = []

    for outer_index, outer in enumerate(message.get("instructions") or []):
        if outer.get("programId") != PROGRAM_ID or "data" not in outer:
            continue
        encoded_data = str(outer["data"])
        decode_error: str | None = None
        try:
            decoded = decode_integrity_pool_instruction(encoded_data)
        except ValueError as exc:
            # The complete address history contains malformed calls sent by
            # third parties. They are still part of the instruction ledger,
            # but a successful unknown/malformed call would be an unacceptable
            # semantic gap and must stop collection.
            if success:
                raise
            raw = base58_decode(encoded_data)
            decode_error = str(exc)
            decoded = {
                "instruction": "unrecognized_failed_instruction",
                "arguments": {
                    "discriminator_hex": raw[:8].hex(),
                    "payload_hex": raw[8:].hex(),
                },
                "raw_hex": raw.hex(),
            }
        name = decoded["instruction"]
        arguments = decoded["arguments"]
        instruction_accounts = [str(value) for value in outer.get("accounts") or []]
        row = {
            "signature": signature,
            "slot": int(transaction["slot"]),
            "block_time_unix": block_time_unix,
            "block_time": block_time,
            "outer_instruction_index": outer_index,
            "instruction": name,
            "instruction_discriminator_hex": decoded["raw_hex"][:16],
            "arguments_json": json.dumps(
                arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ),
            "amount_raw": (
                str(arguments["amount"]) if arguments.get("amount") is not None else None
            ),
            "position_index": arguments.get("position_index"),
            "slash_event_index": (
                str(arguments["index"]) if arguments.get("index") is not None else None
            ),
            "slash_ratio_raw": (
                str(arguments["slash_ratio_raw"])
                if arguments.get("slash_ratio_raw") is not None
                else None
            ),
            "value_raw": (
                str(arguments["value_raw"])
                if arguments.get("value_raw") is not None
                else None
            ),
            "reward_program_authority": arguments.get("reward_program_authority"),
            "decode_error": decode_error,
            "actor": instruction_accounts[0] if instruction_accounts else accounts[0],
            "accounts": instruction_accounts,
            "success": success,
            "transaction_error_json": (
                json.dumps(
                    meta["err"], ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                if meta.get("err") is not None
                else None
            ),
            "program_id": PROGRAM_ID,
            "rule_id": "PYTH_OIS_INSTRUCTION_V1",
        }
        instructions.append(row)
        if not success:
            continue

        if name in {"delegate", "undelegate"}:
            amount = int(arguments["amount"])
            stake_events.append({
                "signature": signature,
                "slot": int(transaction["slot"]),
                "block_time_unix": block_time_unix,
                "block_time": block_time,
                "outer_instruction_index": outer_index,
                "event": name,
                "owner": instruction_accounts[0],
                "publisher": instruction_accounts[3],
                "stake_account_positions": instruction_accounts[5],
                "position_index": arguments.get("position_index"),
                "amount_raw": str(amount),
                "signed_stake_delta_raw": str(amount if name == "delegate" else -amount),
                "asset": "PYTH",
                "asset_decimals": 6,
                "semantic_class": "integrity_pool_position_mutation",
                "rule_id": "PYTH_OIS_DELEGATE_UNDELEGATE_V1",
            })

        transfers = _inner_token_transfers(meta, outer_index)
        if name == "advance_delegation_record":
            if len(instruction_accounts) < 10:
                raise ValueError("advance_delegation_record account list is incomplete")
            pool_reward_custody = instruction_accounts[4]
            delegator_custody = instruction_accounts[5]
            publisher_custody = instruction_accounts[8] if len(instruction_accounts) >= 12 else None
            for transfer_index, transfer in enumerate(transfers):
                if transfer["source"] != pool_reward_custody:
                    continue
                if transfer["destination"] == delegator_custody:
                    reward_role = "delegator_reward"
                    beneficiary = instruction_accounts[1]
                elif publisher_custody and transfer["destination"] == publisher_custody:
                    reward_role = "publisher_reward"
                    beneficiary = instruction_accounts[6]
                else:
                    raise ValueError("unexpected OIS reward-transfer destination")
                economic_events.append({
                    "signature": signature,
                    "slot": int(transaction["slot"]),
                    "block_time_unix": block_time_unix,
                    "block_time": block_time,
                    "outer_instruction_index": outer_index,
                    "inner_transfer_index": transfer_index,
                    "event": "reward_transfer",
                    "reward_role": reward_role,
                    "beneficiary": beneficiary,
                    "source_token_account": transfer["source"],
                    "destination_token_account": transfer["destination"],
                    "authority": transfer["authority"],
                    "amount_raw": transfer["amount_raw"],
                    "asset": "PYTH",
                    "mint": PYTH_MINT,
                    "asset_decimals": 6,
                    "semantic_class": "paid_reward_to_stake_custody",
                    "rule_id": "PYTH_OIS_ADVANCE_DELEGATION_REWARD_TRANSFER_V1",
                })

        if name == "slash":
            if len(instruction_accounts) < 12:
                raise ValueError("slash account list is incomplete")
            slash_custody = instruction_accounts[11]
            slashed_total = 0
            for transfer_index, transfer in enumerate(transfers):
                if transfer["destination"] != slash_custody:
                    continue
                amount = int(transfer["amount_raw"])
                slashed_total += amount
                economic_events.append({
                    "signature": signature,
                    "slot": int(transaction["slot"]),
                    "block_time_unix": block_time_unix,
                    "block_time": block_time,
                    "outer_instruction_index": outer_index,
                    "inner_transfer_index": transfer_index,
                    "event": "principal_slash_transfer",
                    "reward_role": None,
                    "beneficiary": instruction_accounts[5],
                    "stake_account_positions": instruction_accounts[6],
                    "source_token_account": transfer["source"],
                    "destination_token_account": transfer["destination"],
                    "authority": transfer["authority"],
                    "amount_raw": transfer["amount_raw"],
                    "asset": "PYTH",
                    "mint": PYTH_MINT,
                    "asset_decimals": 6,
                    "semantic_class": "realized_principal_slash",
                    "rule_id": "PYTH_OIS_SLASH_TOKEN_TRANSFER_V1",
                })
            stake_events.append({
                "signature": signature,
                "slot": int(transaction["slot"]),
                "block_time_unix": block_time_unix,
                "block_time": block_time,
                "outer_instruction_index": outer_index,
                "event": "slash",
                "owner": None,
                "publisher": instruction_accounts[5],
                "stake_account_positions": instruction_accounts[6],
                "position_index": None,
                "amount_raw": str(slashed_total),
                "signed_stake_delta_raw": str(-slashed_total),
                "asset": "PYTH",
                "asset_decimals": 6,
                "semantic_class": (
                    "realized_integrity_pool_slash"
                    if slashed_total
                    else "slash_instruction_no_current_epoch_transfer"
                ),
                "rule_id": "PYTH_OIS_SLASH_POSITION_MUTATION_V1",
            })

        if name == "create_slash_event":
            economic_events.append({
                "signature": signature,
                "slot": int(transaction["slot"]),
                "block_time_unix": block_time_unix,
                "block_time": block_time,
                "outer_instruction_index": outer_index,
                "inner_transfer_index": None,
                "event": "slash_parameter_created",
                "reward_role": None,
                "beneficiary": instruction_accounts[6],
                "slash_event_account": instruction_accounts[5],
                "source_token_account": None,
                "destination_token_account": instruction_accounts[2],
                "authority": instruction_accounts[1],
                "amount_raw": None,
                "slash_event_index": str(arguments["index"]),
                "slash_ratio_raw": str(arguments["slash_ratio_raw"]),
                "asset": "PYTH",
                "mint": PYTH_MINT,
                "asset_decimals": 6,
                "semantic_class": "designed_slash_parameter_not_applied_slash",
                "rule_id": "PYTH_OIS_CREATE_SLASH_EVENT_V1",
            })

    return instructions, stake_events, economic_events
