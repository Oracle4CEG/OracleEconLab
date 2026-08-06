"""Parse Tellor tip funding and realized tip-withdrawal transactions."""
from __future__ import annotations

from typing import Any


ORACLE_MODULE = "tellor1jgp27m8fykex4e4jtt0l7ze8q528ux2l38pvhu"
TIPS_ESCROW_MODULE = "tellor1zgan0w9a88r2drnqcgm535lmxrm05wg5cc6xsd"


def attributes(event: dict[str, Any]) -> dict[str, str]:
    return {str(row["key"]): str(row["value"]) for row in event.get("attributes") or []}


def loya_amount(value: str) -> int | None:
    total = 0
    matched = False
    for coin in value.split(","):
        if coin.endswith("loya") and coin[:-4].isdigit():
            total += int(coin[:-4])
            matched = True
    return total if matched else None


def _events(tx: dict[str, Any]) -> list[dict[str, Any]]:
    return tx.get("tx_result", {}).get("events") or tx.get("events") or []


def parse_tip_transactions(
    transactions: list[dict[str, Any]],
    time_by_height: dict[int, str],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for tx in transactions:
        height = int(tx["height"])
        tx_hash = str(tx["hash"]).upper()
        events = _events(tx)
        transfers = [attributes(event) for event in events if event.get("type") == "transfer"]
        for event_index, event in enumerate(events):
            if event.get("type") != "tip_added":
                continue
            row = attributes(event)
            msg_index = row.get("msg_index")
            candidates = []
            for transfer in transfers:
                if transfer.get("sender") != row["tipper"]:
                    continue
                if transfer.get("recipient") != ORACLE_MODULE:
                    continue
                if msg_index is not None and transfer.get("msg_index") != msg_index:
                    continue
                amount = loya_amount(transfer.get("amount", ""))
                if amount is not None:
                    candidates.append(amount)
            if len(candidates) != 1:
                raise ValueError(
                    f"{tx_hash}: tip_added expected one gross transfer, got {candidates}"
                )
            gross = candidates[0]
            net = int(row["amount"])
            burn = gross - net
            output.append(
                {
                    "height": height,
                    "block_time": time_by_height[height],
                    "source_tx": tx_hash,
                    "event_index": event_index,
                    "msg_index": int(msg_index) if msg_index is not None else None,
                    "tipper": row["tipper"],
                    "query_id": row["query_id"].lower().removeprefix("0x"),
                    "querymeta_id": int(row["querymeta_id"]),
                    "gross_tip_loya_raw": str(gross),
                    "protocol_burn_loya_raw": str(burn),
                    "net_tip_funding_loya_raw": str(net),
                    "asset": "loya",
                    "asset_decimals": 6,
                    "cashflow_verified": burn == gross * 2 // 100,
                    "semantic_class": "query_reward_funding_not_reporter_payment",
                    "rule_id": "TELLOR_TIP_ADDED_AND_TRANSFER_V1",
                }
            )
    return output


def parse_withdraw_transactions(
    transactions: list[dict[str, Any]],
    time_by_height: dict[int, str],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for tx in transactions:
        height = int(tx["height"])
        tx_hash = str(tx["hash"]).upper()
        events = _events(tx)
        spent = [attributes(event) for event in events if event.get("type") == "coin_spent"]
        for event_index, event in enumerate(events):
            if event.get("type") != "tip_withdrawn":
                continue
            row = attributes(event)
            msg_index = row.get("msg_index")
            amount = int(row["amount"])
            cashflow_matches = []
            for coin_event in spent:
                if coin_event.get("spender") != TIPS_ESCROW_MODULE:
                    continue
                if msg_index is not None and coin_event.get("msg_index") != msg_index:
                    continue
                spent_amount = loya_amount(coin_event.get("amount", ""))
                if spent_amount is not None:
                    cashflow_matches.append(spent_amount)
            output.append(
                {
                    "height": height,
                    "block_time": time_by_height[height],
                    "source_tx": tx_hash,
                    "event_index": event_index,
                    "msg_index": int(msg_index) if msg_index is not None else None,
                    "selector": row["selector"],
                    "validator": row["validator"],
                    "reward_withdrawn_to_stake_loya_raw": str(amount),
                    "new_validator_shares": row.get("shares"),
                    "withdraw_event_schema": (
                        "v2_with_validator_shares"
                        if row.get("shares") is not None
                        else "v1_without_validator_shares"
                    ),
                    "asset": "loya",
                    "asset_decimals": 6,
                    "cashflow_verified": cashflow_matches.count(amount) == 1,
                    "escrow_coin_spent_matches": cashflow_matches.count(amount),
                    "semantic_class": "realized_reward_compounded_to_stake",
                    "rule_id": "TELLOR_TIP_WITHDRAWN_AND_ESCROW_FLOW_V1",
                }
            )
    return output
