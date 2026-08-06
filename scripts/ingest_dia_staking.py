"""Collect DIA Lasernet staking and realized reward withdrawals through cutoff."""
from __future__ import annotations

import gzip
import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
RPC_URL = "https://rpc.diadata.org/"
CHAIN_ID = 1050
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
STAKING = "0x677Cf1299c367F6cf6F3E1669aCC18Fd059a5919"
WDIA = "0x9F5dA8630d47178baB71F5923644A28B15cBdCa7"
TOPICS = {
    "Staked": "0x1449c6dd7851abc30abf37f57715f492010519147cc2652fbc38202c18a6ee90",
    "UnstakeRequested": "0x828764c21e74c28710e19919735825aba966621c95cbd913f8ed65a2d298f48c",
    "RewardAdded": "0xfb5edb6eb340a01f6a67189edc978df97841c43752c212fc85995ea230017635",
}
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
UNSTAKE_SELECTOR = "0x2e17de78"
STAKING_STORES_SELECTOR = "0xf3877be8"


class Rpc:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.request_id = 0

    def call(self, method: str, params: list[Any]) -> Any:
        error: Exception | None = None
        for attempt in range(6):
            try:
                self.request_id += 1
                response = self.session.post(
                    RPC_URL,
                    json={"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params},
                    timeout=60,
                )
                response.raise_for_status()
                body = response.json()
                if "error" in body:
                    raise RuntimeError(str(body["error"]))
                return body["result"]
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                error = exc
                if attempt < 5:
                    time.sleep(min(2**attempt, 15))
        raise RuntimeError(f"DIA RPC failed: {method}") from error

    def block_at_or_before(self, unix: int) -> int:
        low, high = 0, int(self.call("eth_blockNumber", []), 16)
        while low < high:
            middle = (low + high + 1) // 2
            timestamp = int(self.call("eth_getBlockByNumber", [hex(middle), False])["timestamp"], 16)
            if timestamp <= unix:
                low = middle
            else:
                high = middle - 1
        return low

    def deployment_block(self, address: str, high: int) -> int:
        low = 0
        while low < high:
            middle = (low + high) // 2
            if self.call("eth_getCode", [address, hex(middle)]) == "0x":
                low = middle + 1
            else:
                high = middle
        return low


def address_topic(address: str) -> str:
    return "0x" + "0" * 24 + address.lower().removeprefix("0x")


def topic_address(value: str) -> str:
    return "0x" + value[-40:].lower()


def words(value: str) -> list[int]:
    raw = value.removeprefix("0x")
    return [int(raw[offset : offset + 64], 16) for offset in range(0, len(raw), 64)]


def atomic_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(".jsonl.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)


def main() -> None:
    rpc = Rpc()
    if int(rpc.call("eth_chainId", []), 16) != CHAIN_ID:
        raise RuntimeError("unexpected DIA Lasernet chain id")
    cutoff_block = rpc.block_at_or_before(int(CUTOFF.timestamp()))
    deployment_block = rpc.deployment_block(STAKING, cutoff_block)
    raw: dict[str, list[dict[str, Any]]] = {}
    for event, topic in TOPICS.items():
        raw[event] = rpc.call("eth_getLogs", [{
            "address": STAKING, "fromBlock": hex(deployment_block),
            "toBlock": hex(cutoff_block), "topics": [topic],
        }])
    raw["WithdrawalTransfers"] = rpc.call("eth_getLogs", [{
        "address": WDIA, "fromBlock": hex(deployment_block), "toBlock": hex(cutoff_block),
        "topics": [TRANSFER, address_topic(STAKING)],
    }])

    raw_dir = (ROOT / "data/raw/dia_lasernet").resolve()
    curated = (ROOT / "data/curated").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "staking_logs.json.gz"
    temporary_raw = raw_path.with_suffix(".gz.tmp")
    with gzip.open(temporary_raw, "wt", encoding="utf-8") as handle:
        json.dump(raw, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    temporary_raw.replace(raw_path)

    event_rows: list[dict[str, Any]] = []
    for event in ("Staked", "UnstakeRequested", "RewardAdded"):
        for log in raw[event]:
            decoded: dict[str, Any] = {}
            values = words(log["data"])
            if event == "Staked":
                decoded = {
                    "actor": topic_address(log["topics"][1]),
                    "staking_store_index": int(log["topics"][2], 16),
                    "principal_amount_raw": str(values[0]),
                }
            elif event == "UnstakeRequested":
                decoded = {
                    "actor": topic_address(log["topics"][1]),
                    "staking_store_index": int(log["topics"][2], 16),
                }
            else:
                decoded = {
                    "reward_funding_amount_raw": str(values[0]),
                    "actor": "0x" + f"{values[1]:040x}",
                }
            event_rows.append({
                "event": event,
                **decoded,
                "block_number": int(log["blockNumber"], 16),
                "block_time_unix": int(log["blockTimestamp"], 16),
                "transaction_hash": log["transactionHash"].lower(),
                "log_index": int(log["logIndex"], 16),
                "asset": "wDIA",
                "asset_decimals": 18,
                "source_contract": STAKING.lower(),
                "rule_id": "DIA_LASERNET_STAKING_EVENT_V1",
            })

    transfers_by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for log in raw["WithdrawalTransfers"]:
        transfers_by_tx[log["transactionHash"].lower()].append(log)
    withdrawals: list[dict[str, Any]] = []
    exact = 0
    for tx_hash, transfer_logs in sorted(transfers_by_tx.items()):
        tx = rpc.call("eth_getTransactionByHash", [tx_hash])
        if not str(tx["input"]).startswith(UNSTAKE_SELECTOR):
            raise RuntimeError(f"non-unstake outgoing wDIA transfer: {tx_hash}")
        staking_store_index = int(tx["input"][10:74], 16)
        call_data = STAKING_STORES_SELECTOR + f"{staking_store_index:064x}"
        state = words(rpc.call("eth_call", [
            {"to": STAKING, "data": call_data}, hex(int(tx["blockNumber"], 16) - 1),
        ]))
        if len(state) != 11:
            raise RuntimeError("unexpected stakingStores return shape")
        principal = state[8]
        principal_wallet_reward = state[9]
        beneficiary_reward = state[10]
        paid = sum(int(log["data"], 16) for log in transfer_logs)
        expected = principal + principal_wallet_reward + beneficiary_reward
        is_exact = paid == expected
        exact += int(is_exact)
        withdrawals.append({
            "staking_store_index": staking_store_index,
            "beneficiary": "0x" + f"{state[0]:040x}",
            "principal_payout_wallet": "0x" + f"{state[1]:040x}",
            "principal_unstaker": "0x" + f"{state[2]:040x}",
            "principal_returned_raw": str(principal),
            "principal_wallet_reward_raw": str(principal_wallet_reward),
            "beneficiary_reward_raw": str(beneficiary_reward),
            "total_reward_raw": str(principal_wallet_reward + beneficiary_reward),
            "total_paid_raw": str(paid),
            "payment_transfer_count": len(transfer_logs),
            "payment_exact": is_exact,
            "block_number": int(tx["blockNumber"], 16),
            "block_time_unix": int(transfer_logs[0]["blockTimestamp"], 16),
            "transaction_hash": tx_hash,
            "asset": "wDIA",
            "asset_decimals": 18,
            "source_contract": STAKING.lower(),
            "rule_id": "DIA_LASERNET_UNSTAKE_REWARD_DECOMPOSITION_V1",
            "interpretation": "Reward is the historical stakingStores requested reward fields immediately before realized unstake payment.",
        })

    event_rows.sort(key=lambda row: (row["block_number"], row["log_index"]))
    withdrawals.sort(key=lambda row: (row["block_number"], row["transaction_hash"]))
    atomic_jsonl(curated / "dia_staking_events.jsonl", event_rows)
    atomic_jsonl(curated / "dia_staking_withdrawals.jsonl", withdrawals)
    manifest = {
        "dataset": "DIA Lasernet realized staking reward ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff": CUTOFF.isoformat(),
        "chain_id": CHAIN_ID,
        "cutoff_block": cutoff_block,
        "deployment_block": deployment_block,
        "event_counts": {name: len(rows) for name, rows in raw.items()},
        "staking_events": len(event_rows),
        "realized_withdrawals": len(withdrawals),
        "exact_principal_reward_payment_decompositions": exact,
        "realized_reward_amount_raw": str(sum(int(row["total_reward_raw"]) for row in withdrawals)),
        "slashing_status_at_cutoff": "not_implemented_per_official_staking_FAQ",
        "slashing_amount_imputed_as_zero": False,
        "raw_logs": str(raw_path),
        "curated_events": str(curated / "dia_staking_events.jsonl"),
        "curated_withdrawals": str(curated / "dia_staking_withdrawals.jsonl"),
        "official_contract_discovery": "DIA staking app runtime configuration and ABI",
        "all_required_assertions_pass": bool(withdrawals) and exact == len(withdrawals),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"DIA staking QC failed: {manifest}")
    path = ROOT / "data/manifests/dia_staking.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(path)


if __name__ == "__main__":
    main()
