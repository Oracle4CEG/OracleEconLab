"""Collect Chainlink ETH/USD service evidence and LINK flows on Ethereum."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from eth_utils import keccak

from .chainlink_staking import CONTRACTS as STAKING_CONTRACTS
from .ethereum_audit import CUTOFF_UTC, deployment_block_by_code, int_quantity, selector, timestamp_to_block, utc_timestamp
from .rpc import JsonRpc, RpcError, write_json


LINK_TOKEN = "0x514910771af9ca656af840dff83e8264ecf986ca"
ETH_USD_PROXY = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
TRANSFER_TOPIC = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
ANSWER_UPDATED_TOPIC = "0x" + keccak(text="AnswerUpdated(int256,uint256,uint256)").hex()
NEW_TRANSMISSION_TOPIC = "0x" + keccak(text="NewTransmission(uint32,int192,address,int192[],bytes,bytes32)").hex()
AGGREGATOR_PROPOSED_TOPIC = "0x" + keccak(text="AggregatorProposed(address,address)").hex()
AGGREGATOR_CONFIRMED_TOPIC = "0x" + keccak(text="AggregatorConfirmed(address,address)").hex()


def topic_address(value: str) -> str:
    return "0x" + "0" * 24 + value[2:].lower()


def save(path: Path, logs: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=9) as handle:
        for log in logs:
            handle.write(json.dumps(log, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)
    path.with_suffix(path.suffix + ".sha256").write_text(hashlib.sha256(path.read_bytes()).hexdigest() + "\n", encoding="utf-8")


def load(path: Path) -> list[dict[str, Any]] | None:
    digest = path.with_suffix(path.suffix + ".sha256")
    if not path.is_file() or not digest.is_file():
        return None
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest.read_text(encoding="utf-8").strip():
        raise RuntimeError(f"checksum mismatch: {path}")
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def scan_link_flows(
    rpc: JsonRpc, start: int, end: int, chunk: int, workers: int, folder: Path
) -> dict[str, Any]:
    targets = [topic_address(address) for address in STAKING_CONTRACTS.values()]
    ranges = [(first, min(first + chunk - 1, end)) for first in range(start, end + 1, chunk)]

    def fetch(block_range: tuple[int, int]) -> list[dict[str, Any]]:
        first, last = block_range
        last = min(first + chunk - 1, end)
        path = folder / f"chainlink_link_flows_{first}_{last}.jsonl.gz"
        logs = load(path)
        if logs is not None:
            return logs
        error: Exception | None = None
        for attempt in range(6):
            try:
                call_rpc = JsonRpc(rpc.url, timeout_seconds=rpc.timeout_seconds)
                outgoing = call_rpc.call("eth_getLogs", [{"address": LINK_TOKEN, "topics": [TRANSFER_TOPIC, targets], "fromBlock": hex(first), "toBlock": hex(last)}])
                incoming = call_rpc.call("eth_getLogs", [{"address": LINK_TOKEN, "topics": [TRANSFER_TOPIC, None, targets], "fromBlock": hex(first), "toBlock": hex(last)}])
                merged = {(row["transactionHash"].lower(), row["logIndex"].lower()): row for row in outgoing + incoming}
                logs = sorted(merged.values(), key=lambda row: (int(row["blockNumber"], 16), int(row["logIndex"], 16)))
                save(path, logs)
                return logs
            except RpcError as exc:
                error = exc
                time.sleep(min(2**attempt, 15))
        raise RuntimeError(f"failed LINK flow range {first}-{last}: {error}")

    counts: Counter[str] = Counter(); total = chunks = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch, item) for item in ranges]
        for future in as_completed(futures):
            logs = future.result(); total += len(logs); chunks += 1
            for row in logs:
                sender = "0x" + row["topics"][1][-40:].lower(); receiver = "0x" + row["topics"][2][-40:].lower()
                counts["outgoing_from_protocol" if sender in {x.lower() for x in STAKING_CONTRACTS.values()} else "incoming_to_protocol"] += 1
            if chunks % 10 == 0 or chunks == len(ranges):
                print(f"Chainlink LINK flows: {chunks}/{len(ranges)} chunks, {total} logs", flush=True)
    return {"from_block": start, "to_block": end, "chunks": chunks, "logs": total, "direction_counts": dict(counts)}


def scan_feed(
    rpc: JsonRpc, start: int, end: int, chunk: int, workers: int, folder: Path
) -> dict[str, Any]:
    # The proxy's authoritative phase mapping is more reliable than scanning
    # for upgrade events: some upgrades are emitted by the Feed Registry, not
    # by this proxy address.
    ranges = [(first, min(first + chunk - 1, end)) for first in range(start, end + 1, chunk)]
    def phase_at(block: int) -> int:
        return int(
            rpc.call(
                "eth_call",
                [{"to": ETH_USD_PROXY, "data": selector("phaseId()")}, hex(block)],
            ),
            16,
        )

    start_phase = phase_at(start); phase_id = phase_at(end)
    aggregators: set[str] = set()
    phase_aggregators: dict[int, str] = {}
    for phase in range(1, phase_id + 1):
        data = selector("phaseAggregators(uint16)") + phase.to_bytes(32, "big").hex()
        value = rpc.call("eth_call", [{"to": ETH_USD_PROXY, "data": data}, hex(end)])
        address = "0x" + value[-40:].lower()
        aggregators.add(address); phase_aggregators[phase] = address

    def first_block_of_phase(target: int) -> int:
        low, high = start - 1, end
        while low + 1 < high:
            middle = (low + high) // 2
            if phase_at(middle) >= target:
                high = middle
            else:
                low = middle
        return high

    phase_intervals: list[dict[str, Any]] = []
    for phase in range(start_phase, phase_id + 1):
        valid_from = start if phase == start_phase else first_block_of_phase(phase)
        valid_to = end if phase == phase_id else first_block_of_phase(phase + 1) - 1
        phase_intervals.append({
            "phase_id": phase,
            "aggregator": phase_aggregators[phase],
            "valid_from_block": valid_from,
            "valid_to_block": valid_to,
        })
    current = rpc.call("eth_call", [{"to": ETH_USD_PROXY, "data": selector("aggregator()")}, hex(end)])
    aggregators.add("0x" + current[-40:].lower())
    aggregators.discard("0x" + "00" * 20)
    addresses = [ETH_USD_PROXY] + sorted(aggregators)
    def fetch(block_range: tuple[int, int]) -> list[dict[str, Any]]:
        first, last = block_range
        last = min(first + chunk - 1, end)
        path = folder / f"chainlink_eth_usd_{first}_{last}.jsonl.gz"
        logs = load(path)
        if logs is not None:
            return logs
        error: Exception | None = None
        for attempt in range(6):
            try:
                call_rpc = JsonRpc(rpc.url, timeout_seconds=rpc.timeout_seconds)
                logs = call_rpc.call("eth_getLogs", [{
                    "address": addresses,
                    "topics": [[
                        ANSWER_UPDATED_TOPIC,
                        NEW_TRANSMISSION_TOPIC,
                        AGGREGATOR_PROPOSED_TOPIC,
                        AGGREGATOR_CONFIRMED_TOPIC,
                    ]],
                    "fromBlock": hex(first),
                    "toBlock": hex(last),
                }])
                save(path, logs)
                return logs
            except RpcError as exc:
                error = exc
                time.sleep(min(2**attempt, 15))
        raise RuntimeError(f"failed ETH/USD range {first}-{last}: {error}")

    counts: Counter[str] = Counter(); total = chunks = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch, item) for item in ranges]
        for future in as_completed(futures):
            logs = future.result(); total += len(logs); chunks += 1
            for row in logs:
                topic = row["topics"][0].lower()
                event = "AnswerUpdated" if topic == ANSWER_UPDATED_TOPIC else "NewTransmission" if topic == NEW_TRANSMISSION_TOPIC else "proxy_or_config_event"
                counts[event] += 1
            if chunks % 10 == 0 or chunks == len(ranges):
                print(f"Chainlink ETH/USD: {chunks}/{len(ranges)} chunks, {total} logs", flush=True)
    return {
        "from_block": start, "to_block": end, "chunks": chunks, "logs": total,
        "start_phase_id": start_phase, "phase_id": phase_id,
        "phase_intervals": phase_intervals, "aggregators": sorted(aggregators),
        "event_counts_raw_all_phase_contracts": dict(counts),
    }


def collect(args: argparse.Namespace) -> Path:
    root = Path(args.root).resolve(); rpc = JsonRpc(args.rpc_url, timeout_seconds=args.timeout)
    chain_id = int_quantity(rpc.call("eth_chainId", []))
    if chain_id != 1: raise RuntimeError(f"expected Ethereum chain ID 1, got {chain_id}")
    head = int_quantity(rpc.call("eth_blockNumber", [])); cutoff = timestamp_to_block(rpc, utc_timestamp(CUTOFF_UTC), head)
    staking_manifest = json.loads((root / "data/manifests/chainlink_staking_v02_raw.json").read_text(encoding="utf-8"))
    start = int(staking_manifest["raw_log_scan"]["from_block"])
    proxy_deployment = deployment_block_by_code(rpc, ETH_USD_PROXY, head)
    if proxy_deployment is None: raise RuntimeError("ETH/USD proxy has no code")
    end = min(head, cutoff)
    link = scan_link_flows(rpc, start, end, args.chunk_size, args.workers, root / "data/raw/ethereum/chainlink_link_flows")
    feed = scan_feed(rpc, max(start, proxy_deployment), end, args.chunk_size, args.workers, root / "data/raw/ethereum/chainlink_eth_usd")
    manifest = {
        "protocol": "Chainlink Staking v0.2 supporting evidence", "chain_id": 1,
        "generated_at_utc": datetime.now(UTC).isoformat(), "cutoff_block": cutoff,
        "link_token": LINK_TOKEN, "eth_usd_proxy": ETH_USD_PROXY, "link_flows": link, "eth_usd_feed": feed,
        "interpretation_guard": "Transfers are reconciliation evidence; feed events establish service timing but do not alone prove a valid alert.",
    }
    output = root / "data/manifests/chainlink_evidence_raw.json"; write_json(output, manifest); return output


def register_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("ingest-chainlink-evidence", help="collect LINK flows and ETH/USD feed evidence")
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8545")
    parser.add_argument("--root", default=".")
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=120)
    parser.set_defaults(handler=collect)
