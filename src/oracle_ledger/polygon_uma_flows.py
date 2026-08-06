"""Collect and reconcile Polygon USDC flows for Polymarket UMA request rounds."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from eth_utils import keccak

from .ethereum_audit import CUTOFF_UTC, int_quantity, timestamp_to_block, utc_timestamp
from .polygon_uma import CONTRACTS, _load_cached_range, _save_range, load_env_url
from .rpc import JsonRpc, RpcError, write_json


TOKENS = {
    "USDC_e": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
    "USDC": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
}
UMA_STORE = "0xe58480ca74f1a819fafdd77beded4e2d5629943d"
TRANSFER_TOPIC = "0x" + keccak(text="Transfer(address,address,uint256)").hex()


def topic_address(value: str) -> str:
    return "0x" + "0" * 24 + value[2:].lower()


def data_root(root: Path) -> Path:
    configured = os.environ.get("ORACLE_LEDGER_CURATED_DIR", "data/curated")
    path = Path(configured); return path if path.is_absolute() else root / path


def collect(args: argparse.Namespace) -> Path:
    root = Path(args.root).resolve(); rpc_url = args.rpc_url or load_env_url(root / ".env", args.rpc_env_key)
    rpc = JsonRpc(rpc_url, timeout_seconds=args.timeout)
    if int_quantity(rpc.call("eth_chainId", [])) != 137: raise RuntimeError("expected Polygon chain ID 137")
    head = int_quantity(rpc.call("eth_blockNumber", [])); cutoff = timestamp_to_block(rpc, utc_timestamp(CUTOFF_UTC), head)
    polygon_manifest = json.loads((root / "data/manifests/polygon_uma_raw.json").read_text())
    start = int(polygon_manifest["raw_log_scan"]["from_block"]); end = min(head, cutoff)
    targets = sorted({address.lower() for address in CONTRACTS.values()} | {UMA_STORE})
    target_topics = [topic_address(address) for address in targets]
    raw_dir = root / "data/raw/polygon/uma_token_flows"; raw_dir.mkdir(parents=True, exist_ok=True)
    ranges = [(first, min(first + args.chunk_size - 1, end)) for first in range(start, end + 1, args.chunk_size)]

    def fetch(block_range):
        first, last = block_range; path = raw_dir / f"polygon_uma_token_flows_{first}_{last}.jsonl.gz"
        cached = _load_cached_range(path)
        if cached is not None: return first, last, cached
        call_rpc = JsonRpc(rpc_url, timeout_seconds=args.timeout); error = None
        for attempt in range(6):
            try:
                outgoing = call_rpc.call("eth_getLogs", [{"address": list(TOKENS.values()), "topics": [TRANSFER_TOPIC, target_topics], "fromBlock": hex(first), "toBlock": hex(last)}])
                incoming = call_rpc.call("eth_getLogs", [{"address": list(TOKENS.values()), "topics": [TRANSFER_TOPIC, None, target_topics], "fromBlock": hex(first), "toBlock": hex(last)}])
                merged = {(row["transactionHash"].lower(), row["logIndex"].lower()): row for row in outgoing + incoming}
                logs = sorted(merged.values(), key=lambda row: (int(row["blockNumber"], 16), int(row["logIndex"], 16)))
                _save_range(path, logs); return first, last, logs
            except RpcError as exc:
                error = exc; time.sleep(min(2**attempt, 15))
        raise RuntimeError(f"failed flow range {first}-{last}: {error}")

    total = chunks = 0
    for offset in range(0, len(ranges), args.workers * 4):
        batch = ranges[offset : offset + args.workers * 4]
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            results = [future.result() for future in as_completed([executor.submit(fetch, item) for item in batch])]
        total += sum(len(logs) for _, _, logs in results); chunks += len(results)
        if chunks % 240 == 0 or chunks == len(ranges):
            print(f"Polygon UMA flows: {chunks}/{len(ranges)} chunks, {total} logs", flush=True)
    manifest = {
        "protocol": "Polygon Polymarket UMA token flow evidence", "chain_id": 137,
        "from_block": start, "to_block": end, "chunks": chunks, "logs": total,
        "tokens": TOKENS, "targets": targets,
        "interpretation_guard": "Transfers are reconciliation evidence and are not independently labelled rewards.",
    }
    output = root / "data/manifests/polygon_uma_token_flows_raw.json"; write_json(output, manifest); return output


def collect_missing_receipts(args: argparse.Namespace) -> Path:
    """Preserve receipts for settlement transfers omitted by provider getLogs indexes."""
    root = Path(args.root).resolve(); rpc_url = args.rpc_url or load_env_url(root / ".env", args.rpc_env_key)
    qc_path = data_root(root) / "polygon_uma_request_flow_qc.jsonl"
    rounds_path = data_root(root) / "polygon_uma_request_rounds.jsonl"
    missing_ids = {
        row["oo_request_id"]
        for row in (json.loads(line) for line in qc_path.open(encoding="utf-8"))
        if row.get("settlement_flow_exact") is False
    }
    transactions = {
        row["settlement_tx"]
        for row in (json.loads(line) for line in rounds_path.open(encoding="utf-8"))
        if row["oo_request_id"] in missing_ids and row.get("settlement_tx")
    }

    def receipt(tx: str) -> dict[str, Any]:
        call_rpc = JsonRpc(rpc_url, timeout_seconds=args.timeout); error = None
        for attempt in range(6):
            try:
                result = call_rpc.call("eth_getTransactionReceipt", [tx])
                if result is None:
                    raise RpcError(f"missing receipt for {tx}")
                return result
            except RpcError as exc:
                error = exc; time.sleep(min(2**attempt, 15))
        raise RuntimeError(f"receipt failed {tx}: {error}")

    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(receipt, tx) for tx in sorted(transactions)]
        for index, future in enumerate(as_completed(futures), 1):
            receipts.append(future.result())
            if index % 100 == 0 or index == len(futures):
                print(f"Polygon UMA settlement receipts: {index}/{len(futures)}", flush=True)
    receipts.sort(key=lambda row: (int(row["blockNumber"], 16), int(row["transactionIndex"], 16)))
    raw = root / "data/raw/polygon/uma_token_flows/settlement_receipt_fallback.jsonl.gz"
    temporary = raw.with_suffix(raw.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=9) as handle:
        for row in receipts:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(raw)
    digest = hashlib.sha256(raw.read_bytes()).hexdigest()
    raw.with_suffix(raw.suffix + ".sha256").write_text(digest + "\n", encoding="utf-8")
    manifest = {
        "protocol": "Polygon UMA settlement receipt fallback",
        "reason": "provider eth_getLogs omission for otherwise canonical receipt logs",
        "requested_mismatches": len(missing_ids), "requested_transactions": len(transactions),
        "receipts": len(receipts), "raw_receipts": str(raw), "sha256": digest,
    }
    output = root / "data/manifests/polygon_uma_token_flow_receipts.json"; write_json(output, manifest); return output


def build(root: Path) -> Path:
    root = root.resolve(); curated = data_root(root); raw_dir = root / "data/raw/polygon/uma_token_flows"
    token_role = {address: role for role, address in TOKENS.items()}; by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    output = curated / "polygon_uma_token_flows.jsonl"; temporary = output.with_suffix(output.suffix + ".tmp")
    counts: Counter[str] = Counter()
    seen: set[tuple[str, str]] = set()
    with temporary.open("w", encoding="utf-8") as handle:
        def append_log(log: dict[str, Any], evidence_method: str) -> None:
            key = (log["transactionHash"].lower(), log["logIndex"].lower())
            if key in seen or log["address"].lower() not in token_role or not log.get("topics") or log["topics"][0].lower() != TRANSFER_TOPIC:
                return
            sender = "0x" + log["topics"][1][-40:].lower(); receiver = "0x" + log["topics"][2][-40:].lower()
            row = {
                "token": log["address"].lower(), "token_role": token_role[log["address"].lower()],
                "sender": sender, "receiver": receiver, "amount_raw": str(int(log["data"], 16)),
                "source_tx": log["transactionHash"].lower(), "source_block": int(log["blockNumber"], 16),
                "log_index": int(log["logIndex"], 16), "evidence_method": evidence_method,
            }
            seen.add(key); by_tx[row["source_tx"]].append(row)
            handle.write(json.dumps(row, separators=(",", ":")) + "\n"); counts[row["token_role"]] += 1

        for path in sorted(raw_dir.glob("*.jsonl.gz")):
            if path.name == "settlement_receipt_fallback.jsonl.gz":
                continue
            with gzip.open(path, "rt", encoding="utf-8") as source:
                for line in source:
                    append_log(json.loads(line), "eth_getLogs")
        receipt_path = raw_dir / "settlement_receipt_fallback.jsonl.gz"
        if receipt_path.is_file():
            digest_path = receipt_path.with_suffix(receipt_path.suffix + ".sha256")
            if not digest_path.is_file() or hashlib.sha256(receipt_path.read_bytes()).hexdigest() != digest_path.read_text(encoding="utf-8").strip():
                raise RuntimeError(f"checksum mismatch for receipt fallback: {receipt_path}")
            with gzip.open(receipt_path, "rt", encoding="utf-8") as source:
                for line in source:
                    for log in json.loads(line)["logs"]:
                        append_log(log, "eth_getTransactionReceipt_fallback")
    temporary.replace(output)
    qc_output = curated / "polygon_uma_request_flow_qc.jsonl"; qc_temp = qc_output.with_suffix(qc_output.suffix + ".tmp")
    qc: Counter[str] = Counter()
    oov2 = CONTRACTS["optimistic_oracle_v2"].lower()
    with (curated / "polygon_uma_request_rounds.jsonl").open() as rounds, qc_temp.open("w", encoding="utf-8") as handle:
        for line in rounds:
            row = json.loads(line); result = {"oo_request_id": row["oo_request_id"], "currency": row["currency"], "status": row["status"]}
            if row.get("settlement_tx"):
                winner = row.get("proposer") if row.get("disputer", "0x" + "00" * 20) == "0x" + "00" * 20 or row.get("resolved_price_raw") == row.get("proposed_price_raw") else row.get("disputer")
                transfers = [x for x in by_tx.get(row["settlement_tx"], []) if x["token"] == row["currency"] and x["sender"] == oov2 and x["receiver"] == winner]
                paid = sum(int(x["amount_raw"]) for x in transfers); expected = int(row["gross_payout_raw"])
                result.update(settlement_transfer_raw=str(paid), gross_payout_raw=str(expected), settlement_flow_exact=(paid == expected))
                qc["settlement_exact" if paid == expected else "settlement_mismatch"] += 1
            handle.write(json.dumps(result, separators=(",", ":")) + "\n")
    qc_temp.replace(qc_output)
    manifest = {
        "protocol": "Polygon UMA token flow reconciliation", "flows": sum(counts.values()), "flows_by_token": dict(counts),
        "settlement_flow_qc": dict(qc), "outputs": {"flows": str(output), "request_qc": str(qc_output)},
    }
    manifest_path = root / "data/manifests/polygon_uma_token_flow_ledger.json"; write_json(manifest_path, manifest); return manifest_path


def register_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("ingest-polygon-uma-flows", help="collect Polygon USDC flows for UMA reconciliation")
    parser.add_argument("--rpc-url"); parser.add_argument("--rpc-env-key", default="NODE_URL2")
    parser.add_argument("--root", default="."); parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=24); parser.add_argument("--timeout", type=int, default=120)
    parser.set_defaults(handler=collect)
    receipts = subparsers.add_parser("ingest-polygon-uma-flow-receipts", help="backfill settlement flow evidence from canonical receipts")
    receipts.add_argument("--rpc-url"); receipts.add_argument("--rpc-env-key", default="NODE_URL2")
    receipts.add_argument("--root", default="."); receipts.add_argument("--workers", type=int, default=24)
    receipts.add_argument("--timeout", type=int, default=120); receipts.set_defaults(handler=collect_missing_receipts)
