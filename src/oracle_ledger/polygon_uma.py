"""Resumable Polygon collection for Polymarket adapters, UMA OOV2 and ChildTunnel."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from eth_utils import keccak

from .ethereum_audit import CUTOFF_UTC, code_hash, deployment_block_by_code, int_quantity, timestamp_to_block, utc_timestamp
from .rpc import JsonRpc, RpcError, write_json


CHAIN_ID = 137
CONTRACTS = {
    "adapter_v1_0": "0xcb1822859cef82cd2eb4e6276c7916e692995130",
    "adapter_v1_0_1": "0xb97455fcf78eb37375e8be6f26df895341ca073d",
    "adapter_v2_0": "0x6a9d222616c90fca5754cd1333cfd9b7fb6a4f74",
    "adapter_v3_historical": "0x71392e133063cc0d16f40e1f9b60227404bc03f7",
    "adapter_v3_current": "0x157ce2d672854c848c9b79c49a8cc6cc89176a49",
    "optimistic_oracle_v2": "0xee3afe347d5c74317041e2618c49534daf887c24",
    "oracle_child_tunnel": "0xac60353a54873c446101216829a6a98cdbbc3f3d",
}
HISTORICAL_CHILD_TUNNELS = {
    "oracle_child_tunnel_legacy": "0xbed4c1fc0fd95a2020ec351379b22d8582b904e3",
}

EVENT_SIGNATURES = (
    # Polymarket adapter, current official interface. Unknown legacy topics are retained.
    "QuestionInitialized(bytes32,uint256,address,bytes,address,uint256,uint256)",
    "QuestionPaused(bytes32)",
    "QuestionUnpaused(bytes32)",
    "QuestionFlagged(bytes32)",
    "QuestionUnflagged(bytes32)",
    "QuestionReset(bytes32)",
    "QuestionResolved(bytes32,int256,uint256[])",
    "QuestionManuallyResolved(bytes32,uint256[])",
    "QuestionEmergencyResolved(bytes32,uint256[])",
    "AncillaryDataUpdated(bytes32,address,bytes)",
    # Legacy Polymarket adapter v1.0/v1.0.1 interfaces.
    "QuestionInitialized(bytes32,bytes,uint256,address,uint256,uint256,bool)",
    "QuestionUpdated(bytes32,bytes,uint256,address,uint256,uint256,bool)",
    "ResolutionDataRequested(address,uint256,bytes32,bytes32,bytes,address,uint256,uint256,bool)",
    "QuestionSettled(bytes32,int256,bool)",
    "QuestionResolved(bytes32,bool)",
    "QuestionFlaggedForAdminResolution(bytes32)",
    "NewFinderAddress(address,address)",
    "AuthorizedUser(address)",
    "DeauthorizedUser(address)",
    "UnauthorizedUser(address)",
    # UMA OptimisticOracleV2 official interface.
    "RequestPrice(address,bytes32,uint256,bytes,address,uint256,uint256)",
    "ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,address)",
    "DisputePrice(address,address,address,bytes32,uint256,bytes,int256)",
    "Settle(address,address,address,bytes32,uint256,bytes,int256,uint256)",
    # UMA Polygon cross-chain OracleBaseTunnel.
    "PriceRequestAdded(bytes32,uint256,bytes,bytes32)",
    "PushedPrice(bytes32,uint256,bytes,int256,bytes32)",
    "PriceRequestBridged(address,bytes32,uint256,bytes,bytes32,bytes32)",
    "ResolvedLegacyRequest(bytes32,uint256,bytes,int256,bytes32,bytes32)",
    "MessageSent(bytes)",
)
TOPIC_TO_SIGNATURE = {"0x" + keccak(text=signature).hex(): signature for signature in EVENT_SIGNATURES}


def load_env_url(path: Path, key: str) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        if name.strip() == key:
            return value.strip().strip('"').strip("'")
    raise RuntimeError(f"{key} is not set in {path}")


def _load_cached_range(path: Path) -> list[dict[str, Any]] | None:
    digest_path = path.with_suffix(path.suffix + ".sha256")
    if not path.is_file() or not digest_path.is_file():
        return None
    expected = digest_path.read_text(encoding="utf-8").strip()
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise RuntimeError(f"checksum mismatch for cached raw range: {path}")
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def _save_range(path: Path, logs: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=9) as handle:
        for log in logs:
            handle.write(json.dumps(log, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)
    path.with_suffix(path.suffix + ".sha256").write_text(
        hashlib.sha256(path.read_bytes()).hexdigest() + "\n", encoding="utf-8"
    )


def scan_logs(
    rpc: JsonRpc,
    from_block: int,
    to_block: int,
    chunk_size: int,
    raw_dir: Path,
    workers: int,
    contracts: dict[str, str] | None = None,
) -> dict[str, Any]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    contracts = contracts or CONTRACTS
    roles = {address.lower(): role for role, address in contracts.items()}
    event_counts: Counter[str] = Counter()
    unknown_topics: Counter[str] = Counter()
    by_role: dict[str, Counter[str]] = defaultdict(Counter)
    total_logs = chunks = 0
    ranges = [(start, min(start + chunk_size - 1, to_block)) for start in range(from_block, to_block + 1, chunk_size)]

    def fetch(block_range: tuple[int, int]) -> tuple[int, int, list[dict[str, Any]]]:
        start, end = block_range
        path = raw_dir / f"polygon_uma_{start}_{end}.jsonl.gz"
        cached = _load_cached_range(path)
        if cached is not None:
            return start, end, cached
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                logs = rpc.call(
                    "eth_getLogs",
                    [{"address": list(contracts.values()), "fromBlock": hex(start), "toBlock": hex(end)}],
                )
            except RpcError as exc:
                last_error = exc
                if "Block range limit exceeded" in str(exc):
                    raise RuntimeError(f"provider rejected configured chunk size {chunk_size}; use <= 10000") from exc
                time.sleep(min(2**attempt, 15))
                continue
            _save_range(path, logs)
            return start, end, logs
        raise RuntimeError(f"failed Polygon log range {start}-{end} after retries: {last_error}")

    # Process bounded batches: enough concurrency to amortize provider latency,
    # without retaining the complete raw corpus in memory.
    batch_size = max(workers * 4, 1)
    completed = 0
    for batch_start in range(0, len(ranges), batch_size):
        batch = ranges[batch_start : batch_start + batch_size]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(fetch, block_range) for block_range in batch]
            results = [future.result() for future in as_completed(futures)]
        for start, end, logs in sorted(results):
            chunks += 1
            total_logs += len(logs)
            for log in logs:
                role = roles.get(log["address"].lower(), "unknown_address")
                signature = TOPIC_TO_SIGNATURE.get(log["topics"][0].lower())
                if signature is None:
                    unknown_topics[log["topics"][0].lower()] += 1
                    by_role[role]["unclassified_topic"] += 1
                else:
                    event = signature.split("(", 1)[0]
                    event_counts[event] += 1
                    by_role[role][event] += 1
            completed += 1
        if completed % 25 == 0 or completed == len(ranges):
            last_end = max(end for _, end, _ in results)
            print(f"polygon UMA scan: {last_end}/{to_block}, chunks={completed}/{len(ranges)}, logs={total_logs}", flush=True)
    return {
        "from_block": from_block,
        "to_block": to_block,
        "chunks": chunks,
        "total_logs": total_logs,
        "event_counts": dict(sorted(event_counts.items())),
        "event_counts_by_contract": {role: dict(sorted(counts.items())) for role, counts in sorted(by_role.items())},
        "unknown_topic_counts": dict(sorted(unknown_topics.items())),
    }


def collect(args: argparse.Namespace) -> Path:
    root = Path(args.root).resolve()
    rpc_url = args.rpc_url or load_env_url(root / ".env", args.rpc_env_key)
    rpc = JsonRpc(rpc_url, timeout_seconds=args.timeout)
    chain_id = int_quantity(rpc.call("eth_chainId", []))
    if chain_id != CHAIN_ID:
        raise RuntimeError(f"expected Polygon chain ID {CHAIN_ID}, got {chain_id}")
    head = int_quantity(rpc.call("eth_blockNumber", []))
    cutoff = timestamp_to_block(rpc, utc_timestamp(CUTOFF_UTC), head)
    evidence: dict[str, dict[str, Any]] = {}
    deployments: list[int] = []
    def contract_evidence(item: tuple[str, str]) -> tuple[str, dict[str, Any], int | None]:
        role, address = item
        contract_rpc = JsonRpc(rpc_url, timeout_seconds=args.timeout)
        deployment = deployment_block_by_code(contract_rpc, address, head)
        if deployment is None:
            return role, {"address": address, "status": "no_code_at_head"}, None
        code = contract_rpc.call("eth_getCode", [address, hex(cutoff)])
        return role, {
            "address": address,
            "deployment_block": deployment,
            "runtime_code_hash_at_cutoff": code_hash(code) if code != "0x" else None,
            "runtime_code_bytes_at_cutoff": (len(code) - 2) // 2,
        }, deployment

    with ThreadPoolExecutor(max_workers=min(args.workers, len(CONTRACTS))) as executor:
        futures = [executor.submit(contract_evidence, item) for item in CONTRACTS.items()]
        for future in as_completed(futures):
            role, item_evidence, deployment = future.result()
            evidence[role] = item_evidence
            if deployment is not None:
                deployments.append(deployment)
            print(
                f"{role}: deployment={deployment}, cutoff_code_bytes={item_evidence.get('runtime_code_bytes_at_cutoff', 0)}",
                flush=True,
            )
    if not deployments:
        raise RuntimeError("none of the configured Polygon contracts has bytecode")
    scan = scan_logs(
        rpc,
        min(deployments),
        min(cutoff, head),
        args.chunk_size,
        root / "data/raw/polygon/uma",
        args.workers,
    )
    manifest = {
        "protocol": "Polymarket UMA / OptimisticOracleV2",
        "chain_id": CHAIN_ID,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rpc_client": rpc.call("web3_clientVersion", []),
        "head_block": head,
        "cutoff_block": cutoff,
        "contracts": evidence,
        "raw_log_scan": scan,
        "source_commits": {
            "Polymarket/uma-ctf-adapter": "8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc",
            "UMAprotocol/protocol": "a16ee53125c433dfa4e29738b73d9069ff109c03",
        },
        "interpretation_guard": "Raw address-scoped logs only; no payout is labelled as reward at this stage.",
    }
    output = root / "data/manifests/polygon_uma_raw.json"
    write_json(output, manifest)
    return output


def collect_legacy(args: argparse.Namespace) -> Path:
    root = Path(args.root).resolve()
    rpc_url = args.rpc_url or load_env_url(root / ".env", args.rpc_env_key)
    rpc = JsonRpc(rpc_url, timeout_seconds=args.timeout)
    chain_id = int_quantity(rpc.call("eth_chainId", []))
    if chain_id != CHAIN_ID:
        raise RuntimeError(f"expected Polygon chain ID {CHAIN_ID}, got {chain_id}")
    head = int_quantity(rpc.call("eth_blockNumber", []))
    cutoff = timestamp_to_block(rpc, utc_timestamp(CUTOFF_UTC), head)
    address = HISTORICAL_CHILD_TUNNELS["oracle_child_tunnel_legacy"]
    deployment = deployment_block_by_code(rpc, address, head)
    if deployment is None:
        raise RuntimeError(f"legacy ChildTunnel has no code at head: {address}")
    code = rpc.call("eth_getCode", [address, hex(cutoff)])
    scan = scan_logs(
        rpc, deployment, min(cutoff, head), args.chunk_size,
        root / "data/raw/polygon/uma_legacy_child_tunnel", args.workers,
        HISTORICAL_CHILD_TUNNELS,
    )
    manifest = {
        "protocol": "UMA Polygon historical ChildTunnel", "chain_id": CHAIN_ID,
        "generated_at_utc": datetime.now(UTC).isoformat(), "head_block": head, "cutoff_block": cutoff,
        "contract": {
            "role": "oracle_child_tunnel_legacy", "address": address, "deployment_block": deployment,
            "runtime_code_hash_at_cutoff": code_hash(code), "runtime_code_bytes_at_cutoff": (len(code) - 2) // 2,
        },
        "raw_log_scan": scan,
        "discovery_manifest": "data/manifests/polygon_uma_bridge_discovery.json",
    }
    output = root / "data/manifests/polygon_uma_legacy_child_tunnel_raw.json"
    write_json(output, manifest); return output


def register_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("ingest-polygon-uma", help="collect Polygon Polymarket adapter, OOV2 and ChildTunnel logs")
    parser.add_argument("--rpc-url")
    parser.add_argument("--rpc-env-key", default="NODE_URL2")
    parser.add_argument("--root", default=".")
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=120)
    parser.set_defaults(handler=collect)
    legacy = subparsers.add_parser("ingest-polygon-uma-legacy-bridge", help="collect the discovered historical Polygon ChildTunnel")
    legacy.add_argument("--rpc-url")
    legacy.add_argument("--rpc-env-key", default="NODE_URL2")
    legacy.add_argument("--root", default=".")
    legacy.add_argument("--chunk-size", type=int, default=10_000)
    legacy.add_argument("--workers", type=int, default=24)
    legacy.add_argument("--timeout", type=int, default=120)
    legacy.set_defaults(handler=collect_legacy)
