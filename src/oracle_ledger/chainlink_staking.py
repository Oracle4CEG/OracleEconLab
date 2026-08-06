"""Raw, resumable Chainlink Staking v0.2 collection from an Ethereum archive node.

The collector intentionally records protocol events and does not label a LINK
transfer as an oracle reward. Reward and slashing interpretation remains a
separate, rule-governed stage.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from eth_utils import keccak

from .ethereum_audit import CUTOFF_UTC, code_hash, deployment_block_by_code, int_quantity, timestamp_to_block, utc_timestamp
from .rpc import JsonRpc, RpcError, write_json


# Address configuration retrieved from the official staking.chain.link mainnet
# application configuration. Each address is revalidated on the local node.
CONTRACTS = {
    "reward_vault": "0x996913c8c08472f584ab8834e925b06d0eb1d813",
    "operator_pool": "0xa1d76a7ca72128541e9fcacafbd a3a92ef94fdc5".replace(" ", ""),
    "community_pool": "0xbc10f2e862ed4502144c7d632a3459f49dfcdb5e",
    "price_feed_alert_controller": "0x27484ba119d12649be2a9854e4d3b44cc3fdbad7",
}

# Canonical event signatures from the official v0.2 front-end ABI. They are
# used only for event decoding/counting; all original topics/data are retained.
EVENT_SIGNATURES = (
    "Staked(address,uint256,uint256)",
    "Staked(address,uint256,uint256,uint256)",
    "Unstaked(address,uint256,uint256)",
    "Unstaked(address,uint256,uint256,uint256)",
    "UnbondingPeriodStarted(address)",
    "UnbondingPeriodReset(address)",
    "StakerMigrated(address,uint256,bytes)",
    "Slashed(address,uint256,uint256,uint256)",
    "SlasherConfigSet(address,uint256,uint256)",
    "StakerRewardUpdated(address,uint256,uint256,uint256,uint256,uint256)",
    "CommunityPoolRewardUpdated(uint256)",
    "OperatorPoolRewardUpdated(uint256,uint256)",
    "RewardAdded(address,uint256,uint256)",
    "RewardClaimed(address,uint256)",
    "RewardFinalized(address,bool)",
    "ForfeitedRewardDistributed(uint256,uint256,uint256,bool)",
    "AlerterRewardDeposited(uint256,uint256)",
    "AlerterRewardWithdrawn(uint256,uint256)",
    "AlertingRewardPaid(address,uint256,uint256)",
    "AlertRaised(address,uint256,uint256)",
    "FeedConfigSet(address,uint32,uint32,uint96,uint96)",
)
TOPIC_TO_SIGNATURE = {"0x" + keccak(text=signature).hex(): signature for signature in EVENT_SIGNATURES}


def hex_quantity(value: int) -> str:
    return hex(value)


def scan_logs(
    rpc: JsonRpc,
    from_block: int,
    to_block: int,
    chunk_size: int,
    raw_dir: Path,
) -> tuple[Counter[str], dict[str, Counter[str]], Counter[str], int, int]:
    """Collect logs for all v0.2 contracts, adapting to RPC result limits."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    contracts_by_address = {address.lower(): role for role, address in CONTRACTS.items()}
    total_by_event: Counter[str] = Counter()
    by_contract: dict[str, Counter[str]] = defaultdict(Counter)
    unknown_topics: Counter[str] = Counter()
    total_logs = chunks = 0
    start = from_block
    while start <= to_block:
        end = min(start + chunk_size - 1, to_block)
        while True:
            output_path = raw_dir / f"chainlink_staking_v02_{start}_{end}.jsonl.gz"
            digest_path = output_path.with_suffix(output_path.suffix + ".sha256")
            if output_path.exists() and digest_path.exists():
                with gzip.open(output_path, "rt", encoding="utf-8") as handle:
                    logs = [json.loads(line) for line in handle]
                break
            try:
                logs = rpc.call(
                    "eth_getLogs",
                    [{"address": list(CONTRACTS.values()), "fromBlock": hex_quantity(start), "toBlock": hex_quantity(end)}],
                )
            except RpcError as exc:
                suggested = re.search(r"range (\d+)-(\d+)", str(exc))
                suggested_end = int(suggested.group(2)) if suggested and int(suggested.group(1)) == start else None
                smaller_end = suggested_end if suggested_end is not None and suggested_end < end else start + (end - start) // 2
                if smaller_end < start or smaller_end >= end:
                    raise
                end = smaller_end
                continue
            temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
            with gzip.open(temporary_path, "wt", encoding="utf-8", compresslevel=9) as handle:
                for log in logs:
                    handle.write(json.dumps(log, sort_keys=True, separators=(",", ":")) + "\n")
            temporary_path.replace(output_path)
            digest_path.write_text(hashlib.sha256(output_path.read_bytes()).hexdigest() + "\n", encoding="utf-8")
            break
        chunks += 1
        total_logs += len(logs)
        for log in logs:
            role = contracts_by_address[log["address"].lower()]
            first_topic = log["topics"][0].lower()
            signature = TOPIC_TO_SIGNATURE.get(first_topic)
            if signature is None:
                unknown_topics[first_topic] += 1
                by_contract[role]["unclassified_topic"] += 1
            else:
                total_by_event[signature] += 1
                by_contract[role][signature] += 1
        start = end + 1
    return total_by_event, by_contract, unknown_topics, total_logs, chunks


def collect(args: argparse.Namespace) -> Path:
    root = Path(args.root).resolve()
    rpc = JsonRpc(args.rpc_url, timeout_seconds=args.timeout)
    chain_id = int_quantity(rpc.call("eth_chainId", []))
    if chain_id != 1:
        raise RuntimeError(f"expected Ethereum chain ID 1, got {chain_id}")
    head = int_quantity(rpc.call("eth_blockNumber", []))
    cutoff = timestamp_to_block(rpc, utc_timestamp(CUTOFF_UTC), head)
    deployments: dict[str, int] = {}
    contract_evidence: dict[str, dict[str, Any]] = {}
    for role, address in CONTRACTS.items():
        deployment = deployment_block_by_code(rpc, address, head)
        if deployment is None:
            raise RuntimeError(f"{role} has no bytecode at head: {address}")
        code = rpc.call("eth_getCode", [address, hex_quantity(cutoff)])
        deployments[role] = deployment
        contract_evidence[role] = {
            "address": address,
            "deployment_block": deployment,
            "runtime_code_hash_at_cutoff": code_hash(code),
            "runtime_code_bytes_at_cutoff": (len(code) - 2) // 2,
        }
    counts, by_contract, unknown_topics, total_logs, chunks = scan_logs(
        rpc,
        min(deployments.values()),
        min(cutoff, head),
        args.chunk_size,
        root / "data/raw/ethereum/chainlink_staking_v02",
    )
    manifest = {
        "protocol": "Chainlink Staking v0.2",
        "chain_id": 1,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rpc_client": rpc.call("web3_clientVersion", []),
        "head_block": head,
        "cutoff_block": cutoff,
        "contracts": contract_evidence,
        "raw_log_scan": {
            "from_block": min(deployments.values()),
            "to_block": min(cutoff, head),
            "chunks": chunks,
            "total_logs": total_logs,
            "event_counts": dict(sorted(counts.items())),
            "event_counts_by_contract": {role: dict(sorted(events.items())) for role, events in sorted(by_contract.items())},
            "unknown_topic_counts": dict(sorted(unknown_topics.items())),
        },
        "source": {
            "address_registry": "https://staking.chain.link/ official mainnet application configuration",
            "event_abi": "https://staking.chain.link/ official v0.2 front-end ABI",
        },
        "interpretation_guard": "Raw logs only. LINK transfers and claimed rewards are not automatically honesty-linked rewards.",
    }
    manifest_path = root / "data/manifests/chainlink_staking_v02_raw.json"
    write_json(manifest_path, manifest)
    return manifest_path


def register_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("ingest-chainlink-staking-v02", help="collect raw Chainlink Staking v0.2 Ethereum logs")
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8545")
    parser.add_argument("--root", default=".")
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--timeout", type=int, default=120)
    parser.set_defaults(handler=collect)
