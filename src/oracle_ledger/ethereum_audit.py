"""Ethereum VotingV2 feasibility audit.

This module deliberately performs no payoff calculation. It only establishes that
the node, code, deployment range and raw event corpus are usable for later,
rule-governed ingestion. All quantities are Python integers or strings.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from eth_utils import keccak

from .rpc import JsonRpc, RpcError, write_json


CHAIN_ID = 1
VOTING_V2 = "0x004395edb43efca9885cedad51ec9faf93bd34ac"
VOTING_TOKEN = "0x04fa0d235c4abf4bcf4787af4cf447de572ef828"
CUTOFF_UTC = "2026-06-30T23:59:59Z"
WINDOW_START_UTC = "2023-04-01T00:00:00Z"

# Signatures were transcribed from UMAprotocol/protocol VotingV2.sol and
# Staker.sol at the commit recorded in the audit manifest. Querying all logs
# at the contract address additionally preserves unknown-version evidence.
EVENT_SIGNATURES = (
    "RequestAdded(address,uint32,bytes32,uint256,bytes,bool)",
    "VoteCommitted(address,address,uint32,bytes32,uint256,bytes)",
    "EncryptedVote(address,uint32,bytes32,uint256,bytes,bytes)",
    "VoteRevealed(address,address,uint32,bytes32,uint256,bytes,int256,uint128)",
    "RequestResolved(uint32,uint256,bytes32,uint256,bytes,int256)",
    "RequestRolled(bytes32,uint256,bytes,uint32)",
    "RequestDeleted(bytes32,uint256,bytes,uint32)",
    "VoterSlashed(address,uint256,int128)",
    "VoterSlashApplied(address,int128,uint128)",
    "SlashingLibraryChanged(address)",
    "GatAndSpatChanged(uint128,uint64)",
    "MaxRollsChanged(uint32)",
    "MaxRequestsPerRoundChanged(uint32)",
    "Staked(address,address,uint128,uint128,uint128,uint128)",
    "RequestedUnstake(address,uint128,uint64,uint128)",
    "ExecutedUnstake(address,uint128,uint128)",
    "UpdatedReward(address,uint128,uint64)",
    "WithdrawnRewards(address,address,uint128)",
    "SetNewEmissionRate(uint128)",
    "SetNewUnstakeCoolDown(uint64)",
    "DelegateSet(address,address)",
    "DelegatorSet(address,address)",
    "OwnershipTransferred(address,address)",
)
TOPIC_TO_SIGNATURE = {"0x" + keccak(text=signature).hex(): signature for signature in EVENT_SIGNATURES}
VOTER_SLASHED_TOPIC = "0x" + keccak(text="VoterSlashed(address,uint256,int128)").hex()
VOTER_SLASH_APPLIED_TOPIC = "0x" + keccak(text="VoterSlashApplied(address,int128,uint128)").hex()


def hex_quantity(value: int) -> str:
    return hex(value)


def int_quantity(value: str) -> int:
    return int(value, 16)


def topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


def selector(signature: str) -> str:
    return "0x" + keccak(text=signature)[:4].hex()


def utc_timestamp(iso8601: str) -> int:
    return int(datetime.fromisoformat(iso8601.replace("Z", "+00:00")).replace(tzinfo=UTC).timestamp())


def timestamp_to_block(rpc: JsonRpc, target_timestamp: int, head_block: int) -> int:
    """Return the greatest block with timestamp <= target; no float arithmetic."""
    low, high = 0, head_block
    while low < high:
        midpoint = (low + high + 1) // 2
        block = rpc.call("eth_getBlockByNumber", [hex_quantity(midpoint), False])
        if int_quantity(block["timestamp"]) <= target_timestamp:
            low = midpoint
        else:
            high = midpoint - 1
    return low


def deployment_block_by_code(rpc: JsonRpc, address: str, head_block: int) -> int | None:
    """Find first block with runtime code, as mandated by the specification."""
    latest_code = rpc.call("eth_getCode", [address, hex_quantity(head_block)])
    if latest_code == "0x":
        return None
    low, high = 0, head_block
    while low < high:
        midpoint = (low + high) // 2
        code = rpc.call("eth_getCode", [address, hex_quantity(midpoint)])
        if code == "0x":
            low = midpoint + 1
        else:
            high = midpoint
    return low


def signed_abi_word(data_hex: str) -> int:
    raw = bytes.fromhex(data_hex[2:])
    if len(raw) != 32:
        raise ValueError(f"expected one ABI word, received {len(raw)} bytes")
    return int.from_bytes(raw, byteorder="big", signed=True)


def code_hash(code_hex: str) -> str:
    return "0x" + keccak(bytes.fromhex(code_hex[2:])).hex()


def erc20_decimals(rpc: JsonRpc, token: str) -> int:
    result = rpc.call("eth_call", [{"to": token, "data": selector("decimals()")}, "latest"])
    return int_quantity(result)


def erc20_symbol(rpc: JsonRpc, token: str) -> str:
    result = rpc.call("eth_call", [{"to": token, "data": selector("symbol()")}, "latest"])
    raw = bytes.fromhex(result[2:])
    # Standard dynamic string ABI, with bytes32 fallback for unusual ERC-20s.
    if len(raw) == 32:
        return raw.rstrip(b"\x00").decode("utf-8", errors="replace")
    offset = int.from_bytes(raw[:32], "big")
    length = int.from_bytes(raw[offset : offset + 32], "big")
    return raw[offset + 32 : offset + 32 + length].decode("utf-8", errors="replace")


def find_deployment_transaction(rpc: JsonRpc, address: str, deployment_block: int) -> str | None:
    """Use reth trace_block when available; leave unresolved rather than guessing."""
    try:
        traces = rpc.call("trace_block", [hex_quantity(deployment_block)])
    except RpcError:
        return None
    target = address.lower()
    for trace in traces:
        if trace.get("type") != "create":
            continue
        created = trace.get("result", {}).get("address", "").lower()
        if created == target:
            return trace.get("transactionHash")
    return None


@dataclass
class LogScan:
    counts: Counter[str]
    unknown_topic_counts: Counter[str]
    positive_voter_slashed: int
    negative_voter_slashed: int
    zero_voter_slashed: int
    voter_slash_applied: int
    log_count: int
    ranges: int


def scan_logs(
    rpc: JsonRpc,
    address: str,
    from_block: int,
    to_block: int,
    chunk_size: int,
    raw_dir: Path,
) -> LogScan:
    """Fetch address-scoped logs in resumable, content-addressed range files."""
    counts: Counter[str] = Counter()
    unknown_topics: Counter[str] = Counter()
    pos = neg = zero = applied = total = range_count = 0
    raw_dir.mkdir(parents=True, exist_ok=True)
    start = from_block
    while start <= to_block:
        end = min(start + chunk_size - 1, to_block)
        while True:
            output_path = raw_dir / f"votingv2_{start}_{end}.jsonl.gz"
            digest_path = output_path.with_suffix(output_path.suffix + ".sha256")
            if output_path.exists() and digest_path.exists():
                with gzip.open(output_path, "rt", encoding="utf-8") as handle:
                    logs = [json.loads(line) for line in handle]
                break
            try:
                logs = rpc.call(
                    "eth_getLogs",
                    [{"address": address, "fromBlock": hex_quantity(start), "toBlock": hex_quantity(end)}],
                )
            except RpcError as exc:
                # Reth reports a safe retry range on its max-result error. Use it
                # when available; otherwise bisect. This keeps collection resumable
                # across providers with different range/result limits.
                suggested = re.search(r"range (\d+)-(\d+)", str(exc))
                suggested_end = int(suggested.group(2)) if suggested and int(suggested.group(1)) == start else None
                # Some reth versions repeat the displayed retry range even when it
                # is the failing range. In that case bisect rather than failing.
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
        range_count += 1
        total += len(logs)
        for log in logs:
            first_topic = log["topics"][0].lower()
            signature = TOPIC_TO_SIGNATURE.get(first_topic)
            if signature is None:
                unknown_topics[first_topic] += 1
            else:
                counts[signature] += 1
            if first_topic == VOTER_SLASHED_TOPIC:
                value = signed_abi_word(log["data"])
                if value > 0:
                    pos += 1
                elif value < 0:
                    neg += 1
                else:
                    zero += 1
            elif first_topic == VOTER_SLASH_APPLIED_TOPIC:
                applied += 1
        start = end + 1
    return LogScan(counts, unknown_topics, pos, neg, zero, applied, total, range_count)


def markdown_table(rows: Iterable[tuple[str, str]]) -> str:
    rendered = ["| Field | Value |", "|---|---:|"]
    rendered.extend(f"| {key} | {value} |" for key, value in rows)
    return "\n".join(rendered)


def build_audit(args: argparse.Namespace) -> Path:
    root = Path(args.root).resolve()
    rpc = JsonRpc(args.rpc_url, timeout_seconds=args.timeout)
    chain_id = int_quantity(rpc.call("eth_chainId", []))
    if chain_id != CHAIN_ID:
        raise RuntimeError(f"expected Ethereum chain ID {CHAIN_ID}, got {chain_id}")
    head_block = int_quantity(rpc.call("eth_blockNumber", []))
    client_version = rpc.call("web3_clientVersion", [])
    syncing = rpc.call("eth_syncing", [])
    cutoff_block = timestamp_to_block(rpc, utc_timestamp(CUTOFF_UTC), head_block)
    start_block = timestamp_to_block(rpc, utc_timestamp(WINDOW_START_UTC), head_block)
    deployment_block = deployment_block_by_code(rpc, VOTING_V2, head_block)
    if deployment_block is None:
        raise RuntimeError("VotingV2 has no code at chain head")
    deployment_tx = find_deployment_transaction(rpc, VOTING_V2, deployment_block)
    code_at_cutoff = rpc.call("eth_getCode", [VOTING_V2, hex_quantity(cutoff_block)])
    token_code = rpc.call("eth_getCode", [VOTING_TOKEN, hex_quantity(cutoff_block)])
    scan_end = min(cutoff_block, head_block)
    scan = scan_logs(
        rpc,
        VOTING_V2,
        deployment_block,
        scan_end,
        args.chunk_size,
        root / "data/raw/ethereum/logs",
    )
    decimals = erc20_decimals(rpc, VOTING_TOKEN)
    symbol = erc20_symbol(rpc, VOTING_TOKEN)
    manifest = {
        "audit_version": "0.1.0",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rpc": {"url": args.rpc_url, "client_version": client_version, "syncing": syncing},
        "ethereum": {
            "chain_id": chain_id,
            "head_block": head_block,
            "window_start_block": start_block,
            "cutoff_block": cutoff_block,
            "voting_v2": {
                "address": VOTING_V2,
                "deployment_block": deployment_block,
                "deployment_tx_hash": deployment_tx,
                "runtime_code_hash_at_cutoff": code_hash(code_at_cutoff),
                "runtime_code_bytes_at_cutoff": (len(code_at_cutoff) - 2) // 2,
            },
            "voting_token": {
                "address": VOTING_TOKEN,
                "runtime_code_hash_at_cutoff": code_hash(token_code),
                "decimals": decimals,
                "symbol": symbol,
            },
            "logs": {
                "from_block": deployment_block,
                "to_block": scan_end,
                "chunks": scan.ranges,
                "total": scan.log_count,
                "by_signature": dict(sorted(scan.counts.items())),
                "unknown_topic_counts": dict(sorted(scan.unknown_topic_counts.items())),
                "voter_slashed": {
                    "positive_reward_events": scan.positive_voter_slashed,
                    "negative_penalty_events": scan.negative_voter_slashed,
                    "zero_events": scan.zero_voter_slashed,
                    "slash_applied_events": scan.voter_slash_applied,
                },
            },
        },
        "semantic_verification": {
            "configured_source": "UMAprotocol/protocol VotingV2.sol and Staker.sol",
            "source_commit": "a16ee53125c433dfa4e29738b73d9069ff109c03",
            "status": "event_abi_compatible_only",
            "blocking_issue": "Exact deployed-source/bytecode semantic equivalence has not yet been independently reproduced; no payoff ingestion is authorised.",
        },
        "scope_limitations": [
            "No POLYGON_ARCHIVE_RPC_URL or POLYGON_VALIDATION_RPC_URL was configured, so Polygon contracts, OOV2 rewards/bonds, and Grade A cross-chain links were not audited.",
            "No Ethereum validation RPC was configured, so independent-provider sampling is pending.",
            "The audit stores raw Ethereum logs but does not derive payoffs, rewards, penalties, or DVM matches.",
        ],
    }
    manifest_path = root / "data/manifests/ethereum_feasibility_audit.json"
    write_json(manifest_path, manifest)
    estimated_calls = scan.ranges + 2 * 25 + 8
    report = f"""# Feasibility audit — Ethereum UMA VotingV2

Generated from the local Ethereum archive node on {manifest['generated_at_utc']}. This is an audit-first collection: it contains no payoff ledger and makes no `Settle.payout`-to-reward inference.

## Node and scope

{markdown_table([
    ('RPC client', client_version),
    ('Chain ID', str(chain_id)),
    ('Node syncing', str(syncing).lower()),
    ('Head block at audit', str(head_block)),
    ('Primary-window start block (2023-04-01)', str(start_block)),
    ('Fixed cutoff block (2026-06-30 23:59:59 UTC)', str(cutoff_block)),
    ('Raw log scan range', f'{deployment_block}–{scan_end}'),
])}

## Contract and token verification

{markdown_table([
    ('VotingV2 address', VOTING_V2),
    ('VotingV2 deployment block (first non-empty code)', str(deployment_block)),
    ('VotingV2 deployment transaction', deployment_tx or 'unresolved — trace evidence did not identify a direct create'),
    ('VotingV2 runtime code hash at cutoff', code_hash(code_at_cutoff)),
    ('VotingV2 runtime code bytes at cutoff', str((len(code_at_cutoff)-2)//2)),
    ('UMA voting token', f'{symbol} — {VOTING_TOKEN}'),
    ('UMA token decimals', str(decimals)),
    ('UMA token runtime code hash at cutoff', code_hash(token_code)),
])}

## Raw event corpus

All logs at the VotingV2 address were fetched in {scan.ranges} resumable chunks. Each chunk is preserved as gzipped JSONL under `data/raw/ethereum/logs/` with a SHA-256 sidecar. Unknown topics are preserved and reported rather than discarded.

{markdown_table([(signature, str(scan.counts.get(signature, 0))) for signature in EVENT_SIGNATURES] + [('Unknown event topics', str(sum(scan.unknown_topic_counts.values()))), ('All address-scoped logs', str(scan.log_count))])}

## Signed `VoterSlashed` classification

Positive signed values are classified as correct-vote redistribution rewards; negative values are classified as voter penalties. These are event counts only — raw signed amounts have not been aggregated and `VoterSlashApplied` is not summed with `VoterSlashed`.

{markdown_table([
    ('Positive VoterSlashed (reward) events', str(scan.positive_voter_slashed)),
    ('Negative VoterSlashed (penalty) events', str(scan.negative_voter_slashed)),
    ('Zero VoterSlashed events', str(scan.zero_voter_slashed)),
    ('VoterSlashApplied events (reconciliation-only)', str(scan.voter_slash_applied)),
])}

## Reward, bond, dispute, settlement, and exact DVM-link status

The local node covers only Ethereum in this audit. Request rewards, proposal bonds, OOV2 disputes/settlements, reward-token distributions, and exact Polygon–Ethereum links require the Polygon archive corpus. They are intentionally **not inferred** from Ethereum voting events.

| Required audit item | Status |
|---|---|
| Reward token types / decimals | UMA voting token found: {symbol}, {decimals} decimals; Polygon OOV2 currencies pending |
| Request reward distribution | Pending Polygon OOV2 `RequestPrice` logs |
| Proposal-bond distribution | Pending Polygon adapter/OOV2 historical state |
| Dispute and settlement counts | Pending Polygon OOV2 logs |
| Grade A cross-chain links | Pending Polygon ChildTunnel + exact ancillary bytes/hash matching |
| DVM request count eligible for primary dataset | Pending; must be based only on Grade A links |

## RPC volume and version findings

The completed Ethereum pass used approximately {estimated_calls} RPC requests ({scan.ranges} `eth_getLogs` calls plus block/code/token/trace probes). A full Ethereum DVM pass will additionally need transaction/receipt evidence and historical calls for linked requests. The current event ABI matches the configured UMA source signatures, but exact deployed-source bytecode/semantic equivalence is still unresolved because the compiler metadata source bundle was not independently recovered. Under the specification this is a stop condition for economic ingestion of this contract version.

## Recommendation

**Do not start full economic-ledger ingestion yet.** The Ethereum raw-event feasibility is positive, but the audit is incomplete without Polygon archive and validation RPC endpoints, exact source/bytecode verification, and Grade A bridge matching. Continue with read-only Ethereum collection and verification only; no rewards or slashes should be published from this audit.
"""
    report_path = root / "reports/feasibility_audit.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")
    return report_path


def register_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("audit-ethereum", help="collect and audit Ethereum VotingV2 raw logs")
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8545")
    parser.add_argument("--root", default=".")
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--timeout", type=int, default=120)
    parser.set_defaults(handler=build_audit)
