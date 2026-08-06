"""Fill the retained tail of Tellor report history from archive-capable peers.

The canonical public RPC is the only peer found with genesis-to-head
``block_results`` retention.  Several peers advertised by its live
``net_info`` endpoint retain overlapping recent archive windows, however.
This helper validates those peers against ``tellor-1`` and fills only segments
whose complete height range is available from the selected peer.  It writes
the same receipt-backed raw segments as
``ingest_tellor_reports_block_results.py``; the canonical collector remains
responsible for whole-range coverage QC and final JSONL/Parquet generation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import orjson
import requests

from scripts.ingest_tellor_reports_block_results import (
    ROOT,
    atomic_json,
    collect_segment,
    report_rows,
)


@dataclass(frozen=True)
class ArchivePeer:
    rpc_url: str
    earliest_height: int


DEFAULT_PEERS = (
    ArchivePeer("http://3.91.103.4:26657", 17_216_001),
    ArchivePeer("http://149.50.116.116:47657", 17_706_498),
    ArchivePeer("http://116.202.221.88:26657", 19_040_001),
    ArchivePeer("http://15.204.211.26:26657", 19_104_001),
)


def rpc(session: requests.Session, url: str, method: str, params: Any) -> Any:
    response = session.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("error"):
        raise RuntimeError(f"{url} {method}: {body['error']}")
    return body["result"]


def validate_peer(peer: ArchivePeer, cutoff_height: int) -> dict[str, Any]:
    session = requests.Session()
    session.trust_env = False
    status = rpc(session, peer.rpc_url, "status", {})
    node = status["node_info"]
    sync = status["sync_info"]
    if node["network"] != "tellor-1":
        raise RuntimeError(f"{peer.rpc_url} is not tellor-1")
    if sync["catching_up"]:
        raise RuntimeError(f"{peer.rpc_url} is catching up")
    latest = int(sync["latest_block_height"])
    if latest < cutoff_height:
        raise RuntimeError(
            f"{peer.rpc_url} latest height {latest} is below {cutoff_height}"
        )
    for height in (peer.earliest_height, cutoff_height):
        result = rpc(session, peer.rpc_url, "block_results", {"height": str(height)})
        if int(result.get("height") or 0) != height:
            raise RuntimeError(
                f"{peer.rpc_url} failed archive validation at {height}"
            )
    return {
        "rpc_url": peer.rpc_url,
        "earliest_height": peer.earliest_height,
        "latest_height": latest,
        "moniker": node["moniker"],
    }


def parse_peer(value: str) -> ArchivePeer:
    try:
        url, height = value.rsplit(",", 1)
        return ArchivePeer(url, int(height))
    except (ValueError, TypeError) as exc:
        raise argparse.ArgumentTypeError(
            "peer must be RPC_URL,EARLIEST_RETAINED_HEIGHT"
        ) from exc


def report_fingerprint(
    rpc_url: str,
    heights: list[int],
    cutoff_height: int,
) -> dict[str, Any]:
    session = requests.Session()
    session.trust_env = False
    payload = [
        {
            "jsonrpc": "2.0",
            "id": height,
            "method": "block_results",
            "params": {"height": str(height)},
        }
        for height in heights
    ]
    response = session.post(rpc_url, json=payload, timeout=60)
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, list):
        raise RuntimeError(f"{rpc_url} returned a non-batch cross-check response")
    by_height = {int(item["id"]): item for item in body}
    rows = []
    for height in heights:
        item = by_height[height]
        if item.get("error"):
            raise RuntimeError(f"{rpc_url} cross-check failed: {item['error']}")
        rows.extend(report_rows(height, item["result"], cutoff_height))
    encoded = sorted(
        orjson.dumps(row, option=orjson.OPT_SORT_KEYS) for row in rows
    )
    digest = hashlib.sha256(b"\n".join(encoded)).hexdigest()
    return {
        "rpc_url": rpc_url,
        "heights": [heights[0], heights[-1]],
        "rows": len(rows),
        "sha256": digest,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Accelerate Tellor report tail collection with validated peers"
    )
    parser.add_argument("--cutoff-height", type=int, default=19_890_860)
    parser.add_argument("--segment-heights", type=int, default=2_000)
    parser.add_argument("--per-peer-workers", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument(
        "--canonical-rpc-url",
        default="https://mainnet.tellorlayer.com/rpc",
    )
    parser.add_argument("--peer", action="append", type=parse_peer)
    args = parser.parse_args()
    peers = tuple(args.peer or DEFAULT_PEERS)
    if args.segment_heights <= 0:
        raise SystemExit("--segment-heights must be positive")

    validations = [validate_peer(peer, args.cutoff_height) for peer in peers]
    crosscheck_heights = list(
        range(args.cutoff_height - 9, args.cutoff_height + 1)
    )
    canonical_fingerprint = report_fingerprint(
        args.canonical_rpc_url,
        crosscheck_heights,
        args.cutoff_height,
    )
    peer_fingerprints = [
        report_fingerprint(
            peer.rpc_url,
            crosscheck_heights,
            args.cutoff_height,
        )
        for peer in peers
    ]
    mismatches = [
        row["rpc_url"]
        for row in peer_fingerprints
        if (
            row["rows"] != canonical_fingerprint["rows"]
            or row["sha256"] != canonical_fingerprint["sha256"]
        )
    ]
    if mismatches:
        raise RuntimeError(f"Tellor peer cross-check mismatch: {mismatches}")
    print(json.dumps({"validated_peers": validations}, sort_keys=True), flush=True)

    raw_dir = (
        ROOT / "data/raw/tellor_layer/report_block_events_full"
    ).resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    first_segment = min(
        1
        + (
            (peer.earliest_height - 1 + args.segment_heights - 1)
            // args.segment_heights
        )
        * args.segment_heights
        for peer in peers
    )
    segments = [
        (start, min(start + args.segment_heights - 1, args.cutoff_height))
        for start in range(first_segment, args.cutoff_height + 1, args.segment_heights)
    ]
    # Work downward so the widest overlap is shared first. This also keeps the
    # accelerator far ahead of the canonical ascending collector.
    segments.reverse()

    assignments: list[tuple[int, int, ArchivePeer]] = []
    peer_counts = {peer.rpc_url: 0 for peer in peers}
    for start, end in segments:
        eligible = [peer for peer in peers if peer.earliest_height <= start]
        if not eligible:
            continue
        peer = min(eligible, key=lambda item: peer_counts[item.rpc_url])
        peer_counts[peer.rpc_url] += 1
        assignments.append((start, end, peer))

    completed = 0
    reports = 0
    executors = {
        peer.rpc_url: ThreadPoolExecutor(
            max_workers=max(1, args.per_peer_workers)
        )
        for peer in peers
    }
    try:
        futures = {
            executors[peer.rpc_url].submit(
                collect_segment,
                peer.rpc_url,
                None,
                args.timeout,
                start,
                end,
                args.cutoff_height,
                raw_dir,
            ): (start, end, peer.rpc_url)
            for start, end, peer in assignments
        }
        for future in as_completed(futures):
            receipt = future.result()
            completed += int(receipt["scanned_blocks"])
            reports += int(receipt["reports"])
    finally:
        for executor in executors.values():
            executor.shutdown(wait=True, cancel_futures=True)

    expected_blocks = args.cutoff_height - first_segment + 1
    manifest = {
        "dataset": "Tellor report archive-peer tail acceleration",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "cutoff_height": args.cutoff_height,
        "first_accelerated_height": first_segment,
        "segments": len(assignments),
        "scanned_blocks": completed,
        "expected_blocks": expected_blocks,
        "reports": reports,
        "assignment_counts": peer_counts,
        "validated_peers": validations,
        "canonical_crosscheck": canonical_fingerprint,
        "peer_crosschecks": peer_fingerprints,
        "crosscheck_mismatches": mismatches,
        "raw_directory": str(raw_dir),
        "all_required_assertions_pass": (
            completed == expected_blocks and not mismatches
        ),
        "scope_guard": (
            "Peers were discovered through the canonical node's live peer "
            "information, verified as synchronized tellor-1 nodes, tested at "
            "their earliest declared retained height, and required to match "
            "the canonical RPC's complete new_report-row hash over the same "
            "ten cutoff-adjacent blocks. The canonical collector independently "
            "checks whole-chain receipt coverage and output parity."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor peer acceleration QC failed: {manifest}")
    manifest_path = (
        ROOT / "data/manifests/tellor_report_archive_peer_acceleration.json"
    )
    atomic_json(manifest_path, manifest)
    print(json.dumps(manifest, sort_keys=True), flush=True)
    print(manifest_path, flush=True)


if __name__ == "__main__":
    main()
