"""Collect canonical Flare RewardClaimed logs from an archive JSON-RPC.

The official Flare RPC limits ``eth_getLogs`` to 30 blocks.  Ankr's public
archive endpoint permits deterministic 1,000-block windows and returns
``blockTimestamp`` on every log.  This collector writes the same 500,000-block
raw segment layout consumed by ``ingest_flare_claims_chill.py`` so the RPC
result can be used as the completeness source instead of relying on
best-effort Explorer pages.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

import requests

from oracle_ledger.flare_events import REWARD_CLAIMED_TOPIC, log_key


ROOT = Path(__file__).resolve().parents[1]
REWARD_MANAGER = "0xC8f55c5aA2C752eE285Bd872855C749f4ee6239B"
# RewardManager deployment and the last Flare block whose timestamp is at or
# before the fixed 2026-06-30T23:59:59Z cutoff.  The cutoff block is also
# resolved again by the final decoder/QC from the public Flare RPC.
DEFAULT_START_BLOCK = 29_549_020
DEFAULT_END_BLOCK = 64_054_789
DEFAULT_ENDPOINTS = (
    "https://flare.public-rpc.com",
    "https://rpc.ankr.com/flare",
)
PRINT_LOCK = Lock()


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


class RpcClient:
    def __init__(self, endpoints: list[str], timeout: int = 120) -> None:
        self.endpoints = endpoints
        self.timeout = timeout
        self.sessions: dict[str, requests.Session] = {}

    def _session(self, endpoint: str) -> requests.Session:
        direct = endpoint.startswith("direct+")
        url = endpoint.removeprefix("direct+")
        session = self.sessions.get(endpoint)
        if session is None:
            session = requests.Session()
            session.trust_env = not direct
            session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"
            self.sessions[endpoint] = session
        return session

    def logs(self, start: int, end: int, attempt_seed: int) -> list[dict[str, Any]]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": hex(start),
                "toBlock": hex(end),
                "address": REWARD_MANAGER,
                "topics": [REWARD_CLAIMED_TOPIC],
            }],
        }
        last_error: Exception | None = None
        for attempt in range(12):
            endpoint = self.endpoints[(attempt_seed + attempt) % len(self.endpoints)]
            url = endpoint.removeprefix("direct+")
            try:
                response = self._session(endpoint).post(
                    url,
                    json=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    raise RuntimeError(str(body["error"]))
                rows = body.get("result")
                if not isinstance(rows, list):
                    raise RuntimeError(f"invalid JSON-RPC response: {body}")
                return rows
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt == 11:
                    break
                time.sleep(min(0.5 * 2 ** min(attempt, 5), 15))
        raise RuntimeError(f"Flare RPC failed for blocks {start}-{end}") from last_error


def normalize_log(row: dict[str, Any], start: int, end: int) -> dict[str, Any]:
    block = int(row["blockNumber"], 16)
    if not start <= block <= end:
        raise RuntimeError(f"RPC returned out-of-range log {block} for {start}-{end}")
    if str(row["address"]).lower() != REWARD_MANAGER.lower():
        raise RuntimeError("RPC address filter mismatch")
    if not row.get("topics") or str(row["topics"][0]).lower() != REWARD_CLAIMED_TOPIC:
        raise RuntimeError("RPC topic filter mismatch")
    if row.get("removed") not in (False, None):
        raise RuntimeError("RPC returned a removed log for a finalized range")
    timestamp = row.get("blockTimestamp")
    if not isinstance(timestamp, str):
        raise RuntimeError("archive RPC omitted blockTimestamp")
    normalized = dict(row)
    # The existing decoder consumes the Etherscan/Blockscout spelling.
    normalized["timeStamp"] = timestamp
    return normalized


def collect_segment(
    endpoints: list[str],
    start: int,
    end: int,
    rpc_span: int,
    directory: Path,
) -> dict[str, Any]:
    name = f"{start:08d}_{end:08d}"
    target = directory / f"{name}.jsonl.gz"
    receipt_path = directory / f"{name}.done.json"
    if target.is_file() and receipt_path.is_file():
        prior = json.loads(receipt_path.read_text(encoding="utf-8"))
        if (
            prior.get("complete")
            and prior.get("start_block") == start
            and prior.get("end_block") == end
            and prior.get("rpc_span") == rpc_span
        ):
            return prior

    temporary = target.with_suffix(target.suffix + ".tmp")
    client = RpcClient(endpoints)
    digest = hashlib.sha256()
    count = 0
    requests_made = 0
    previous_key: tuple[int, int, int, str] | None = None
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        for sub_start in range(start, end + 1, rpc_span):
            sub_end = min(sub_start + rpc_span - 1, end)
            rows = [
                normalize_log(row, sub_start, sub_end)
                for row in client.logs(sub_start, sub_end, sub_start)
            ]
            rows.sort(key=log_key)
            requests_made += 1
            for row in rows:
                key = log_key(row)
                if previous_key is not None and key <= previous_key:
                    raise RuntimeError(f"duplicate/non-monotonic RPC log in segment {name}")
                line = json.dumps(
                    row,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ) + "\n"
                handle.write(line)
                digest.update(line.encode())
                previous_key = key
                count += 1
    temporary.replace(target)
    receipt = {
        "complete": True,
        "source": "Flare archive JSON-RPC eth_getLogs",
        "endpoints": [value.removeprefix("direct+") for value in endpoints],
        "start_block": start,
        "end_block": end,
        "rpc_span": rpc_span,
        "requests": requests_made,
        "rows": count,
        "sha256_uncompressed_jsonl": digest.hexdigest(),
        "raw_file": str(target),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(receipt_path, receipt)
    with PRINT_LOCK:
        print(f"Flare canonical RPC segment {start}-{end}: {count:,} logs", flush=True)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect canonical Flare RewardClaimed RPC logs")
    parser.add_argument("--start-block", type=int, default=DEFAULT_START_BLOCK)
    parser.add_argument("--end-block", type=int, default=DEFAULT_END_BLOCK)
    parser.add_argument("--segment-blocks", type=int, default=500_000)
    parser.add_argument("--rpc-span", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--rpc-url", action="append", default=[])
    parser.add_argument("--raw-subdir", default="reward_claimed_rpc")
    args = parser.parse_args()

    if args.start_block > args.end_block:
        raise ValueError("start block exceeds end block")
    endpoints = args.rpc_url or [
        *DEFAULT_ENDPOINTS,
        *(f"direct+{value}" for value in DEFAULT_ENDPOINTS),
    ]
    directory = (
        ROOT / "data/raw/flare_fsp/onchain_events" / args.raw_subdir
    ).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    segments = [
        (start, min(start + args.segment_blocks - 1, args.end_block))
        for start in range(args.start_block, args.end_block + 1, args.segment_blocks)
    ]
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_segment,
                endpoints,
                start,
                end,
                args.rpc_span,
                directory,
            ): (start, end)
            for start, end in segments
        }
        for future in as_completed(futures):
            results.append(future.result())
    print(
        f"Flare canonical RPC collection complete: "
        f"{sum(int(row['rows']) for row in results):,} logs in {len(results)} segments",
        flush=True,
    )


if __name__ == "__main__":
    main()
