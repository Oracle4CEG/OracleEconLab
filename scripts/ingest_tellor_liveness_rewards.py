"""Collect all Tellor Layer liveness-reward allocations through the cutoff."""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from oracle_ledger.tellor_layer import TellorClient
from oracle_ledger.tellor_rewards import parse_liveness_reward_block


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_RPC = "https://mainnet.tellorlayer.com/rpc"
DEFAULT_API = "https://mainnet.tellorlayer.com"
LIVENESS_UPGRADE_HEIGHT = 13_280_690
EVENT_QUERY = "liveness_rewards_distributed.total_distributed EXISTS"
PRINT_LOCK = Lock()


class BatchRpc:
    def __init__(self, url: str, timeout: int = 120) -> None:
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"

    def post(self, payload: Any) -> Any:
        last_error: Exception | None = None
        for attempt in range(8):
            try:
                response = self.session.post(self.url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                body = response.json()
                if isinstance(body, dict) and body.get("error"):
                    raise RuntimeError(str(body["error"]))
                return body
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt == 7:
                    break
                time.sleep(min(2**attempt, 30))
        raise RuntimeError("Tellor batch RPC failed after retries") from last_error

    def block_search(self, query: str, page: int, per_page: int = 100) -> dict[str, Any]:
        body = self.post({
            "jsonrpc": "2.0",
            "id": page,
            "method": "block_search",
            "params": {
                "query": query,
                "page": str(page),
                "per_page": str(per_page),
                "order_by": "asc",
            },
        })
        return body["result"]

    def block_results(self, heights: list[int]) -> list[dict[str, Any]]:
        payload = [
            {
                "jsonrpc": "2.0",
                "id": height,
                "method": "block_results",
                "params": {"height": str(height)},
            }
            for height in heights
        ]
        rows = self.post(payload)
        if not isinstance(rows, list):
            raise RuntimeError("Tellor RPC did not return a batch array")
        by_id = {int(row["id"]): row for row in rows}
        output = []
        for height in heights:
            row = by_id[height]
            if row.get("error"):
                raise RuntimeError(f"block_results failed at {height}: {row['error']}")
            output.append(row["result"])
        return output


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def collect_segment(
    rpc_url: str,
    start: int,
    end: int,
    raw_dir: Path,
) -> dict[str, Any]:
    name = f"{start:08d}_{end:08d}"
    raw_path = raw_dir / f"{name}.jsonl.gz"
    done_path = raw_dir / f"{name}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if prior.get("complete"):
            return prior

    client = BatchRpc(rpc_url)
    query = f"{EVENT_QUERY} AND block.height >= {start} AND block.height <= {end}"
    headers: list[tuple[int, str]] = []
    page = 1
    total = None
    while total is None or len(headers) < total:
        result = client.block_search(query, page)
        if total is None:
            total = int(result.get("total_count") or 0)
        blocks = result.get("blocks") or []
        for block in blocks:
            header = block["block"]["header"]
            headers.append((int(header["height"]), header["time"]))
        if not blocks:
            break
        page += 1
    if total is None or len(headers) != total:
        raise RuntimeError(f"Tellor block_search incomplete in {start}-{end}: {len(headers)} != {total}")
    if len({height for height, _ in headers}) != len(headers):
        raise RuntimeError(f"duplicate liveness block heights in {start}-{end}")
    headers.sort()

    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    result_count = 0
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        time_by_height = dict(headers)
        # The public Tellor RPC enforces a maximum JSON-RPC batch size of 10.
        for offset in range(0, len(headers), 10):
            heights = [height for height, _ in headers[offset : offset + 10]]
            for height, result in zip(heights, client.block_results(heights), strict=True):
                record = {
                    "height": height,
                    "block_time": time_by_height[height],
                    "result": result,
                }
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
                result_count += 1
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "start_height": start,
        "end_height": end,
        "liveness_blocks": len(headers),
        "block_results": result_count,
        "block_search_pages": page - 1,
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    with PRINT_LOCK:
        print(f"Tellor reward segment {start}-{end}: {result_count:,} blocks", flush=True)
    return receipt


def iter_records(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in sorted(paths):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def write_rows(jsonl_path: Path, parquet_path: Path, rows: Iterable[dict[str, Any]]) -> int:
    jsonl_temporary = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    parquet_temporary = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    batch: list[dict[str, Any]] = []
    count = 0
    with jsonl_temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            batch.append(row)
            count += 1
            if len(batch) >= 50_000:
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    writer = pq.ParquetWriter(parquet_temporary, table.schema, compression="zstd")
                writer.write_table(table)
                batch.clear()
        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(parquet_temporary, table.schema, compression="zstd")
            writer.write_table(table)
    if writer is None:
        raise RuntimeError(f"no rows for {jsonl_path.name}")
    writer.close()
    jsonl_temporary.replace(jsonl_path)
    parquet_temporary.replace(parquet_path)
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Tellor liveness reward events")
    parser.add_argument("--rpc-url", default=os.getenv("TELLOR_RPC_URL", DEFAULT_RPC))
    parser.add_argument("--api-url", default=os.getenv("TELLOR_API_URL", DEFAULT_API))
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--segment-heights", type=int, default=500_000)
    args = parser.parse_args()

    client = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = client.height_at_or_before(CUTOFF)
    if cutoff_height < LIVENESS_UPGRADE_HEIGHT:
        raise RuntimeError("cutoff predates Tellor liveness rewards")
    cutoff_time = client.block_time(cutoff_height).isoformat()
    raw_dir = (ROOT / "data/raw/tellor_layer/liveness_rewards").resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)

    segments = []
    for start in range(LIVENESS_UPGRADE_HEIGHT, cutoff_height + 1, args.segment_heights):
        segments.append((start, min(start + args.segment_heights - 1, cutoff_height)))
    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(collect_segment, args.rpc_url, start, end, raw_dir): (start, end)
            for start, end in segments
        }
        for future in as_completed(futures):
            receipts.append(future.result())

    raw_paths = [raw_dir / f"{start:08d}_{end:08d}.jsonl.gz" for start, end in segments]
    distributions: list[dict[str, Any]] = []
    accruals: list[dict[str, Any]] = []
    previous_height = 0
    for record in iter_records(raw_paths):
        height = int(record["height"])
        if height <= previous_height:
            raise RuntimeError("Tellor reward blocks are not globally ordered")
        previous_height = height
        events = record["result"].get("finalize_block_events") or []
        block_distributions, block_accruals = parse_liveness_reward_block(
            height, record["block_time"], events
        )
        distributions.extend(block_distributions)
        accruals.extend(block_accruals)

    distribution_jsonl = curated_dir / "tellor_liveness_reward_distributions.jsonl"
    accrual_jsonl = curated_dir / "tellor_reporter_reward_accruals.jsonl"
    distribution_count = write_rows(
        distribution_jsonl,
        curated_dir / "tellor_liveness_reward_distributions.parquet",
        distributions,
    )
    accrual_count = write_rows(
        accrual_jsonl,
        curated_dir / "tellor_reporter_reward_accruals.parquet",
        accruals,
    )
    liveness_accruals = [row for row in accruals if row["reward_source"] == "liveness_tbr"]
    tip_accruals = [row for row in accruals if row["reward_source"] == "query_tip"]
    linked_by_block: dict[tuple[int, int], int] = {}
    for row in liveness_accruals:
        key = (row["height"], int(row["distribution_index"]))
        linked_by_block[key] = linked_by_block.get(key, 0) + 1
    count_mismatches = sum(
        linked_by_block.get((row["height"], row["distribution_index"]), 0) != row["reporter_count"]
        for row in distributions
    )
    block_count = sum(int(row["block_results"]) for row in receipts)
    heights = [row["height"] for row in distributions]
    manifest = {
        "dataset": "Tellor Layer liveness reward allocation ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "cutoff_block_time": cutoff_time,
        "first_height": LIVENESS_UPGRADE_HEIGHT,
        "source_event_query": EVENT_QUERY,
        "source_commit": "943a2709ef0a60eb560447278b2f59923b9de484",
        "segments": len(segments),
        "block_results": block_count,
        "distribution_events": distribution_count,
        "reward_accrual_events": accrual_count,
        "liveness_reward_accrual_events": len(liveness_accruals),
        "tip_reward_accrual_events_coincident_with_liveness_blocks": len(tip_accruals),
        "reporter_count_mismatches": count_mismatches,
        "duplicate_distribution_heights": len(heights) - len(set(heights)),
        "raw_directory": str(raw_dir),
        "curated_distributions": str(distribution_jsonl),
        "curated_accruals": str(accrual_jsonl),
        "all_required_assertions_pass": (
            block_count == distribution_count
            and count_mismatches == 0
            and len(heights) == len(set(heights))
        ),
        "scope_guard": (
            "This ledger is complete for liveness/TBR allocation blocks. "
            "rewards_accumulated changes internal reporter/selector accounting and is not an account payment. "
            "Tip accruals on non-liveness blocks and realized MsgWithdrawTip payments are collected separately."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor reward QC failed: {manifest}")
    manifest_path = ROOT / "data/manifests/tellor_liveness_rewards.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Tellor Layer liveness reward QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Height range: {LIVENESS_UPGRADE_HEIGHT}–{cutoff_height}

- Liveness distribution blocks: {distribution_count:,}.
- Reporter liveness accrual events: {len(liveness_accruals):,}.
- Tip accrual events found in the same blocks: {len(tip_accruals):,}.
- Distribution reporter-count mismatches: {count_mismatches}.
- Duplicate distribution heights: {manifest['duplicate_distribution_heights']}.

These are internal reward allocations backed by the TimeBasedRewards-to-TipsEscrow
module transfer. They are not labeled as realized account payments; those require
the separate `tip_withdrawn` staking/payment event.
"""
    (ROOT / "reports/tellor_liveness_rewards_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
