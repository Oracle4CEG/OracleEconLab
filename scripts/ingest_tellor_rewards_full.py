"""Collect the complete observable Tellor report-reward allocation history."""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from oracle_ledger.tellor_layer import TellorClient
from oracle_ledger.tellor_rewards import parse_full_reward_block


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_RPC = "https://mainnet.tellorlayer.com/rpc"
DEFAULT_API = "https://mainnet.tellorlayer.com"
EVENT_QUERIES = (
    "rewards_added.delegator EXISTS",
    "rewards_accumulated.reporter EXISTS",
)
SOURCE_COMMIT = "943a2709ef0a60eb560447278b2f59923b9de484"
PRINT_LOCK = Lock()


class BatchRpc:
    def __init__(self, url: str, timeout: int = 180) -> None:
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"

    def post(self, payload: Any) -> Any:
        last_error: Exception | None = None
        for attempt in range(10):
            try:
                response = self.session.post(self.url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                body = response.json()
                if isinstance(body, dict) and body.get("error"):
                    raise RuntimeError(str(body["error"]))
                return body
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt == 9:
                    break
                time.sleep(min(2**attempt, 30))
        raise RuntimeError("Tellor RPC failed after retries") from last_error

    def block_search(self, query: str, page: int) -> dict[str, Any]:
        return self.post(
            {
                "jsonrpc": "2.0",
                "id": page,
                "method": "block_search",
                "params": {
                    "query": query,
                    "page": str(page),
                    "per_page": "100",
                    "order_by": "asc",
                },
            }
        )["result"]

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
        last_error: Exception | None = None
        # Some public reverse proxies occasionally return a valid JSON object
        # for a batch request (usually a transient upstream error) instead of
        # the required list. Retry the exact immutable heights rather than
        # failing an otherwise complete resumable segment.
        for attempt in range(10):
            try:
                rows = self.post(payload)
                # Some Tendermint reverse proxies unwrap a one-element JSON-RPC
                # batch into the corresponding response object.  The response
                # is still unambiguous because the immutable height is its id.
                if isinstance(rows, dict) and len(heights) == 1:
                    rows = [rows]
                if not isinstance(rows, list):
                    raise RuntimeError("Tellor batch response is not a list")
                by_id = {int(row["id"]): row for row in rows}
                output = []
                for height in heights:
                    row = by_id[height]
                    if row.get("error"):
                        raise RuntimeError(
                            f"block_results failed at {height}: {row['error']}"
                        )
                    output.append(row["result"])
                return output
            except (KeyError, RuntimeError) as exc:
                last_error = exc
                if attempt == 9:
                    break
                time.sleep(min(0.5 * 2**attempt, 15))
        raise RuntimeError(
            f"Tellor block_results batch failed for {heights}"
        ) from last_error


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def collect_segment(
    search_rpc_url: str,
    result_rpc_url: str,
    start: int,
    end: int,
    raw_dir: Path,
    event_queries: tuple[str, ...] = EVENT_QUERIES,
) -> dict[str, Any]:
    name = f"{start:08d}_{end:08d}"
    raw_path = raw_dir / f"{name}.jsonl.gz"
    done_path = raw_dir / f"{name}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if prior.get("complete"):
            return prior
    search_client = BatchRpc(search_rpc_url)
    result_client = BatchRpc(result_rpc_url)
    header_by_height: dict[int, str] = {}
    query_counts: dict[str, int] = {}
    query_pages: dict[str, int] = {}
    for event_query in event_queries:
        query = (
            f"{event_query} AND block.height >= {start} AND block.height <= {end}"
        )
        headers: list[tuple[int, str]] = []
        page = 1
        total: int | None = None
        while total is None or len(headers) < total:
            result = search_client.block_search(query, page)
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
            raise RuntimeError(
                f"incomplete Tellor reward block search {start}-{end}: "
                f"{event_query} {len(headers)} != {total}"
            )
        if len(headers) != len({height for height, _ in headers}):
            raise RuntimeError(f"duplicate blocks for {event_query} in {start}-{end}")
        for height, block_time in headers:
            existing = header_by_height.get(height)
            if existing is not None and existing != block_time:
                raise RuntimeError(f"conflicting block time at {height}")
            header_by_height[height] = block_time
        query_counts[event_query] = len(headers)
        query_pages[event_query] = page - 1

    heights = sorted(header_by_height)
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        # The public Tellor RPC enforces a maximum JSON-RPC batch size of 10.
        for offset in range(0, len(heights), 10):
            batch = heights[offset : offset + 10]
            results = result_client.block_results(batch)
            for height, result in zip(batch, results, strict=True):
                handle.write(
                    json.dumps(
                        {
                            "height": height,
                            "block_time": header_by_height[height],
                            "result": result,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "start_height": start,
        "end_height": end,
        "unique_reward_blocks": len(heights),
        "query_block_counts": query_counts,
        "query_pages": query_pages,
        "event_queries": event_queries,
        "search_rpc_url": search_rpc_url,
        "result_rpc_url": result_rpc_url,
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    with PRINT_LOCK:
        print(
            f"Tellor full rewards {start}-{end}: {len(heights):,} blocks ("
            + ", ".join(
                f"{query.split('.')[0]} {query_counts[query]:,}"
                for query in event_queries
            )
            + ")",
            flush=True,
        )
    return receipt


def iter_records(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


class StreamingTableSink:
    """Write a large homogeneous ledger without retaining it in memory."""

    def __init__(
        self,
        jsonl_path: Path,
        parquet_path: Path,
        batch_size: int = 100_000,
        empty_schema: pa.Schema | None = None,
        output_schema: pa.Schema | None = None,
    ) -> None:
        self.jsonl_path = jsonl_path
        self.parquet_path = parquet_path
        self.jsonl_tmp = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
        self.parquet_tmp = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
        self.handle = self.jsonl_tmp.open("w", encoding="utf-8")
        self.writer: pq.ParquetWriter | None = None
        self.columns: list[str] | None = None
        self.batch: list[dict[str, Any]] = []
        self.batch_size = batch_size
        self.count = 0
        self.empty_schema = empty_schema
        self.output_schema = output_schema

    def add(self, row: dict[str, Any]) -> None:
        if self.columns is None:
            self.columns = sorted(row)
        unknown = set(row) - set(self.columns)
        if unknown:
            raise RuntimeError(
                f"schema drift in {self.jsonl_path.name}: {sorted(unknown)}"
            )
        normalized = {key: row.get(key) for key in self.columns}
        self.handle.write(
            json.dumps(
                normalized,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        self.batch.append(normalized)
        self.count += 1
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.batch:
            return
        if self.output_schema is not None:
            table = pa.Table.from_pylist(
                self.batch, schema=self.output_schema
            )
        elif self.writer is None:
            table = pa.Table.from_pylist(self.batch)
        else:
            table = pa.Table.from_pylist(self.batch, schema=self.writer.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(
                self.parquet_tmp,
                table.schema,
                compression="zstd",
            )
        self.writer.write_table(table)
        self.batch.clear()

    def close(self) -> int:
        self.flush()
        self.handle.close()
        if self.writer is None:
            if self.empty_schema is None:
                raise RuntimeError(f"no rows for {self.jsonl_path.name}")
            pq.write_table(
                pa.Table.from_pylist([], schema=self.empty_schema),
                self.parquet_tmp,
                compression="zstd",
            )
        else:
            self.writer.close()
        self.jsonl_tmp.replace(self.jsonl_path)
        self.parquet_tmp.replace(self.parquet_path)
        return self.count


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect all observable Tellor reward accruals")
    parser.add_argument("--rpc-url", default=os.getenv("TELLOR_RPC_URL", DEFAULT_RPC))
    parser.add_argument(
        "--search-rpc-url",
        help="RPC whose block event index is used for block_search (defaults to --rpc-url)",
    )
    parser.add_argument(
        "--result-rpc-url",
        help="RPC used for block_results (defaults to --rpc-url)",
    )
    parser.add_argument("--api-url", default=os.getenv("TELLOR_API_URL", DEFAULT_API))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--segment-heights", type=int, default=500_000)
    parser.add_argument("--start-height", type=int, default=1)
    parser.add_argument("--end-height", type=int)
    parser.add_argument(
        "--event-query",
        action="append",
        choices=EVENT_QUERIES,
        help="Limit collection to one or more event queries; repeat for both",
    )
    parser.add_argument(
        "--raw-subdir",
        default="",
        help="Optional resumable raw subdirectory under rewards_full",
    )
    parser.add_argument(
        "--collect-only",
        action="store_true",
        help="Stop after raw blocks and exact receipts are collected",
    )
    args = parser.parse_args()

    chain = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = chain.height_at_or_before(CUTOFF)
    cutoff_time = chain.block_time(cutoff_height).isoformat()
    end_height = min(args.end_height or cutoff_height, cutoff_height)
    if args.start_height < 1 or args.start_height > end_height:
        raise ValueError(
            f"invalid collection range {args.start_height}-{end_height}"
        )
    event_queries = tuple(args.event_query or EVENT_QUERIES)
    search_rpc_url = args.search_rpc_url or args.rpc_url
    result_rpc_url = args.result_rpc_url or args.rpc_url
    raw_dir = (ROOT / "data/raw/tellor_layer/rewards_full").resolve()
    if args.raw_subdir:
        raw_dir = raw_dir / args.raw_subdir
    curated_dir = (ROOT / "data/curated").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)
    segments = [
        (start, min(start + args.segment_heights - 1, cutoff_height))
        for start in range(
            args.start_height, end_height + 1, args.segment_heights
        )
    ]
    segments = [
        (start, min(end, end_height))
        for start, end in segments
    ]
    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_segment,
                search_rpc_url,
                result_rpc_url,
                start,
                end,
                raw_dir,
                event_queries,
            ): (
                start,
                end,
            )
            for start, end in segments
        }
        for future in as_completed(futures):
            receipts.append(future.result())

    if args.collect_only:
        summary_path = raw_dir / "collection_summary.json"
        atomic_json(
            summary_path,
            {
                "complete": True,
                "fixed_cutoff": CUTOFF.isoformat(),
                "start_height": args.start_height,
                "end_height": end_height,
                "segments": len(segments),
                "event_queries": event_queries,
                "unique_reward_blocks": sum(
                    int(row["unique_reward_blocks"]) for row in receipts
                ),
                "search_rpc_url": search_rpc_url,
                "result_rpc_url": result_rpc_url,
                "finished_at_utc": datetime.now(UTC).isoformat(),
            },
        )
        print(summary_path)
        return

    raw_paths = [
        raw_dir / f"{start:08d}_{end:08d}.jsonl.gz" for start, end in segments
    ]
    distribution_sink = StreamingTableSink(
        curated_dir / "tellor_liveness_reward_distributions_full.jsonl",
        curated_dir / "tellor_liveness_reward_distributions_full.parquet",
        output_schema=pa.schema([
            pa.field("height", pa.int64()),
            pa.field("block_time", pa.string()),
            pa.field("distribution_index", pa.int64()),
            pa.field("total_distributed_loya_raw", pa.string()),
            pa.field("reporter_count", pa.int64()),
            pa.field("standard_opportunities", pa.int64()),
            pa.field("non_standard_queries", pa.int64()),
            pa.field("semantic_class", pa.string()),
            pa.field("rule_id", pa.string()),
        ]),
    )
    current_sink = StreamingTableSink(
        curated_dir / "tellor_reporter_reward_accruals_full.jsonl",
        curated_dir / "tellor_reporter_reward_accruals_full.parquet",
        output_schema=pa.schema([
            pa.field("height", pa.int64()),
            pa.field("block_time", pa.string()),
            pa.field("event_index", pa.int64()),
            pa.field("distribution_index", pa.int64()),
            pa.field("reward_source", pa.string()),
            pa.field("reporter", pa.string()),
            pa.field("commission_loya_decimal", pa.string()),
            pa.field("net_reward_loya_decimal", pa.string()),
            pa.field("gross_reward_loya_decimal", pa.string()),
            pa.field("period_total_loya_decimal", pa.string()),
            pa.field("semantic_class", pa.string()),
            pa.field("rule_id", pa.string()),
        ]),
    )
    legacy_sink = StreamingTableSink(
        curated_dir / "tellor_legacy_selector_reward_accruals.jsonl",
        curated_dir / "tellor_legacy_selector_reward_accruals.parquet",
        empty_schema=pa.schema([
            pa.field("height", pa.int64()),
            pa.field("block_time", pa.string()),
            pa.field("event_index", pa.int64()),
            pa.field("reward_source", pa.string()),
            pa.field("selector_event_value_utf8_lossy", pa.string()),
            pa.field("selector_address_observable", pa.bool_()),
            pa.field("incremental_reward_loya_raw", pa.string()),
            pa.field("incremental_reward_observable", pa.bool_()),
            pa.field("cumulative_selector_tips_loya_decimal", pa.string()),
            pa.field("semantic_class", pa.string()),
            pa.field("rule_id", pa.string()),
        ]),
    )
    blocks_without_target_events = 0
    previous_height = 0
    liveness_accrual_count = 0
    tip_current_count = 0
    observable_legacy_count = 0
    legacy_incremental_total = 0
    current_gross_total = Decimal(0)
    reporter_count_mismatches = 0
    for record in iter_records(raw_paths):
        height = int(record["height"])
        if height <= previous_height:
            raise RuntimeError("Tellor full reward blocks are not globally ordered")
        previous_height = height
        events = record["result"].get("finalize_block_events") or []
        block_distributions, block_current, block_legacy = parse_full_reward_block(
            height, record["block_time"], events
        )
        if not block_current and not block_legacy:
            blocks_without_target_events += 1
        block_linked_counts: dict[int, int] = {}
        for row in block_distributions:
            distribution_sink.add(row)
        for row in block_current:
            current_sink.add(row)
            current_gross_total += Decimal(row["gross_reward_loya_decimal"])
            if row["reward_source"] == "liveness_tbr":
                liveness_accrual_count += 1
                distribution_index = int(row["distribution_index"])
                block_linked_counts[distribution_index] = (
                    block_linked_counts.get(distribution_index, 0) + 1
                )
            else:
                tip_current_count += 1
        for row in block_distributions:
            if (
                block_linked_counts.get(int(row["distribution_index"]), 0)
                != int(row["reporter_count"])
            ):
                reporter_count_mismatches += 1
        for row in block_legacy:
            legacy_sink.add(row)
            if row["incremental_reward_loya_raw"] is not None:
                observable_legacy_count += 1
                legacy_incremental_total += int(
                    row["incremental_reward_loya_raw"]
                )

    distribution_count = distribution_sink.close()
    current_count = current_sink.close()
    legacy_count = legacy_sink.close()
    raw_block_count = sum(int(row["unique_reward_blocks"]) for row in receipts)
    manifest = {
        "dataset": "Tellor Layer complete observable reward-allocation ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "cutoff_block_time": cutoff_time,
        "source_event_queries": EVENT_QUERIES,
        "source_commit": SOURCE_COMMIT,
        "segments": len(segments),
        "unique_reward_blocks": raw_block_count,
        "blocks_without_target_events": blocks_without_target_events,
        "liveness_distribution_events": distribution_count,
        "current_reporter_period_accrual_events": current_count,
        "current_liveness_accrual_events": liveness_accrual_count,
        "current_query_tip_accrual_events": tip_current_count,
        "legacy_selector_tip_accrual_events": legacy_count,
        "legacy_event_query_complete_zero_observed": legacy_count == 0,
        "legacy_incremental_amount_observable_events": observable_legacy_count,
        "legacy_cumulative_only_events": (
            legacy_count - observable_legacy_count
        ),
        "legacy_incremental_reward_total_loya_raw": str(legacy_incremental_total),
        "current_gross_reward_total_loya_decimal": str(current_gross_total),
        "reporter_count_mismatches": reporter_count_mismatches,
        "all_required_assertions_pass": (
            blocks_without_target_events == 0
            and reporter_count_mismatches == 0
            and raw_block_count > 0
            and current_count + legacy_count > 0
        ),
        "scope_guard": (
            "Deployed legacy rewards_added exposes a cumulative amount and a "
            "raw-byte selector value, not a canonical address or per-event "
            "increment; it is retained as cumulative-only evidence. Current "
            "rewards_accumulated is an internal allocation/accrual event, not "
            "an account payment. "
            "Only tip_withdrawn is a realized selector payment into stake and is "
            "collected in the separate Tellor tip/withdrawal transaction ledger. "
            "Periods before rewards_added was deployed have no event-level "
            "selector allocation interface and are not silently imputed."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor full reward QC failed: {manifest}")
    manifest_path = ROOT / "data/manifests/tellor_rewards_full.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Tellor Layer full reward-allocation QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff height: {cutoff_height:,}

- Reward event blocks: {raw_block_count:,}.
- Legacy selector tip accruals: {legacy_count:,}.
- Current reporter-period accruals: {current_count:,}.
- Current liveness accruals: {liveness_accrual_count:,}.
- Current query-tip accruals: {tip_current_count:,}.
- Liveness distribution events: {distribution_count:,}.
- Reporter-count mismatches: {reporter_count_mismatches}.

Accrual and realized payment are deliberately separated. The actual
`tip_withdrawn` staking cash flow is reconciled in the transaction ledger.
"""
    (ROOT / "reports/tellor_rewards_full_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
