"""Collect Tellor's immutable, full-history MicroReport event ledger.

The public Tellor REST state prunes old ``MicroReport`` values and its
``tx_search`` index times out for the full ``new_report`` history.  Historical
CometBFT ``block_results`` remain available, however.  This collector scans
every block exactly once in disjoint, resumable segments and extracts the
canonical ``new_report`` events emitted by successful ``SetValue`` calls.

The RPC server accepts at most ten JSON-RPC calls per batch, so each worker
keeps a persistent, source-bound HTTP session and requests ten consecutive
heights at a time.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock, get_ident
from typing import Any, Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
import orjson
import duckdb
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

from oracle_ledger.tellor_layer import TellorClient


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
SOURCE_COMMIT = "943a2709ef0a60eb560447278b2f59923b9de484"
REQUIRED_ATTRIBUTES = {
    "reporter",
    "reporter_power",
    "query_type",
    "query_id",
    "value",
    "cyclelist",
    "aggregate_method",
    "timestamp",
    "meta_id",
}
PRINT_LOCK = Lock()


class SourceAddressAdapter(HTTPAdapter):
    """Bind urllib3 connection pools to one configured local IP address."""

    def __init__(self, source_address: str, *args: Any, **kwargs: Any) -> None:
        self.source_address = source_address
        super().__init__(*args, **kwargs)

    def init_poolmanager(
        self,
        connections: int,
        maxsize: int,
        block: bool = False,
        **pool_kwargs: Any,
    ) -> None:
        pool_kwargs["source_address"] = (self.source_address, 0)
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            **pool_kwargs,
        )


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def parse_bool(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"invalid boolean event attribute: {value!r}")


class BlockResultsClient:
    def __init__(
        self,
        rpc_url: str,
        source_address: str | None,
        timeout: int,
    ) -> None:
        self.rpc_url = rpc_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"
        if source_address:
            self.session.mount("https://", SourceAddressAdapter(source_address))
            self.session.mount("http://", SourceAddressAdapter(source_address))

    def fetch(self, heights: list[int]) -> list[dict[str, Any]]:
        if not heights or len(heights) > 10:
            raise ValueError("Tellor block_results batch must contain 1-10 heights")
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
        for attempt in range(100):
            try:
                response = self.session.post(
                    self.rpc_url,
                    json=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                body = orjson.loads(response.content)
                if not isinstance(body, list):
                    raise RuntimeError(f"non-batch block_results response: {body!r}")
                by_height: dict[int, dict[str, Any]] = {}
                for item in body:
                    if item.get("error"):
                        raise RuntimeError(str(item["error"]))
                    height = int(item["id"])
                    result = item.get("result")
                    if not isinstance(result, dict):
                        raise RuntimeError(f"missing block_results result at {height}")
                    if int(result.get("height") or 0) != height:
                        raise RuntimeError(f"block_results height mismatch at {height}")
                    by_height[height] = result
                if set(by_height) != set(heights):
                    raise RuntimeError(
                        f"block_results response coverage mismatch: "
                        f"{sorted(by_height)} != {heights}"
                    )
                return [by_height[height] for height in heights]
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt == 99:
                    break
                time.sleep(min(0.25 * 2 ** min(attempt, 6), 15))
        raise RuntimeError(
            f"Tellor block_results failed for {heights[0]}-{heights[-1]}"
        ) from last_error


def report_rows(
    height: int,
    result: dict[str, Any],
    cutoff_height: int,
) -> Iterator[dict[str, Any]]:
    for tx_index, tx_result in enumerate(result.get("txs_results") or []):
        if int(tx_result.get("code") or 0) != 0:
            continue
        for event_index, event in enumerate(tx_result.get("events") or []):
            if event.get("type") != "new_report":
                continue
            attributes = {
                str(item["key"]): str(item["value"])
                for item in event.get("attributes") or []
            }
            missing = REQUIRED_ATTRIBUTES - set(attributes)
            if missing:
                raise RuntimeError(
                    f"new_report omitted {sorted(missing)} at "
                    f"{height}:{tx_index}:{event_index}"
                )
            query_id = attributes["query_id"].lower().removeprefix("0x")
            if len(query_id) != 64:
                raise RuntimeError(
                    f"invalid Tellor query id at "
                    f"{height}:{tx_index}:{event_index}: {query_id}"
                )
            yield {
                "reporter": attributes["reporter"],
                "power": int(attributes["reporter_power"]),
                "query_type": attributes["query_type"],
                "query_id": query_id,
                "aggregate_method": attributes["aggregate_method"],
                "value": attributes["value"],
                "timestamp_ms": int(attributes["timestamp"]),
                "cyclelist": parse_bool(attributes["cyclelist"]),
                "block_number": height,
                "meta_id": int(attributes["meta_id"]),
                "source_snapshot_height": cutoff_height,
                "coverage_lower_height_exclusive": 0,
                "source_tx_index": tx_index,
                "source_event_index": event_index,
                "asset": "loya",
                "asset_decimals": 6,
            }


def collect_segment(
    rpc_url: str,
    source_address: str | None,
    timeout: int,
    start: int,
    end: int,
    cutoff_height: int,
    raw_dir: Path,
) -> dict[str, Any]:
    stem = f"{start:08d}_{end:08d}"
    raw_path = raw_dir / f"{stem}.jsonl.gz"
    done_path = raw_dir / f"{stem}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if (
            prior.get("complete") is True
            and int(prior.get("start_height") or 0) == start
            and int(prior.get("end_height") or 0) == end
            and int(prior.get("scanned_blocks") or 0) == end - start + 1
        ):
            return prior

    client = BlockResultsClient(rpc_url, source_address, timeout)
    # Multiple bounded accelerators may legitimately reach the same segment.
    # A process/thread-qualified staging name prevents their gzip streams from
    # being interleaved before the first complete writer atomically publishes.
    temporary = raw_path.with_name(
        f"{raw_path.name}.tmp.{os.getpid()}.{get_ident()}"
    )
    report_count = 0
    report_transaction_count = 0
    request_batches = 0
    with gzip.open(temporary, "wb", compresslevel=1) as handle:
        for batch_start in range(start, end + 1, 10):
            heights = list(range(batch_start, min(batch_start + 10, end + 1)))
            results = client.fetch(heights)
            request_batches += 1
            for height, result in zip(heights, results, strict=True):
                rows = list(report_rows(height, result, cutoff_height))
                report_transaction_count += len(
                    {
                        int(row["source_tx_index"])
                        for row in rows
                    }
                )
                for row in rows:
                    handle.write(orjson.dumps(row, option=orjson.OPT_SORT_KEYS))
                    handle.write(b"\n")
                    report_count += 1
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "source_method": "block_results",
        "start_height": start,
        "end_height": end,
        "scanned_blocks": end - start + 1,
        "request_batches": request_batches,
        "report_transactions": report_transaction_count,
        "reports": report_count,
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    with PRINT_LOCK:
        print(
            f"Tellor block events {start:,}-{end:,}: "
            f"{report_count:,} reports",
            flush=True,
        )
    return receipt


def iter_jsonl(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with gzip.open(path, "rb") as handle:
            for line in handle:
                yield orjson.loads(line)


SCHEMA = pa.schema(
    [
        pa.field("reporter", pa.string()),
        pa.field("power", pa.int64()),
        pa.field("query_type", pa.string()),
        pa.field("query_id", pa.string()),
        pa.field("aggregate_method", pa.string()),
        pa.field("value", pa.string()),
        pa.field("timestamp_ms", pa.int64()),
        pa.field("cyclelist", pa.bool_()),
        pa.field("block_number", pa.int64()),
        pa.field("meta_id", pa.int64()),
        pa.field("source_snapshot_height", pa.int64()),
        pa.field("coverage_lower_height_exclusive", pa.int64()),
        pa.field("source_tx_index", pa.int64()),
        pa.field("source_event_index", pa.int64()),
        pa.field("asset", pa.string()),
        pa.field("asset_decimals", pa.int64()),
    ]
)


def write_outputs(
    rows: Iterable[dict[str, Any]],
    jsonl_path: Path,
    parquet_path: Path,
) -> dict[str, Any]:
    jsonl_tmp = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    parquet_tmp = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    writer = pq.ParquetWriter(parquet_tmp, SCHEMA, compression="zstd")
    batch: list[dict[str, Any]] = []
    count = 0
    first_height: int | None = None
    last_height: int | None = None
    non_monotonic_heights = 0
    zero_meta_ids = 0
    post_cutoff_timestamps = 0
    reporters: set[str] = set()
    with jsonl_tmp.open("wb") as jsonl:
        for row in rows:
            height = int(row["block_number"])
            reporter = str(row["reporter"])
            meta_id = int(row["meta_id"])
            if last_height is not None and height < last_height:
                non_monotonic_heights += 1
            if meta_id == 0:
                zero_meta_ids += 1
            reporters.add(reporter)
            first_height = height if first_height is None else first_height
            last_height = height
            if int(row["timestamp_ms"]) > int(CUTOFF.timestamp() * 1_000):
                post_cutoff_timestamps += 1
            jsonl.write(orjson.dumps(row, option=orjson.OPT_SORT_KEYS))
            jsonl.write(b"\n")
            batch.append(row)
            count += 1
            if len(batch) >= 100_000:
                writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
                batch.clear()
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
    writer.close()
    if count == 0 or first_height is None or last_height is None:
        raise RuntimeError("Tellor block-event history is empty")
    jsonl_tmp.replace(jsonl_path)
    parquet_tmp.replace(parquet_path)
    return {
        "report_rows": count,
        "first_report_height": first_height,
        "last_report_height": last_height,
        "reporter_universe_count": len(reporters),
        "non_monotonic_height_rows": non_monotonic_heights,
        "zero_meta_id_rows": zero_meta_ids,
        "post_cutoff_timestamp_rows": post_cutoff_timestamps,
        "jsonl_report_rows": count,
    }


def exact_key_qc(parquet_path: Path) -> dict[str, int]:
    """Check the two immutable keys defined by the event and keeper source."""

    connection = duckdb.connect()
    source = "'" + str(parquet_path).replace("'", "''") + "'"
    duplicate_event_ids = connection.execute(
        "SELECT count(*) - count(DISTINCT "
        "(block_number, source_tx_index, source_event_index)) "
        f"FROM read_parquet({source})"
    ).fetchone()[0]
    duplicate_storage_keys = connection.execute(
        "SELECT count(*) - count(DISTINCT (query_id, reporter, meta_id)) "
        f"FROM read_parquet({source})"
    ).fetchone()[0]
    connection.close()
    return {
        "duplicate_source_event_rows": int(duplicate_event_ids),
        "duplicate_report_storage_key_rows": int(duplicate_storage_keys),
    }


def count_newlines(path: Path) -> int:
    count = 0
    with path.open("rb") as handle:
        while chunk := handle.read(64 * 1024 * 1024):
            count += chunk.count(b"\n")
    return count


def inspect_existing_outputs(
    jsonl_path: Path,
    parquet_path: Path,
) -> dict[str, Any]:
    """Re-run output QC without rebuilding a receipt-complete 82M-row table."""

    if not jsonl_path.is_file() or not parquet_path.is_file():
        raise RuntimeError("Tellor existing-output finalization requires JSONL and Parquet")
    parquet = pq.ParquetFile(parquet_path)
    if parquet.schema_arrow != SCHEMA:
        raise RuntimeError(
            f"Tellor Parquet schema mismatch: {parquet.schema_arrow} != {SCHEMA}"
        )
    report_rows = int(parquet.metadata.num_rows)
    previous_height: int | None = None
    non_monotonic_heights = 0
    for batch in parquet.iter_batches(
        batch_size=1_000_000,
        columns=["block_number"],
    ):
        heights = batch.column(0)
        if len(heights) == 0:
            continue
        if previous_height is not None and int(heights[0].as_py()) < previous_height:
            non_monotonic_heights += 1
        if len(heights) > 1:
            decreases = pc.less(
                heights.slice(1),
                heights.slice(0, len(heights) - 1),
            )
            non_monotonic_heights += int(pc.sum(decreases).as_py() or 0)
        previous_height = int(heights[-1].as_py())

    connection = duckdb.connect()
    source = "'" + str(parquet_path).replace("'", "''") + "'"
    (
        first_height,
        last_height,
        reporter_count,
        zero_meta_ids,
        post_cutoff,
    ) = connection.execute(
        "SELECT min(block_number), max(block_number), "
        "count(DISTINCT reporter), count(*) FILTER (WHERE meta_id=0), "
        f"count(*) FILTER (WHERE timestamp_ms>{int(CUTOFF.timestamp() * 1_000)}) "
        f"FROM read_parquet({source})"
    ).fetchone()
    connection.close()
    output = {
        "report_rows": report_rows,
        "jsonl_report_rows": count_newlines(jsonl_path),
        "first_report_height": int(first_height),
        "last_report_height": int(last_height),
        "reporter_universe_count": int(reporter_count),
        "non_monotonic_height_rows": non_monotonic_heights,
        "zero_meta_id_rows": int(zero_meta_ids),
        "post_cutoff_timestamp_rows": int(post_cutoff),
    }
    output.update(exact_key_qc(parquet_path))
    return output


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect full Tellor reports from historical block_results"
    )
    parser.add_argument(
        "--rpc-url",
        default="https://mainnet.tellorlayer.com/rpc",
    )
    parser.add_argument(
        "--api-url",
        default="https://mainnet.tellorlayer.com",
    )
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--segment-heights", type=int, default=10_000)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--cutoff-height", type=int)
    parser.add_argument("--source-address", action="append", default=[])
    parser.add_argument(
        "--finalize-existing-outputs",
        action="store_true",
        help="validate already-written JSONL/Parquet after receipt-complete collection",
    )
    args = parser.parse_args()
    if args.segment_heights <= 0:
        raise SystemExit("--segment-heights must be positive")

    chain = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = args.cutoff_height or chain.height_at_or_before(CUTOFF)
    cutoff_time = chain.block_time(cutoff_height).isoformat()
    if chain.block_time(cutoff_height) > CUTOFF:
        raise RuntimeError("configured Tellor cutoff height is after fixed cutoff")

    raw_dir = (
        ROOT / "data/raw/tellor_layer/report_block_events_full"
    ).resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    manifest_dir = ROOT / "data/manifests"
    for path in (raw_dir, curated_dir, manifest_dir):
        path.mkdir(parents=True, exist_ok=True)
    segments = [
        (start, min(start + args.segment_heights - 1, cutoff_height))
        for start in range(1, cutoff_height + 1, args.segment_heights)
    ]
    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {}
        for index, (start, end) in enumerate(segments):
            source_address = (
                args.source_address[index % len(args.source_address)]
                if args.source_address
                else None
            )
            future = executor.submit(
                collect_segment,
                args.rpc_url,
                source_address,
                args.timeout,
                start,
                end,
                cutoff_height,
                raw_dir,
            )
            futures[future] = (start, end)
        for future in as_completed(futures):
            receipts.append(future.result())

    by_start = {int(row["start_height"]): row for row in receipts}
    expected_starts = {start for start, _ in segments}
    if set(by_start) != expected_starts:
        raise RuntimeError("Tellor block-event segment receipts are incomplete")
    scanned_blocks = sum(int(row["scanned_blocks"]) for row in receipts)
    if scanned_blocks != cutoff_height:
        raise RuntimeError(
            f"Tellor block-event coverage mismatch: "
            f"{scanned_blocks} != {cutoff_height}"
        )
    raw_paths = [Path(by_start[start]["raw_file"]) for start, _ in segments]
    jsonl_path = curated_dir / "tellor_micro_reports.jsonl"
    parquet_path = curated_dir / "tellor_micro_reports.parquet"
    if args.finalize_existing_outputs:
        output = inspect_existing_outputs(jsonl_path, parquet_path)
    else:
        output = write_outputs(iter_jsonl(raw_paths), jsonl_path, parquet_path)
        output.update(exact_key_qc(parquet_path))
    receipt_reports = sum(int(row["reports"]) for row in receipts)
    transaction_count = sum(int(row["report_transactions"]) for row in receipts)
    manifest = {
        "dataset": "Tellor Layer complete immutable micro-report event ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "cutoff_block_time": cutoff_time,
        "source_method": "block_results",
        "source_event": "new_report",
        "source_commit": SOURCE_COMMIT,
        "segments": len(segments),
        "scanned_blocks": scanned_blocks,
        "report_transactions": transaction_count,
        "receipt_report_rows": receipt_reports,
        **output,
        "raw_directory": str(raw_dir),
        "curated_jsonl": str(jsonl_path),
        "curated_parquet": str(parquet_path),
    }
    manifest["all_required_assertions_pass"] = (
        scanned_blocks == cutoff_height
        and int(output["report_rows"]) == receipt_reports
        and int(output["jsonl_report_rows"]) == receipt_reports
        and int(output["report_rows"]) >= transaction_count
        and int(output["report_rows"]) > 0
        and int(output["last_report_height"]) <= cutoff_height
        and int(output["non_monotonic_height_rows"]) == 0
        and int(output["zero_meta_id_rows"]) == 0
        and int(output["duplicate_source_event_rows"]) == 0
        and int(output["duplicate_report_storage_key_rows"]) == 0
        and int(output["post_cutoff_timestamp_rows"]) == 0
    )
    manifest["scope_guard"] = (
        "Every height from genesis through the fixed cutoff was requested "
        "exactly once in disjoint receipt-backed segments. Each output row is "
        "the immutable new_report event emitted by the same successful "
        "SetValue call that stores MicroReport; no report is imputed from an "
        "aggregate and no pruned REST-state history is assumed. The source "
        "keeper keys reports by (query_id, reporter, query.Id); query.Id is an "
        "aggregate-group identifier and is not required to increase in each "
        "reporter's submission-time order while multiple queries are open."
    )
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor block-event QC failed: {manifest}")
    manifest_path = manifest_dir / "tellor_micro_reports.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Tellor full immutable block-event report QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff height: {cutoff_height:,}

- Blocks scanned: {scanned_blocks:,}/{cutoff_height:,}.
- `new_report` transactions: {transaction_count:,}.
- Micro-report events: {output['report_rows']:,}.
- Reporter universe: {output['reporter_universe_count']:,}.
- Height range: {output['first_report_height']:,}–{output['last_report_height']:,}.
- Receipt/output difference: {receipt_reports - int(output['report_rows'])}.
- JSONL/Parquet rows: {output['jsonl_report_rows']:,}/{output['report_rows']:,}.
- Non-monotonic height rows: {output['non_monotonic_height_rows']}.
- Duplicate event/storage-key rows: {output['duplicate_source_event_rows']}/{output['duplicate_report_storage_key_rows']}.
- Zero `meta_id` rows: {output['zero_meta_id_rows']}.
- Post-cutoff timestamp rows: {output['post_cutoff_timestamp_rows']}.

`SetValue` emits `new_report` with the reporter, power, query, value,
aggregation method, timestamp, cycle-list flag, and meta id written to
`MicroReport`. The keeper rejects an existing `(query_id, reporter, meta_id)`
storage key. Historical `block_results` are immutable and remain available
after the module prunes old report state.
"""
    (ROOT / "reports/tellor_micro_reports_qc.md").write_text(
        report,
        encoding="utf-8",
    )
    print(manifest_path)


if __name__ == "__main__":
    main()
