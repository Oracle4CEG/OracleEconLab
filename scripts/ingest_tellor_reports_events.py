"""Reconstruct Tellor's complete micro-report ledger from immutable tx events.

Every successful ``SetValue`` writes the ``MicroReport`` and emits a
``new_report`` transaction event containing the same economic/research fields.
Unlike module state, transaction results are not removed by the 30-day report
pruner.  A full CometBFT snapshot therefore provides an exact, append-only
history through ``tx_search`` without stitching slow historical REST states.
"""
from __future__ import annotations

import argparse
import gzip
import json
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


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
SOURCE_COMMIT = "9d304bb4d5e82f105f6d7633942a2d67a08befde"
EVENT_QUERY = "new_report.reporter EXISTS"
PRINT_LOCK = Lock()


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


class TxSearchClient:
    def __init__(self, rpc_url: str, timeout: int = 180) -> None:
        self.rpc_url = rpc_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"

    def page(self, query: str, page: int) -> dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "id": page,
            "method": "tx_search",
            "params": {
                "query": query,
                "prove": False,
                "page": str(page),
                "per_page": "100",
                "order_by": "asc",
            },
        }
        last_error: Exception | None = None
        for attempt in range(10):
            try:
                response = self.session.post(
                    self.rpc_url,
                    json=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    raise RuntimeError(str(body["error"]))
                return body["result"]
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt == 9:
                    break
                time.sleep(min(0.25 * 2**attempt, 10))
        raise RuntimeError(f"Tellor tx_search failed: {query} page {page}") from last_error


def parse_bool(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"invalid boolean event attribute: {value!r}")


def report_events(
    tx: dict[str, Any],
    start: int,
    end: int,
) -> Iterator[dict[str, Any]]:
    height = int(tx["height"])
    if not start <= height <= end:
        raise RuntimeError(f"tx_search returned height {height} outside {start}-{end}")
    tx_hash = str(tx.get("hash") or "").upper()
    for event_index, event in enumerate(tx.get("tx_result", {}).get("events") or []):
        if event.get("type") != "new_report":
            continue
        attributes = {
            str(item["key"]): str(item["value"])
            for item in event.get("attributes") or []
        }
        required = {
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
        missing = required - set(attributes)
        if missing:
            raise RuntimeError(f"new_report event omitted {sorted(missing)} at {tx_hash}")
        query_id = attributes["query_id"].lower().removeprefix("0x")
        if len(query_id) != 64:
            raise RuntimeError(f"invalid Tellor query id at {tx_hash}: {query_id}")
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
            "source_snapshot_height": end,
            "coverage_lower_height_exclusive": start - 1,
            "source_tx": tx_hash,
            "source_event_index": event_index,
            "asset": "loya",
            "asset_decimals": 6,
        }


def collect_segment(
    rpc_url: str,
    start: int,
    end: int,
    raw_dir: Path,
) -> dict[str, Any]:
    stem = f"{start:08d}_{end:08d}"
    raw_path = raw_dir / f"{stem}.jsonl.gz"
    done_path = raw_dir / f"{stem}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if (
            prior.get("complete")
            and int(prior.get("start_height") or 0) == start
            and int(prior.get("end_height") or 0) == end
        ):
            return prior

    query = f"{EVENT_QUERY} AND tx.height >= {start} AND tx.height <= {end}"
    client = TxSearchClient(rpc_url)
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    tx_count = 0
    report_count = 0
    page = 1
    total: int | None = None
    seen_sources: set[tuple[str, int]] = set()
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        while total is None or tx_count < total:
            result = client.page(query, page)
            if total is None:
                total = int(result.get("total_count") or 0)
            txs = result.get("txs") or []
            if not txs:
                break
            for tx in txs:
                tx_count += 1
                rows = list(report_events(tx, start, end))
                if not rows:
                    raise RuntimeError(
                        f"indexed Tellor transaction has no new_report event: {tx.get('hash')}"
                    )
                for row in rows:
                    source = (str(row["source_tx"]), int(row["source_event_index"]))
                    if source in seen_sources:
                        raise RuntimeError(f"duplicate Tellor report event source: {source}")
                    seen_sources.add(source)
                    handle.write(
                        json.dumps(
                            row,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
                    report_count += 1
            page += 1
    if total is None or tx_count != total:
        raise RuntimeError(
            f"incomplete Tellor report segment {start}-{end}: {tx_count} != {total}"
        )
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "event_query": EVENT_QUERY,
        "start_height": start,
        "end_height": end,
        "transactions": tx_count,
        "reports": report_count,
        "pages": page - 1,
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    with PRINT_LOCK:
        print(
            f"Tellor report events {start}-{end}: "
            f"{report_count:,} reports in {tx_count:,} txs",
            flush=True,
        )
    return receipt


def iter_jsonl(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def write_outputs(
    rows: Iterable[dict[str, Any]],
    jsonl_path: Path,
    parquet_path: Path,
) -> tuple[int, int, int, int, int]:
    jsonl_tmp = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    parquet_tmp = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    batch: list[dict[str, Any]] = []
    count = 0
    first_height: int | None = None
    last_height: int | None = None
    non_monotonic = 0
    invalid_timestamps = 0
    with jsonl_tmp.open("w", encoding="utf-8") as jsonl:
        for row in rows:
            height = int(row["block_number"])
            if last_height is not None and height < last_height:
                non_monotonic += 1
            first_height = height if first_height is None else first_height
            last_height = height
            if int(row["timestamp_ms"]) > int(CUTOFF.timestamp() * 1_000):
                invalid_timestamps += 1
            jsonl.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
            batch.append(row)
            count += 1
            if len(batch) >= 100_000:
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    writer = pq.ParquetWriter(
                        parquet_tmp,
                        table.schema,
                        compression="zstd",
                    )
                writer.write_table(table)
                batch.clear()
        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(
                    parquet_tmp,
                    table.schema,
                    compression="zstd",
                )
            writer.write_table(table)
    if writer is None or first_height is None or last_height is None:
        raise RuntimeError("Tellor report-event history is empty")
    writer.close()
    jsonl_tmp.replace(jsonl_path)
    parquet_tmp.replace(parquet_path)
    return count, first_height, last_height, non_monotonic, invalid_timestamps


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Tellor reports from immutable tx events")
    parser.add_argument("--rpc-url", default="http://127.0.0.1:36657")
    parser.add_argument("--api-url", default="http://127.0.0.1:31317")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--segment-heights", type=int, default=100_000)
    parser.add_argument("--cutoff-height", type=int)
    args = parser.parse_args()

    chain = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = args.cutoff_height or chain.height_at_or_before(CUTOFF)
    cutoff_time = chain.block_time(cutoff_height).isoformat()
    if chain.block_time(cutoff_height) > CUTOFF:
        raise RuntimeError("configured Tellor cutoff height is after fixed cutoff")

    raw_dir = (ROOT / "data/raw/tellor_layer/report_events_full").resolve()
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
        futures = {
            executor.submit(collect_segment, args.rpc_url, start, end, raw_dir): (start, end)
            for start, end in segments
        }
        for future in as_completed(futures):
            receipts.append(future.result())
    by_start = {int(row["start_height"]): row for row in receipts}
    if set(by_start) != {start for start, _ in segments}:
        raise RuntimeError("Tellor report-event segment receipts are incomplete")
    raw_paths = [Path(by_start[start]["raw_file"]) for start, _ in segments]

    jsonl_path = curated_dir / "tellor_micro_reports.jsonl"
    parquet_path = curated_dir / "tellor_micro_reports.parquet"
    (
        report_count,
        first_height,
        last_height,
        non_monotonic,
        invalid_timestamps,
    ) = write_outputs(iter_jsonl(raw_paths), jsonl_path, parquet_path)
    receipt_reports = sum(int(row["reports"]) for row in receipts)
    transaction_count = sum(int(row["transactions"]) for row in receipts)
    manifest = {
        "dataset": "Tellor Layer complete immutable micro-report event ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "cutoff_block_time": cutoff_time,
        "source_event": "new_report",
        "event_query": EVENT_QUERY,
        "source_commit": SOURCE_COMMIT,
        "segments": len(segments),
        "report_transactions": transaction_count,
        "report_rows": report_count,
        "receipt_report_rows": receipt_reports,
        "first_report_height": first_height,
        "last_report_height": last_height,
        "non_monotonic_rows": non_monotonic,
        "post_cutoff_timestamp_rows": invalid_timestamps,
        "raw_directory": str(raw_dir),
        "curated_jsonl": str(jsonl_path),
        "curated_parquet": str(parquet_path),
        "all_required_assertions_pass": (
            report_count == receipt_reports
            and report_count >= transaction_count
            and report_count > 0
            and last_height <= cutoff_height
            and non_monotonic == 0
            and invalid_timestamps == 0
        ),
        "scope_guard": (
            "Each row is the immutable new_report transaction event emitted "
            "in the same successful SetValue call that stores MicroReport. "
            "All MicroReport research fields are event attributes. This avoids "
            "the module's 30-day state pruner; no report or reward is imputed."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor report-event QC failed: {manifest}")
    manifest_path = manifest_dir / "tellor_micro_reports.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Tellor full immutable report-event QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff height: {cutoff_height:,}

- `new_report` transactions: {transaction_count:,}.
- Micro-report events: {report_count:,}.
- Height range: {first_height:,}–{last_height:,}.
- Receipt/output difference: {receipt_reports - report_count}.
- Non-monotonic/post-cutoff rows: {non_monotonic}/{invalid_timestamps}.

`SetValue` emits `new_report` with the same reporter, power, query, value,
aggregation method, timestamp, cycle-list flag, and meta id written to
`MicroReport`. Transaction history is retained after the module prunes old
report state.
"""
    (ROOT / "reports/tellor_micro_reports_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
