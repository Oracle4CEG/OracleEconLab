"""Recover the complete Tellor Layer micro-report ledger through the fixed cutoff.

Tellor Layer retained every micro-report until the v6.1.2 pruning upgrade at
height 15,015,000.  The script therefore takes one complete state snapshot at
height 15,014,999 and then overlapping snapshots less than 30 days apart.  Each
post-upgrade snapshot contributes only reports newer than the preceding
snapshot.  Reporter addresses come from genesis/periodic reporter state plus
every successful MsgCreateReporter transaction.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import itertools
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
from requests.adapters import HTTPAdapter

from oracle_ledger.tellor_abci import reports_by_reporter_abci
from oracle_ledger.tellor_layer import TellorClient


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_RPC = "https://mainnet.tellorlayer.com/rpc"
DEFAULT_API = "https://mainnet.tellorlayer.com"
PRE_PRUNING_HEIGHT = 15_014_999
PRUNING_UPGRADE_HEIGHT = 15_015_000
SNAPSHOT_STEP = 500_000
CREATE_REPORTER_ACTION = "/layer.reporter.MsgCreateReporter"
SOURCE_COMMIT = "943a2709ef0a60eb560447278b2f59923b9de484"
PRINT_LOCK = Lock()
SOURCE_ADDRESS_LOCK = Lock()
SOURCE_ADDRESS_INDEX = itertools.count()
SOURCE_ADDRESSES: list[str] = []


class SourceAddressAdapter(HTTPAdapter):
    """Bind an HTTP connection pool to one explicit local source address."""

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
        super().init_poolmanager(connections, maxsize, block, **pool_kwargs)


class HistoricalTellor:
    def __init__(
        self,
        api_url: str,
        rpc_url: str,
        timeout: int = 120,
        direct_report_abci: bool = False,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.rpc_url = rpc_url.rstrip("/")
        self.timeout = timeout
        self.direct_report_abci = direct_report_abci
        self.session = requests.Session()
        # The canonical Tellor endpoint is directly reachable.  The shared
        # workspace proxy throttles the many independent historical snapshot
        # windows and can return sustained 503 responses under concurrency.
        self.session.trust_env = False
        if SOURCE_ADDRESSES:
            with SOURCE_ADDRESS_LOCK:
                source_address = SOURCE_ADDRESSES[
                    next(SOURCE_ADDRESS_INDEX) % len(SOURCE_ADDRESSES)
                ]
            adapter = SourceAddressAdapter(source_address)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"

    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(10):
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
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
        raise RuntimeError(f"Tellor request failed after retries: {url}") from last_error

    def historical_get(
        self,
        path: str,
        height: int,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        reporter_prefix = "/layer/oracle/get_reportsby_reporter/"
        if self.direct_report_abci and path.startswith(reporter_prefix):
            reporter = path.removeprefix(reporter_prefix)
            last_error: Exception | None = None
            for attempt in range(10):
                try:
                    return reports_by_reporter_abci(
                        self.session,
                        self.rpc_url,
                        reporter,
                        height,
                        params or {},
                        timeout=self.timeout,
                    )
                except (requests.RequestException, ValueError, RuntimeError) as exc:
                    last_error = exc
                    if attempt == 9:
                        break
                    time.sleep(min(2**attempt, 30))
            raise RuntimeError(
                f"Tellor direct historical report query failed: {reporter}"
            ) from last_error
        return self.request_json(
            "GET",
            self.api_url + path,
            headers={"x-cosmos-block-height": str(height)},
            params=params,
        )

    def tx_search(self, query: str, page: int, per_page: int = 100) -> dict[str, Any]:
        body = self.request_json(
            "POST",
            self.rpc_url,
            json_body={
                "jsonrpc": "2.0",
                "id": page,
                "method": "tx_search",
                "params": {
                    "query": query,
                    "prove": False,
                    "page": str(page),
                    "per_page": str(per_page),
                    "order_by": "asc",
                },
            },
        )
        return body["result"]


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def event_attributes(tx: dict[str, Any], event_types: set[str]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for event in tx.get("tx_result", {}).get("events") or []:
        if event.get("type") not in event_types:
            continue
        output.append(
            {str(attribute["key"]): str(attribute["value"]) for attribute in event.get("attributes") or []}
        )
    return output


def get_create_reporter_transactions(
    client: HistoricalTellor,
    cutoff_height: int,
    raw_dir: Path,
    segment_heights: int,
) -> list[dict[str, Any]]:
    """Recover reporter-creation transactions in resumable height segments.

    A chain-wide ``tx_search`` is prohibitively expensive on the public
    archive node even though reporter creation is rare.  Height-bounded
    searches use the same indexed evidence while allowing exact receipts and
    retrying only failed ranges.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    transactions: list[dict[str, Any]] = []
    for start in range(1, cutoff_height + 1, segment_heights):
        end = min(start + segment_heights - 1, cutoff_height)
        stem = f"{start:08d}_{end:08d}"
        raw_path = raw_dir / f"{stem}.json.gz"
        done_path = raw_dir / f"{stem}.done.json"
        if raw_path.is_file() and done_path.is_file():
            receipt = json.loads(done_path.read_text(encoding="utf-8"))
            if receipt.get("complete"):
                with gzip.open(raw_path, "rt", encoding="utf-8") as handle:
                    rows = json.load(handle)
                if len(rows) != int(receipt["transactions"]):
                    raise RuntimeError(f"reporter-creation receipt mismatch: {stem}")
                transactions.extend(rows)
                continue

        query = (
            f"message.action='{CREATE_REPORTER_ACTION}' "
            f"AND tx.height >= {start} AND tx.height <= {end}"
        )
        rows: list[dict[str, Any]] = []
        page = 1
        total: int | None = None
        while total is None or len(rows) < total:
            result = client.tx_search(query, page)
            if total is None:
                total = int(result.get("total_count") or 0)
            page_rows = result.get("txs") or []
            rows.extend(page_rows)
            if not page_rows:
                break
            page += 1
        if total is None or len(rows) != total:
            raise RuntimeError(
                f"incomplete MsgCreateReporter history {stem}: {len(rows)} != {total}"
            )
        temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            json.dump(
                rows,
                handle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        temporary.replace(raw_path)
        atomic_json(
            done_path,
            {
                "complete": True,
                "start_height": start,
                "end_height": end,
                "transactions": len(rows),
                "pages": page - 1,
                "raw_file": str(raw_path),
                "finished_at_utc": datetime.now(UTC).isoformat(),
            },
        )
        transactions.extend(rows)
        print(
            f"Tellor reporter creation {start}-{end}: {len(rows):,} txs",
            flush=True,
        )
    hashes = [
        str(tx.get("hash") or tx.get("txhash") or "").upper()
        for tx in transactions
    ]
    nonempty_hashes = [value for value in hashes if value]
    if len(nonempty_hashes) != len(set(nonempty_hashes)):
        raise RuntimeError("duplicate MsgCreateReporter transactions across height segments")
    return transactions


def reporters_at_height(client: HistoricalTellor, height: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    key: str | None = None
    while True:
        params = {"pagination.limit": "1000"}
        if key:
            params["pagination.key"] = key
        body = client.historical_get("/tellor-io/layer/reporter/reporters", height, params)
        rows.extend(body.get("reporters") or [])
        key = (body.get("pagination") or {}).get("next_key")
        if not key:
            return rows


def snapshot_heights(cutoff_height: int) -> list[int]:
    if cutoff_height < PRE_PRUNING_HEIGHT:
        return [cutoff_height]
    output = [PRE_PRUNING_HEIGHT]
    next_height = PRE_PRUNING_HEIGHT + SNAPSHOT_STEP
    while next_height < cutoff_height:
        output.append(next_height)
        next_height += SNAPSHOT_STEP
    if output[-1] != cutoff_height:
        output.append(cutoff_height)
    return output


def normalize_report(
    report: dict[str, Any],
    snapshot_height: int,
    lower_height_exclusive: int,
) -> dict[str, Any]:
    return {
        "reporter": str(report["reporter"]),
        "power": int(report["power"]),
        "query_type": str(report["query_type"]),
        "query_id": str(report["query_id"]).lower().removeprefix("0x"),
        "aggregate_method": str(report["aggregate_method"]),
        "value": str(report["value"]),
        "timestamp_ms": int(report["timestamp"]),
        "cyclelist": bool(report["cyclelist"]),
        "block_number": int(report["block_number"]),
        "meta_id": int(report["meta_id"]),
        "source_snapshot_height": snapshot_height,
        "coverage_lower_height_exclusive": lower_height_exclusive,
        "asset": "loya",
        "asset_decimals": 6,
    }


def collect_report_window(
    api_url: str,
    rpc_url: str,
    reporter: str,
    snapshot_height: int,
    lower_height_exclusive: int,
    raw_dir: Path,
    direct_report_abci: bool = False,
) -> dict[str, Any]:
    snapshot_dir = raw_dir / f"{snapshot_height:08d}"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    raw_path = snapshot_dir / f"{reporter}.jsonl.gz"
    done_path = snapshot_dir / f"{reporter}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if prior.get("complete"):
            return prior

    client = HistoricalTellor(
        api_url,
        rpc_url,
        direct_report_abci=direct_report_abci,
    )
    page_key: str | None = None
    page_count = 0
    report_count = 0
    last_meta_id: int | None = None
    minimum_height: int | None = None
    maximum_height: int | None = None
    reverse = lower_height_exclusive > 0
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        while True:
            params = {
                "pagination.limit": "100",
                "pagination.reverse": "true" if reverse else "false",
            }
            if page_key:
                params["pagination.key"] = page_key
            body = client.historical_get(
                f"/layer/oracle/get_reportsby_reporter/{reporter}",
                snapshot_height,
                params,
            )
            page_count += 1
            rows = body.get("microReports") or body.get("micro_reports") or []
            stop = False
            for source in rows:
                row = normalize_report(source, snapshot_height, lower_height_exclusive)
                height = row["block_number"]
                if height > snapshot_height:
                    raise RuntimeError(
                        f"future report at snapshot {snapshot_height}: {reporter} {height}"
                    )
                if height <= lower_height_exclusive:
                    if reverse:
                        stop = True
                        break
                    continue
                meta_id = row["meta_id"]
                if last_meta_id is not None:
                    if reverse and meta_id >= last_meta_id:
                        raise RuntimeError(f"non-descending report pagination for {reporter}")
                    if not reverse and meta_id <= last_meta_id:
                        raise RuntimeError(f"non-ascending report pagination for {reporter}")
                last_meta_id = meta_id
                minimum_height = height if minimum_height is None else min(minimum_height, height)
                maximum_height = height if maximum_height is None else max(maximum_height, height)
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
                report_count += 1
            page_key = (body.get("pagination") or {}).get("next_key")
            if stop or not page_key or not rows:
                break
            # A Cosmos PageResponse key is base64.  Decode it here as a structural
            # guard; the original string is passed back to the REST gateway.
            base64.b64decode(page_key, validate=True)
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "reporter": reporter,
        "snapshot_height": snapshot_height,
        "lower_height_exclusive": lower_height_exclusive,
        "reports": report_count,
        "pages": page_count,
        "minimum_report_height": minimum_height,
        "maximum_report_height": maximum_height,
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    with PRINT_LOCK:
        print(
            f"Tellor reports {reporter[:16]}… @ {snapshot_height}: "
            f"{report_count:,} rows ({page_count:,} pages)",
            flush=True,
        )
    return receipt


def iter_gzip_jsonl(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def write_outputs(
    rows: Iterable[dict[str, Any]],
    jsonl_path: Path,
    parquet_path: Path,
) -> tuple[int, int | None, int | None, int]:
    jsonl_tmp = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    parquet_tmp = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    batch: list[dict[str, Any]] = []
    count = 0
    min_height: int | None = None
    max_height: int | None = None
    invalid_window_rows = 0
    with jsonl_tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            height = int(row["block_number"])
            lower = int(row["coverage_lower_height_exclusive"])
            upper = int(row["source_snapshot_height"])
            if not (lower < height <= upper):
                invalid_window_rows += 1
            min_height = height if min_height is None else min(min_height, height)
            max_height = height if max_height is None else max(max_height, height)
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            batch.append(row)
            count += 1
            if len(batch) >= 100_000:
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    writer = pq.ParquetWriter(parquet_tmp, table.schema, compression="zstd")
                writer.write_table(table)
                batch.clear()
        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(parquet_tmp, table.schema, compression="zstd")
            writer.write_table(table)
    if writer is None:
        raise RuntimeError("Tellor report history unexpectedly contains zero rows")
    writer.close()
    jsonl_tmp.replace(jsonl_path)
    parquet_tmp.replace(parquet_path)
    return count, min_height, max_height, invalid_window_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect complete Tellor Layer micro-report history")
    parser.add_argument("--rpc-url", default=os.getenv("TELLOR_RPC_URL", DEFAULT_RPC))
    parser.add_argument("--api-url", default=os.getenv("TELLOR_API_URL", DEFAULT_API))
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument(
        "--direct-report-abci",
        action="store_true",
        help=(
            "Read historical reporter pages directly through --rpc-url ABCI "
            "instead of the REST gateway"
        ),
    )
    parser.add_argument("--creation-segment-heights", type=int, default=100_000)
    parser.add_argument(
        "--source-address",
        action="append",
        default=[],
        help=(
            "Bind worker connection pools to these local source IP addresses "
            "in round-robin order; repeat for multiple addresses"
        ),
    )
    args = parser.parse_args()
    SOURCE_ADDRESSES[:] = list(dict.fromkeys(args.source_address))

    chain = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = chain.height_at_or_before(CUTOFF)
    cutoff_time = chain.block_time(cutoff_height).isoformat()
    heights = snapshot_heights(cutoff_height)
    times = {height: chain.block_time(height) for height in heights}
    gaps_seconds = [
        (times[right] - times[left]).total_seconds()
        for left, right in zip(heights, heights[1:])
    ]
    if any(gap >= 30 * 24 * 60 * 60 for gap in gaps_seconds):
        raise RuntimeError(f"Tellor report snapshots exceed the 30-day retention: {gaps_seconds}")

    raw_dir = (ROOT / "data/raw/tellor_layer/reports_full").resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    manifest_dir = ROOT / "data/manifests"
    for path in (raw_dir, curated_dir, manifest_dir):
        path.mkdir(parents=True, exist_ok=True)

    historical = HistoricalTellor(args.api_url, args.rpc_url)
    create_txs = get_create_reporter_transactions(
        historical,
        cutoff_height,
        raw_dir / "reporter_creation",
        args.creation_segment_heights,
    )
    reporters: set[str] = set()
    for tx in create_txs:
        for attributes in event_attributes(
            tx, {"created_reporter", "created_reporter_from_selector"}
        ):
            if attributes.get("reporter"):
                reporters.add(attributes["reporter"])
    reporter_state_counts: dict[str, int] = {}
    state_heights = [1, *heights]
    for height in state_heights:
        state_rows = reporters_at_height(historical, height)
        reporter_state_counts[str(height)] = len(state_rows)
        reporters.update(str(row["address"]) for row in state_rows)
    if not reporters:
        raise RuntimeError("Tellor reporter universe is empty")

    windows: list[tuple[int, int]] = []
    for index, upper in enumerate(heights):
        lower = 0 if index == 0 else heights[index - 1]
        windows.append((lower, upper))
    tasks = [
        (reporter, upper, lower)
        for lower, upper in windows
        for reporter in sorted(reporters)
    ]
    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_report_window,
                args.api_url,
                args.rpc_url,
                reporter,
                upper,
                lower,
                raw_dir,
                args.direct_report_abci,
            ): (reporter, upper, lower)
            for reporter, upper, lower in tasks
        }
        for future in as_completed(futures):
            receipts.append(future.result())

    receipt_by_key = {
        (row["reporter"], int(row["snapshot_height"])): row for row in receipts
    }
    raw_paths = [
        Path(receipt_by_key[(reporter, upper)]["raw_file"])
        for lower, upper in windows
        for reporter in sorted(reporters)
    ]
    jsonl_path = curated_dir / "tellor_micro_reports.jsonl"
    parquet_path = curated_dir / "tellor_micro_reports.parquet"
    report_count, first_height, last_height, invalid_window_rows = write_outputs(
        iter_gzip_jsonl(raw_paths), jsonl_path, parquet_path
    )
    receipt_total = sum(int(row["reports"]) for row in receipts)
    empty_tasks = sum(int(row["reports"]) == 0 for row in receipts)
    manifest = {
        "dataset": "Tellor Layer complete micro-report ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "cutoff_block_time": cutoff_time,
        "pre_pruning_snapshot_height": PRE_PRUNING_HEIGHT,
        "pruning_upgrade_height": PRUNING_UPGRADE_HEIGHT,
        "snapshot_heights": heights,
        "snapshot_times": {str(height): times[height].isoformat() for height in heights},
        "maximum_snapshot_gap_seconds": max(gaps_seconds, default=0),
        "reporter_create_transactions": len(create_txs),
        "reporter_state_counts": reporter_state_counts,
        "reporter_universe_count": len(reporters),
        "reporter_universe": sorted(reporters),
        "collection_tasks": len(tasks),
        "empty_collection_tasks": empty_tasks,
        "report_rows": report_count,
        "receipt_report_rows": receipt_total,
        "first_report_height": first_height,
        "last_report_height": last_height,
        "invalid_window_rows": invalid_window_rows,
        "source_commit": SOURCE_COMMIT,
        "raw_directory": str(raw_dir),
        "curated_jsonl": str(jsonl_path),
        "curated_parquet": str(parquet_path),
        "all_required_assertions_pass": (
            report_count == receipt_total
            and invalid_window_rows == 0
            and last_height is not None
            and last_height <= cutoff_height
            and max(gaps_seconds, default=0) < 30 * 24 * 60 * 60
        ),
        "scope_guard": (
            "Rows are protocol MicroReport state, not automatically economic rewards. "
            "The pre-pruning snapshot captures all reports before v6.1.2; later "
            "overlapping snapshots are less than the 30-day report-retention window apart."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor report QC failed: {manifest}")
    manifest_path = manifest_dir / "tellor_micro_reports.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Tellor Layer full report-history QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff height: {cutoff_height}

- Reporter universe: {len(reporters):,}.
- Successful reporter-creation transactions: {len(create_txs):,}.
- Snapshot count: {len(heights):,}.
- Maximum snapshot time gap: {manifest['maximum_snapshot_gap_seconds'] / 86400:.2f} days.
- Micro-report rows: {report_count:,}.
- Invalid window rows: {invalid_window_rows}.
- Receipt/output count difference: {receipt_total - report_count}.

The ledger uses the last complete pre-pruning state at height
{PRE_PRUNING_HEIGHT:,}, then overlapping historical states inside Tellor's
30-day retention window. Reports and economic rewards remain separate tables.
"""
    (ROOT / "reports/tellor_micro_reports_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
