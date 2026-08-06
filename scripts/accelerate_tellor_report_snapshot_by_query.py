#!/usr/bin/env python3
"""Finish Tellor reporter tails through the canonical primary report index.

The public ``GetReportsbyReporter`` query walks a secondary index and performs
one primary-store lookup per report.  ``GetReportsbyQid`` walks the canonical
primary report collection directly.  This collector exhaustively probes the
query-id universe, collects every active reporter/query-id pair in disjoint
meta-id ranges, and materializes the exact raw shard format consumed by
``accelerate_tellor_report_snapshot.py``.

Completeness rests on two independently persisted facts:

* every query id in the cutoff aggregate ledger is probed at the historical
  snapshot (plus any query id already observed in canonical report shards);
* for an active reporter/query pair, forward and reverse probes establish its
  first and last meta id, after which every intervening range is traversed.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import itertools
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock, local
from typing import Any

import duckdb
import requests
from requests.adapters import HTTPAdapter

from accelerate_tellor_report_snapshot import (
    ROOT,
    atomic_json,
    decode_tellor_address,
    normalize_report,
)


THREAD_STATE = local()
PRINT_LOCK = Lock()
SOURCE_ADDRESS_LOCK = Lock()
SOURCE_ADDRESS_INDEX = itertools.count()
SOURCE_ADDRESSES: list[str] = []


class SourceAddressAdapter(HTTPAdapter):
    """Bind one worker connection pool to an explicit local address."""

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


def session() -> requests.Session:
    value = getattr(THREAD_STATE, "session", None)
    if value is None:
        value = requests.Session()
        value.trust_env = False
        value.headers["User-Agent"] = "oracle-accountability-atlas/0.1"
        if SOURCE_ADDRESSES:
            with SOURCE_ADDRESS_LOCK:
                source_address = SOURCE_ADDRESSES[
                    next(SOURCE_ADDRESS_INDEX) % len(SOURCE_ADDRESSES)
                ]
            adapter = SourceAddressAdapter(source_address)
            value.mount("https://", adapter)
            value.mount("http://", adapter)
        THREAD_STATE.session = value
    return value


def historical_qid_get(
    api_url: str,
    query_id: str,
    snapshot_height: int,
    params: dict[str, str],
) -> dict[str, Any]:
    url = api_url.rstrip("/") + f"/layer/oracle/get_reportsby_qid/{query_id}"
    last_error: Exception | None = None
    for attempt in range(12):
        try:
            response = session().get(
                url,
                headers={"x-cosmos-block-height": str(snapshot_height)},
                params=params,
                timeout=180,
            )
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or body.get("error"):
                raise RuntimeError(str(body))
            return body
        except (
            requests.RequestException,
            OSError,
            TimeoutError,
            ValueError,
            RuntimeError,
        ) as exc:
            last_error = exc
            if attempt == 11:
                break
            time.sleep(min(0.5 * 2 ** min(attempt, 6), 30))
    raise RuntimeError(
        f"Tellor historical query-id query failed: {query_id}"
    ) from last_error


def pagination_key(reporter_bytes: bytes, meta_id: int) -> str:
    # GetReportsbyQid paginates a Triple[query_id, reporter, meta_id].
    # The fixed query-id prefix is removed from PageResponse.NextKey, leaving
    # BytesKey(reporter) in non-terminal form followed by uint64(meta_id).
    raw = bytes([len(reporter_bytes)]) + reporter_bytes + meta_id.to_bytes(8, "big")
    return base64.b64encode(raw).decode("ascii")


def decoded_pagination_key(value: str) -> tuple[bytes, int]:
    raw = base64.b64decode(value, validate=True)
    if len(raw) != 29 or raw[0] != 20:
        raise RuntimeError("unexpected Tellor query-id pagination key")
    return raw[1:21], int.from_bytes(raw[21:], "big")


def response_rows(body: dict[str, Any]) -> list[dict[str, Any]]:
    rows = body.get("microReports") or body.get("micro_reports") or []
    if not isinstance(rows, list):
        raise RuntimeError("Tellor query-id response rows are not a list")
    return rows


def probe_pair(
    api_url: str,
    reporter: str,
    reporter_bytes: bytes,
    query_id: str,
    snapshot_height: int,
    start_meta: int,
    end_meta: int,
    probe_dir: Path,
) -> dict[str, Any]:
    path = probe_dir / f"{query_id}.json"
    if path.is_file():
        prior = json.loads(path.read_text(encoding="utf-8"))
        if (
            prior.get("complete")
            and prior.get("reporter") == reporter
            and prior.get("query_id") == query_id
            and int(prior.get("snapshot_height", -1)) == snapshot_height
            and int(prior.get("coverage_start_meta_inclusive", -1)) == start_meta
            and int(prior.get("coverage_end_meta_exclusive", -1)) == end_meta
        ):
            return prior

    first_body = historical_qid_get(
        api_url,
        query_id,
        snapshot_height,
        {
            "pagination.limit": "1",
            "pagination.key": pagination_key(reporter_bytes, start_meta),
        },
    )
    first_candidates = [
        row
        for row in response_rows(first_body)
        if row.get("reporter") == reporter
        and start_meta <= int(row["meta_id"]) < end_meta
    ]
    if not first_candidates:
        receipt = {
            "complete": True,
            "active": False,
            "reporter": reporter,
            "query_id": query_id,
            "snapshot_height": snapshot_height,
            "coverage_start_meta_inclusive": start_meta,
            "coverage_end_meta_exclusive": end_meta,
            "first_meta_id": None,
            "last_meta_id": None,
            "finished_at_utc": datetime.now(UTC).isoformat(),
        }
        atomic_json(path, receipt)
        return receipt

    first_meta = min(int(row["meta_id"]) for row in first_candidates)
    last_body = historical_qid_get(
        api_url,
        query_id,
        snapshot_height,
        {
            "pagination.limit": "1",
            "pagination.key": pagination_key(reporter_bytes, end_meta - 1),
            "pagination.reverse": "true",
        },
    )
    last_candidates = [
        row
        for row in response_rows(last_body)
        if row.get("reporter") == reporter
        and start_meta <= int(row["meta_id"]) < end_meta
    ]
    if not last_candidates:
        raise RuntimeError(
            f"Tellor reverse probe lost active pair: {reporter} {query_id}"
        )
    last_meta = max(int(row["meta_id"]) for row in last_candidates)
    if first_meta > last_meta:
        raise RuntimeError("Tellor query-id probe bounds are inverted")
    receipt = {
        "complete": True,
        "active": True,
        "reporter": reporter,
        "query_id": query_id,
        "snapshot_height": snapshot_height,
        "coverage_start_meta_inclusive": start_meta,
        "coverage_end_meta_exclusive": end_meta,
        "first_meta_id": first_meta,
        "last_meta_id": last_meta,
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(path, receipt)
    return receipt


def collect_query_range(
    api_url: str,
    reporter: str,
    reporter_bytes: bytes,
    query_id: str,
    snapshot_height: int,
    start_meta: int,
    end_meta: int,
    output_dir: Path,
) -> dict[str, Any]:
    query_dir = output_dir / query_id
    query_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{start_meta:010d}_{end_meta:010d}"
    raw_path = query_dir / f"{stem}.jsonl.gz"
    done_path = query_dir / f"{stem}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if (
            prior.get("complete")
            and prior.get("reporter") == reporter
            and prior.get("query_id") == query_id
            and int(prior.get("start_meta_inclusive", -1)) == start_meta
            and int(prior.get("end_meta_exclusive", -1)) == end_meta
        ):
            return prior

    key = pagination_key(reporter_bytes, start_meta)
    previous_page_key: str | None = None
    previous_meta: int | None = None
    pages = 0
    count = 0
    first_meta: int | None = None
    last_meta: int | None = None
    minimum_height: int | None = None
    maximum_height: int | None = None
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=3) as handle:
        while True:
            body = historical_qid_get(
                api_url,
                query_id,
                snapshot_height,
                {
                    "pagination.limit": "100",
                    "pagination.key": key,
                },
            )
            pages += 1
            rows = response_rows(body)
            reached_end = False
            saw_new_row = False
            for source in rows:
                if source.get("reporter") != reporter:
                    reached_end = True
                    break
                meta_id = int(source["meta_id"])
                if meta_id < start_meta:
                    raise RuntimeError("Tellor query-id range returned an earlier meta id")
                if meta_id >= end_meta:
                    reached_end = True
                    break
                # CollectionPaginate returns NextKey inclusively on the next
                # request.  Skip exactly that repeated boundary row.
                if previous_meta is not None and meta_id == previous_meta:
                    continue
                if previous_meta is not None and meta_id < previous_meta:
                    raise RuntimeError("Tellor query-id pagination moved backwards")
                row = normalize_report(source, snapshot_height)
                if row["query_id"] != query_id:
                    raise RuntimeError("Tellor query-id endpoint returned another query id")
                if int(row["block_number"]) > snapshot_height:
                    raise RuntimeError("Tellor historical query returned a future report")
                height = int(row["block_number"])
                minimum_height = (
                    height if minimum_height is None else min(minimum_height, height)
                )
                maximum_height = (
                    height if maximum_height is None else max(maximum_height, height)
                )
                first_meta = meta_id if first_meta is None else first_meta
                last_meta = meta_id
                previous_meta = meta_id
                saw_new_row = True
                handle.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                count += 1

            next_key = (body.get("pagination") or {}).get("next_key")
            if reached_end or not rows or not next_key:
                break
            next_reporter, next_meta = decoded_pagination_key(str(next_key))
            if next_reporter != reporter_bytes or next_meta >= end_meta:
                break
            if previous_page_key == next_key:
                raise RuntimeError("Tellor query-id pagination key did not advance")
            if previous_meta is not None and next_meta < previous_meta:
                raise RuntimeError("Tellor query-id pagination key moved backwards")
            if not saw_new_row and previous_meta is not None and next_meta == previous_meta:
                raise RuntimeError("Tellor query-id pagination repeated without progress")
            previous_page_key = str(next_key)
            key = str(next_key)

    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "reporter": reporter,
        "query_id": query_id,
        "snapshot_height": snapshot_height,
        "start_meta_inclusive": start_meta,
        "end_meta_exclusive": end_meta,
        "reports": count,
        "pages": pages,
        "first_meta_id": first_meta,
        "last_meta_id": last_meta,
        "minimum_report_height": minimum_height,
        "maximum_report_height": maximum_height,
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    with PRINT_LOCK:
        print(
            f"Tellor qid shard {reporter[:14]}… {query_id[:10]}… "
            f"{start_meta:,}-{end_meta - 1:,}: {count:,} reports/{pages:,} pages",
            flush=True,
        )
    return receipt


def query_id_universe(aggregate_parquet: Path, snapshot_dir: Path) -> list[str]:
    cache_path = snapshot_dir / ".query_shards" / "query_id_universe.json"
    aggregate_stat = aggregate_parquet.stat()
    if cache_path.is_file():
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
        values = cached.get("query_ids") or []
        if (
            cached.get("complete")
            and int(cached.get("aggregate_size_bytes", -1))
            == aggregate_stat.st_size
            and int(cached.get("aggregate_mtime_ns", -1))
            == aggregate_stat.st_mtime_ns
            and values
            and all(isinstance(value, str) and len(value) == 64 for value in values)
        ):
            return sorted(set(values))

    connection = duckdb.connect()
    aggregate_path = str(aggregate_parquet).replace("'", "''")
    rows = connection.execute(
        f"""
        SELECT DISTINCT lower(replace(query_id, '0x', '')) AS query_id
        FROM read_parquet('{aggregate_path}')
        ORDER BY query_id
        """
    ).fetchall()
    connection.close()
    values = {str(row[0]) for row in rows}

    # Union query ids already observed in canonical raw shards.  This makes the
    # universe resilient even if a historical report somehow never aggregated.
    for path in snapshot_dir.glob("*.jsonl.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                values.add(str(row["query_id"]).lower().removeprefix("0x"))
    for path in (snapshot_dir / ".shards").glob("*/*.jsonl.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                values.add(str(row["query_id"]).lower().removeprefix("0x"))

    invalid = sorted(value for value in values if len(value) != 64)
    if invalid:
        raise RuntimeError(f"invalid Tellor query ids in universe: {invalid[:3]}")
    output = sorted(values)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(
        cache_path,
        {
            "complete": True,
            "aggregate_parquet": str(aggregate_parquet),
            "aggregate_size_bytes": aggregate_stat.st_size,
            "aggregate_mtime_ns": aggregate_stat.st_mtime_ns,
            "query_id_count": len(output),
            "query_ids": output,
            "includes_query_ids_observed_in_existing_canonical_report_shards": True,
            "finished_at_utc": datetime.now(UTC).isoformat(),
        },
    )
    return output


def exact_complete_shards(
    shard_dir: Path,
    minimum_meta: int,
    maximum_meta_exclusive: int,
    shard_size: int,
) -> set[int]:
    complete: set[int] = set()
    for path in shard_dir.glob("*.done.json"):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
            start = int(row["start_meta_inclusive"])
            end = int(row["end_meta_exclusive"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            row.get("complete")
            and start >= minimum_meta
            and (start - minimum_meta) % shard_size == 0
            and end == min(start + shard_size, maximum_meta_exclusive)
        ):
            complete.add(start)
    return complete


def last_missing_run(
    shard_dir: Path,
    minimum_meta: int,
    maximum_meta_exclusive: int,
    shard_size: int,
) -> tuple[int, int] | None:
    complete = exact_complete_shards(
        shard_dir,
        minimum_meta,
        maximum_meta_exclusive,
        shard_size,
    )
    missing = [
        start
        for start in range(minimum_meta, maximum_meta_exclusive, shard_size)
        if start not in complete
    ]
    if not missing:
        return None
    final_start = missing[-1]
    run_start = final_start
    missing_set = set(missing)
    while run_start - shard_size in missing_set:
        run_start -= shard_size
    return run_start, maximum_meta_exclusive


def materialize_interval(
    reporter: str,
    snapshot_height: int,
    interval_start: int,
    interval_end: int,
    receipts: list[dict[str, Any]],
    standard_shard_dir: Path,
    standard_shard_size: int,
) -> tuple[int, int]:
    rows: list[dict[str, Any]] = []
    pages = 0
    for receipt in receipts:
        pages += int(receipt["pages"])
        with gzip.open(receipt["raw_file"], "rt", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                meta_id = int(row["meta_id"])
                if not interval_start <= meta_id < interval_end:
                    raise RuntimeError("Tellor query shard row escaped its interval")
                rows.append(row)
    rows.sort(key=lambda row: int(row["meta_id"]))
    for previous, current in zip(rows, rows[1:]):
        if int(previous["meta_id"]) == int(current["meta_id"]):
            raise RuntimeError(
                f"duplicate Tellor reporter/meta pair: {reporter} {current['meta_id']}"
            )

    total = 0
    written = 0
    index = 0
    for shard_start in range(interval_start, interval_end, standard_shard_size):
        shard_end = min(shard_start + standard_shard_size, interval_end)
        shard_rows: list[dict[str, Any]] = []
        while index < len(rows) and int(rows[index]["meta_id"]) < shard_end:
            if int(rows[index]["meta_id"]) < shard_start:
                raise RuntimeError("Tellor interval materialization moved backwards")
            shard_rows.append(rows[index])
            index += 1
        total += len(shard_rows)
        stem = f"{shard_start:010d}_{shard_end:010d}"
        raw_path = standard_shard_dir / f"{stem}.jsonl.gz"
        done_path = standard_shard_dir / f"{stem}.done.json"
        if raw_path.is_file() and done_path.is_file():
            prior = json.loads(done_path.read_text(encoding="utf-8"))
            if (
                prior.get("complete")
                and int(prior["start_meta_inclusive"]) == shard_start
                and int(prior["end_meta_exclusive"]) == shard_end
            ):
                # Tail ranges are selected from missing receipts.  Reaching a
                # pre-existing shard indicates concurrent modification.
                raise RuntimeError(f"standard Tellor shard appeared concurrently: {raw_path}")

        temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
        minimum_height = (
            min(int(row["block_number"]) for row in shard_rows)
            if shard_rows
            else None
        )
        maximum_height = (
            max(int(row["block_number"]) for row in shard_rows)
            if shard_rows
            else None
        )
        with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=3) as handle:
            for row in shard_rows:
                handle.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
        temporary.replace(raw_path)
        atomic_json(
            done_path,
            {
                "complete": True,
                "reporter": reporter,
                "snapshot_height": snapshot_height,
                "start_meta_inclusive": shard_start,
                "end_meta_exclusive": shard_end,
                "reports": len(shard_rows),
                "pages": pages,
                "first_meta_id": (
                    int(shard_rows[0]["meta_id"]) if shard_rows else None
                ),
                "last_meta_id": (
                    int(shard_rows[-1]["meta_id"]) if shard_rows else None
                ),
                "minimum_report_height": minimum_height,
                "maximum_report_height": maximum_height,
                "raw_file": str(raw_path),
                "collection_method": (
                    "exhaustive canonical GetReportsbyQid primary-index traversal"
                ),
                "finished_at_utc": datetime.now(UTC).isoformat(),
            },
        )
        written += 1
    if index != len(rows) or total != len(rows):
        raise RuntimeError("Tellor interval materialization count mismatch")
    return written, total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-height", type=int, default=15_014_999)
    parser.add_argument("--api-url", default="https://mainnet.tellorlayer.com")
    parser.add_argument(
        "--aggregate-parquet",
        type=Path,
        default=ROOT / "data/curated/tellor_aggregate_height_index.parquet",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=ROOT / "data/raw/tellor_layer/reports_full",
    )
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--standard-shard-size", type=int, default=1_000)
    parser.add_argument("--query-shard-size", type=int, default=50_000)
    parser.add_argument(
        "--source-address",
        action="append",
        default=[],
        help=(
            "Bind worker connection pools to these local source IP addresses "
            "in round-robin order; repeat for multiple addresses"
        ),
    )
    parser.add_argument(
        "--reporter-range",
        action="append",
        required=True,
        help="REPORTER:MIN_META_INCLUSIVE:MAX_META_INCLUSIVE",
    )
    args = parser.parse_args()
    SOURCE_ADDRESSES[:] = list(dict.fromkeys(args.source_address))
    if args.query_shard_size % args.standard_shard_size:
        raise SystemExit("--query-shard-size must be divisible by --standard-shard-size")

    snapshot_dir = args.raw_root / str(args.snapshot_height)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    query_ids = query_id_universe(args.aggregate_parquet, snapshot_dir)
    print(f"Tellor exhaustive query-id universe: {len(query_ids):,}", flush=True)

    parsed_ranges: list[tuple[str, int, int]] = []
    for value in args.reporter_range:
        reporter, minimum_text, maximum_text = value.split(":", 2)
        decode_tellor_address(reporter)
        parsed_ranges.append((reporter, int(minimum_text), int(maximum_text)))

    for reporter, minimum_meta, maximum_meta in parsed_ranges:
        maximum_exclusive = maximum_meta + 1
        standard_dir = snapshot_dir / ".shards" / reporter
        standard_dir.mkdir(parents=True, exist_ok=True)
        query_root = snapshot_dir / ".query_shards" / reporter
        prior_manifest_path = query_root / "manifest.json"
        if prior_manifest_path.is_file():
            prior_manifest = json.loads(
                prior_manifest_path.read_text(encoding="utf-8")
            )
            if (
                prior_manifest.get("all_required_assertions_pass") is True
                and prior_manifest.get("reporter") == reporter
                and int(prior_manifest.get("snapshot_height", -1))
                == args.snapshot_height
                and int(
                    prior_manifest.get("coverage_end_meta_exclusive", -1)
                )
                == maximum_exclusive
            ):
                print(
                    f"Tellor reporter query-index tail already complete: {reporter}",
                    flush=True,
                )
                continue
        tail = last_missing_run(
            standard_dir,
            minimum_meta,
            maximum_exclusive,
            args.standard_shard_size,
        )
        if tail is None:
            print(f"Tellor reporter already complete: {reporter}", flush=True)
            continue
        tail_start, tail_end = tail
        print(
            f"Tellor reporter primary-index tail: {reporter} "
            f"{tail_start:,}-{tail_end - 1:,}",
            flush=True,
        )

        probe_dir = query_root / "probes"
        range_dir = query_root / "ranges"
        probe_dir.mkdir(parents=True, exist_ok=True)
        range_dir.mkdir(parents=True, exist_ok=True)
        reporter_bytes = decode_tellor_address(reporter)

        probes: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = [
                executor.submit(
                    probe_pair,
                    args.api_url,
                    reporter,
                    reporter_bytes,
                    query_id,
                    args.snapshot_height,
                    tail_start,
                    tail_end,
                    probe_dir,
                )
                for query_id in query_ids
            ]
            for index, future in enumerate(as_completed(futures), 1):
                probes.append(future.result())
                if index % 50 == 0 or index == len(futures):
                    print(
                        f"Tellor qid probes {reporter[:14]}… "
                        f"{index:,}/{len(futures):,}",
                        flush=True,
                    )
        active = [row for row in probes if row["active"]]
        print(
            f"Tellor active query ids {reporter[:14]}…: "
            f"{len(active):,}/{len(probes):,}",
            flush=True,
        )

        tasks: list[tuple[str, int, int]] = []
        for probe in active:
            first = int(probe["first_meta_id"])
            last = int(probe["last_meta_id"])
            first_interval = (
                tail_start
                + ((first - tail_start) // args.query_shard_size)
                * args.query_shard_size
            )
            last_interval = (
                tail_start
                + ((last - tail_start) // args.query_shard_size)
                * args.query_shard_size
            )
            for start in range(
                first_interval,
                last_interval + args.query_shard_size,
                args.query_shard_size,
            ):
                tasks.append((str(probe["query_id"]), start, min(start + args.query_shard_size, tail_end)))

        receipts: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            future_map = {
                executor.submit(
                    collect_query_range,
                    args.api_url,
                    reporter,
                    reporter_bytes,
                    query_id,
                    args.snapshot_height,
                    start,
                    end,
                    range_dir,
                ): (query_id, start, end)
                for query_id, start, end in tasks
            }
            for index, future in enumerate(as_completed(future_map), 1):
                receipts.append(future.result())
                if index % 25 == 0 or index == len(future_map):
                    reports = sum(int(row["reports"]) for row in receipts)
                    print(
                        f"Tellor qid ranges {reporter[:14]}… "
                        f"{index:,}/{len(future_map):,}; {reports:,} reports",
                        flush=True,
                    )

        by_interval: dict[tuple[int, int], list[dict[str, Any]]] = {}
        for receipt in receipts:
            key = (
                int(receipt["start_meta_inclusive"]),
                int(receipt["end_meta_exclusive"]),
            )
            by_interval.setdefault(key, []).append(receipt)

        written_shards = 0
        written_reports = 0
        for interval_start in range(tail_start, tail_end, args.query_shard_size):
            interval_end = min(interval_start + args.query_shard_size, tail_end)
            written, reports = materialize_interval(
                reporter,
                args.snapshot_height,
                interval_start,
                interval_end,
                by_interval.get((interval_start, interval_end), []),
                standard_dir,
                args.standard_shard_size,
            )
            written_shards += written
            written_reports += reports
            print(
                f"Tellor materialized {reporter[:14]}… through "
                f"{interval_end - 1:,}: {written_reports:,} reports",
                flush=True,
            )

        manifest = {
            "all_required_assertions_pass": True,
            "reporter": reporter,
            "snapshot_height": args.snapshot_height,
            "coverage_start_meta_inclusive": tail_start,
            "coverage_end_meta_exclusive": tail_end,
            "query_id_universe_count": len(query_ids),
            "query_ids_probed": len(probes),
            "active_query_ids": len(active),
            "query_range_tasks": len(tasks),
            "query_range_receipts": len(receipts),
            "reports_materialized": written_reports,
            "standard_shards_materialized": written_shards,
            "collection_method": (
                "canonical historical GetReportsbyQid primary-index traversal"
            ),
            "finished_at_utc": datetime.now(UTC).isoformat(),
        }
        atomic_json(query_root / "manifest.json", manifest)
        print(json.dumps(manifest, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
