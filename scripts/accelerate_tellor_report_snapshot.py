#!/usr/bin/env python3
"""Finish large Tellor reporter snapshots with independent meta-id ranges.

Tellor's reporter index is ordered by ``reporter_address || meta_id``.  The
standard REST pagination key exposes that exact key, so disjoint meta-id
ranges can be traversed concurrently without changing the evidence source or
the resulting raw receipt format used by ``ingest_tellor_reports_full.py``.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import itertools
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock, local
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from websockets.exceptions import WebSocketException
from websockets.sync.client import connect as websocket_connect

from oracle_ledger.tellor_abci import (
    decode_reports_by_reporter_rpc,
    reports_by_reporter_abci,
    reports_by_reporter_rpc_request,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_API = "https://mainnet.tellorlayer.com"
DEFAULT_RPC = "https://mainnet.tellorlayer.com/rpc"
BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
THREAD_STATE = local()
PRINT_LOCK = Lock()
SOURCE_ADDRESS_LOCK = Lock()
SOURCE_ADDRESS_INDEX = itertools.count()
SOURCE_ADDRESSES: list[str] = []
USE_ENV_PROXY = False
DIRECT_ABCI_RPC_URL: str | None = None
DIRECT_ABCI_WEBSOCKET_URL: str | None = None


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


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def bech32_polymod(values: list[int]) -> int:
    generators = (0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)
    checksum = 1
    for value in values:
        top = checksum >> 25
        checksum = ((checksum & 0x1FFFFFF) << 5) ^ value
        for index, generator in enumerate(generators):
            if (top >> index) & 1:
                checksum ^= generator
    return checksum


def bech32_hrp_expand(hrp: str) -> list[int]:
    return [ord(value) >> 5 for value in hrp] + [0] + [
        ord(value) & 31 for value in hrp
    ]


def convert_bits(values: list[int], from_bits: int, to_bits: int) -> bytes:
    accumulator = 0
    bits = 0
    output = bytearray()
    max_value = (1 << to_bits) - 1
    for value in values:
        if value < 0 or value >> from_bits:
            raise ValueError("invalid bech32 data value")
        accumulator = (accumulator << from_bits) | value
        bits += from_bits
        while bits >= to_bits:
            bits -= to_bits
            output.append((accumulator >> bits) & max_value)
    if bits >= from_bits or ((accumulator << (to_bits - bits)) & max_value):
        raise ValueError("non-zero bech32 padding")
    return bytes(output)


def decode_tellor_address(value: str) -> bytes:
    if value.lower() != value or not value.startswith("tellor1"):
        raise ValueError(f"invalid Tellor bech32 address: {value}")
    separator = value.rfind("1")
    hrp = value[:separator]
    encoded = [BECH32_CHARSET.find(character) for character in value[separator + 1 :]]
    if any(item < 0 for item in encoded) or len(encoded) < 6:
        raise ValueError(f"invalid Tellor bech32 payload: {value}")
    if bech32_polymod(bech32_hrp_expand(hrp) + encoded) != 1:
        raise ValueError(f"invalid Tellor bech32 checksum: {value}")
    decoded = convert_bits(encoded[:-6], 5, 8)
    if len(decoded) != 20:
        raise ValueError(f"unexpected Tellor address width: {len(decoded)}")
    return decoded


def session() -> requests.Session:
    value = getattr(THREAD_STATE, "session", None)
    if value is None:
        value = requests.Session()
        # The official Tellor endpoint is directly reachable.  The shared
        # workspace proxy rate-limits concurrent historical queries and can
        # emit sustained 503s, while direct sessions preserve independent
        # TCP pools and the same canonical application-state response.
        value.trust_env = USE_ENV_PROXY
        if SOURCE_ADDRESSES:
            with SOURCE_ADDRESS_LOCK:
                source_address = SOURCE_ADDRESSES[
                    next(SOURCE_ADDRESS_INDEX) % len(SOURCE_ADDRESSES)
                ]
            adapter = SourceAddressAdapter(source_address)
            value.mount("https://", adapter)
            value.mount("http://", adapter)
        value.headers["User-Agent"] = "oracle-accountability-atlas/0.1"
        THREAD_STATE.session = value
    return value


def historical_get(
    api_url: str,
    reporter: str,
    snapshot_height: int,
    params: dict[str, str],
) -> dict[str, Any]:
    url = (
        api_url.rstrip("/")
        + f"/layer/oracle/get_reportsby_reporter/{reporter}"
    )
    last_error: Exception | None = None
    for attempt in range(12):
        try:
            if DIRECT_ABCI_WEBSOCKET_URL:
                websocket = getattr(THREAD_STATE, "websocket", None)
                if websocket is None:
                    websocket = websocket_connect(
                        DIRECT_ABCI_WEBSOCKET_URL,
                        open_timeout=30,
                        close_timeout=1,
                        ping_interval=None,
                        max_size=16 * 1024 * 1024,
                    )
                    THREAD_STATE.websocket = websocket
                identifier, payload = reports_by_reporter_rpc_request(
                    reporter,
                    snapshot_height,
                    params,
                )
                websocket.send(
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                raw = websocket.recv(timeout=180)
                if not isinstance(raw, str):
                    raise RuntimeError("Tellor ABCI WebSocket returned binary data")
                return decode_reports_by_reporter_rpc(
                    json.loads(raw),
                    identifier,
                    snapshot_height,
                )
            if DIRECT_ABCI_RPC_URL:
                return reports_by_reporter_abci(
                    session(),
                    DIRECT_ABCI_RPC_URL,
                    reporter,
                    snapshot_height,
                    params,
                    timeout=180,
                )
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
            WebSocketException,
            OSError,
            TimeoutError,
            ValueError,
            RuntimeError,
        ) as exc:
            last_error = exc
            websocket = getattr(THREAD_STATE, "websocket", None)
            if websocket is not None:
                try:
                    websocket.close()
                except Exception:
                    pass
                THREAD_STATE.websocket = None
            if attempt == 11:
                break
            time.sleep(min(0.5 * 2 ** min(attempt, 6), 30))
    raise RuntimeError(
        f"Tellor historical reporter query failed: {reporter}"
    ) from last_error


def normalize_report(
    report: dict[str, Any],
    snapshot_height: int,
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
        "coverage_lower_height_exclusive": 0,
        "asset": "loya",
        "asset_decimals": 6,
    }


def collect_shard(
    api_url: str,
    reporter: str,
    address: bytes,
    snapshot_height: int,
    start_meta: int,
    end_meta: int,
    shard_dir: Path,
) -> dict[str, Any]:
    stem = f"{start_meta:010d}_{end_meta:010d}"
    raw_path = shard_dir / f"{stem}.jsonl.gz"
    done_path = shard_dir / f"{stem}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if (
            prior.get("complete")
            and int(prior["start_meta_inclusive"]) == start_meta
            and int(prior["end_meta_exclusive"]) == end_meta
        ):
            return prior

    page_key = address + start_meta.to_bytes(8, "big")
    pages = 0
    count = 0
    first_meta: int | None = None
    last_meta: int | None = None
    minimum_height: int | None = None
    maximum_height: int | None = None
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        while True:
            body = historical_get(
                api_url,
                reporter,
                snapshot_height,
                {
                    "pagination.limit": "100",
                    "pagination.key": base64.b64encode(page_key).decode(),
                },
            )
            pages += 1
            rows = body.get("microReports") or body.get("micro_reports") or []
            reached_end = False
            for source in rows:
                row = normalize_report(source, snapshot_height)
                meta_id = int(row["meta_id"])
                if meta_id < start_meta:
                    raise RuntimeError(
                        f"Tellor shard returned meta {meta_id} before {start_meta}"
                    )
                if meta_id >= end_meta:
                    reached_end = True
                    break
                if row["reporter"] != reporter:
                    raise RuntimeError("Tellor reporter index returned another reporter")
                if int(row["block_number"]) > snapshot_height:
                    raise RuntimeError("Tellor historical query returned a future report")
                if last_meta is not None and meta_id <= last_meta:
                    raise RuntimeError("Tellor shard pagination is not strictly ascending")
                first_meta = meta_id if first_meta is None else first_meta
                last_meta = meta_id
                height = int(row["block_number"])
                minimum_height = (
                    height if minimum_height is None else min(minimum_height, height)
                )
                maximum_height = (
                    height if maximum_height is None else max(maximum_height, height)
                )
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
            next_key_text = (body.get("pagination") or {}).get("next_key")
            if reached_end or not rows or not next_key_text:
                break
            next_key = base64.b64decode(next_key_text, validate=True)
            if len(next_key) != 28 or next_key[:20] != address:
                raise RuntimeError("unexpected Tellor reporter pagination key")
            next_meta = int.from_bytes(next_key[20:], "big")
            if next_meta <= (last_meta if last_meta is not None else start_meta - 1):
                raise RuntimeError("Tellor reporter pagination key did not advance")
            if next_meta >= end_meta:
                break
            page_key = next_key
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "reporter": reporter,
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
            f"Tellor shard {reporter[:16]}… {start_meta:,}-{end_meta - 1:,}: "
            f"{count:,} reports in {pages:,} pages",
            flush=True,
        )
    return receipt


def reporter_max_meta(api_url: str, reporter: str, height: int) -> int:
    body = historical_get(
        api_url,
        reporter,
        height,
        {"pagination.limit": "1", "pagination.reverse": "true"},
    )
    rows = body.get("microReports") or body.get("micro_reports") or []
    if not rows:
        raise RuntimeError(f"reporter has no reports at height {height}: {reporter}")
    return int(rows[0]["meta_id"])


def reporter_min_meta(api_url: str, reporter: str, height: int) -> int:
    body = historical_get(
        api_url,
        reporter,
        height,
        {"pagination.limit": "1"},
    )
    rows = body.get("microReports") or body.get("micro_reports") or []
    if not rows:
        raise RuntimeError(f"reporter has no reports at height {height}: {reporter}")
    return int(rows[0]["meta_id"])


def write_seeded_shard(
    reporter: str,
    snapshot_height: int,
    start_meta: int,
    end_meta: int,
    rows: list[dict[str, Any]],
    shard_dir: Path,
) -> bool:
    """Commit a complete shard proven by an earlier sequential traversal."""
    stem = f"{start_meta:010d}_{end_meta:010d}"
    raw_path = shard_dir / f"{stem}.jsonl.gz"
    done_path = shard_dir / f"{stem}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if (
            prior.get("complete")
            and int(prior["start_meta_inclusive"]) == start_meta
            and int(prior["end_meta_exclusive"]) == end_meta
        ):
            return False
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    minimum_height: int | None = None
    maximum_height: int | None = None
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        for row in rows:
            height = int(row["block_number"])
            minimum_height = (
                height if minimum_height is None else min(minimum_height, height)
            )
            maximum_height = (
                height if maximum_height is None else max(maximum_height, height)
            )
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
            "start_meta_inclusive": start_meta,
            "end_meta_exclusive": end_meta,
            "reports": len(rows),
            "pages": max(1, math.ceil(len(rows) / 100)),
            "first_meta_id": int(rows[0]["meta_id"]) if rows else None,
            "last_meta_id": int(rows[-1]["meta_id"]) if rows else None,
            "minimum_report_height": minimum_height,
            "maximum_report_height": maximum_height,
            "raw_file": str(raw_path),
            "collection_method": (
                "complete prefix from prior canonical sequential REST traversal"
            ),
            "finished_at_utc": datetime.now(UTC).isoformat(),
        },
    )
    return True


def seed_shards_from_partial(
    reporter: str,
    snapshot_height: int,
    minimum_meta: int,
    maximum_meta: int,
    shard_size: int,
    snapshot_dir: Path,
    shard_dir: Path,
) -> tuple[int, int]:
    """Reuse complete ranges from an interrupted sequential snapshot.

    The earlier collector traversed the same reporter index in strictly
    ascending order. Seeing the first row of a later shard proves every prior
    shard is complete, even when the gzip's final line was interrupted.
    """
    partial = snapshot_dir / f"{reporter}.jsonl.gz.tmp"
    if not partial.is_file():
        return 0, 0
    bucket_start = minimum_meta
    bucket_end = min(bucket_start + shard_size, maximum_meta + 1)
    bucket_rows: list[dict[str, Any]] = []
    seeded_shards = 0
    seeded_rows = 0
    previous_meta: int | None = None
    stream_complete = False
    try:
        with gzip.open(partial, "rt", encoding="utf-8") as source:
            for line in source:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    break
                meta_id = int(row["meta_id"])
                if row.get("reporter") != reporter:
                    raise RuntimeError("partial Tellor snapshot has another reporter")
                if int(row.get("source_snapshot_height", -1)) != snapshot_height:
                    raise RuntimeError("partial Tellor snapshot height mismatch")
                if meta_id < minimum_meta or meta_id > maximum_meta:
                    raise RuntimeError("partial Tellor snapshot meta id outside bounds")
                if previous_meta is not None and meta_id <= previous_meta:
                    raise RuntimeError("partial Tellor snapshot is not strictly ordered")
                if previous_meta is None and meta_id != minimum_meta:
                    raise RuntimeError("partial Tellor snapshot does not start at first meta id")
                while meta_id >= bucket_end:
                    if write_seeded_shard(
                        reporter,
                        snapshot_height,
                        bucket_start,
                        bucket_end,
                        bucket_rows,
                        shard_dir,
                    ):
                        seeded_shards += 1
                        seeded_rows += len(bucket_rows)
                    bucket_rows = []
                    bucket_start = bucket_end
                    bucket_end = min(bucket_start + shard_size, maximum_meta + 1)
                bucket_rows.append(row)
                previous_meta = meta_id
            else:
                stream_complete = True
    except (EOFError, OSError):
        stream_complete = False
    if stream_complete and previous_meta == maximum_meta:
        if write_seeded_shard(
            reporter,
            snapshot_height,
            bucket_start,
            bucket_end,
            bucket_rows,
            shard_dir,
        ):
            seeded_shards += 1
            seeded_rows += len(bucket_rows)
    return seeded_shards, seeded_rows


def merge_reporter(
    reporter: str,
    snapshot_height: int,
    receipts: list[dict[str, Any]],
    snapshot_dir: Path,
) -> dict[str, Any]:
    output = snapshot_dir / f"{reporter}.jsonl.gz"
    temporary = output.with_suffix(output.suffix + ".tmp")
    report_count = 0
    pages = 0
    minimum_height: int | None = None
    maximum_height: int | None = None
    previous_meta: int | None = None
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as target:
        for receipt in sorted(
            receipts, key=lambda row: int(row["start_meta_inclusive"])
        ):
            pages += int(receipt["pages"])
            with gzip.open(receipt["raw_file"], "rt", encoding="utf-8") as source:
                for line in source:
                    row = json.loads(line)
                    meta_id = int(row["meta_id"])
                    if previous_meta is not None and meta_id <= previous_meta:
                        raise RuntimeError(
                            f"duplicate/non-monotonic Tellor meta id {meta_id}"
                        )
                    previous_meta = meta_id
                    height = int(row["block_number"])
                    minimum_height = (
                        height
                        if minimum_height is None
                        else min(minimum_height, height)
                    )
                    maximum_height = (
                        height
                        if maximum_height is None
                        else max(maximum_height, height)
                    )
                    target.write(
                        json.dumps(
                            row,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
                    report_count += 1
    expected_count = sum(int(row["reports"]) for row in receipts)
    if report_count != expected_count:
        raise RuntimeError(
            f"Tellor shard merge count mismatch: {report_count} != {expected_count}"
        )
    temporary.replace(output)
    receipt = {
        "complete": True,
        "reporter": reporter,
        "snapshot_height": snapshot_height,
        "lower_height_exclusive": 0,
        "reports": report_count,
        "pages": pages,
        "minimum_report_height": minimum_height,
        "maximum_report_height": maximum_height,
        "raw_file": str(output),
        "collection_method": "disjoint reporter-meta-id range pagination",
        "shards": len(receipts),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(snapshot_dir / f"{reporter}.done.json", receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-height", type=int, default=15_014_999)
    parser.add_argument("--api-url", default=DEFAULT_API)
    parser.add_argument(
        "--direct-abci-rpc-url",
        default=None,
        help=(
            "Query the same historical protobuf state directly through this "
            "archive CometBFT RPC instead of the REST gateway"
        ),
    )
    parser.add_argument(
        "--direct-abci-websocket-url",
        default=None,
        help=(
            "Query historical protobuf state over persistent CometBFT "
            "WebSocket connections; takes precedence over HTTP ABCI"
        ),
    )
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--meta-shard-size", type=int, default=100_000)
    parser.add_argument("--reporter", action="append", default=[])
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
        "--use-env-proxy",
        action="store_true",
        help="Use HTTP(S)_PROXY from the environment for this independent pass",
    )
    parser.add_argument(
        "--skip-partial-seed",
        action="store_true",
        help="Skip rescanning prior sequential .tmp files after shards were seeded",
    )
    args = parser.parse_args()
    global USE_ENV_PROXY, DIRECT_ABCI_RPC_URL, DIRECT_ABCI_WEBSOCKET_URL
    USE_ENV_PROXY = args.use_env_proxy
    DIRECT_ABCI_RPC_URL = args.direct_abci_rpc_url
    DIRECT_ABCI_WEBSOCKET_URL = args.direct_abci_websocket_url
    SOURCE_ADDRESSES[:] = list(dict.fromkeys(args.source_address))

    snapshot_dir = (
        ROOT / f"data/raw/tellor_layer/reports_full/{args.snapshot_height:08d}"
    ).resolve()
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    reporters = list(args.reporter)
    if not reporters:
        reporters = sorted(
            path.name.removesuffix(".jsonl.gz.tmp")
            for path in snapshot_dir.glob("*.jsonl.gz.tmp")
            if not (snapshot_dir / path.name.removesuffix(".jsonl.gz.tmp")).with_suffix(
                ".done.json"
            ).is_file()
        )
    if not reporters:
        raise RuntimeError("no incomplete Tellor reporters selected")

    tasks: list[tuple[str, bytes, int, int, Path]] = []
    minimums: dict[str, int] = {}
    maximums: dict[str, int] = {}
    for reporter in reporters:
        address = decode_tellor_address(reporter)
        minimum = reporter_min_meta(
            args.api_url, reporter, args.snapshot_height
        )
        maximum = reporter_max_meta(
            args.api_url, reporter, args.snapshot_height
        )
        if minimum > maximum:
            raise RuntimeError(
                f"Tellor reporter meta-id bounds are inverted: {minimum} > {maximum}"
            )
        minimums[reporter] = minimum
        maximums[reporter] = maximum
        shard_dir = snapshot_dir / ".shards" / reporter
        shard_dir.mkdir(parents=True, exist_ok=True)
        if args.skip_partial_seed:
            seeded_shards, seeded_rows = 0, 0
        else:
            seeded_shards, seeded_rows = seed_shards_from_partial(
                reporter,
                args.snapshot_height,
                minimum,
                maximum,
                args.meta_shard_size,
                snapshot_dir,
                shard_dir,
            )
        end_exclusive = maximum + 1
        for start in range(minimum, end_exclusive, args.meta_shard_size):
            tasks.append(
                (
                    reporter,
                    address,
                    start,
                    min(start + args.meta_shard_size, end_exclusive),
                    shard_dir,
                )
            )
        print(
            f"Tellor reporter {reporter}: meta-id {minimum:,}-{maximum:,}, "
            f"{math.ceil((maximum - minimum + 1) / args.meta_shard_size):,} shards; "
            f"seeded {seeded_shards:,} shards/{seeded_rows:,} rows",
            flush=True,
        )

    by_reporter: dict[str, list[dict[str, Any]]] = {
        reporter: [] for reporter in reporters
    }
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_shard,
                args.api_url,
                reporter,
                address,
                args.snapshot_height,
                start,
                end,
                shard_dir,
            ): reporter
            for reporter, address, start, end, shard_dir in tasks
        }
        for future in as_completed(futures):
            by_reporter[futures[future]].append(future.result())

    for reporter in reporters:
        expected_shards = math.ceil(
            (maximums[reporter] - minimums[reporter] + 1)
            / args.meta_shard_size
        )
        if len(by_reporter[reporter]) != expected_shards:
            raise RuntimeError("Tellor reporter shard receipt count mismatch")
        receipt = merge_reporter(
            reporter,
            args.snapshot_height,
            by_reporter[reporter],
            snapshot_dir,
        )
        print(
            f"Tellor reporter snapshot complete {reporter}: "
            f"{receipt['reports']:,} rows",
            flush=True,
        )


if __name__ == "__main__":
    main()
