"""Export Tellor's complete on-chain aggregate-height index at the fixed cutoff.

Cosmos SDK collections stores the oracle aggregate secondary index under:

    0x13 || uint64_be(aggregate_height) || 0x20 || query_id || uint64_be(timestamp_ms)

The archive node's ``/store/oracle/subspace`` ABCI query can export a prefix.
Using the first six bytes of the height partitions the index into exact,
non-overlapping 65,536-height buckets, avoiding both block-by-block scans and
unbounded whole-store responses.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import time
from collections import Counter
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
DEFAULT_RPC = "https://mainnet.tellorlayer.com/rpc"
DEFAULT_API = "https://mainnet.tellorlayer.com"
SOURCE_COMMIT = "943a2709ef0a60eb560447278b2f59923b9de484"
AGGREGATES_HEIGHT_INDEX_PREFIX = b"\x13"
HEIGHT_BUCKET_BITS = 16
HEIGHT_PREFIX_BYTES = 6
PRINT_LOCK = Lock()


def read_varint(payload: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if offset >= len(payload) or shift >= 70:
            raise ValueError("invalid protobuf varint")
        byte = payload[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, offset
        shift += 7


def protobuf_fields(payload: bytes) -> Iterator[tuple[int, int, Any]]:
    offset = 0
    while offset < len(payload):
        tag, offset = read_varint(payload, offset)
        field_number = tag >> 3
        wire_type = tag & 7
        if wire_type == 0:
            value, offset = read_varint(payload, offset)
        elif wire_type == 1:
            if offset + 8 > len(payload):
                raise ValueError("truncated fixed64 protobuf field")
            value = payload[offset : offset + 8]
            offset += 8
        elif wire_type == 2:
            length, offset = read_varint(payload, offset)
            end = offset + length
            if end > len(payload):
                raise ValueError("truncated length-delimited protobuf field")
            value = payload[offset:end]
            offset = end
        elif wire_type == 5:
            if offset + 4 > len(payload):
                raise ValueError("truncated fixed32 protobuf field")
            value = payload[offset : offset + 4]
            offset += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire_type}")
        yield field_number, wire_type, value


def parse_subspace_pair_keys(payload: bytes) -> Iterator[bytes]:
    """Decode the SDK ``Pairs`` response while retaining the raw store keys."""
    for field_number, wire_type, pair_payload in protobuf_fields(payload):
        if field_number != 1 or wire_type != 2:
            raise ValueError("unexpected field in Cosmos subspace Pairs response")
        key: bytes | None = None
        for pair_field, pair_wire, value in protobuf_fields(pair_payload):
            if pair_field == 1 and pair_wire == 2:
                key = value
            elif pair_field == 2 and pair_wire == 2:
                # Multi indexes use NoValue, so this is normally absent.
                continue
            else:
                raise ValueError("unexpected field in Cosmos subspace Pair")
        if key is None:
            raise ValueError("Cosmos subspace Pair has no key")
        yield key


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


class ArchiveRpc:
    def __init__(self, url: str, timeout: int = 180) -> None:
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"

    def aggregate_index_prefix(self, prefix: bytes, height: int) -> bytes:
        payload = {
            "jsonrpc": "2.0",
            "id": prefix.hex(),
            "method": "abci_query",
            "params": {
                "path": "/store/oracle/subspace",
                "data": prefix.hex(),
                "height": str(height),
                "prove": False,
            },
        }
        last_error: Exception | None = None
        for attempt in range(10):
            try:
                response = self.session.post(
                    self.url,
                    json=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    raise RuntimeError(str(body["error"]))
                result = body["result"]["response"]
                if int(result.get("code") or 0) != 0:
                    raise RuntimeError(str(result))
                returned_prefix = base64.b64decode(result.get("key") or "")
                if returned_prefix != prefix:
                    raise RuntimeError(
                        f"ABCI prefix mismatch {returned_prefix.hex()} != {prefix.hex()}"
                    )
                return base64.b64decode(result.get("value") or "")
            except (requests.RequestException, ValueError, KeyError, RuntimeError) as exc:
                last_error = exc
                if attempt == 9:
                    break
                time.sleep(min(2**attempt, 30))
        raise RuntimeError(
            f"Tellor aggregate index query failed for prefix {prefix.hex()}"
        ) from last_error


def decode_index_key(key: bytes, bucket: int) -> dict[str, Any]:
    if len(key) != 50:
        raise ValueError(f"unexpected aggregate-height index key width: {len(key)}")
    if key[0:1] != AGGREGATES_HEIGHT_INDEX_PREFIX or key[9] != 32:
        raise ValueError(f"unexpected aggregate-height index key: {key.hex()}")
    height = int.from_bytes(key[1:9], "big")
    if height >> HEIGHT_BUCKET_BITS != bucket:
        raise ValueError(
            f"aggregate height {height} does not belong to bucket {bucket}"
        )
    return {
        "aggregate_height": height,
        "query_id": key[10:42].hex(),
        "aggregate_timestamp_ms": int.from_bytes(key[42:50], "big"),
        "state_index_key_hex": key.hex(),
        "state_height": None,
        "chain_id": "tellor-1",
    }


def collect_bucket(
    rpc_url: str,
    state_height: int,
    bucket: int,
    raw_dir: Path,
) -> dict[str, Any]:
    start = bucket << HEIGHT_BUCKET_BITS
    end = start + (1 << HEIGHT_BUCKET_BITS) - 1
    stem = f"{start:08d}_{end:08d}"
    raw_path = raw_dir / f"{stem}.pb.gz"
    done_path = raw_dir / f"{stem}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if (
            prior.get("complete")
            and int(prior.get("state_height") or 0) == state_height
            and int(prior.get("bucket") or -1) == bucket
        ):
            return prior

    prefix = (
        AGGREGATES_HEIGHT_INDEX_PREFIX
        + bucket.to_bytes(HEIGHT_PREFIX_BYTES, "big")
    )
    payload = ArchiveRpc(rpc_url).aggregate_index_prefix(prefix, state_height)
    keys = list(parse_subspace_pair_keys(payload))
    for key in keys:
        decode_index_key(key, bucket)

    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with gzip.open(temporary, "wb", compresslevel=6) as handle:
        handle.write(payload)
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "bucket": bucket,
        "start_height": start,
        "end_height": end,
        "state_height": state_height,
        "prefix_hex": prefix.hex(),
        "rows": len(keys),
        "uncompressed_protobuf_bytes": len(payload),
        "sha256_uncompressed_protobuf": hashlib.sha256(payload).hexdigest(),
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    with PRINT_LOCK:
        print(
            f"Tellor aggregate index {start}-{end}: {len(keys):,} rows",
            flush=True,
        )
    return receipt


def iter_bucket_rows(
    raw_paths: Iterable[tuple[int, Path]],
    state_height: int,
    maximum_aggregate_height: int | None = None,
) -> Iterator[dict[str, Any]]:
    for bucket, path in raw_paths:
        with gzip.open(path, "rb") as handle:
            payload = handle.read()
        for key in parse_subspace_pair_keys(payload):
            row = decode_index_key(key, bucket)
            if (
                maximum_aggregate_height is not None
                and int(row["aggregate_height"]) > maximum_aggregate_height
            ):
                continue
            row["state_height"] = state_height
            yield row


def write_outputs(
    rows: Iterable[dict[str, Any]],
    jsonl_path: Path,
    parquet_path: Path,
) -> tuple[int, int | None, int | None, int, Counter[str]]:
    jsonl_tmp = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    parquet_tmp = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    batch: list[dict[str, Any]] = []
    count = 0
    first_height: int | None = None
    last_height: int | None = None
    non_monotonic = 0
    query_counts: Counter[str] = Counter()
    with jsonl_tmp.open("w", encoding="utf-8") as jsonl:
        for row in rows:
            height = int(row["aggregate_height"])
            if last_height is not None and height < last_height:
                non_monotonic += 1
            first_height = height if first_height is None else first_height
            last_height = height
            query_counts[str(row["query_id"])] += 1
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
    if writer is None:
        raise RuntimeError("Tellor aggregate-height index is empty")
    writer.close()
    jsonl_tmp.replace(jsonl_path)
    parquet_tmp.replace(parquet_path)
    return count, first_height, last_height, non_monotonic, query_counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Tellor's complete aggregate-height state index"
    )
    parser.add_argument(
        "--rpc-url",
        default=os.getenv("TELLOR_RPC_URL", DEFAULT_RPC),
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("TELLOR_API_URL", DEFAULT_API),
    )
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    chain = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = chain.height_at_or_before(CUTOFF)
    cutoff_time = chain.block_time(cutoff_height).isoformat()
    raw_dir = (
        ROOT / "data/raw/tellor_layer/aggregate_height_index"
    ).resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    manifest_dir = ROOT / "data/manifests"
    for path in (raw_dir, curated_dir, manifest_dir):
        path.mkdir(parents=True, exist_ok=True)

    buckets = list(range((cutoff_height >> HEIGHT_BUCKET_BITS) + 1))
    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_bucket,
                args.rpc_url,
                cutoff_height,
                bucket,
                raw_dir,
            ): bucket
            for bucket in buckets
        }
        for future in as_completed(futures):
            receipts.append(future.result())

    receipt_by_bucket = {int(row["bucket"]): row for row in receipts}
    if set(receipt_by_bucket) != set(buckets):
        raise RuntimeError("Tellor aggregate-index bucket receipts are incomplete")
    raw_paths = [
        (
            bucket,
            raw_dir
            / (
                f"{bucket << HEIGHT_BUCKET_BITS:08d}_"
                f"{(bucket << HEIGHT_BUCKET_BITS) + (1 << HEIGHT_BUCKET_BITS) - 1:08d}"
                ".pb.gz"
            ),
        )
        for bucket in buckets
    ]
    jsonl_path = curated_dir / "tellor_aggregate_height_index.jsonl"
    parquet_path = curated_dir / "tellor_aggregate_height_index.parquet"
    (
        row_count,
        first_height,
        last_height,
        non_monotonic,
        query_counts,
    ) = write_outputs(
        iter_bucket_rows(
            raw_paths,
            cutoff_height,
            maximum_aggregate_height=cutoff_height,
        ),
        jsonl_path,
        parquet_path,
    )
    receipt_rows = sum(int(row["rows"]) for row in receipts)
    post_cutoff_rows_excluded = receipt_rows - row_count
    manifest = {
        "dataset": "Tellor Layer complete on-chain aggregate-height index",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "cutoff_block_time": cutoff_time,
        "state_store": "oracle",
        "state_index_prefix_hex": AGGREGATES_HEIGHT_INDEX_PREFIX.hex(),
        "source_commit": SOURCE_COMMIT,
        "height_bucket_size": 1 << HEIGHT_BUCKET_BITS,
        "buckets": len(buckets),
        "aggregate_rows": row_count,
        "raw_index_rows": receipt_rows,
        "post_cutoff_rows_excluded": post_cutoff_rows_excluded,
        "first_aggregate_height": first_height,
        "last_aggregate_height": last_height,
        "query_id_count": len(query_counts),
        "top_query_ids": dict(query_counts.most_common(25)),
        "non_monotonic_rows": non_monotonic,
        "raw_directory": str(raw_dir),
        "curated_jsonl": str(jsonl_path),
        "curated_parquet": str(parquet_path),
        "all_required_assertions_pass": (
            row_count + post_cutoff_rows_excluded == receipt_rows
            and row_count > 0
            and post_cutoff_rows_excluded >= 0
            and non_monotonic == 0
            and first_height is not None
            and first_height >= 1
            and last_height is not None
            and last_height <= cutoff_height
        ),
        "scope_guard": (
            "Rows are primary-key references retained by Tellor's append-only "
            "AggregatesHeightIndex. Some archive subspace responses contained "
            "keys written after the requested historical state; the exported "
            "ledger therefore enforces the fixed cutoff directly on the "
            "on-chain aggregate_height key and records the excluded count. "
            "Rows are exact aggregate occurrences, not inferred reports or rewards."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor aggregate-index QC failed: {manifest}")
    manifest_path = manifest_dir / "tellor_aggregate_index.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Tellor aggregate-height index QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff height: {cutoff_height:,}

- Aggregate rows: {row_count:,}.
- Raw index rows: {receipt_rows:,}.
- Post-cutoff rows excluded by on-chain aggregate height: {post_cutoff_rows_excluded:,}.
- Query ids: {len(query_counts):,}.
- Height range: {first_height:,}–{last_height:,}.
- Exact 65,536-height state-prefix buckets: {len(buckets):,}.
- Raw/output difference: {receipt_rows - row_count}.
- Non-monotonic rows: {non_monotonic}.

The ledger is exported directly from the archive state's
`AggregatesHeightIndex`; no aggregate height is inferred from report timing.
"""
    (ROOT / "reports/tellor_aggregate_index_qc.md").write_text(
        report,
        encoding="utf-8",
    )
    print(manifest_path)


if __name__ == "__main__":
    main()
