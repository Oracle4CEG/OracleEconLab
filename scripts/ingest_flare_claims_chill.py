"""Collect complete Flare RewardManager claims and VoterRegistry chill history.

The Flare public RPC limits ``eth_getLogs`` to 30 blocks.  The official Flare
Explorer exposes the same indexed chain data in pages of at most 1,000 logs, so
this adapter traverses non-overlapping block segments with an explicit cursor.
Every full page is closed by an exact-last-block query before the cursor moves,
which prevents silently dropping logs at a page boundary.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from oracle_ledger.flare_events import (
    BENEFICIARY_CHILLED_UINT256_TOPIC,
    BURN_ADDRESS,
    CLAIM_TYPES,
    REWARD_CLAIMED_TOPIC,
    decode_beneficiary_chilled,
    decode_reward_claimed,
    log_key,
)
from oracle_ledger.flare_fsp import FlareRpc, iso_timestamp, uint256_call_data


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_RPC = "https://flare-api.flare.network/ext/C/rpc"
DEFAULT_EXPLORER_API = "https://flare-explorer.flare.network/api"
DEFAULT_EXPLORER_V2 = "https://flare-explorer.flare.network/api/v2"
REWARD_MANAGER = "0xC8f55c5aA2C752eE285Bd872855C749f4ee6239B"
VOTER_REGISTRY = "0x2580101692366e2f331e891180d9ffdF861Fce83"
FIRST_REWARD_EPOCH = 228
LAST_REWARD_EPOCH = 410
GET_REWARD_EPOCH_TOTALS_SELECTOR = "0xdf339638"
MAX_EXPLORER_ROWS = 1_000
PRINT_LOCK = Lock()


class ExplorerClient:
    def __init__(self, base_url: str, timeout: int = 90) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"

    def logs(self, address: str, topic0: str, start: int, end: int) -> list[dict[str, Any]]:
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": str(start),
            "toBlock": str(end),
            "address": address,
            "topic0": topic0,
        }
        last_error: Exception | None = None
        for attempt in range(8):
            try:
                response = self.session.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                body = response.json()
                if body.get("status") == "0" and body.get("message") == "No logs found":
                    return []
                if body.get("status") != "1" or not isinstance(body.get("result"), list):
                    raise RuntimeError(f"unexpected explorer response: {body}")
                rows = body["result"]
                if len(rows) > MAX_EXPLORER_ROWS:
                    raise RuntimeError(f"explorer returned {len(rows)} rows above documented page cap")
                return rows
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt == 7:
                    break
                time.sleep(min(2**attempt, 30))
        raise RuntimeError(f"Flare Explorer request failed for blocks {start}-{end}") from last_error


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def contract_deployment_block(rpc: FlareRpc, address: str, high: int) -> int:
    low = 0
    while low < high:
        middle = (low + high) // 2
        if rpc.call("eth_getCode", [address, hex(middle)]) == "0x":
            low = middle + 1
        else:
            high = middle
    if rpc.call("eth_getCode", [address, hex(low)]) == "0x":
        raise RuntimeError(f"contract has no code by cutoff: {address}")
    return low


def raw_log_identity(row: dict[str, Any]) -> tuple[str, int]:
    return str(row["transactionHash"]).lower(), int(row["logIndex"], 16)


def collect_segment(
    client: ExplorerClient,
    address: str,
    topic0: str,
    start: int,
    end: int,
    directory: Path,
) -> dict[str, Any]:
    name = f"{start:08d}_{end:08d}"
    target = directory / f"{name}.jsonl.gz"
    receipt = directory / f"{name}.done.json"
    if target.is_file() and receipt.is_file():
        prior = json.loads(receipt.read_text(encoding="utf-8"))
        if prior.get("complete") and prior.get("start_block") == start and prior.get("end_block") == end:
            return prior

    temporary = target.with_suffix(target.suffix + ".tmp")
    cursor = start
    count = 0
    pages = 0
    digest = hashlib.sha256()
    last_source_key: tuple[int, int, int, str] | None = None
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        while cursor <= end:
            rows = client.logs(address, topic0, cursor, end)
            pages += 1
            if not rows:
                break
            rows.sort(key=log_key)
            if any(not cursor <= log_key(row)[0] <= end for row in rows):
                raise RuntimeError(f"explorer returned out-of-range log for segment {name}")
            if any(str(row["topics"][0]).lower() != topic0 for row in rows):
                raise RuntimeError(f"explorer topic filter mismatch for segment {name}")

            # A full page can end midway through a block. Fetch that exact block
            # and merge it before advancing to the next block.
            if len(rows) == MAX_EXPLORER_ROWS:
                last_block = log_key(rows[-1])[0]
                boundary_rows = client.logs(address, topic0, last_block, last_block)
                if len(boundary_rows) == MAX_EXPLORER_ROWS:
                    raise RuntimeError(
                        f"at least {MAX_EXPLORER_ROWS} matching logs in block {last_block}; "
                        "the explorer API cannot prove this block complete"
                    )
                by_identity = {raw_log_identity(row): row for row in rows}
                for row in boundary_rows:
                    by_identity[raw_log_identity(row)] = row
                rows = sorted(by_identity.values(), key=log_key)

            for row in rows:
                source_key = log_key(row)
                if last_source_key is not None and source_key <= last_source_key:
                    if source_key == last_source_key:
                        continue
                    raise RuntimeError(f"non-monotonic Flare Explorer logs in segment {name}")
                line = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
                handle.write(line)
                digest.update(line.encode())
                last_source_key = source_key
                count += 1
            cursor = log_key(rows[-1])[0] + 1
            if len(rows) < MAX_EXPLORER_ROWS and cursor <= end:
                # The initial response was not capped, so the queried suffix is complete.
                break
    temporary.replace(target)
    result = {
        "complete": True,
        "start_block": start,
        "end_block": end,
        "rows": count,
        "requests": pages,
        "sha256_uncompressed_jsonl": digest.hexdigest(),
        "raw_file": str(target),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(receipt, result)
    with PRINT_LOCK:
        print(f"Flare segment {start}-{end}: {count:,} logs", flush=True)
    return result


def iter_raw_logs(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in sorted(paths):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def merge_segment_sources(
    primary_dir: Path,
    supplemental_dirs: list[Path],
    output_dir: Path,
    start: int,
    end: int,
) -> dict[str, Any]:
    """Union independent Explorer passes by canonical chain-log identity.

    The Explorer index can transiently omit rows while still returning a
    successful response. Independent passes are retained and unioned by
    ``(transactionHash, logIndex)``. The epoch-state reconciliation below is
    still the completeness oracle.
    """
    name = f"{start:08d}_{end:08d}"
    source_paths = [
        directory / f"{name}.jsonl.gz"
        for directory in [primary_dir, *supplemental_dirs]
    ]
    missing = [str(path) for path in source_paths if not path.is_file()]
    if missing:
        raise RuntimeError(f"missing Flare union source(s) for {name}: {missing}")

    source_proof = [
        {
            "path": str(path),
            "size": path.stat().st_size,
            "mtime_ns": path.stat().st_mtime_ns,
        }
        for path in source_paths
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{name}.jsonl.gz"
    receipt_path = output_dir / f"{name}.done.json"
    if target.is_file() and receipt_path.is_file():
        prior = json.loads(receipt_path.read_text(encoding="utf-8"))
        if prior.get("complete") and prior.get("sources") == source_proof:
            return prior

    by_identity: dict[tuple[str, int], dict[str, Any]] = {}
    source_rows: list[int] = []
    conflicting_rows = 0
    for path in source_paths:
        count = 0
        for row in iter_raw_logs([path]):
            count += 1
            identity = raw_log_identity(row)
            previous = by_identity.get(identity)
            if previous is not None:
                immutable_previous = (
                    str(previous.get("address", "")).lower(),
                    str(previous.get("blockHash", "")).lower(),
                    str(previous.get("transactionHash", "")).lower(),
                    str(previous.get("data", "")).lower(),
                    tuple(
                        str(topic).lower()
                        for topic in previous.get("topics") or []
                    ),
                )
                immutable_current = (
                    str(row.get("address", "")).lower(),
                    str(row.get("blockHash", "")).lower(),
                    str(row.get("transactionHash", "")).lower(),
                    str(row.get("data", "")).lower(),
                    tuple(str(topic).lower() for topic in row.get("topics") or []),
                )
                if immutable_previous != immutable_current:
                    conflicting_rows += 1
                    raise RuntimeError(
                        f"conflicting Flare log payload for {name} {identity}"
                    )
            else:
                by_identity[identity] = row
        source_rows.append(count)

    rows = sorted(by_identity.values(), key=log_key)
    temporary = target.with_suffix(target.suffix + ".tmp")
    digest = hashlib.sha256()
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        for row in rows:
            line = json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ) + "\n"
            handle.write(line)
            digest.update(line.encode())
    temporary.replace(target)
    receipt = {
        "complete": True,
        "start_block": start,
        "end_block": end,
        "rows": len(rows),
        "source_rows": source_rows,
        "rows_added_by_union": len(rows) - min(source_rows),
        "conflicting_rows": conflicting_rows,
        "sources": source_proof,
        "sha256_uncompressed_jsonl": digest.hexdigest(),
        "raw_file": str(target),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(receipt_path, receipt)
    with PRINT_LOCK:
        print(
            f"Flare union {start}-{end}: {source_rows} -> {len(rows):,} logs",
            flush=True,
        )
    return receipt


def write_decoded_outputs(
    raw_paths: list[Path],
    jsonl_path: Path,
    parquet_path: Path,
) -> tuple[int, Counter[str], dict[int, dict[str, int]], int, int]:
    jsonl_temporary = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    parquet_temporary = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    batch: list[dict[str, Any]] = []
    claim_types: Counter[str] = Counter()
    epoch_amounts: dict[int, dict[str, int]] = defaultdict(lambda: {"claimed": 0, "fee_burned": 0})
    count = 0
    fee_burn_events = 0
    duplicate_keys = 0
    previous_key: tuple[int, int, int, str] | None = None
    with jsonl_temporary.open("w", encoding="utf-8") as jsonl:
        for raw in iter_raw_logs(raw_paths):
            source_key = log_key(raw)
            if previous_key is not None:
                if source_key == previous_key:
                    duplicate_keys += 1
                    continue
                if source_key < previous_key:
                    raise RuntimeError("raw Flare claim segments are not globally ordered")
            previous_key = source_key
            row = decode_reward_claimed(raw)
            if row["reward_epoch_id"] < FIRST_REWARD_EPOCH or row["reward_epoch_id"] > LAST_REWARD_EPOCH:
                raise RuntimeError(f"claim epoch outside fixed release: {row['reward_epoch_id']}")
            jsonl.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            claim_types[row["claim_type"]] += 1
            fee_burn_events += int(row["is_fee_burn"])
            bucket = "fee_burned" if row["is_fee_burn"] else "claimed"
            epoch_amounts[row["reward_epoch_id"]][bucket] += int(row["amount_raw"])
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
        raise RuntimeError("no Flare RewardClaimed events collected")
    writer.close()
    jsonl_temporary.replace(jsonl_path)
    parquet_temporary.replace(parquet_path)
    return count, claim_types, epoch_amounts, duplicate_keys, fee_burn_events


def reuse_decoded_outputs(
    jsonl_path: Path,
    parquet_path: Path,
    epoch_qc_path: Path,
    expected_rows: int,
) -> tuple[int, Counter[str], dict[int, dict[str, int]], int, int]:
    """Reuse a fully committed decode after an interrupted manifest pass.

    ``write_decoded_outputs`` only replaces the final JSONL and Parquet after
    the complete raw stream has passed its global ordering/duplicate checks.
    Therefore a Parquet row count equal to the immutable raw receipt total is
    sufficient to resume the cheap aggregate/QC phase without decoding all
    canonical logs again.
    """
    if not jsonl_path.is_file() or not parquet_path.is_file() or not epoch_qc_path.is_file():
        raise RuntimeError("requested Flare decoded-output reuse, but committed outputs are missing")
    parquet = pq.ParquetFile(parquet_path)
    count = int(parquet.metadata.num_rows)
    if count != expected_rows:
        raise RuntimeError(
            f"Flare committed Parquet/raw receipt row mismatch: {count} != {expected_rows}"
        )
    claim_types: Counter[str] = Counter()
    fee_burn_events = 0
    for batch in parquet.iter_batches(
        batch_size=1_000_000,
        columns=["claim_type", "is_fee_burn"],
    ):
        for item in pc.value_counts(batch.column(0)).to_pylist():
            claim_types[str(item["values"])] += int(item["counts"])
        fee_burn_events += int(pc.sum(batch.column(1)).as_py() or 0)
    epoch_amounts: dict[int, dict[str, int]] = defaultdict(
        lambda: {"claimed": 0, "fee_burned": 0}
    )
    with epoch_qc_path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            epoch_amounts[int(row["reward_epoch_id"])] = {
                "claimed": int(row["observed_claimed_raw"]),
                "fee_burned": int(row["observed_fee_burn_raw"]),
            }
    if len(epoch_amounts) != LAST_REWARD_EPOCH - FIRST_REWARD_EPOCH + 1:
        raise RuntimeError("Flare committed epoch-QC output has incomplete epoch coverage")
    return count, claim_types, epoch_amounts, 0, fee_burn_events


def decode_epoch_totals(encoded: str) -> dict[str, int]:
    raw = encoded.removeprefix("0x")
    if len(raw) != 5 * 64:
        raise ValueError("unexpected getRewardEpochTotals return width")
    values = [int(raw[offset : offset + 64], 16) for offset in range(0, len(raw), 64)]
    return dict(zip(
        ["total_rewards", "inflation_rewards", "initialised_rewards", "claimed_rewards", "burned_rewards"],
        values,
        strict=True,
    ))


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect complete Flare claims and chill event history")
    parser.add_argument("--rpc-url", default=os.getenv("FLARE_RPC_URL", DEFAULT_RPC))
    parser.add_argument("--explorer-api", default=DEFAULT_EXPLORER_API)
    parser.add_argument("--explorer-v2", default=DEFAULT_EXPLORER_V2)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--segment-blocks", type=int, default=500_000)
    parser.add_argument(
        "--claim-raw-dir",
        help=(
            "Primary RewardClaimed raw segment directory. When omitted, use "
            "data/raw/flare_fsp/onchain_events/reward_claimed. A canonical "
            "RPC directory produced by ingest_flare_claims_rpc_raw.py may be "
            "supplied here; its completed receipts are reused without an "
            "Explorer recollection."
        ),
    )
    parser.add_argument(
        "--supplemental-raw-dir",
        action="append",
        default=[],
        help=(
            "Independent reward_claimed raw directory to union with the current "
            "pass; repeat for multiple prior passes"
        ),
    )
    parser.add_argument(
        "--merged-raw-subdir",
        default="reward_claimed_merged",
        help="Raw subdirectory for the deduplicated multi-pass union",
    )
    parser.add_argument(
        "--reuse-decoded-output",
        action="store_true",
        help=(
            "Resume after the decoded JSONL/Parquet were atomically committed; "
            "validate them against raw receipts and rebuild only aggregate QC"
        ),
    )
    args = parser.parse_args()

    raw_dir = (ROOT / "data/raw/flare_fsp/onchain_events").resolve()
    claim_raw_dir = (
        Path(args.claim_raw_dir).resolve()
        if args.claim_raw_dir
        else raw_dir / "reward_claimed"
    )
    curated_dir = (ROOT / "data/curated").resolve()
    manifest_dir = ROOT / "data/manifests"
    for path in (raw_dir, claim_raw_dir, curated_dir, manifest_dir):
        path.mkdir(parents=True, exist_ok=True)

    rpc = FlareRpc(args.rpc_url)
    if int(rpc.call("eth_chainId", []), 16) != 14:
        raise RuntimeError("Flare RPC did not return chain id 14")
    cutoff_block, cutoff_header = rpc.block_at_or_before(int(CUTOFF.timestamp()))
    reward_deployment = contract_deployment_block(rpc, REWARD_MANAGER, cutoff_block)
    registry_deployment = contract_deployment_block(rpc, VOTER_REGISTRY, cutoff_block)

    # Pin the deployed ABIs supplied by the official Flare Explorer.
    for address, filename in (
        (REWARD_MANAGER, "reward_manager_contract.json"),
        (VOTER_REGISTRY, "voter_registry_contract.json"),
    ):
        response = requests.get(
            f"{args.explorer_v2}/smart-contracts/{address}",
            timeout=90,
            headers={"User-Agent": "oracle-accountability-atlas/0.1"},
        )
        response.raise_for_status()
        atomic_json(raw_dir / filename, response.json())

    explorer = ExplorerClient(args.explorer_api)
    segments = []
    for start in range(reward_deployment, cutoff_block + 1, args.segment_blocks):
        segments.append((start, min(start + args.segment_blocks - 1, cutoff_block)))
    segment_results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_segment,
                ExplorerClient(args.explorer_api),
                REWARD_MANAGER,
                REWARD_CLAIMED_TOPIC,
                start,
                end,
                claim_raw_dir,
            ): (start, end)
            for start, end in segments
        }
        for future in as_completed(futures):
            segment_results.append(future.result())

    supplemental_dirs = [Path(value).resolve() for value in args.supplemental_raw_dir]
    active_claim_raw_dir = claim_raw_dir
    if supplemental_dirs:
        merged_dir = raw_dir / args.merged_raw_subdir
        merged_results: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {
                executor.submit(
                    merge_segment_sources,
                    claim_raw_dir,
                    supplemental_dirs,
                    merged_dir,
                    start,
                    end,
                ): (start, end)
                for start, end in segments
            }
            for future in as_completed(futures):
                merged_results.append(future.result())
        segment_results = merged_results
        active_claim_raw_dir = merged_dir

    claim_jsonl = curated_dir / "flare_reward_claim_events.jsonl"
    claim_parquet = curated_dir / "flare_reward_claim_events.parquet"
    qc_jsonl = curated_dir / "flare_reward_claim_epoch_qc.jsonl"
    raw_paths = [
        active_claim_raw_dir / f"{start:08d}_{end:08d}.jsonl.gz"
        for start, end in segments
    ]
    raw_unique = sum(int(row["rows"]) for row in segment_results)
    if args.reuse_decoded_output:
        (
            claim_count,
            claim_types,
            epoch_amounts,
            duplicate_keys,
            fee_burn_events,
        ) = reuse_decoded_outputs(
            claim_jsonl,
            claim_parquet,
            qc_jsonl,
            raw_unique,
        )
    else:
        (
            claim_count,
            claim_types,
            epoch_amounts,
            duplicate_keys,
            fee_burn_events,
        ) = write_decoded_outputs(raw_paths, claim_jsonl, claim_parquet)

    chill_raw = explorer.logs(
        VOTER_REGISTRY,
        BENEFICIARY_CHILLED_UINT256_TOPIC,
        registry_deployment,
        cutoff_block,
    )
    if len(chill_raw) == MAX_EXPLORER_ROWS:
        raise RuntimeError("VoterRegistry chill history hit explorer result cap")
    chill_raw.sort(key=log_key)
    chill_rows = [decode_beneficiary_chilled(row) for row in chill_raw]
    chill_jsonl = curated_dir / "flare_beneficiary_chill_events.jsonl"
    temporary = chill_jsonl.with_suffix(chill_jsonl.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in chill_rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(chill_jsonl)
    pq.write_table(pa.Table.from_pylist(chill_rows), curated_dir / "flare_beneficiary_chill_events.parquet", compression="zstd")
    with gzip.open(raw_dir / "beneficiary_chilled.json.gz.tmp", "wt", encoding="utf-8") as handle:
        json.dump(chill_raw, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
    (raw_dir / "beneficiary_chilled.json.gz.tmp").replace(raw_dir / "beneficiary_chilled.json.gz")

    total_calls = [
        [{"to": REWARD_MANAGER, "data": uint256_call_data(GET_REWARD_EPOCH_TOTALS_SELECTOR, epoch)}, hex(cutoff_block)]
        for epoch in range(FIRST_REWARD_EPOCH, LAST_REWARD_EPOCH + 1)
    ]
    encoded_totals: list[str] = []
    for offset in range(0, len(total_calls), 50):
        encoded_totals.extend(rpc.batch("eth_call", total_calls[offset : offset + 50]))
    onchain_by_epoch = {
        epoch: decode_epoch_totals(encoded)
        for epoch, encoded in zip(range(FIRST_REWARD_EPOCH, LAST_REWARD_EPOCH + 1), encoded_totals, strict=True)
    }
    epoch_qc: list[dict[str, Any]] = []
    for epoch in range(FIRST_REWARD_EPOCH, LAST_REWARD_EPOCH + 1):
        observed = epoch_amounts[epoch]
        state = onchain_by_epoch[epoch]
        fee_burned = observed["fee_burned"]
        epoch_qc.append({
            "reward_epoch_id": epoch,
            "observed_claimed_raw": str(observed["claimed"]),
            "onchain_claimed_raw": str(state["claimed_rewards"]),
            "claimed_matches": observed["claimed"] == state["claimed_rewards"],
            "observed_fee_burn_raw": str(fee_burned),
            "onchain_burned_raw": str(state["burned_rewards"]),
            "inferred_expiry_or_unclaimed_burn_raw": str(state["burned_rewards"] - fee_burned),
            "burn_not_below_observed_fee_burn": state["burned_rewards"] >= fee_burned,
            "total_rewards_raw": str(state["total_rewards"]),
            "inflation_rewards_raw": str(state["inflation_rewards"]),
            "initialised_rewards_raw": str(state["initialised_rewards"]),
            "state_block": cutoff_block,
        })
    temporary = qc_jsonl.with_suffix(qc_jsonl.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in epoch_qc:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(qc_jsonl)
    pq.write_table(pa.Table.from_pylist(epoch_qc), curated_dir / "flare_reward_claim_epoch_qc.parquet", compression="zstd")

    all_claimed_match = all(row["claimed_matches"] for row in epoch_qc)
    all_burn_bounds = all(row["burn_not_below_observed_fee_burn"] for row in epoch_qc)
    chill_keys = [(row["source_tx"], row["source_log_index"]) for row in chill_rows]
    manifest = {
        "dataset": "Flare RewardManager realized claims and VoterRegistry chill event ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": 14,
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_block": cutoff_block,
        "cutoff_block_time": iso_timestamp(int(cutoff_header["timestamp"], 16)),
        "cutoff_block_hash": cutoff_header["hash"],
        "reward_manager": REWARD_MANAGER,
        "reward_manager_deployment_block": reward_deployment,
        "voter_registry": VOTER_REGISTRY,
        "voter_registry_deployment_block": registry_deployment,
        "source": (
            "Flare archive JSON-RPC canonical logs plus official Explorer ABI "
            "and Flare public RPC state"
            if args.claim_raw_dir
            else "official Flare Explorer indexed chain logs plus Flare public RPC state"
        ),
        "reward_claimed_topic": REWARD_CLAIMED_TOPIC,
        "beneficiary_chilled_topic": BENEFICIARY_CHILLED_UINT256_TOPIC,
        "first_reward_epoch": FIRST_REWARD_EPOCH,
        "last_reward_epoch": LAST_REWARD_EPOCH,
        "segments": len(segments),
        "segment_rows": raw_unique,
        "active_claim_raw_directory": str(active_claim_raw_dir),
        "supplemental_raw_directories": [
            str(path) for path in supplemental_dirs
        ],
        "reward_claim_events": claim_count,
        "reward_claims_by_type": dict(claim_types),
        "fee_burn_events": fee_burn_events,
        "beneficiary_chill_events": len(chill_rows),
        "duplicate_claim_source_keys": duplicate_keys,
        "duplicate_chill_source_keys": len(chill_keys) - len(set(chill_keys)),
        "claimed_epochs_matching_onchain_state": sum(row["claimed_matches"] for row in epoch_qc),
        "epochs_checked": len(epoch_qc),
        "burn_bound_epochs_passing": sum(row["burn_not_below_observed_fee_burn"] for row in epoch_qc),
        "raw_directory": str(raw_dir),
        "curated_claim_jsonl": str(claim_jsonl),
        "curated_claim_parquet": str(claim_parquet),
        "curated_chill_jsonl": str(chill_jsonl),
        "curated_epoch_qc": str(qc_jsonl),
        "all_required_assertions_pass": (
            raw_unique == claim_count
            and duplicate_keys == 0
            and len(chill_keys) == len(set(chill_keys))
            and all_claimed_match
            and all_burn_bounds
            and all(name in CLAIM_TYPES.values() for name in claim_types)
        ),
        "scope_guard": (
            "RewardClaimed is a realized RewardManager accounting/payment event. "
            "Fee burns emit RewardClaimed to 0xdead; expiry/unclaimed burns do not, "
            "so their realized amount is the on-chain epoch burned total minus observed fee-burn events."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Flare claim/chill QC failed: {manifest}")
    manifest_path = manifest_dir / "flare_claims_chill.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Flare realized claims and chill-history QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff block: {cutoff_block} ({manifest['cutoff_block_time']})

- Realized `RewardClaimed` events: {claim_count:,}.
- Claim types: {dict(claim_types)}.
- Historical `BeneficiaryChilled` events: {len(chill_rows)}.
- Reward epochs whose event sums exactly equal `getRewardEpochTotals(...).claimedRewards`: {manifest['claimed_epochs_matching_onchain_state']}/{len(epoch_qc)}.
- Duplicate claim/chill source keys: {duplicate_keys}/{manifest['duplicate_chill_source_keys']}.
- Raw explorer segments: {len(segments)}; raw decoded rows: {raw_unique:,}.

`RewardClaimed` is emitted by the deployed RewardManager for successful claims.
Claims sent to `{BURN_ADDRESS}` are explicit FEE burns. Expired/unclaimed rewards
are burned without a `RewardClaimed` event; those amounts are retained separately
as the on-chain burned total less explicit FEE-burn events.
"""
    (ROOT / "reports/flare_claims_chill_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
