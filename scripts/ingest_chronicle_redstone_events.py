"""Collect Ethereum event evidence for the Chronicle and RedStone census panels."""
from __future__ import annotations

import argparse
import gzip
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from eth_abi import decode
from eth_utils import keccak

from oracle_ledger.ecosystem_events import (
    CHRONICLE_TOPICS,
    REDSTONE_TOPICS,
    decode_chronicle_log,
    decode_redstone_log,
)


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
CUTOFF_BLOCK = 25_433_938
DEFAULT_RPC = "http://127.0.0.1:8545"
MAX_BLOCK_RANGE = 100_000
CHRONICLE_SOURCE_COMMIT = "12ff06ca78811e01313afde4b38fe959d6647096"
CHRONICLE_REGISTRY_COMMIT = "f06e0a8209fcb47f1535d1b874cb8ff254a9c2c5"
REDSTONE_SOURCE_COMMIT = "95f8cb9fd00b0a9f12c3e4bc45206533c90f09cb"
PRINT_LOCK = Lock()


class LogQueryTooLarge(RuntimeError):
    """Raised when reth asks the caller to narrow an eth_getLogs query."""


class EvmRpcResponseError(RuntimeError):
    """A deterministic JSON-RPC response error that should not be retried."""


class EvmRpc:
    def __init__(self, url: str, timeout: int = 180) -> None:
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "User-Agent": "oracle-accountability-atlas/0.1",
            }
        )
        self.next_id = 1

    def call(self, method: str, params: list[Any]) -> Any:
        request_id = self.next_id
        self.next_id += 1
        last_error: Exception | None = None
        for attempt in range(8):
            try:
                response = self.session.post(
                    self.url,
                    json={
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": method,
                        "params": params,
                    },
                    timeout=self.timeout,
                )
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    if (
                        method == "eth_getLogs"
                        and "query exceeds max results" in str(body["error"])
                    ):
                        raise LogQueryTooLarge(str(body["error"]))
                    raise EvmRpcResponseError(str(body["error"]))
                return body["result"]
            except (LogQueryTooLarge, EvmRpcResponseError):
                raise
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt == 7:
                    break
                time.sleep(min(2**attempt, 30))
        raise RuntimeError(f"EVM RPC failed after retries: {method}") from last_error

    def batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        if not calls:
            return []
        payload = []
        ids: list[int] = []
        for method, params in calls:
            request_id = self.next_id
            self.next_id += 1
            ids.append(request_id)
            payload.append(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            )
        last_error: Exception | None = None
        for attempt in range(8):
            try:
                response = self.session.post(self.url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, list):
                    raise RuntimeError("batch response is not a list")
                by_id = {int(row["id"]): row for row in body}
                output = []
                for request_id in ids:
                    row = by_id[request_id]
                    if row.get("error"):
                        raise RuntimeError(str(row["error"]))
                    output.append(row["result"])
                return output
            except (requests.RequestException, ValueError, KeyError, RuntimeError) as exc:
                last_error = exc
                if attempt == 7:
                    break
                time.sleep(min(2**attempt, 30))
        raise RuntimeError("EVM batch RPC failed after retries") from last_error


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def collect_log_segment(
    rpc_url: str,
    protocol: str,
    start: int,
    end: int,
    topics: list[str],
    addresses: list[str] | None,
    raw_dir: Path,
) -> dict[str, Any]:
    protocol_dir = raw_dir / protocol.lower()
    protocol_dir.mkdir(parents=True, exist_ok=True)
    name = f"{start:08d}_{end:08d}"
    raw_path = protocol_dir / f"{name}.jsonl.gz"
    done_path = protocol_dir / f"{name}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if prior.get("complete"):
            return prior

    client = EvmRpc(rpc_url)
    address_batches: list[list[str] | None]
    if addresses:
        address_batches = [addresses]
    else:
        address_batches = [None]

    query_count = 0

    def fetch_logs(
        lower: int, upper: int, address_batch: list[str] | None
    ) -> list[dict[str, Any]]:
        nonlocal query_count
        query: dict[str, Any] = {
            "fromBlock": hex(lower),
            "toBlock": hex(upper),
            "topics": [topics],
        }
        if address_batch:
            query["address"] = address_batch
        query_count += 1
        try:
            return client.call("eth_getLogs", [query])
        except LogQueryTooLarge:
            if lower >= upper:
                raise
            midpoint = (lower + upper) // 2
            return [
                *fetch_logs(lower, midpoint, address_batch),
                *fetch_logs(midpoint + 1, upper, address_batch),
            ]

    logs_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    query_ranges = [(start, end)]
    if protocol == "RedStone":
        query_ranges = [
            (lower, min(lower + 9_999, end))
            for lower in range(start, end + 1, 10_000)
        ]
    for address_batch in address_batches:
        for lower, upper in query_ranges:
            for log in fetch_logs(lower, upper, address_batch):
                key = (
                    str(log["blockHash"]),
                    str(log["transactionHash"]),
                    str(log["logIndex"]),
                )
                logs_by_key[key] = log
    logs = list(logs_by_key.values())
    logs.sort(
        key=lambda row: (
            int(row["blockNumber"], 16),
            int(row["transactionIndex"], 16),
            int(row["logIndex"], 16),
        )
    )
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        for row in logs:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "protocol": protocol,
        "start_block": start,
        "end_block": end,
        "logs": len(logs),
        "eth_get_logs_queries": query_count,
        "address_batches": len(address_batches),
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    if logs:
        with PRINT_LOCK:
            print(f"{protocol} segment {start}-{end}: {len(logs):,} logs", flush=True)
    return receipt


def iter_raw_logs(paths: Iterable[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def load_redstone_manifest() -> tuple[
    dict[str, list[str]], list[str], list[str], list[dict[str, str]]
]:
    manifest_root = (
        ROOT
        / "data/raw/source_audit/redstone_monorepo_95f8/"
        "packages/relayer-remote-config/main"
    ).resolve()
    paths = [
        manifest_root / "relayer-manifests-multi-feed/ethereumMultiFeed.json",
        *sorted((manifest_root / "relayer-manifests").glob("ethereum*.json")),
    ]
    labels: dict[str, set[str]] = {}
    adapters: set[str] = set()
    price_feeds: set[str] = set()
    excluded_price_feeds: list[dict[str, str]] = []
    address_pattern = re.compile(r"0x[0-9a-fA-F]{40}")
    for path in paths:
        body = json.loads(path.read_text(encoding="utf-8"))
        chain = body.get("chain") or {}
        if int(chain.get("id") or 0) != 1:
            raise RuntimeError(f"non-Ethereum RedStone manifest in scope: {path}")
        adapter = str(body["adapterContract"]).lower()
        if not address_pattern.fullmatch(adapter):
            raise RuntimeError(f"invalid RedStone adapter address in {path}: {adapter}")
        adapters.add(adapter)
        labels.setdefault(adapter, set()).add(f"{path.stem}::adapter")
        for feed_name, config in (body.get("priceFeeds") or {}).items():
            address = (
                str(config.get("priceFeedAddress"))
                if isinstance(config, dict)
                else str(config)
            ).lower()
            if not address_pattern.fullmatch(address):
                excluded_price_feeds.append(
                    {
                        "manifest": str(path),
                        "feed_name": str(feed_name),
                        "configured_value": address,
                        "reason": "not_an_evm_contract_address",
                    }
                )
                continue
            price_feeds.add(address)
            labels.setdefault(address, set()).add(str(feed_name))
    return (
        {address: sorted(values) for address, values in labels.items()},
        sorted(adapters),
        sorted(price_feeds),
        excluded_price_feeds,
    )


def call_at(client: EvmRpc, address: str, signature: str, block: int) -> str | None:
    selector = "0x" + keccak(text=signature)[:4].hex()
    try:
        result = client.call(
            "eth_call", [{"to": address, "data": selector}, hex(block)]
        )
    except RuntimeError:
        return None
    return result if result and result != "0x" else None


def chronicle_contract_metadata(
    client: EvmRpc,
    addresses: list[str],
    cutoff_block: int,
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for address in addresses:
        code = client.call("eth_getCode", [address, hex(cutoff_block)])
        wat_raw = call_at(client, address, "wat()", cutoff_block)
        wat = None
        if wat_raw and len(bytes.fromhex(wat_raw.removeprefix("0x"))) >= 32:
            (wat_bytes,) = decode(["bytes32"], bytes.fromhex(wat_raw.removeprefix("0x")))
            wat = wat_bytes.rstrip(b"\x00").decode("utf-8", errors="replace")
        bar_raw = call_at(client, address, "bar()", cutoff_block)
        max_reward_raw = call_at(client, address, "maxChallengeReward()", cutoff_block)
        output[address] = {
            "address": address,
            "code_present_at_cutoff": code != "0x",
            "wat": wat,
            "bar": int(bar_raw, 16) if bar_raw else None,
            "max_challenge_reward_raw": str(int(max_reward_raw, 16))
            if max_reward_raw
            else None,
            "validated_scribe_contract": code != "0x" and wat is not None,
        }
    return output


def block_times(
    client: EvmRpc, heights: list[int], cache_path: Path
) -> dict[int, int]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    output: dict[int, int] = {}
    if cache_path.is_file():
        with cache_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                output[int(row["height"])] = int(row["timestamp"])
    missing_heights = [height for height in heights if height not in output]
    batches = [
        missing_heights[offset : offset + 100]
        for offset in range(0, len(missing_heights), 100)
    ]

    def fetch(batch: list[int]) -> dict[int, int]:
        batch_client = EvmRpc(client.url, timeout=client.timeout)
        results = batch_client.batch(
            [("eth_getBlockByNumber", [hex(height), False]) for height in batch]
        )
        output: dict[int, int] = {}
        for height, block in zip(batch, results, strict=True):
            if block is None:
                raise RuntimeError(f"missing Ethereum block {height}")
            output[height] = int(block["timestamp"], 16)
        return output

    completed = 0
    with cache_path.open("a", encoding="utf-8") as cache:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(fetch, batch) for batch in batches]
            for future in as_completed(futures):
                rows = future.result()
                output.update(rows)
                for height, timestamp in sorted(rows.items()):
                    cache.write(
                        json.dumps(
                            {"height": height, "timestamp": timestamp},
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
                cache.flush()
                completed += 1
                if completed % 250 == 0:
                    print(
                        f"Ethereum block-time batches {completed:,}/{len(batches):,}",
                        flush=True,
                    )
    if any(height not in output for height in heights):
        raise RuntimeError("Ethereum block-time cache is incomplete")
    return output


def write_table(
    rows: list[dict[str, Any]], jsonl_path: Path, parquet_path: Path
) -> None:
    if not rows:
        raise RuntimeError(f"no rows for {jsonl_path.name}")
    columns = sorted({key for row in rows for key in row})
    normalized = [{key: row.get(key) for key in columns} for row in rows]
    jsonl_tmp = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    parquet_tmp = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    with jsonl_tmp.open("w", encoding="utf-8") as handle:
        for row in normalized:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    pq.write_table(pa.Table.from_pylist(normalized), parquet_tmp, compression="zstd")
    jsonl_tmp.replace(jsonl_path)
    parquet_tmp.replace(parquet_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Chronicle and RedStone Ethereum events")
    parser.add_argument("--rpc-url", default=DEFAULT_RPC)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--segment-blocks", type=int, default=MAX_BLOCK_RANGE)
    args = parser.parse_args()
    if args.segment_blocks > MAX_BLOCK_RANGE:
        raise RuntimeError(f"local reth max range is {MAX_BLOCK_RANGE}")

    client = EvmRpc(args.rpc_url)
    chain_id = int(client.call("eth_chainId", []), 16)
    if chain_id != 1:
        raise RuntimeError(f"expected Ethereum chain id 1, got {chain_id}")
    cutoff = client.call("eth_getBlockByNumber", [hex(CUTOFF_BLOCK), False])
    next_block = client.call("eth_getBlockByNumber", [hex(CUTOFF_BLOCK + 1), False])
    cutoff_timestamp = int(cutoff["timestamp"], 16)
    next_timestamp = int(next_block["timestamp"], 16)
    if not (
        cutoff_timestamp <= int(CUTOFF.timestamp()) < next_timestamp
    ):
        raise RuntimeError(
            f"fixed Ethereum cutoff block mismatch: {cutoff_timestamp}, {next_timestamp}"
        )

    (
        labels_by_address,
        redstone_adapters,
        redstone_price_feeds,
        redstone_excluded_price_feeds,
    ) = load_redstone_manifest()
    redstone_addresses = sorted(set(redstone_adapters) | set(redstone_price_feeds))
    raw_dir = (ROOT / "data/raw/ecosystem_evm_events").resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    manifest_dir = ROOT / "data/manifests"
    for path in (raw_dir, curated_dir, manifest_dir):
        path.mkdir(parents=True, exist_ok=True)

    segments = [
        (start, min(start + args.segment_blocks - 1, CUTOFF_BLOCK))
        for start in range(0, CUTOFF_BLOCK + 1, args.segment_blocks)
    ]
    jobs: list[tuple[str, int, int, list[str], list[str] | None]] = []
    for start, end in segments:
        jobs.append(("Chronicle", start, end, sorted(CHRONICLE_TOPICS), None))
        jobs.append(
            (
                "RedStone",
                start,
                end,
                sorted(REDSTONE_TOPICS),
                redstone_addresses,
            )
        )
    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_log_segment,
                args.rpc_url,
                protocol,
                start,
                end,
                topics,
                addresses,
                raw_dir,
            ): (protocol, start, end)
            for protocol, start, end, topics, addresses in jobs
        }
        for future in as_completed(futures):
            receipts.append(future.result())

    receipt_by_key = {
        (row["protocol"], int(row["start_block"])): row for row in receipts
    }
    chronicle_raw = [
        Path(receipt_by_key[("Chronicle", start)]["raw_file"])
        for start, _ in segments
    ]
    redstone_raw = [
        Path(receipt_by_key[("RedStone", start)]["raw_file"])
        for start, _ in segments
    ]
    chronicle_logs = list(iter_raw_logs(chronicle_raw))
    redstone_logs = list(iter_raw_logs(redstone_raw))

    chronicle_addresses = sorted(
        {str(log["address"]).lower() for log in chronicle_logs}
    )
    metadata = chronicle_contract_metadata(client, chronicle_addresses, CUTOFF_BLOCK)
    excluded_candidates = sorted(
        address
        for address, row in metadata.items()
        if not row["validated_scribe_contract"]
    )
    chronicle_rows = []
    for log in chronicle_logs:
        address = str(log["address"]).lower()
        if address in excluded_candidates:
            continue
        row = decode_chronicle_log(log)
        row["oracle_name"] = metadata[address]["wat"]
        row["bar_at_cutoff"] = metadata[address]["bar"]
        row["max_challenge_reward_at_cutoff_raw"] = metadata[address][
            "max_challenge_reward_raw"
        ]
        chronicle_rows.append(row)
    redstone_rows = [
        decode_redstone_log(log, labels_by_address) for log in redstone_logs
    ]
    all_heights = sorted(
        {int(row["block_number"]) for row in [*chronicle_rows, *redstone_rows]}
    )
    timestamps = block_times(
        client, all_heights, raw_dir / "ethereum_block_times.jsonl"
    )
    for row in [*chronicle_rows, *redstone_rows]:
        unix_time = timestamps[int(row["block_number"])]
        row["block_timestamp"] = unix_time
        row["block_time"] = datetime.fromtimestamp(unix_time, tz=UTC).isoformat()

    chronicle_rows.sort(
        key=lambda row: (
            row["block_number"],
            row["transaction_index"],
            row["log_index"],
        )
    )
    redstone_rows.sort(
        key=lambda row: (
            row["block_number"],
            row["transaction_index"],
            row["log_index"],
        )
    )
    write_table(
        chronicle_rows,
        curated_dir / "chronicle_ethereum_events.jsonl",
        curated_dir / "chronicle_ethereum_events.parquet",
    )
    write_table(
        redstone_rows,
        curated_dir / "redstone_ethereum_push_events.jsonl",
        curated_dir / "redstone_ethereum_push_events.parquet",
    )

    def duplicate_count(rows: list[dict[str, Any]]) -> int:
        keys = [(row["transaction_hash"], row["log_index"]) for row in rows]
        return len(keys) - len(set(keys))

    chronicle_counts: dict[str, int] = {}
    for row in chronicle_rows:
        chronicle_counts[row["event_name"]] = (
            chronicle_counts.get(row["event_name"], 0) + 1
        )
    redstone_counts: dict[str, int] = {}
    for row in redstone_rows:
        redstone_counts[row["event_name"]] = redstone_counts.get(row["event_name"], 0) + 1
    successful_txs = {
        row["transaction_hash"]
        for row in chronicle_rows
        if row["event_name"] == "OpPokeChallengedSuccessfully"
    }
    self_drop_txs = {
        row["transaction_hash"]
        for row in chronicle_rows
        if row["event_name"] == "FeedDropped" and row.get("self_governed_drop")
    }
    reward_txs = {
        row["transaction_hash"]
        for row in chronicle_rows
        if row["event_name"] == "OpChallengeRewardPaid"
    }
    successful_without_self_drop = len(successful_txs - self_drop_txs)
    reward_without_success = len(reward_txs - successful_txs)
    manifest = {
        "dataset": "Chronicle and RedStone Ethereum event observability ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": chain_id,
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_block": CUTOFF_BLOCK,
        "cutoff_block_hash": cutoff["hash"],
        "cutoff_block_time": datetime.fromtimestamp(cutoff_timestamp, tz=UTC).isoformat(),
        "segments_per_protocol": len(segments),
        "chronicle_source_commit": CHRONICLE_SOURCE_COMMIT,
        "chronicle_validator_registry_commit": CHRONICLE_REGISTRY_COMMIT,
        "redstone_source_and_manifest_commit": REDSTONE_SOURCE_COMMIT,
        "chronicle_raw_candidates": len(chronicle_logs),
        "chronicle_excluded_unvalidated_candidates": len(excluded_candidates),
        "chronicle_excluded_addresses": excluded_candidates,
        "chronicle_validated_contracts": len(chronicle_addresses) - len(excluded_candidates),
        "chronicle_contract_metadata": metadata,
        "chronicle_event_rows": len(chronicle_rows),
        "chronicle_event_counts": chronicle_counts,
        "chronicle_realized_challenge_reward_raw_wei": str(
            sum(
                int(row["reward_amount_raw"])
                for row in chronicle_rows
                if row["event_name"] == "OpChallengeRewardPaid"
            )
        ),
        "chronicle_successful_challenge_without_self_drop": successful_without_self_drop,
        "chronicle_reward_without_successful_challenge": reward_without_success,
        "chronicle_duplicate_events": duplicate_count(chronicle_rows),
        "redstone_manifest_adapter_contracts": len(redstone_adapters),
        "redstone_manifest_price_feed_contracts": len(redstone_price_feeds),
        "redstone_manifest_non_address_feed_entries": redstone_excluded_price_feeds,
        "redstone_event_rows": len(redstone_rows),
        "redstone_event_counts": redstone_counts,
        "redstone_duplicate_events": duplicate_count(redstone_rows),
        "redstone_realized_reward_events": 0,
        "redstone_realized_slash_events": 0,
        "all_required_assertions_pass": (
            len(chronicle_rows) == sum(chronicle_counts.values())
            and len(redstone_rows) == sum(redstone_counts.values())
            and duplicate_count(chronicle_rows) == 0
            and duplicate_count(redstone_rows) == 0
            and successful_without_self_drop == 0
            and reward_without_success == 0
            and all(not row["removed"] for row in [*chronicle_rows, *redstone_rows])
            and all(row["block_number"] <= CUTOFF_BLOCK for row in [*chronicle_rows, *redstone_rows])
        ),
        "scope_guard": (
            "Chronicle rows cover Ethereum Scribe/ScribeOptimistic event signatures and "
            "include realized ETH challenge rewards and self-governed feed drops. "
            "RedStone rows cover official Ethereum Push-adapter manifests pinned at the "
            "cutoff; the Pull model embeds signed payloads in consumer calls and has no "
            "global price-update log. Official scoped EVM adapter contracts define no "
            "publisher reward or slash settlement event, so those values are verified "
            "absent rather than inferred as zero protocol-wide."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Chronicle/RedStone QC failed: {manifest}")
    manifest_path = manifest_dir / "chronicle_redstone_ethereum_events.json"
    atomic_json(manifest_path, manifest)
    evidence = [
        {
            "oracle_network": "Chronicle",
            "security_chain": "Ethereum",
            "delivery_model": "Scribe and ScribeOptimistic push",
            "report_event_interface": "Poked and OpPoked",
            "reward_interface": "OpChallengeRewardPaid (realized ETH only)",
            "penalty_interface": "successful challenge -> self-governed FeedDropped",
            "monetary_slash_interface": None,
            "source_commit": CHRONICLE_SOURCE_COMMIT,
            "fixed_cutoff": CUTOFF.isoformat(),
            "event_rows": len(chronicle_rows),
        },
        {
            "oracle_network": "RedStone",
            "security_chain": "off-chain signed data packages; delivery-chain verification",
            "delivery_model": "Pull payloads plus Push adapters",
            "report_event_interface": "Push: ValueUpdate/AnswerUpdated; Pull: consumer calldata",
            "reward_interface": None,
            "penalty_interface": None,
            "monetary_slash_interface": None,
            "source_commit": REDSTONE_SOURCE_COMMIT,
            "fixed_cutoff": CUTOFF.isoformat(),
            "event_rows": len(redstone_rows),
        },
    ]
    write_table(
        evidence,
        curated_dir / "ecosystem_observability_evidence.jsonl",
        curated_dir / "ecosystem_observability_evidence.parquet",
    )
    report = f"""# Chronicle / RedStone Ethereum observability QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Ethereum cutoff block: {CUTOFF_BLOCK:,}

- Chronicle validated Scribe contracts: {manifest['chronicle_validated_contracts']:,}.
- Chronicle events: {len(chronicle_rows):,}.
- Chronicle successful invalid-report challenges: {chronicle_counts.get('OpPokeChallengedSuccessfully', 0):,}.
- Chronicle realized ETH challenge rewards: {chronicle_counts.get('OpChallengeRewardPaid', 0):,}.
- Chronicle reward total: {manifest['chronicle_realized_challenge_reward_raw_wei']} wei.
- RedStone official Ethereum adapter contracts: {len(redstone_adapters):,}.
- RedStone official Ethereum price-feed contracts: {len(redstone_price_feeds):,}.
- RedStone Push events: {len(redstone_rows):,}.
- Duplicate events: Chronicle {manifest['chronicle_duplicate_events']}, RedStone {manifest['redstone_duplicate_events']}.

Chronicle's challenge bounty is a realized reward only when
`OpChallengeRewardPaid` is emitted after the ETH send succeeds. A
`FeedDropped` event is non-monetary exclusion unless another transfer proves a
monetary loss. RedStone Push delivery is event-observable, while its Pull model
has no global update ledger; the scoped official EVM adapters expose no
publisher reward/slash settlement interface.
"""
    (ROOT / "reports/chronicle_redstone_observability_qc.md").write_text(
        report, encoding="utf-8"
    )
    print(manifest_path)


if __name__ == "__main__":
    main()
