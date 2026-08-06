"""Collect complete Pyth OIS Solana transaction/stake/reward/slash history."""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock, local
from typing import Any, Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from oracle_ledger.pyth_history import PROGRAM_ID, parse_integrity_pool_transaction


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_SIGNATURE_RPC = "https://api.mainnet-beta.solana.com"
DEFAULT_TRANSACTION_RPCS = (
    ("https://public.rpc.solanavibestation.com/historical", 4.0),
    ("https://solana-mainnet.gateway.tatum.io", 2.0),
    ("https://api.mainnet-beta.solana.com", 0.8),
    ("https://solana.lava.build", 0.5),
)
SOURCE_COMMIT = "68a9a36ec3d41364490e71b056b422c99f13e0cf"
PRINT_LOCK = Lock()


class SolanaRpc:
    def __init__(self, url: str, timeout: int = 180) -> None:
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"
        self.request_id = 0

    def call(self, method: str, params: list[Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(10):
            try:
                self.request_id += 1
                response = self.session.post(
                    self.url,
                    json={"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params},
                    timeout=self.timeout,
                )
                if response.status_code == 429:
                    raise requests.RequestException("HTTP 429")
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    raise RuntimeError(str(body["error"]))
                return body["result"]
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt == 9:
                    break
                time.sleep(min(1.5**attempt, 30))
        raise RuntimeError(f"Solana RPC failed after retries: {method}") from last_error


class ArchiveRpcPool:
    """Rate-limit and fail over among independent archive-capable RPCs."""

    def __init__(self, endpoints: list[tuple[str, float]], timeout: int = 30) -> None:
        if not endpoints:
            raise ValueError("at least one archive RPC endpoint is required")
        self.endpoints = []
        for configured_url, rps in endpoints:
            url = configured_url
            batch_capable = True
            use_proxy = False
            while True:
                if url.startswith("single+"):
                    batch_capable = False
                    url = url.removeprefix("single+")
                elif url.startswith("proxy+"):
                    use_proxy = True
                    url = url.removeprefix("proxy+")
                else:
                    break
            self.endpoints.append(
                {
                    "url": url,
                    "use_proxy": use_proxy,
                    "batch_capable": batch_capable,
                    "interval": 1.0 / rps,
                    "next_at": 0.0,
                }
            )
        self.timeout = timeout
        self.lock = Lock()
        self.thread_state = local()
        self.successes = 0
        self.attempts_by_host: Counter[str] = Counter()
        self.successes_by_host: Counter[str] = Counter()
        self.failures_by_host: Counter[str] = Counter()

    def _reserve_endpoint(
        self, *, require_batch: bool = False
    ) -> tuple[int, str, bool, float]:
        with self.lock:
            candidates = [
                item
                for item in range(len(self.endpoints))
                if not require_batch or bool(self.endpoints[item]["batch_capable"])
            ]
            if not candidates:
                raise RuntimeError("no batch-capable archive RPC endpoint configured")
            index = min(
                candidates,
                key=lambda item: float(self.endpoints[item]["next_at"]),
            )
            endpoint = self.endpoints[index]
            now = time.monotonic()
            scheduled = max(now, float(endpoint["next_at"]))
            endpoint["next_at"] = scheduled + float(endpoint["interval"])
            return (
                index,
                str(endpoint["url"]),
                bool(endpoint["use_proxy"]),
                max(0.0, scheduled - now),
            )

    def _cooldown(self, index: int, seconds: float) -> None:
        with self.lock:
            self.endpoints[index]["next_at"] = max(
                float(self.endpoints[index]["next_at"]),
                time.monotonic() + seconds,
            )

    @staticmethod
    def _host(url: str) -> str:
        return url.split("//", 1)[-1].split("/", 1)[0]

    def _record_attempt(self, url: str) -> None:
        with self.lock:
            self.attempts_by_host[self._host(url)] += 1

    def _record_failure(self, url: str) -> None:
        with self.lock:
            self.failures_by_host[self._host(url)] += 1

    def _record_success(self, url: str) -> None:
        with self.lock:
            host = self._host(url)
            self.successes += 1
            self.successes_by_host[host] += 1
            if self.successes % 100 == 0:
                print(
                    "Pyth archive RPC progress "
                    f"{self.successes:,}: success={dict(self.successes_by_host)} "
                    f"failure={dict(self.failures_by_host)}",
                    flush=True,
                )

    def _session(self, use_proxy: bool) -> requests.Session:
        sessions = getattr(self.thread_state, "sessions", None)
        if sessions is None:
            sessions = {}
            self.thread_state.sessions = sessions
        session = sessions.get(use_proxy)
        if session is None:
            session = requests.Session()
            # The workspace proxy is shared and its provider-side IP rate limits
            # are much lower than the public RPCs' per-client limits. These
            # archive hosts are reachable directly, so use the host connection.
            session.trust_env = use_proxy
            session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"
            sessions[use_proxy] = session
        return session

    def get_transaction(self, signature: str) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(40):
            endpoint_index, url, use_proxy, wait_seconds = self._reserve_endpoint()
            if wait_seconds:
                time.sleep(wait_seconds)
            self._record_attempt(url)
            try:
                response = self._session(use_proxy).post(
                    url,
                    json={
                        "jsonrpc": "2.0",
                        "id": signature[:16],
                        "method": "getTransaction",
                        "params": [
                            signature,
                            {
                                "commitment": "finalized",
                                "encoding": "jsonParsed",
                                "maxSupportedTransactionVersion": 0,
                            },
                        ],
                    },
                    timeout=self.timeout,
                )
                if response.status_code == 429:
                    self._cooldown(endpoint_index, min(0.5 + 0.25 * attempt, 5.0))
                    raise requests.RequestException("HTTP 429")
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    error = body["error"]
                    code = error.get("code") if isinstance(error, dict) else None
                    if code == 429 or code == -32005:
                        self._cooldown(endpoint_index, min(0.5 + 0.25 * attempt, 5.0))
                    raise RuntimeError(str(body["error"]))
                transaction = body.get("result")
                if transaction is None:
                    # Every requested signature came from the program's finalized
                    # signature history. A null response therefore means this
                    # particular routed backend lacks archive data; fail over.
                    self._cooldown(endpoint_index, 0.2)
                    raise RuntimeError("archive backend returned null")
                self._record_success(url)
                return transaction
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                self._record_failure(url)
                time.sleep(min(0.05 * (attempt + 1), 0.5))
        raise RuntimeError(
            f"all archive RPCs failed for transaction {signature}"
        ) from last_error

    def _get_transaction_batch(
        self, signatures: list[str]
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        """Fetch up to ten transactions in one archive-RPC HTTP request.

        Public Solana archive providers commonly support JSON-RPC batches even
        when an occasional backend shard returns ``null`` for one signature.
        Successful members are retained and only missing members fall back to
        the single-transaction failover path.
        """
        last_error: Exception | None = None
        for attempt in range(40):
            endpoint_index, url, use_proxy, wait_seconds = self._reserve_endpoint(
                require_batch=True
            )
            if wait_seconds:
                time.sleep(wait_seconds)
            self._record_attempt(url)
            try:
                response = self._session(use_proxy).post(
                    url,
                    json=[
                        {
                            "jsonrpc": "2.0",
                            "id": signature,
                            "method": "getTransaction",
                            "params": [
                                signature,
                                {
                                    "commitment": "finalized",
                                    "encoding": "jsonParsed",
                                    "maxSupportedTransactionVersion": 0,
                                },
                            ],
                        }
                        for signature in signatures
                    ],
                    timeout=self.timeout,
                )
                if response.status_code == 429:
                    self._cooldown(endpoint_index, min(0.5 + 0.25 * attempt, 5.0))
                    raise requests.RequestException("HTTP 429")
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, list):
                    if isinstance(body, dict) and body.get("error"):
                        error = body["error"]
                        code = error.get("code") if isinstance(error, dict) else None
                        if code in (429, -32005):
                            self._cooldown(
                                endpoint_index, min(0.5 + 0.25 * attempt, 5.0)
                            )
                    raise RuntimeError(f"archive batch response is not a list: {body}")
                by_id = {str(row.get("id")): row for row in body}
                found: dict[str, dict[str, Any]] = {}
                missing: list[str] = []
                for signature in signatures:
                    row = by_id.get(signature)
                    transaction = row.get("result") if row and not row.get("error") else None
                    if transaction is None:
                        missing.append(signature)
                    else:
                        found[signature] = transaction
                        self._record_success(url)
                # A valid list response proves this provider accepts JSON-RPC
                # batching. Partial nulls are repaired individually below.
                return found, missing
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                self._record_failure(url)
                time.sleep(min(0.05 * (attempt + 1), 0.5))
        raise RuntimeError("all archive RPCs rejected transaction batch") from last_error

    def get_transactions(self, signatures: list[str]) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        for offset in range(0, len(signatures), 10):
            missing = signatures[offset : offset + 10]
            # Archive gateways can route one member of an otherwise successful
            # batch to a shard that returns null. Rebatching the shrinking
            # missing set is substantially faster and gentler than immediately
            # issuing many independent retries.
            for _ in range(1):
                found, missing = self._get_transaction_batch(missing)
                output.update(found)
                if not missing:
                    break
            for signature in missing:
                output[signature] = self.get_transaction(signature)
        if set(output) != set(signatures):
            raise RuntimeError("Pyth archive batch did not return every signature")
        return output


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def collect_signatures(rpc_url: str, path: Path) -> tuple[list[dict[str, Any]], int]:
    complete_path = path.with_suffix(path.suffix + ".done.json")
    if path.is_file() and complete_path.is_file():
        rows = list(iter_gzip_jsonl(path))
        return rows, int(json.loads(complete_path.read_text(encoding="utf-8"))["pages"])
    rpc = SolanaRpc(rpc_url)
    signatures: list[dict[str, Any]] = []
    before: str | None = None
    pages = 0
    while True:
        config: dict[str, Any] = {"limit": 1_000, "commitment": "finalized"}
        if before:
            config["before"] = before
        rows = rpc.call("getSignaturesForAddress", [PROGRAM_ID, config])
        pages += 1
        if not rows:
            break
        signatures.extend(rows)
        before = rows[-1]["signature"]
        print(
            f"Pyth signatures page {pages}: {len(signatures):,}, "
            f"oldest slot {rows[-1]['slot']} time {rows[-1].get('blockTime')}",
            flush=True,
        )
        if len(rows) < 1_000:
            # Query once more with the final cursor; an empty page is the
            # completeness proof rather than assuming a short page is terminal.
            continue
    temporary = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        for row in signatures:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)
    atomic_json(complete_path, {
        "complete": True,
        "pages": pages,
        "signatures": len(signatures),
        "oldest_slot": signatures[-1]["slot"] if signatures else None,
        "oldest_block_time": signatures[-1].get("blockTime") if signatures else None,
        "empty_terminal_page_observed": True,
    })
    return signatures, pages


def iter_gzip_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            yield json.loads(line)


def collect_transaction_chunk(
    rpc_pool: ArchiveRpcPool,
    chunk_index: int,
    signatures: list[dict[str, Any]],
    raw_dir: Path,
) -> dict[str, Any]:
    path = raw_dir / f"{chunk_index:06d}.jsonl.gz"
    receipt_path = raw_dir / f"{chunk_index:06d}.done.json"
    if path.is_file() and receipt_path.is_file():
        prior = json.loads(receipt_path.read_text(encoding="utf-8"))
        if prior.get("complete") and prior.get("signatures") == len(signatures):
            return prior
    temporary = path.with_suffix(path.suffix + ".tmp")
    count = 0
    try:
        with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
            transactions = rpc_pool.get_transactions(
                [str(row["signature"]) for row in signatures]
            )
            for signature_row in signatures:
                signature = signature_row["signature"]
                transaction = transactions[signature]
                record = {"signature_record": signature_row, "transaction": transaction}
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
                count += 1
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    temporary.replace(path)
    receipt = {
        "complete": True,
        "chunk_index": chunk_index,
        "signatures": len(signatures),
        "transactions": count,
        "first_signature": signatures[0]["signature"],
        "last_signature": signatures[-1]["signature"],
        "raw_file": str(path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(receipt_path, receipt)
    with PRINT_LOCK:
        print(f"Pyth transaction chunk {chunk_index}: {count} transactions", flush=True)
    return receipt


def write_rows(jsonl_path: Path, parquet_path: Path, rows: Iterable[dict[str, Any]]) -> int:
    row_list = list(rows)
    if not row_list:
        raise RuntimeError(f"no rows for {jsonl_path.name}")
    columns = sorted({key for row in row_list for key in row})
    # Optional instruction fields are sparse and occur in different eras.  A
    # schema inferred only from the first output batch can therefore assign
    # Arrow's ``null`` type to a column that becomes a string later.  Infer and
    # unify schemas across the complete in-memory row set before streaming.
    schemas = []
    for offset in range(0, len(row_list), 25_000):
        normalized_batch = [
            {key: row.get(key) for key in columns}
            for row in row_list[offset : offset + 25_000]
        ]
        schemas.append(pa.Table.from_pylist(normalized_batch).schema)
    output_schema = pa.unify_schemas(schemas)
    jsonl_temporary = jsonl_path.with_suffix(jsonl_path.suffix + ".tmp")
    parquet_temporary = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    writer = pq.ParquetWriter(
        parquet_temporary, output_schema, compression="zstd"
    )
    batch: list[dict[str, Any]] = []
    count = 0
    with jsonl_temporary.open("w", encoding="utf-8") as handle:
        for row in row_list:
            normalized = {key: row.get(key) for key in columns}
            handle.write(json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            batch.append(normalized)
            count += 1
            if len(batch) >= 25_000:
                table = pa.Table.from_pylist(batch, schema=output_schema)
                writer.write_table(table)
                batch.clear()
        if batch:
            table = pa.Table.from_pylist(batch, schema=output_schema)
            writer.write_table(table)
    writer.close()
    jsonl_temporary.replace(jsonl_path)
    parquet_temporary.replace(parquet_path)
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Pyth OIS full Solana history")
    parser.add_argument(
        "--signature-rpc-url",
        default=os.getenv("PYTH_SOLANA_RPC_URL", DEFAULT_SIGNATURE_RPC),
    )
    parser.add_argument(
        "--transaction-rpc-url",
        action="append",
        help=(
            "Archive RPC URL, optionally prefixed with 'single+' for endpoints "
            "that reject JSON-RPC batches or 'proxy+' to use the workspace "
            "proxy, and suffixed with ',RPS'. Repeat to use a pool."
        ),
    )
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--chunk-size", type=int, default=200)
    args = parser.parse_args()
    raw_dir = (ROOT / "data/raw/pyth_ois/history").resolve()
    tx_raw_dir = raw_dir / "transactions"
    curated_dir = (ROOT / "data/curated").resolve()
    for path in (raw_dir, tx_raw_dir, curated_dir):
        path.mkdir(parents=True, exist_ok=True)

    signatures, signature_pages = collect_signatures(
        args.signature_rpc_url, raw_dir / "program_signatures.jsonl.gz"
    )
    cutoff_unix = int(CUTOFF.timestamp())
    eligible = [
        row for row in signatures
        if row.get("blockTime") is not None and int(row["blockTime"]) <= cutoff_unix
    ]
    if not eligible:
        raise RuntimeError("no Pyth OIS signatures at or before cutoff")
    # Source ordering is newest first. Chunks preserve that order and are sorted
    # chronologically only at the curated-output stage.
    chunks = [
        eligible[offset : offset + args.chunk_size]
        for offset in range(0, len(eligible), args.chunk_size)
    ]
    configured_urls = args.transaction_rpc_url
    if not configured_urls and os.getenv("PYTH_SOLANA_ARCHIVE_RPC_URL"):
        configured_urls = [os.environ["PYTH_SOLANA_ARCHIVE_RPC_URL"]]
    if configured_urls:
        endpoints = []
        for item in configured_urls:
            if "," in item:
                url, rps_text = item.rsplit(",", 1)
                endpoints.append((url, float(rps_text)))
            else:
                endpoints.append((item, 1.0))
    else:
        endpoints = list(DEFAULT_TRANSACTION_RPCS)
    rpc_pool = ArchiveRpcPool(endpoints)
    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_transaction_chunk,
                rpc_pool,
                index,
                chunk,
                tx_raw_dir,
            ): index
            for index, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            receipts.append(future.result())

    raw_records = []
    for index in range(len(chunks)):
        raw_records.extend(iter_gzip_jsonl(tx_raw_dir / f"{index:06d}.jsonl.gz"))
    raw_records.sort(
        key=lambda row: (
            int(row["transaction"]["slot"]),
            int(row["signature_record"].get("transactionIndex") or 0),
            row["signature_record"]["signature"],
        )
    )
    instruction_rows: list[dict[str, Any]] = []
    stake_rows: list[dict[str, Any]] = []
    economic_rows: list[dict[str, Any]] = []
    signatures_without_direct_instruction = 0
    for record in raw_records:
        signature = record["signature_record"]["signature"]
        instructions, stake, economics = parse_integrity_pool_transaction(
            signature, record["transaction"]
        )
        if not instructions:
            signatures_without_direct_instruction += 1
        instruction_rows.extend(instructions)
        stake_rows.extend(stake)
        economic_rows.extend(economics)

    instruction_count = write_rows(
        curated_dir / "pyth_ois_instructions.jsonl",
        curated_dir / "pyth_ois_instructions.parquet",
        instruction_rows,
    )
    stake_count = write_rows(
        curated_dir / "pyth_ois_stake_events.jsonl",
        curated_dir / "pyth_ois_stake_events.parquet",
        stake_rows,
    )
    economic_count = write_rows(
        curated_dir / "pyth_ois_economic_events.jsonl",
        curated_dir / "pyth_ois_economic_events.parquet",
        economic_rows,
    )
    instruction_counts = Counter(row["instruction"] for row in instruction_rows)
    successful_instruction_counts = Counter(
        row["instruction"] for row in instruction_rows if row["success"]
    )
    failed_unrecognized = [
        row for row in instruction_rows
        if row["instruction"] == "unrecognized_failed_instruction"
    ]
    economic_counts = Counter(row["event"] for row in economic_rows)
    reward_rows = [row for row in economic_rows if row["event"] == "reward_transfer"]
    realized_slashes = [
        row for row in economic_rows if row["event"] == "principal_slash_transfer"
    ]
    slash_parameters = [
        row for row in economic_rows if row["event"] == "slash_parameter_created"
    ]
    signature_keys = [row["signature"] for row in eligible]
    tx_count = sum(int(row["transactions"]) for row in receipts)
    first_time = min(int(row["blockTime"]) for row in eligible)
    last_time = max(int(row["blockTime"]) for row in eligible)
    manifest = {
        "dataset": "Pyth OIS complete Solana transaction, stake, reward and slash ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff": CUTOFF.isoformat(),
        "settlement_chain": "Solana Mainnet",
        "program_id": PROGRAM_ID,
        "source_commit": SOURCE_COMMIT,
        "signature_rpc_host": args.signature_rpc_url.split("//", 1)[-1].split("/", 1)[0],
        "transaction_rpc_hosts": [
            url.split("//", 1)[-1].split("/", 1)[0] for url, _ in endpoints
        ],
        "signature_pages_including_empty_terminal_page": signature_pages,
        "all_program_signatures": len(signatures),
        "program_signatures_through_cutoff": len(eligible),
        "first_included_block_time": datetime.fromtimestamp(first_time, UTC).isoformat(),
        "last_included_block_time": datetime.fromtimestamp(last_time, UTC).isoformat(),
        "transaction_chunks": len(chunks),
        "archive_transactions": tx_count,
        "direct_program_instructions": instruction_count,
        "instructions_by_type": dict(instruction_counts),
        "successful_instructions_by_type": dict(successful_instruction_counts),
        "failed_unrecognized_instructions": len(failed_unrecognized),
        "successful_unrecognized_instructions": 0,
        "signatures_without_direct_program_instruction": signatures_without_direct_instruction,
        "stake_mutation_events": stake_count,
        "economic_events": economic_count,
        "economic_events_by_type": dict(economic_counts),
        "realized_reward_transfers": len(reward_rows),
        "realized_reward_amount_raw": str(sum(int(row["amount_raw"]) for row in reward_rows)),
        "slash_parameters_created": len(slash_parameters),
        "realized_slash_transfers": len(realized_slashes),
        "realized_slash_amount_raw": str(sum(int(row["amount_raw"]) for row in realized_slashes)),
        "duplicate_signature_keys": len(signature_keys) - len(set(signature_keys)),
        "raw_directory": str(raw_dir),
        "all_required_assertions_pass": (
            tx_count == len(eligible)
            and len(signature_keys) == len(set(signature_keys))
            and successful_instruction_counts.get("initialize_pool", 0) == 1
            and successful_instruction_counts.get(
                "unrecognized_failed_instruction", 0
            ) == 0
            and all(row["block_time_unix"] <= cutoff_unix for row in instruction_rows)
        ),
        "scope_guard": (
            "Delegate/undelegate rows are position mutations. Reward rows require the "
            "AdvanceDelegationRecord SPL-token transfer into stake custody. "
            "CreateSlashEvent is only a slash parameter; a realized slash requires the "
            "Slash instruction's SPL-token transfer to slash custody. Historical weekly "
            "publisher factors overwritten by the 52-slot circular buffer are not present "
            "in Solana transaction payloads and require historical Pythnet publisher-cap state. "
            "Successful legacy Anchor IdlWrite calls are retained as non-economic deployment "
            "metadata; malformed third-party calls are retained only as failed instructions."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Pyth OIS history QC failed: {manifest}")
    manifest_path = ROOT / "data/manifests/pyth_ois_history.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Pyth OIS full transaction-history QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}

- Program signatures through cutoff: {len(eligible):,}; archive transactions: {tx_count:,}.
- Direct OIS instructions: {instruction_count:,}; types: {dict(successful_instruction_counts)}.
- Stake position mutations: {stake_count:,}.
- Realized reward transfers: {len(reward_rows):,}; amount: {manifest['realized_reward_amount_raw']} raw PYTH.
- Slash parameters created: {len(slash_parameters):,}.
- Realized slash transfers: {len(realized_slashes):,}; amount: {manifest['realized_slash_amount_raw']} raw PYTH.
- Duplicate signatures: {manifest['duplicate_signature_keys']}.

The signature walk continued to an empty terminal page. Rewards are labeled only
when the program's `AdvanceDelegationRecord` instruction caused an SPL-token
transfer into delegator or publisher stake custody. Slash parameters are kept
separate from token transfers that actually reduce stake.
"""
    (ROOT / "reports/pyth_ois_history_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
