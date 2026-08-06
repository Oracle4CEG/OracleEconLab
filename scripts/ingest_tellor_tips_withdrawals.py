"""Collect Tellor query-tip funding and realized tip withdrawal cash flows."""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from oracle_ledger.tellor_layer import TellorClient
from oracle_ledger.tellor_payments import (
    parse_tip_transactions,
    parse_withdraw_transactions,
)


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_RPC = "https://mainnet.tellorlayer.com/rpc"
DEFAULT_API = "https://mainnet.tellorlayer.com"
ACTIONS = {
    "tip": "/layer.oracle.MsgTip",
    "withdraw": "/layer.reporter.MsgWithdrawTip",
}
SOURCE_COMMIT = "943a2709ef0a60eb560447278b2f59923b9de484"
PRINT_LOCK = Lock()


class TxRpc:
    def __init__(self, url: str, timeout: int = 180) -> None:
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
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
        raise RuntimeError("Tellor transaction RPC failed after retries") from last_error

    def tx_search(self, query: str, page: int) -> dict[str, Any]:
        return self.post(
            {
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
        )["result"]

    def block_time_batch(self, heights: list[int]) -> dict[int, str]:
        rows = self.post(
            [
                {
                    "jsonrpc": "2.0",
                    "id": height,
                    "method": "block",
                    "params": {"height": str(height)},
                }
                for height in heights
            ]
        )
        if not isinstance(rows, list):
            raise RuntimeError("Tellor block batch is not a list")
        by_id = {int(row["id"]): row for row in rows}
        output: dict[int, str] = {}
        for height in heights:
            row = by_id[height]
            if row.get("error"):
                raise RuntimeError(f"Tellor block lookup failed at {height}: {row['error']}")
            output[height] = row["result"]["block"]["header"]["time"]
        return output


def load_block_time_cache(path: Path) -> dict[int, str]:
    output: dict[int, str] = {}
    if not path.is_file():
        return output
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            output[int(row["height"])] = str(row["block_time"])
    return output


def fetch_block_times(
    rpc_url: str,
    heights: list[int],
    cache_path: Path,
    workers: int,
) -> dict[int, str]:
    """Fetch exact block times concurrently and checkpoint every RPC batch."""
    output = load_block_time_cache(cache_path)
    pending = [height for height in heights if height not in output]
    batches = [pending[offset : offset + 10] for offset in range(0, len(pending), 10)]
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    completed = 0
    with cache_path.open("a", encoding="utf-8") as cache_handle:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {
                executor.submit(TxRpc(rpc_url).block_time_batch, batch): batch
                for batch in batches
            }
            for future in as_completed(futures):
                rows = future.result()
                for height in sorted(rows):
                    output[height] = rows[height]
                    cache_handle.write(
                        json.dumps(
                            {"height": height, "block_time": rows[height]},
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
                cache_handle.flush()
                completed += 1
                if completed % 100 == 0:
                    print(
                        f"Tellor block-time batches {completed:,}/{len(batches):,}",
                        flush=True,
                    )
    missing = [height for height in heights if height not in output]
    if missing:
        raise RuntimeError(f"missing Tellor block times: {missing[:10]}")
    return output


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def collect_segment(
    rpc_url: str,
    action_name: str,
    action: str,
    start: int,
    end: int,
    raw_dir: Path,
) -> dict[str, Any]:
    action_dir = raw_dir / action_name
    action_dir.mkdir(parents=True, exist_ok=True)
    name = f"{start:08d}_{end:08d}"
    raw_path = action_dir / f"{name}.jsonl.gz"
    done_path = action_dir / f"{name}.done.json"
    if raw_path.is_file() and done_path.is_file():
        prior = json.loads(done_path.read_text(encoding="utf-8"))
        if prior.get("complete"):
            return prior
    client = TxRpc(rpc_url)
    query = (
        f"message.action='{action}' AND tx.height >= {start} AND tx.height <= {end}"
    )
    transactions: list[dict[str, Any]] = []
    page = 1
    total: int | None = None
    while total is None or len(transactions) < total:
        result = client.tx_search(query, page)
        if total is None:
            total = int(result.get("total_count") or 0)
        rows = result.get("txs") or []
        transactions.extend(rows)
        if not rows:
            break
        page += 1
    if total is None or len(transactions) != total:
        raise RuntimeError(
            f"incomplete Tellor {action_name} tx search {start}-{end}: "
            f"{len(transactions)} != {total}"
        )
    hashes = [str(row["hash"]) for row in transactions]
    if len(hashes) != len(set(hashes)):
        raise RuntimeError(f"duplicate Tellor {action_name} transactions {start}-{end}")
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        for row in transactions:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(raw_path)
    receipt = {
        "complete": True,
        "action_name": action_name,
        "action": action,
        "start_height": start,
        "end_height": end,
        "transactions": len(transactions),
        "pages": page - 1,
        "raw_file": str(raw_path),
        "finished_at_utc": datetime.now(UTC).isoformat(),
    }
    atomic_json(done_path, receipt)
    if transactions:
        with PRINT_LOCK:
            print(
                f"Tellor {action_name} {start}-{end}: {len(transactions):,} txs",
                flush=True,
            )
    return receipt


def iter_transactions(paths: Iterable[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield json.loads(line)


def action_event_count(transactions: list[dict[str, Any]], action: str) -> int:
    count = 0
    for tx in transactions:
        for event in tx.get("tx_result", {}).get("events") or []:
            if event.get("type") != "message":
                continue
            attrs = {
                str(row["key"]): str(row["value"])
                for row in event.get("attributes") or []
            }
            if attrs.get("action") == action:
                count += 1
    return count


def write_rows(rows: list[dict[str, Any]], jsonl_path: Path, parquet_path: Path) -> None:
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
    parser = argparse.ArgumentParser(description="Collect Tellor tip and withdrawal transactions")
    parser.add_argument("--rpc-url", default=os.getenv("TELLOR_RPC_URL", DEFAULT_RPC))
    parser.add_argument("--api-url", default=os.getenv("TELLOR_API_URL", DEFAULT_API))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--time-workers", type=int, default=6)
    parser.add_argument("--segment-heights", type=int, default=100_000)
    args = parser.parse_args()
    chain = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = chain.height_at_or_before(CUTOFF)
    cutoff_time = chain.block_time(cutoff_height).isoformat()
    raw_dir = (ROOT / "data/raw/tellor_layer/tips_withdrawals").resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)
    segments = [
        (start, min(start + args.segment_heights - 1, cutoff_height))
        for start in range(1, cutoff_height + 1, args.segment_heights)
    ]
    receipts: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                collect_segment,
                args.rpc_url,
                action_name,
                action,
                start,
                end,
                raw_dir,
            ): (action_name, start, end)
            for action_name, action in ACTIONS.items()
            for start, end in segments
        }
        for future in as_completed(futures):
            receipts.append(future.result())

    receipt_by_key = {
        (row["action_name"], int(row["start_height"])): row for row in receipts
    }
    transactions: dict[str, list[dict[str, Any]]] = {}
    for action_name in ACTIONS:
        paths = [
            Path(receipt_by_key[(action_name, start)]["raw_file"])
            for start, _ in segments
        ]
        transactions[action_name] = list(iter_transactions(paths))
    all_heights = sorted(
        {
            int(tx["height"])
            for action_transactions in transactions.values()
            for tx in action_transactions
        }
    )
    time_by_height = fetch_block_times(
        args.rpc_url,
        all_heights,
        raw_dir / "block_times.jsonl",
        args.time_workers,
    )
    tip_rows = parse_tip_transactions(transactions["tip"], time_by_height)
    withdraw_rows = parse_withdraw_transactions(
        transactions["withdraw"], time_by_height
    )
    tip_rows.sort(key=lambda row: (row["height"], row["source_tx"], row["event_index"]))
    withdraw_rows.sort(
        key=lambda row: (row["height"], row["source_tx"], row["event_index"])
    )
    write_rows(
        tip_rows,
        curated_dir / "tellor_query_tip_funding.jsonl",
        curated_dir / "tellor_query_tip_funding.parquet",
    )
    write_rows(
        withdraw_rows,
        curated_dir / "tellor_tip_withdrawals_realized.jsonl",
        curated_dir / "tellor_tip_withdrawals_realized.parquet",
    )
    tip_action_events = action_event_count(transactions["tip"], ACTIONS["tip"])
    withdraw_action_events = action_event_count(
        transactions["withdraw"], ACTIONS["withdraw"]
    )
    tip_cashflow_failures = sum(not row["cashflow_verified"] for row in tip_rows)
    withdraw_cashflow_failures = sum(
        not row["cashflow_verified"] for row in withdraw_rows
    )
    tip_gross = sum(int(row["gross_tip_loya_raw"]) for row in tip_rows)
    tip_burn = sum(int(row["protocol_burn_loya_raw"]) for row in tip_rows)
    tip_net = sum(int(row["net_tip_funding_loya_raw"]) for row in tip_rows)
    realized = sum(
        int(row["reward_withdrawn_to_stake_loya_raw"]) for row in withdraw_rows
    )
    withdrawal_schema_counts: dict[str, int] = {}
    for row in withdraw_rows:
        schema = str(row["withdraw_event_schema"])
        withdrawal_schema_counts[schema] = withdrawal_schema_counts.get(schema, 0) + 1
    manifest = {
        "dataset": "Tellor query-tip funding and realized withdrawal ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "cutoff_block_time": cutoff_time,
        "source_commit": SOURCE_COMMIT,
        "segments_per_action": len(segments),
        "tip_transactions": len(transactions["tip"]),
        "tip_action_events": tip_action_events,
        "tip_funding_events": len(tip_rows),
        "gross_tip_loya_raw": str(tip_gross),
        "protocol_tip_burn_loya_raw": str(tip_burn),
        "net_tip_funding_loya_raw": str(tip_net),
        "tip_cashflow_failures": tip_cashflow_failures,
        "withdraw_transactions": len(transactions["withdraw"]),
        "withdraw_action_events": withdraw_action_events,
        "realized_withdrawal_events": len(withdraw_rows),
        "realized_withdrawal_loya_raw": str(realized),
        "withdrawal_event_schema_counts": withdrawal_schema_counts,
        "withdraw_cashflow_failures": withdraw_cashflow_failures,
        "all_required_assertions_pass": (
            tip_action_events == len(tip_rows)
            and withdraw_action_events == len(withdraw_rows)
            and tip_cashflow_failures == 0
            and withdraw_cashflow_failures == 0
            and tip_gross - tip_burn == tip_net
            and len(tip_rows) > 0
            and len(withdraw_rows) > 0
        ),
        "scope_guard": (
            "tip_added is query reward funding after the protocol's 2% burn, "
            "not a reporter payment. tip_withdrawn is realized reward because "
            "the TipsEscrow module cash flow is delegated into the selector's stake."
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor tip/withdrawal QC failed: {manifest}")
    manifest_path = ROOT / "data/manifests/tellor_tips_withdrawals.json"
    atomic_json(manifest_path, manifest)
    report = f"""# Tellor tip funding / realized withdrawal QC

Generated: {manifest['generated_at_utc']}  
Fixed cutoff: {manifest['fixed_cutoff']}  
Cutoff height: {cutoff_height:,}

- Tip funding events: {len(tip_rows):,}.
- Gross tip funding: {tip_gross:,} loya.
- Protocol burn: {tip_burn:,} loya.
- Net query reward funding: {tip_net:,} loya.
- Realized withdrawals compounded to stake: {len(withdraw_rows):,}.
- Realized withdrawal amount: {realized:,} loya.
- Withdrawal event schemas: {withdrawal_schema_counts}.
- Cash-flow failures: tip {tip_cashflow_failures}, withdrawal {withdraw_cashflow_failures}.
"""
    (ROOT / "reports/tellor_tips_withdrawals_qc.md").write_text(
        report, encoding="utf-8"
    )
    print(manifest_path)


if __name__ == "__main__":
    main()
