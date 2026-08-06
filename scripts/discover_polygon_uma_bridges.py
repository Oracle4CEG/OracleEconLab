"""Discover historical Polygon DVM bridge addresses from disputed OO receipts."""
from __future__ import annotations

import gzip
import hashlib
import json
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from oracle_ledger.polygon_uma import TOPIC_TO_SIGNATURE, load_env_url
from oracle_ledger.rpc import JsonRpc, RpcError, write_json


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    unlinked: set[str] = set()
    with (ROOT / "data/curated/uma_polygon_ethereum_grade_a_links.jsonl").open() as handle:
        for line in handle:
            row = json.loads(line)
            if row["cross_chain_match_grade"] == "U":
                unlinked.add(row["oo_request_id"])
    transactions: set[str] = set()
    with (ROOT / "data/curated/polygon_uma_request_rounds.jsonl").open() as handle:
        for line in handle:
            row = json.loads(line)
            if row["oo_request_id"] in unlinked and row.get("dispute_tx"):
                transactions.add(row["dispute_tx"])
    rpc_url = load_env_url(ROOT / ".env", "NODE_URL2")

    def receipt(tx: str):
        rpc = JsonRpc(rpc_url, timeout_seconds=60)
        last = None
        for attempt in range(6):
            try:
                return rpc.call("eth_getTransactionReceipt", [tx])
            except RpcError as exc:
                last = exc; time.sleep(min(2**attempt, 15))
        raise RuntimeError(f"receipt failed {tx}: {last}")

    receipts = []
    with ThreadPoolExecutor(max_workers=24) as executor:
        futures = [executor.submit(receipt, tx) for tx in sorted(transactions)]
        for index, future in enumerate(as_completed(futures), 1):
            receipts.append(future.result())
            if index % 100 == 0:
                print(f"receipts {index}/{len(futures)}", flush=True)
    receipts.sort(key=lambda row: (int(row["blockNumber"], 16), int(row["transactionIndex"], 16)))
    raw = ROOT / "data/raw/polygon/uma_bridge_discovery/dispute_receipts.jsonl.gz"
    raw.parent.mkdir(parents=True, exist_ok=True)
    temporary = raw.with_suffix(raw.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=9) as handle:
        for row in receipts:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(raw)
    raw.with_suffix(raw.suffix + ".sha256").write_text(hashlib.sha256(raw.read_bytes()).hexdigest() + "\n", encoding="utf-8")

    addresses: Counter[str] = Counter(); blocks = []
    for item in receipts:
        blocks.append(int(item["blockNumber"], 16))
        for log in item["logs"]:
            signature = TOPIC_TO_SIGNATURE.get(log["topics"][0].lower(), "")
            if signature.startswith("PriceRequestAdded("):
                addresses[log["address"].lower()] += 1
    manifest = {
        "unlinked_disputes": len(unlinked), "receipts": len(receipts),
        "block_range": [min(blocks), max(blocks)] if blocks else None,
        "price_request_added_addresses": dict(sorted(addresses.items())),
        "raw_receipts": str(raw),
    }
    output = ROOT / "data/manifests/polygon_uma_bridge_discovery.json"
    write_json(output, manifest); print(output)


if __name__ == "__main__":
    main()
