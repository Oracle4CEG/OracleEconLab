"""Collect Tellor Layer reporter jail/unjail events and reconstruct lifecycles."""
from __future__ import annotations

import argparse
import gzip
import json
import os
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from oracle_ledger.tellor_layer import TellorClient, event_attributes


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_RPC = "https://mainnet.tellorlayer.com/rpc"
DEFAULT_API = "https://mainnet.tellorlayer.com"
UINT64_MAX = 2**64 - 1


def atomic_jsonl(path: Path, rows: list[dict[str, Any]], *, gzip_output: bool = False) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    opener = gzip.open if gzip_output else open
    with opener(temporary, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)


def normalized_txs(client: TellorClient, event_type: str, cutoff_height: int) -> list[dict[str, Any]]:
    query = f"{event_type}.reporter EXISTS AND tx.height <= {cutoff_height}"
    rows: list[dict[str, Any]] = []
    for tx in client.tx_search(query):
        decoded = client.decoded_tx(tx["hash"])
        response = decoded["tx_response"]
        rows.append({
            "tx_hash": tx["hash"].lower(),
            "height": int(tx["height"]),
            "timestamp": response["timestamp"],
            "code": int(response.get("code") or 0),
            "event_type": event_type,
            "events": response.get("events") or [],
        })
    return sorted(rows, key=lambda row: (row["height"], row["tx_hash"]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", default=os.getenv("TELLOR_RPC_URL", DEFAULT_RPC))
    parser.add_argument("--api-url", default=os.getenv("TELLOR_API_URL", DEFAULT_API))
    args = parser.parse_args()

    client = TellorClient(args.rpc_url, args.api_url)
    cutoff_height = client.height_at_or_before(CUTOFF)
    raw_dir = (ROOT / "data/raw/tellor_layer/jail").resolve()
    curated_dir = (ROOT / "data/curated").resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)

    transactions = (
        normalized_txs(client, "jailed_reporter", cutoff_height)
        + normalized_txs(client, "unjailed_reporter", cutoff_height)
    )
    transactions.sort(key=lambda row: (row["height"], row["tx_hash"], row["event_type"]))
    atomic_jsonl(raw_dir / "jail_transactions.jsonl.gz", transactions, gzip_output=True)

    events: list[dict[str, Any]] = []
    for tx in transactions:
        event_rows = event_attributes({"events": tx["events"]}, tx["event_type"])
        for event_index, attributes in enumerate(event_rows):
            duration = attributes.get("duration")
            reporter = attributes["reporter"]
            events.append({
                "jail_event_id": f"{tx['tx_hash']}:{tx['event_type']}:{event_index}",
                "event_type": tx["event_type"],
                "reporter": reporter,
                # Legacy MsgUnjailReporter was self-only and emitted no caller
                # attribute. In that version the reporter is necessarily signer.
                "caller": attributes.get("caller") or (
                    reporter if tx["event_type"] == "unjailed_reporter" else None
                ),
                "duration_seconds": int(duration) if duration is not None else None,
                "height": tx["height"],
                "block_time": tx["timestamp"],
                "tx_hash": tx["tx_hash"],
                "source_semantics": (
                    "state_transition_unjailed_to_jailed"
                    if tx["event_type"] == "jailed_reporter"
                    else "successful_explicit_unjail_transaction"
                ),
            })
    events.sort(key=lambda row: (row["height"], row["tx_hash"], row["jail_event_id"]))
    atomic_jsonl(curated_dir / "tellor_jail_events.jsonl", events)

    by_reporter: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        by_reporter[row["reporter"]].append(row)
    lifecycles: list[dict[str, Any]] = []
    unmatched_unjails = 0
    for reporter, reporter_events in sorted(by_reporter.items()):
        open_start: dict[str, Any] | None = None
        sequence = 0
        for row in reporter_events:
            if row["event_type"] == "jailed_reporter":
                if open_start is not None:
                    raise RuntimeError(f"second jail start without unjail for {reporter}")
                open_start = row
                continue
            if open_start is None:
                unmatched_unjails += 1
                continue
            sequence += 1
            start_time = datetime.fromisoformat(open_start["block_time"].replace("Z", "+00:00"))
            duration = int(open_start["duration_seconds"])
            scheduled_end = None if duration == UINT64_MAX else (start_time + timedelta(seconds=duration)).isoformat()
            lifecycles.append({
                "jail_lifecycle_id": f"{reporter}:{sequence}",
                "reporter": reporter,
                "jail_start_height": open_start["height"],
                "jail_start_time": open_start["block_time"],
                "jail_start_tx": open_start["tx_hash"],
                "jail_duration_seconds": duration,
                "jail_end_scheduled_time": scheduled_end,
                "indefinite_jail": duration == UINT64_MAX,
                "unjail_height": row["height"],
                "unjail_time": row["block_time"],
                "unjail_tx": row["tx_hash"],
                "unjail_caller": row["caller"],
                "lifecycle_status_at_cutoff": "unjailed",
                "rule_id": "TELLOR_REPORTER_JAIL_EVENT_PAIR_V1",
            })
            open_start = None
        if open_start is not None:
            sequence += 1
            start_time = datetime.fromisoformat(open_start["block_time"].replace("Z", "+00:00"))
            duration = int(open_start["duration_seconds"])
            scheduled_end = None if duration == UINT64_MAX else (start_time + timedelta(seconds=duration)).isoformat()
            lifecycles.append({
                "jail_lifecycle_id": f"{reporter}:{sequence}",
                "reporter": reporter,
                "jail_start_height": open_start["height"],
                "jail_start_time": open_start["block_time"],
                "jail_start_tx": open_start["tx_hash"],
                "jail_duration_seconds": duration,
                "jail_end_scheduled_time": scheduled_end,
                "indefinite_jail": duration == UINT64_MAX,
                "unjail_height": None,
                "unjail_time": None,
                "unjail_tx": None,
                "unjail_caller": None,
                "lifecycle_status_at_cutoff": "jail_started_no_unjail_event_by_cutoff",
                "rule_id": "TELLOR_REPORTER_JAIL_EVENT_PAIR_V1",
            })
    atomic_jsonl(curated_dir / "tellor_jail_lifecycles.jsonl", lifecycles)

    starts = [row for row in events if row["event_type"] == "jailed_reporter"]
    unjails = [row for row in events if row["event_type"] == "unjailed_reporter"]
    manifest = {
        "dataset": "Tellor Layer reporter jail lifecycle ledger",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "chain_id": "tellor-1",
        "fixed_cutoff": CUTOFF.isoformat(),
        "cutoff_height": cutoff_height,
        "jail_start_events": len(starts),
        "unjail_events": len(unjails),
        "lifecycles": len(lifecycles),
        "lifecycle_status_counts": dict(Counter(row["lifecycle_status_at_cutoff"] for row in lifecycles)),
        "duration_counts": {str(k): v for k, v in Counter(row["duration_seconds"] for row in starts).items()},
        "unmatched_unjail_events": unmatched_unjails,
        "duplicate_event_ids": len(events) - len({row["jail_event_id"] for row in events}),
        "raw_transactions": str(raw_dir / "jail_transactions.jsonl.gz"),
        "curated_events": str(curated_dir / "tellor_jail_events.jsonl"),
        "curated_lifecycles": str(curated_dir / "tellor_jail_lifecycles.jsonl"),
        "source_code_evidence": {
            "jail_start": "x/reporter/keeper/jail.go:JailReporter emits jailed_reporter only on false-to-true transition",
            "unjail": "x/reporter/keeper/msg_server.go:UnjailReporter emits unjailed_reporter after successful state mutation",
        },
        "all_required_assertions_pass": (
            len(starts) == len(lifecycles)
            and unmatched_unjails == 0
            and len(events) == len({row["jail_event_id"] for row in events})
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"Tellor jail lifecycle QC failed: {manifest}")
    path = ROOT / "data/manifests/tellor_jail_lifecycle.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
    print(path)


if __name__ == "__main__":
    main()
