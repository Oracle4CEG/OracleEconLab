"""Collect the durable rolling Pyth OIS reward state from Solana mainnet.

OIS stake/reward/slash settlement programs are deployed on Solana mainnet;
Pythnet supplies publisher-cap inputs.  The integrity-pool account retains 52
weekly reward events, so this adapter publishes that strict subset and does not
claim full history or transaction-level reward payments without an archive
Solana RPC/indexer.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from oracle_ledger.pyth_ois import EPOCH_SECONDS, FRAC64_MULTIPLIER, decode_pool_config, decode_pool_data


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
DEFAULT_RPC = "https://api.mainnet-beta.solana.com"
PROGRAM_ID = "pyti8TM4zRVBjmarcgAPmTNNAXYKJv7WVHrkrm6woLN"
REPOSITORY = "pyth-network/governance"
PYTH_DECIMALS = 6


class SolanaRpc:
    def __init__(self, url: str) -> None:
        self.url = url
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "oracle-accountability-atlas/0.1"
        self.request_id = 0

    def call(self, method: str, params: list[Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(7):
            try:
                self.request_id += 1
                response = self.session.post(
                    self.url,
                    json={"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params},
                    timeout=180,
                )
                if response.status_code == 429:
                    raise requests.RequestException("rate limited")
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    raise RuntimeError(f"Solana RPC error for {method}: {body['error']}")
                return body["result"]
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt < 6:
                    time.sleep(min(2**attempt, 30))
        raise RuntimeError(f"Solana RPC failed after retries: {method}") from last_error

    def program_accounts_by_size(self, size: int) -> list[dict[str, Any]]:
        return self.call(
            "getProgramAccounts",
            [PROGRAM_ID, {"commitment": "finalized", "encoding": "base64", "filters": [{"dataSize": size}]}],
        )


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect the Pyth OIS durable rolling reward panel")
    parser.add_argument("--rpc-url", default=os.getenv("PYTH_SOLANA_RPC_URL", DEFAULT_RPC))
    args = parser.parse_args()
    raw_dir = (ROOT / "data/raw/pyth_ois").resolve(); raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir = (ROOT / "data/curated").resolve(); curated_dir.mkdir(parents=True, exist_ok=True)
    rpc = SolanaRpc(args.rpc_url)

    source_response = requests.get(
        f"https://api.github.com/repos/{REPOSITORY}/commits/main",
        timeout=60,
        headers={"User-Agent": "oracle-accountability-atlas/0.1"},
    )
    source_response.raise_for_status()
    source_commit = source_response.json()["sha"]
    snapshot_slot = int(rpc.call("getSlot", [{"commitment": "finalized"}]))
    snapshot_time = int(rpc.call("getBlockTime", [snapshot_slot]))
    program_account = rpc.call("getAccountInfo", [PROGRAM_ID, {"commitment": "finalized", "encoding": "base64"}])
    if not program_account["value"] or not program_account["value"]["executable"]:
        raise RuntimeError("Pyth OIS program is not executable at the snapshot")
    pool_accounts = rpc.program_accounts_by_size(2 * 1024 * 1024)
    config_accounts = rpc.program_accounts_by_size(1_000)
    slash_accounts = rpc.program_accounts_by_size(56)
    if len(pool_accounts) != 1 or len(config_accounts) != 1:
        raise RuntimeError(f"unexpected OIS singleton account count: pool={len(pool_accounts)} config={len(config_accounts)}")

    pool_pubkey = pool_accounts[0]["pubkey"]
    pool_raw = base64.b64decode(pool_accounts[0]["account"]["data"][0])
    config_pubkey = config_accounts[0]["pubkey"]
    config_raw = base64.b64decode(config_accounts[0]["account"]["data"][0])
    pool = decode_pool_data(pool_raw)
    config = decode_pool_config(config_raw)
    if config["pool_data"] != pool_pubkey:
        raise RuntimeError("PoolConfig does not point to the collected PoolData account")

    cutoff_epoch = int(CUTOFF.timestamp()) // EPOCH_SECONDS
    completed_events = sorted(
        [row for row in pool["events"] if row["epoch"] and (int(row["epoch"]) + 1) * EPOCH_SECONDS <= int(CUTOFF.timestamp())],
        key=lambda row: row["epoch"],
    )
    epochs: list[dict[str, Any]] = []
    publisher_factors: list[dict[str, Any]] = []
    for event in completed_events:
        epoch = int(event["epoch"])
        y = int(event["y"])
        start = epoch * EPOCH_SECONDS
        end = (epoch + 1) * EPOCH_SECONDS
        active_publishers = 0
        for index, (self_ratio, other_ratio, fee) in enumerate(event["publisher_factors"]):
            self_rate = y * int(self_ratio) // FRAC64_MULTIPLIER
            other_rate = y * int(other_ratio) // FRAC64_MULTIPLIER
            delegator_rate = other_rate * (FRAC64_MULTIPLIER - int(fee)) // FRAC64_MULTIPLIER
            publisher_fee_rate = other_rate - delegator_rate
            if self_ratio or other_ratio:
                active_publishers += 1
            publisher_factors.append({
                "epoch_id": epoch,
                "epoch_start_time_unix": start,
                "epoch_end_time_unix": end,
                "publisher_index": index,
                "publisher": pool["publishers"][index],
                "publisher_stake_account": pool["publisher_stake_accounts"][index],
                "reward_rate_y_raw": str(y),
                "self_reward_ratio_raw": str(self_ratio),
                "delegated_reward_ratio_raw": str(other_ratio),
                "delegation_fee_raw": str(fee),
                "publisher_self_reward_rate_raw": str(self_rate),
                "delegator_net_reward_rate_raw": str(delegator_rate),
                "publisher_delegation_fee_reward_rate_raw": str(publisher_fee_rate),
                "rate_decimals": 6,
                "reward_active_regime": y > 0,
                "has_positive_reward_factor": self_ratio > 0 or other_ratio > 0,
                "source_account": pool_pubkey,
                "source_storage_index": event["storage_index"],
            })
        epochs.append({
            "epoch_id": epoch,
            "epoch_start_time_unix": start,
            "epoch_start_time": datetime.fromtimestamp(start, UTC).isoformat().replace("+00:00", "Z"),
            "epoch_end_time_unix": end,
            "epoch_end_time": datetime.fromtimestamp(end, UTC).isoformat().replace("+00:00", "Z"),
            "reward_rate_y_raw": str(y),
            "rate_decimals": 6,
            "reward_active_regime": y > 0,
            "registered_publishers_in_current_stable_index": len(pool["publishers"]),
            "publishers_with_positive_reward_factor": active_publishers,
            "source_account": pool_pubkey,
            "source_storage_index": event["storage_index"],
        })

    slash_counters = [
        {
            "publisher_index": index,
            "publisher": publisher,
            "slash_events_created_lifetime": int(pool["slash_counters"][index]),
            "source_account": pool_pubkey,
            "snapshot_slot": snapshot_slot,
            "snapshot_time_unix": snapshot_time,
        }
        for index, publisher in enumerate(pool["publishers"])
    ]
    write_jsonl(curated_dir / "pyth_ois_reward_epochs.jsonl", epochs)
    write_jsonl(curated_dir / "pyth_ois_publisher_epoch_factors.jsonl", publisher_factors)
    write_jsonl(curated_dir / "pyth_ois_slash_counters.jsonl", slash_counters)

    raw_snapshot = {
        "program_id": PROGRAM_ID,
        "program_account": program_account,
        "snapshot_slot": snapshot_slot,
        "snapshot_time_unix": snapshot_time,
        "pool_pubkey": pool_pubkey,
        "pool_data_base64": base64.b64encode(pool_raw).decode(),
        "config_pubkey": config_pubkey,
        "pool_config_base64": base64.b64encode(config_raw).decode(),
        "slash_event_accounts": slash_accounts,
    }
    raw_path = raw_dir / "rolling_program_state.json.gz"
    temporary = raw_path.with_suffix(raw_path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=9) as handle:
        json.dump(raw_snapshot, handle, sort_keys=True, separators=(",", ":")); handle.write("\n")
    temporary.replace(raw_path)

    epoch_ids = [row["epoch_id"] for row in epochs]
    zero_epochs = [row["epoch_id"] for row in epochs if not row["reward_active_regime"]]
    nonzero_epochs = [row["epoch_id"] for row in epochs if row["reward_active_regime"]]
    all_assertions = (
        bool(epochs)
        and len(epoch_ids) == len(set(epoch_ids))
        and max(epoch_ids) <= cutoff_epoch - 1
        and all(int(row["reward_rate_y_raw"]) == 1_924 for row in epochs if row["epoch_id"] <= 2_936)
        and all(int(row["reward_rate_y_raw"]) == 0 for row in epochs if row["epoch_id"] >= 2_937)
        and len(publisher_factors) == len(epochs) * len(pool["publishers"])
        and sum(pool["slash_counters"]) == len(slash_accounts) == 0
    )
    manifest = {
        "dataset": "Pyth Oracle Integrity Staking durable rolling reward panel",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff": CUTOFF.isoformat(),
        "settlement_chain": "Solana_Mainnet",
        "publisher_cap_input_chain": "Pythnet",
        "program_id": PROGRAM_ID,
        "pool_data_account": pool_pubkey,
        "pool_config_account": config_pubkey,
        "source_repository": f"https://github.com/{REPOSITORY}",
        "source_commit": source_commit,
        "snapshot_slot": snapshot_slot,
        "snapshot_time": datetime.fromtimestamp(snapshot_time, UTC).isoformat(),
        "raw_snapshot": str(raw_path),
        "raw_snapshot_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
        "first_complete_retained_epoch": min(epoch_ids),
        "last_complete_epoch_at_cutoff": max(epoch_ids),
        "complete_epochs": len(epochs),
        "reward_active_epochs": len(nonzero_epochs),
        "reward_zero_epochs": len(zero_epochs),
        "first_zero_reward_epoch": min(zero_epochs) if zero_epochs else None,
        "publishers": len(pool["publishers"]),
        "publisher_epoch_factor_rows": len(publisher_factors),
        "durable_lifetime_slash_counter_sum": sum(pool["slash_counters"]),
        "open_slash_event_accounts": len(slash_accounts),
        "all_required_assertions_pass": all_assertions,
        "scope_limit": "The current PoolData circular buffer retains 52 events; the strict complete-epoch subset is 2025-07-17 through 2026-06-25. Full deployment history, per-position stake history, actual reward transfers, supported-symbol history, and transaction-level slashes require archive Solana/indexer data plus Pythnet publisher metadata. Current delegation balances are intentionally excluded because the snapshot is after the fixed cutoff.",
    }
    if not all_assertions:
        raise RuntimeError(f"Pyth OIS QC failed: {manifest}")
    manifest_path = ROOT / "data/manifests/pyth_ois_rolling_state.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = f"""# Pyth OIS durable rolling-state QC

Generated: {manifest['generated_at_utc']}  
Settlement chain: Solana Mainnet  
Fixed cutoff: {manifest['fixed_cutoff']}

## Result

- The official OIS integrity-pool program `{PROGRAM_ID}` is deployed on Solana Mainnet. Pythnet supplies publisher-cap messages; it is not the OIS stake settlement chain.
- Durable complete reward epochs recovered from the 52-slot circular buffer: {len(epochs)} ({min(epoch_ids)}–{max(epoch_ids)}, 2025-07-17 through 2026-06-25).
- Publisher/epoch reward-factor rows: {len(publisher_factors):,} across {len(pool['publishers'])} stable publisher indices.
- Reward-active epochs (`Y=1924`): {len(nonzero_epochs)}. Reward-paused epochs (`Y=0`): {len(zero_epochs)}, beginning with epoch {min(zero_epochs)} (2026-04-16 through 2026-04-23), matching the OP-PIP-103 transition week.
- Durable lifetime `num_slash_events` counter total: {sum(pool['slash_counters'])}; open `SlashEvent` accounts: {len(slash_accounts)}. No realized OIS slash event is observed in these on-chain counters.

## Interpretation guards

- `Y` and publisher factors are rates with six decimal places, not paid reward amounts.
- A positive factor is reward eligibility/cap evidence, not an objective data-correctness judgment.
- Zero observed slash counters means no realized slash was found in this durable program state; it does not mean the slashing mechanism is absent.
- Current stake/delegation balances are excluded because the live snapshot occurs after the fixed cutoff.
- Full history before the circular buffer, per-position epoch stake, reward transfers, publisher symbols/quality metrics, and transaction evidence require an archive Solana RPC/indexer plus Pythnet metadata access.
"""
    (ROOT / "reports/pyth_ois_qc.md").write_text(report, encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
