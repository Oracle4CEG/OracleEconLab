#!/usr/bin/env python3
"""Validate a deterministic CLOB price sample against Polygon OrderFilled logs."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/polymarket/decision_time_price_onchain_validation_v1"
OUT = ROOT / "data/curated/parquet/polymarket_decision_time_price_onchain_validation.parquet"
MANIFEST = ROOT / "data/manifests/polymarket_decision_time_price_onchain_validation.json"
REPORT = ROOT / "reports/polymarket_decision_time_price_onchain_validation.md"
ORDER_FILLED = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
EXCHANGES = [
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
]
BLOCK_WINDOW = 5000
TOLERANCE = 0.02


def env_rpc() -> str | None:
    values = dict(os.environ)
    if values.get("ORACLE_NATURE_OFFLINE", "0").lower() in {"1", "true", "yes"}:
        return None
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                values.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return values.get("NODE_URL2") or values.get("POLYGON_RPC_URL")


def rpc(url: str, method: str, params: list[Any]) -> Any:
    response = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=120)
    response.raise_for_status()
    body = response.json()
    if body.get("error"):
        raise RuntimeError(f"{method}: {body['error']}")
    return body["result"]


def select_sample() -> pd.DataFrame:
    samples = pd.read_parquet(ROOT / "data/applications/trustworthy_ai_challenge/decision_samples.parquet")
    prices = pd.read_parquet(ROOT / "data/curated/parquet/polymarket_decision_time_prices.parquet")
    rounds = pd.read_parquet(ROOT / "data/curated/parquet/polygon_uma_request_rounds.parquet")[["oo_request_id", "proposal_block"]]
    frame = samples.merge(prices, on=["sample_id", "decision_time_unix"]).merge(
        rounds, left_on="sample_id", right_on="oo_request_id", validate="one_to_one"
    )
    frame = frame[
        frame.coverage_status.eq("complete")
        & frame.yes_last_price.notna() & frame.no_last_price.notna()
        & (frame.decision_time_unix < 1777374000)  # Before the documented 2026-04-28 V2 cutover.
    ].sort_values("decision_time_unix")
    selected = []
    for outcome in [0, 1]:
        group = frame[frame.proposal_rejected_by_protocol.eq(outcome)].reset_index(drop=True)
        indices = sorted(set(round(value) for value in pd.Series(range(12)).map(lambda i: i * (len(group) - 1) / 11)))
        selected.append(group.iloc[indices])
    result = pd.concat(selected).sort_values(["decision_time_unix", "sample_id"]).reset_index(drop=True)
    if len(result) != 24:
        raise RuntimeError(f"expected 24 validation cases, got {len(result)}")
    return result


def cache_path(sample_id: str) -> Path:
    return RAW / f"{sample_id.removeprefix('0x')}.json.gz"


def decode_log(log: dict[str, Any], tokens: set[int]) -> dict[str, Any] | None:
    data = log["data"].removeprefix("0x")
    values = [int(data[index * 64:(index + 1) * 64], 16) for index in range(5)]
    maker_asset, taker_asset, maker_amount, taker_amount, fee = values
    token = taker_asset if maker_asset == 0 else maker_asset if taker_asset == 0 else None
    if token not in tokens or maker_amount == 0 or taker_amount == 0:
        return None
    price = maker_amount / taker_amount if maker_asset == 0 else taker_amount / maker_amount
    return {
        "block_number": int(log["blockNumber"], 16), "log_index": int(log["logIndex"], 16),
        "transaction_hash": log["transactionHash"].lower(), "contract": log["address"].lower(),
        "token_id": str(token), "price": float(price), "fee_raw": str(fee),
    }


def validate_one(row: dict[str, Any], url: str | None) -> dict[str, Any]:
    target = cache_path(row["sample_id"])
    if target.exists():
        with gzip.open(target, "rt", encoding="utf-8") as handle:
            cached = json.load(handle)
        if cached.get("from_block") == int(row["proposal_block"]) - BLOCK_WINDOW:
            return cached
    if not url:
        raise RuntimeError(f"missing cached on-chain audit and Polygon RPC for {row['sample_id']}")
    price_raw = ROOT / "data/raw/polymarket/decision_time_prices_v1" / f"{row['sample_id'].removeprefix('0x')}.json.gz"
    with gzip.open(price_raw, "rt", encoding="utf-8") as handle:
        price_payload = json.load(handle)
    yes_token = str(price_payload["primary_yes"]["token_id"])
    no_token = str(price_payload["secondary_no"]["token_id"])
    proposal_block = int(row["proposal_block"])
    query = {
        "fromBlock": hex(proposal_block - BLOCK_WINDOW), "toBlock": hex(proposal_block),
        "address": EXCHANGES, "topics": [ORDER_FILLED],
    }
    logs = rpc(url, "eth_getLogs", [query])
    decoded = [value for log in logs if (value := decode_log(log, {int(yes_token), int(no_token)}))]
    by_token: dict[str, dict[str, Any]] = {}
    for value in decoded:
        by_token[value["token_id"]] = value
    receipts = {}
    for value in by_token.values():
        tx = value["transaction_hash"]
        receipts[tx] = rpc(url, "eth_getTransactionReceipt", [tx])
    payload = {
        "sample_id": row["sample_id"], "proposal_block": proposal_block,
        "from_block": proposal_block - BLOCK_WINDOW, "contracts": EXCHANGES,
        "topic0": ORDER_FILLED, "rpc_endpoint_redacted": True,
        "rpc_log_count": len(logs), "relevant_decoded_logs": decoded,
        "last_by_token": by_token, "receipts": receipts,
        "yes_token": yes_token, "no_token": no_token,
        "api_yes_price": float(row["yes_last_price"]), "api_no_price": float(row["no_last_price"]),
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(target, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
    return payload


def main() -> None:
    sample = select_sample()
    url = env_rpc()
    if url and int(rpc(url, "eth_chainId", []), 16) != 137:
        raise RuntimeError("NODE_URL2 is not Polygon chain 137")
    payloads = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(validate_one, row, url): row["sample_id"] for row in sample.to_dict("records")}
        for future in as_completed(futures):
            payloads.append(future.result())
    rows = []
    for payload in sorted(payloads, key=lambda value: value["sample_id"]):
        yes = payload["last_by_token"].get(payload["yes_token"])
        no = payload["last_by_token"].get(payload["no_token"])
        for side, value, api_price in [("yes", yes, payload["api_yes_price"]), ("no", no, payload["api_no_price"])]:
            receipt = payload["receipts"].get(value["transaction_hash"]) if value else None
            rows.append({
                "sample_id": payload["sample_id"], "side": side,
                "api_price": api_price, "onchain_price": None if value is None else value["price"],
                "absolute_difference": None if value is None else abs(api_price - value["price"]),
                "transaction_hash": None if value is None else value["transaction_hash"],
                "source_block": None if value is None else value["block_number"],
                "source_log_index": None if value is None else value["log_index"],
                "exchange_contract": None if value is None else value["contract"],
                "receipt_status": None if receipt is None else int(receipt["status"], 16),
                "before_or_at_proposal": False if value is None else value["block_number"] <= payload["proposal_block"],
                "within_price_tolerance": False if value is None else abs(api_price - value["price"]) <= TOLERANCE,
                "raw_snapshot": str(cache_path(payload["sample_id"])),
                "raw_sha256": hashlib.sha256(cache_path(payload["sample_id"]).read_bytes()).hexdigest(),
            })
    result = pd.DataFrame(rows)
    checks = {
        "sample_cases": result.sample_id.nunique(), "side_rows": len(result),
        "both_sides_found": int(result.onchain_price.notna().sum()),
        "successful_receipts": int(result.receipt_status.eq(1).sum()),
        "before_or_at_proposal": int(result.before_or_at_proposal.sum()),
        "within_0_02_price_tolerance": int(result.within_price_tolerance.sum()),
        "post_decision_logs": int((~result.before_or_at_proposal & result.onchain_price.notna()).sum()),
        "median_absolute_price_difference": float(result.absolute_difference.median()),
        "p95_absolute_price_difference": float(result.absolute_difference.quantile(.95)),
        "api_onchain_spearman": float(result[["api_price", "onchain_price"]].corr(method="spearman").iloc[0, 1]),
    }
    if checks["sample_cases"] != 24 or checks["side_rows"] != 48 or checks["post_decision_logs"] != 0:
        raise RuntimeError(f"on-chain price audit failed: {checks}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(OUT, index=False)
    manifest = {
        "dataset": "Polymarket decision-time price on-chain validation",
        "sampling": "24 deterministic cases stratified by protocol outcome and decision time",
        "chain_id": 137, "block_window": BLOCK_WINDOW, "price_tolerance": TOLERANCE,
        "checks": checks, "output": str(OUT),
        "evidence_grade_rule": "Grade B corroborated when the linked token has a successful pre-decision OrderFilled receipt; API sampling price is not asserted equal to the final on-chain fill.",
        "all_required_assertions_pass": (
            checks["both_sides_found"] >= 40
            and checks["successful_receipts"] >= 40
            and checks["api_onchain_spearman"] >= 0.8
        ),
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"insufficient on-chain validation coverage: {checks}")
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")
    REPORT.write_text(
        "# Polymarket decision-time price on-chain validation\n\n"
        f"- Deterministic cases: {checks['sample_cases']}; side observations: {checks['side_rows']}.\n"
        f"- Last OrderFilled observations found: {checks['both_sides_found']}/48.\n"
        f"- Successful transaction receipts: {checks['successful_receipts']}/48.\n"
        f"- API/on-chain prices within ±{TOLERANCE:.2f}: {checks['within_0_02_price_tolerance']}/48.\n"
        f"- Median absolute API/final-fill difference: {checks['median_absolute_price_difference']:.4f}; Spearman correlation: {checks['api_onchain_spearman']:.3f}.\n"
        f"- Post-decision logs: {checks['post_decision_logs']}.\n\n"
        "This deterministic audit corroborates market/token/transaction linkage against legacy Polygon CTF/NegRisk Exchange OrderFilled logs before the documented V2 cutover. The API series is sampled, so it is not asserted equal to the final fill before proposal.\n",
        encoding="utf-8",
    )
    print(json.dumps(checks, indent=2))


if __name__ == "__main__":
    main()
