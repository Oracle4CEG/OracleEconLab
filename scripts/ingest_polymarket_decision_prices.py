#!/usr/bin/env python3
"""Freeze proposal-time Polymarket price histories for the 810 UMA benchmark.

The official CLOB API is queried with endTs equal to the OOV2 proposal time.
Gamma token IDs are used only as linkage keys; markets-by-token establishes the
primary (Yes) and secondary (No) orientation rather than trusting list order.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
CURATED = Path(os.environ.get(
    "ORACLE_NATURE_CURATED_ROOT", str(ROOT / "data/curated")
))
RAW = ROOT / "data/raw/polymarket/decision_time_prices_v1"
OUT = CURATED / "parquet/polymarket_decision_time_prices.parquet"
PROVENANCE = CURATED / "parquet/polymarket_decision_time_price_provenance.parquet"
MANIFEST = ROOT / "data/manifests/polymarket_decision_time_prices.json"
REPORT = ROOT / "reports/polymarket_decision_time_prices_qc.md"
BASE = "https://clob.polymarket.com"
WINDOW_SECONDS = 7 * 86400
FIDELITY_MINUTES = 10


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def request_json(path: str, params: dict[str, Any] | None = None, attempts: int = 5) -> Any:
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(BASE + path, params=params, timeout=40)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            error = exc
            time.sleep(min(4.0, 0.25 * 2**attempt))
    raise RuntimeError(f"CLOB request failed for {path}: {error}")


def load_targets() -> pd.DataFrame:
    con = duckdb.connect()
    targets = con.execute(f"""
        SELECT d.sample_id, d.decision_time_unix, d.proposed_price_class,
               g.gamma_link_grade, g.clobTokenIds
        FROM read_parquet('{ROOT / 'data/applications/trustworthy_ai_challenge/decision_samples.parquet'}') d
        LEFT JOIN read_parquet('{CURATED / 'parquet/polygon_uma_gamma_links.parquet'}') g
          ON d.sample_id=g.oo_request_id
        ORDER BY d.sample_id
    """).fetchdf()
    con.close()
    if len(targets) != 810 or targets.sample_id.nunique() != 810:
        raise RuntimeError("expected exactly 810 benchmark targets")
    return targets


def raw_path(sample_id: str) -> Path:
    return RAW / f"{sample_id.removeprefix('0x')}.json.gz"


def fetch_target(row: dict[str, Any], refresh: bool, offline: bool) -> dict[str, Any]:
    target = raw_path(row["sample_id"])
    if target.exists() and not refresh:
        with gzip.open(target, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    if offline:
        raise RuntimeError(f"offline mode requires cached price evidence: {target}")
    retrieved = datetime.now(UTC).isoformat()
    if not row.get("clobTokenIds"):
        payload = {
            "sample_id": row["sample_id"], "decision_time_unix": int(row["decision_time_unix"]),
            "status": "unavailable_no_exact_gamma_token_link", "retrieved_at_utc": retrieved,
            "endpoint_base": BASE, "credentials_used": False,
        }
    else:
        gamma_tokens = [str(value) for value in json.loads(row["clobTokenIds"])]
        mapping = request_json(f"/markets-by-token/{gamma_tokens[0]}")
        primary = str(mapping["primary_token_id"])
        secondary = str(mapping["secondary_token_id"])
        if set(gamma_tokens) != {primary, secondary}:
            raise RuntimeError(f"CLOB/Gamma token set mismatch for {row['sample_id']}")
        end = int(row["decision_time_unix"])
        params = {
            "startTs": end - WINDOW_SECONDS, "endTs": end,
            "fidelity": FIDELITY_MINUTES,
        }
        histories = {}
        for label, token in [("primary_yes", primary), ("secondary_no", secondary)]:
            body = request_json("/prices-history", {**params, "market": token})
            history = sorted(
                [{"t": int(point["t"]), "p": float(point["p"])} for point in body.get("history", [])],
                key=lambda point: point["t"],
            )
            if any(point["t"] > end for point in history):
                raise RuntimeError(f"post-decision price returned for {row['sample_id']}")
            histories[label] = {"token_id": token, "history": history}
        payload = {
            "sample_id": row["sample_id"], "decision_time_unix": end,
            "status": "complete", "retrieved_at_utc": retrieved,
            "endpoint_base": BASE, "credentials_used": False,
            "request_parameters": params, "condition_id": mapping.get("condition_id"),
            "gamma_token_ids": gamma_tokens, **histories,
        }
    target.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(target, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
    return payload


def price_at_or_before(history: list[dict[str, Any]], timestamp: int) -> float | None:
    eligible = [point["p"] for point in history if point["t"] <= timestamp]
    return eligible[-1] if eligible else None


def history_features(history: list[dict[str, Any]], decision: int, prefix: str) -> dict[str, Any]:
    if not history:
        return {
            f"{prefix}_last_price": None, f"{prefix}_price_age_seconds": None,
            **{f"{prefix}_momentum_{hours}h": None for hours in [1, 6, 24]},
            f"{prefix}_volatility_24h": None, f"{prefix}_range_24h": None,
            **{f"{prefix}_points_{hours}h": 0 for hours in [1, 6, 24, 168]},
        }
    last = history[-1]
    output: dict[str, Any] = {
        f"{prefix}_last_price": float(last["p"]),
        f"{prefix}_price_age_seconds": decision - int(last["t"]),
    }
    for hours in [1, 6, 24]:
        prior = price_at_or_before(history, decision - hours * 3600)
        output[f"{prefix}_momentum_{hours}h"] = None if prior is None else float(last["p"] - prior)
    recent = [point["p"] for point in history if point["t"] >= decision - 86400]
    differences = np.diff(recent) if len(recent) >= 2 else np.array([])
    output[f"{prefix}_volatility_24h"] = float(np.std(differences)) if len(differences) else None
    output[f"{prefix}_range_24h"] = float(max(recent) - min(recent)) if recent else None
    for hours in [1, 6, 24, 168]:
        output[f"{prefix}_points_{hours}h"] = sum(
            point["t"] >= decision - hours * 3600 for point in history
        )
    return output


def build_outputs(targets: pd.DataFrame, payloads: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_map = targets.set_index("sample_id")
    rows = []
    provenance = []
    for payload in sorted(payloads, key=lambda value: value["sample_id"]):
        sample_id = payload["sample_id"]
        target = target_map.loc[sample_id]
        base = {
            "sample_id": sample_id, "decision_time_unix": int(payload["decision_time_unix"]),
            "coverage_status": payload["status"], "gamma_link_grade": target.gamma_link_grade,
        }
        if payload["status"] == "complete":
            decision = int(payload["decision_time_unix"])
            yes = payload["primary_yes"]["history"]
            no = payload["secondary_no"]["history"]
            base.update(history_features(yes, decision, "yes"))
            base.update(history_features(no, decision, "no"))
            yes_last = base["yes_last_price"]
            no_last = base["no_last_price"]
            base.update({
                "price_sum": None if yes_last is None or no_last is None else yes_last + no_last,
                "market_confidence": None if yes_last is None else abs(yes_last - 0.5) * 2,
                "proposal_aligned_market_probability": (
                    yes_last if target.proposed_price_class == "binary_one"
                    else no_last if target.proposed_price_class == "binary_zero" else None
                ),
                "total_points_24h": base["yes_points_24h"] + base["no_points_24h"],
                "total_points_168h": base["yes_points_168h"] + base["no_points_168h"],
            })
        rows.append(base)
        path = raw_path(sample_id)
        provenance.append({
            "sample_id": sample_id, "raw_snapshot": str(path), "raw_sha256": sha256(path),
            "retrieved_at_utc": payload["retrieved_at_utc"], "source": BASE,
            "decision_time_unix": int(payload["decision_time_unix"]),
            "request_end_ts": int(payload["decision_time_unix"]),
            "evidence_grade": "B" if payload["status"] == "complete" else "U",
            "interpretation": "Official historical price index truncated at decision time; not an independently decoded full on-chain fill ledger.",
        })
    frame = pd.DataFrame(rows)
    all_feature_columns = sorted(set().union(*(row.keys() for row in rows)))
    frame = frame.reindex(columns=all_feature_columns)
    return frame, pd.DataFrame(provenance)


def validate(frame: pd.DataFrame, provenance: pd.DataFrame, payloads: list[dict[str, Any]]) -> dict[str, Any]:
    complete = frame.coverage_status.eq("complete")
    future_points = 0
    mapping_mismatches = 0
    for payload in payloads:
        if payload["status"] != "complete":
            continue
        if set(payload["gamma_token_ids"]) != {
            payload["primary_yes"]["token_id"], payload["secondary_no"]["token_id"]
        }:
            mapping_mismatches += 1
        for side in ["primary_yes", "secondary_no"]:
            future_points += sum(
                point["t"] > payload["decision_time_unix"] for point in payload[side]["history"]
            )
    checks = {
        "all_810_targets_represented": len(frame) == 810 and frame.sample_id.nunique() == 810,
        "exact_token_mapping_rows": int(complete.sum()),
        "unavailable_rows": int((~complete).sum()),
        "token_mapping_mismatches": mapping_mismatches,
        "post_decision_points": future_points,
        "raw_snapshot_checksum_unique": provenance.raw_sha256.nunique() == len(provenance),
        "primary_price_bounds_failures": int((~frame.loc[complete, "yes_last_price"].dropna().between(0, 1)).sum()),
        "secondary_price_bounds_failures": int((~frame.loc[complete, "no_last_price"].dropna().between(0, 1)).sum()),
    }
    if (
        checks["all_810_targets_represented"] is not True
        or checks["token_mapping_mismatches"] != 0
        or checks["post_decision_points"] != 0
        or checks["primary_price_bounds_failures"] != 0
        or checks["secondary_price_bounds_failures"] != 0
    ):
        raise RuntimeError(f"decision-time price QC failed: {checks}")
    return checks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--workers", type=int, default=20)
    args = parser.parse_args()
    offline = os.environ.get("ORACLE_NATURE_OFFLINE", "0").lower() in {"1", "true", "yes"}
    targets = load_targets()
    records = targets.to_dict("records")
    payloads: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_target, row, args.refresh, offline): row["sample_id"] for row in records}
        for index, future in enumerate(as_completed(futures), start=1):
            payloads.append(future.result())
            if index % 100 == 0:
                print(f"fetched_or_loaded={index}/{len(records)}", flush=True)
    frame, provenance = build_outputs(targets, payloads)
    checks = validate(frame, provenance, payloads)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(OUT, index=False)
    provenance.to_parquet(PROVENANCE, index=False)
    manifest = {
        "dataset": "Polymarket decision-time historical price evidence",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source": BASE, "window_seconds": WINDOW_SECONDS,
        "fidelity_minutes": FIDELITY_MINUTES, "credentials_used": False,
        "raw_directory": str(RAW), "output": str(OUT), "provenance": str(PROVENANCE),
        "rows": len(frame), "checks": checks, "all_required_assertions_pass": True,
        "official_contract_context": {
            "chain_id": 137,
            "v1_ctf_exchange": "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
            "v1_neg_risk_exchange": "0xc5d563a36ae78145c45a50134d48a1215220f80a",
            "order_filled_topic": "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6",
        },
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")
    REPORT.write_text(
        "# Polymarket decision-time historical price QC\n\n"
        f"- Fixed benchmark targets: {len(frame)}.\n"
        f"- Exact primary/secondary token mappings: {checks['exact_token_mapping_rows']}.\n"
        f"- Unavailable without exact token link: {checks['unavailable_rows']}.\n"
        f"- Post-decision price points: {checks['post_decision_points']}.\n"
        f"- Token mapping mismatches: {checks['token_mapping_mismatches']}.\n"
        f"- Window: {WINDOW_SECONDS // 86400} days ending exactly at each OOV2 proposal timestamp; fidelity {FIDELITY_MINUTES} minutes.\n\n"
        "The official CLOB price index is frozen with raw response checksums and used as Grade-B indexed evidence. "
        "It is not relabeled as an independently decoded full Polygon OrderFilled ledger.\n",
        encoding="utf-8",
    )
    print(json.dumps({"rows": len(frame), "checks": checks, "output": str(OUT)}, indent=2))


if __name__ == "__main__":
    main()
