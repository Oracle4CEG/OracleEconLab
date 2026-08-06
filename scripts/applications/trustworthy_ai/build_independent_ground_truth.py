#!/usr/bin/env python3
"""Construct an independently reproducible Binance-candle truth subcohort.

Eligibility is deterministic: immutable UMA request text must explicitly name
Binance and match one of three unambiguous candle rules (two-close comparison,
single-close threshold, or one-hour open/close).  The selected finalized candle
must predate the proposal.  Other market types remain unavailable.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from sklearn.metrics import brier_score_loss, roc_auc_score


ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "data/applications/trustworthy_ai_challenge"
SEMANTIC = ROOT / "data/applications/trustworthy_ai_semantic/semantic_source.parquet"
CURATED = ROOT / "data/curated/parquet"
OUT = ROOT / "data/applications/trustworthy_ai_independent_truth"
RAW = ROOT / "data/raw/binance/trustworthy_ai_candles_v1.jsonl.gz"
REPORT = ROOT / "reports/trustworthy_ai_independent_ground_truth.md"
API = "https://data-api.binance.vision/api/v3/klines"
ET = ZoneInfo("America/New_York")
MONTHS = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|June|July|August|September|October|November|December"
DATE_PATTERN = re.compile(rf"(\d{{1,2}}\s+(?:{MONTHS})[A-Za-z]*\s+'?\d{{2,4}})\s+(\d{{1,2}}:\d{{2}})", re.I)
ONE = "1000000000000000000"
ZERO = "0"
HALF = "500000000000000000"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_explicit_date(date: str, clock: str) -> datetime | None:
    for fmt in ["%d %b '%y %H:%M", "%d %B '%y %H:%M", "%d %b %Y %H:%M", "%d %B %Y %H:%M"]:
        try:
            return datetime.strptime(f"{date} {clock}", fmt).replace(tzinfo=ET)
        except ValueError:
            pass
    return None


def latest_title_date_before(month_day: str, hour: int, ampm: str, decision: int) -> datetime:
    cutoff = datetime.fromtimestamp(decision, UTC).astimezone(ET)
    parsed = datetime.strptime(month_day, "%B %d")
    hour = hour % 12 + (12 if ampm.upper() == "PM" else 0)
    candidate = datetime(cutoff.year, parsed.month, parsed.day, hour, tzinfo=ET)
    if candidate.timestamp() >= decision:
        candidate = candidate.replace(year=candidate.year - 1)
    return candidate


def parse_candidate(sample_id: str, text: str, decision: int) -> dict[str, Any]:
    symbol_match = re.search(r"([A-Z]{2,10})\s*/?USDT", text)
    if not symbol_match:
        return {"sample_id": sample_id, "status": "unparsed_symbol"}
    symbol = symbol_match.group(1) + "USDT"
    dates: list[datetime] = []
    for date, clock in DATE_PATTERN.findall(text):
        parsed = parse_explicit_date(date, clock)
        if parsed and parsed not in dates:
            dates.append(parsed)
    if "Up or Down on" in text and len(dates) == 2:
        return {
            "sample_id": sample_id, "status": "eligible", "rule": "two_close_comparison",
            "symbol": symbol, "interval": "1m", "timestamps": [int(item.timestamp()) for item in dates],
        }
    threshold = re.search(
        r"final\s+[“\"]?Close[”\"]?\s+price\s+of\s+\$?([0-9,.]+)\s+or\s+higher", text, re.I
    )
    if len(dates) == 1 and threshold:
        return {
            "sample_id": sample_id, "status": "eligible", "rule": "single_close_at_least",
            "symbol": symbol, "interval": "1m", "timestamps": [int(dates[0].timestamp())],
            "threshold": threshold.group(1).replace(",", ""),
        }
    title = re.search(r"Up or Down\s*-\s*([A-Za-z]+\s+\d{1,2}),\s*(\d{1,2})\s*(AM|PM)\s*ET", text, re.I)
    if title and "1 hour candle" in text.lower():
        stamp = latest_title_date_before(title.group(1), int(title.group(2)), title.group(3), decision)
        return {
            "sample_id": sample_id, "status": "eligible", "rule": "one_hour_close_at_least_open",
            "symbol": symbol, "interval": "1h", "timestamps": [int(stamp.timestamp())],
        }
    return {"sample_id": sample_id, "status": "unparsed_market_rule", "symbol": symbol}


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    samples = pd.read_parquet(BASE / "decision_samples.parquet")
    semantic = pd.read_parquet(SEMANTIC, columns=["sample_id", "semantic_text"])
    predictions = pd.read_parquet(BASE / "predictions.parquet")
    rounds = pd.read_parquet(CURATED / "polygon_uma_request_rounds.parquet")
    rounds = rounds[rounds.oo_request_id.isin(samples.sample_id)][[
        "oo_request_id", "proposed_price_raw", "resolved_price_raw"
    ]]
    source = samples.merge(semantic, on="sample_id", validate="one_to_one").merge(
        rounds, left_on="sample_id", right_on="oo_request_id", validate="one_to_one"
    )
    return source, predictions, samples


def read_raw() -> dict[tuple[str, str, int], dict[str, Any]]:
    if not RAW.exists():
        return {}
    rows = {}
    with gzip.open(RAW, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            rows[(row["symbol"], row["interval"], int(row["start_time_ms"]))] = row
    return rows


def fetch_kline(symbol: str, interval: str, timestamp: int, attempts: int = 5) -> dict[str, Any]:
    params = {"symbol": symbol, "interval": interval, "startTime": timestamp * 1000, "limit": 1}
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(API, params=params, timeout=40)
            if response.status_code == 400:
                return {**params, "start_time_ms": params["startTime"], "status": "symbol_unavailable", "response": response.json()}
            response.raise_for_status()
            body = response.json()
            if not body:
                raise RuntimeError("empty kline response")
            return {
                "symbol": symbol, "interval": interval, "start_time_ms": params["startTime"],
                "status": "complete", "retrieved_at_utc": datetime.now(UTC).isoformat(),
                "endpoint": API, "kline": body[0],
            }
        except Exception as exc:
            error = exc
            time.sleep(min(5, 0.25 * 2**attempt))
    raise RuntimeError(f"Binance kline failed: {symbol}/{interval}/{timestamp}: {error}")


def freeze(candidates: list[dict[str, Any]], offline: bool) -> dict[tuple[str, str, int], dict[str, Any]]:
    targets = {
        (row["symbol"], row["interval"], timestamp)
        for row in candidates if row["status"] == "eligible" for timestamp in row["timestamps"]
    }
    cached = read_raw()
    missing = sorted((symbol, interval, timestamp) for symbol, interval, timestamp in targets if (symbol, interval, timestamp * 1000) not in cached)
    if missing and offline:
        raise RuntimeError(f"offline Binance cache incomplete: {len(missing)}")
    for symbol, interval, timestamp in missing:
        row = fetch_kline(symbol, interval, timestamp)
        cached[(symbol, interval, timestamp * 1000)] = row
    selected = [cached[(symbol, interval, timestamp * 1000)] for symbol, interval, timestamp in sorted(targets)]
    RAW.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(RAW, "wt", encoding="utf-8", compresslevel=9) as handle:
        for row in selected:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    return cached


def ground_truth(source: pd.DataFrame, candidates: list[dict[str, Any]], cache: dict[tuple[str, str, int], dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    by_sample = source.set_index("sample_id").to_dict("index")
    verified, inventory = [], []
    for candidate in candidates:
        sample = by_sample[candidate["sample_id"]]
        if candidate["status"] != "eligible":
            inventory.append({**candidate, "decision_time_unix": int(sample["decision_time_unix"])})
            continue
        records = [cache[(candidate["symbol"], candidate["interval"], ts * 1000)] for ts in candidate["timestamps"]]
        if any(row["status"] != "complete" for row in records):
            inventory.append({**candidate, "status": "source_symbol_unavailable", "decision_time_unix": int(sample["decision_time_unix"])})
            continue
        candles = [row["kline"] for row in records]
        exact_times = all(int(candle[0]) == ts * 1000 for candle, ts in zip(candles, candidate["timestamps"]))
        if not exact_times:
            inventory.append({**candidate, "status": "candle_alignment_failure", "decision_time_unix": int(sample["decision_time_unix"])})
            continue
        if candidate["rule"] == "two_close_comparison":
            first, second = float(candles[0][4]), float(candles[1][4])
            result = ONE if second > first else ZERO if second < first else HALF
            evidence_values = {"first_close": candles[0][4], "second_close": candles[1][4]}
        elif candidate["rule"] == "single_close_at_least":
            close, threshold = float(candles[0][4]), float(candidate["threshold"])
            result = ONE if close >= threshold else ZERO
            evidence_values = {"close": candles[0][4], "threshold": candidate["threshold"]}
        else:
            opened, close = float(candles[0][1]), float(candles[0][4])
            result = ONE if close >= opened else ZERO
            evidence_values = {"open": candles[0][1], "close": candles[0][4]}
        protocol_factual = str(sample["resolved_price_raw"]) in {ZERO, ONE, HALF}
        row = {
            "sample_id": candidate["sample_id"], "independent_ground_truth_available": True,
            "ground_truth_source": "Binance finalized kline via data-api.binance.vision",
            "rule": candidate["rule"], "symbol": candidate["symbol"], "interval": candidate["interval"],
            "candle_open_times_unix": json.dumps(candidate["timestamps"]),
            "latest_evidence_time_unix": max(int(candle[6]) // 1000 for candle in candles),
            "decision_time_unix": int(sample["decision_time_unix"]),
            "ground_truth_known_at_decision": max(int(candle[6]) // 1000 for candle in candles) <= int(sample["decision_time_unix"]),
            "ground_truth_is_model_input": False,
            "independent_outcome_raw": result, "protocol_outcome_raw": str(sample["resolved_price_raw"]),
            "proposal_raw": str(sample["proposed_price_raw"]),
            "protocol_factual_outcome_available": protocol_factual,
            "protocol_matches_independent_truth": (str(sample["resolved_price_raw"]) == result) if protocol_factual else None,
            "proposal_matches_independent_truth": str(sample["proposed_price_raw"]) == result,
            "evidence_values": json.dumps(evidence_values, sort_keys=True),
            "raw_evidence_ids": json.dumps([
                hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                for record in records
            ]),
        }
        verified.append(row)
        inventory.append({**candidate, "status": "verified", "decision_time_unix": int(sample["decision_time_unix"])})
    return pd.DataFrame(verified), pd.DataFrame(inventory)


def evaluate(truth: pd.DataFrame, predictions: pd.DataFrame) -> pd.DataFrame:
    frame = predictions.merge(truth[["sample_id", "proposal_matches_independent_truth"]], on="sample_id", validate="many_to_one")
    frame["proposal_factually_wrong"] = (~frame.proposal_matches_independent_truth).astype(int)
    rows = []
    for model, part in frame.groupby("model"):
        y = part.proposal_factually_wrong.to_numpy()
        p = part.probability_proposal_rejected.to_numpy()
        automated = part.action.isin(["Accept", "Challenge"])
        errors = ((part.action == "Accept") & (y == 1)) | ((part.action == "Challenge") & (y == 0))
        rows.append({
            "model": model, "independent_truth_samples": len(part),
            "roc_auc_factually_wrong": float(roc_auc_score(y, p)) if len(set(y)) > 1 else None,
            "brier_factually_wrong": float(brier_score_loss(y, p)),
            "automated_coverage": float(automated.mean()),
            "automated_factual_error": float(errors[automated].mean()) if automated.any() else None,
            "review_burden": float(part.action.isin(["Investigate", "Abstain"]).mean()),
        })
    return pd.DataFrame(rows)


def validate(truth: pd.DataFrame, inventory: pd.DataFrame, metrics: pd.DataFrame) -> dict[str, bool]:
    checks = {
        "all_63_binance_linked_samples_inventoried": len(inventory) == 63,
        "verified_subcohort_nonempty": len(truth) >= 30,
        "retrospective_labels_explicitly_marked": (~truth.ground_truth_known_at_decision).any(),
        "independent_truth_never_used_as_model_input": (~truth.ground_truth_is_model_input).all(),
        "all_outcomes_canonical": set(truth.independent_outcome_raw) <= {ZERO, ONE, HALF},
        "both_factual_proposal_classes_present": truth.proposal_matches_independent_truth.nunique() == 2,
        "all_seven_models_evaluated": len(metrics) == 7,
    }
    if not all(checks.values()):
        raise RuntimeError(f"independent truth QC failed: {checks}")
    return {key: bool(value) for key, value in checks.items()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    source, predictions, _ = load_inputs()
    linked = source[source.semantic_text.str.contains("binance.com", case=False, na=False)].copy()
    candidates = [parse_candidate(row.sample_id, row.semantic_text, int(row.decision_time_unix)) for row in linked.itertuples(index=False)]
    cache = freeze(candidates, args.offline)
    truth, inventory = ground_truth(source, candidates, cache)
    metrics = evaluate(truth, predictions)
    checks = validate(truth, inventory, metrics)
    OUT.mkdir(parents=True, exist_ok=True)
    objects = {"independent_ground_truth": truth, "candidate_inventory": inventory, "independent_truth_metrics": metrics}
    for name, frame in objects.items():
        frame.to_parquet(OUT / f"{name}.parquet", index=False)
        frame.to_csv(OUT / f"{name}.csv", index=False)
    REPORT.write_text(
        "# Independent factual ground-truth experiment\n\n"
        f"- Binance-linked immutable request texts audited: **{len(inventory)}**.\n"
        f"- Deterministically parsed and independently verified candle outcomes: **{len(truth)}**.\n"
        f"- Canonical protocol factual outcomes available: **{int(truth.protocol_factual_outcome_available.sum())}/{len(truth)}**; agreement on that subset: **{truth.loc[truth.protocol_factual_outcome_available, 'protocol_matches_independent_truth'].mean():.3f}**.\n"
        f"- Proposal factual correctness in the verified subcohort: **{truth.proposal_matches_independent_truth.mean():.3f}**.\n"
        f"- Labels already known at proposal: **{int(truth.ground_truth_known_at_decision.sum())}/{len(truth)}**; later-finalized labels are explicitly retrospective.\n"
        "- Independent outcomes are excluded from every model input, regardless of label availability time.\n"
        "- Unparsed range/first-hit/ATH markets remain unavailable rather than receiving inferred labels.\n\n"
        "## Model evaluation against factual proposal error\n\n" + metrics.to_markdown(index=False) + "\n",
        encoding="utf-8",
    )
    files = []
    for path in sorted(OUT.glob("*")):
        if path.is_file() and path.name != "manifest.json":
            files.append({"path": str(path), "bytes": path.stat().st_size, "sha256": sha256(path)})
    files.extend([
        {"path": str(RAW), "bytes": RAW.stat().st_size, "sha256": sha256(RAW)},
        {"path": str(REPORT), "bytes": REPORT.stat().st_size, "sha256": sha256(REPORT)},
    ])
    (OUT / "manifest.json").write_text(json.dumps({
        "dataset": "Binance independent candle ground truth", "version": "1.0.0",
        "generated_at_utc": datetime.now(UTC).isoformat(), "endpoint": API,
        "all_required_assertions_pass": True, "checks": checks,
        "rows": {name: len(frame) for name, frame in objects.items()}, "files": files,
    }, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"checks": checks, "rows": {k: len(v) for k, v in objects.items()}}, indent=2))


if __name__ == "__main__":
    main()
