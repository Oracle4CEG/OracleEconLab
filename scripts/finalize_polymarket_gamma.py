"""Merge checksum-verified ascending/descending Gamma caches and link UMA rounds."""
from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from ingest_polymarket_gamma import BASE, CUTOFF, FIELDS, load_page, parse_time, raw_amount


ROOT = Path(__file__).resolve().parents[1]


def stream(folder: Path, pattern: str = "page_*.json.gz") -> tuple[list[dict], int]:
    pages = sorted(folder.glob(pattern)); rows = []
    for page in pages:
        rows.extend((load_page(page) or {}).get("markets") or [])
    return rows, len(pages)


def main() -> None:
    raw_dir = ROOT / "data/raw/polymarket/gamma_markets"
    closed_a, pages_a = stream(raw_dir / "closed"); closed_b, pages_b = stream(raw_dir / "closed_desc"); opened, open_pages = stream(raw_dir / "open")
    targeted, targeted_batches = stream(raw_dir / "targeted_questions", "batch_*.json.gz")
    by_id = {str(row["id"]): row for row in closed_a + closed_b + opened + targeted}; snapshot = datetime.now(UTC).isoformat()
    curated = ROOT / "data/curated"; markets_path = curated / "polymarket_gamma_markets.jsonl"
    eligible = 0; question_lookup: dict[str, list[dict]] = {}; resolved_by: Counter[str] = Counter()
    with markets_path.open("w", encoding="utf-8") as handle:
        for market in sorted(by_id.values(), key=lambda row: int(row["id"])):
            row = {field: market.get(field) for field in FIELDS}; created = parse_time(row.get("createdAt"))
            row["created_before_cutoff"] = created is not None and created <= CUTOFF; row["metadata_snapshot_time_utc"] = snapshot
            eligible += bool(row["created_before_cutoff"])
            if row.get("resolvedBy"): resolved_by[str(row["resolvedBy"]).lower()] += 1
            if row["created_before_cutoff"] and row.get("questionID"): question_lookup.setdefault(str(row["questionID"]).lower(), []).append(row)
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    links_path = curated / "polygon_uma_gamma_links.jsonl"; grades: Counter[str] = Counter(); grades_by_sample: Counter[str] = Counter(); comparisons: Counter[str] = Counter()
    with (curated / "polygon_uma_request_rounds.jsonl").open(encoding="utf-8") as rounds, links_path.open("w", encoding="utf-8") as handle:
        for line in rounds:
            request = json.loads(line); matches = question_lookup.get(str(request.get("question_id", "")).lower(), [])
            grade = "A" if len(matches) == 1 else "ambiguous" if len(matches) > 1 else "U"
            result = {"oo_request_id": request["oo_request_id"], "question_id": request.get("question_id"), "sample_tier": request.get("sample_tier"), "gamma_link_grade": grade}; grades[grade] += 1; grades_by_sample[f"{request.get('sample_tier')}:{grade}"] += 1
            if len(matches) == 1:
                market = matches[0]; result.update({field: market.get(field) for field in FIELDS})
                reward = raw_amount(market.get("umaReward")); bond = raw_amount(market.get("umaBond"))
                result["gamma_uma_reward_raw_6dec"] = reward; result["gamma_uma_bond_raw_6dec"] = bond
                result["reward_matches_onchain"] = reward is not None and reward == request.get("question_reward_raw")
                result["bond_matches_onchain"] = bond is not None and bond == request.get("proposal_bond_raw")
                comparisons["reward_missing" if reward is None else "reward_match" if result["reward_matches_onchain"] else "reward_mismatch"] += 1
                comparisons["bond_missing" if bond is None else "bond_match" if result["bond_matches_onchain"] else "bond_mismatch"] += 1
            handle.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    manifest = {
        "dataset": "Polymarket Gamma market metadata", "source": BASE, "pagination": "bidirectional keyset",
        "snapshot_time_utc": snapshot, "cutoff_utc": CUTOFF.isoformat(), "raw_pages_or_batches": pages_a + pages_b + open_pages + targeted_batches,
        "closed_ascending_pages": pages_a, "closed_descending_pages": pages_b, "open_pages": open_pages,
        "targeted_question_batches": targeted_batches,
        "markets": len(by_id), "markets_created_before_cutoff": eligible, "resolved_by_counts": dict(resolved_by),
        "uma_round_gamma_link_grades": dict(grades), "uma_round_gamma_grades_by_sample": dict(grades_by_sample),
        "metadata_onchain_comparisons": dict(comparisons),
        "outputs": {"markets": str(markets_path), "uma_links": str(links_path)},
        "scope_note": "Broad bidirectional discovery snapshot plus exhaustive lookup of every included on-chain UMA question; not an indiscriminate full Gamma warehouse.",
        "interpretation_guard": "Gamma is mutable snapshot metadata; on-chain values remain authoritative.",
    }
    output = ROOT / "data/manifests/polymarket_gamma.json"; output.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"); print(output)


if __name__ == "__main__": main()
