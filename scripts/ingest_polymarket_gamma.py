"""Fetch Polymarket Gamma markets with resumable keyset pagination and link UMA rounds."""
from __future__ import annotations

import gzip
import hashlib
import json
import time
import urllib.parse
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
BASE = "https://gamma-api.polymarket.com/markets/keyset"
CUTOFF = datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
FIELDS = (
    "id", "question", "description", "conditionId", "questionID", "resolvedBy",
    "resolutionSource", "endDate", "closedTime", "category", "volumeNum",
    "liquidityNum", "umaBond", "umaReward", "umaResolutionStatus",
    "umaResolutionStatuses", "negRisk", "clobTokenIds", "createdAt", "updatedAt",
    "closed", "active", "archived", "slug",
)


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def save_page(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=9) as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
    temporary.replace(path)
    path.with_suffix(path.suffix + ".sha256").write_text(
        hashlib.sha256(path.read_bytes()).hexdigest() + "\n", encoding="utf-8"
    )


def load_page(path: Path) -> dict | None:
    digest_path = path.with_suffix(path.suffix + ".sha256")
    if not path.is_file() or not digest_path.is_file():
        return None
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest_path.read_text(encoding="utf-8").strip():
        raise RuntimeError(f"checksum mismatch: {path}")
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def fetch_all(closed: bool, raw_dir: Path) -> tuple[list[dict], int]:
    label = "closed" if closed else "open"
    target = raw_dir / label; target.mkdir(parents=True, exist_ok=True)
    cursor = None; page_number = 0; markets: list[dict] = []; seen_cursors: set[str] = set()
    session = requests.Session(); session.headers.update({"User-Agent": "oracle-accountability-atlas/0.1"})
    while True:
        page_path = target / f"page_{page_number:06d}.json.gz"
        page = load_page(page_path)
        if page is None:
            params = {"limit": "100", "ascending": "true", "closed": str(closed).lower()}
            if cursor:
                params["after_cursor"] = cursor
            url = BASE + "?" + urllib.parse.urlencode(params)
            error = None
            for attempt in range(7):
                try:
                    response = session.get(url, timeout=120); response.raise_for_status(); page = response.json()
                    save_page(page_path, page); break
                except Exception as exc:  # transient public-API transport/5xx errors
                    error = exc; time.sleep(min(2**attempt, 30))
            else:
                raise RuntimeError(f"Gamma page failed ({label}, {page_number}): {error}")
        rows = page.get("markets") or []
        markets.extend(rows)
        next_cursor = page.get("next_cursor")
        page_number += 1
        if page_number % 50 == 0 or not next_cursor:
            print(f"Gamma {label}: pages={page_number}, markets={len(markets)}", flush=True)
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            raise RuntimeError(f"Gamma repeated cursor in {label} stream")
        seen_cursors.add(next_cursor); cursor = next_cursor
    return markets, page_number


def raw_amount(value, decimals: int = 6) -> str | None:
    if value in {None, ""}:
        return None
    try:
        amount = Decimal(str(value)) * (Decimal(10) ** decimals)
        if amount != amount.to_integral_value():
            return None
        return str(int(amount))
    except (InvalidOperation, ValueError):
        return None


def main() -> None:
    raw_dir = ROOT / "data/raw/polymarket/gamma_markets"
    closed, closed_pages = fetch_all(True, raw_dir)
    opened, open_pages = fetch_all(False, raw_dir)
    by_id = {str(row["id"]): row for row in closed + opened}
    snapshot = datetime.now(UTC).isoformat()
    curated_dir = ROOT / "data/curated"
    markets_path = curated_dir / "polymarket_gamma_markets.jsonl"
    eligible = 0; question_lookup: dict[str, list[dict]] = {}
    resolved_by: Counter[str] = Counter()
    with markets_path.open("w", encoding="utf-8") as handle:
        for market in sorted(by_id.values(), key=lambda row: int(row["id"])):
            row = {field: market.get(field) for field in FIELDS}
            created = parse_time(row.get("createdAt")); row["created_before_cutoff"] = created is not None and created <= CUTOFF
            row["metadata_snapshot_time_utc"] = snapshot
            eligible += bool(row["created_before_cutoff"])
            if row.get("resolvedBy"):
                resolved_by[str(row["resolvedBy"]).lower()] += 1
            if row["created_before_cutoff"] and row.get("questionID"):
                question_lookup.setdefault(str(row["questionID"]).lower(), []).append(row)
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")

    links_path = curated_dir / "polygon_uma_gamma_links.jsonl"
    grades: Counter[str] = Counter(); comparisons: Counter[str] = Counter()
    with (curated_dir / "polygon_uma_request_rounds.jsonl").open(encoding="utf-8") as rounds, links_path.open("w", encoding="utf-8") as handle:
        for line in rounds:
            request_round = json.loads(line); matches = question_lookup.get(str(request_round.get("question_id", "")).lower(), [])
            grade = "A" if len(matches) == 1 else ("ambiguous" if len(matches) > 1 else "U")
            result = {"oo_request_id": request_round["oo_request_id"], "question_id": request_round.get("question_id"), "gamma_link_grade": grade}
            grades[grade] += 1
            if len(matches) == 1:
                market = matches[0]
                result.update({field: market.get(field) for field in FIELDS})
                gamma_reward = raw_amount(market.get("umaReward")); gamma_bond = raw_amount(market.get("umaBond"))
                result["gamma_uma_reward_raw_6dec"] = gamma_reward; result["gamma_uma_bond_raw_6dec"] = gamma_bond
                result["reward_matches_onchain"] = gamma_reward is not None and gamma_reward == request_round.get("question_reward_raw")
                result["bond_matches_onchain"] = gamma_bond is not None and gamma_bond == request_round.get("proposal_bond_raw")
                comparisons["reward_match" if result["reward_matches_onchain"] else "reward_missing_or_mismatch"] += 1
                comparisons["bond_match" if result["bond_matches_onchain"] else "bond_missing_or_mismatch"] += 1
            handle.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")

    manifest = {
        "dataset": "Polymarket Gamma market metadata",
        "source": BASE, "pagination": "keyset", "snapshot_time_utc": snapshot,
        "cutoff_utc": CUTOFF.isoformat(), "raw_pages": closed_pages + open_pages,
        "closed_pages": closed_pages, "open_pages": open_pages, "markets": len(by_id),
        "markets_created_before_cutoff": eligible, "resolved_by_counts": dict(resolved_by),
        "uma_round_gamma_link_grades": dict(grades), "metadata_onchain_comparisons": dict(comparisons),
        "outputs": {"markets": str(markets_path), "uma_links": str(links_path)},
        "interpretation_guard": "Gamma is mutable snapshot metadata; on-chain values remain authoritative.",
    }
    manifest_path = ROOT / "data/manifests/polymarket_gamma.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
