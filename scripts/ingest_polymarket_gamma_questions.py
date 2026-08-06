"""Exhaustively fetch Gamma metadata for every on-chain UMA question ID."""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from ingest_polymarket_gamma import BASE, load_page, save_page


ROOT = Path(__file__).resolve().parents[1]
BATCH_SIZE = 30


def main() -> None:
    question_ids = sorted({
        str(row.get("question_id", "")).lower()
        for row in (json.loads(line) for line in (ROOT / "data/curated/polygon_uma_request_rounds.jsonl").open())
        if row.get("question_id")
    })
    batches = [question_ids[index:index + BATCH_SIZE] for index in range(0, len(question_ids), BATCH_SIZE)]
    target = ROOT / "data/raw/polymarket/gamma_markets/targeted_questions"; target.mkdir(parents=True, exist_ok=True)

    def fetch(index: int, ids: list[str], closed: bool):
        label = "closed" if closed else "open"; path = target / f"batch_{index:06d}_{label}.json.gz"; cached = load_page(path)
        if cached is not None: return path, cached
        params: list[tuple[str, str]] = [("limit", "100"), ("closed", str(closed).lower())] + [("question_ids", value) for value in ids]
        markets = []; cursor = None; error = None
        for page_number in range(20):
            page_params = params + ([("after_cursor", cursor)] if cursor else [])
            for attempt in range(7):
                try:
                    response = requests.get(BASE, params=page_params, headers={"User-Agent": "oracle-accountability-atlas/0.1"}, timeout=120)
                    response.raise_for_status(); page = response.json(); break
                except Exception as exc:
                    error = exc; time.sleep(min(2**attempt, 30))
            else: raise RuntimeError(f"Gamma question batch {index}/{label} failed: {error}")
            markets.extend(page.get("markets") or []); cursor = page.get("next_cursor")
            if not cursor: break
        value = {"question_ids": ids, "closed": closed, "markets": markets}; save_page(path, value); return path, value

    results = []
    with ThreadPoolExecutor(max_workers=24) as executor:
        futures = [executor.submit(fetch, index, batch, closed) for index, batch in enumerate(batches) for closed in (True, False)]
        for completed, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if completed % 200 == 0 or completed == len(futures): print(f"Gamma targeted: {completed}/{len(futures)} batches", flush=True)
    markets = {str(row["id"]): row for _, result in results for row in result["markets"]}
    found_questions = {str(row.get("questionID", "")).lower() for row in markets.values() if row.get("questionID")}
    manifest = {
        "dataset": "Polymarket Gamma exact on-chain question lookup", "question_ids": len(question_ids),
        "request_batches": len(results), "markets": len(markets), "question_ids_found": len(found_questions),
        "question_ids_not_found": len(set(question_ids) - found_questions), "batch_size": BATCH_SIZE,
        "raw_directory": str(target), "source": BASE,
    }
    output = ROOT / "data/manifests/polymarket_gamma_targeted.json"; output.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"); print(output)


if __name__ == "__main__": main()
