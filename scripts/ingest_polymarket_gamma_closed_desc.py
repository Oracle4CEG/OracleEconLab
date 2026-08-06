"""Fetch closed Gamma markets in descending ID order until overlapping ascending cache."""
from __future__ import annotations

import argparse
import json
import time
import urllib.parse
from pathlib import Path

import requests

from ingest_polymarket_gamma import BASE, load_page, save_page


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--stop-id", type=int, required=True); args = parser.parse_args()
    target = ROOT / "data/raw/polymarket/gamma_markets/closed_desc"; target.mkdir(parents=True, exist_ok=True)
    session = requests.Session(); session.headers.update({"User-Agent": "oracle-accountability-atlas/0.1"})
    cursor = None; page_number = 0; markets = 0; seen: set[str] = set()
    while True:
        path = target / f"page_{page_number:06d}.json.gz"; page = load_page(path)
        if page is None:
            params = {"limit": "100", "order": "id", "ascending": "false", "closed": "true"}
            if cursor: params["after_cursor"] = cursor
            url = BASE + "?" + urllib.parse.urlencode(params); error = None
            for attempt in range(7):
                try:
                    response = session.get(url, timeout=120); response.raise_for_status(); page = response.json(); save_page(path, page); break
                except Exception as exc:
                    error = exc; time.sleep(min(2**attempt, 30))
            else: raise RuntimeError(f"Gamma descending page failed {page_number}: {error}")
        rows = page.get("markets") or []; markets += len(rows); page_number += 1
        minimum = min((int(row["id"]) for row in rows), default=0)
        if page_number % 50 == 0 or minimum <= args.stop_id:
            print(f"Gamma closed-desc: pages={page_number}, markets={markets}, min_id={minimum}", flush=True)
        if not rows or minimum <= args.stop_id or not page.get("next_cursor"): break
        cursor = page["next_cursor"]
        if cursor in seen: raise RuntimeError("Gamma descending cursor repeated")
        seen.add(cursor)
    print(target)


if __name__ == "__main__": main()
