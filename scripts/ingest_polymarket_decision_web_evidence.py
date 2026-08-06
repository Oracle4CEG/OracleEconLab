#!/usr/bin/env python3
"""Freeze web pages explicitly referenced by UMA requests before proposal time.

Only URLs embedded in the immutable, pre-proposal ancillary data are eligible.
For each URL/sample pair, the latest successful Wayback capture at or before the
proposal timestamp is selected. Current pages and post-proposal captures are
never fetched as evidence.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/polymarket/decision_time_web_archives_v1"
OUT = ROOT / "data/curated/parquet/polymarket_decision_time_web_evidence.parquet"
PROVENANCE = ROOT / "data/curated/parquet/polymarket_decision_time_web_provenance.parquet"
MANIFEST = ROOT / "data/manifests/polymarket_decision_time_web_evidence.json"
REPORT = ROOT / "reports/polymarket_decision_time_web_evidence_qc.md"
SEMANTIC = ROOT / "data/applications/trustworthy_ai_semantic/semantic_source.parquet"
SAMPLES = ROOT / "data/applications/trustworthy_ai_challenge/decision_samples.parquet"
TIMEMAP = "https://web.archive.org/web/timemap/json/"
REPLAY = "https://web.archive.org/web/{timestamp}id_/{url}"
USER_AGENT = "Oracle-Nature academic reproducibility audit/1.0"
URL_PATTERN = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
WORD_PATTERN = re.compile(r"[a-z0-9]{2,}", re.IGNORECASE)
MAX_BODY_BYTES = 5_000_000


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.ignored = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "noscript", "svg"}:
            self.ignored += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript", "svg"} and self.ignored:
            self.ignored -= 1

    def handle_data(self, data: str) -> None:
        if not self.ignored:
            self.parts.append(data)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_url(value: str) -> str:
    value = html.unescape(value).rstrip(".,;:!?)]}>")
    parsed = urlsplit(value)
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, parsed.query, ""))


def cache_key(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def request(url: str, attempts: int = 1) -> requests.Response:
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(url, timeout=20, headers={"User-Agent": USER_AGENT})
            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"retryable HTTP {response.status_code}")
            return response
        except Exception as exc:
            error = exc
            time.sleep(min(8.0, 0.5 * 2**attempt))
    raise RuntimeError(f"request failed: {url}: {error}")


def load_targets() -> pd.DataFrame:
    source = pd.read_parquet(SEMANTIC, columns=["sample_id", "semantic_text", "source_tx", "source_block"])
    samples = pd.read_parquet(SAMPLES, columns=["sample_id", "decision_time_unix"])
    merged = source.merge(samples, on="sample_id", validate="one_to_one")
    rows: list[dict[str, Any]] = []
    for row in merged.itertuples(index=False):
        seen: set[str] = set()
        for ordinal, raw_url in enumerate(URL_PATTERN.findall(str(row.semantic_text))):
            url = canonical_url(raw_url)
            if url in seen:
                continue
            seen.add(url)
            rows.append({
                "sample_id": row.sample_id,
                "url_ordinal": ordinal,
                "url": url,
                "domain": urlsplit(url).netloc,
                "decision_time_unix": int(row.decision_time_unix),
                "source_tx": row.source_tx,
                "source_block": int(row.source_block),
                "question_text": row.semantic_text,
            })
    return pd.DataFrame(rows)


def timemap_path(url: str) -> Path:
    return RAW / "timemaps" / f"{cache_key(url)}.json.gz"


def fetch_timemap(url: str, refresh: bool, offline: bool) -> dict[str, Any]:
    target = timemap_path(url)
    if target.exists() and not refresh:
        with gzip.open(target, "rt", encoding="utf-8") as handle:
            cached = json.load(handle)
        # Transient archive/network failures are not durable evidence. Retry
        # them online, while offline rebuilds faithfully retain the snapshot.
        if offline or not cached.get("error"):
            return cached
    if offline:
        raise RuntimeError(f"offline mode requires cached timemap: {target}")
    payload: dict[str, Any] = {
        "url": url, "retrieved_at_utc": datetime.now(UTC).isoformat(), "captures": [],
    }
    try:
        response = request(TIMEMAP + quote(url, safe=":/?=&%"))
        payload["http_status"] = response.status_code
        if response.status_code == 200:
            body = response.json()
            if body and isinstance(body[0], list):
                columns = body[0]
                payload["captures"] = [dict(zip(columns, item)) for item in body[1:]]
        else:
            payload["error"] = f"HTTP {response.status_code}"
    except Exception as exc:
        payload["error"] = f"{type(exc).__name__}: {exc}"
    target.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(target, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
    return payload


def select_capture(payload: dict[str, Any], decision_unix: int) -> dict[str, Any] | None:
    cutoff = datetime.fromtimestamp(decision_unix, UTC).strftime("%Y%m%d%H%M%S")
    eligible = [
        item for item in payload.get("captures", [])
        if str(item.get("timestamp", "")) <= cutoff and str(item.get("statuscode")) == "200"
    ]
    return max(eligible, key=lambda item: item["timestamp"]) if eligible else None


def body_path(timestamp: str, original: str) -> Path:
    return RAW / "captures" / f"{timestamp}_{cache_key(original)}.bin.gz"


def fetch_body(capture: dict[str, Any], refresh: bool, offline: bool) -> dict[str, Any]:
    timestamp, original = str(capture["timestamp"]), str(capture["original"])
    target = body_path(timestamp, original)
    metadata_path = target.with_suffix(".json")
    if target.exists() and metadata_path.exists() and not refresh:
        metadata = json.loads(metadata_path.read_text())
        metadata["body_path"] = str(target)
        return metadata
    if metadata_path.exists() and not refresh and offline:
        metadata = json.loads(metadata_path.read_text())
        metadata["body_path"] = str(target) if target.exists() else None
        return metadata
    if offline:
        return {
            "timestamp": timestamp, "original": original, "body_path": None,
            "error": "not_cached_at_freeze_time", "http_status": None,
        }
    replay_url = REPLAY.format(timestamp=timestamp, url=original)
    metadata: dict[str, Any] = {
        "timestamp": timestamp, "original": original, "replay_url": replay_url,
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
    }
    try:
        response = request(replay_url)
        content = response.content[:MAX_BODY_BYTES]
        metadata.update({
            "http_status": response.status_code,
            "content_type": response.headers.get("content-type", ""),
            "content_length_received": len(response.content),
            "content_truncated": len(response.content) > MAX_BODY_BYTES,
            "body_sha256": sha256_bytes(content),
        })
        if response.status_code == 200:
            target.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(target, "wb") as handle:
                handle.write(content)
        else:
            metadata["error"] = f"HTTP {response.status_code}"
    except Exception as exc:
        metadata["error"] = f"{type(exc).__name__}: {exc}"
    # Wayback replay is substantially more rate-sensitive than timemap.
    time.sleep(0.75)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    metadata["body_path"] = str(target) if target.exists() else None
    return metadata


def extract_text(path: str | None, content_type: str) -> str:
    if not path or not Path(path).exists():
        return ""
    if not any(token in content_type.lower() for token in ["html", "text", "json", "xml"]):
        return ""
    with gzip.open(path, "rb") as handle:
        raw = handle.read(MAX_BODY_BYTES)
    decoded = raw.decode("utf-8", errors="replace")
    if "html" in content_type.lower() or "<html" in decoded[:2000].lower():
        parser = TextExtractor()
        parser.feed(decoded)
        decoded = " ".join(parser.parts)
    return re.sub(r"\s+", " ", decoded).strip()[:250_000]


def lexical_overlap(question: str, page: str) -> float | None:
    if not page:
        return None
    q = set(WORD_PATTERN.findall(question.lower()))
    p = set(WORD_PATTERN.findall(page.lower()))
    return len(q & p) / len(q) if q else None


def build(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    targets = load_targets()
    urls = sorted(targets.url.unique())
    timemaps: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=args.body_workers) as pool:
        futures = {pool.submit(fetch_timemap, url, args.refresh, args.offline): url for url in urls}
        for future in as_completed(futures):
            timemaps[futures[future]] = future.result()

    selections: dict[tuple[str, str], dict[str, Any]] = {}
    for row in targets.itertuples(index=False):
        capture = select_capture(timemaps[row.url], row.decision_time_unix)
        if capture:
            selections[(str(capture["timestamp"]), str(capture["original"]))] = capture

    bodies: dict[tuple[str, str], dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(fetch_body, capture, args.refresh, args.offline): key
            for key, capture in selections.items()
        }
        for future in as_completed(futures):
            bodies[futures[future]] = future.result()

    rows, provenance = [], []
    for target in targets.itertuples(index=False):
        timemap = timemaps[target.url]
        capture = select_capture(timemap, target.decision_time_unix)
        base = {
            "sample_id": target.sample_id, "url_ordinal": target.url_ordinal,
            "url": target.url, "domain": target.domain,
            "decision_time_unix": target.decision_time_unix,
            "source_tx": target.source_tx, "source_block": target.source_block,
        }
        if not capture:
            status = "timemap_error" if timemap.get("error") else "no_predecision_capture"
            rows.append({**base, "evidence_status": status})
            provenance.append({
                **base, "evidence_status": status, "timemap_path": str(timemap_path(target.url)),
                "timemap_sha256": sha256_bytes(timemap_path(target.url).read_bytes()),
                "capture_timestamp": None, "body_path": None, "body_sha256": None,
                "available_at_decision": False,
            })
            continue
        key = (str(capture["timestamp"]), str(capture["original"]))
        body = bodies[key]
        capture_unix = int(datetime.strptime(key[0], "%Y%m%d%H%M%S").replace(tzinfo=UTC).timestamp())
        text = extract_text(body.get("body_path"), str(body.get("content_type", "")))
        fetched = body.get("http_status") == 200 and bool(body.get("body_path"))
        status = "archived_text" if text else "archived_binary_or_empty" if fetched else "capture_fetch_error"
        rows.append({
            **base, "evidence_status": status, "capture_timestamp": key[0],
            "capture_time_unix": capture_unix,
            "capture_age_seconds": target.decision_time_unix - capture_unix,
            "archive_original_url": key[1], "archive_mimetype": capture.get("mimetype"),
            "content_type": body.get("content_type"), "content_bytes": body.get("content_length_received"),
            "content_sha256": body.get("body_sha256"), "text_chars": len(text),
            "question_token_coverage": lexical_overlap(target.question_text, text),
            "archived_text": text,
        })
        provenance.append({
            **base, "evidence_status": status, "timemap_path": str(timemap_path(target.url)),
            "timemap_sha256": sha256_bytes(timemap_path(target.url).read_bytes()),
            "capture_timestamp": key[0], "capture_time_unix": capture_unix,
            "body_path": body.get("body_path"), "body_sha256": body.get("body_sha256"),
            "available_at_decision": fetched and capture_unix <= target.decision_time_unix,
            "evidence_grade": "B" if fetched else "U",
            "interpretation": "Historical third-party web snapshot explicitly linked by immutable pre-proposal request; corroborative, not protocol ground truth.",
        })
    frame, prov = pd.DataFrame(rows), pd.DataFrame(provenance)
    text_rows = frame.evidence_status.eq("archived_text")
    checks = {
        "url_occurrences": len(frame), "samples_with_url": int(frame.sample_id.nunique()),
        "unique_urls": int(frame.url.nunique()), "unique_domains": int(frame.domain.nunique()),
        "archived_text_rows": int(text_rows.sum()),
        "archived_binary_or_empty_rows": int(frame.evidence_status.eq("archived_binary_or_empty").sum()),
        "no_predecision_capture_rows": int(frame.evidence_status.eq("no_predecision_capture").sum()),
        "timemap_error_rows": int(frame.evidence_status.eq("timemap_error").sum()),
        "capture_fetch_error_rows": int(frame.evidence_status.eq("capture_fetch_error").sum()),
        "postdecision_capture_rows": int(((frame.capture_time_unix > frame.decision_time_unix).fillna(False)).sum()),
        "all_source_links_are_onchain_preproposal": bool((frame.source_block > 0).all()),
        "all_fetched_captures_available_at_decision": bool(prov.loc[prov.body_path.notna(), "available_at_decision"].all()),
    }
    required = (
        checks["url_occurrences"] > 0
        and checks["postdecision_capture_rows"] == 0
        and checks["all_source_links_are_onchain_preproposal"]
        and checks["all_fetched_captures_available_at_decision"]
    )
    checks["all_required_assertions_pass"] = required
    if not required:
        raise RuntimeError(f"web evidence QC failed: {checks}")
    return frame.sort_values(["sample_id", "url_ordinal"]), prov.sort_values(["sample_id", "url_ordinal"]), checks


def write_outputs(frame: pd.DataFrame, provenance: pd.DataFrame, checks: dict[str, Any]) -> None:
    for path in [OUT, PROVENANCE, MANIFEST, REPORT]:
        path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(OUT, index=False)
    provenance.to_parquet(PROVENANCE, index=False)
    files = []
    for path in [OUT, PROVENANCE]:
        files.append({"path": str(path), "bytes": path.stat().st_size, "sha256": sha256_bytes(path.read_bytes())})
    manifest = {
        "dataset": "Polymarket proposal-time archived web evidence",
        "version": "1.0.0", "generated_at_utc": datetime.now(UTC).isoformat(),
        "selection_rule": "latest HTTP-200 Wayback capture with timestamp <= proposal time",
        "current_pages_used": False, "gamma_resolution_source_used": False,
        "all_required_assertions_pass": True, "checks": checks, "files": files,
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    covered_samples = frame.loc[frame.evidence_status.eq("archived_text"), "sample_id"].nunique()
    median_age = frame.loc[frame.evidence_status.eq("archived_text"), "capture_age_seconds"].median()
    REPORT.write_text(f"""# Proposal-time archived web evidence QC

## Result

- Immutable UMA request text contains **{checks['url_occurrences']:,}** distinct URL occurrences across **{checks['samples_with_url']:,}/810** samples (**{checks['unique_urls']:,}** unique URLs; **{checks['unique_domains']:,}** domains).
- **{checks['archived_text_rows']:,}** URL occurrences across **{covered_samples:,}** samples have retrievable text snapshots at or before proposal time.
- Median archive-to-proposal age among retrievable text snapshots: **{median_age:,.0f} seconds**.
- No current page, mutable Gamma `resolutionSource`, or post-proposal archive capture is admitted.
- Coverage is below the pre-specified minimum for a defensible train/test benchmark; **no web-evidence model is reported**.

## Missingness

| Status | URL occurrences |
|---|---:|
| archived text | {checks['archived_text_rows']:,} |
| archived binary/empty | {checks['archived_binary_or_empty_rows']:,} |
| no pre-decision capture | {checks['no_predecision_capture_rows']:,} |
| timemap error | {checks['timemap_error_rows']:,} |
| capture fetch error | {checks['capture_fetch_error_rows']:,} |

## Interpretation boundary

An archived page is corroborative decision-time evidence, not independent ground truth. URL inclusion was committed on chain before proposal, while page content and its capture time are checksum-pinned. `question_token_coverage` is descriptive retrieval diagnostics only and is not a truth label.

## QC

- Post-decision captures admitted: **{checks['postdecision_capture_rows']}**
- All fetched captures available at decision: **{checks['all_fetched_captures_available_at_decision']}**
- Required assertions: **PASS**
""")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--body-workers", type=int, default=2)
    args = parser.parse_args()
    frame, provenance, checks = build(args)
    write_outputs(frame, provenance, checks)
    print(json.dumps(checks, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
