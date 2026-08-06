"""Build a reproducible Oracle-universe census without crawling every delivery chain."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
SOURCE_URL = "https://api.llama.fi/protocols"
ALIASES = {"Api3": "API3", "Flare FTSO": "Flare_FTSOv2"}


def canonical(name: str) -> str:
    return ALIASES.get(name, name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Oracle universe registry")
    parser.add_argument("--use-cache", action="store_true", help="rebuild from the checksum-protected raw snapshot without refetching")
    args = parser.parse_args()
    raw = ROOT / "data/raw/ecosystem/defillama_protocols.json.gz"
    if args.use_cache:
        if not raw.is_file():
            raise RuntimeError(f"cached universe snapshot is missing: {raw}")
        with gzip.open(raw, "rb") as handle:
            body = handle.read()
        old_manifest = ROOT / "data/manifests/oracle_universe_registry.json"
        retrieved = json.loads(old_manifest.read_text(encoding="utf-8"))["snapshot_time_utc"]
    else:
        request = urllib.request.Request(SOURCE_URL, headers={"User-Agent": "oracle-accountability-atlas/0.1"})
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read()
        retrieved = datetime.now(UTC).isoformat()
        raw.parent.mkdir(parents=True, exist_ok=True)
        temporary = raw.with_suffix(raw.suffix + ".tmp")
        with gzip.open(temporary, "wb", compresslevel=9) as handle:
            handle.write(body)
        temporary.replace(raw)
    digest = hashlib.sha256(raw.read_bytes()).hexdigest()
    raw.with_suffix(raw.suffix + ".sha256").write_text(digest + "\n", encoding="utf-8")

    seeds = yaml.safe_load((ROOT / "registry/oracle_networks.yaml").read_text(encoding="utf-8"))["networks"]
    seed_by_name = {row["oracle_network"]: row for row in seeds}
    integrations: dict[str, list[dict]] = defaultdict(list)
    for protocol in json.loads(body):
        for oracle in protocol.get("oracles") or []:
            integrations[canonical(str(oracle))].append(protocol)

    output = ROOT / "registry/oracle_universe.jsonl"
    with output.open("w", encoding="utf-8") as handle:
        for name in sorted(set(integrations) | set(seed_by_name), key=str.casefold):
            protocols = integrations.get(name, [])
            seed = seed_by_name.get(name, {})
            chains = sorted({chain for row in protocols for chain in (row.get("chains") or [])})
            tvl = sum((Decimal(str(row.get("tvl") or 0)) for row in protocols), Decimal(0))
            record = {
                "oracle_network": name,
                "oracle_family": seed.get("oracle_family", "pending_classification"),
                "security_chain": seed.get("security_chain", "pending_observability_audit"),
                "delivery_chains_observed": chains,
                "number_of_integrated_protocols_observed": len(protocols),
                "integrated_protocol_tvl_sum_usd": format(tvl, "f"),
                "reward_mechanism_documented": seed.get("reward_mechanism_documented", "pending_review"),
                "penalty_mechanism_documented": seed.get("penalty_mechanism_documented", "pending_review"),
                "reward_onchain_observable": seed.get("reward_onchain_observable", "pending_observability_audit"),
                "penalty_onchain_observable": seed.get("penalty_onchain_observable", "pending_observability_audit"),
                "report_level_observable": seed.get("report_level_observable", "pending_observability_audit"),
                "publisher_level_observable": seed.get("publisher_level_observable", "pending_observability_audit"),
                "deep_panel_status": seed.get("deep_panel_status", "universe_only_pending_observability_audit"),
                "source_evidence": seed.get("source_evidence"),
                "universe_source": SOURCE_URL,
                "snapshot_time_utc": retrieved,
                "measurement_note": "TVL is the sum of current protocol TVL for protocols carrying this oracle tag; multi-oracle protocols can appear in multiple rows and this is not a non-overlapping market-share measure.",
            }
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")

    manifest = {
        "dataset": "Oracle ecosystem universe census",
        "snapshot_time_utc": retrieved,
        "source_url": SOURCE_URL,
        "raw_snapshot": str(raw),
        "raw_snapshot_sha256": digest,
        "oracle_categories": len(set(integrations) | set(seed_by_name)),
        "protocol_oracle_assignments": sum(len(rows) for rows in integrations.values()),
        "deep_seed_networks_present": sorted(seed_by_name),
        "output": str(output),
        "scope_note": "Universe census, not an event crawl and not proof that a tagged mechanism has observable rewards or penalties.",
        "rebuilt_from_cached_snapshot": args.use_cache,
    }
    manifest_path = ROOT / "data/manifests/oracle_universe_registry.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(manifest_path)


if __name__ == "__main__":
    main()
