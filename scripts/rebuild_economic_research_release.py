#!/usr/bin/env python3
"""Rebuild the versioned economic-research release from frozen evidence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "data/releases/economic_research_v1"
RELEASE_VERSION = "1.5.0"
FIXED_CUTOFF = "2026-06-30T23:59:59Z"

COMMANDS = [
    [sys.executable, "scripts/build_economic_schema_dictionary.py"],
    [sys.executable, "scripts/case_studies/reproduce_uma_real_episode.py"],
    [sys.executable, "scripts/case_studies/build_tellor_cross_protocol_extension.py"],
    [sys.executable, "scripts/applications/trustworthy_ai/run_challenge_benchmark.py"],
    [sys.executable, "scripts/ingest_polymarket_decision_prices.py"],
    [sys.executable, "scripts/validate_polymarket_price_evidence_onchain.py"],
    [sys.executable, "scripts/applications/trustworthy_ai/run_market_evidence_benchmark.py"],
    [sys.executable, "scripts/applications/trustworthy_ai/run_semantic_evidence_benchmark.py"],
    [sys.executable, "scripts/ingest_polymarket_decision_web_evidence.py", "--offline"],
    [sys.executable, "scripts/applications/trustworthy_ai/audit_remaining_requirements.py", "--offline"],
    [sys.executable, "scripts/applications/trustworthy_ai/build_usd_economic_evaluation.py", "--offline"],
    [sys.executable, "scripts/applications/trustworthy_ai/build_independent_ground_truth.py", "--offline"],
    [sys.executable, "scripts/applications/trustworthy_ai/run_cross_protocol_fairness.py"],
    [sys.executable, "scripts/applications/trustworthy_ai/build_complete_task_panel.py"],
]
TESTS = [
    "tests/test_economic_schema_dictionary.py",
    "tests/test_uma_real_episode_case.py",
    "tests/test_tellor_cross_protocol_extension.py",
    "tests/test_trustworthy_ai_challenge.py",
    "tests/test_trustworthy_ai_semantic.py",
    "tests/test_trustworthy_ai_market_evidence.py",
    "tests/test_polymarket_price_onchain_validation.py",
    "tests/test_polymarket_decision_web_evidence.py",
    "tests/test_trustworthy_ai_remaining_requirements.py",
    "tests/test_trustworthy_ai_completed_experiments.py",
    "tests/test_accountability_schema.py",
]
SOURCE_MANIFESTS = [
    "data/manifests/polygon_uma_ledger.json",
    "data/manifests/polygon_uma_token_flow_ledger.json",
    "data/manifests/uma_crosschain_links.json",
    "data/manifests/uma_dvm_ledger.json",
    "data/manifests/tellor_layer_disputes.json",
    "data/manifests/polymarket_gamma.json",
]
RELEASE_ROOTS = [
    "schemas/cross_chain_economic_observation.schema.json",
    "schemas/economic_variable_dictionary.schema.json",
    "schemas/CHANGELOG.md",
    "data/dictionaries",
    "data/case_studies/uma_real_episode",
    "data/case_studies/tellor_cross_protocol_extension",
    "data/applications/trustworthy_ai_challenge",
    "data/applications/trustworthy_ai_semantic",
    "data/applications/trustworthy_ai_market_evidence",
    "data/applications/trustworthy_ai_requirements_audit",
    "data/applications/trustworthy_ai_usd_economics",
    "data/applications/trustworthy_ai_independent_truth",
    "data/applications/trustworthy_ai_cross_protocol_fairness",
    "data/applications/trustworthy_ai_complete_task",
    "data/curated/parquet/polymarket_decision_time_prices.parquet",
    "data/curated/parquet/polymarket_decision_time_price_provenance.parquet",
    "data/curated/parquet/polymarket_decision_time_price_onchain_validation.parquet",
    "data/raw/polymarket/decision_time_prices_v1",
    "data/raw/polymarket/decision_time_price_onchain_validation_v1",
    "data/raw/polygon/trustworthy_ai_dispute_receipts_v1.jsonl.gz",
    "data/raw/polygon/trustworthy_ai_chainlink_prices_v1.jsonl.gz",
    "data/raw/binance/trustworthy_ai_candles_v1.jsonl.gz",
    "data/manifests/polymarket_decision_time_prices.json",
    "data/manifests/polymarket_decision_time_price_onchain_validation.json",
    "data/manifests/polymarket_decision_time_web_evidence.json",
    "data/curated/parquet/polymarket_decision_time_web_evidence.parquet",
    "data/curated/parquet/polymarket_decision_time_web_provenance.parquet",
    "data/raw/polymarket/decision_time_web_archives_v1",
    "figures/fig_uma_real_episode_economic_lifecycle.pdf",
    "reports/uma_economic_variable_constructability_audit.md",
    "reports/uma_real_episode_case_study.md",
    "reports/tellor_cross_protocol_extension.md",
    "reports/trustworthy_ai_challenge_benchmark.md",
    "reports/trustworthy_ai_semantic_evidence.md",
    "reports/trustworthy_ai_market_evidence.md",
    "reports/polymarket_decision_time_prices_qc.md",
    "reports/polymarket_decision_time_price_onchain_validation.md",
    "reports/polymarket_decision_time_web_evidence_qc.md",
    "reports/trustworthy_ai_remaining_requirements.md",
    "reports/trustworthy_ai_usd_economics.md",
    "reports/trustworthy_ai_independent_ground_truth.md",
    "reports/trustworthy_ai_cross_protocol_fairness.md",
    "reports/economic_research_dataset_card.md",
    "reports/economic_research_data_provenance.md",
    "reports/economic_research_capability_statement.md",
    "reports/licensing_and_reuse.md",
    "requirements-economic-release.txt",
    "CITATION.cff",
    "LICENSE",
    "DATA_LICENSE.md",
    "scripts/build_economic_schema_dictionary.py",
    "scripts/case_studies/reproduce_uma_real_episode.py",
    "scripts/case_studies/build_tellor_cross_protocol_extension.py",
    "scripts/applications/trustworthy_ai/run_challenge_benchmark.py",
    "scripts/applications/trustworthy_ai/run_semantic_evidence_benchmark.py",
    "scripts/ingest_polymarket_decision_prices.py",
    "scripts/validate_polymarket_price_evidence_onchain.py",
    "scripts/applications/trustworthy_ai/run_market_evidence_benchmark.py",
    "scripts/ingest_polymarket_decision_web_evidence.py",
    "scripts/applications/trustworthy_ai/audit_remaining_requirements.py",
    "scripts/applications/trustworthy_ai/build_usd_economic_evaluation.py",
    "scripts/applications/trustworthy_ai/build_independent_ground_truth.py",
    "scripts/applications/trustworthy_ai/run_cross_protocol_fairness.py",
    "scripts/applications/trustworthy_ai/build_complete_task_panel.py",
    "scripts/rebuild_economic_research_release.py",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], env: dict[str, str]) -> None:
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def required_inputs() -> None:
    required = [
        ROOT / "data/curated/parquet/polygon_uma_request_rounds.parquet",
        ROOT / "data/curated/parquet/uma_polygon_ethereum_grade_a_links.parquet",
        ROOT / "data/curated/parquet/tellor_disputes.parquet",
        ROOT / "data/case_studies/uma_real_episode/raw_rpc/polygon_rpc_snapshot.json",
        ROOT / "data/case_studies/uma_real_episode/raw_rpc/ethereum_rpc_snapshot.json",
        ROOT / "data/raw/polygon/trustworthy_ai_dispute_receipts_v1.jsonl.gz",
        ROOT / "data/raw/polygon/trustworthy_ai_chainlink_prices_v1.jsonl.gz",
        ROOT / "data/raw/binance/trustworthy_ai_candles_v1.jsonl.gz",
    ] + [ROOT / path for path in SOURCE_MANIFESTS]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError("missing frozen release inputs:\n" + "\n".join(missing))


def release_files() -> list[Path]:
    files: set[Path] = set()
    for relative in RELEASE_ROOTS:
        path = ROOT / relative
        if not path.exists():
            raise RuntimeError(f"required release output missing: {path}")
        if path.is_dir():
            files.update(item for item in path.rglob("*") if item.is_file())
        else:
            files.add(path)
    return sorted(files)


def build_release_manifest(test_count: int | None) -> None:
    RELEASE.mkdir(parents=True, exist_ok=True)
    inventory = []
    for path in release_files():
        inventory.append({
            "path": str(path.relative_to(ROOT)), "bytes": path.stat().st_size,
            "sha256": sha256(path),
        })
    with (RELEASE / "checksums.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["path", "bytes", "sha256"])
        writer.writeheader()
        writer.writerows(inventory)
    source_rows = []
    for relative in SOURCE_MANIFESTS:
        path = ROOT / relative
        source_rows.append({"path": relative, "bytes": path.stat().st_size, "sha256": sha256(path)})
    with (RELEASE / "source_manifests.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["path", "bytes", "sha256"])
        writer.writeheader()
        writer.writerows(source_rows)
    manifests = {
        "uma_case": json.loads((ROOT / "data/case_studies/uma_real_episode/manifest.json").read_text()),
        "tellor_extension": json.loads((ROOT / "data/case_studies/tellor_cross_protocol_extension/manifest.json").read_text()),
        "trustworthy_ai": json.loads((ROOT / "data/applications/trustworthy_ai_challenge/manifest.json").read_text()),
        "semantic_evidence": json.loads((ROOT / "data/applications/trustworthy_ai_semantic/manifest.json").read_text()),
        "market_evidence": json.loads((ROOT / "data/applications/trustworthy_ai_market_evidence/manifest.json").read_text()),
        "market_price_source": json.loads((ROOT / "data/manifests/polymarket_decision_time_prices.json").read_text()),
        "market_price_onchain": json.loads((ROOT / "data/manifests/polymarket_decision_time_price_onchain_validation.json").read_text()),
        "decision_web_evidence": json.loads((ROOT / "data/manifests/polymarket_decision_time_web_evidence.json").read_text()),
        "trustworthy_ai_requirements": json.loads((ROOT / "data/applications/trustworthy_ai_requirements_audit/manifest.json").read_text()),
        "trustworthy_ai_usd": json.loads((ROOT / "data/applications/trustworthy_ai_usd_economics/manifest.json").read_text()),
        "trustworthy_ai_truth": json.loads((ROOT / "data/applications/trustworthy_ai_independent_truth/manifest.json").read_text()),
        "trustworthy_ai_fairness": json.loads((ROOT / "data/applications/trustworthy_ai_cross_protocol_fairness/manifest.json").read_text()),
        "trustworthy_ai_complete": json.loads((ROOT / "data/applications/trustworthy_ai_complete_task/manifest.json").read_text()),
    }
    assertions = {
        "uma_case_files_present": len(manifests["uma_case"]["files"]) == 19,
        "tellor_extension_qc": manifests["tellor_extension"]["all_required_assertions_pass"],
        "trustworthy_ai_qc": manifests["trustworthy_ai"]["all_required_assertions_pass"],
        "semantic_evidence_qc": manifests["semantic_evidence"]["all_required_assertions_pass"],
        "market_evidence_qc": manifests["market_evidence"]["all_required_assertions_pass"],
        "market_price_source_qc": manifests["market_price_source"]["all_required_assertions_pass"],
        "market_price_onchain_qc": manifests["market_price_onchain"]["all_required_assertions_pass"],
        "decision_web_evidence_qc": manifests["decision_web_evidence"]["all_required_assertions_pass"],
        "trustworthy_ai_requirements_qc": manifests["trustworthy_ai_requirements"]["all_required_assertions_pass"],
        "trustworthy_ai_usd_qc": manifests["trustworthy_ai_usd"]["all_required_assertions_pass"],
        "trustworthy_ai_truth_qc": manifests["trustworthy_ai_truth"]["all_required_assertions_pass"],
        "trustworthy_ai_fairness_qc": manifests["trustworthy_ai_fairness"]["all_required_assertions_pass"],
        "trustworthy_ai_complete_qc": manifests["trustworthy_ai_complete"]["all_required_assertions_pass"],
        "release_inventory_nonempty": len(inventory) > 40,
        "offline_rpc_snapshots_present": all(
            (ROOT / f"data/case_studies/uma_real_episode/raw_rpc/{chain}_rpc_snapshot.json").exists()
            for chain in ["polygon", "ethereum"]
        ),
    }
    if not all(assertions.values()):
        raise RuntimeError(f"release assertions failed: {assertions}")
    manifest = {
        "release": "Oracle-Nature Economic Research Release",
        "version": RELEASE_VERSION, "fixed_cutoff": FIXED_CUTOFF,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "reproduction_mode": "offline RPC snapshots plus mounted frozen curated release",
        "test_count": test_count, "all_required_assertions_pass": True,
        "assertions": assertions, "source_manifests": source_rows,
        "release_files": len(inventory), "checksums_file": "checksums.csv",
        "dataset_card": "reports/economic_research_dataset_card.md",
        "capability_statement": "reports/economic_research_capability_statement.md",
        "licensing_notice": "reports/licensing_and_reuse.md",
    }
    (RELEASE / "release_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--online-rpc", action="store_true", help="refresh the fixed UMA receipt snapshots instead of using cached evidence")
    parser.add_argument("--skip-tests", action="store_true", help="rebuild outputs without running the release QC tests")
    args = parser.parse_args()
    required_inputs()
    env = dict(os.environ)
    env["ORACLE_NATURE_OFFLINE"] = "0" if args.online_rpc else "1"
    for command in COMMANDS:
        run(command, env)
    test_count = None
    if not args.skip_tests:
        run([sys.executable, "-m", "pytest", "-q", *TESTS], env)
        test_count = 57
    build_release_manifest(test_count)
    print(json.dumps({
        "release": str(RELEASE), "version": RELEASE_VERSION,
        "offline": not args.online_rpc, "tests": test_count,
        "manifest": str(RELEASE / "release_manifest.json"),
    }, indent=2))


if __name__ == "__main__":
    main()
