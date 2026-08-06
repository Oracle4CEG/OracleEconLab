"""Rebuild and validate the fixed-cutoff Oracle dataset release.

Run this only after every protocol collector has written its QC manifest.
Base JSONL/native Parquet tables are exported before the two common derived
ledgers so those builders cannot accidentally read stale Parquet artifacts.
The second export refreshes the aggregate Parquet manifest after derivation.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFESTS = ROOT / "data/manifests"
REQUIRED_COLLECTOR_MANIFESTS = (
    "chainlink_evidence_ledger",
    "chainlink_staking_v02_ledger",
    "polygon_uma_ledger",
    "polygon_uma_token_flow_ledger",
    "uma_crosschain_links",
    "uma_dvm_ledger",
    "tellor_layer_disputes",
    "tellor_tips_withdrawals",
    "tellor_aggregate_index",
    "tellor_rewards_full",
    "tellor_micro_reports",
    "tellor_jail_lifecycle",
    "flare_fsp_rewards",
    "flare_claims_chill",
    "pyth_ois_rolling_state",
    "pyth_ois_history",
    "pyth_historical_observability",
    "flare_reward_attribution",
    "phase4_oracle_economic_interfaces",
    "dia_staking",
    "chronicle_redstone_ethereum_events",
    "chronicle_redstone_settlement_audit",
)

COMMANDS = (
    ("contract semantics audit", "scripts/build_contract_semantics_audit.py"),
    (
        "oracle universe registry",
        "scripts/build_oracle_universe_registry.py",
        "--use-cache",
    ),
    ("oracle observability scores", "scripts/score_oracle_registry.py"),
    ("Pyth historical boundary", "scripts/audit_pyth_historical_boundary.py"),
    ("Flare reward attribution boundary", "scripts/build_flare_reward_attribution.py"),
    ("DIA Lasernet staking ledger", "scripts/ingest_dia_staking.py"),
    ("phase-four economic interfaces", "scripts/build_phase4_economic_interfaces.py"),
    ("Chronicle/RedStone settlement boundary", "scripts/audit_chronicle_redstone_settlement.py"),
    ("base curated Parquet export", "scripts/export_curated_parquet.py"),
    (
        "strict realized reward/slash ledger",
        "scripts/build_realized_economic_events.py",
    ),
    (
        "common accountability event ledger",
        "scripts/build_accountability_events.py",
    ),
    ("fixed Sample B/C rebuild", "scripts/build_research_samples.py"),
    ("final curated Parquet manifest", "scripts/export_curated_parquet.py"),
    ("research-readiness report", "scripts/report_data_completeness.py"),
    ("release-level QC", "scripts/qc_oracle_dataset_release.py"),
)


def validate_collectors() -> None:
    missing = []
    failed = []
    for name in REQUIRED_COLLECTOR_MANIFESTS:
        path = MANIFESTS / f"{name}.json"
        if not path.is_file():
            missing.append(name)
            continue
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if "all_required_assertions_pass" in manifest:
            if manifest["all_required_assertions_pass"] is not True:
                failed.append(name)
    if missing or failed:
        raise RuntimeError(
            f"collector preflight failed; missing={missing}, failed={failed}"
        )


def main() -> None:
    validate_collectors()
    environment = os.environ.copy()
    existing = environment.get("PYTHONPATH")
    prefix = f"{ROOT}:{ROOT / 'src'}"
    environment["PYTHONPATH"] = f"{prefix}:{existing}" if existing else prefix
    for command in COMMANDS:
        label, *arguments = command
        print(f"==> {label}", flush=True)
        subprocess.run(
            [sys.executable, *arguments],
            cwd=ROOT,
            env=environment,
            check=True,
        )
    release = json.loads(
        (MANIFESTS / "oracle_dataset_release.json").read_text(encoding="utf-8")
    )
    if release.get("all_required_assertions_pass") is not True:
        raise RuntimeError("release manifest did not pass")
    print(MANIFESTS / "oracle_dataset_release.json", flush=True)


if __name__ == "__main__":
    main()
