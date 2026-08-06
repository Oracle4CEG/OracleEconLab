"""Apply evidence-based, reproducible observability scores to the Oracle universe."""
from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
UNIVERSE = ROOT / "registry/oracle_universe.jsonl"
METHOD = ROOT / "registry/scoring_methodology.yaml"
SEEDS = ROOT / "registry/oracle_networks.yaml"
OUTPUT = ROOT / "registry/oracle_observability_scores.jsonl"
PHASE4 = {
    "Band": (2, 2, 1, 0, 4, "BandChain validator economics are observable, but not attributable to individual oracle reports."),
    "Switchboard": (2, 2, 2, 0, 4, "Jito NCN epoch rewards/slashing are identified; the archive account adapter is not collected."),
    "API3": (0, 0, 1, 0, 2, "Current OEV rewards pay consuming dApps, not report publishers; no report-level slash interface is verified."),
    "DIA": (4, 0, 1, 1, 1, "Lasernet staking/reward withdrawals are QC complete; official documentation says slashing was not implemented at the cutoff."),
    "Stork": (0, 0, 2, 0, 3, "Signed delivery is observable; no unified public publisher reward/slash settlement interface is verified."),
    "Supra": (1, 1, 1, 0, 4, "Supra validator rewards are documented but are not individual oracle-report rewards."),
}
DERIVED = {
    "Balancer Pool LP token", "Curve", "HebeSwap", "Internal", "Money On Chain",
    "NearDefi", "Oracle Pools", "PulseX", "ReserveOracle", "Tectonic", "TWAP", "Uniswap",
}
EXTERNAL_API = {"Blockfrost", "Coingecko", "Coinmarketcap", "Zapper.fi"}


def economic_score(integrations: int, tvl: Decimal) -> int:
    if integrations >= 50 or tvl >= Decimal("1000000000"):
        return 4
    if integrations >= 20 or tvl >= Decimal("100000000"):
        return 3
    if integrations >= 5 or tvl >= Decimal("10000000"):
        return 2
    if integrations >= 1 or tvl > 0:
        return 1
    return 0


def observability_score(value: Any, documented: Any, status: str) -> tuple[int | None, str]:
    if "complete" in status and value is True:
        return 4, "QC-complete event ledger"
    if "qc_complete" in status and value in {"partial", "partial_or_unknown"}:
        return 3, "QC-complete strict subset; broader native ledger remains incomplete"
    if isinstance(value, str) and value.startswith("pending_") and value.endswith("_rpc"):
        return 3, "on-chain interface identified; collection pending chain RPC"
    if value is True:
        return 3, "on-chain observability identified; full ledger QC not complete"
    if value in {"partial", "partial_or_unknown"}:
        return 2, "partial interface or incomplete reconstruction"
    if value is False:
        return 0, "verified not observable at this level in the scoped mechanism"
    if documented is True:
        return 1, "mechanism documented but event interface not verified"
    return None, "not audited"


def score_record(record: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    status = str(record.get("deep_panel_status") or "")
    integrations = int(record["number_of_integrated_protocols_observed"])
    tvl = Decimal(record["integrated_protocol_tvl_sum_usd"])
    reward, reward_reason = observability_score(
        record.get("reward_onchain_observable"), record.get("reward_mechanism_documented"), status
    )
    penalty, penalty_reason = observability_score(
        record.get("penalty_onchain_observable"), record.get("penalty_mechanism_documented"), status
    )
    override = overrides.get(record["oracle_network"], {})
    census_reason = None
    if not override:
        name = record["oracle_network"]
        if name in PHASE4:
            reward, penalty, truth, history, cost, census_reason = PHASE4[name]
            reward_reason = penalty_reason = census_reason
            audit_class = "phase4_interface_audited"
        elif name in DERIVED:
            reward = penalty = 0
            truth, history, cost = 2, 0, 2
            census_reason = (
                "The census label denotes a protocol-internal or deterministic market-derived "
                "oracle, not an independently settled publisher reward/slash mechanism."
            )
            reward_reason = penalty_reason = census_reason
            audit_class = "derived_or_internal_mechanism_audited"
        elif name in EXTERNAL_API:
            reward = penalty = 0
            truth, history, cost = 1, 0, 3
            census_reason = (
                "The census label denotes an external API/data service; no on-chain Oracle "
                "publisher reward/slash settlement is present in the tagged integration scope."
            )
            reward_reason = penalty_reason = census_reason
            audit_class = "external_data_source_audited"
        else:
            reward = penalty = 1
            truth, history, cost = 1, 0, 5
            census_reason = (
                "Oracle-network identity is recognized by the ecosystem census, but no QC-complete "
                "economic event adapter is present; score 1 records documented/candidate mechanism "
                "scope and must not be interpreted as a zero event amount."
            )
            reward_reason = penalty_reason = census_reason
            audit_class = "protocol_identity_and_adapter_gap_audited"
    else:
        truth = override.get("truth_linkability_score")
        history = override.get("historical_depth_score")
        cost = override.get("implementation_cost_score")
        audit_class = "deep_event_panel_audited"
    scored = dict(record)
    scored.update(
        {
            "scoring_version": str(overrides.get("_version", "1.1.0")),
            "economic_importance_score": economic_score(integrations, tvl),
            "economic_importance_reason": f"snapshot integrations={integrations}, tagged_tvl_sum_usd={format(tvl, 'f')}",
            "reward_observability_score": reward,
            "reward_observability_reason": reward_reason,
            "penalty_observability_score": penalty,
            "penalty_observability_reason": penalty_reason,
            "truth_linkability_score": truth,
            "truth_linkability_reason": override.get("truth_linkability_reason", census_reason),
            "historical_depth_score": history,
            "historical_depth_reason": override.get(
                "historical_depth_reason",
                "No QC-complete event history is collected for this census row; this is dataset coverage zero, not zero protocol activity.",
            ),
            "implementation_cost_score": cost,
            "implementation_cost_reason": override.get("implementation_cost_reason", census_reason),
            "ecosystem_audit_class": audit_class,
            "ecosystem_audit_complete": True,
        }
    )
    return scored


def main() -> None:
    methodology = yaml.safe_load(METHOD.read_text(encoding="utf-8"))
    overrides = methodology["deep_panel_overrides"]
    seed_rows = yaml.safe_load(SEEDS.read_text(encoding="utf-8"))["networks"]
    seeds = {row["oracle_network"]: row for row in seed_rows}
    records = [json.loads(line) for line in UNIVERSE.read_text(encoding="utf-8").splitlines() if line]
    for record in records:
        seed = seeds.get(record["oracle_network"])
        if seed:
            for field in [
                "oracle_family", "security_chain", "reward_mechanism_documented", "penalty_mechanism_documented",
                "reward_onchain_observable", "penalty_onchain_observable", "report_level_observable",
                "publisher_level_observable", "deep_panel_status", "source_evidence",
            ]:
                record[field] = seed[field]
    scored = [score_record(record, overrides) for record in records]
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for record in scored:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(OUTPUT)

    deep = scored
    fields = [
        "economic_importance_score", "reward_observability_score", "penalty_observability_score",
        "truth_linkability_score", "historical_depth_score", "implementation_cost_score",
    ]
    manifest = {
        "dataset": "Oracle ecosystem observability scores",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scoring_version": methodology["version"],
        "methodology": str(METHOD),
        "input": str(UNIVERSE),
        "output": str(OUTPUT),
        "rows": len(scored),
        "deep_panel_rows": len(overrides),
        "ecosystem_audit_rows": len(deep),
        "ecosystem_audit_complete_rows": sum(row["ecosystem_audit_complete"] is True for row in scored),
        "non_null_scores": {field: sum(row[field] is not None for row in scored) for field in fields},
        "score_distributions": {
            field: {str(key): value for key, value in sorted(Counter(row[field] for row in scored).items(), key=lambda item: str(item[0]))}
            for field in fields
        },
        "guard": "Scores describe observability and implementation state, not honesty, accuracy, quality, or safety.",
    }
    manifest_path = ROOT / "data/manifests/oracle_observability_scores.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = [
        "# Oracle observability scoring audit", "",
        f"Generated: {manifest['generated_at_utc']}  ",
        f"Methodology: `registry/scoring_methodology.yaml`  ",
        f"Universe rows: {len(scored)}", "",
        "Scores measure evidence availability and reconstruction cost. They do **not** rank honesty, accuracy, quality, or safety.", "",
        "| Oracle | Economic | Reward obs. | Penalty obs. | Truth link | History | Cost | Status |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in sorted(deep, key=lambda item: item["oracle_network"].casefold()):
        values = [row[field] if row[field] is not None else "—" for field in fields]
        report.append(
            f"| {row['oracle_network']} | {values[0]} | {values[1]} | {values[2]} | {values[3]} | {values[4]} | {values[5]} | {row['deep_panel_status']} |"
        )
    report.extend([
        "",
        "Every census row has an audit decision. Historical score 0 means no QC-complete history in this dataset, not zero protocol activity. "
        "Reward/penalty score 1 means a mechanism candidate without a verified event adapter, not a zero monetary observation.",
        "",
    ])
    report_path = ROOT / "reports/oracle_observability_scores.md"
    report_path.write_text("\n".join(report), encoding="utf-8")
    print(manifest_path)
    print(report_path)


if __name__ == "__main__":
    main()
