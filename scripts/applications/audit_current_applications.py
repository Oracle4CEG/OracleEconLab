"""Audit the first Applications implementation before rebuilding it."""
from __future__ import annotations

import json
from datetime import UTC, datetime

import duckdb

from scripts.applications.common import CUTOFF, MANIFESTS, ROOT, atomic_text, pq, release_checks, sha256


def main() -> None:
    con = duckdb.connect()
    release = release_checks(con)
    manifest = json.loads((MANIFESTS / "curated_parquet.json").read_text())
    native_rows = sum(int(x["rows"]) for x in manifest["files"])
    actor_counts = dict(
        con.execute(
            f"""SELECT oracle_network, count(DISTINCT actor)
                FROM {pq('sample_b_observable_accountability')}
                WHERE actor IS NOT NULL GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    )
    unit_counts = dict(
        con.execute(
            f"""SELECT oracle_network, count(DISTINCT accountability_unit_id)
                FROM {pq('sample_b_observable_accountability')}
                GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    )
    realized = {
        protocol: {"reward": reward, "slash": slash}
        for protocol, reward, slash in con.execute(
            f"""SELECT oracle_network,
                       count(*) FILTER (include_in_realized_reward),
                       count(*) FILTER (include_in_realized_slash)
                FROM {pq('realized_reward_slash_events')}
                GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    }
    coverage = dict(
        con.execute(
            f"""SELECT observability_grade, count(*)
                FROM {pq('accountability_events')} GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    )
    text = f"""# Applications Redesign Audit

Generated: {datetime.now(UTC).isoformat()}  
Fixed cutoff: `{CUTOFF}`  
Release manifest SHA-256: `{sha256(MANIFESTS / 'oracle_dataset_release.json')}`

## Recomputed release facts

- Registry entries: **{release['registry']}**.
- Unified accountability rows: **{release['accountability']:,}**.
- Sample B rows: **{release['sample_b']:,}**.
- Sample C rows: **{release['sample_c']:,}**.
- Curated application manifest: **{release['manifest_tables']} tables / {release['manifest_rows']:,} rows**.
- Release-QC manifest scope: **{len(manifest['files'])} protocol-native/derived tables / {native_rows:,} rows**.
- Sample-B actor counts: `{json.dumps(actor_counts, sort_keys=True)}`.
- Sample-B accountability-unit counts: `{json.dumps(unit_counts, sort_keys=True)}`.
- Realized reward/slash event counts: `{json.dumps(realized, sort_keys=True)}`.
- Unified observability-grade counts: `{json.dumps(coverage, sort_keys=True)}`.

All application queries use the immutable fixed-cutoff Parquet release. No source
Parquet is modified.

## Why the current applications are insufficient

1. **Mechanism clustering is confounded by evidence completeness.** The old
   feature matrix encodes unknown mechanisms as absent and mixes event-depth,
   transaction observability, and market integration with design. Consequently
   51 of 56 Registry systems form a residual “low-observability” cluster. That
   group is an evidence state, not an Oracle mechanism family, and must not be
   retained as the main taxonomy.
2. **Financial economics lacks a conversion object.** Existing stage counts,
   concentration strata, and lifecycle rows are reusable, but the narrative
   mainly repeats semantic fields. It does not explicitly model
   designed/configured → eligible → accrued/claimable → paid/applied, identify
   where denominators align, or quantify right-censoring and enforcement gaps.
3. **Geography is not a mature ecosystem result.** The existing coordinate
   output is derived from UMA Gamma text, covers 16 countries, and has no
   completed external-human precision estimate. It cannot support a
   cross-ecosystem geography claim. Geography must be preliminary until the
   stratified gold review is completed; semantic-domain coverage can be the
   primary multi-protocol result.

## Results retained

- Release counts and source manifests; Sample B/Sample C scope rules.
- Transaction-gated `economic_semantics_events` and
  `realized_reward_slash_events`, including `do_not_sum_group`.
- UMA principal/reward/forfeiture decomposition and right-censored lifecycle.
- Flare entitlement/claim timestamps and non-monetary conditions.
- Protocol--asset actor reward strata.
- UMA source text, deterministic location evidence, and the existing
  high/ambiguous distinction as inputs to a larger annotation package.
- Protocol-internal actor features for UMA and Chainlink as a supplement only.

## Results removed from the main paper

- The old three-cluster mechanism taxonomy and its “51 low-observability
  systems” mechanism interpretation.
- Five-protocol actor-cluster enumeration as a main result.
- Count-only claim-realization bars where claimable and paid denominators do not
  align.
- UMA-only coordinates presented as ecosystem geographic coverage.
- A single fixed-release bar labeled temporal geographic expansion.

## New construction required

- A status-aware mechanism-design matrix with `observed_yes`, `observed_no`,
  `not_applicable`, `unknown`, and `structurally_unobservable`; a separate
  observability matrix; a 40% core-feature missingness gate.
- Hierarchical and medoid robustness clustering, component enrichment,
  protocol--component network communities, rare-component and medoid-distance
  outliers.
- A stage-aware accountability conversion table, protocol-native capital-lock
  and latency tables, protocol--asset--role concentration, penalty-frequency
  denominators, signed outcomes, and explicit coverage breaks.
- A multi-protocol semantic label layer from structured protocol metadata and
  deterministic query/feed rules; a protocol × domain × geography × time cube.
- A 1,000-row stratified review package and gold-label template. Until external
  humans complete it and precision reaches 0.90, geography remains preliminary.

## Feasibility and research questions

| Application | Sample/validation condition | Feasibility | Core question |
|---|---|---|---|
| Mechanism space | Registry 56; primary sample excludes >40% unknown core design features | Feasible, with unclassified rows retained in observability map | Can heterogeneous systems share a mechanism space without missingness or market size driving taxonomy? |
| Accountability conversion | Five deep panels plus strict transaction/state evidence; ratios only for aligned denominators | Feasible for stage counts, enforcement frequency, concentration and selected lifecycles; some amount ratios remain unavailable | Where do designed incentives and penalties become observable paid/applied outcomes, and where does evidence break? |
| Geographic × semantic coverage | Multi-protocol structured metadata; geography requires 1,000 external-human labels and ≥0.90 precision | Semantic analysis feasible; geography preliminary | Which real-world semantic domains and explicit regions are represented, and where are coverage gaps? |

The rebuilt chapter will answer these questions descriptively. It will not infer
causal effects, protocol quality, actor identity, or operator geography.
"""
    atomic_text(ROOT / "reports/applications_redesign_audit.md", text)


if __name__ == "__main__":
    main()
