"""Freeze the Atlas Sample B and Sample C analysis tables.

Sample B is the complete five-network observable-accountability panel at the
fixed cutoff.  Sample C contains only outcomes that can be tied to a protocol
truth/reliability rule.  A row in Sample C is not a claim of external truth.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
FIXED_CUTOFF = "2026-06-30T23:59:59Z"
MAIN_START_UNIX = 1_680_307_200
NETWORKS = ("Chainlink", "Pyth", "UMA", "Tellor", "Flare_FTSOv2")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_parquet(connection: duckdb.DuckDBPyConnection, query: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    escaped = str(temporary).replace("'", "''")
    connection.execute(
        f"COPY ({query}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
    )
    temporary.replace(output)


def build_samples(parquet_dir: Path, output_dir: Path, manifest_path: Path) -> dict[str, object]:
    required = {
        name: parquet_dir / f"{name}.parquet"
        for name in (
            "accountability_events",
            "uma_dvm_requests",
            "uma_dvm_votes_events",
            "uma_polygon_ethereum_grade_a_links",
            "flare_provider_feed_performance",
        )
    }
    missing = [str(path) for path in required.values() if not path.is_file()]
    if missing:
        raise RuntimeError(f"missing required fixed-cutoff inputs: {missing}")

    connection = duckdb.connect()
    source = lambda name: f"read_parquet('{required[name].as_posix()}')"
    network_sql = ", ".join(f"'{value}'" for value in NETWORKS)
    sample_b = output_dir / "sample_b_observable_accountability.parquet"
    sample_c = output_dir / "sample_c_strict_honesty_events.parquet"

    sample_b_query = f"""
        SELECT
          a.*,
          'B' analysis_sample,
          '{FIXED_CUTOFF}' fixed_cutoff,
          'SAMPLE_B_ALL_FIVE_NETWORK_OBSERVABLE_ROWS_V1' sample_rule_id,
          CAST(NULL AS VARCHAR) strict_event_class,
          CAST(NULL AS VARCHAR) strict_metric_name,
          CAST(NULL AS VARCHAR) strict_metric_numerator_raw,
          CAST(NULL AS VARCHAR) strict_metric_denominator_raw,
          CAST(NULL AS BIGINT) strict_metric_rate_ppm
        FROM {source('accountability_events')} a
        WHERE oracle_network IN ({network_sql})
        UNION ALL BY NAME
        SELECT
          '1.0.0' schema_version,
          sha256(concat_ws('|', 'flare_feed_epoch', reward_epoch_id::VARCHAR, voter_address, feed_name)) accountability_event_id,
          'Flare_FTSOv2' oracle_network,
          'continuous_consensus_band_reward' mechanism_family,
          'Flare_Mainnet' security_chain,
          'Flare_Mainnet' delivery_chain,
          'publisher_pool_epoch' accountability_unit_type,
          concat_ws(':', reward_epoch_id::VARCHAR, voter_address, feed_name) accountability_unit_id,
          'provider_feed_epoch_consensus_band_performance' event_granularity,
          voter_address actor,
          'data_provider' actor_role,
          CASE WHEN ftso_scaling_condition_met THEN NULL ELSE 'reward_ineligibility_condition' END nonmonetary_penalty,
          'consensus_median_alignment' truth_basis,
          CASE WHEN ftso_scaling_condition_met THEN 'consensus_band_condition_met' ELSE 'consensus_band_condition_not_met' END outcome_status,
          false external_truth_available,
          'official_reward_epoch_calculation_file' source_event,
          concat('reward-calculation-data:', source_file) source_tx,
          'FLARE_FTSO_PROVIDER_FEED_PERFORMANCE_V1' rule_id,
          'A' observability_grade,
          'A' confidence_grade,
          'flare_provider_feed_performance' native_table,
          'observable_accountability_panel' sample_tier,
          'Consensus-band alignment is protocol-recognized reliability, not external objective price truth.' interpretation_note,
          'B' analysis_sample,
          '{FIXED_CUTOFF}' fixed_cutoff,
          'SAMPLE_B_FLARE_FEED_CONSENSUS_ALIGNMENT_V1' sample_rule_id,
          CAST(NULL AS VARCHAR) strict_event_class,
          'consensus_band_hit_rate' strict_metric_name,
          feed_hits::VARCHAR strict_metric_numerator_raw,
          total_hits::VARCHAR strict_metric_denominator_raw,
          hit_rate_ppm strict_metric_rate_ppm
        FROM {source('flare_provider_feed_performance')}
    """
    atomic_parquet(connection, sample_b_query, sample_b)

    # The DVM reveal join operationalizes wrong-vote versus no-valid-reveal.
    # VoterSlashApplied is absent from accountability_events and cannot be
    # counted a second time here.
    sample_c_query = f"""
        WITH final_round_revealed AS (
          SELECT DISTINCT v.dvm_request_id, v.voter
          FROM {source('uma_dvm_votes_events')} v
          JOIN {source('uma_dvm_requests')} q
            ON v.dvm_request_id=q.dvm_request_id AND v.round_id=q.round_id
          WHERE v.revealed AND v.revealed_price_raw IS NOT NULL
        ),
        grade_a AS (
          SELECT DISTINCT oo_request_id
          FROM {source('uma_polygon_ethereum_grade_a_links')}
          WHERE cross_chain_match_grade='A'
            AND sample_tier='primary'
            AND try_cast(dvm_time AS BIGINT) >= {MAIN_START_UNIX}
        ),
        uma_dvm AS (
          SELECT
            a.*,
            'C' analysis_sample,
            '{FIXED_CUTOFF}' fixed_cutoff,
            'SAMPLE_C_UMA_DVM_SIGNED_VOTER_SLASH_V1' sample_rule_id,
            CASE
              WHEN a.outcome_status='DVM_CORRECT_VOTE_REDISTRIBUTION' THEN 'uma_correct_vote_redistribution'
              WHEN r.voter IS NOT NULL THEN 'uma_wrong_vote_slash'
              ELSE 'uma_no_valid_reveal_slash'
            END strict_event_class,
            CAST(NULL AS VARCHAR) strict_metric_name,
            CAST(NULL AS VARCHAR) strict_metric_numerator_raw,
            CAST(NULL AS VARCHAR) strict_metric_denominator_raw,
            CAST(NULL AS BIGINT) strict_metric_rate_ppm
          FROM {source('accountability_events')} a
          LEFT JOIN final_round_revealed r
            ON a.accountability_unit_id=r.dvm_request_id AND a.actor=r.voter
          WHERE a.native_table='uma_dvm_voter_payoffs'
            AND a.outcome_status IN ('DVM_CORRECT_VOTE_REDISTRIBUTION','DVM_NEGATIVE_SLASH')
        ),
        uma_oov2 AS (
          SELECT
            a.*,
            'C' analysis_sample,
            '{FIXED_CUTOFF}' fixed_cutoff,
            'SAMPLE_C_UMA_GRADE_A_PRIMARY_DISPUTE_V1' sample_rule_id,
            CASE
              WHEN a.outcome_status='settled_disputed_disputer_wins' THEN 'uma_dvm_confirmed_disputer_correct'
              ELSE 'uma_dvm_confirmed_proposer_correct'
            END strict_event_class,
            CAST(NULL AS VARCHAR) strict_metric_name,
            CAST(NULL AS VARCHAR) strict_metric_numerator_raw,
            CAST(NULL AS VARCHAR) strict_metric_denominator_raw,
            CAST(NULL AS BIGINT) strict_metric_rate_ppm
          FROM {source('accountability_events')} a
          JOIN grade_a g ON a.accountability_unit_id=g.oo_request_id
          WHERE a.native_table='polygon_uma_request_rounds'
            AND a.outcome_status LIKE 'settled_disputed_%'
        ),
        tellor AS (
          SELECT
            a.*,
            'C' analysis_sample,
            '{FIXED_CUTOFF}' fixed_cutoff,
            'SAMPLE_C_TELLOR_RESOLVED_DISPUTE_AND_REWARD_V1' sample_rule_id,
            CASE
              WHEN a.native_table='tellor_disputes' AND a.outcome_status LIKE '%SUPPORT' THEN 'tellor_report_supported'
              WHEN a.native_table='tellor_disputes' THEN 'tellor_report_rejected'
              WHEN a.native_table='tellor_jail_events' AND a.outcome_status='jailed_reporter' THEN 'tellor_reporter_jailed'
              WHEN a.native_table='tellor_jail_events' THEN 'tellor_reporter_unjailed'
              ELSE 'tellor_dispute_voter_reward_paid'
            END strict_event_class,
            CAST(NULL AS VARCHAR) strict_metric_name,
            CAST(NULL AS VARCHAR) strict_metric_numerator_raw,
            CAST(NULL AS VARCHAR) strict_metric_denominator_raw,
            CAST(NULL AS BIGINT) strict_metric_rate_ppm
          FROM {source('accountability_events')} a
          WHERE a.native_table='tellor_disputes'
             OR a.native_table='tellor_jail_events'
             OR (a.native_table='tellor_dispute_payments' AND a.reward_class='dispute_vote_reward')
        ),
        flare AS (
          SELECT
            '1.0.0' schema_version,
            sha256(concat_ws('|', 'flare_feed_epoch', reward_epoch_id::VARCHAR, voter_address, feed_name)) accountability_event_id,
            'Flare_FTSOv2' oracle_network,
            'continuous_consensus_band_reward' mechanism_family,
            'Flare_Mainnet' security_chain,
            'Flare_Mainnet' delivery_chain,
            'publisher_pool_epoch' accountability_unit_type,
            concat_ws(':', reward_epoch_id::VARCHAR, voter_address, feed_name) accountability_unit_id,
            'provider_feed_epoch_consensus_band_performance' event_granularity,
            CAST(NULL AS BIGINT) event_time_unix,
            voter_address actor,
            'data_provider' actor_role,
            CAST(NULL AS VARCHAR) reward_class,
            CAST(NULL AS VARCHAR) reward_amount_raw,
            CAST(NULL AS VARCHAR) penalty_class,
            CASE WHEN ftso_scaling_condition_met THEN NULL ELSE 'reward_ineligibility_condition' END nonmonetary_penalty,
            'consensus_median_alignment' truth_basis,
            CASE WHEN ftso_scaling_condition_met THEN 'consensus_band_condition_met' ELSE 'consensus_band_condition_not_met' END outcome_status,
            false external_truth_available,
            'official_reward_epoch_calculation_file' source_event,
            concat('reward-calculation-data:', source_file) source_tx,
            'FLARE_FTSO_PROVIDER_FEED_PERFORMANCE_V1' rule_id,
            'A' observability_grade,
            'A' confidence_grade,
            'flare_provider_feed_performance' native_table,
            'strict_honesty_linked_events' sample_tier,
            'Consensus-band alignment is protocol-recognized reliability, not external objective price truth.' interpretation_note,
            'C' analysis_sample,
            '{FIXED_CUTOFF}' fixed_cutoff,
            'SAMPLE_C_FLARE_FEED_CONSENSUS_ALIGNMENT_V1' sample_rule_id,
            CASE WHEN ftso_scaling_condition_met THEN 'flare_consensus_band_condition_met' ELSE 'flare_consensus_band_condition_not_met' END strict_event_class,
            'consensus_band_hit_rate' strict_metric_name,
            feed_hits::VARCHAR strict_metric_numerator_raw,
            total_hits::VARCHAR strict_metric_denominator_raw,
            hit_rate_ppm strict_metric_rate_ppm
          FROM {source('flare_provider_feed_performance')}
        )
        SELECT * FROM uma_dvm
        UNION ALL BY NAME SELECT * FROM uma_oov2
        UNION ALL BY NAME SELECT * FROM tellor
        UNION ALL BY NAME SELECT * FROM flare
    """
    atomic_parquet(connection, sample_c_query, sample_c)

    b_rows = connection.execute(f"SELECT count(*) FROM read_parquet('{sample_b.as_posix()}')").fetchone()[0]
    c_rows = connection.execute(f"SELECT count(*) FROM read_parquet('{sample_c.as_posix()}')").fetchone()[0]
    b_networks = dict(connection.execute(
        f"SELECT oracle_network, count(*) FROM read_parquet('{sample_b.as_posix()}') GROUP BY 1 ORDER BY 1"
    ).fetchall())
    c_classes = dict(connection.execute(
        f"SELECT strict_event_class, count(*) FROM read_parquet('{sample_c.as_posix()}') GROUP BY 1 ORDER BY 1"
    ).fetchall())
    c_networks = dict(connection.execute(
        f"SELECT oracle_network, count(*) FROM read_parquet('{sample_c.as_posix()}') GROUP BY 1 ORDER BY 1"
    ).fetchall())
    duplicate_b = connection.execute(
        f"SELECT count(*)-count(DISTINCT accountability_event_id) FROM read_parquet('{sample_b.as_posix()}')"
    ).fetchone()[0]
    duplicate_c = connection.execute(
        f"SELECT count(*)-count(DISTINCT accountability_event_id) FROM read_parquet('{sample_c.as_posix()}')"
    ).fetchone()[0]
    manifest: dict[str, object] = {
        "dataset": "Oracle Accountability Atlas fixed research samples",
        "schema_version": "1.0.0",
        "fixed_cutoff": FIXED_CUTOFF,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "sample_b": {
            "definition": "All observable accountability rows for the five deep-panel networks.",
            "path": str(sample_b),
            "sha256": sha256_file(sample_b),
            "rows": b_rows,
            "rows_by_network": b_networks,
            "duplicate_event_ids": duplicate_b,
        },
        "sample_c": {
            "definition": "Protocol-rule-linked correctness or reliability outcomes; not external objective truth.",
            "path": str(sample_c),
            "sha256": sha256_file(sample_c),
            "rows": c_rows,
            "rows_by_network": c_networks,
            "rows_by_strict_event_class": c_classes,
            "duplicate_event_ids": duplicate_c,
            "zero_row_networks": {
                "Chainlink": "No valid AlertRaised/operator Slashed event observed at the fixed cutoff.",
                "Pyth": "No adjudicated realized OIS data-quality slash observed in the durable rolling-state panel.",
            },
        },
        "assertions": {
            "sample_b_has_all_five_networks": set(b_networks) == set(NETWORKS),
            "sample_b_unique_event_ids": duplicate_b == 0,
            "sample_c_unique_event_ids": duplicate_c == 0,
            "sample_c_excludes_undisputed_acceptance": connection.execute(
                f"SELECT count(*)=0 FROM read_parquet('{sample_c.as_posix()}') WHERE truth_basis='undisputed_acceptance'"
            ).fetchone()[0],
            "sample_c_excludes_base_staking_reward": connection.execute(
                f"SELECT count(*)=0 FROM read_parquet('{sample_c.as_posix()}') WHERE reward_class LIKE 'base_staking%'"
            ).fetchone()[0],
            "sample_c_event_ids_are_subset_of_sample_b": connection.execute(f"""
                SELECT count(*)=0
                FROM read_parquet('{sample_c.as_posix()}') c
                ANTI JOIN read_parquet('{sample_b.as_posix()}') b USING (accountability_event_id)
            """).fetchone()[0],
        },
    }
    manifest["all_required_assertions_pass"] = all(manifest["assertions"].values())
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(manifest_path)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-dir", type=Path, default=ROOT / "data/curated/parquet")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data/curated/parquet")
    parser.add_argument("--manifest", type=Path, default=ROOT / "data/manifests/research_samples.json")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    result = build_samples(arguments.parquet_dir.resolve(), arguments.output_dir.resolve(), arguments.manifest.resolve())
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
