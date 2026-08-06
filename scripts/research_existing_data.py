"""Run the first empirical study supported by the QC-complete UMA and Chainlink ledgers.

The study is descriptive. It deliberately keeps request/vote accountability
(UMA) separate from service/staking accountability (Chainlink), uses integer or
Decimal monetary arithmetic, and never interprets an undisputed UMA proposal as
externally verified truth.
"""
from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
PARQUET = (ROOT / "data/curated/parquet").resolve()
ANALYSIS = ROOT / "data/analysis"
REPORTS = ROOT / "reports"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
getcontext().prec = 50


def table(name: str) -> str:
    return f"read_parquet('{PARQUET / (name + '.parquet')}')"


def one(connection: duckdb.DuckDBPyConnection, query: str) -> dict[str, Any]:
    cursor = connection.execute(query)
    columns = [item[0] for item in cursor.description]
    return dict(zip(columns, cursor.fetchone(), strict=True))


def rows(connection: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = connection.execute(query)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def decimal(value: Any) -> Decimal:
    return Decimal(0) if value is None else Decimal(str(value))


def token(raw: Any, decimals: int) -> str:
    value = decimal(raw) / (Decimal(10) ** decimals)
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def display(value: Any, places: int = 3) -> str:
    """Round values for the Markdown report while keeping JSON values exact."""
    rendered = f"{decimal(value):,.{places}f}"
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def percent(numerator: Any, denominator: Any, places: int = 3) -> str | None:
    denominator_decimal = decimal(denominator)
    if denominator_decimal == 0:
        return None
    value = decimal(numerator) * Decimal(100) / denominator_decimal
    return f"{value:.{places}f}%"


def ratio(numerator: Any, denominator: Any, places: int = 3) -> str | None:
    denominator_decimal = decimal(denominator)
    if denominator_decimal == 0:
        return None
    return f"{decimal(numerator) / denominator_decimal:.{places}f}"


def wilson(successes: int, total: int) -> list[str] | None:
    if total == 0:
        return None
    z = Decimal("1.959963984540054")
    n = Decimal(total); p = Decimal(successes) / n
    denominator = Decimal(1) + z * z / n
    center = (p + z * z / (Decimal(2) * n)) / denominator
    margin = z * ((p * (Decimal(1) - p) / n + z * z / (Decimal(4) * n * n)).sqrt()) / denominator
    return [f"{(center - margin) * 100:.3f}%", f"{(center + margin) * 100:.3f}%"]


def concentration(values: list[tuple[int, Decimal]]) -> dict[str, Any]:
    by_count = sorted(values, key=lambda item: item[0], reverse=True)
    by_amount = sorted(values, key=lambda item: item[1], reverse=True)
    count_total = sum(item[0] for item in values)
    amount_total = sum((item[1] for item in values), Decimal(0))

    def share(items: list[tuple[int, Decimal]], limit: int, position: int, total: Decimal) -> str | None:
        if total == 0:
            return None
        numerator = sum((Decimal(str(item[position])) for item in items[:limit]), Decimal(0))
        return f"{numerator * 100 / total:.3f}%"

    return {
        "unique_actors": len(values),
        "round_or_event_share_top1": share(by_count, 1, 0, Decimal(count_total)),
        "round_or_event_share_top5": share(by_count, 5, 0, Decimal(count_total)),
        "round_or_event_share_top10": share(by_count, 10, 0, Decimal(count_total)),
        "amount_share_top1": share(by_amount, 1, 1, amount_total),
        "amount_share_top10": share(by_amount, 10, 1, amount_total),
        "amount_share_top100": share(by_amount, 100, 1, amount_total),
    }


def main() -> None:
    required = [
        "polygon_uma_request_rounds", "uma_polygon_ethereum_grade_a_links",
        "uma_dvm_votes_events", "uma_dvm_voter_payoffs",
        "chainlink_staking_v02_events", "chainlink_event_link_flow_qc",
        "chainlink_eth_usd_reports", "polygon_uma_gamma_links",
    ]
    missing = [name for name in required if not (PARQUET / f"{name}.parquet").is_file()]
    if missing:
        raise RuntimeError(f"missing Parquet inputs: {missing}")

    connection = duckdb.connect()
    rounds_table = table("polygon_uma_request_rounds")
    payoffs_table = table("uma_dvm_voter_payoffs")
    votes_table = table("uma_dvm_votes_events")
    links_table = table("uma_polygon_ethereum_grade_a_links")
    chain_events_table = table("chainlink_staking_v02_events")
    chain_qc_table = table("chainlink_event_link_flow_qc")
    feed_table = table("chainlink_eth_usd_reports")

    uma_counts = one(connection, f"""
        SELECT
          count(*) total_rounds,
          sum(sample_tier = 'primary') primary_rounds,
          sum(sample_tier = 'primary' AND status = 'settled') primary_settled,
          sum(sample_tier = 'primary' AND status = 'requested') primary_requested,
          sum(sample_tier = 'primary' AND status = 'disputed') primary_open_dispute,
          sum(sample_tier = 'primary' AND economic_status = 'settled_undisputed') primary_undisputed,
          sum(sample_tier = 'primary' AND economic_status = 'settled_disputed_proposer_wins') primary_proposer_wins,
          sum(sample_tier = 'primary' AND economic_status = 'settled_disputed_disputer_wins') primary_disputer_wins
        FROM {rounds_table}
    """)
    disputed = int(uma_counts["primary_proposer_wins"] + uma_counts["primary_disputer_wins"])
    uma_counts["primary_disputed_settled"] = disputed
    uma_counts["primary_dispute_rate"] = percent(disputed, uma_counts["primary_settled"])
    uma_counts["primary_dispute_rate_wilson_95"] = wilson(disputed, int(uma_counts["primary_settled"]))
    uma_counts["disputer_success_rate"] = percent(uma_counts["primary_disputer_wins"], disputed)
    uma_counts["disputer_success_rate_wilson_95"] = wilson(int(uma_counts["primary_disputer_wins"]), disputed)

    uma_raw = one(connection, f"""
        SELECT
          sum(cast(explicit_report_reward_raw AS DECIMAL(38,0))) explicit_report_reward_raw,
          sum(cast(reward_refunded_or_rolled_raw AS DECIMAL(38,0))) reward_refunded_or_rolled_raw,
          sum(cast(dispute_winner_reward_raw AS DECIMAL(38,0))) dispute_winner_reward_raw,
          sum(cast(bond_forfeited_raw AS DECIMAL(38,0))) bond_forfeited_raw,
          sum(cast(final_fee_forfeited_raw AS DECIMAL(38,0))) final_fee_forfeited_raw,
          sum(cast(protocol_fee_raw AS DECIMAL(38,0))) protocol_fee_raw,
          median(cast(reward_raw AS DECIMAL(38,0))) median_reward_raw,
          median(cast(effective_bond_raw AS DECIMAL(38,0))) median_bond_raw
        FROM {rounds_table}
        WHERE sample_tier = 'primary' AND status = 'settled'
    """)
    uma_economics = {key: str(value) for key, value in uma_raw.items() if key.endswith("_raw")}
    for key, value in uma_raw.items():
        if key.endswith("_raw"):
            uma_economics[key.removesuffix("_raw") + "_usdc_units"] = token(value, 6)
    total_penalty = decimal(uma_raw["bond_forfeited_raw"]) + decimal(uma_raw["final_fee_forfeited_raw"])
    uma_economics["loser_penalties_raw"] = str(total_penalty)
    uma_economics["loser_penalties_usdc_units"] = token(total_penalty, 6)
    uma_economics["dispute_reward_to_routine_reward_ratio"] = ratio(
        uma_raw["dispute_winner_reward_raw"], uma_raw["explicit_report_reward_raw"]
    )
    uma_economics["mean_routine_reward_per_undisputed_usdc_units"] = token(
        decimal(uma_raw["explicit_report_reward_raw"]) / decimal(uma_counts["primary_undisputed"]), 6
    )
    uma_economics["mean_winner_reward_per_dispute_usdc_units"] = token(
        decimal(uma_raw["dispute_winner_reward_raw"]) / decimal(disputed), 6
    )
    uma_economics["mean_loser_penalty_per_dispute_usdc_units"] = token(
        total_penalty / decimal(disputed), 6
    )

    adapter_rows = rows(connection, f"""
        SELECT adapter_version, count(*) settled,
          sum(disputer IS NOT NULL AND lower(disputer) <> '{ZERO_ADDRESS}') disputed,
          sum(economic_status = 'settled_disputed_disputer_wins') disputer_wins
        FROM {rounds_table}
        WHERE sample_tier = 'primary' AND status = 'settled'
        GROUP BY adapter_version ORDER BY adapter_version
    """)
    for row in adapter_rows:
        row["dispute_rate"] = percent(row["disputed"], row["settled"])
        row["dispute_rate_wilson_95"] = wilson(int(row["disputed"]), int(row["settled"]))
    if len(adapter_rows) == 2:
        rates = {row["adapter_version"]: decimal(row["disputed"]) / decimal(row["settled"]) for row in adapter_rows}
        adapter_rate_ratio = ratio(rates.get("adapter_v2_0"), rates.get("adapter_v3_current"))
    else:
        adapter_rate_ratio = None

    reward_presence = rows(connection, f"""
        SELECT cast(reward_raw AS DECIMAL(38,0)) > 0 reward_positive,
          count(*) settled,
          sum(disputer IS NOT NULL AND lower(disputer) <> '{ZERO_ADDRESS}') disputed,
          median(cast(reward_raw AS DECIMAL(38,0))) median_reward_raw,
          median(cast(effective_bond_raw AS DECIMAL(38,0))) median_bond_raw
        FROM {rounds_table}
        WHERE sample_tier = 'primary' AND status = 'settled'
        GROUP BY reward_positive ORDER BY reward_positive
    """)
    for row in reward_presence:
        row["dispute_rate"] = percent(row["disputed"], row["settled"])
        row["dispute_rate_wilson_95"] = wilson(int(row["disputed"]), int(row["settled"]))
        row["median_reward_usdc_units"] = token(row.pop("median_reward_raw"), 6)
        row["median_bond_usdc_units"] = token(row.pop("median_bond_raw"), 6)

    yearly = rows(connection, f"""
        SELECT year(to_timestamp(block_time)) calendar_year, count(*) settled,
          sum(disputer IS NOT NULL AND lower(disputer) <> '{ZERO_ADDRESS}') disputed,
          sum(economic_status = 'settled_disputed_disputer_wins') disputer_wins,
          sum(cast(explicit_report_reward_raw AS DECIMAL(38,0))) explicit_reward_raw
        FROM {rounds_table}
        WHERE sample_tier = 'primary' AND status = 'settled'
        GROUP BY calendar_year ORDER BY calendar_year
    """)
    for row in yearly:
        row["dispute_rate"] = percent(row["disputed"], row["settled"])
        row["explicit_reward_usdc_units"] = token(row.pop("explicit_reward_raw"), 6)

    proposer_rows = connection.execute(f"""
        SELECT count(*) n,
          sum(cast(explicit_report_reward_raw AS DECIMAL(38,0)) +
            CASE WHEN economic_status = 'settled_disputed_proposer_wins'
                 THEN cast(dispute_winner_reward_raw AS DECIMAL(38,0)) ELSE 0 END) amount
        FROM {rounds_table}
        WHERE sample_tier = 'primary' AND status = 'settled'
        GROUP BY proposer
    """).fetchall()
    disputer_rows = connection.execute(f"""
        SELECT count(*) n,
          sum(CASE WHEN economic_status = 'settled_disputed_disputer_wins'
                   THEN cast(dispute_winner_reward_raw AS DECIMAL(38,0)) ELSE 0 END) amount
        FROM {rounds_table}
        WHERE sample_tier = 'primary' AND status = 'settled'
          AND disputer IS NOT NULL AND lower(disputer) <> '{ZERO_ADDRESS}'
        GROUP BY disputer
    """).fetchall()

    dvm_scope = one(connection, f"""
        SELECT count(DISTINCT dvm_request_id) requests, count(DISTINCT voter) voters, count(*) events,
          sum(cast(correct_vote_redistribution_raw AS DECIMAL(38,0))) positive_raw,
          sum(cast(wrong_or_no_vote_slash_raw AS DECIMAL(38,0))) negative_raw
        FROM {payoffs_table}
    """)
    dvm_linked = one(connection, f"""
        WITH linked AS (
          SELECT DISTINCT dvm_request_id FROM {links_table} WHERE cross_chain_match_grade = 'A'
        )
        SELECT count(DISTINCT p.dvm_request_id) requests, count(DISTINCT voter) voters, count(*) events,
          sum(cast(correct_vote_redistribution_raw AS DECIMAL(38,0))) positive_raw,
          sum(cast(wrong_or_no_vote_slash_raw AS DECIMAL(38,0))) negative_raw
        FROM {payoffs_table} p JOIN linked USING (dvm_request_id)
    """)

    def dvm_render(scope: dict[str, Any]) -> dict[str, Any]:
        positive = decimal(scope["positive_raw"]); negative = decimal(scope["negative_raw"])
        return {
            "requests": scope["requests"], "voters": scope["voters"], "events": scope["events"],
            "positive_redistribution_raw": str(positive),
            "positive_redistribution_uma": token(positive, 18),
            "negative_slash_raw": str(negative), "negative_slash_uma": token(negative, 18),
            "observed_signed_gap_raw": str(positive - negative),
            "observed_signed_gap_uma": token(positive - negative, 18),
        }

    dvm_negative = rows(connection, f"""
        WITH reveals AS (
          SELECT DISTINCT dvm_request_id, voter FROM {votes_table} WHERE revealed
        ), negative AS (
          SELECT p.*, r.voter IS NOT NULL had_reveal
          FROM {payoffs_table} p LEFT JOIN reveals r USING (dvm_request_id, voter)
          WHERE classification_rule_id = 'DVM_NEGATIVE_SLASH'
        )
        SELECT had_reveal, count(*) events, count(DISTINCT voter) voters,
          sum(cast(wrong_or_no_vote_slash_raw AS DECIMAL(38,0))) penalty_raw
        FROM negative GROUP BY had_reveal ORDER BY had_reveal
    """)
    negative_total_events = sum(int(row["events"]) for row in dvm_negative)
    negative_total_amount = sum((decimal(row["penalty_raw"]) for row in dvm_negative), Decimal(0))
    for row in dvm_negative:
        row["participation_proxy"] = "wrong_vote_with_reveal" if row.pop("had_reveal") else "no_valid_reveal"
        row["event_share"] = percent(row["events"], negative_total_events)
        row["penalty_raw"] = str(row["penalty_raw"])
        row["penalty_uma"] = token(row["penalty_raw"], 18)
        row["penalty_share"] = percent(row["penalty_raw"], negative_total_amount)

    voter_rows = connection.execute(f"""
        SELECT count(*) n,
          sum(cast(correct_vote_redistribution_raw AS DECIMAL(38,0))) positive,
          sum(cast(wrong_or_no_vote_slash_raw AS DECIMAL(38,0))) negative
        FROM {payoffs_table} GROUP BY voter
    """).fetchall()
    voter_positive_concentration = concentration([(int(row[0]), decimal(row[1])) for row in voter_rows])
    voter_negative_concentration = concentration([(int(row[0]), decimal(row[2])) for row in voter_rows])

    chain_counts = {row["event"]: row["n"] for row in rows(connection, f"SELECT event, count(*) n FROM {chain_events_table} GROUP BY event")}
    chain_amounts = one(connection, f"""
        SELECT
          sum(CASE WHEN event = 'Staked' THEN cast(amount_raw AS DECIMAL(38,0)) ELSE 0 END) staked_raw,
          sum(CASE WHEN event = 'Unstaked' THEN cast(amount_raw AS DECIMAL(38,0)) ELSE 0 END) unstaked_raw,
          sum(CASE WHEN event = 'RewardClaimed' THEN cast(reward_claimed_raw AS DECIMAL(38,0)) ELSE 0 END) claimed_raw,
          count(DISTINCT CASE WHEN event IN ('Staked','Unstaked') THEN staker END) stakers,
          count(DISTINCT CASE WHEN event = 'RewardClaimed' THEN staker END) claimants
        FROM {chain_events_table}
    """)
    chain_claims = one(connection, f"""
        SELECT count(*) claims,
          median(cast(reward_claimed_raw AS DECIMAL(38,0))) median_raw,
          quantile_cont(cast(reward_claimed_raw AS DECIMAL(38,0)), 0.9) p90_raw,
          quantile_cont(cast(reward_claimed_raw AS DECIMAL(38,0)), 0.99) p99_raw
        FROM {chain_events_table} WHERE event = 'RewardClaimed'
    """)
    claimant_rows = connection.execute(f"""
        SELECT count(*) n, sum(cast(reward_claimed_raw AS DECIMAL(38,0))) amount
        FROM {chain_events_table} WHERE event = 'RewardClaimed' GROUP BY staker
    """).fetchall()
    stake_routes = rows(connection, f"""
        SELECT flow_route, count(*) events,
          sum(cast(expected_amount_raw AS DECIMAL(38,0))) amount_raw
        FROM {chain_qc_table} WHERE event = 'Staked'
        GROUP BY flow_route ORDER BY events DESC
    """)
    for row in stake_routes:
        row["amount_link"] = token(row["amount_raw"], 18); row["amount_raw"] = str(row["amount_raw"])
    forfeiture = one(connection, f"""
        SELECT count(*) events,
          sum(cast(vested_reward_raw AS DECIMAL(38,0))) vested_raw,
          sum(cast(reclaimed_reward_raw AS DECIMAL(38,0))) reclaimed_raw,
          sum(CASE WHEN operator_reward THEN 1 ELSE 0 END) operator_events
        FROM {chain_qc_table} WHERE event = 'ForfeitedRewardDistributed'
    """)
    finalized = {str(row["reward_forfeited"]).lower(): row["n"] for row in rows(
        connection, f"SELECT reward_forfeited, count(*) n FROM {chain_events_table} WHERE event = 'RewardFinalized' GROUP BY 1"
    )}
    feed_gap = one(connection, f"""
        WITH times AS (
          SELECT DISTINCT cast(updated_at AS BIGINT) update_time
          FROM {feed_table} WHERE event = 'AnswerUpdated'
        ), gaps AS (
          SELECT update_time - lag(update_time) OVER (ORDER BY update_time) gap FROM times
        )
        SELECT count(gap) intervals, median(gap) p50_seconds,
          quantile_cont(gap, 0.9) p90_seconds, quantile_cont(gap, 0.99) p99_seconds,
          max(gap) max_seconds, sum(gap > 1800) over_30m,
          sum(gap > 3600) over_1h, sum(gap > 10800) over_3h
        FROM gaps WHERE gap IS NOT NULL
    """)
    feed_by_aggregator = rows(connection, f"""
        SELECT aggregator, count(*) updates, min(cast(updated_at AS BIGINT)) first_timestamp,
          max(cast(updated_at AS BIGINT)) last_timestamp
        FROM {feed_table} WHERE event = 'AnswerUpdated'
        GROUP BY aggregator ORDER BY first_timestamp
    """)
    feed_config = rows(connection, f"""
        SELECT source_block, feed, threshold_1_seconds, threshold_2_seconds,
          operator_slash_amount_raw, alerter_reward_amount_raw, configuration_version
        FROM {chain_events_table} WHERE event = 'FeedConfigSet'
    """)

    chain_manifest = json.loads((ROOT / "data/manifests/chainlink_evidence_ledger.json").read_text(encoding="utf-8"))
    gamma_manifest = json.loads((ROOT / "data/manifests/polymarket_gamma.json").read_text(encoding="utf-8"))
    universe_manifest = json.loads((ROOT / "data/manifests/oracle_universe_registry.json").read_text(encoding="utf-8"))
    gamma_comparisons = gamma_manifest["metadata_onchain_comparisons"]
    comparison_denominator = gamma_comparisons["reward_match"] + gamma_comparisons["reward_mismatch"]

    chainlink = {
        "accountability_unit": "service_availability_window_and_staker",
        "event_counts": chain_counts,
        "participants": {"unique_stakers": chain_amounts["stakers"], "unique_reward_claimants": chain_amounts["claimants"]},
        "gross_flows_not_balances": {
            "staked_raw": str(chain_amounts["staked_raw"]), "staked_link": token(chain_amounts["staked_raw"], 18),
            "unstaked_raw": str(chain_amounts["unstaked_raw"]), "unstaked_link": token(chain_amounts["unstaked_raw"], 18),
            "reward_claimed_raw": str(chain_amounts["claimed_raw"]), "reward_claimed_link": token(chain_amounts["claimed_raw"], 18),
        },
        "reward_claims": {
            "claims": chain_claims["claims"], "median_link": token(chain_claims["median_raw"], 18),
            "p90_link": token(chain_claims["p90_raw"], 18), "p99_link": token(chain_claims["p99_raw"], 18),
            "concentration": concentration([(int(row[0]), decimal(row[1])) for row in claimant_rows]),
        },
        "stake_flow_routes": stake_routes,
        "forfeiture": {
            "events": forfeiture["events"], "vested_reward_forfeited_raw": str(forfeiture["vested_raw"]),
            "vested_reward_forfeited_link": token(forfeiture["vested_raw"], 18),
            "reclaimed_reward_raw": str(forfeiture["reclaimed_raw"]), "reclaimed_reward_link": token(forfeiture["reclaimed_raw"], 18),
            "operator_reward_events": forfeiture["operator_events"], "reward_finalized_counts": finalized,
            "interpretation": "accounting redistribution; not a principal slash or an ERC-20 transfer",
        },
        "link_flow_qc": chain_manifest["event_link_flow_qc"],
        "feed_service_window": {
            "active_phase_events": chain_manifest["feed_events"],
            "excluded_outside_active_phase": chain_manifest["feed_events_excluded_outside_active_phase"],
            "phase_intervals": chain_manifest["feed_phase_intervals"],
            "gap_statistics": feed_gap, "by_aggregator": feed_by_aggregator, "configuration": feed_config,
            "observed_alert_events": chain_counts.get("AlertRaised", 0),
            "observed_slash_events": chain_counts.get("Slashed", 0),
        },
    }

    summary = {
        "analysis_version": "0.2.0",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff_utc": "2026-06-30T23:59:59Z",
        "study_design": {
            "type": "descriptive_event_level_accountability_study",
            "causal_claims": False,
            "strict_comparison_guard": "UMA request/vote rewards are not pooled with Chainlink service/staking rewards",
        },
        "uma_oov2_primary": {
            "accountability_unit": "request_and_dispute",
            "truth_guard": "undisputed acceptance is protocol acceptance, not external ground truth",
            "counts": uma_counts, "economics": uma_economics,
            "adapter_cohorts": adapter_rows, "v2_to_v3_dispute_rate_ratio": adapter_rate_ratio,
            "reward_presence_cohorts": reward_presence, "yearly": yearly,
            "proposer_concentration": concentration([(int(row[0]), decimal(row[1])) for row in proposer_rows]),
            "disputer_concentration": concentration([(int(row[0]), decimal(row[1])) for row in disputer_rows]),
        },
        "uma_dvm": {
            "accountability_unit": "voting_request_and_voter",
            "all_resolved_requests": dvm_render(dvm_scope),
            "grade_a_polymarket_linked": dvm_render(dvm_linked),
            "grade_a_share_of_positive_amount": percent(dvm_linked["positive_raw"], dvm_scope["positive_raw"]),
            "grade_a_share_of_negative_amount": percent(dvm_linked["negative_raw"], dvm_scope["negative_raw"]),
            "negative_event_participation_proxy": dvm_negative,
            "positive_voter_concentration": voter_positive_concentration,
            "negative_voter_concentration": voter_negative_concentration,
            "realization_guard": "VoterSlashed only; VoterSlashApplied is excluded from payoff totals",
        },
        "chainlink": chainlink,
        "metadata_and_universe": {
            "oracle_categories": universe_manifest["oracle_categories"],
            "protocol_oracle_assignments": universe_manifest["protocol_oracle_assignments"],
            "gamma_grade_a_links": gamma_manifest["uma_round_gamma_link_grades"]["A"],
            "gamma_unresolved_links": gamma_manifest["uma_round_gamma_link_grades"]["U"],
            "gamma_primary_grade_a": gamma_manifest["uma_round_gamma_grades_by_sample"]["primary:A"],
            "gamma_primary_unresolved": gamma_manifest["uma_round_gamma_grades_by_sample"]["primary:U"],
            "gamma_reward_comparison": gamma_comparisons,
            "gamma_nonmissing_match_rate": percent(gamma_comparisons["reward_match"], comparison_denominator),
            "guard": "Gamma is mutable auxiliary metadata and never overrides on-chain values",
        },
        "qc_assertions": {},
    }

    qc = summary["qc_assertions"]
    qc["primary_outcomes_partition"] = int(uma_counts["primary_settled"]) == int(uma_counts["primary_undisputed"]) + disputed
    qc["dispute_penalty_allocation_identity"] = total_penalty == decimal(uma_raw["dispute_winner_reward_raw"]) + decimal(uma_raw["protocol_fee_raw"])
    qc["chainlink_cash_flow_mismatches"] = sum(
        value for key, value in chain_manifest["event_link_flow_qc"].items() if key.endswith("_mismatch")
    )
    qc["forfeiture_is_accounting_only"] = chain_manifest["event_link_flow_qc"].get("ForfeitedRewardDistributed_accounting_only") == forfeiture["events"]
    threshold = int(feed_config[0]["threshold_1_seconds"]) if feed_config else None
    qc["max_feed_gap_below_primary_alert_threshold"] = threshold is not None and int(feed_gap["max_seconds"]) < threshold
    qc["all_required_assertions_pass"] = all(value is True or value == 0 for key, value in qc.items() if key != "all_required_assertions_pass")
    if not qc["all_required_assertions_pass"]:
        raise RuntimeError(f"research QC failed: {qc}")

    ANALYSIS.mkdir(parents=True, exist_ok=True); REPORTS.mkdir(parents=True, exist_ok=True)
    summary_path = ANALYSIS / "existing_data_research.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    yearly_path = ANALYSIS / "uma_primary_yearly.csv"
    with yearly_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(yearly[0]) if yearly else [])
        if yearly:
            writer.writeheader(); writer.writerows(yearly)

    comparison_path = ANALYSIS / "mechanism_comparison.csv"
    comparison_rows = [
        {"dimension": "accountability_unit", "UMA": "request / dispute / voting_request", "Chainlink": "service_availability_window / staker", "strictly_comparable": "no"},
        {"dimension": "truth_basis", "UMA": "undisputed_acceptance or DVM vote", "Chainlink": "valid-report availability condition", "strictly_comparable": "no"},
        {"dimension": "observed_positive_outcome", "UMA": "explicit report reward / dispute reward / correct-vote redistribution", "Chainlink": "staking reward claim", "strictly_comparable": "no"},
        {"dimension": "observed_negative_outcome", "UMA": "bond+fee forfeiture / voter slash", "Chainlink": "locked-reward forfeiture; no observed principal slash", "strictly_comparable": "taxonomy only"},
        {"dimension": "cash_flow_reconciliation", "UMA": "settled OOV2 41826/41826 exact", "Chainlink": "stake/unstake/reward 50772/50772 exact", "strictly_comparable": "QC only"},
    ]
    with comparison_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(comparison_rows[0])); writer.writeheader(); writer.writerows(comparison_rows)

    report = render_report(summary)
    report_path = REPORTS / "existing_data_research.md"
    report_path.write_text(report, encoding="utf-8")
    print(summary_path); print(yearly_path); print(comparison_path); print(report_path)


def render_report(summary: dict[str, Any]) -> str:
    oo = summary["uma_oov2_primary"]; counts = oo["counts"]; economics = oo["economics"]
    dvm = summary["uma_dvm"]; chain = summary["chainlink"]; metadata = summary["metadata_and_universe"]
    no_reveal = next(row for row in dvm["negative_event_participation_proxy"] if row["participation_proxy"] == "no_valid_reveal")
    direct = next(row for row in chain["stake_flow_routes"] if row["flow_route"] == "direct_staker_to_pool")
    mediated = next(row for row in chain["stake_flow_routes"] if row["flow_route"] == "contract_mediated_to_pool")
    feed = chain["feed_service_window"]; gaps = feed["gap_statistics"]; config = feed["configuration"][0]
    cohorts = {row["adapter_version"]: row for row in oo["adapter_cohorts"]}
    v2 = cohorts.get("adapter_v2_0", {}); v3 = cohorts.get("adapter_v3_current", {})
    yearly_lines = [
        f"| {row['calendar_year']} | {row['settled']} | {row['disputed']} | {row['dispute_rate']} | {row['disputer_wins']} | {display(row['explicit_reward_usdc_units'])} |"
        for row in oo["yearly"]
    ]
    lines = [
        "# 现有数据第一阶段实证研究",
        "",
        f"生成时间：{summary['generated_at_utc']}  ",
        "固定截止：2026-06-30 23:59:59 UTC  ",
        "研究性质：事件级描述性研究；不作因果识别。",
        "",
        "## 摘要",
        "",
        f"UMA 主样本有 {counts['primary_settled']} 个已结算 request，其中 {counts['primary_undisputed']} 个未争议、{counts['primary_disputed_settled']} 个有争议，争议率 {counts['primary_dispute_rate']}（Wilson 95% CI {counts['primary_dispute_rate_wilson_95'][0]}–{counts['primary_dispute_rate_wilson_95'][1]}）。",
        f"虽然争议只占约 2%，争议胜方奖励为 {display(economics['dispute_winner_reward_usdc_units'])} USDC units，高于全部未争议显式报告奖励 {display(economics['explicit_report_reward_usdc_units'])} USDC units；争议是低频但高金额的问责尾部。",
        f"DVM 的 {display(dvm['all_resolved_requests']['negative_slash_uma'])} UMA 负向 slash 中，{no_reveal['penalty_share']} 的金额和 {no_reveal['event_share']} 的事件没有有效 reveal 记录，说明已实现惩罚更多关联参与失败，而不是已 reveal 后的错误价格。",
        f"Chainlink 窗口内最大 ETH/USD 更新间隔为 {gaps['max_seconds']} 秒，低于链上配置阈值 {config['threshold_1_seconds']} 秒；没有观察到 AlertRaised 或 Slashed。因此本窗口支持 service continuity 描述，但不提供 alert/slash 因果样本。",
        "",
        "## 1. 研究设计与不可跨越的边界",
        "",
        "- UMA 的观察单位是 request、dispute 与 voting request；Chainlink 的观察单位是 service window 与 staker。两者不合并计算“每报告奖励”。",
        "- `accepted_undisputed` 只表示协议接受，不表示经过外部真值验证。只有 Grade-A 跨链连接的争议才能用于 DVM 裁决归因。",
        "- Chainlink `RewardClaimed` 是 staking/service incentive，不是逐次报价正确奖励；forfeiture 是 locked reward 的账务重分配，不是 principal slash。",
        "- 所有金额均用原始整数或 Decimal 计算。Polygon 两种 USDC 资产均为 6 decimals，但报告称为 USDC units，不假定始终等于一美元。",
        "",
        "## 2. UMA OOV2：低频争议承载高金额问责",
        "",
        "| 指标 | 结果 |",
        "|---|---:|",
        f"| Primary rounds | {counts['primary_rounds']} |",
        f"| Primary settled | {counts['primary_settled']} |",
        f"| 未争议结算 | {counts['primary_undisputed']} |",
        f"| 有争议结算 | {counts['primary_disputed_settled']} |",
        f"| 争议率 | {counts['primary_dispute_rate']} |",
        f"| Proposer 胜 | {counts['primary_proposer_wins']} |",
        f"| Disputer 胜 | {counts['primary_disputer_wins']} |",
        f"| Disputer 胜率 | {counts['disputer_success_rate']} |",
        f"| 显式报告奖励 | {display(economics['explicit_report_reward_usdc_units'])} USDC units |",
        f"| 争议胜方奖励 | {display(economics['dispute_winner_reward_usdc_units'])} USDC units |",
        f"| 败方 bond+final fee 损失 | {display(economics['loser_penalties_usdc_units'])} USDC units |",
        f"| Protocol fee | {display(economics['protocol_fee_usdc_units'])} USDC units |",
        f"| 争议 refund/roll-forward reward | {display(economics['reward_refunded_or_rolled_usdc_units'])} USDC units |",
        "",
        f"每个未争议 request 的平均显式奖励约 {display(economics['mean_routine_reward_per_undisputed_usdc_units'])} USDC units；每个争议的平均胜方奖励约 {display(economics['mean_winner_reward_per_dispute_usdc_units'])}，平均败方损失约 {display(economics['mean_loser_penalty_per_dispute_usdc_units'])}。败方损失严格分解为胜方奖励与 protocol fee，QC 恒等式通过。",
        "",
        "### 描述性异质性",
        "",
        f"Adapter v2 已结算 {v2.get('settled')} 个，争议率 {v2.get('dispute_rate')}；当前 v3 已结算 {v3.get('settled')} 个，争议率 {v3.get('dispute_rate')}；比率约 {oo['v2_to_v3_dispute_rate_ratio']}。这是不同时间、市场和参与者构成的 cohort 差异，不能解释为版本升级的因果效果。",
        f"Proposer 共 {oo['proposer_concentration']['unique_actors']} 个，头部 1 个地址占 {oo['proposer_concentration']['round_or_event_share_top1']} rounds，头部 10 个占 {oo['proposer_concentration']['round_or_event_share_top10']}；disputer 共 {oo['disputer_concentration']['unique_actors']} 个，头部 10 个占 {oo['disputer_concentration']['round_or_event_share_top10']} disputes。",
        "",
        "### 时间分布",
        "",
        "| 年份 | Settled | Disputed | 争议率 | Disputer 胜 | 显式报告奖励（USDC units） |",
        "|---:|---:|---:|---:|---:|---:|",
        *yearly_lines,
        "",
        "2026 年只有 11 个已结算观察，36.364% 的争议率不能与完整年度直接比较。各年度也未控制 adapter、市场或参与者构成，因此这里只报告描述性变化。",
        "",
        "## 3. UMA DVM：参与失败是负向事件的主要代理类别",
        "",
        "| 范围 | Requests | Payoff events | 正向再分配 | 负向 slash |",
        "|---|---:|---:|---:|---:|",
        f"| 全部 VotingV2 已解析请求 | {dvm['all_resolved_requests']['requests']} | {dvm['all_resolved_requests']['events']} | {display(dvm['all_resolved_requests']['positive_redistribution_uma'])} UMA | {display(dvm['all_resolved_requests']['negative_slash_uma'])} UMA |",
        f"| Grade-A Polymarket 连接 | {dvm['grade_a_polymarket_linked']['requests']} | {dvm['grade_a_polymarket_linked']['events']} | {display(dvm['grade_a_polymarket_linked']['positive_redistribution_uma'])} UMA | {display(dvm['grade_a_polymarket_linked']['negative_slash_uma'])} UMA |",
        "",
        f"Grade-A Polymarket 请求贡献了已观察正向再分配金额的 {dvm['grade_a_share_of_positive_amount']} 和负向 slash 金额的 {dvm['grade_a_share_of_negative_amount']}。这说明 Polymarket 是本 DVM 样本的重要组成，但不能据此推断其占 UMA 全生态经济价值的比例。",
        f"负向 slash 中，无有效 reveal 代理占 {no_reveal['event_share']} 事件和 {no_reveal['penalty_share']} 金额；有 reveal 但未匹配最终价格的记录才作为 wrong-vote proxy。这里的 no-vote/wrong-vote 是事件存在性代理，而非完整反事实 stake replay。",
        f"正向再分配金额头部 10 个 voter 占 {dvm['positive_voter_concentration']['amount_share_top10']}；负向金额头部 10 个占 {dvm['negative_voter_concentration']['amount_share_top10']}。`VoterSlashApplied` 没有再次加入这些金额。",
        "",
        "## 4. Chainlink：可观察的是 service/staking 问责，不是逐报告真值奖励",
        "",
        "| 指标 | 结果 |",
        "|---|---:|",
        f"| Unique stakers | {chain['participants']['unique_stakers']} |",
        f"| Reward claimants | {chain['participants']['unique_reward_claimants']} |",
        f"| Gross Staked flow | {display(chain['gross_flows_not_balances']['staked_link'])} LINK |",
        f"| Gross Unstaked flow | {display(chain['gross_flows_not_balances']['unstaked_link'])} LINK |",
        f"| RewardClaimed flow | {display(chain['gross_flows_not_balances']['reward_claimed_link'])} LINK |",
        f"| Reward claim median / p90 / p99 | {display(chain['reward_claims']['median_link'])} / {display(chain['reward_claims']['p90_link'])} / {display(chain['reward_claims']['p99_link'])} LINK |",
        f"| Forfeiture accounting events | {chain['forfeiture']['events']} |",
        f"| Vested reward redistributed | {display(chain['forfeiture']['vested_reward_forfeited_link'])} LINK |",
        f"| Active-phase AnswerUpdated | {feed['active_phase_events'].get('AnswerUpdated', 0)} |",
        f"| Active-phase NewTransmission | {feed['active_phase_events'].get('NewTransmission', 0)} |",
        f"| Maximum update gap | {gaps['max_seconds']} seconds |",
        f"| Gaps over 3 hours | {gaps['over_3h']} |",
        "",
        f"Staked 中 {direct['events']} 条、{display(direct['amount_link'])} LINK 是 staker 直接入池；{mediated['events']} 条、{display(mediated['amount_link'])} LINK 经固定中介合约进入 v0.2 pool。两条路径都已与事件金额精确对账。Stake/unstake 是期间 gross flow，不是截止日余额。",
        f"奖励领取者头部 10 个地址占领取金额 {chain['reward_claims']['concentration']['amount_share_top10']}、头部 100 个占 {chain['reward_claims']['concentration']['amount_share_top100']}。相比 UMA proposer rounds 的头部集中度，Chainlink claim 金额分布更分散，但因参与角色和经济单位不同，只能作结构描述。",
        f"Feed 配置记录给出阈值 {config['threshold_1_seconds']} / {config['threshold_2_seconds']} 秒、operator slash {token(config['operator_slash_amount_raw'], 18)} LINK、alerter reward {token(config['alerter_reward_amount_raw'], 18)} LINK。观察窗口没有跨过主要阈值，也没有 AlertRaised/Slashed 事件；504 条非 active-phase 的预发布/旧 phase 日志已从 service-window 统计剔除。",
        "",
        "## 5. 元数据与生态覆盖",
        "",
        f"生态普查包含 {metadata['oracle_categories']} 类 Oracle/机制标签和 {metadata['protocol_oracle_assignments']} 个协议–Oracle 关联。Gamma 对 {metadata['gamma_grade_a_links']} 个 UMA rounds 达到 A 级映射，另有 {metadata['gamma_unresolved_links']} 个 U；在 Gamma 同时提供 reward/bond 的可比较记录中，链上金额匹配率为 {metadata['gamma_nonmissing_match_rate']}。仍有大量缺失字段，因此 Gamma 只用于文本和映射，链上金额保持权威。",
        "",
        "## 6. 当前可以回答与不能回答的问题",
        "",
        "可以回答：",
        "",
        "- UMA 中协议接受、争议胜负、bond/fee 损失和 DVM 已实现再分配的规模与集中度；",
        "- Chainlink staking/reward/forfeiture 的事件和 LINK 资金路径；",
        "- ETH/USD active-phase 报告间隔是否触及已观测配置阈值；",
        "- 两种机制在问责单位、真值基础和可观测性上的结构差异。",
        "",
        "不能回答：",
        "",
        "- 未争议 UMA proposal 是否符合外部客观真值；",
        "- Chainlink 某次价格报告获得多少“正确性奖励”；",
        "- 没有实际 alert/slash 样本时的威慑或因果效果；",
        "- 本报告仍是 UMA/Chainlink 两机制主分析；Tellor 仅有严格争议子样本、Flare 已另建完整 epoch/provider/feed 模块、Pyth 仅有滚动 reward-factor 子样本，因此不能作五协议完整支付级比较；",
        "- 地址集中度背后的真实组织身份。",
        "",
        "## 7. 研究结论",
        "",
        "现有证据支持一个清晰的机制差异：UMA 把问责集中在少量高金额争议和投票再分配上；Chainlink 则以持续 staking reward 和 service-window 条件提供安全保障。本样本中 UMA 的 realized penalties 大量可见，而 Chainlink 的 principal slash/alert 没有实现样本。这个差异首先是机制与样本实现状态的差异，不能简单解释为哪一个 Oracle“更诚实”或“更安全”。",
        "",
        "复现命令：",
        "",
        "```bash",
        "PYTHONPATH=src python scripts/research_existing_data.py",
        "```",
    ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
