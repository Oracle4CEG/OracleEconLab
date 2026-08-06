"""Map QC-complete native oracle ledgers into the common event schema."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
PARQUET = (ROOT / "data/curated/parquet").resolve()
OUTPUT = PARQUET / "accountability_events.parquet"
MANIFEST = ROOT / "data/manifests/accountability_events.json"
LINK = "0x514910771af9ca656af840dff83e8264ecf986ca"
UMA = "0x04fa0d235c4abf4bcf4787af4cf447de572ef828"
FLARE_SYSTEMS_MANAGER = "0x89e50DC0380e597ecE79c8494bAAFD84537AD0D4"
WDIA = "0x9f5da8630d47178bab71f5923644a28b15cbdca7"

COLUMNS = [
    "schema_version", "accountability_event_id", "oracle_network", "mechanism_family", "security_chain",
    "delivery_chain", "accountability_unit_type", "accountability_unit_id", "event_granularity",
    "event_time_unix", "actor", "actor_role", "counterparty", "counterparty_role", "reward_class",
    "reward_amount_raw", "reward_asset", "reward_asset_decimals", "reward_accrual_time_unix",
    "reward_payment_time_unix", "penalty_class", "principal_slashed_raw", "bond_forfeited_raw",
    "fee_forfeited_raw", "reward_forfeited_raw", "penalty_asset", "penalty_asset_decimals",
    "nonmonetary_penalty", "principal_locked_raw", "principal_returned_raw", "principal_asset",
    "principal_asset_decimals", "truth_basis", "outcome_status", "external_truth_available",
    "service_window_seconds", "service_threshold_seconds", "source_event", "source_tx", "source_block",
    "source_log_index", "source_contract", "rule_id", "parameter_version", "observability_grade",
    "confidence_grade", "native_table", "sample_tier", "interpretation_note",
]


def source(name: str) -> str:
    path = PARQUET / f"{name}.parquet"
    if not path.is_file():
        raise RuntimeError(f"missing required input: {path}")
    return f"read_parquet('{path}')"


def main() -> None:
    connection = duckdb.connect()
    rounds = source("polygon_uma_request_rounds")
    payoffs = source("uma_dvm_voter_payoffs")
    staking = source("chainlink_staking_v02_events")
    feed = source("chainlink_eth_usd_reports")
    tellor_disputes = source("tellor_disputes")
    tellor_votes = source("tellor_dispute_votes")
    tellor_payments = source("tellor_dispute_payments")
    tellor_reports = source("tellor_micro_reports")
    tellor_withdrawals = source("tellor_tip_withdrawals_realized")
    tellor_jail_events = source("tellor_jail_events")
    flare_claims = source("flare_reward_claims")
    flare_conditions = source("flare_provider_conditions")
    flare_chill = source("flare_beneficiary_chill_state")
    flare_claim_events = source("flare_reward_claim_events")
    flare_chill_events = source("flare_beneficiary_chill_events")
    pyth_factors = source("pyth_ois_publisher_epoch_factors")
    pyth_stake_events = source("pyth_ois_stake_events")
    pyth_economic_events = source("pyth_ois_economic_events")
    chronicle_events = source("chronicle_ethereum_events")
    redstone_events = source("redstone_ethereum_push_events")
    dia_withdrawals = source("dia_staking_withdrawals")
    evidence = json.loads((ROOT / "data/manifests/chainlink_evidence_ledger.json").read_text(encoding="utf-8"))
    config_rows = [row for row in evidence.get("feed_phase_intervals", [])]
    if not config_rows:
        raise RuntimeError("Chainlink active phase intervals are missing")
    staking_manifest = json.loads((ROOT / "data/manifests/chainlink_staking_v02_ledger.json").read_text(encoding="utf-8"))
    feed_config = connection.execute(
        f"SELECT threshold_1_seconds, configuration_version FROM {staking} WHERE event='FeedConfigSet' LIMIT 1"
    ).fetchone()
    if not feed_config:
        raise RuntimeError("Chainlink FeedConfigSet is missing")
    threshold_seconds, configuration_version = int(feed_config[0]), str(feed_config[1])

    connection.execute(f"""
        CREATE TEMP VIEW uma_oov2_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'uma_oov2', oo_request_id, coalesce(settlement_tx, source_tx), economic_status)) accountability_event_id,
          'UMA' oracle_network,
          'optimistic_oracle_and_dvm' mechanism_family,
          'Ethereum' security_chain,
          'Polygon' delivery_chain,
          'request' accountability_unit_type,
          oo_request_id accountability_unit_id,
          'request_round_economic_outcome' event_granularity,
          try_cast(request_time AS BIGINT) event_time_unix,
          CASE
            WHEN economic_status='settled_disputed_disputer_wins' THEN disputer
            ELSE proposer
          END actor,
          CASE
            WHEN economic_status='settled_disputed_disputer_wins' THEN 'correct_disputer'
            WHEN economic_status='settled_disputed_proposer_wins' THEN 'correct_proposer'
            ELSE 'proposer'
          END actor_role,
          CASE
            WHEN economic_status='settled_disputed_disputer_wins' THEN proposer
            WHEN economic_status='settled_disputed_proposer_wins' THEN disputer
            ELSE NULL
          END counterparty,
          CASE
            WHEN economic_status='settled_disputed_disputer_wins' THEN 'incorrect_proposer'
            WHEN economic_status='settled_disputed_proposer_wins' THEN 'unsuccessful_disputer'
            ELSE NULL
          END counterparty_role,
          CASE
            WHEN economic_status='settled_undisputed' AND try_cast(explicit_report_reward_raw AS DECIMAL(38,0)) > 0 THEN 'explicit_report_reward'
            WHEN economic_status LIKE 'settled_disputed_%' THEN 'dispute_winner_reward'
            ELSE NULL
          END reward_class,
          CASE
            WHEN economic_status='settled_undisputed' THEN explicit_report_reward_raw
            WHEN economic_status LIKE 'settled_disputed_%' THEN dispute_winner_reward_raw
            ELSE NULL
          END reward_amount_raw,
          CASE WHEN status='settled' THEN currency ELSE NULL END reward_asset,
          CASE WHEN status='settled' THEN 6::SMALLINT ELSE NULL END reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CASE WHEN economic_status LIKE 'settled_disputed_%' THEN 'bond_forfeiture_and_final_fee_forfeiture' ELSE NULL END penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CASE WHEN economic_status LIKE 'settled_disputed_%' THEN bond_forfeited_raw ELSE NULL END bond_forfeited_raw,
          CASE WHEN economic_status LIKE 'settled_disputed_%' THEN final_fee_forfeited_raw ELSE NULL END fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CASE WHEN economic_status LIKE 'settled_disputed_%' THEN currency ELSE NULL END penalty_asset,
          CASE WHEN economic_status LIKE 'settled_disputed_%' THEN 6::SMALLINT ELSE NULL END penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          effective_bond_raw principal_locked_raw,
          principal_returned_raw,
          currency principal_asset,
          6::SMALLINT principal_asset_decimals,
          CASE WHEN economic_status LIKE 'settled_disputed_%' THEN 'protocol_vote_adjudication' ELSE 'undisputed_acceptance' END truth_basis,
          economic_status outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          CASE WHEN status='settled' THEN 'Settle' ELSE event END source_event,
          coalesce(settlement_tx, source_tx) source_tx,
          coalesce(settlement_block, source_block) source_block,
          CASE WHEN settlement_tx IS NULL THEN log_index ELSE NULL END source_log_index,
          source_contract,
          rule_id,
          adapter_version parameter_version,
          CASE WHEN question_link_grade='A' THEN 'A' ELSE 'U' END observability_grade,
          coalesce(question_link_grade, 'U') confidence_grade,
          'polygon_uma_request_rounds' native_table,
          sample_tier,
          'Undisputed acceptance is protocol acceptance, not external objective truth.' interpretation_note
        FROM {rounds}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW uma_dvm_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'uma_dvm', source_tx, source_block::VARCHAR, log_index::VARCHAR, coalesce(dvm_request_id, request_index), voter)) accountability_event_id,
          'UMA' oracle_network,
          'optimistic_oracle_and_dvm' mechanism_family,
          'Ethereum' security_chain,
          CAST(NULL AS VARCHAR) delivery_chain,
          'voting_request' accountability_unit_type,
          coalesce(dvm_request_id, request_index, concat(source_tx, ':', log_index::VARCHAR)) accountability_unit_id,
          'voter_payoff' event_granularity,
          CAST(NULL AS BIGINT) event_time_unix,
          voter actor,
          'voter' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CASE WHEN classification_rule_id='DVM_CORRECT_VOTE_REDISTRIBUTION' THEN 'correct_vote_redistribution' ELSE NULL END reward_class,
          CASE WHEN classification_rule_id='DVM_CORRECT_VOTE_REDISTRIBUTION' THEN correct_vote_redistribution_raw ELSE NULL END reward_amount_raw,
          CASE WHEN classification_rule_id='DVM_CORRECT_VOTE_REDISTRIBUTION' THEN '{UMA}' ELSE NULL END reward_asset,
          CASE WHEN classification_rule_id='DVM_CORRECT_VOTE_REDISTRIBUTION' THEN 18::SMALLINT ELSE NULL END reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CASE WHEN classification_rule_id='DVM_NEGATIVE_SLASH' THEN 'principal_slash' ELSE NULL END penalty_class,
          CASE WHEN classification_rule_id='DVM_NEGATIVE_SLASH' THEN wrong_or_no_vote_slash_raw ELSE NULL END principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CASE WHEN classification_rule_id='DVM_NEGATIVE_SLASH' THEN '{UMA}' ELSE NULL END penalty_asset,
          CASE WHEN classification_rule_id='DVM_NEGATIVE_SLASH' THEN 18::SMALLINT ELSE NULL END penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'protocol_vote_adjudication' truth_basis,
          classification_rule_id outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          event source_event,
          source_tx,
          source_block,
          log_index source_log_index,
          source_contract,
          classification_rule_id rule_id,
          CAST(NULL AS VARCHAR) parameter_version,
          'A' observability_grade,
          confidence_grade,
          'uma_dvm_voter_payoffs' native_table,
          'observable_accountability_panel' sample_tier,
          'VoterSlashApplied is excluded; signed VoterSlashed deltas are mapped exactly once.' interpretation_note
        FROM {payoffs}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW chainlink_staking_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'chainlink_staking', source_tx, log_index::VARCHAR, event, coalesce(staker, pool, source_contract))) accountability_event_id,
          'Chainlink' oracle_network,
          'staking_service_security' mechanism_family,
          'Ethereum' security_chain,
          'Ethereum' delivery_chain,
          'service_availability_window' accountability_unit_type,
          concat(coalesce(staker, pool, source_contract), ':', source_tx, ':', log_index::VARCHAR) accountability_unit_id,
          CASE
            WHEN event IN ('Staked','Unstaked') THEN 'staking_principal_action'
            WHEN event='RewardClaimed' THEN 'staking_reward_payment'
            WHEN event='RewardFinalized' THEN 'locked_reward_finalization'
            ELSE 'forfeited_reward_accounting_distribution'
          END event_granularity,
          CAST(NULL AS BIGINT) event_time_unix,
          coalesce(staker, pool) actor,
          CASE WHEN staker IS NOT NULL THEN 'staker' ELSE 'reward_pool' END actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CASE WHEN event='RewardClaimed' THEN 'base_staking_reward_and_delegation_reward' ELSE NULL END reward_class,
          CASE WHEN event='RewardClaimed' THEN reward_claimed_raw ELSE NULL END reward_amount_raw,
          CASE WHEN event='RewardClaimed' THEN '{LINK}' ELSE NULL END reward_asset,
          CASE WHEN event='RewardClaimed' THEN 18::SMALLINT ELSE NULL END reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CASE WHEN event IN ('RewardFinalized','ForfeitedRewardDistributed') AND (reward_forfeited OR event='ForfeitedRewardDistributed') THEN 'reward_forfeiture' ELSE NULL END penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CASE WHEN event='ForfeitedRewardDistributed' THEN vested_reward_raw ELSE NULL END reward_forfeited_raw,
          CASE WHEN event IN ('RewardFinalized','ForfeitedRewardDistributed') AND (reward_forfeited OR event='ForfeitedRewardDistributed') THEN '{LINK}' ELSE NULL END penalty_asset,
          CASE WHEN event IN ('RewardFinalized','ForfeitedRewardDistributed') AND (reward_forfeited OR event='ForfeitedRewardDistributed') THEN 18::SMALLINT ELSE NULL END penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CASE WHEN event='Staked' THEN amount_raw ELSE NULL END principal_locked_raw,
          CASE WHEN event='Unstaked' THEN amount_raw ELSE NULL END principal_returned_raw,
          CASE WHEN event IN ('Staked','Unstaked') THEN '{LINK}' ELSE NULL END principal_asset,
          CASE WHEN event IN ('Staked','Unstaked') THEN 18::SMALLINT ELSE NULL END principal_asset_decimals,
          'service_availability_condition' truth_basis,
          CASE
            WHEN event='RewardFinalized' AND reward_forfeited THEN 'locked_reward_forfeited'
            WHEN event='RewardFinalized' THEN 'locked_reward_finalized_not_forfeited'
            WHEN event='ForfeitedRewardDistributed' THEN 'forfeiture_accounting_redistributed'
            ELSE event
          END outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          event source_event,
          source_tx,
          source_block,
          log_index source_log_index,
          source_contract,
          concat('CHAINLINK_STAKING_V02_', upper(event)) rule_id,
          contract_role parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'chainlink_staking_v02_events' native_table,
          'observable_accountability_panel' sample_tier,
          CASE WHEN event='ForfeitedRewardDistributed' THEN 'Accounting redistribution only; no ERC-20 transfer or principal slash is invented.' ELSE 'Staking rewards are service incentives, not report-level truth rewards.' END interpretation_note
        FROM {staking}
        WHERE event IN ('Staked','Unstaked','RewardClaimed','RewardFinalized','ForfeitedRewardDistributed')
    """)

    connection.execute(f"""
        CREATE TEMP VIEW chainlink_feed_common AS
        WITH updates AS (
          SELECT *,
            lag(try_cast(updated_at AS BIGINT)) OVER (ORDER BY try_cast(updated_at AS BIGINT), source_block, log_index) previous_update,
            try_cast(updated_at AS BIGINT) current_update
          FROM {feed}
          WHERE event='AnswerUpdated'
        )
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'chainlink_feed_window', aggregator, source_tx, log_index::VARCHAR)) accountability_event_id,
          'Chainlink' oracle_network,
          'staking_service_security' mechanism_family,
          'Ethereum' security_chain,
          'Ethereum' delivery_chain,
          'service_availability_window' accountability_unit_type,
          concat(aggregator, ':', previous_update::VARCHAR, ':', current_update::VARCHAR) accountability_unit_id,
          'active_feed_report_interval' event_granularity,
          current_update event_time_unix,
          aggregator actor,
          'secured_service_aggregator' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CAST(NULL AS VARCHAR) reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'service_availability_condition' truth_basis,
          CASE WHEN current_update - previous_update < {threshold_seconds} THEN 'within_primary_alert_threshold' ELSE 'at_or_over_primary_alert_threshold' END outcome_status,
          false external_truth_available,
          current_update - previous_update service_window_seconds,
          {threshold_seconds}::BIGINT service_threshold_seconds,
          event source_event,
          source_tx,
          source_block,
          log_index source_log_index,
          aggregator source_contract,
          'CHAINLINK_ACTIVE_PHASE_SERVICE_WINDOW_V1' rule_id,
          '{configuration_version}' parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'chainlink_eth_usd_reports' native_table,
          'observable_accountability_panel' sample_tier,
          'A below-threshold interval is service-continuity evidence, not proof of report-level price correctness.' interpretation_note
        FROM updates
        WHERE previous_update IS NOT NULL
    """)

    connection.execute(f"""
        CREATE TEMP VIEW tellor_disputes_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'tellor_dispute', dispute_id)) accountability_event_id,
          'Tellor' oracle_network,
          'reporter_dispute_oracle' mechanism_family,
          'tellor-1' security_chain,
          'tellor-1' delivery_chain,
          'dispute' accountability_unit_type,
          dispute_id accountability_unit_id,
          'dispute_resolved_report' event_granularity,
          try_cast(epoch(try_cast(dispute_start_time AS TIMESTAMPTZ)) AS BIGINT) event_time_unix,
          CASE WHEN vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT') THEN disputer ELSE reporter END actor,
          CASE WHEN vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT') THEN 'successful_disputer' ELSE 'successful_reporter' END actor_role,
          CASE WHEN vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT') THEN reporter ELSE disputer END counterparty,
          CASE WHEN vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT') THEN 'slashed_reporter' ELSE 'unsuccessful_disputer' END counterparty_role,
          CASE WHEN vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT') THEN 'dispute_monitoring_reward' ELSE 'dispute_defense_reward' END reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CASE WHEN vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT') THEN 'principal_slash' ELSE 'dispute_fee_forfeiture' END penalty_class,
          CASE WHEN vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT') THEN slash_amount_raw ELSE NULL END principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CASE WHEN vote_result IN ('AGAINST','NO_QUORUM_MAJORITY_AGAINST') THEN dispute_fee_raw ELSE NULL END fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          'loya' penalty_asset,
          6::SMALLINT penalty_asset_decimals,
          'reporter_jailed_on_fully_funded_dispute' nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'protocol_vote_adjudication' truth_basis,
          vote_result outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'new_dispute' source_event,
          source_tx,
          source_block,
          CAST(NULL AS BIGINT) source_log_index,
          CAST(NULL AS VARCHAR) source_contract,
          rule_id,
          category parameter_version,
          'A' observability_grade,
          confidence_grade,
          'tellor_disputes' native_table,
          'strict_honesty_linked_events' sample_tier,
          'Designed slash/fee amounts come from finalized dispute state; gross settlement withdrawals remain separate.' interpretation_note
        FROM {tellor_disputes}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW tellor_votes_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'tellor_vote', source_tx, dispute_id, voter)) accountability_event_id,
          'Tellor' oracle_network,
          'reporter_dispute_oracle' mechanism_family,
          'tellor-1' security_chain,
          'tellor-1' delivery_chain,
          'dispute' accountability_unit_type,
          dispute_id accountability_unit_id,
          'dispute_vote' event_granularity,
          try_cast(epoch(try_cast(block_time AS TIMESTAMPTZ)) AS BIGINT) event_time_unix,
          voter actor,
          'dispute_voter' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CAST(NULL AS VARCHAR) reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'protocol_vote_adjudication' truth_basis,
          choice outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'voted_on_dispute' source_event,
          source_tx,
          source_block,
          CAST(NULL AS BIGINT) source_log_index,
          CAST(NULL AS VARCHAR) source_contract,
          'TELLOR_LAYER_DISPUTE_VOTE_V1' rule_id,
          CAST(NULL AS VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'tellor_dispute_votes' native_table,
          'strict_honesty_linked_events' sample_tier,
          'Voting participation is separate from any subsequently claimed voter reward.' interpretation_note
        FROM {tellor_votes}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW tellor_payments_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'tellor_dispute_payment', source_tx, dispute_id, event, actor)) accountability_event_id,
          'Tellor' oracle_network,
          'reporter_dispute_oracle' mechanism_family,
          'tellor-1' security_chain,
          'tellor-1' delivery_chain,
          'dispute' accountability_unit_type,
          dispute_id accountability_unit_id,
          CASE WHEN event='MsgClaimReward' THEN 'dispute_voter_reward_payment' ELSE 'gross_dispute_settlement_receipt' END event_granularity,
          try_cast(epoch(try_cast(block_time AS TIMESTAMPTZ)) AS BIGINT) event_time_unix,
          actor,
          CASE WHEN event='MsgClaimReward' THEN 'dispute_voter' ELSE 'dispute_fee_payer' END actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CASE WHEN event='MsgClaimReward' THEN 'dispute_vote_reward' ELSE NULL END reward_class,
          CASE WHEN event='MsgClaimReward' THEN received_loya_raw ELSE NULL END reward_amount_raw,
          CASE WHEN event='MsgClaimReward' THEN 'loya' ELSE NULL END reward_asset,
          CASE WHEN event='MsgClaimReward' THEN 6::SMALLINT ELSE NULL END reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CASE WHEN event='MsgClaimReward' THEN try_cast(epoch(try_cast(block_time AS TIMESTAMPTZ)) AS BIGINT) ELSE NULL END reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'protocol_vote_adjudication' truth_basis,
          event outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          event source_event,
          source_tx,
          source_block,
          CAST(NULL AS BIGINT) source_log_index,
          CAST(NULL AS VARCHAR) source_contract,
          CASE WHEN event='MsgClaimReward' THEN 'TELLOR_LAYER_VOTER_REWARD_PAYMENT_V1' ELSE 'TELLOR_LAYER_GROSS_SETTLEMENT_RECEIPT_V1' END rule_id,
          CAST(NULL AS VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'tellor_dispute_payments' native_table,
          'strict_honesty_linked_events' sample_tier,
          CASE WHEN event='MsgClaimReward' THEN 'Observed loya received by the claimant.' ELSE 'Gross receipt may mix fee return, principal return, and settlement gain; it is not labeled entirely as reward.' END interpretation_note
        FROM {tellor_payments}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW flare_claims_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'flare_fsp_claim', reward_epoch_id::VARCHAR, beneficiary, claim_type_id::VARCHAR)) accountability_event_id,
          'Flare_FTSOv2' oracle_network,
          'fsp_reward_epoch_and_ftso_scaling' mechanism_family,
          'Flare_Mainnet' security_chain,
          'Flare_Mainnet' delivery_chain,
          'reward_epoch' accountability_unit_type,
          reward_epoch_id::VARCHAR accountability_unit_id,
          'aggregate_fsp_merkle_entitlement' event_granularity,
          epoch_end_time_unix event_time_unix,
          beneficiary actor,
          concat(lower(claim_type), '_claim_beneficiary') actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          'aggregate_fsp_reward_entitlement' reward_class,
          amount_raw reward_amount_raw,
          asset reward_asset,
          asset_decimals::SMALLINT reward_asset_decimals,
          epoch_end_time_unix reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'service_availability_condition' truth_basis,
          'merkle_entitlement_finalized' outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'RewardMerkleClaim' source_event,
          CAST(NULL AS VARCHAR) source_tx,
          CAST(NULL AS BIGINT) source_block,
          CAST(NULL AS BIGINT) source_log_index,
          '{FLARE_SYSTEMS_MANAGER}' source_contract,
          'FLARE_FSP_ONCHAIN_RECONCILED_MERKLE_ENTITLEMENT_V1' rule_id,
          claim_type parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'flare_reward_claims' native_table,
          'observable_accountability_panel' sample_tier,
          'The Merkle tree aggregates FSP protocols; this amount is not labeled wholly as an FTSO median-accuracy reward, and it is an entitlement rather than an observed payment.' interpretation_note
        FROM {flare_claims}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW flare_conditions_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'flare_provider_condition', reward_epoch_id::VARCHAR, voter_address)) accountability_event_id,
          'Flare_FTSOv2' oracle_network,
          'fsp_reward_epoch_and_ftso_scaling' mechanism_family,
          'Flare_Mainnet' security_chain,
          'Flare_Mainnet' delivery_chain,
          'reward_epoch' accountability_unit_type,
          reward_epoch_id::VARCHAR accountability_unit_id,
          'provider_minimum_condition_outcome' event_granularity,
          epoch_end_time_unix event_time_unix,
          voter_address actor,
          'registered_voter_provider' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CASE WHEN eligible_for_reward THEN 'fsp_epoch_reward_eligibility' ELSE NULL END reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          epoch_end_time_unix reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CASE WHEN NOT eligible_for_reward THEN 'epoch_reward_ineligibility' ELSE NULL END penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CASE
            WHEN new_number_of_passes < passes_held THEN 'pass_balance_decreased'
            WHEN NOT eligible_for_reward THEN 'reward_ineligible'
            ELSE NULL
          END nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'service_availability_condition' truth_basis,
          CASE WHEN eligible_for_reward THEN 'eligible_for_reward' ELSE 'ineligible_for_reward' END outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'MinimalConditionsAndPasses' source_event,
          CAST(NULL AS VARCHAR) source_tx,
          CAST(NULL AS BIGINT) source_block,
          CAST(NULL AS BIGINT) source_log_index,
          CAST(NULL AS VARCHAR) source_contract,
          'FLARE_FSP_PROVIDER_MINIMUM_CONDITION_V1' rule_id,
          concat('ftso=', coalesce(ftso_scaling_condition_met::VARCHAR, 'null'), ';fu=', coalesce(fast_updates_condition_met::VARCHAR, 'null'), ';staking=', coalesce(staking_condition_met::VARCHAR, 'null'), ';fdc=', coalesce(fdc_condition_met::VARCHAR, 'null')) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'flare_provider_conditions' native_table,
          'observable_accountability_panel' sample_tier,
          'Eligibility is a composite FSP condition. FTSO feed-hit rows in the native ledger isolate consensus-band performance; no unreported forfeiture amount is invented.' interpretation_note
        FROM {flare_conditions}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW flare_chill_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'flare_chill_state', beneficiary, state_block::VARCHAR)) accountability_event_id,
          'Flare_FTSOv2' oracle_network,
          'fsp_reward_epoch_and_ftso_scaling' mechanism_family,
          'Flare_Mainnet' security_chain,
          'Flare_Mainnet' delivery_chain,
          'reward_epoch' accountability_unit_type,
          chilled_until_reward_epoch_id::VARCHAR accountability_unit_id,
          'beneficiary_chill_state_at_cutoff' event_granularity,
          1782863999::BIGINT event_time_unix,
          beneficiary actor,
          'reward_beneficiary' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CAST(NULL AS VARCHAR) reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          'beneficiary_chill' penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          concat('chilled_until_reward_epoch_', chilled_until_reward_epoch_id::VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'service_availability_condition' truth_basis,
          CASE WHEN active_at_cutoff_epoch THEN 'active_chill_state_at_cutoff' ELSE 'historical_nonzero_chill_state_at_cutoff' END outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'chilledUntilRewardEpochId' source_event,
          CAST(NULL AS VARCHAR) source_tx,
          state_block source_block,
          CAST(NULL AS BIGINT) source_log_index,
          source_contract,
          'FLARE_VOTER_REGISTRY_CHILL_STATE_AT_CUTOFF_V1' rule_id,
          CAST(NULL AS VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'flare_beneficiary_chill_state' native_table,
          'observable_accountability_panel' sample_tier,
          'This is contract state at the fixed cutoff, not a reconstructed historical BeneficiaryChilled event timestamp.' interpretation_note
        FROM {flare_chill}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW pyth_ois_factors_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|', 'pyth_ois_factor', epoch_id::VARCHAR, publisher)) accountability_event_id,
          'Pyth' oracle_network,
          'publisher_pool_integrity_staking' mechanism_family,
          'Solana_Mainnet' security_chain,
          'multi_chain' delivery_chain,
          'publisher_pool_epoch' accountability_unit_type,
          concat(epoch_id::VARCHAR, ':', publisher) accountability_unit_id,
          'publisher_epoch_reward_factor' event_granularity,
          epoch_end_time_unix event_time_unix,
          publisher actor,
          'publisher_pool' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CASE WHEN reward_active_regime AND has_positive_reward_factor THEN 'ois_stake_reward_rate' ELSE NULL END reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          epoch_end_time_unix reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'service_availability_condition' truth_basis,
          CASE
            WHEN NOT reward_active_regime THEN 'reward_rate_paused_by_governance'
            WHEN has_positive_reward_factor THEN 'positive_reward_factor'
            ELSE 'no_positive_reward_factor'
          END outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'IntegrityPoolRewardEvent' source_event,
          CAST(NULL AS VARCHAR) source_tx,
          CAST(NULL AS BIGINT) source_block,
          source_storage_index::BIGINT source_log_index,
          source_account source_contract,
          'PYTH_OIS_DURABLE_ROLLING_REWARD_FACTOR_V1' rule_id,
          concat('Y=', reward_rate_y_raw, ';self=', self_reward_ratio_raw, ';delegated=', delegated_reward_ratio_raw, ';fee=', delegation_fee_raw) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'pyth_ois_publisher_epoch_factors' native_table,
          'observable_accountability_panel' sample_tier,
          'Rates are not paid amounts and positive reward factors are not objective correctness judgments. No realized slash is invented when durable lifetime slash counters are zero.' interpretation_note
        FROM {pyth_factors}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW tellor_reports_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','tellor_report',reporter,meta_id::VARCHAR,block_number::VARCHAR)) accountability_event_id,
          'Tellor' oracle_network,
          'reporter_dispute_oracle' mechanism_family,
          'tellor-1' security_chain,
          'tellor-1' delivery_chain,
          'individual_report' accountability_unit_type,
          concat(query_id,':',meta_id::VARCHAR) accountability_unit_id,
          'micro_report' event_granularity,
          (timestamp_ms / 1000)::BIGINT event_time_unix,
          reporter actor,
          'reporter' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CAST(NULL AS VARCHAR) reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'protocol_aggregate' truth_basis,
          'report_submitted' outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'new_report' source_event,
          CAST(NULL AS VARCHAR) source_tx,
          block_number source_block,
          meta_id source_log_index,
          'x/oracle' source_contract,
          'TELLOR_NEW_REPORT_BLOCK_EVENT_V1' rule_id,
          concat('query_type=',query_type,';method=',aggregate_method,';power=',power::VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'tellor_micro_reports' native_table,
          CASE
            WHEN power > 0 THEN 'observable_accountability_panel'
            ELSE 'supplementary_no_stake_report'
          END sample_tier,
          CASE
            WHEN power > 0 THEN 'The row comes from the immutable new_report event emitted by a successful SetValue transaction. It is not automatically a paid reward; rewards and withdrawals are separate ledgers.'
            ELSE 'This immutable new_report has zero reporting power and is retained as a no-stake supplementary row; it must not enter the standard disputable honesty panel.'
          END interpretation_note
        FROM {tellor_reports}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW tellor_tip_withdrawals_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','tellor_tip_withdrawal',source_tx,event_index::VARCHAR,selector)) accountability_event_id,
          'Tellor' oracle_network,
          'reporter_dispute_oracle' mechanism_family,
          'tellor-1' security_chain,
          'tellor-1' delivery_chain,
          'individual_report' accountability_unit_type,
          concat(source_tx,':',event_index::VARCHAR) accountability_unit_id,
          'settled_reward_compounded_to_stake' event_granularity,
          try_cast(epoch(try_cast(block_time AS TIMESTAMPTZ)) AS BIGINT) event_time_unix,
          selector actor,
          'selector' actor_role,
          validator counterparty,
          'validator' counterparty_role,
          'tip_reward' reward_class,
          reward_withdrawn_to_stake_loya_raw reward_amount_raw,
          asset reward_asset,
          asset_decimals::SMALLINT reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          try_cast(epoch(try_cast(block_time AS TIMESTAMPTZ)) AS BIGINT) reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'protocol_recognized_reporting' truth_basis,
          'reward_paid_to_stake' outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'tip_withdrawn' source_event,
          source_tx,
          height source_block,
          event_index source_log_index,
          'x/reporter' source_contract,
          rule_id,
          concat('shares=',new_validator_shares) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'tellor_tip_withdrawals_realized' native_table,
          'observable_accountability_panel' sample_tier,
          'Tips escrow coin-spent evidence exactly matches the amount delegated into stake.' interpretation_note
        FROM {tellor_withdrawals}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW flare_claim_events_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','flare_claim_event',source_tx,source_log_index::VARCHAR)) accountability_event_id,
          'Flare_FTSOv2' oracle_network,
          'fsp_reward_epoch_and_ftso_scaling' mechanism_family,
          'Flare_Mainnet' security_chain,
          'Flare_Mainnet' delivery_chain,
          'reward_epoch' accountability_unit_type,
          reward_epoch_id::VARCHAR accountability_unit_id,
          CASE WHEN is_fee_burn THEN 'reward_fee_burn' ELSE 'realized_reward_claim' END event_granularity,
          block_time_unix event_time_unix,
          recipient actor,
          CASE WHEN is_fee_burn THEN 'burn_address' ELSE 'claim_recipient' END actor_role,
          reward_owner counterparty,
          'reward_owner' counterparty_role,
          CASE WHEN NOT is_fee_burn THEN 'aggregate_fsp_reward_claim' ELSE NULL END reward_class,
          CASE WHEN NOT is_fee_burn THEN amount_raw ELSE NULL END reward_amount_raw,
          CASE WHEN NOT is_fee_burn THEN asset ELSE NULL END reward_asset,
          CASE WHEN NOT is_fee_burn THEN asset_decimals::SMALLINT ELSE NULL END reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CASE WHEN NOT is_fee_burn THEN block_time_unix ELSE NULL END reward_payment_time_unix,
          CASE WHEN is_fee_burn THEN 'protocol_fee_burn' ELSE NULL END penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CASE WHEN is_fee_burn THEN amount_raw ELSE NULL END reward_forfeited_raw,
          CASE WHEN is_fee_burn THEN asset ELSE NULL END penalty_asset,
          CASE WHEN is_fee_burn THEN asset_decimals::SMALLINT ELSE NULL END penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'service_availability_condition' truth_basis,
          CASE WHEN is_fee_burn THEN 'fee_burned' ELSE 'reward_claimed' END outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'RewardClaimed' source_event,
          source_tx,
          source_block,
          source_log_index,
          source_contract,
          rule_id,
          claim_type parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'flare_reward_claim_events' native_table,
          'observable_accountability_panel' sample_tier,
          'The emitted claim is realized; a FEE burn is kept as accounting and not counted as a provider reward.' interpretation_note
        FROM {flare_claim_events}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW flare_chill_events_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','flare_chill_event',source_tx,source_log_index::VARCHAR)) accountability_event_id,
          'Flare_FTSOv2' oracle_network,
          'fsp_reward_epoch_and_ftso_scaling' mechanism_family,
          'Flare_Mainnet' security_chain,
          'Flare_Mainnet' delivery_chain,
          'reward_epoch' accountability_unit_type,
          chilled_until_reward_epoch_id::VARCHAR accountability_unit_id,
          'beneficiary_chill_event' event_granularity,
          block_time_unix event_time_unix,
          beneficiary actor,
          'reward_beneficiary' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CAST(NULL AS VARCHAR) reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          'chill' penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          concat('chilled_until_reward_epoch_',chilled_until_reward_epoch_id::VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'service_availability_condition' truth_basis,
          'beneficiary_chilled' outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'BeneficiaryChilled' source_event,
          source_tx,
          source_block,
          source_log_index,
          source_contract,
          rule_id,
          CAST(NULL AS VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'flare_beneficiary_chill_events' native_table,
          'strict_honesty_linked_events' sample_tier,
          'This is the historical on-chain chill event, not merely cutoff state.' interpretation_note
        FROM {flare_chill_events}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW pyth_stake_events_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','pyth_stake',signature,outer_instruction_index::VARCHAR,event)) accountability_event_id,
          'Pyth' oracle_network,
          'publisher_pool_integrity_staking' mechanism_family,
          'Solana_Mainnet' security_chain,
          'multi_chain' delivery_chain,
          'publisher_pool_epoch' accountability_unit_type,
          concat(publisher,':',stake_account_positions) accountability_unit_id,
          'stake_position_mutation' event_granularity,
          block_time_unix event_time_unix,
          owner actor,
          'delegator' actor_role,
          publisher counterparty,
          'publisher_pool' counterparty_role,
          CAST(NULL AS VARCHAR) reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CASE WHEN event='delegate' THEN amount_raw ELSE NULL END principal_locked_raw,
          CASE WHEN event='undelegate' THEN amount_raw ELSE NULL END principal_returned_raw,
          asset principal_asset,
          asset_decimals::SMALLINT principal_asset_decimals,
          'independent_reference_adjudication' truth_basis,
          event outcome_status,
          true external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          event source_event,
          signature source_tx,
          slot source_block,
          outer_instruction_index source_log_index,
          'pyti8TM4zRVBjmarcgAPmTNNAXYKJv7WVHrkrm6woLN' source_contract,
          rule_id,
          concat('position_index=',coalesce(position_index::VARCHAR,'null')) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'pyth_ois_stake_events' native_table,
          'observable_accountability_panel' sample_tier,
          'Delegation and undelegation mutate stake positions; they are not rewards or penalties.' interpretation_note
        FROM {pyth_stake_events}
        WHERE event IN ('delegate','undelegate')
    """)

    connection.execute(f"""
        CREATE TEMP VIEW pyth_economic_events_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','pyth_economic',signature,outer_instruction_index::VARCHAR,
                 coalesce(inner_transfer_index::VARCHAR,'parameter'),event)) accountability_event_id,
          'Pyth' oracle_network,
          'publisher_pool_integrity_staking' mechanism_family,
          'Solana_Mainnet' security_chain,
          'multi_chain' delivery_chain,
          'publisher_pool_epoch' accountability_unit_type,
          concat(coalesce(beneficiary,'unknown'),':',signature) accountability_unit_id,
          event event_granularity,
          block_time_unix event_time_unix,
          beneficiary actor,
          CASE WHEN event='reward_transfer' THEN coalesce(reward_role,'reward_beneficiary') ELSE 'publisher_pool' END actor_role,
          CASE WHEN event='reward_transfer' THEN source_token_account ELSE destination_token_account END counterparty,
          CASE WHEN event='reward_transfer' THEN 'pool_reward_custody' ELSE 'slash_custody' END counterparty_role,
          CASE WHEN event='reward_transfer' THEN coalesce(reward_role,'ois_reward') ELSE NULL END reward_class,
          CASE WHEN event='reward_transfer' THEN amount_raw ELSE NULL END reward_amount_raw,
          CASE WHEN event='reward_transfer' THEN asset ELSE NULL END reward_asset,
          CASE WHEN event='reward_transfer' THEN asset_decimals::SMALLINT ELSE NULL END reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CASE WHEN event='reward_transfer' THEN block_time_unix ELSE NULL END reward_payment_time_unix,
          CASE WHEN event='principal_slash_transfer' THEN 'principal_slash'
               ELSE NULL END penalty_class,
          CASE WHEN event='principal_slash_transfer' THEN amount_raw ELSE NULL END principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CASE WHEN event='principal_slash_transfer' THEN asset ELSE NULL END penalty_asset,
          CASE WHEN event='principal_slash_transfer' THEN asset_decimals::SMALLINT ELSE NULL END penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'independent_reference_adjudication' truth_basis,
          semantic_class outcome_status,
          true external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          event source_event,
          signature source_tx,
          slot source_block,
          (outer_instruction_index * 1000000 + coalesce(inner_transfer_index,0)) source_log_index,
          'pyti8TM4zRVBjmarcgAPmTNNAXYKJv7WVHrkrm6woLN' source_contract,
          rule_id,
          CAST(NULL AS VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'pyth_ois_economic_events' native_table,
          CASE WHEN event='principal_slash_transfer' THEN 'strict_honesty_linked_events' ELSE 'observable_accountability_panel' END sample_tier,
          'This table contains only realized reward or principal-slash SPL-token transfers. At the fixed cutoff every observed row is a reward transfer and no principal-slash transfer exists.' interpretation_note
        FROM {pyth_economic_events}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW chronicle_events_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','chronicle',transaction_hash,log_index::VARCHAR)) accountability_event_id,
          'Chronicle' oracle_network,
          'optimistic_schnorr_oracle' mechanism_family,
          'Ethereum' security_chain,
          'Ethereum' delivery_chain,
          CASE WHEN event_name LIKE 'OpPokeChallenged%' THEN 'dispute' ELSE 'individual_report' END accountability_unit_type,
          concat(contract_address,':',transaction_hash,':',log_index::VARCHAR) accountability_unit_id,
          semantic_class event_granularity,
          block_timestamp event_time_unix,
          coalesce(challenger,caller,contract_address) actor,
          CASE WHEN challenger IS NOT NULL THEN 'challenger' ELSE 'oracle_actor' END actor_role,
          contract_address counterparty,
          'scribe_contract' counterparty_role,
          CASE WHEN event_name='OpChallengeRewardPaid' THEN 'dispute_monitoring_reward' ELSE NULL END reward_class,
          CASE WHEN event_name='OpChallengeRewardPaid' THEN reward_amount_raw ELSE NULL END reward_amount_raw,
          CASE WHEN event_name='OpChallengeRewardPaid' THEN reward_asset ELSE NULL END reward_asset,
          CASE WHEN event_name='OpChallengeRewardPaid' THEN reward_asset_decimals::SMALLINT ELSE NULL END reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CASE WHEN event_name='OpChallengeRewardPaid' THEN block_timestamp ELSE NULL END reward_payment_time_unix,
          CASE WHEN event_name='FeedDropped' AND self_governed_drop THEN 'feed_exclusion' ELSE NULL END penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CASE WHEN event_name='FeedDropped' AND self_governed_drop THEN 'feed_dropped' ELSE NULL END nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'cryptographic_validation_and_challenge' truth_basis,
          event_name outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          event_name source_event,
          transaction_hash source_tx,
          block_number source_block,
          log_index source_log_index,
          contract_address source_contract,
          CASE WHEN event_name='OpChallengeRewardPaid' THEN 'CHRONICLE_OP_CHALLENGE_PAYMENT_V1'
               ELSE concat('CHRONICLE_',upper(event_name),'_V1') END rule_id,
          oracle_name parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'chronicle_ethereum_events' native_table,
          CASE WHEN event_name LIKE 'OpPokeChallenged%' OR event_name='FeedDropped' THEN 'strict_honesty_linked_events'
               ELSE 'ecosystem_observability' END sample_tier,
          'Challenge rewards are monetary only when the payment event is emitted; FeedDropped is nonmonetary exclusion.' interpretation_note
        FROM {chronicle_events}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW redstone_events_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','redstone',transaction_hash,log_index::VARCHAR)) accountability_event_id,
          'RedStone' oracle_network,
          'signed_payload_and_push_adapter' mechanism_family,
          'offchain_signed_data' security_chain,
          'Ethereum' delivery_chain,
          'individual_report' accountability_unit_type,
          concat(contract_address,':',transaction_hash,':',log_index::VARCHAR) accountability_unit_id,
          semantic_class event_granularity,
          block_timestamp event_time_unix,
          contract_address actor,
          'push_adapter' actor_role,
          CAST(NULL AS VARCHAR) counterparty,
          CAST(NULL AS VARCHAR) counterparty_role,
          CAST(NULL AS VARCHAR) reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CASE WHEN semantic_class='rejected_report_update' THEN event_name ELSE NULL END nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'signed_payload_verification' truth_basis,
          event_name outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          event_name source_event,
          transaction_hash source_tx,
          block_number source_block,
          log_index source_log_index,
          contract_address source_contract,
          concat('REDSTONE_',upper(event_name),'_V1') rule_id,
          CAST(feed_labels AS VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'redstone_ethereum_push_events' native_table,
          'ecosystem_observability' sample_tier,
          'Push delivery is observable; no publisher reward or slash settlement event is inferred.' interpretation_note
        FROM {redstone_events}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW tellor_jail_events_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','tellor_jail',jail_event_id)) accountability_event_id,
          'Tellor' oracle_network,
          'reporter_dispute_oracle' mechanism_family,
          'tellor-1' security_chain,
          'multi_chain' delivery_chain,
          'reporter_jail_lifecycle' accountability_unit_type,
          reporter accountability_unit_id,
          event_type event_granularity,
          epoch(try_cast(block_time AS TIMESTAMPTZ))::BIGINT event_time_unix,
          reporter actor,
          'reporter' actor_role,
          try_cast(caller AS VARCHAR) counterparty,
          CASE WHEN caller IS NULL THEN NULL ELSE 'unjail_caller' END counterparty_role,
          CAST(NULL AS VARCHAR) reward_class,
          CAST(NULL AS VARCHAR) reward_amount_raw,
          CAST(NULL AS VARCHAR) reward_asset,
          CAST(NULL AS SMALLINT) reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          CAST(NULL AS BIGINT) reward_payment_time_unix,
          CASE WHEN event_type='jailed_reporter' THEN 'jail' ELSE NULL END penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CASE WHEN event_type='jailed_reporter'
               THEN concat('reporter_jailed_duration_seconds_',duration_seconds::VARCHAR)
               ELSE NULL END nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          CAST(NULL AS VARCHAR) principal_returned_raw,
          CAST(NULL AS VARCHAR) principal_asset,
          CAST(NULL AS SMALLINT) principal_asset_decimals,
          'protocol_vote_adjudication' truth_basis,
          event_type outcome_status,
          false external_truth_available,
          duration_seconds::BIGINT service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          event_type source_event,
          tx_hash source_tx,
          height source_block,
          CAST(NULL AS BIGINT) source_log_index,
          CAST(NULL AS VARCHAR) source_contract,
          'TELLOR_REPORTER_JAIL_LIFECYCLE_V1' rule_id,
          CAST(NULL AS VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'tellor_jail_events' native_table,
          'strict_honesty_linked_events' sample_tier,
          'Jail and unjail are explicit state-transition events; duration zero is an immediate-unjail eligibility rule, not absence of punishment.' interpretation_note
        FROM {tellor_jail_events}
    """)

    connection.execute(f"""
        CREATE TEMP VIEW dia_staking_withdrawals_common AS
        SELECT
          '1.1.0' schema_version,
          sha256(concat_ws('|','dia_staking_withdrawal',transaction_hash,staking_store_index::VARCHAR)) accountability_event_id,
          'DIA' oracle_network,
          'oracle_network_staking' mechanism_family,
          'DIA_Lasernet' security_chain,
          'multi_chain' delivery_chain,
          'staking_position' accountability_unit_type,
          staking_store_index::VARCHAR accountability_unit_id,
          'realized_unstake_reward_payment' event_granularity,
          block_time_unix event_time_unix,
          beneficiary actor,
          'staker_beneficiary' actor_role,
          source_contract counterparty,
          'staking_contract' counterparty_role,
          'base_staking_reward' reward_class,
          total_reward_raw reward_amount_raw,
          '{WDIA}' reward_asset,
          18::SMALLINT reward_asset_decimals,
          CAST(NULL AS BIGINT) reward_accrual_time_unix,
          block_time_unix reward_payment_time_unix,
          CAST(NULL AS VARCHAR) penalty_class,
          CAST(NULL AS VARCHAR) principal_slashed_raw,
          CAST(NULL AS VARCHAR) bond_forfeited_raw,
          CAST(NULL AS VARCHAR) fee_forfeited_raw,
          CAST(NULL AS VARCHAR) reward_forfeited_raw,
          CAST(NULL AS VARCHAR) penalty_asset,
          CAST(NULL AS SMALLINT) penalty_asset_decimals,
          CAST(NULL AS VARCHAR) nonmonetary_penalty,
          CAST(NULL AS VARCHAR) principal_locked_raw,
          principal_returned_raw,
          '{WDIA}' principal_asset,
          18::SMALLINT principal_asset_decimals,
          'staking_participation_not_individual_report_truth' truth_basis,
          'realized_staking_reward_withdrawal' outcome_status,
          false external_truth_available,
          CAST(NULL AS BIGINT) service_window_seconds,
          CAST(NULL AS BIGINT) service_threshold_seconds,
          'unstake_and_wDIA_Transfer' source_event,
          transaction_hash source_tx,
          block_number source_block,
          CAST(NULL AS BIGINT) source_log_index,
          source_contract,
          rule_id,
          CAST(NULL AS VARCHAR) parameter_version,
          'A' observability_grade,
          'A' confidence_grade,
          'dia_staking_withdrawals' native_table,
          'ecosystem_observability' sample_tier,
          interpretation interpretation_note
        FROM {dia_withdrawals}
    """)

    union = " UNION ALL ".join(
        f"SELECT {', '.join(COLUMNS)} FROM {view}"
        for view in [
            "uma_oov2_common", "uma_dvm_common", "chainlink_staking_common", "chainlink_feed_common",
            "tellor_disputes_common", "tellor_votes_common", "tellor_payments_common",
            "flare_claims_common", "flare_conditions_common", "flare_chill_common",
            "pyth_ois_factors_common", "tellor_reports_common",
            "tellor_tip_withdrawals_common", "flare_claim_events_common",
            "flare_chill_events_common", "pyth_stake_events_common",
            "pyth_economic_events_common", "chronicle_events_common",
            "redstone_events_common", "tellor_jail_events_common",
            "dia_staking_withdrawals_common",
        ]
    )
    temporary = OUTPUT.with_suffix(".tmp.parquet")
    connection.execute(f"COPY ({union}) TO '{temporary}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)")
    temporary.replace(OUTPUT)

    common = f"read_parquet('{OUTPUT}')"
    counts = dict(connection.execute(f"SELECT native_table, count(*) FROM {common} GROUP BY 1 ORDER BY 1").fetchall())
    duplicate_ids = connection.execute(
        f"SELECT count(*) - count(DISTINCT accountability_event_id) FROM {common}"
    ).fetchone()[0]
    invalid_raw = connection.execute(f"""
        SELECT count(*) FROM {common}
        WHERE list_any_value(list_transform([
          reward_amount_raw, principal_slashed_raw, bond_forfeited_raw, fee_forfeited_raw,
          reward_forfeited_raw, principal_locked_raw, principal_returned_raw
        ], x -> x IS NOT NULL AND NOT regexp_full_match(x, '-?[0-9]+')))
    """).fetchone()[0]
    source_sums = {
        "uma_round_reward_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='polygon_uma_request_rounds'"
        ).fetchone()[0],
        "dvm_positive_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='uma_dvm_voter_payoffs'"
        ).fetchone()[0],
        "dvm_negative_raw": connection.execute(
            f"SELECT sum(try_cast(principal_slashed_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='uma_dvm_voter_payoffs'"
        ).fetchone()[0],
        "chainlink_claimed_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='chainlink_staking_v02_events'"
        ).fetchone()[0],
        "tellor_support_slash_raw": connection.execute(
            f"SELECT sum(try_cast(principal_slashed_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='tellor_disputes'"
        ).fetchone()[0],
        "tellor_against_fee_forfeited_raw": connection.execute(
            f"SELECT sum(try_cast(fee_forfeited_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='tellor_disputes'"
        ).fetchone()[0],
        "tellor_voter_claimed_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='tellor_dispute_payments'"
        ).fetchone()[0],
        "flare_fsp_entitlement_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='flare_reward_claims'"
        ).fetchone()[0],
        "tellor_tip_withdrawal_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='tellor_tip_withdrawals_realized'"
        ).fetchone()[0],
        "flare_realized_claim_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='flare_reward_claim_events'"
        ).fetchone()[0],
        "pyth_realized_reward_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='pyth_ois_economic_events'"
        ).fetchone()[0],
        "pyth_realized_slash_raw": connection.execute(
            f"SELECT sum(try_cast(principal_slashed_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='pyth_ois_economic_events'"
        ).fetchone()[0],
        "chronicle_challenge_reward_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {common} WHERE native_table='chronicle_ethereum_events'"
        ).fetchone()[0],
    }
    native_sums = {
        "uma_round_reward_raw": connection.execute(
            f"SELECT sum(try_cast(CASE WHEN economic_status='settled_undisputed' THEN explicit_report_reward_raw WHEN economic_status LIKE 'settled_disputed_%' THEN dispute_winner_reward_raw END AS DECIMAL(38,0))) FROM {rounds}"
        ).fetchone()[0],
        "dvm_positive_raw": connection.execute(
            f"SELECT sum(try_cast(correct_vote_redistribution_raw AS DECIMAL(38,0))) FROM {payoffs}"
        ).fetchone()[0],
        "dvm_negative_raw": connection.execute(
            f"SELECT sum(try_cast(wrong_or_no_vote_slash_raw AS DECIMAL(38,0))) FROM {payoffs}"
        ).fetchone()[0],
        "chainlink_claimed_raw": connection.execute(
            f"SELECT sum(try_cast(reward_claimed_raw AS DECIMAL(38,0))) FROM {staking} WHERE event='RewardClaimed'"
        ).fetchone()[0],
        "tellor_support_slash_raw": connection.execute(
            f"SELECT sum(try_cast(slash_amount_raw AS DECIMAL(38,0))) FROM {tellor_disputes} WHERE vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT')"
        ).fetchone()[0],
        "tellor_against_fee_forfeited_raw": connection.execute(
            f"SELECT sum(try_cast(dispute_fee_raw AS DECIMAL(38,0))) FROM {tellor_disputes} WHERE vote_result IN ('AGAINST','NO_QUORUM_MAJORITY_AGAINST')"
        ).fetchone()[0],
        "tellor_voter_claimed_raw": connection.execute(
            f"SELECT sum(try_cast(received_loya_raw AS DECIMAL(38,0))) FROM {tellor_payments} WHERE event='MsgClaimReward'"
        ).fetchone()[0],
        "flare_fsp_entitlement_raw": connection.execute(
            f"SELECT sum(try_cast(amount_raw AS DECIMAL(38,0))) FROM {flare_claims}"
        ).fetchone()[0],
        "tellor_tip_withdrawal_raw": connection.execute(
            f"SELECT sum(try_cast(reward_withdrawn_to_stake_loya_raw AS DECIMAL(38,0))) FROM {tellor_withdrawals}"
        ).fetchone()[0],
        "flare_realized_claim_raw": connection.execute(
            f"SELECT sum(try_cast(amount_raw AS DECIMAL(38,0))) FROM {flare_claim_events} WHERE NOT is_fee_burn"
        ).fetchone()[0],
        "pyth_realized_reward_raw": connection.execute(
            f"SELECT sum(try_cast(amount_raw AS DECIMAL(38,0))) FROM {pyth_economic_events} WHERE event='reward_transfer'"
        ).fetchone()[0],
        "pyth_realized_slash_raw": connection.execute(
            f"SELECT sum(try_cast(amount_raw AS DECIMAL(38,0))) FROM {pyth_economic_events} WHERE event='principal_slash_transfer'"
        ).fetchone()[0],
        "chronicle_challenge_reward_raw": connection.execute(
            f"SELECT sum(try_cast(reward_amount_raw AS DECIMAL(38,0))) FROM {chronicle_events} WHERE event_name='OpChallengeRewardPaid'"
        ).fetchone()[0],
    }
    sum_qc = {key: source_sums[key] == native_sums[key] for key in source_sums}
    expected_counts = {
        "polygon_uma_request_rounds": connection.execute(f"SELECT count(*) FROM {rounds}").fetchone()[0],
        "uma_dvm_voter_payoffs": connection.execute(f"SELECT count(*) FROM {payoffs}").fetchone()[0],
        "chainlink_staking_v02_events": connection.execute(
            f"SELECT count(*) FROM {staking} WHERE event IN ('Staked','Unstaked','RewardClaimed','RewardFinalized','ForfeitedRewardDistributed')"
        ).fetchone()[0],
        "chainlink_eth_usd_reports": connection.execute(
            f"SELECT greatest(count(*)-1,0) FROM {feed} WHERE event='AnswerUpdated'"
        ).fetchone()[0],
        "tellor_disputes": connection.execute(f"SELECT count(*) FROM {tellor_disputes}").fetchone()[0],
        "tellor_dispute_votes": connection.execute(f"SELECT count(*) FROM {tellor_votes}").fetchone()[0],
        "tellor_dispute_payments": connection.execute(f"SELECT count(*) FROM {tellor_payments}").fetchone()[0],
        "flare_reward_claims": connection.execute(f"SELECT count(*) FROM {flare_claims}").fetchone()[0],
        "flare_provider_conditions": connection.execute(f"SELECT count(*) FROM {flare_conditions}").fetchone()[0],
        "flare_beneficiary_chill_state": connection.execute(f"SELECT count(*) FROM {flare_chill}").fetchone()[0],
        "pyth_ois_publisher_epoch_factors": connection.execute(f"SELECT count(*) FROM {pyth_factors}").fetchone()[0],
        "tellor_micro_reports": connection.execute(f"SELECT count(*) FROM {tellor_reports}").fetchone()[0],
        "tellor_tip_withdrawals_realized": connection.execute(f"SELECT count(*) FROM {tellor_withdrawals}").fetchone()[0],
        "tellor_jail_events": connection.execute(f"SELECT count(*) FROM {tellor_jail_events}").fetchone()[0],
        "flare_reward_claim_events": connection.execute(f"SELECT count(*) FROM {flare_claim_events}").fetchone()[0],
        "flare_beneficiary_chill_events": connection.execute(f"SELECT count(*) FROM {flare_chill_events}").fetchone()[0],
        "pyth_ois_stake_events": connection.execute(
            f"SELECT count(*) FROM {pyth_stake_events} WHERE event IN ('delegate','undelegate')"
        ).fetchone()[0],
        "pyth_ois_economic_events": connection.execute(f"SELECT count(*) FROM {pyth_economic_events}").fetchone()[0],
        "chronicle_ethereum_events": connection.execute(f"SELECT count(*) FROM {chronicle_events}").fetchone()[0],
        "redstone_ethereum_push_events": connection.execute(f"SELECT count(*) FROM {redstone_events}").fetchone()[0],
        "dia_staking_withdrawals": connection.execute(f"SELECT count(*) FROM {dia_withdrawals}").fetchone()[0],
    }
    count_qc = {key: counts.get(key, 0) == expected for key, expected in expected_counts.items()}
    threshold_crossings = connection.execute(
        f"SELECT count(*) FROM {common} WHERE native_table='chainlink_eth_usd_reports' AND outcome_status='at_or_over_primary_alert_threshold'"
    ).fetchone()[0]
    manifest = {
        "dataset": "Oracle Accountability Atlas common accountability events",
        "schema_version": "1.1.0",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff": "2026-06-30T23:59:59Z",
        "output": str(OUTPUT),
        "schema_sql": str(ROOT / "schemas/accountability_events.sql"),
        "schema_json": str(ROOT / "schemas/accountability_events.schema.json"),
        "rows": sum(counts.values()),
        "rows_by_native_table": counts,
        "expected_rows_by_native_table": expected_counts,
        "row_count_qc": count_qc,
        "duplicate_event_ids": duplicate_ids,
        "invalid_raw_amount_fields": invalid_raw,
        "monetary_sum_qc": sum_qc,
        "chainlink_service_windows_at_or_over_primary_threshold": threshold_crossings,
        "chainlink_source_ledger_rows": staking_manifest["rows"],
        "all_required_assertions_pass": duplicate_ids == 0 and invalid_raw == 0 and all(sum_qc.values()) and all(count_qc.values()) and threshold_crossings == 0,
        "interpretation_guard": "The common table harmonizes event meaning; it does not make protocol-native reward units directly comparable.",
    }
    if not manifest["all_required_assertions_pass"]:
        raise RuntimeError(f"common accountability QC failed: {manifest}")
    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    parquet_manifest_path = ROOT / "data/manifests/curated_parquet.json"
    if parquet_manifest_path.is_file():
        parquet_manifest = json.loads(parquet_manifest_path.read_text(encoding="utf-8"))
        entry = {
            "source": "derived from QC-complete native Parquet ledgers",
            "parquet": str(OUTPUT),
            "rows": manifest["rows"],
            "bytes": OUTPUT.stat().st_size,
            "schema_mode": "common_accountability_schema_v1",
        }
        files = [row for row in parquet_manifest["files"] if Path(row["parquet"]) != OUTPUT]
        files.append(entry)
        parquet_manifest["files"] = files
        parquet_manifest["total_rows_across_tables"] = sum(int(row["rows"]) for row in files)
        parquet_manifest["generated_at_utc"] = datetime.now(UTC).isoformat()
        parquet_manifest_path.write_text(json.dumps(parquet_manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(MANIFEST)
    print(OUTPUT)


if __name__ == "__main__":
    main()
