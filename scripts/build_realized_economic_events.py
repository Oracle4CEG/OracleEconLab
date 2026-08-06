#!/usr/bin/env python3
"""Build source- and transaction-gated reward/slash evidence tables.

The wide evidence table keeps accruals, entitlements, parameters and accounting
events.  The strict table keeps only observed payments or applied stake/principal
changes.  This prevents event-name based inflation and, in particular, prevents
UMA VoterSlashed and VoterSlashApplied from being counted twice.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
PARQUET = (ROOT / "data/curated/parquet").resolve()
EVIDENCE_OUTPUT = PARQUET / "economic_semantics_events.parquet"
REALIZED_OUTPUT = PARQUET / "realized_reward_slash_events.parquet"
MANIFEST = ROOT / "data/manifests/realized_reward_slash_events.json"
REPORT = ROOT / "reports/realized_reward_slash_audit.md"
SOURCE_AUDIT = ROOT / "data/manifests/contract_semantics_audit.json"
LINK = "0x514910771af9ca656af840dff83e8264ecf986ca"
UMA = "0x04fa0d235c4abf4bcf4787af4cf447de572ef828"
PYTH_OIS = "pyti8TM4zRVBjmarcgAPmTNNAXYKJv7WVHrkrm6woLN"
WDIA = "0x9f5da8630d47178bab71f5923644a28b15cbdca7"


COLUMNS = [
    "evidence_id", "oracle_network", "security_chain", "mechanism", "economic_kind",
    "economic_evidence_class", "realization_status", "actor", "counterparty", "amount_raw",
    "signed_amount_raw", "asset", "asset_decimals", "source_event", "source_tx", "source_block",
    "source_log_index", "source_contract", "source_table", "tx_or_state_evidence",
    "cashflow_verified", "state_delta_verified", "source_semantics_rule_id", "source_confidence",
    "include_in_realized_reward", "include_in_realized_slash", "do_not_sum_group",
    "interpretation_note",
]


def source(name: str) -> str:
    path = PARQUET / f"{name}.parquet"
    if not path.is_file():
        raise RuntimeError(f"missing required input: {path}")
    return f"read_parquet('{path}')"


def decimal_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def build_views(connection: duckdb.DuckDBPyConnection) -> None:
    rounds = source("polygon_uma_request_rounds")
    uma_flows = source("polygon_uma_token_flows")
    uma_flow_qc = source("polygon_uma_request_flow_qc")
    payoffs = source("uma_dvm_voter_payoffs")
    dvm_staking = source("uma_dvm_staking_events")
    chainlink = source("chainlink_staking_v02_events")
    chainlink_qc = source("chainlink_event_link_flow_qc")
    tellor_disputes = source("tellor_disputes")
    tellor_payments = source("tellor_dispute_payments")
    tellor_withdrawals = source("tellor_tip_withdrawals_realized")
    tellor_current_accruals = source("tellor_reporter_reward_accruals_full")
    tellor_legacy_accruals = source("tellor_legacy_selector_reward_accruals")
    flare_claims = source("flare_reward_claims")
    flare_claim_events = source("flare_reward_claim_events")
    pyth_factors = source("pyth_ois_publisher_epoch_factors")
    pyth_economic = source("pyth_ois_economic_events")
    chronicle_events = source("chronicle_ethereum_events")
    dia_withdrawals = source("dia_staking_withdrawals")

    connection.execute(f"""
        CREATE TEMP VIEW uma_round_evidence AS
        SELECT r.*, q.settlement_flow_exact,
          CASE
            WHEN r.economic_status='settled_disputed_proposer_wins' THEN r.disputer
            WHEN r.economic_status='settled_disputed_disputer_wins' THEN r.proposer
          END loser,
          CASE
            WHEN r.economic_status='settled_disputed_proposer_wins' THEN r.dispute_tx
            WHEN r.economic_status='settled_disputed_disputer_wins' THEN r.proposal_tx
          END loser_funding_tx,
          EXISTS (
            SELECT 1 FROM {uma_flows} i
            WHERE lower(i.source_tx) = lower(CASE
                    WHEN r.economic_status='settled_disputed_proposer_wins' THEN r.dispute_tx
                    WHEN r.economic_status='settled_disputed_disputer_wins' THEN r.proposal_tx END)
              AND lower(i.sender) = lower(CASE
                    WHEN r.economic_status='settled_disputed_proposer_wins' THEN r.disputer
                    WHEN r.economic_status='settled_disputed_disputer_wins' THEN r.proposer END)
              AND lower(i.receiver) = lower(r.source_contract)
              AND lower(i.token) = lower(r.currency)
              AND try_cast(i.amount_raw AS HUGEINT) =
                    try_cast(r.effective_bond_raw AS HUGEINT) + try_cast(r.final_fee_raw AS HUGEINT)
          ) AS loser_input_exact
        FROM {rounds} r
        LEFT JOIN {uma_flow_qc} q USING (oo_request_id)
    """)

    views: list[str] = []
    def add(name: str, sql: str) -> None:
        connection.execute(f"CREATE TEMP VIEW {name} AS {sql}")
        views.append(name)

    add("uma_oov2_rewards", f"""
        SELECT
          sha256(concat_ws('|','uma_oov2_reward',oo_request_id,settlement_tx)) evidence_id,
          'UMA' oracle_network, 'Polygon' security_chain, 'OptimisticOracleV2' mechanism,
          'reward' economic_kind, 'paid_reward_embedded_in_settlement' economic_evidence_class,
          'paid' realization_status,
          CASE WHEN economic_status='settled_disputed_disputer_wins' THEN disputer ELSE proposer END actor,
          CASE WHEN economic_status='settled_disputed_disputer_wins' THEN proposer
               WHEN economic_status='settled_disputed_proposer_wins' THEN disputer ELSE requester END counterparty,
          CASE WHEN economic_status='settled_undisputed' THEN explicit_report_reward_raw
               ELSE dispute_winner_reward_raw END amount_raw,
          CASE WHEN economic_status='settled_undisputed' THEN explicit_report_reward_raw
               ELSE dispute_winner_reward_raw END signed_amount_raw,
          currency asset, 6::SMALLINT asset_decimals, 'Settle' source_event,
          settlement_tx source_tx, settlement_block source_block, NULL::BIGINT source_log_index,
          source_contract, 'polygon_uma_request_rounds' source_table,
          'Settle gross payout equals observed ERC20 outflow; source code decomposes principal and reward' tx_or_state_evidence,
          settlement_flow_exact cashflow_verified, settlement_flow_exact state_delta_verified,
          'UMA_OOV2_SETTLEMENT_PAYMENT_V1' source_semantics_rule_id, 'B' source_confidence,
          settlement_flow_exact include_in_realized_reward, false include_in_realized_slash,
          concat('uma_oov2:',oo_request_id,':settlement') do_not_sum_group,
          'Reward is a component of the exact gross settlement transfer, not a separate ERC20 transfer.' interpretation_note
        FROM uma_round_evidence
        WHERE status='settled'
          AND try_cast(CASE WHEN economic_status='settled_undisputed' THEN explicit_report_reward_raw
                            ELSE dispute_winner_reward_raw END AS HUGEINT) > 0
    """)

    for suffix, amount_column, semantic_class, rule_id, note in [
        ("bond", "bond_forfeited_raw", "realized_bond_forfeiture", "UMA_OOV2_BOND_FORFEITURE_V1", "Loser bond loss is source-derived and the losing funding input plus settlement outflow reconcile."),
        ("fee", "final_fee_forfeited_raw", "realized_final_fee_forfeiture", "UMA_OOV2_BOND_FORFEITURE_V1", "Loser final fee is sent to the UMA Store during dispute and is not returned at settlement."),
    ]:
        add(f"uma_oov2_{suffix}_penalties", f"""
            SELECT
              sha256(concat_ws('|','uma_oov2_{suffix}',oo_request_id,settlement_tx)) evidence_id,
              'UMA' oracle_network, 'Polygon' security_chain, 'OptimisticOracleV2' mechanism,
              'penalty' economic_kind, '{semantic_class}' economic_evidence_class,
              'realized' realization_status, loser actor,
              CASE WHEN economic_status='settled_disputed_disputer_wins' THEN disputer ELSE proposer END counterparty,
              {amount_column} amount_raw, concat('-', {amount_column}) signed_amount_raw,
              currency asset, 6::SMALLINT asset_decimals, 'Settle' source_event,
              settlement_tx source_tx, settlement_block source_block, NULL::BIGINT source_log_index,
              source_contract, 'polygon_uma_request_rounds' source_table,
              'Loser funding ERC20 input and winner gross settlement ERC20 output both reconcile' tx_or_state_evidence,
              (loser_input_exact AND settlement_flow_exact) cashflow_verified,
              (loser_input_exact AND settlement_flow_exact) state_delta_verified,
              '{rule_id}' source_semantics_rule_id, 'B' source_confidence,
              false include_in_realized_reward,
              (loser_input_exact AND settlement_flow_exact) include_in_realized_slash,
              concat('uma_oov2:',oo_request_id,':{suffix}') do_not_sum_group,
              '{note}' interpretation_note
            FROM uma_round_evidence
            WHERE economic_status LIKE 'settled_disputed_%'
              AND try_cast({amount_column} AS HUGEINT) > 0
        """)

    add("uma_dvm_accruals", f"""
        SELECT
          sha256(concat_ws('|','uma_dvm_accrual',source_tx,log_index::VARCHAR)) evidence_id,
          'UMA' oracle_network, 'Ethereum' security_chain, 'VotingV2' mechanism,
          CASE WHEN try_cast(signed_slash_delta_raw AS HUGEINT) > 0 THEN 'reward' ELSE 'slash' END economic_kind,
          CASE WHEN try_cast(signed_slash_delta_raw AS HUGEINT) > 0 THEN 'accrued_stake_redistribution'
               WHEN try_cast(signed_slash_delta_raw AS HUGEINT) < 0 THEN 'calculated_stake_slash'
               ELSE 'zero_stake_delta' END economic_evidence_class,
          'accrued_not_applied' realization_status, voter actor, NULL::VARCHAR counterparty,
          CASE WHEN try_cast(signed_slash_delta_raw AS HUGEINT) < 0 THEN wrong_or_no_vote_slash_raw
               ELSE correct_vote_redistribution_raw END amount_raw,
          signed_slash_delta_raw signed_amount_raw, '{UMA}' asset, 18::SMALLINT asset_decimals,
          event source_event, source_tx, source_block, log_index source_log_index, source_contract,
          'uma_dvm_voter_payoffs' source_table,
          'VoterSlashed adds signed delta to unappliedSlash; no stake balance mutation yet' tx_or_state_evidence,
          false cashflow_verified, false state_delta_verified,
          'UMA_DVM_VOTER_SLASH_ACCRUAL_V1' source_semantics_rule_id, 'A' source_confidence,
          false include_in_realized_reward, false include_in_realized_slash,
          concat('uma_dvm:',coalesce(dvm_request_id,request_index),':',voter) do_not_sum_group,
          'Never sum this request-level accrual with VoterSlashApplied.' interpretation_note
        FROM {payoffs}
    """)

    add("uma_dvm_applied", f"""
        SELECT
          sha256(concat_ws('|','uma_dvm_applied',source_tx,log_index::VARCHAR)) evidence_id,
          'UMA' oracle_network, 'Ethereum' security_chain, 'VotingV2' mechanism,
          CASE WHEN try_cast(raw_data_words[1] AS HUGEINT) > 0 THEN 'reward'
               WHEN try_cast(raw_data_words[1] AS HUGEINT) < 0 THEN 'slash' ELSE 'state_delta' END economic_kind,
          CASE WHEN try_cast(raw_data_words[1] AS HUGEINT) > 0 THEN 'applied_net_stake_increase'
               WHEN try_cast(raw_data_words[1] AS HUGEINT) < 0 THEN 'applied_net_stake_slash'
               ELSE 'zero_applied_stake_delta' END economic_evidence_class,
          'applied' realization_status, actor, NULL::VARCHAR counterparty,
          abs(try_cast(raw_data_words[1] AS HUGEINT))::VARCHAR amount_raw,
          raw_data_words[1] signed_amount_raw, '{UMA}' asset, 18::SMALLINT asset_decimals,
          event source_event, source_tx, source_block, log_index source_log_index, source_contract,
          'uma_dvm_staking_events' source_table,
          concat('VotingV2 stake mutated; event postStake=',raw_data_words[2]) tx_or_state_evidence,
          false cashflow_verified, true state_delta_verified,
          'UMA_DVM_VOTER_SLASH_APPLIED_V1' source_semantics_rule_id, 'A' source_confidence,
          try_cast(raw_data_words[1] AS HUGEINT) > 0 include_in_realized_reward,
          try_cast(raw_data_words[1] AS HUGEINT) < 0 include_in_realized_slash,
          concat('uma_dvm_applied:',source_tx,':',log_index::VARCHAR) do_not_sum_group,
          'This is the realized aggregate net stake change; underlying VoterSlashed rows remain accrual-only.' interpretation_note
        FROM {dvm_staking}
        WHERE event='VoterSlashApplied'
    """)

    add("chainlink_paid_rewards", f"""
        SELECT
          sha256(concat_ws('|','chainlink_reward',e.source_tx,e.log_index::VARCHAR)) evidence_id,
          'Chainlink' oracle_network, 'Ethereum' security_chain, 'Staking v0.2 RewardVault' mechanism,
          'reward' economic_kind, 'paid_reward' economic_evidence_class, 'paid' realization_status,
          e.staker actor, e.source_contract counterparty, e.reward_claimed_raw amount_raw,
          e.reward_claimed_raw signed_amount_raw, '{LINK}' asset, 18::SMALLINT asset_decimals,
          e.event source_event, e.source_tx, e.source_block, e.log_index source_log_index,
          e.source_contract, 'chainlink_staking_v02_events' source_table,
          concat('Observed LINK transfer RewardVault->staker amount=',q.observed_link_flow_raw) tx_or_state_evidence,
          q.flow_exact cashflow_verified, q.flow_exact state_delta_verified,
          'CHAINLINK_REWARD_CLAIM_PAYMENT_V1' source_semantics_rule_id, 'A' source_confidence,
          q.flow_exact include_in_realized_reward, false include_in_realized_slash,
          concat('chainlink:',e.source_tx,':',e.log_index::VARCHAR) do_not_sum_group,
          'Verified source transfers LINK before emitting RewardClaimed; transfer amount matches the event exactly.' interpretation_note
        FROM {chainlink} e
        JOIN {chainlink_qc} q
          ON e.event=q.event AND e.source_tx=q.source_tx AND lower(e.staker)=lower(q.actor)
        WHERE e.event='RewardClaimed'
    """)

    add("chainlink_accounting", f"""
        SELECT
          sha256(concat_ws('|','chainlink_accounting',source_tx,log_index::VARCHAR,event)) evidence_id,
          'Chainlink' oracle_network, 'Ethereum' security_chain, 'Staking v0.2 RewardVault' mechanism,
          'accounting' economic_kind,
          CASE WHEN event='ForfeitedRewardDistributed' THEN 'accounting_redistribution'
               ELSE 'reward_eligibility_finalization' END economic_evidence_class,
          'not_paid' realization_status, staker actor, source_contract counterparty,
          CASE WHEN event='ForfeitedRewardDistributed' THEN vested_reward_raw ELSE NULL END amount_raw,
          NULL::VARCHAR signed_amount_raw, '{LINK}' asset, 18::SMALLINT asset_decimals,
          event source_event, source_tx, source_block, log_index source_log_index, source_contract,
          'chainlink_staking_v02_events' source_table,
          'No LINK transfer expected for this internal reward-accounting event' tx_or_state_evidence,
          false cashflow_verified, event='ForfeitedRewardDistributed' state_delta_verified,
          'CHAINLINK_FORFEITURE_ACCOUNTING_V1' source_semantics_rule_id, 'A' source_confidence,
          false include_in_realized_reward, false include_in_realized_slash,
          concat('chainlink_accounting:',source_tx,':',log_index::VARCHAR) do_not_sum_group,
          'Forfeiture changes unvested reward accounting; it is neither a paid reward nor a principal slash.' interpretation_note
        FROM {chainlink}
        WHERE event='ForfeitedRewardDistributed' OR (event='RewardFinalized' AND reward_forfeited)
    """)

    add("chainlink_alert_parameters", f"""
        SELECT
          sha256(concat_ws('|','chainlink_alert_parameter',source_tx,log_index::VARCHAR,parameter_kind)) evidence_id,
          'Chainlink' oracle_network, 'Ethereum' security_chain, 'Staking v0.2 alert controller' mechanism,
          'parameter' economic_kind,
          CASE WHEN parameter_kind='operator_slash' THEN 'designed_slash_parameter'
               ELSE 'designed_alert_reward_parameter' END economic_evidence_class,
          'configured_not_executed' realization_status, feed actor, source_contract counterparty,
          CASE WHEN parameter_kind='operator_slash' THEN operator_slash_amount_raw
               ELSE alerter_reward_amount_raw END amount_raw,
          NULL::VARCHAR signed_amount_raw, '{LINK}' asset, 18::SMALLINT asset_decimals,
          event source_event, source_tx, source_block, log_index source_log_index, source_contract,
          'chainlink_staking_v02_events' source_table,
          concat('FeedConfigSet transaction; priority/regular thresholds=',threshold_1_seconds::VARCHAR,'/',threshold_2_seconds::VARCHAR,' seconds') tx_or_state_evidence,
          false cashflow_verified, true state_delta_verified,
          'CHAINLINK_ALERT_CONFIG_PARAMETER_V1' source_semantics_rule_id, 'A' source_confidence,
          false include_in_realized_reward, false include_in_realized_slash,
          concat('chainlink_alert_config:',feed,':',parameter_kind) do_not_sum_group,
          'An active configured amount is not a slash or reward until slashAndReward mutates principal and pays the alerter.' interpretation_note
        FROM {chainlink}
        CROSS JOIN (VALUES ('operator_slash'),('alerter_reward')) kinds(parameter_kind)
        WHERE event='FeedConfigSet'
    """)

    add("tellor_paid_rewards", f"""
        SELECT
          sha256(concat_ws('|','tellor_reward',source_tx,dispute_id,actor)) evidence_id,
          'Tellor' oracle_network, 'Tellor Layer' security_chain, 'Dispute module' mechanism,
          'reward' economic_kind, 'paid_reward' economic_evidence_class, 'paid' realization_status,
          actor, 'dispute_module' counterparty, received_loya_raw amount_raw,
          received_loya_raw signed_amount_raw, 'loya' asset, 6::SMALLINT asset_decimals,
          event source_event, source_tx, source_block, NULL::BIGINT source_log_index,
          'x/dispute' source_contract, 'tellor_dispute_payments' source_table,
          'Transaction receipt shows loya received by claimant; source calls SendCoinsFromModuleToAccount' tx_or_state_evidence,
          true cashflow_verified, true state_delta_verified,
          'TELLOR_VOTER_REWARD_PAYMENT_V1' source_semantics_rule_id, 'B' source_confidence,
          true include_in_realized_reward, false include_in_realized_slash,
          concat('tellor:',source_tx,':',dispute_id) do_not_sum_group,
          'Observed bank-module payment, not merely a calculated voter reward pool.' interpretation_note
        FROM {tellor_payments}
        WHERE event='MsgClaimReward' AND try_cast(received_loya_raw AS HUGEINT) > 0
    """)

    add("tellor_realized_slashes", f"""
        SELECT
          sha256(concat_ws('|','tellor_reporter_slash',dispute_id,source_tx)) evidence_id,
          'Tellor' oracle_network, 'Tellor Layer' security_chain, 'Dispute module' mechanism,
          'slash' economic_kind, 'realized_principal_slash' economic_evidence_class,
          'finalized' realization_status, reporter actor, disputer counterparty,
          slash_amount_raw amount_raw, concat('-',slash_amount_raw) signed_amount_raw,
          asset, asset_decimals::SMALLINT asset_decimals, 'dispute_executed' source_event,
          source_tx, source_block, NULL::BIGINT source_log_index, 'x/dispute' source_contract,
          'tellor_disputes' source_table,
          concat('Resolved outcome=',vote_result,'; SUPPORT does not call ReturnSlashedTokens') tx_or_state_evidence,
          false cashflow_verified, true state_delta_verified,
          'TELLOR_REPORTER_SLASH_FINAL_V1' source_semantics_rule_id, 'B' source_confidence,
          false include_in_realized_reward, true include_in_realized_slash,
          concat('tellor_dispute:',dispute_id,':reporter_slash') do_not_sum_group,
          'Stake was escrowed at dispute funding and remains slashed only for SUPPORT outcomes.' interpretation_note
        FROM {tellor_disputes}
        WHERE vote_result IN ('SUPPORT','NO_QUORUM_MAJORITY_SUPPORT')
          AND try_cast(slash_amount_raw AS HUGEINT) > 0
    """)

    add("tellor_current_reward_accruals", f"""
        SELECT
          sha256(concat_ws('|','tellor_reward_accrual',height::VARCHAR,event_index::VARCHAR,reporter)) evidence_id,
          'Tellor' oracle_network, 'Tellor Layer' security_chain, 'Reporter rewards' mechanism,
          'reward' economic_kind, 'accrued_reporter_period_reward' economic_evidence_class,
          'accrued_not_paid' realization_status, reporter actor, 'tips_escrow' counterparty,
          gross_reward_loya_decimal amount_raw, gross_reward_loya_decimal signed_amount_raw,
          'loya' asset, 6::SMALLINT asset_decimals, 'rewards_accumulated' source_event,
          NULL::VARCHAR source_tx, height source_block, event_index source_log_index,
          'x/reporter' source_contract, 'tellor_reporter_reward_accruals_full' source_table,
          concat('reward_source=',reward_source,'; commission=',commission_loya_decimal,
                 '; net=',net_reward_loya_decimal) tx_or_state_evidence,
          false cashflow_verified, true state_delta_verified,
          'TELLOR_REWARD_ACCRUAL_V1' source_semantics_rule_id, 'B' source_confidence,
          false include_in_realized_reward, false include_in_realized_slash,
          concat('tellor_accrual:',height::VARCHAR,':',event_index::VARCHAR) do_not_sum_group,
          'Reporter-period accrual is retained separately from a selector withdrawal.' interpretation_note
        FROM {tellor_current_accruals}
    """)

    add("tellor_legacy_reward_accruals", f"""
        SELECT
          sha256(concat_ws('|','tellor_legacy_reward_accrual',height::VARCHAR,event_index::VARCHAR,
                 selector_event_value_utf8_lossy)) evidence_id,
          'Tellor' oracle_network, 'Tellor Layer' security_chain, 'Legacy selector tips' mechanism,
          'reward' economic_kind, 'accrued_selector_tip_reward' economic_evidence_class,
          'accrued_not_paid' realization_status, selector_event_value_utf8_lossy actor,
          'tips_escrow' counterparty,
          incremental_reward_loya_raw amount_raw, incremental_reward_loya_raw signed_amount_raw,
          'loya' asset, 6::SMALLINT asset_decimals, 'rewards_added' source_event,
          NULL::VARCHAR source_tx, height source_block, event_index source_log_index,
          'x/reporter' source_contract, 'tellor_legacy_selector_reward_accruals' source_table,
          concat('post-update cumulative selector balance=',cumulative_selector_tips_loya_decimal) tx_or_state_evidence,
          false cashflow_verified, true state_delta_verified,
          rule_id source_semantics_rule_id, 'B' source_confidence,
          false include_in_realized_reward, false include_in_realized_slash,
          concat('tellor_legacy_accrual:',height::VARCHAR,':',event_index::VARCHAR) do_not_sum_group,
          'Incremental reward is an escrow/accounting accrual, not yet a wallet or stake payment.' interpretation_note
        FROM {tellor_legacy_accruals}
        WHERE incremental_reward_observable
          AND incremental_reward_loya_raw IS NOT NULL
    """)

    add("tellor_tip_withdrawal_payments", f"""
        SELECT
          sha256(concat_ws('|','tellor_tip_withdrawal',source_tx,event_index::VARCHAR,selector)) evidence_id,
          'Tellor' oracle_network, 'Tellor Layer' security_chain, 'Reporter tips escrow' mechanism,
          'reward' economic_kind, 'paid_reward_compounded_to_stake' economic_evidence_class,
          'paid_to_stake' realization_status, selector actor, validator counterparty,
          reward_withdrawn_to_stake_loya_raw amount_raw,
          reward_withdrawn_to_stake_loya_raw signed_amount_raw,
          asset, asset_decimals::SMALLINT asset_decimals, 'tip_withdrawn' source_event,
          source_tx, height source_block, event_index source_log_index,
          'x/reporter' source_contract, 'tellor_tip_withdrawals_realized' source_table,
          concat('TipsEscrow coin_spent matched once; new validator shares=',new_validator_shares) tx_or_state_evidence,
          cashflow_verified, cashflow_verified state_delta_verified,
          'TELLOR_TIP_WITHDRAWAL_PAYMENT_V1' source_semantics_rule_id, 'B' source_confidence,
          cashflow_verified include_in_realized_reward, false include_in_realized_slash,
          concat('tellor_tip_withdrawal:',source_tx,':',event_index::VARCHAR) do_not_sum_group,
          'A settled selector reward was moved from tips escrow into bonded stake.' interpretation_note
        FROM {tellor_withdrawals}
    """)

    add("flare_entitlements", f"""
        SELECT
          sha256(concat_ws('|','flare_entitlement',reward_epoch_id::VARCHAR,claim_index::VARCHAR,beneficiary)) evidence_id,
          'Flare' oracle_network, 'Flare Mainnet' security_chain, 'FSP RewardManager' mechanism,
          'reward' economic_kind, 'claimable_entitlement' economic_evidence_class,
          'claimable_not_observed_paid' realization_status, beneficiary actor, NULL::VARCHAR counterparty,
          amount_raw, amount_raw signed_amount_raw, asset, asset_decimals::SMALLINT asset_decimals,
          'MerkleRewardLeaf' source_event, NULL::VARCHAR source_tx, NULL::BIGINT source_block,
          claim_index source_log_index, 'RewardManager' source_contract, 'flare_reward_claims' source_table,
          concat('Merkle proof/root reconciled; claim transaction absent; root=',merkle_root) tx_or_state_evidence,
          false cashflow_verified, true state_delta_verified,
          'FLARE_MERKLE_ENTITLEMENT_V1' source_semantics_rule_id, 'B' source_confidence,
          false include_in_realized_reward, false include_in_realized_slash,
          concat('flare:',reward_epoch_id::VARCHAR,':',claim_index::VARCHAR) do_not_sum_group,
          'A Merkle entitlement is not evidence that claim() transferred or wrapped FLR.' interpretation_note
        FROM {flare_claims}
    """)

    add("flare_realized_claims", f"""
        SELECT
          sha256(concat_ws('|','flare_reward_claim',source_tx,source_log_index::VARCHAR)) evidence_id,
          'Flare' oracle_network, 'Flare Mainnet' security_chain, 'RewardManager' mechanism,
          'reward' economic_kind, 'paid_reward_claim' economic_evidence_class,
          'paid_or_wrapped' realization_status, recipient actor, reward_owner counterparty,
          amount_raw, amount_raw signed_amount_raw, asset, asset_decimals::SMALLINT asset_decimals,
          'RewardClaimed' source_event, source_tx, source_block,
          source_log_index, source_contract, 'flare_reward_claim_events' source_table,
          concat('successful RewardManager claim; claim_type=',claim_type,
                 '; beneficiary=',beneficiary) tx_or_state_evidence,
          true cashflow_verified, true state_delta_verified,
          'FLARE_REWARD_PAYMENT_V1' source_semantics_rule_id, 'B' source_confidence,
          true include_in_realized_reward, false include_in_realized_slash,
          concat('flare_claim:',source_tx,':',source_log_index::VARCHAR) do_not_sum_group,
          'Fee-burn claims are excluded; every retained row is an emitted successful claim.' interpretation_note
        FROM {flare_claim_events}
        WHERE NOT is_fee_burn
    """)

    add("pyth_reward_parameters", f"""
        SELECT
          sha256(concat_ws('|','pyth_factor',epoch_id::VARCHAR,publisher)) evidence_id,
          'Pyth' oracle_network, 'Solana Mainnet' security_chain, 'Oracle Integrity Staking' mechanism,
          'parameter' economic_kind, 'reward_parameter' economic_evidence_class,
          'parameter_not_payment' realization_status, publisher actor, NULL::VARCHAR counterparty,
          reward_rate_y_raw amount_raw, NULL::VARCHAR signed_amount_raw, 'rate_fixed_6' asset,
          rate_decimals::SMALLINT asset_decimals, 'PublisherEpochFactor' source_event,
          NULL::VARCHAR source_tx, NULL::BIGINT source_block, publisher_index source_log_index,
          source_account source_contract, 'pyth_ois_publisher_epoch_factors' source_table,
          concat('On-chain factor/rate state; no claim token transfer collected; epoch=',epoch_id::VARCHAR) tx_or_state_evidence,
          false cashflow_verified, true state_delta_verified,
          'PYTH_REWARD_FACTOR_PARAMETER_V1' source_semantics_rule_id, 'B' source_confidence,
          false include_in_realized_reward, false include_in_realized_slash,
          concat('pyth:',epoch_id::VARCHAR,':',publisher) do_not_sum_group,
          'Reward rate and eligibility are calculation inputs, not PYTH paid to a wallet.' interpretation_note
        FROM {pyth_factors}
    """)

    add("pyth_realized_reward_transfers", f"""
        SELECT
          sha256(concat_ws('|','pyth_reward_transfer',signature,
                 outer_instruction_index::VARCHAR,inner_transfer_index::VARCHAR)) evidence_id,
          'Pyth' oracle_network, 'Solana Mainnet' security_chain, 'Oracle Integrity Staking' mechanism,
          'reward' economic_kind, 'paid_reward_to_stake_custody' economic_evidence_class,
          'paid_to_stake_custody' realization_status, beneficiary actor,
          source_token_account counterparty, amount_raw, amount_raw signed_amount_raw,
          asset, asset_decimals::SMALLINT asset_decimals, 'reward_transfer' source_event,
          signature source_tx, slot source_block,
          (outer_instruction_index * 1000000 + inner_transfer_index) source_log_index,
          '{PYTH_OIS}' source_contract, 'pyth_ois_economic_events' source_table,
          concat('SPL transfer ',source_token_account,' -> ',destination_token_account,
                 '; role=',reward_role) tx_or_state_evidence,
          true cashflow_verified, true state_delta_verified,
          'PYTH_REWARD_PAYMENT_V1' source_semantics_rule_id, 'B' source_confidence,
          true include_in_realized_reward, false include_in_realized_slash,
          concat('pyth_reward:',signature,':',outer_instruction_index::VARCHAR,':',
                 inner_transfer_index::VARCHAR) do_not_sum_group,
          'Reward requires the AdvanceDelegationRecord inner SPL-token transfer.' interpretation_note
        FROM {pyth_economic}
        WHERE event='reward_transfer'
    """)

    add("pyth_realized_slash_transfers", f"""
        SELECT
          sha256(concat_ws('|','pyth_slash_transfer',signature,
                 outer_instruction_index::VARCHAR,inner_transfer_index::VARCHAR)) evidence_id,
          'Pyth' oracle_network, 'Solana Mainnet' security_chain, 'Oracle Integrity Staking' mechanism,
          'slash' economic_kind, 'realized_principal_slash' economic_evidence_class,
          'applied' realization_status, beneficiary actor,
          destination_token_account counterparty, amount_raw, concat('-',amount_raw) signed_amount_raw,
          asset, asset_decimals::SMALLINT asset_decimals, 'principal_slash_transfer' source_event,
          signature source_tx, slot source_block,
          (outer_instruction_index * 1000000 + inner_transfer_index) source_log_index,
          '{PYTH_OIS}' source_contract, 'pyth_ois_economic_events' source_table,
          concat('SPL transfer ',source_token_account,' -> slash custody ',
                 destination_token_account) tx_or_state_evidence,
          true cashflow_verified, true state_delta_verified,
          'PYTH_SLASH_APPLIED_V1' source_semantics_rule_id, 'B' source_confidence,
          false include_in_realized_reward, true include_in_realized_slash,
          concat('pyth_slash:',signature,':',outer_instruction_index::VARCHAR,':',
                 inner_transfer_index::VARCHAR) do_not_sum_group,
          'CreateSlashEvent parameters are excluded; only stake-to-slash-custody transfers enter.' interpretation_note
        FROM {pyth_economic}
        WHERE event='principal_slash_transfer'
    """)

    add("chronicle_challenge_rewards", f"""
        SELECT
          sha256(concat_ws('|','chronicle_challenge_reward',transaction_hash,log_index::VARCHAR)) evidence_id,
          'Chronicle' oracle_network, 'Ethereum' security_chain, 'ScribeOptimistic' mechanism,
          'reward' economic_kind, 'paid_invalid_report_challenge_reward' economic_evidence_class,
          'paid' realization_status, challenger actor, contract_address counterparty,
          reward_amount_raw amount_raw, reward_amount_raw signed_amount_raw,
          reward_asset asset, reward_asset_decimals::SMALLINT asset_decimals,
          event_name source_event, transaction_hash source_tx, block_number source_block,
          log_index source_log_index, contract_address source_contract,
          'chronicle_ethereum_events' source_table,
          'OpChallengeRewardPaid is emitted only after the ETH send returns success' tx_or_state_evidence,
          true cashflow_verified, true state_delta_verified,
          'CHRONICLE_OP_CHALLENGE_PAYMENT_V1' source_semantics_rule_id, 'B' source_confidence,
          true include_in_realized_reward, false include_in_realized_slash,
          concat('chronicle_reward:',transaction_hash,':',log_index::VARCHAR) do_not_sum_group,
          'Successful challenge/drop without this payment event remains a nonmonetary result.' interpretation_note
        FROM {chronicle_events}
        WHERE event_name='OpChallengeRewardPaid'
    """)

    add("dia_realized_staking_rewards", f"""
        SELECT
          sha256(concat_ws('|','dia_staking_reward',transaction_hash,
                 staking_store_index::VARCHAR)) evidence_id,
          'DIA' oracle_network, 'DIA Lasernet' security_chain,
          'DIAExternalStaking' mechanism,
          'reward' economic_kind, 'paid_base_staking_reward' economic_evidence_class,
          'paid' realization_status, beneficiary actor,
          source_contract counterparty, total_reward_raw amount_raw,
          total_reward_raw signed_amount_raw, '{WDIA}' asset,
          18::SMALLINT asset_decimals, 'unstake+wDIA Transfer' source_event,
          transaction_hash source_tx, block_number source_block,
          NULL::BIGINT source_log_index, source_contract,
          'dia_staking_withdrawals' source_table,
          'Historical stakingStores principal/reward fields at block-1 sum exactly to outgoing wDIA transfers' tx_or_state_evidence,
          payment_exact cashflow_verified, payment_exact state_delta_verified,
          'DIA_LASERNET_UNSTAKE_REWARD_DECOMPOSITION_V1' source_semantics_rule_id,
          'A' source_confidence, payment_exact include_in_realized_reward,
          false include_in_realized_slash,
          concat('dia_staking:',staking_store_index::VARCHAR,':',transaction_hash) do_not_sum_group,
          'Only the two source-defined reward fields enter reward totals; returned principal is excluded.' interpretation_note
        FROM {dia_withdrawals}
        WHERE try_cast(total_reward_raw AS HUGEINT) > 0
    """)

    union_sql = " UNION ALL ".join(f"SELECT {','.join(COLUMNS)} FROM {view}" for view in views)
    connection.execute(f"CREATE TEMP VIEW all_economic_semantics AS {union_sql}")


def summarize(connection: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, object]]:
    rows = connection.execute(f"""
        SELECT oracle_network, economic_evidence_class, asset, asset_decimals,
               count(*) row_count,
               sum(try_cast(amount_raw AS HUGEINT))::VARCHAR amount_raw
        FROM {table}
        GROUP BY ALL ORDER BY oracle_network, economic_evidence_class, asset
    """).fetchall()
    return [
        {
            "oracle_network": row[0], "economic_evidence_class": row[1], "asset": row[2],
            "asset_decimals": row[3], "row_count": row[4], "amount_raw": decimal_string(row[5]),
        }
        for row in rows
    ]


def main() -> None:
    if not SOURCE_AUDIT.is_file():
        raise RuntimeError("run scripts/build_contract_semantics_audit.py first")
    source_audit = json.loads(SOURCE_AUDIT.read_text(encoding="utf-8"))
    available_rules = {row["rule_id"] for row in source_audit["rules"]}

    PARQUET.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    build_views(connection)
    used_rules = {row[0] for row in connection.execute("SELECT DISTINCT source_semantics_rule_id FROM all_economic_semantics").fetchall()}
    missing_rules = sorted(used_rules - available_rules)
    if missing_rules:
        raise RuntimeError(f"economic rows use unaudited semantic rules: {missing_rules}")

    connection.execute(f"COPY (SELECT * FROM all_economic_semantics) TO '{EVIDENCE_OUTPUT}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    connection.execute(f"""
        COPY (
          SELECT * FROM all_economic_semantics
          WHERE include_in_realized_reward OR include_in_realized_slash
        ) TO '{REALIZED_OUTPUT}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    evidence = f"read_parquet('{EVIDENCE_OUTPUT}')"
    realized = f"read_parquet('{REALIZED_OUTPUT}')"

    dvm_accrued_net_raw = connection.execute(
        f"SELECT sum(try_cast(signed_amount_raw AS HUGEINT))::VARCHAR FROM {evidence} WHERE source_event='VoterSlashed'"
    ).fetchone()[0]
    dvm_applied_net_raw = connection.execute(
        f"SELECT sum(try_cast(signed_amount_raw AS HUGEINT))::VARCHAR FROM {evidence} WHERE source_event='VoterSlashApplied'"
    ).fetchone()[0]
    qc = {
        "voter_slashed_rows_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE source_event='VoterSlashed'").fetchone()[0],
        "voter_slash_applied_rows_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE source_event='VoterSlashApplied'").fetchone()[0],
        "dvm_voter_slashed_accrued_net_raw": dvm_accrued_net_raw,
        "dvm_voter_slash_applied_net_raw": dvm_applied_net_raw,
        "dvm_accrued_net_equals_applied_net": dvm_accrued_net_raw == dvm_applied_net_raw,
        "uma_dispute_penalty_rows_not_flow_exact": connection.execute(
            f"SELECT count(*) FROM {evidence} WHERE economic_evidence_class IN ('realized_bond_forfeiture','realized_final_fee_forfeiture') AND NOT cashflow_verified"
        ).fetchone()[0],
        "chainlink_claims_not_flow_exact": connection.execute(f"SELECT count(*) FROM {evidence} WHERE oracle_network='Chainlink' AND source_event='RewardClaimed' AND NOT cashflow_verified").fetchone()[0],
        "chainlink_realized_slash_rows": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='Chainlink' AND economic_kind='slash'").fetchone()[0],
        "flare_rows_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='Flare'").fetchone()[0],
        "pyth_rows_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='Pyth'").fetchone()[0],
        "tellor_tip_withdrawals_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE source_table='tellor_tip_withdrawals_realized'").fetchone()[0],
        "chronicle_rewards_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='Chronicle'").fetchone()[0],
        "dia_rewards_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='DIA'").fetchone()[0],
        "dia_rewards_not_payment_exact": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='DIA' AND NOT cashflow_verified").fetchone()[0],
        "flare_fee_burns_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='Flare' AND actor='0x000000000000000000000000000000000000dead'").fetchone()[0],
        "pyth_parameter_rows_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='Pyth' AND economic_evidence_class='reward_parameter'").fetchone()[0],
        "tellor_accrual_rows_in_realized": connection.execute(f"SELECT count(*) FROM {realized} WHERE oracle_network='Tellor' AND realization_status='accrued_not_paid'").fetchone()[0],
        "realized_rows_without_payment_or_state_delta": connection.execute(f"SELECT count(*) FROM {realized} WHERE NOT cashflow_verified AND NOT state_delta_verified").fetchone()[0],
        "duplicate_evidence_ids": connection.execute(f"SELECT count(*)-count(DISTINCT evidence_id) FROM {evidence}").fetchone()[0],
    }
    expected_zero = [
        "voter_slashed_rows_in_realized", "chainlink_claims_not_flow_exact",
        "uma_dispute_penalty_rows_not_flow_exact", "flare_fee_burns_in_realized",
        "pyth_parameter_rows_in_realized", "tellor_accrual_rows_in_realized",
        "realized_rows_without_payment_or_state_delta", "duplicate_evidence_ids",
        "dia_rewards_not_payment_exact",
    ]
    expected_positive = [
        "flare_rows_in_realized", "pyth_rows_in_realized",
        "tellor_tip_withdrawals_in_realized",
        "dia_rewards_in_realized",
    ]
    if (
        any(qc[key] != 0 for key in expected_zero)
        or any(qc[key] <= 0 for key in expected_positive)
        or not qc["dvm_accrued_net_equals_applied_net"]
    ):
        raise RuntimeError(f"strict economic QC failed: {qc}")

    manifest = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "fixed_cutoff": "2026-06-30T23:59:59Z",
        "evidence_output": str(EVIDENCE_OUTPUT),
        "realized_output": str(REALIZED_OUTPUT),
        "row_counts": {
            "economic_semantics_events": connection.execute(f"SELECT count(*) FROM {evidence}").fetchone()[0],
            "realized_reward_slash_events": connection.execute(f"SELECT count(*) FROM {realized}").fetchone()[0],
        },
        "evidence_summary": summarize(connection, evidence),
        "realized_summary": summarize(connection, realized),
        "qc": qc,
        "counting_policy": {
            "paid_reward": "requires observed token/bank transfer, or an exact gross transfer whose source-level decomposition isolates reward",
            "realized_slash": "requires applied stake/principal mutation or fully reconciled bond/fee loss",
            "excluded": "accruals, unclaimed Merkle entitlements, reward factors, eligibility outcomes, alerts, fee burns, and accounting redistribution",
            "uma_guard": "VoterSlashed is accrual-only; VoterSlashApplied is the aggregate realized net delta; never sum both",
        },
    }
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    rows = []
    for item in manifest["realized_summary"]:
        rows.append(
            f"| {item['oracle_network']} | {item['economic_evidence_class']} | {item['row_count']:,} | "
            f"{item['amount_raw']} | {item['asset']} |"
        )
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(
        "# Realized reward/slash audit\n\n"
        "Only source-defined balance effects with transaction or applied-state evidence enter this strict table.\n\n"
        f"- Broad semantics evidence: {manifest['row_counts']['economic_semantics_events']:,} rows.\n"
        f"- Strict paid/applied events: {manifest['row_counts']['realized_reward_slash_events']:,} rows.\n"
        f"- `VoterSlashed` in strict table: {qc['voter_slashed_rows_in_realized']}; `VoterSlashApplied`: {qc['voter_slash_applied_rows_in_realized']:,}.\n"
        f"- DVM accrued signed net equals applied signed net: {qc['dvm_accrued_net_equals_applied_net']} (`{qc['dvm_voter_slash_applied_net_raw']}` raw UMA).\n"
        f"- UMA disputed bond/fee rows without exact funding and settlement flows: {qc['uma_dispute_penalty_rows_not_flow_exact']}.\n"
        f"- Flare/Pyth strict rows: {qc['flare_rows_in_realized']}/{qc['pyth_rows_in_realized']}.\n\n"
        f"- DIA source-decomposed, transfer-exact staking rewards: {qc['dia_rewards_in_realized']}.\n\n"
        "## Strict realized rows\n\n| Protocol | Class | Rows | Raw amount | Asset |\n|---|---|---:|---:|---|\n"
        + "\n".join(rows)
        + "\n\nAmounts are not summed across assets. UMA applied stake changes are net realization events; their underlying request-level `VoterSlashed` accruals remain in the broad table only. Flare includes emitted claim events, Pyth includes inner SPL-token reward/slash transfers, while unclaimed Merkle leaves and reward/slash parameters remain broad evidence only.\n",
        encoding="utf-8",
    )
    print(json.dumps({"row_counts": manifest["row_counts"], "qc": qc, "manifest": str(MANIFEST)}, indent=2))


if __name__ == "__main__":
    main()
