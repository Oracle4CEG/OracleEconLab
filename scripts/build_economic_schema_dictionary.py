#!/usr/bin/env python3
"""Build the P0 economic schema, variable dictionary and UMA feasibility audit."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd
from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "schemas"
OUT = ROOT / "data/dictionaries"
REPORT = ROOT / "reports/uma_economic_variable_constructability_audit.md"
CUTOFF = 1782863999

OOV2 = ROOT / "data/curated/parquet/polygon_uma_request_rounds.parquet"
OOV2_EVENTS = ROOT / "data/curated/parquet/polygon_oov2_events.parquet"
OOV2_FLOWS = ROOT / "data/curated/parquet/polygon_uma_token_flows.parquet"
OOV2_FLOW_QC = ROOT / "data/curated/parquet/polygon_uma_request_flow_qc.parquet"
DVM_REQUESTS = ROOT / "data/curated/parquet/uma_dvm_requests.parquet"
DVM_PAYOFFS = ROOT / "data/curated/parquet/uma_dvm_voter_payoffs.parquet"
DVM_STAKING = ROOT / "data/curated/parquet/uma_dvm_staking_events.parquet"
CROSSCHAIN = ROOT / "data/curated/parquet/uma_polygon_ethereum_grade_a_links.parquet"

UMA_OOV2_SOURCE = (
    "https://raw.githubusercontent.com/UMAprotocol/protocol/"
    "a16ee53125c433dfa4e29738b73d9069ff109c03/packages/core/contracts/"
    "optimistic-oracle-v2/implementation/OptimisticOracleV2.sol"
)
UMA_DVM_SOURCE = (
    "https://eth.blockscout.com/api/v2/smart-contracts/"
    "0x004395edb43EFca9885CEdad51EC9fAf93Bd34ac"
)
UMA_DOCS = "https://github.com/UMAprotocol/uma-docs/blob/master/faqs.md"
PROV_O = "https://www.w3.org/TR/prov-o/"
CROISSANT = "https://docs.mlcommons.org/croissant/docs/croissant-spec-1.1.html"
TOWNSEND_CSV = "https://doi.org/10.1016/0022-0531(79)90031-0"
HOLMSTROM_MH = "https://doi.org/10.2307/3003320"
MYERSON_MD = "https://doi.org/10.1287/moor.6.1.58"
GROSSMAN_STIGLITZ = "https://www.jstor.org/stable/1805228"
NIST_AI_RMF = "https://airc.nist.gov/airmf-resources/airmf/3-sec-characteristics/"


def variable(
    name: str,
    track: str,
    formula: str,
    indexing: str,
    unit: str,
    measure: str,
    definition: str,
    mechanism: str,
    hypothesis: str,
    mapping: str,
    timing: str,
    evidence: str,
    missing: str,
    validation: str,
    refs: str,
    limits: str,
    constructability: str,
    source_tables: str,
    notes: str = "",
) -> dict[str, object]:
    academic_refs: list[str] = []
    if track in {"incentive", "penalty", "outcome"}:
        academic_refs.extend([HOLMSTROM_MH, MYERSON_MD])
    if track in {"monitoring", "cost"}:
        academic_refs.extend([TOWNSEND_CSV, GROSSMAN_STIGLITZ])
    if track == "capital":
        academic_refs.append(TOWNSEND_CSV)
    if track == "trustworthy_ai":
        academic_refs.append(NIST_AI_RMF)
    references = "; ".join(dict.fromkeys([refs, *academic_refs]))
    return {
        "variable_name": name,
        "track": track,
        "mathematical_definition": formula,
        "indexing": indexing,
        "observation_unit": unit,
        "measurement_unit": measure,
        "economic_definition": definition,
        "economic_mechanism": mechanism,
        "hypothesis_role": hypothesis,
        "source_mapping": mapping,
        "decision_time_availability": timing,
        "evidence_class": evidence,
        "missing_value_rule": missing,
        "validation_rule": validation,
        "references": references,
        "measurement_limitations": limits,
        "uma_constructability": constructability,
        "uma_source_tables": source_tables,
        "uma_non_null_observations": None,
        "notes": notes,
    }


def definitions() -> list[dict[str, object]]:
    oov2_refs = f"{UMA_OOV2_SOURCE}; {UMA_DOCS}"
    dvm_refs = f"{UMA_DVM_SOURCE}; {ROOT / 'reports/contract_semantics_audit.md'}"
    return [
        variable(
            "episode_id", "identity", "H(protocol || chain_id || native_unit_id || actor_role)",
            "i indexes a unique economic lifecycle; a indexes an actor when actor-specific",
            "request or actor-episode", "identifier",
            "Stable identifier for one economically coherent lifecycle.",
            "Prevents repeated logs from being treated as independent economic choices.",
            "All events belonging to one lifecycle map to one episode.",
            "polygon_uma_request_rounds.oo_request_id; uma_dvm_requests.dvm_request_id",
            "T0", "derived", "Never impute; an unresolvable native id excludes the row.",
            "Unique inside protocol, chain and native unit; deterministic under reruns.",
            f"{PROV_O}; {oov2_refs}",
            "The same cross-chain lifecycle may have multiple native identifiers; link grade must be retained.",
            "complete", "polygon_uma_request_rounds; uma_dvm_requests",
        ),
        variable(
            "decision_time_unix", "timing", "t_i^d = block_timestamp(first decision event for i)",
            "i=request or actor-episode", "request or actor-episode", "Unix seconds",
            "Time at which an economic actor must choose an action.",
            "Defines the information set and prevents future leakage.",
            "Only evidence with timestamp <= t_i^d may enter an agent decision.",
            "polygon_oov2_events.block_time for RequestPrice/ProposePrice/DisputePrice",
            "T0", "event", "Null if the native event has no block timestamp; never replace with settlement time.",
            "Timestamp equals the canonical block timestamp for the cited transaction.", oov2_refs,
            "A request contains several decision times; the actor role must specify which one is used.",
            "complete", "polygon_oov2_events",
        ),
        variable(
            "challenge_deadline_unix", "timing", "d_i = expirationTime_i",
            "i=OOV2 proposal", "proposal", "Unix seconds",
            "Last time at which the proposal can be disputed under the recorded request parameters.",
            "Defines the monitoring window and the option value of waiting.",
            "Longer windows may increase verification opportunity and capital lock.",
            "polygon_uma_request_rounds.expiration_time from ProposePrice",
            "T1", "event", "Null before proposal or when expirationTime is absent; not zero.",
            "d_i >= proposal_time_i and matches the ProposePrice event payload.", oov2_refs,
            "Deadline is protocol time, not proof that any monitor observed the proposal.",
            "complete", "polygon_uma_request_rounds; polygon_oov2_events",
        ),
        variable(
            "bond_raw", "capital", "B_i = effectiveBond_i",
            "i=OOV2 proposal/dispute", "request", "raw token integer",
            "Principal placed at risk by the proposal and, when challenged, the dispute.",
            "Risk-bearing commitment and the private cost of an incorrect claim.",
            "Higher B_i may deter frivolous proposals/challenges but raises entry and capital costs.",
            "polygon_uma_request_rounds.effective_bond_raw; currency supplies the asset",
            "T1", "event", "Null when no proposal exists; a source value of 0 is verified zero.",
            "Integer string; positive for proposed requests; reconcile with token transfers.", oov2_refs,
            "Request-level bond does not alone identify each actor's duration of exposure.",
            "complete", "polygon_uma_request_rounds; polygon_uma_token_flows",
        ),
        variable(
            "reward_configured_raw", "incentive", "R_i^cfg = request.reward_i",
            "i=OOV2 request", "request", "raw token integer",
            "Reward offered by the requester before a report is supplied.",
            "Ex-ante incentive for costly information production/reporting.",
            "A larger configured reward may attract participation but is not yet realized income.",
            "polygon_uma_request_rounds.question_reward_raw (cross-check reward_raw)",
            "T0", "contract_parameter", "Null means the request parameter was not recovered; zero is a valid no-reward request.",
            "question_reward_raw = reward_raw where both are populated.", oov2_refs,
            "Configured reward may later be refunded or rolled and must not be labelled paid.",
            "complete", "polygon_uma_request_rounds",
        ),
        variable(
            "reward_to_bond_ratio", "incentive", "rho_i = R_i^cfg / B_i for B_i > 0",
            "i=OOV2 proposed request", "request", "dimensionless ratio",
            "Configured compensation relative to capital placed at risk.",
            "Incentive intensity and risk-return trade-off.",
            "Higher rho_i may affect proposal or dispute participation conditional on observable selection.",
            "question_reward_raw / effective_bond_raw within the same asset and decimals",
            "T1", "derived", "Null when B_i is null or zero; never divide by zero or combine assets.",
            "Both inputs share currency; ratio is nonnegative and recomputable from raw integers.", oov2_refs,
            "Not a causal incentive estimate; request composition and adapter versions confound comparisons.",
            "complete", "polygon_uma_request_rounds",
        ),
        variable(
            "dispute_decision", "monitoring", "D_i = 1[DisputePrice event observed before d_i]",
            "i=OOV2 proposal", "request", "binary",
            "Whether at least one actor paid the on-chain cost to challenge the proposal.",
            "Costly monitoring and public-good provision.",
            "Low dispute rates may reflect agreement, weak monitoring, or high challenge cost; they do not prove truth.",
            "polygon_uma_request_rounds.dispute_tx; polygon_oov2_events.event='DisputePrice'",
            "T1", "event", "0 only when the full challenge window is observed; otherwise right-censored/null.",
            "One request has at most one canonical dispute event in OOV2; transaction and event agree.", oov2_refs,
            "Undisputed acceptance is a protocol outcome, not independent factual correctness.",
            "complete", "polygon_uma_request_rounds; polygon_oov2_events",
        ),
        variable(
            "proposal_upheld", "outcome", "U_i = 1[resolvedPrice_i = proposedPrice_i] for disputed i",
            "i=disputed OOV2 request", "dispute", "binary",
            "Whether the DVM-supported settlement agrees with the proposal.",
            "Ex-post adjudication of the challenger/proposer conflict.",
            "Conditional outcome for studying challenge precision, not population truth accuracy.",
            "proposed_price_raw and resolved_price_raw; Grade-A DVM link where cross-chain",
            "terminal_only", "cross_chain_link", "Null for undisputed, unresolved or non-exactly linked requests.",
            "For Grade-A links, DVM, child-pushed and OOV2 settled prices must be consistent.",
            f"{oov2_refs}; {dvm_refs}",
            "Protocol resolution may differ from independent ground truth.",
            "complete", "polygon_uma_request_rounds; uma_polygon_ethereum_grade_a_links",
        ),
        variable(
            "mandatory_wait_seconds", "timing", "W_i = d_i - t_i^proposal",
            "i=OOV2 proposal", "proposal", "seconds",
            "Protocol-defined minimum period during which a proposal remains challengeable.",
            "Institutional delay that enables monitoring but locks capital.",
            "Separates designed waiting time from excess operational delay.",
            "expiration_time minus ProposePrice.block_time",
            "T1", "derived", "Null when either timestamp is missing; never assume the default liveness.",
            "W_i >= 0; compare to exact event payload and contract version.", oov2_refs,
            "The period can be request-specific and must not be filled from the current global default.",
            "complete", "polygon_uma_request_rounds; polygon_oov2_events",
        ),
        variable(
            "settlement_delay_seconds", "timing", "L_i = t_i^settle - t_i^request",
            "i=OOV2 request", "request", "seconds",
            "Elapsed calendar time from request initiation to settlement.",
            "Total institutional and operational friction.",
            "Longer delay increases uncertainty and may increase capital opportunity cost.",
            "Settle.block_time - RequestPrice.block_time",
            "terminal_only", "derived", "Right-censored at the fixed cutoff for unsettled requests.",
            "Nonnegative; both timestamps come from canonical event logs.", oov2_refs,
            "Does not by itself identify which stage caused delay.",
            "complete", "polygon_oov2_events",
        ),
        variable(
            "excess_delay_seconds", "timing", "X_i = max(0, L_i - W_i)",
            "i=settled OOV2 request with observed W_i", "request", "seconds",
            "Delay beyond the protocol-defined challenge window.",
            "Operational friction net of mandatory institutional waiting.",
            "Separates design-imposed waiting from settlement execution latency.",
            "settlement_delay_seconds - mandatory_wait_seconds",
            "terminal_only", "derived", "Null unless both L_i and W_i are known.",
            "X_i >= 0 and inputs trace to event timestamps.", oov2_refs,
            "For disputed requests, DVM adjudication is part of excess delay and should later be decomposed.",
            "complete", "polygon_oov2_events; polygon_uma_request_rounds",
        ),
        variable(
            "reward_paid_raw", "outcome", "R_i^paid = explicitReportReward_i + disputeWinnerReward_i",
            "i=settled OOV2 request and economic winner", "request", "raw token integer",
            "Non-principal compensation actually embedded in the verified settlement transfer.",
            "Realized incentive payment.",
            "Distinguishes promised reward from realized income.",
            "explicit_report_reward_raw + dispute_winner_reward_raw; settlement transfer QC",
            "terminal_only", "token_flow", "Null when unsettled or transfer evidence unavailable; zero is valid after exact decomposition.",
            "principal_returned + reward_paid = gross_payout where protocol fees are handled by the rule; flow QC exact.",
            oov2_refs,
            "Request-level winner payment requires an actor-role table before individual net payoff analysis.",
            "complete", "polygon_uma_request_rounds; polygon_uma_request_flow_qc; polygon_uma_token_flows",
        ),
        variable(
            "principal_returned_raw", "capital", "P_i^ret = decomposed returned bond principal",
            "i=settled OOV2 request and settlement recipient", "request", "raw token integer",
            "Recovery of previously locked principal; not income.",
            "Capital recovery and exposure closure.",
            "Separating return of capital prevents payout from overstating rewards.",
            "polygon_uma_request_rounds.principal_returned_raw",
            "terminal_only", "token_flow", "Null when unsettled; zero only when exact decomposition proves no principal return.",
            "Must be reconciled within the same transaction and asset; never counted as reward.", oov2_refs,
            "A request aggregate may combine multiple principal components; actor allocation must be explicit.",
            "complete", "polygon_uma_request_rounds; polygon_uma_token_flows",
        ),
        variable(
            "bond_forfeited_raw", "penalty", "F_i^bond = losing bond amount not returned",
            "i=settled disputed OOV2 request", "dispute", "raw token integer",
            "Principal lost by the economically losing side of a dispute.",
            "Penalty enforcement and risk realization.",
            "Measures realized downside rather than a configured slash parameter.",
            "polygon_uma_request_rounds.bond_forfeited_raw",
            "terminal_only", "token_flow", "Null outside a settled eligible dispute; verified zero only after complete decomposition.",
            "Positive only for settled disputed requests; reconcile winner transfer and protocol fee.", oov2_refs,
            "Final fee and redistributed bond components must remain separately identified.",
            "complete", "polygon_uma_request_rounds; polygon_uma_token_flows",
        ),
        variable(
            "protocol_fee_raw", "cost", "C_i^fee = protocol fee transferred from disputed escrow",
            "i=settled disputed OOV2 request", "dispute", "raw token integer",
            "On-chain fee retained or routed by the mechanism during dispute settlement.",
            "Mechanism operation cost paid from escrow.",
            "Reduces private net payoff and may affect willingness to challenge.",
            "polygon_uma_request_rounds.protocol_fee_raw and final_fee_forfeited_raw",
            "terminal_only", "token_flow", "Null outside eligible settled disputes; do not infer from current fee parameters.",
            "Reconcile with settlement transaction flows and economic rule id.", oov2_refs,
            "Protocol fee is not the off-chain social cost of adjudication.",
            "complete", "polygon_uma_request_rounds; polygon_uma_token_flows",
        ),
        variable(
            "gross_payout_raw", "outcome", "G_i = total settlement transfer to recipient",
            "i=settled OOV2 request", "request", "raw token integer",
            "Total transferred amount, including returned principal and reward components; it is not an economic reward.",
            "Cash-flow reconciliation variable, not an economic reward measure.",
            "Used only to verify decomposition; treating G_i as reward overstates incentives.",
            "polygon_uma_request_rounds.gross_payout_raw; request flow QC",
            "terminal_only", "token_flow", "Null when unsettled or transfer unavailable.",
            "gross_payout_raw equals settlement_transfer_raw for all exact-QC settlements.", oov2_refs,
            "Cannot be compared as income without subtracting capital and fees.",
            "complete", "polygon_uma_request_rounds; polygon_uma_request_flow_qc",
        ),
        variable(
            "realized_payoff_raw", "outcome", "Pi_ia = rewards_ia - forfeitures_ia - fees_ia - gas_ia",
            "i=episode, a=actor", "actor-episode", "raw asset units by asset",
            "Actor-specific realized economic gain or loss excluding returned principal.",
            "Private economic consequence of reporting, monitoring or challenging.",
            "Outcome for incentive compatibility and economic-regret evaluation.",
            "Requires role-resolved token flows plus gas receipts and reward/forfeiture decomposition",
            "retrospective_only", "derived", "Null unless all included components share asset and actor attribution; never treat missing cost as zero.",
            "Component sum reconciles to actor-address flows; principal return excluded.", oov2_refs,
            "Current request table is not fully actor-role normalized and gas coverage is incomplete.",
            "partial", "polygon_uma_request_rounds; polygon_uma_token_flows; receipt archives",
        ),
        variable(
            "capital_days_locked_raw", "capital", "K_ia = B_ia * (t_release - t_lock)/86400",
            "i=episode, a=actor", "actor-episode", "raw token units multiplied by days",
            "Quantity of capital tied up over time in its native asset.",
            "Liquidity constraint and capital opportunity cost.",
            "Higher K may discourage participation even if expected payoff is positive.",
            "bond lock/release token flows and their block timestamps",
            "terminal_only", "derived", "Right-censored capital-days use cutoff duration and retain censor flag; never sum across assets.",
            "Release time cannot precede lock time; asset address and decimals remain attached.", oov2_refs,
            "Request-level principal is available, but exact actor-specific lock timestamps require a normalized flow lifecycle.",
            "partial", "polygon_uma_token_flows; polygon_oov2_events",
        ),
        variable(
            "gas_cost_native_raw", "cost", "C_ia^gas = gasUsed_tx * effectiveGasPrice_tx",
            "i=decision transaction, a=actor", "actor-episode", "native chain smallest unit",
            "Direct blockchain execution cost paid by the acting account.",
            "Participation and verification cost.",
            "Higher execution costs can suppress monitoring and challenge activity.",
            "transaction receipt gasUsed and effectiveGasPrice for proposal/dispute/settlement actions",
            "T1", "transaction_receipt", "Null when receipt is absent; never zero-fill missing receipts.",
            "Recompute product from hexadecimal receipt fields and match transaction sender.", oov2_refs,
            "Current archives cover targeted dispute and settlement receipts, not every relevant action uniformly.",
            "partial", "raw Polygon receipt archives",
        ),
        variable(
            "verification_cost_usd", "cost", "C_ia^verify = C_ia^gas,USD + C_ia^investigation + C_ia^delay",
            "i=monitoring decision, a=monitor", "actor-episode", "USD at documented timestamps",
            "Total private cost of investigating and executing a verification action.",
            "Costly information acquisition and public-good provision.",
            "Challenge occurs only when expected benefit exceeds verification and risk cost.",
            "gas receipts plus externally measured labor/API/time costs and timestamped FX conversion",
            "retrospective_only", "external_evidence", "Null if any required cost component is unavailable; partial cost must be labelled partial.",
            "Each USD component cites source and conversion time; no silent zero labor cost.",
            "NIST AI RMF characteristics; economic cost-accounting design in project guidance",
            "Gas is measured and timestamp-converted for the strict cohort; investigation and labor remain scenario assumptions.",
            "partial", "trustworthy_ai_usd_economics; Polygon dispute receipts; historical Chainlink proxy state",
        ),
        variable(
            "monitoring_concentration_hhi", "monitoring", "HHI_t = sum_a s_at^2, s_at = challenges_at / sum_b challenges_bt",
            "a=challenger, t=preregistered time window", "protocol-time window", "dimensionless [0,1]",
            "Concentration of observed challenge activity among monitoring actors.",
            "Public-good provision, specialization and monitoring fragility.",
            "High concentration can indicate reliance on a small set of monitors, without identifying welfare effects.",
            "disputer addresses from canonical DisputePrice events",
            "retrospective_only", "derived", "Null when actor attribution or full window coverage is unavailable; no challenges is not HHI=0.",
            "Shares sum to one within protocol, chain, asset and time window.", oov2_refs,
            "Address concentration is not entity concentration; sybils and delegated execution remain possible.",
            "complete", "polygon_uma_request_rounds; polygon_oov2_events",
        ),
        variable(
            "cross_chain_link_grade", "provenance", "Q_i in {A,B,C,U} under preregistered matching rules",
            "i=cross-chain disputed request", "cross-chain episode", "categorical grade",
            "Strength of evidence that Polygon OOV2 and Ethereum DVM records describe the same dispute.",
            "Identification quality for cross-chain adjudication and economic attribution.",
            "Only Grade A supports primary cross-chain outcome matching.",
            "uma_polygon_ethereum_grade_a_links.cross_chain_match_grade and exact hash/ancillary checks",
            "terminal_only", "cross_chain_link", "U means unresolved and must not be treated as a negative match.",
            "Grade A requires exact child request id, parent request id, identifier/time and ancillary match.",
            f"{dvm_refs}; {ROOT / 'reports/polygon_uma_qc.md'}",
            "A correct technical link does not create independent truth for the resolved value.",
            "complete", "uma_polygon_ethereum_grade_a_links",
        ),
        variable(
            "dvm_signed_payoff_delta_raw", "outcome", "Delta_iar = signedSlashTokens_iar",
            "i=DVM request, a=voter, r=round/request index", "actor-episode", "raw UMA integer, signed",
            "Request-level accrued redistribution: positive for correct-vote redistribution and negative for wrong/no-vote slash.",
            "Voting incentive, participation penalty and redistribution.",
            "Connects voting behavior to request-specific economic accrual.",
            "uma_dvm_voter_payoffs.signed_slash_delta_raw from VoterSlashed",
            "terminal_only", "state_delta", "Null if request-voter event is missing; zero is a real emitted zero delta.",
            "positive + zero + negative classifications are exhaustive; VoterSlashApplied is excluded from this sum.", dvm_refs,
            "It is accrued to unappliedSlash and is not the same as the later account-level stake mutation.",
            "complete", "uma_dvm_voter_payoffs",
        ),
        variable(
            "dvm_applied_stake_delta_raw", "outcome", "A_ak = emitted unappliedSlash at VoterSlashApplied_k",
            "a=voter, k=stake-update transaction", "actor stake-update", "raw UMA integer, signed",
            "Net accumulated slash/redistribution actually applied to the voter stake balance.",
            "Realized account-level balance mutation.",
            "Supports stake reconciliation but not request-level causal attribution.",
            "uma_dvm_staking_events VoterSlashApplied raw_data_words and emitted postStake",
            "terminal_only", "state_delta", "Null when application has not occurred by cutoff; not zero.",
            "pre-stake + applied delta = emitted postStake subject to zero floor; never add VoterSlashed again.", dvm_refs,
            "One application can aggregate multiple request-level accruals.",
            "complete", "uma_dvm_staking_events",
        ),
        variable(
            "independent_ground_truth_available", "outcome", "G_i = 1[independent, timestamped reference outcome passes validation]",
            "i=oracle request", "request", "binary",
            "Whether outcome correctness can be judged independently of the protocol's own settlement.",
            "Separates protocol consensus from empirical truth.",
            "Required before reporting accuracy or truthfulness rather than protocol agreement.",
            "validated external source record linked to immutable request semantics and resolution rule",
            "retrospective_only", "external_evidence", "0 only after an explicit coverage audit; otherwise unavailable/null.",
            "External source timestamp and extraction rule are preserved and independent of model output.", UMA_DOCS,
            "Available for a deterministic Binance-candle subcohort; most requests still provide only protocol outcomes.",
            "partial", "trustworthy_ai_independent_truth; Binance finalized klines",
        ),
        variable(
            "token_only_action_regret_raw", "trustworthy_ai",
            "Regret_i^token = max(0, Pi_i^challenge) - Pi_i(a_agent)",
            "i=resolved decision episode; a in {Accept,Challenge,Abstain}",
            "decision episode", "raw episode-token integer",
            "Ex-post private token payoff forgone relative to the better of taking no token action and challenging.",
            "Action quality under asymmetric bond/reward consequences.",
            "Tests whether an action policy improves private token outcomes before Gas and investigation costs.",
            "exact OOV2 winner reward, losing bond/final-fee forfeiture, protocol outcome, and registered agent action",
            "retrospective_only", "derived",
            "Null for Investigate because its subsequent action and investigation cost are unobserved; never subtract MATIC Gas from USDC.",
            "Computed within one token address and decimals; challenge payoff reconciles to exact settlement flows.",
            f"{oov2_refs}; project next-stage guidance",
            "Partial private-payoff regret only: excludes Gas, labor, external harm, independent truth and USD welfare.",
            "partial", "trustworthy_ai_requirements_audit; polygon_uma_request_rounds; Polygon dispute receipts",
        ),
        variable(
            "economic_regret_usd", "trustworthy_ai", "Regret_i = max_a U_i(a | I_i) - U_i(a_agent | I_i)",
            "i=agent decision episode, a in {Accept,Investigate,Challenge,Abstain}", "decision episode", "USD",
            "Economic loss from the agent's chosen action relative to the best feasible action under the preregistered payoff model.",
            "Decision quality under costly verification, uncertainty and abstention.",
            "Evaluates whether predictive gains survive action costs and asymmetric errors.",
            "decision-time evidence, realized protocol outcome, cost matrix, gas and capital costs",
            "retrospective_only", "derived", "Null without a preregistered utility matrix and sufficient realized cost/outcome evidence.",
            "No future fields in I_i; recompute all action utilities and preserve cost assumptions.",
            "NIST AI Risk Management Framework; project next-stage guidance",
            "Constructed as private-verifier scenario regret with measured FX/Gas/payoff; investigation cost and APR are assumptions, not observations.",
            "partial", "trustworthy_ai_usd_economics; historical Chainlink proxy state; registered cost scenarios",
        ),
        variable(
            "provenance_id", "provenance", "P_i = H(source entities || transformation rule || release version)",
            "i=processed observation", "any processed observation", "identifier",
            "Stable pointer to the entities, activities and agents that generated an observation.",
            "Accountability, auditability and reproducibility of measurement.",
            "Every economic value must be reversible to its primary evidence and transformation.",
            "source transaction/log keys, transformation rule id, code version and release manifest",
            "not_applicable", "derived", "Never optional for released processed observations.",
            "Hash is deterministic; referenced source objects and rules exist and pass checksums.",
            f"{PROV_O}; {CROISSANT}",
            "A hash proves identity/integrity of cited inputs, not semantic correctness by itself.",
            "partial", "all processed tables and manifests",
        ),
    ]


def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def coverage(con: duckdb.DuckDBPyConnection) -> tuple[dict[str, int], list[dict[str, object]]]:
    p = qpath(OOV2)
    e = qpath(OOV2_EVENTS)
    stats = {
        "requests": scalar(con, f"SELECT count(*) FROM read_parquet('{p}')"),
        "proposed": scalar(con, f"SELECT count(*) FROM read_parquet('{p}') WHERE proposal_tx IS NOT NULL"),
        "disputed": scalar(con, f"SELECT count(*) FROM read_parquet('{p}') WHERE dispute_tx IS NOT NULL"),
        "settled": scalar(con, f"SELECT count(*) FROM read_parquet('{p}') WHERE settlement_tx IS NOT NULL"),
        "positive_bond": scalar(con, f"SELECT count(*) FROM read_parquet('{p}') WHERE try_cast(effective_bond_raw AS HUGEINT)>0"),
        "positive_explicit_reward": scalar(con, f"SELECT count(*) FROM read_parquet('{p}') WHERE try_cast(explicit_report_reward_raw AS HUGEINT)>0"),
        "positive_dispute_reward": scalar(con, f"SELECT count(*) FROM read_parquet('{p}') WHERE try_cast(dispute_winner_reward_raw AS HUGEINT)>0"),
        "positive_bond_forfeiture": scalar(con, f"SELECT count(*) FROM read_parquet('{p}') WHERE try_cast(bond_forfeited_raw AS HUGEINT)>0"),
        "exact_settlement_flow": scalar(con, f"SELECT count(*) FROM read_parquet('{qpath(OOV2_FLOW_QC)}') WHERE settlement_flow_exact"),
        "grade_a_crosschain": scalar(con, f"SELECT count(*) FROM read_parquet('{qpath(CROSSCHAIN)}') WHERE cross_chain_match_grade='A'"),
        "dvm_requests": scalar(con, f"SELECT count(*) FROM read_parquet('{qpath(DVM_REQUESTS)}')"),
        "dvm_payoffs": scalar(con, f"SELECT count(*) FROM read_parquet('{qpath(DVM_PAYOFFS)}')"),
        "dvm_positive_payoffs": scalar(con, f"SELECT count(*) FROM read_parquet('{qpath(DVM_PAYOFFS)}') WHERE try_cast(signed_slash_delta_raw AS HUGEINT)>0"),
        "dvm_negative_payoffs": scalar(con, f"SELECT count(*) FROM read_parquet('{qpath(DVM_PAYOFFS)}') WHERE try_cast(signed_slash_delta_raw AS HUGEINT)<0"),
        "dvm_applied": scalar(con, f"SELECT count(*) FROM read_parquet('{qpath(DVM_STAKING)}') WHERE event='VoterSlashApplied'"),
        "proposal_timestamps": scalar(con, f"SELECT count(*) FROM read_parquet('{e}') WHERE event='ProposePrice' AND block_time IS NOT NULL"),
        "dispute_timestamps": scalar(con, f"SELECT count(*) FROM read_parquet('{e}') WHERE event='DisputePrice' AND block_time IS NOT NULL"),
        "settlement_timestamps": scalar(con, f"SELECT count(*) FROM read_parquet('{e}') WHERE event='Settle' AND block_time IS NOT NULL"),
    }
    map_counts = {
        "episode_id": stats["requests"] + stats["dvm_requests"],
        "decision_time_unix": stats["requests"] + stats["proposal_timestamps"] + stats["dispute_timestamps"],
        "challenge_deadline_unix": stats["proposed"],
        "bond_raw": stats["positive_bond"],
        "reward_configured_raw": stats["requests"] - 1,
        "reward_to_bond_ratio": stats["positive_bond"],
        "dispute_decision": stats["proposed"],
        "proposal_upheld": stats["grade_a_crosschain"],
        "mandatory_wait_seconds": stats["proposed"],
        "settlement_delay_seconds": stats["settled"],
        "excess_delay_seconds": stats["settled"],
        "reward_paid_raw": stats["settled"],
        "principal_returned_raw": stats["settled"],
        "bond_forfeited_raw": stats["positive_bond_forfeiture"],
        "protocol_fee_raw": stats["positive_bond_forfeiture"],
        "gross_payout_raw": stats["settled"],
        "realized_payoff_raw": stats["settled"],
        "capital_days_locked_raw": stats["settled"],
        "gas_cost_native_raw": stats["disputed"],
        "verification_cost_usd": 810,
        "monitoring_concentration_hhi": stats["disputed"],
        "cross_chain_link_grade": stats["disputed"],
        "dvm_signed_payoff_delta_raw": stats["dvm_payoffs"],
        "dvm_applied_stake_delta_raw": stats["dvm_applied"],
        "independent_ground_truth_available": 34,
        "token_only_action_regret_raw": 810,
        "economic_regret_usd": 810,
        "provenance_id": 0,
    }
    rows = []
    for name, count in map_counts.items():
        rows.append({"variable_name": name, "non_null_or_eligible_observations": count})
    return stats, rows


def write_schema_field_dictionary() -> None:
    schema = json.loads((SCHEMAS / "cross_chain_economic_observation.schema.json").read_text())
    required = set(schema["required"])
    rows = []
    for name, spec in schema["properties"].items():
        rows.append({
            "field_name": name,
            "required": name in required,
            "json_type_or_enum": json.dumps(spec.get("type", spec.get("enum")), ensure_ascii=False),
            "description": spec.get("description", ""),
        })
    frame = pd.DataFrame(rows)
    frame.to_csv(OUT / "cross_chain_economic_observation_fields.csv", index=False)
    frame.to_parquet(OUT / "cross_chain_economic_observation_fields.parquet", index=False)


def build() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    dictionary_schema = json.loads((SCHEMAS / "economic_variable_dictionary.schema.json").read_text())
    Draft202012Validator.check_schema(dictionary_schema)
    observation_schema = json.loads((SCHEMAS / "cross_chain_economic_observation.schema.json").read_text())
    Draft202012Validator.check_schema(observation_schema)

    entries = definitions()
    validator = Draft202012Validator(dictionary_schema)
    errors = []
    for idx, entry in enumerate(entries):
        for error in validator.iter_errors(entry):
            errors.append(f"entry {idx} {entry['variable_name']}: {error.message}")
    if errors:
        raise RuntimeError("\n".join(errors))
    names = [str(row["variable_name"]) for row in entries]
    if len(names) != len(set(names)):
        raise RuntimeError("economic variable names are not unique")

    con = duckdb.connect()
    stats, coverage_rows = coverage(con)
    con.close()
    counts = {str(row["variable_name"]): int(row["non_null_or_eligible_observations"]) for row in coverage_rows}
    for entry in entries:
        entry["uma_non_null_observations"] = counts.get(str(entry["variable_name"]), 0)

    frame = pd.DataFrame(entries)
    frame.to_csv(OUT / "economic_variable_dictionary.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    frame.to_parquet(OUT / "economic_variable_dictionary.parquet", index=False)
    (OUT / "economic_variable_dictionary.json").write_text(
        json.dumps(entries, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    pd.DataFrame(coverage_rows).to_parquet(OUT / "uma_variable_constructability.parquet", index=False)
    write_schema_field_dictionary()

    source_files = [OOV2, OOV2_EVENTS, OOV2_FLOWS, OOV2_FLOW_QC, DVM_REQUESTS, DVM_PAYOFFS, DVM_STAKING, CROSSCHAIN]
    manifest_files = [
        SCHEMAS / "cross_chain_economic_observation.schema.json",
        SCHEMAS / "economic_variable_dictionary.schema.json",
        OUT / "economic_variable_dictionary.csv",
        OUT / "economic_variable_dictionary.parquet",
        OUT / "economic_variable_dictionary.json",
        OUT / "cross_chain_economic_observation_fields.csv",
        OUT / "cross_chain_economic_observation_fields.parquet",
        OUT / "uma_variable_constructability.parquet",
    ]
    manifest = {
        "schema_version": "1.0.0",
        "fixed_cutoff_unix": CUTOFF,
        "observation_schema": "schemas/cross_chain_economic_observation.schema.json",
        "variable_dictionary_schema": "schemas/economic_variable_dictionary.schema.json",
        "variable_count": len(entries),
        "source_files": [str(p.relative_to(ROOT)) for p in source_files],
        "outputs": [],
    }
    for path in manifest_files:
        h = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest["outputs"].append({
            "path": str(path.relative_to(ROOT)), "bytes": path.stat().st_size, "sha256": h
        })
    (OUT / "economic_schema_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    write_report(entries, stats)


def write_report(entries: list[dict[str, object]], stats: dict[str, int]) -> None:
    by_status: dict[str, list[dict[str, object]]] = {}
    for row in entries:
        by_status.setdefault(str(row["uma_constructability"]), []).append(row)
    summary_rows = []
    for status in ["complete", "partial", "unavailable", "not_applicable"]:
        names = ", ".join(f"`{x['variable_name']}`" for x in by_status.get(status, [])) or "—"
        summary_rows.append(f"| {status} | {len(by_status.get(status, []))} | {names} |")

    variable_rows = []
    for row in entries:
        variable_rows.append(
            f"| `{row['variable_name']}` | {row['observation_unit']} | {row['measurement_unit']} | "
            f"{row['decision_time_availability']} | {row['uma_constructability']} | "
            f"{int(row['uma_non_null_observations'] or 0):,} |"
        )

    REPORT.write_text(
        "# UMA 经济变量可构造性审计\n\n"
        f"**固定截止时间：** 2026-06-30 23:59:59 UTC (`{CUTOFF}`)  \n"
        "**审计目标：** 判断现有数据能否支持经济 observation schema，而不是统计已有日志数量。\n\n"
        "## 结论\n\n"
        "现有 UMA 数据可以完整构造 request/proposal/dispute 层的 bond、配置奖励、奖励强度、争议行为、结算延迟、真实奖励分解、本金返还、bond forfeiture、协议费和跨链匹配等级；也可以构造 DVM request-voter 层的 signed payoff accrual 与账户层 applied stake delta。"
        "严格的 810-sample Trustworthy AI cohort 还具有完整 dispute receipt、actor challenge token payoff、capital-days、Gas 和历史 Chainlink USD 换算；34 个 Binance-candle 样本具有独立真值。USD regret 是明确调查成本/APR 情景下的 private-verifier regret，不是观测到的社会福利；全量 UMA actor-action 面板仍只部分可构造。未观测成本不得填零，协议 resolution 不得冒充独立 ground truth。\n\n"
        "## 当前真实数据规模\n\n"
        "| Evidence unit | Count |\n|---|---:|\n"
        f"| Polygon OOV2 requests | {stats['requests']:,} |\n"
        f"| Proposed requests | {stats['proposed']:,} |\n"
        f"| Disputed requests | {stats['disputed']:,} |\n"
        f"| Settled requests | {stats['settled']:,} |\n"
        f"| Exact settlement-flow reconciliations | {stats['exact_settlement_flow']:,} |\n"
        f"| Grade-A Polygon--Ethereum links | {stats['grade_a_crosschain']:,} |\n"
        f"| Ethereum DVM requests | {stats['dvm_requests']:,} |\n"
        f"| DVM request-voter payoff rows | {stats['dvm_payoffs']:,} |\n"
        f"| Positive redistribution rows | {stats['dvm_positive_payoffs']:,} |\n"
        f"| Negative wrong/no-vote slash rows | {stats['dvm_negative_payoffs']:,} |\n"
        f"| Applied stake-delta events | {stats['dvm_applied']:,} |\n\n"
        "## 可构造性分组\n\n| Status | Variables | Names |\n|---|---:|---|\n"
        + "\n".join(summary_rows)
        + "\n\n## 逐变量状态\n\n"
        "`non-null/eligible` 是变量可定义的候选观察数，不代表最终回归样本；分母必须按变量的观察单位重新声明。\n\n"
        "| Variable | Observation unit | Measurement | Available at | UMA status | Non-null/eligible |\n"
        "|---|---|---|---|---|---:|\n"
        + "\n".join(variable_rows)
        + "\n\n## 关键测量边界\n\n"
        "1. `gross_payout_raw` 包含本金返还，不能用作 reward。\n"
        "2. `reward_configured_raw` 是 ex-ante 参数，只有经过 settlement flow 对账的非本金部分才是 `reward_paid_raw`。\n"
        "3. `VoterSlashed` 是 request-level signed accrual；`VoterSlashApplied` 是账户层 applied net stake delta，二者不得相加。\n"
        "4. 未争议请求只能称为 protocol-accepted，不能称为 objectively true。\n"
        "5. `dispute_decision=0` 仅在完整观察 challenge window 后成立；窗口未结束的样本必须删失。\n"
        "6. Token 数量保持 raw integer、asset address 和 decimals；不同资产不得相加。\n"
        "7. 810-sample 严格 cohort 的 dispute Gas 已完整；这不等于所有 UMA action 都有完整 Gas 面板。\n"
        "8. `token_only_action_regret_raw` 只在同一 episode token 内可构造；USD regret 使用历史 Chainlink FX 及公开的调查成本/APR 情景，因此不能声称观测到的社会福利。\n"
        "9. Independent truth 只覆盖 34 个确定性 Binance candle 规则样本；其余请求不得外推为事实正确。\n\n"
        "## 当前阶段状态\n\n"
        "P0 字典、P1 真实 UMA Episode、P2 Tellor 同 schema 扩展和 P3 严格四行动任务均已形成可执行产物；剩余不可观测量继续按本表的缺失规则发布。\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    build()
    print(json.dumps({
        "schema": str(SCHEMAS / "cross_chain_economic_observation.schema.json"),
        "dictionary": str(OUT / "economic_variable_dictionary.parquet"),
        "audit": str(REPORT),
    }, indent=2))
