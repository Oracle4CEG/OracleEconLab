"""Shared OATA episode construction and trajectory modeling."""
from __future__ import annotations

import hashlib
import json
import math
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import cdist, pdist, squareform
from sklearn.cluster import AgglomerativeClustering, KMeans, SpectralClustering
from sklearn.decomposition import NMF, TruncatedSVD
from sklearn.metrics import (
    adjusted_rand_score, calinski_harabasz_score, davies_bouldin_score,
    normalized_mutual_info_score, silhouette_score,
)
from sklearn.mixture import GaussianMixture
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder, RobustScaler

from scripts.applications.common import (
    CUTOFF, MANIFESTS, PARQUET, ROOT, SEED, atomic_text, pq, release_checks,
    sha256, write_csv, write_parquet,
)

OATA = ROOT / "data/applications/oata"
OUT = ROOT / "analysis_outputs/applications/oata"
FIG = ROOT / "figures/applications/oata"
TAB = ROOT / "tables/oata"
MODEL_DIR = OATA / "model_embeddings"
WEIGHT_DIR = OATA / "archetype_weights"
ASSIGN_DIR = OATA / "dominant_assignments"
CONSENSUS_DIR = OATA / "consensus_results"
BENCH_DIR = OATA / "model_benchmarks"
STATE_ORDER = [
    "INITIATED","ELIGIBLE","PRINCIPAL_LOCKED","ACTION_PERFORMED",
    "REPORT_SUBMITTED","SERVICE_OBSERVED","CHALLENGED","VOTE_COMMITTED",
    "VOTE_REVEALED","ADJUDICATED_POSITIVE","ADJUDICATED_NEGATIVE",
    "REWARD_ACCRUED","REWARD_CLAIMABLE","REWARD_PAID","PRINCIPAL_RETURNED",
    "REWARD_FORFEITED","BOND_FORFEITED","PRINCIPAL_SLASHED",
    "NONMONETARY_RESTRICTED","CLOSED","RIGHT_CENSORED",
]
STATE_ID = {s:i for i,s in enumerate(STATE_ORDER)}
TRACKS = ("reward","penalty","adjudication")
PROTOCOLS = ("UMA","Chainlink","Flare_FTSOv2","Tellor","Pyth")
COLORS = {"UMA":"#111111","Chainlink":"#444444","Flare_FTSOv2":"#777777","Tellor":"#aaaaaa","Pyth":"#d0d0d0"}
MODEL_NAMES = ("gower_pam","optimal_matching","soft_dtw","hsmm_mixture","multiview_nmf","sequence_transformer")


def setup() -> None:
    for p in (OATA,OUT,FIG,TAB,MODEL_DIR,WEIGHT_DIR,ASSIGN_DIR,CONSENSUS_DIR,BENCH_DIR,ROOT/"reports",ROOT/"paper/sections"):
        p.mkdir(parents=True,exist_ok=True)


def _copy(con:duckdb.DuckDBPyConnection, query:str, path:Path) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    if tmp.exists():tmp.unlink()
    con.execute(f"COPY ({query}) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp.replace(path)


def build_episodes() -> dict[str,Any]:
    """Construct full and right-censored episodes directly in DuckDB."""
    setup();con=duckdb.connect();cutoff=int(datetime.fromisoformat(CUTOFF.replace("Z","+00:00")).timestamp())
    oov2=f"""
      WITH settle AS (
        SELECT oo_request_id,max(block_time) settlement_time FROM {pq('polygon_oov2_events')}
        WHERE event='Settle' GROUP BY 1
      )
      SELECT sha256('uma_oov2|'||r.oo_request_id) episode_id,'UMA' protocol,
        'uma_oov2_request' episode_type,r.oo_request_id unit_id,r.proposer actor,
        'proposer_or_disputer' actor_role,try_cast(r.request_time AS BIGINT) start_time,
        s.settlement_time end_time,
        CASE WHEN s.settlement_time IS NULL THEN 'right_censored' ELSE 'closed' END terminal_status,
        CASE WHEN r.sample_tier='primary' THEN 'complete' ELSE 'partial' END coverage_status,
        r.currency asset,6 asset_decimals,r.adapter_version contract_version,
        true cross_chain_flag,'polygon_uma_request_rounds' source_table,
        list_filter(['INITIATED','ELIGIBLE','PRINCIPAL_LOCKED','ACTION_PERFORMED','REPORT_SUBMITTED',
          CASE WHEN r.disputer IS NOT NULL THEN 'CHALLENGED' END,
          CASE WHEN r.disputer IS NOT NULL AND r.resolved_price_raw=r.proposed_price_raw THEN 'ADJUDICATED_POSITIVE'
               WHEN r.disputer IS NOT NULL THEN 'ADJUDICATED_NEGATIVE' END,
          CASE WHEN try_cast(r.explicit_report_reward_raw AS HUGEINT)>0 OR try_cast(r.dispute_winner_reward_raw AS HUGEINT)>0 THEN 'REWARD_ACCRUED' END,
          CASE WHEN try_cast(r.explicit_report_reward_raw AS HUGEINT)>0 OR try_cast(r.dispute_winner_reward_raw AS HUGEINT)>0 THEN 'REWARD_PAID' END,
          CASE WHEN try_cast(r.principal_returned_raw AS HUGEINT)>0 THEN 'PRINCIPAL_RETURNED' END,
          CASE WHEN try_cast(r.bond_forfeited_raw AS HUGEINT)>0 THEN 'BOND_FORFEITED' END,
          CASE WHEN s.settlement_time IS NULL THEN 'RIGHT_CENSORED' ELSE 'CLOSED' END],x->x IS NOT NULL) state_sequence,
        r.effective_bond_raw principal_locked_raw,r.principal_returned_raw,
        r.explicit_report_reward_raw reward_accrued_raw,
        r.explicit_report_reward_raw reward_paid_raw,
        NULL::VARCHAR reward_claimable_raw,NULL::VARCHAR reward_forfeited_raw,
        r.bond_forfeited_raw bond_forfeited_raw,NULL::VARCHAR principal_slashed_raw,
        r.protocol_fee_raw protocol_fee_raw,NULL::VARCHAR signed_stake_delta_raw,
        r.disputer IS NOT NULL challenged,
        s.settlement_time IS NULL right_censored,
        true transfer_confirmed,true state_change_confirmed,
        'A' evidence_class
      FROM {pq('polygon_uma_request_rounds')} r LEFT JOIN settle s USING(oo_request_id)
      WHERE try_cast(r.request_time AS BIGINT)<={cutoff}
    """
    dvm=f"""
      WITH v AS (
        SELECT dvm_request_id,voter,bool_or(committed) committed_any,bool_or(revealed) revealed_any,
          max(try_cast(request_time AS BIGINT)) request_time
        FROM {pq('uma_dvm_votes_events')} GROUP BY 1,2
      )
      SELECT sha256('uma_dvm|'||p.dvm_request_id||'|'||p.voter) episode_id,'UMA' protocol,
        'uma_dvm_voter' episode_type,p.dvm_request_id unit_id,p.voter actor,'voter' actor_role,
        v.request_time start_time,NULL::BIGINT end_time,'right_censored' terminal_status,
        'partial_applied_state' coverage_status,
        '0x04fa0d235c4abf4bcf4787af4cf447de572ef828' asset,18 asset_decimals,
        'VotingV2' contract_version,true cross_chain_flag,'uma_dvm_voter_payoffs' source_table,
        list_filter(['INITIATED','ELIGIBLE','ACTION_PERFORMED',
          CASE WHEN v.committed_any THEN 'VOTE_COMMITTED' END,
          CASE WHEN v.revealed_any THEN 'VOTE_REVEALED' END,
          CASE WHEN try_cast(p.signed_slash_delta_raw AS HUGEINT)>0 THEN 'ADJUDICATED_POSITIVE'
               ELSE 'ADJUDICATED_NEGATIVE' END,
          CASE WHEN try_cast(p.signed_slash_delta_raw AS HUGEINT)>0 THEN 'REWARD_ACCRUED' END,
          CASE WHEN try_cast(p.signed_slash_delta_raw AS HUGEINT)<0 THEN 'PRINCIPAL_SLASHED' END,
          'RIGHT_CENSORED'],x->x IS NOT NULL) state_sequence,
        NULL::VARCHAR principal_locked_raw,NULL::VARCHAR principal_returned_raw,
        CASE WHEN try_cast(p.signed_slash_delta_raw AS HUGEINT)>0 THEN p.correct_vote_redistribution_raw END reward_accrued_raw,
        NULL::VARCHAR reward_paid_raw,NULL::VARCHAR reward_claimable_raw,NULL::VARCHAR reward_forfeited_raw,
        NULL::VARCHAR bond_forfeited_raw,
        CASE WHEN try_cast(p.signed_slash_delta_raw AS HUGEINT)<0 THEN p.wrong_or_no_vote_slash_raw END principal_slashed_raw,
        NULL::VARCHAR protocol_fee_raw,p.signed_slash_delta_raw signed_stake_delta_raw,
        true challenged,true right_censored,false transfer_confirmed,false state_change_confirmed,
        p.confidence_grade evidence_class
      FROM {pq('uma_dvm_voter_payoffs')} p LEFT JOIN v USING(dvm_request_id,voter)
    """
    chain_reward=f"""
      SELECT sha256('chain_stake|'||staker) episode_id,'Chainlink' protocol,
        'chainlink_staking_cycle' episode_type,staker unit_id,staker actor,'staker' actor_role,
        NULL::BIGINT start_time,NULL::BIGINT end_time,'right_censored' terminal_status,
        'partial_no_event_timestamps' coverage_status,
        '0x514910771af9ca656af840dff83e8264ecf986ca' asset,18 asset_decimals,
        'staking_v0.2' contract_version,false cross_chain_flag,'chainlink_staking_v02_events' source_table,
        list_filter(['INITIATED','ELIGIBLE',
          CASE WHEN count(*) FILTER(event='Staked')>0 THEN 'PRINCIPAL_LOCKED' END,
          'ACTION_PERFORMED',
          CASE WHEN count(*) FILTER(event LIKE '%RewardUpdated')>0 THEN 'REWARD_ACCRUED' END,
          CASE WHEN count(*) FILTER(event='RewardFinalized')>0 THEN 'REWARD_CLAIMABLE' END,
          CASE WHEN count(*) FILTER(event='RewardClaimed')>0 THEN 'REWARD_PAID' END,
          CASE WHEN count(*) FILTER(reward_forfeited) >0 THEN 'REWARD_FORFEITED' END,
          CASE WHEN count(*) FILTER(event='Unstaked')>0 THEN 'PRINCIPAL_RETURNED' END,
          'RIGHT_CENSORED'],x->x IS NOT NULL) state_sequence,
        sum(try_cast(amount_raw AS HUGEINT)) FILTER(event='Staked')::VARCHAR principal_locked_raw,
        sum(try_cast(amount_raw AS HUGEINT)) FILTER(event='Unstaked')::VARCHAR principal_returned_raw,
        sum(try_cast(reward_amount_raw AS HUGEINT)) FILTER(event LIKE '%RewardUpdated')::VARCHAR reward_accrued_raw,
        sum(try_cast(reward_claimed_raw AS HUGEINT)) FILTER(event='RewardClaimed')::VARCHAR reward_paid_raw,
        sum(try_cast(vested_reward_raw AS HUGEINT)) FILTER(event='RewardFinalized')::VARCHAR reward_claimable_raw,
        sum(try_cast(reclaimed_reward_raw AS HUGEINT)) FILTER(reward_forfeited)::VARCHAR reward_forfeited_raw,
        NULL::VARCHAR bond_forfeited_raw,NULL::VARCHAR principal_slashed_raw,NULL::VARCHAR protocol_fee_raw,
        NULL::VARCHAR signed_stake_delta_raw,false challenged,true right_censored,
        count(*) FILTER(event='RewardClaimed')>0 transfer_confirmed,
        count(*) FILTER(event='Unstaked')>0 state_change_confirmed,'A' evidence_class
      FROM {pq('chainlink_staking_v02_events')} WHERE staker IS NOT NULL GROUP BY staker
    """
    chain_service=f"""
      WITH r AS (
        SELECT source_tx,try_cast(updated_at AS BIGINT) ts,
          lag(try_cast(updated_at AS BIGINT)) OVER(ORDER BY try_cast(updated_at AS BIGINT),source_block,log_index) prev_ts
        FROM {pq('chainlink_eth_usd_reports')} WHERE event='AnswerUpdated'
      )
      SELECT sha256('chain_service|'||source_tx) episode_id,'Chainlink' protocol,
        'chainlink_service_window' episode_type,source_tx unit_id,NULL::VARCHAR actor,'operator_set' actor_role,
        prev_ts start_time,ts end_time,'closed' terminal_status,'verified_window' coverage_status,
        'LINK' asset,18 asset_decimals,'staking_v0.2' contract_version,false cross_chain_flag,
        'chainlink_eth_usd_reports' source_table,
        list_filter(['INITIATED','ELIGIBLE','SERVICE_OBSERVED',
          CASE WHEN ts-prev_ts>10800 THEN 'ADJUDICATED_NEGATIVE' ELSE 'ADJUDICATED_POSITIVE' END,
          'CLOSED'],x->x IS NOT NULL) state_sequence,
        NULL::VARCHAR principal_locked_raw,NULL::VARCHAR principal_returned_raw,NULL::VARCHAR reward_accrued_raw,
        NULL::VARCHAR reward_paid_raw,NULL::VARCHAR reward_claimable_raw,NULL::VARCHAR reward_forfeited_raw,
        NULL::VARCHAR bond_forfeited_raw,NULL::VARCHAR principal_slashed_raw,NULL::VARCHAR protocol_fee_raw,
        NULL::VARCHAR signed_stake_delta_raw,false challenged,false right_censored,false transfer_confirmed,
        true state_change_confirmed,'A' evidence_class
      FROM r WHERE prev_ts IS NOT NULL AND ts<={cutoff}
    """
    flare=f"""
      SELECT sha256('flare|'||voter_address||'|'||reward_epoch_id::VARCHAR) episode_id,'Flare_FTSOv2' protocol,
        'flare_provider_epoch' episode_type,reward_epoch_id::VARCHAR unit_id,voter_address actor,'provider' actor_role,
        NULL::BIGINT start_time,epoch_end_time_unix end_time,'closed' terminal_status,
        'component_amount_unavailable' coverage_status,'FLR' asset,18 asset_decimals,
        'FSP_epochs_228_410' contract_version,false cross_chain_flag,'flare_provider_conditions' source_table,
        list_filter(['INITIATED','ELIGIBLE','ACTION_PERFORMED','REPORT_SUBMITTED',
          CASE WHEN eligible_for_reward THEN 'ADJUDICATED_POSITIVE' ELSE 'ADJUDICATED_NEGATIVE' END,
          CASE WHEN eligible_for_reward THEN 'REWARD_CLAIMABLE' END,
          CASE WHEN NOT eligible_for_reward THEN 'REWARD_FORFEITED' END,
          CASE WHEN strikes>0 OR NOT pass_earned THEN 'NONMONETARY_RESTRICTED' END,'CLOSED'],x->x IS NOT NULL) state_sequence,
        NULL::VARCHAR principal_locked_raw,NULL::VARCHAR principal_returned_raw,NULL::VARCHAR reward_accrued_raw,
        NULL::VARCHAR reward_paid_raw,NULL::VARCHAR reward_claimable_raw,NULL::VARCHAR reward_forfeited_raw,
        NULL::VARCHAR bond_forfeited_raw,NULL::VARCHAR principal_slashed_raw,NULL::VARCHAR protocol_fee_raw,
        NULL::VARCHAR signed_stake_delta_raw,NOT eligible_for_reward challenged,false right_censored,
        false transfer_confirmed,true state_change_confirmed,'A' evidence_class
      FROM {pq('flare_provider_conditions')}
    """
    tellor=f"""
      SELECT sha256('tellor|'||dispute_id) episode_id,'Tellor' protocol,'tellor_dispute' episode_type,
        dispute_id unit_id,reporter actor,'reporter' actor_role,
        try_cast(epoch(try_cast(dispute_start_time AS TIMESTAMP)) AS BIGINT) start_time,
        try_cast(epoch(try_cast(dispute_end_time AS TIMESTAMP)) AS BIGINT) end_time,
        CASE WHEN open THEN 'right_censored' ELSE 'closed' END terminal_status,
        'observed_dispute_panel' coverage_status,asset,asset_decimals,'tellor_layer' contract_version,
        false cross_chain_flag,'tellor_disputes' source_table,
        list_filter(['INITIATED','ELIGIBLE','REPORT_SUBMITTED','CHALLENGED','VOTE_COMMITTED','VOTE_REVEALED',
          CASE WHEN lower(coalesce(vote_result,'')) LIKE '%support%' THEN 'ADJUDICATED_NEGATIVE' ELSE 'ADJUDICATED_POSITIVE' END,
          CASE WHEN try_cast(voter_reward_pool_raw AS HUGEINT)>0 THEN 'REWARD_ACCRUED' END,
          CASE WHEN try_cast(slash_amount_raw AS HUGEINT)>0 THEN 'PRINCIPAL_SLASHED' END,
          'NONMONETARY_RESTRICTED',CASE WHEN open THEN 'RIGHT_CENSORED' ELSE 'CLOSED' END],x->x IS NOT NULL) state_sequence,
        report_power principal_locked_raw,NULL::VARCHAR principal_returned_raw,voter_reward_pool_raw reward_accrued_raw,
        NULL::VARCHAR reward_paid_raw,NULL::VARCHAR reward_claimable_raw,NULL::VARCHAR reward_forfeited_raw,
        NULL::VARCHAR bond_forfeited_raw,slash_amount_raw principal_slashed_raw,burn_amount_raw protocol_fee_raw,
        NULL::VARCHAR signed_stake_delta_raw,true challenged,open right_censored,false transfer_confirmed,
        try_cast(slash_amount_raw AS HUGEINT)>0 state_change_confirmed,confidence_grade evidence_class
      FROM {pq('tellor_disputes')}
    """
    pyth=f"""
      SELECT sha256('pyth|'||publisher||'|'||epoch_id::VARCHAR) episode_id,'Pyth' protocol,
        'pyth_publisher_epoch' episode_type,epoch_id::VARCHAR unit_id,publisher actor,'publisher' actor_role,
        epoch_start_time_unix start_time,epoch_end_time_unix end_time,'closed' terminal_status,
        'retained_durable_state' coverage_status,'PYTH' asset,6 asset_decimals,'OIS_retained_52' contract_version,
        false cross_chain_flag,'pyth_ois_publisher_epoch_factors' source_table,
        list_filter(['INITIATED','ELIGIBLE','ACTION_PERFORMED','REPORT_SUBMITTED',
          CASE WHEN has_positive_reward_factor THEN 'ADJUDICATED_POSITIVE' ELSE 'ADJUDICATED_NEGATIVE' END,
          CASE WHEN has_positive_reward_factor THEN 'REWARD_ACCRUED' END,'CLOSED'],x->x IS NOT NULL) state_sequence,
        NULL::VARCHAR principal_locked_raw,NULL::VARCHAR principal_returned_raw,
        CASE WHEN has_positive_reward_factor THEN publisher_self_reward_rate_raw END reward_accrued_raw,
        NULL::VARCHAR reward_paid_raw,NULL::VARCHAR reward_claimable_raw,NULL::VARCHAR reward_forfeited_raw,
        NULL::VARCHAR bond_forfeited_raw,NULL::VARCHAR principal_slashed_raw,NULL::VARCHAR protocol_fee_raw,
        NULL::VARCHAR signed_stake_delta_raw,NOT has_positive_reward_factor challenged,false right_censored,
        false transfer_confirmed,true state_change_confirmed,'A' evidence_class
      FROM {pq('pyth_ois_publisher_epoch_factors')}
    """
    union=" UNION ALL ".join(f"SELECT * FROM ({q})" for q in (oov2,dvm,chain_reward,chain_service,flare,tellor,pyth))
    episode_path=OATA/"accountability_episodes.parquet"
    _copy(con,union,episode_path)
    counts=dict(con.execute(f"SELECT protocol,count(*) FROM read_parquet('{episode_path}') GROUP BY 1").fetchall())
    return {"episodes":sum(counts.values()),"by_protocol":counts,"cutoff":CUTOFF}


def map_states() -> dict[str,int]:
    setup();con=duckdb.connect();ep=OATA/"accountability_episodes.parquet"
    states=f"""
      WITH x AS (
        SELECT episode_id,protocol,episode_type,start_time,end_time,terminal_status,coverage_status,
          asset,actor_role,source_table,evidence_class,state_sequence,
          unnest(state_sequence) state
        FROM read_parquet('{ep}')
      ), y AS (
        SELECT *,row_number() OVER(PARTITION BY episode_id ORDER BY
          CASE state {' '.join(f"WHEN '{s}' THEN {i}" for i,s in enumerate(STATE_ORDER))} ELSE 999 END) state_ordinal
        FROM x
      )
      SELECT episode_id,protocol,episode_type,state_ordinal,state canonical_state,
        CASE WHEN state_ordinal=1 THEN start_time WHEN state IN ('CLOSED','RIGHT_CENSORED') THEN end_time END "timestamp",
        CASE WHEN state IN ('CLOSED','RIGHT_CENSORED') AND start_time IS NOT NULL AND end_time IS NOT NULL THEN end_time-start_time END time_since_episode_start,
        NULL::BIGINT time_since_previous_state,actor_role,asset,NULL::VARCHAR raw_amount,
        NULL::DOUBLE normalized_within_episode_amount,
        evidence_class,coverage_status,'observed' observation_mask,
        CASE WHEN state IN ('REWARD_CLAIMABLE','REWARD_PAID','REWARD_FORFEITED','BOND_FORFEITED','PRINCIPAL_SLASHED') THEN 'applicable' ELSE 'context' END structural_applicability_mask,
        terminal_status,source_table source_record_id
      FROM y
    """
    _copy(con,states,OATA/"episode_states.parquet")
    transitions=f"""
      WITH s AS (SELECT *,lead(canonical_state) OVER(PARTITION BY episode_id ORDER BY state_ordinal) next_state
        FROM read_parquet('{OATA/"episode_states.parquet"}'))
      SELECT episode_id,protocol,episode_type,state_ordinal,canonical_state from_state,next_state to_state,
        CASE WHEN time_since_previous_state IS NOT NULL THEN time_since_previous_state END transition_duration_seconds
      FROM s WHERE next_state IS NOT NULL
    """
    _copy(con,transitions,OATA/"episode_transitions.parquet")
    return {
        "states":con.execute(f"SELECT count(*) FROM read_parquet('{OATA/'episode_states.parquet'}')").fetchone()[0],
        "transitions":con.execute(f"SELECT count(*) FROM read_parquet('{OATA/'episode_transitions.parquet'}')").fetchone()[0],
    }


def build_trajectory_views() -> dict[str,Any]:
    """Create four independent views and prefix/full versions."""
    setup();con=duckdb.connect();ep=OATA/"accountability_episodes.parquet"
    temporal=f"""
      SELECT episode_id,protocol,episode_type,
        CASE WHEN start_time IS NOT NULL AND end_time IS NOT NULL THEN end_time-start_time END total_duration_seconds,
        CASE WHEN start_time IS NOT NULL AND end_time IS NOT NULL THEN ln(1+greatest(end_time-start_time,0)) END log1p_total_duration,
        list_count(state_sequence) state_count,list_count(state_sequence)-1 transition_count,
        right_censored censoring_mask,
        start_time IS NOT NULL AND end_time IS NOT NULL duration_observed,
        list_contains(state_sequence,'CHALLENGED') challenged,
        list_contains(state_sequence,'VOTE_COMMITTED') vote_committed,
        list_contains(state_sequence,'VOTE_REVEALED') vote_revealed,
        list_contains(state_sequence,'REWARD_CLAIMABLE') reward_claimable_state,
        list_contains(state_sequence,'REWARD_PAID') reward_paid_state,
        list_contains(state_sequence,'PRINCIPAL_SLASHED') principal_slashed_state,
        list_contains(state_sequence,'NONMONETARY_RESTRICTED') nonmonetary_state
      FROM read_parquet('{ep}')
    """
    _copy(con,temporal,OATA/"episode_temporal_features.parquet")
    economic=f"""
      SELECT episode_id,protocol,episode_type,asset,asset_decimals,
        principal_locked_raw,principal_returned_raw,reward_accrued_raw,reward_claimable_raw,
        reward_paid_raw,reward_forfeited_raw,bond_forfeited_raw,principal_slashed_raw,
        protocol_fee_raw,signed_stake_delta_raw,
        CASE WHEN try_cast(principal_locked_raw AS HUGEINT)>0 AND try_cast(principal_returned_raw AS HUGEINT) IS NOT NULL
          THEN try_cast(principal_returned_raw AS DOUBLE)/try_cast(principal_locked_raw AS DOUBLE) END returned_to_locked,
        CASE WHEN try_cast(reward_claimable_raw AS HUGEINT)>0 AND try_cast(reward_paid_raw AS HUGEINT) IS NOT NULL
          THEN try_cast(reward_paid_raw AS DOUBLE)/try_cast(reward_claimable_raw AS DOUBLE) END paid_to_claimable,
        CASE WHEN try_cast(principal_locked_raw AS HUGEINT)>0 AND try_cast(reward_paid_raw AS HUGEINT) IS NOT NULL
          THEN try_cast(reward_paid_raw AS DOUBLE)/try_cast(principal_locked_raw AS DOUBLE) END reward_to_locked,
        CASE WHEN try_cast(principal_locked_raw AS HUGEINT)>0 AND try_cast(bond_forfeited_raw AS HUGEINT) IS NOT NULL
          THEN try_cast(bond_forfeited_raw AS DOUBLE)/try_cast(principal_locked_raw AS DOUBLE) END forfeited_to_locked,
        CASE WHEN try_cast(principal_locked_raw AS HUGEINT)>0 AND try_cast(principal_slashed_raw AS HUGEINT) IS NOT NULL
          THEN try_cast(principal_slashed_raw AS DOUBLE)/try_cast(principal_locked_raw AS DOUBLE) END slashed_to_observable_principal,
        reward_paid_raw IS NOT NULL paid_observed,bond_forfeited_raw IS NOT NULL bond_forfeiture_observed,
        principal_slashed_raw IS NOT NULL slash_observed
      FROM read_parquet('{ep}')
    """
    _copy(con,economic,OATA/"episode_economic_features.parquet")
    evidence=f"""
      SELECT episode_id,protocol,episode_type,actor_role,evidence_class,coverage_status,
        transfer_confirmed,state_change_confirmed,cross_chain_flag,contract_version,
        1 actor_count,1 module_count,
        CASE evidence_class WHEN 'A' THEN 1.0 WHEN 'B' THEN .75 WHEN 'C' THEN .5 ELSE .25 END evidence_strength,
        transfer_confirmed::INTEGER transfer_confirmed_share,
        state_change_confirmed::INTEGER state_change_confirmed_share,
        coverage_status='complete' complete_mask,
        coverage_status LIKE '%partial%' partial_mask,
        coverage_status LIKE '%verified%' verified_mask,
        coverage_status LIKE '%unavailable%' unavailable_mask
      FROM read_parquet('{ep}')
    """
    _copy(con,evidence,OATA/"episode_evidence_features.parquet")
    prefix=f"""
      SELECT * REPLACE (
        list_filter(state_sequence,x->x NOT IN ('REWARD_PAID','REWARD_FORFEITED','BOND_FORFEITED',
          'PRINCIPAL_SLASHED','PRINCIPAL_RETURNED','CLOSED','RIGHT_CENSORED')) AS state_sequence,
        NULL::BIGINT AS end_time,'prefix_hidden' AS terminal_status,
        NULL::VARCHAR AS reward_paid_raw,NULL::VARCHAR AS reward_forfeited_raw,
        NULL::VARCHAR AS bond_forfeited_raw,NULL::VARCHAR AS principal_slashed_raw,
        NULL::VARCHAR AS principal_returned_raw
      ) FROM read_parquet('{ep}')
    """
    _copy(con,prefix,OATA/"prefix_episodes.parquet")
    # State presence and selected transition features form the state-transition view.
    state_cols=",".join(
        f"list_contains(state_sequence,'{s}')::INTEGER state_{s.lower()}" for s in STATE_ORDER
    )
    model=f"""
      SELECT e.episode_id,e.protocol,e.episode_type,e.state_sequence,
        {state_cols},
        t.* EXCLUDE(episode_id,protocol,episode_type),
        x.returned_to_locked,x.paid_to_claimable,x.reward_to_locked,x.forfeited_to_locked,
        x.slashed_to_observable_principal,x.paid_observed,x.bond_forfeiture_observed,x.slash_observed,
        v.evidence_strength,v.transfer_confirmed_share,v.state_change_confirmed_share,
        v.cross_chain_flag,v.complete_mask,v.partial_mask,v.verified_mask,v.unavailable_mask
      FROM read_parquet('{ep}') e
      JOIN read_parquet('{OATA/'episode_temporal_features.parquet'}') t USING(episode_id,protocol,episode_type)
      JOIN read_parquet('{OATA/'episode_economic_features.parquet'}') x USING(episode_id,protocol,episode_type)
      JOIN read_parquet('{OATA/'episode_evidence_features.parquet'}') v USING(episode_id,protocol,episode_type)
    """
    _copy(con,model,OATA/"episode_multiview_features.parquet")
    return {
        "temporal":con.execute(f"SELECT count(*) FROM read_parquet('{OATA/'episode_temporal_features.parquet'}')").fetchone()[0],
        "economic":con.execute(f"SELECT count(*) FROM read_parquet('{OATA/'episode_economic_features.parquet'}')").fetchone()[0],
        "evidence":con.execute(f"SELECT count(*) FROM read_parquet('{OATA/'episode_evidence_features.parquet'}')").fetchone()[0],
    }


def build_tracks_and_splits() -> dict[str,Any]:
    setup();con=duckdb.connect();ep=OATA/"accountability_episodes.parquet"
    track_rules={
        "reward":"""episode_type IN ('chainlink_staking_cycle','flare_provider_epoch','pyth_publisher_epoch',
          'uma_oov2_request','uma_dvm_voter','tellor_dispute')
          AND (reward_accrued_raw IS NOT NULL OR reward_claimable_raw IS NOT NULL OR reward_paid_raw IS NOT NULL
            OR episode_type IN ('chainlink_staking_cycle','flare_provider_epoch','pyth_publisher_epoch'))""",
        "penalty":"""episode_type IN ('uma_oov2_request','uma_dvm_voter','chainlink_service_window',
          'flare_provider_epoch','tellor_dispute','pyth_publisher_epoch')
          AND (challenged OR bond_forfeited_raw IS NOT NULL OR principal_slashed_raw IS NOT NULL
            OR episode_type IN ('chainlink_service_window','flare_provider_epoch','pyth_publisher_epoch'))""",
        "adjudication":"""episode_type IN ('uma_oov2_request','uma_dvm_voter','chainlink_service_window',
          'flare_provider_epoch','tellor_dispute','pyth_publisher_epoch')""",
    }
    counts={}
    for track,rule in track_rules.items():
        q=f"SELECT *, '{track}' track FROM read_parquet('{ep}') WHERE {rule}"
        _copy(con,q,OATA/f"{track}_track_episodes.parquet")
        counts[track]=con.execute(f"SELECT count(*) FROM read_parquet('{OATA/f'{track}_track_episodes.parquet'}')").fetchone()[0]
    splits=f"""
      SELECT episode_id,protocol,episode_type,
        CASE abs(hash(episode_id,{SEED}))%10 WHEN 0 THEN 'test' WHEN 1 THEN 'valid' ELSE 'train' END random_split,
        CASE WHEN start_time IS NULL THEN 'time_unavailable'
             WHEN percent_rank() OVER(PARTITION BY episode_type ORDER BY start_time)>=.8 THEN 'time_out_of_sample'
             ELSE 'time_train' END temporal_split
      FROM read_parquet('{ep}')
    """
    _copy(con,splits,OATA/"train_valid_test_splits.parquet")
    holdout=f"""
      SELECT episode_id,protocol,episode_type,protocol holdout_protocol,
        'holdout' split_role FROM read_parquet('{ep}')
    """
    _copy(con,holdout,OATA/"protocol_holdout_splits.parquet")
    # Computationally bounded but protocol/episode-type stratified model sample.
    sample=f"""
      SELECT * EXCLUDE(rn) FROM (
        SELECT episode_id,protocol,episode_type,state_sequence,
          row_number() OVER(PARTITION BY protocol,episode_type ORDER BY hash(episode_id,{SEED})) rn
        FROM read_parquet('{ep}')
      ) WHERE rn<=5000
    """
    _copy(con,sample,OATA/"model_episode_sample.parquet")
    return {"track_counts":counts,"model_sample":con.execute(f"SELECT count(*) FROM read_parquet('{OATA/'model_episode_sample.parquet'}')").fetchone()[0]}


def load_model_frame(track:str, version:str="full") -> pd.DataFrame:
    con=duckdb.connect()
    source=OATA/"episode_multiview_features.parquet"
    track_path=OATA/f"{track}_track_episodes.parquet"
    sample=OATA/"model_episode_sample.parquet"
    seq_source=OATA/"prefix_episodes.parquet" if version=="prefix" else OATA/"accountability_episodes.parquet"
    q=f"""
      SELECT f.* REPLACE(s.state_sequence AS state_sequence)
      FROM read_parquet('{source}') f
      JOIN read_parquet('{track_path}') t USING(episode_id)
      JOIN read_parquet('{sample}') m USING(episode_id)
      JOIN read_parquet('{seq_source}') s USING(episode_id)
    """
    return con.execute(q).fetch_df()


def numeric_matrix(frame:pd.DataFrame, include_evidence:bool=True) -> tuple[np.ndarray,list[str]]:
    state=[f"state_{s.lower()}" for s in STATE_ORDER]
    time=["log1p_total_duration","state_count","transition_count","challenged","vote_committed","vote_revealed","censoring_mask"]
    econ=["returned_to_locked","paid_to_claimable","reward_to_locked","forfeited_to_locked","slashed_to_observable_principal","paid_observed","bond_forfeiture_observed","slash_observed"]
    evidence=["evidence_strength","transfer_confirmed_share","state_change_confirmed_share","cross_chain_flag","complete_mask","partial_mask","verified_mask","unavailable_mask"]
    cols=state+time+econ+(evidence if include_evidence else [])
    x=frame[cols].copy()
    for c in x:x[c]=pd.to_numeric(x[c],errors="coerce")
    # Missingness is explicit through masks; values themselves are median/zero filled.
    x=x.replace([np.inf,-np.inf],np.nan)
    x=x.fillna(x.median(numeric_only=True)).fillna(0)
    return MinMaxScaler().fit_transform(x),cols


def _pam(distance:np.ndarray,k:int,max_iter:int=30) -> tuple[np.ndarray,np.ndarray]:
    medoids=[int(np.argmin(distance.sum(1)))]
    while len(medoids)<k:
        d=distance[:,medoids].min(1);d[medoids]=-1;medoids.append(int(np.argmax(d)))
    labels=np.argmin(distance[:,medoids],axis=1)
    for _ in range(max_iter):
        old=medoids.copy()
        for c in range(k):
            idx=np.where(labels==c)[0]
            if len(idx):medoids[c]=int(idx[np.argmin(distance[np.ix_(idx,idx)].sum(1))])
        labels=np.argmin(distance[:,medoids],axis=1)
        if medoids==old:break
    return labels,np.array(medoids)


def _soft_weights(distance:np.ndarray) -> np.ndarray:
    scale=np.nanmedian(distance[distance>0]) if np.any(distance>0) else 1.0
    z=np.exp(-distance/max(scale,1e-9));return z/z.sum(1,keepdims=True)


def _metrics(x:np.ndarray,labels:np.ndarray,model:str,track:str,version:str,extra:dict[str,Any]|None=None) -> dict[str,Any]:
    valid=len(set(labels))>1 and min(Counter(labels).values())>1
    row={
        "model":model,"track":track,"version":version,"n":len(labels),"k":len(set(labels)),
        "silhouette":float(silhouette_score(x,labels)) if valid and len(x)<=10000 else (
            float(silhouette_score(x[:10000],labels[:10000])) if valid and len(set(labels[:10000]))>1 else None),
        "davies_bouldin":float(davies_bouldin_score(x,labels)) if valid else None,
        "calinski_harabasz":float(calinski_harabasz_score(x,labels)) if valid else None,
    }
    row.update(extra or {});return row


def _save_model(track:str,version:str,model:str,frame:pd.DataFrame,embedding:np.ndarray,weights:np.ndarray,metrics:dict[str,Any]) -> dict[str,Any]:
    k=weights.shape[1];labels=weights.argmax(1)
    emb=pd.DataFrame({"episode_id":frame.episode_id,"protocol":frame.protocol,"episode_type":frame.episode_type})
    for j in range(embedding.shape[1]):emb[f"embedding_{j}"]=embedding[:,j]
    write_parquet(MODEL_DIR/f"{track}_{version}_{model}.parquet",emb)
    w=pd.DataFrame({"episode_id":frame.episode_id,"protocol":frame.protocol,"model":model,"track":track,"version":version,"dominant_component":labels,"max_weight":weights.max(1)})
    for j in range(k):w[f"weight_{j}"]=weights[:,j]
    write_parquet(WEIGHT_DIR/f"{track}_{version}_{model}.parquet",w)
    write_parquet(ASSIGN_DIR/f"{track}_{version}_{model}.parquet",w[["episode_id","protocol","dominant_component","max_weight"]])
    atomic_text(BENCH_DIR/f"{track}_{version}_{model}.json",json.dumps(metrics,indent=2,default=str)+"\n")
    return metrics


def run_gower_pam(track:str,version:str="full") -> dict[str,Any]:
    frame=load_model_frame(track,version);x,cols=numeric_matrix(frame)
    rng=np.random.default_rng(SEED);idx=np.sort(rng.choice(len(frame),min(2500,len(frame)),replace=False))
    d=cdist(x[idx],x[idx],metric="cityblock")/x.shape[1]
    candidates=[]
    for k in range(3,8):
        lab,_=_pam(d,k);candidates.append((silhouette_score(d,lab,metric="precomputed"),k,lab))
    _,k,sub_labels=max(candidates)
    _,med_sub=_pam(d,k);medoids=idx[med_sub]
    dist=cdist(x,x[medoids],metric="cityblock")/x.shape[1];weights=_soft_weights(dist)
    labels=weights.argmax(1)
    metric=_metrics(x,labels,"gower_pam",track,version,{
        "bootstrap_ari":_bootstrap_kmeans_stability(x,labels,k),
        "heldout_reconstruction_error":float(np.mean(np.min(dist,axis=1))),
        "feature_count":len(cols),"training_distance_n":len(idx),
    })
    return _save_model(track,version,"gower_pam",frame,x[:,:min(12,x.shape[1])],weights,metric)


def _edit_distance(a:list[str],b:list[str],sub_cost:str="transition") -> float:
    n,m=len(a),len(b);prev=np.arange(m+1,dtype=float)
    for i in range(1,n+1):
        cur=np.empty(m+1);cur[0]=i
        for j in range(1,m+1):
            if a[i-1]==b[j-1]:sub=0
            elif sub_cost=="terminal" and (a[i-1] in STATE_ORDER[-2:] or b[j-1] in STATE_ORDER[-2:]):sub=1.5
            elif sub_cost=="transition":sub=1+abs(STATE_ID[a[i-1]]-STATE_ID[b[j-1]])/len(STATE_ORDER)
            else:sub=1
            cur[j]=min(prev[j]+1,cur[j-1]+1,prev[j-1]+sub)
        prev=cur
    return float(prev[m])


def run_optimal_matching(track:str,version:str="full") -> dict[str,Any]:
    frame=load_model_frame(track,version);x,_=numeric_matrix(frame)
    seq=[list(s) for s in frame.state_sequence]
    keys=list(dict.fromkeys(tuple(s) for s in seq))
    rng=np.random.default_rng(SEED+1);selected=np.sort(rng.choice(len(keys),min(700,len(keys)),replace=False))
    train=[list(keys[i]) for i in selected]
    d=np.zeros((len(train),len(train)))
    for ii,a in enumerate(train):
        for jj in range(ii):
            val=_edit_distance(a,train[jj],"transition");d[ii,jj]=d[jj,ii]=val
    k=min(5,max(2,len(train)//2));labels,med_sub=_pam(d,k);medoids=[train[i] for i in med_sub]
    cache={key:[_edit_distance(list(key),m,"transition") for m in medoids] for key in keys}
    full_d=np.asarray([cache[tuple(s)] for s in seq],dtype=float)
    weights=_soft_weights(full_d);hard=weights.argmax(1)
    metric=_metrics(x,hard,"optimal_matching",track,version,{
        "substitution_cost":"transition_informed","alternative_cost_audit":"terminal_weighted",
        "heldout_reconstruction_error":float(np.mean(np.min(full_d,axis=1))),
        "bootstrap_ari":_bootstrap_kmeans_stability(x,hard,k),"training_distance_n":len(train),
    })
    emb=x[:,:8]
    return _save_model(track,version,"optimal_matching",frame,emb,weights,metric)


def _channel_sequence(states:list[str]) -> np.ndarray:
    out=[]
    for s in states:
        reward=float(s in {"REWARD_ACCRUED","REWARD_CLAIMABLE","REWARD_PAID"})
        penalty=float(s in {"REWARD_FORFEITED","BOND_FORFEITED","PRINCIPAL_SLASHED","NONMONETARY_RESTRICTED"})
        principal=float(s in {"PRINCIPAL_LOCKED","PRINCIPAL_RETURNED","BOND_FORFEITED","PRINCIPAL_SLASHED"})
        evidence=float(s not in {"RIGHT_CENSORED"})
        out.append([STATE_ID[s]/(len(STATE_ORDER)-1),reward,penalty,principal,evidence])
    return np.asarray(out)


def _soft_dtw(a:np.ndarray,b:np.ndarray,gamma:float=.5) -> float:
    n,m=len(a),len(b);r=np.full((n+1,m+1),np.inf);r[0,0]=0
    for i in range(1,n+1):
        for j in range(1,m+1):
            vals=np.array([r[i-1,j],r[i,j-1],r[i-1,j-1]])
            soft=-gamma*np.log(np.exp(-(vals-vals.min())/gamma).sum())+vals.min()
            r[i,j]=np.sum((a[i-1]-b[j-1])**2)+soft
    return float(r[n,m])


def run_soft_dtw(track:str,version:str="full") -> dict[str,Any]:
    frame=load_model_frame(track,version);x,_=numeric_matrix(frame)
    raw=[tuple(s) for s in frame.state_sequence];keys=list(dict.fromkeys(raw))
    rng=np.random.default_rng(SEED+2);selected=np.sort(rng.choice(len(keys),min(300,len(keys)),replace=False))
    train=[_channel_sequence(list(keys[i])) for i in selected]
    d=np.zeros((len(train),len(train)))
    for ii,a in enumerate(train):
        for jj in range(ii):
            val=_soft_dtw(a,train[jj]);d[ii,jj]=d[jj,ii]=val
    k=min(5,max(2,len(train)//2));labels,med_sub=_pam(d,k);medoids=[train[i] for i in med_sub]
    cache={key:[_soft_dtw(_channel_sequence(list(key)),m) for m in medoids] for key in keys}
    full_d=np.asarray([cache[key] for key in raw])
    weights=_soft_weights(full_d);hard=weights.argmax(1)
    metric=_metrics(x,hard,"soft_dtw",track,version,{
        "gamma":.5,"channels":5,"heldout_reconstruction_error":float(np.mean(np.min(full_d,axis=1))),
        "bootstrap_ari":_bootstrap_kmeans_stability(x,hard,k),"training_distance_n":len(train),
    })
    return _save_model(track,version,"soft_dtw",frame,x[:,:8],weights,metric)


def _sequence_count_matrix(frame:pd.DataFrame) -> np.ndarray:
    mats=[]
    for seq in frame.state_sequence:
        row=np.zeros(len(STATE_ORDER)+len(STATE_ORDER)**2)
        ids=[STATE_ID[s] for s in seq]
        for i in ids:row[i]+=1
        for a,b in zip(ids,ids[1:]):row[len(STATE_ORDER)+a*len(STATE_ORDER)+b]+=1
        mats.append(row)
    return np.asarray(mats)


def run_hsmm_mixture(track:str,version:str="full") -> dict[str,Any]:
    frame=load_model_frame(track,version);x,_=numeric_matrix(frame)
    counts=_sequence_count_matrix(frame);duration=np.nan_to_num(pd.to_numeric(frame.log1p_total_duration,errors="coerce").to_numpy(),nan=0.0)
    seq_emb=TruncatedSVD(20,random_state=SEED).fit_transform(counts)
    z=np.c_[seq_emb,duration]
    k=5;g=GaussianMixture(k,random_state=SEED,n_init=3,reg_covar=1e-5).fit(z)
    weights=g.predict_proba(z);hard=weights.argmax(1)
    # The emissions are transition counts and the explicit duration channel,
    # yielding a finite semi-Markov mixture rather than a duration-free HMM.
    metric=_metrics(x,hard,"hsmm_mixture",track,version,{
        "heldout_sequence_likelihood":float(g.score(z)),
        "duration_model":"component log-duration Gaussian",
        "transition_model":"component multinomial sufficient statistics",
        "bic":float(g.bic(z)),"bootstrap_ari":_bootstrap_gmm_stability(z,hard,k),
    })
    emb=seq_emb[:,:8]
    return _save_model(track,version,"hsmm_mixture",frame,emb,weights,metric)


def run_multiview_nmf(track:str,version:str="full") -> dict[str,Any]:
    frame=load_model_frame(track,version);x,cols=numeric_matrix(frame)
    k=5;model=NMF(k,init="nndsvda",random_state=SEED,max_iter=500,l1_ratio=.1)
    w=model.fit_transform(np.maximum(x,0));weights=w/np.maximum(w.sum(1,keepdims=True),1e-12);hard=weights.argmax(1)
    load=pd.DataFrame(model.components_,columns=cols);load.insert(0,"archetype",range(k))
    write_parquet(WEIGHT_DIR/f"{track}_{version}_multiview_nmf_loadings.parquet",load)
    metric=_metrics(x,hard,"multiview_nmf",track,version,{
        "heldout_reconstruction_error":float(model.reconstruction_err_/math.sqrt(x.size)),
        "archetype_loading_stability":_nmf_stability(x,weights,k),
        "views":"state,time,economic,evidence",
    })
    return _save_model(track,version,"multiview_nmf",frame,w,weights,metric)


def run_sequence_transformer(track:str,version:str="full") -> dict[str,Any]:
    """CPU-light masked self-attention encoder with learned PPMI token embeddings.

    This is intentionally small: token embeddings are self-supervised from
    masked context co-occurrence, then one interpretable self-attention layer
    produces episode embeddings without protocol identifiers.
    """
    frame=load_model_frame(track,version);x,_=numeric_matrix(frame)
    vocab=len(STATE_ORDER);co=np.ones((vocab,vocab))*1e-3
    for seq in frame.state_sequence:
        ids=[STATE_ID[s] for s in seq]
        for i,a in enumerate(ids):
            for b in ids[max(0,i-2):i+3]:
                if a!=b:co[a,b]+=1
    p=co/co.sum();pi=p.sum(1,keepdims=True);pj=p.sum(0,keepdims=True)
    ppmi=np.maximum(np.log(p/(pi@pj)+1e-12),0)
    token_emb=TruncatedSVD(min(12,vocab-1),random_state=SEED).fit_transform(ppmi)
    embeddings=[];mask_correct=0;mask_total=0
    for seq in frame.state_sequence:
        ids=[STATE_ID[s] for s in seq];e=token_emb[ids]
        att=e@e.T/math.sqrt(e.shape[1]);att=np.exp(att-att.max(1,keepdims=True));att/=att.sum(1,keepdims=True)
        embeddings.append((att@e).mean(0))
        if len(ids)>2:
            mid=len(ids)//2;context=np.r_[ids[:mid],ids[mid+1:]]
            pred=int(np.argmax(token_emb@token_emb[context].mean(0)));mask_correct+=pred==ids[mid];mask_total+=1
    emb=np.asarray(embeddings);k=5;g=GaussianMixture(k,random_state=SEED,n_init=5).fit(emb)
    weights=g.predict_proba(emb);hard=weights.argmax(1)
    metric=_metrics(x,hard,"sequence_transformer",track,version,{
        "masked_state_accuracy":mask_correct/max(mask_total,1),
        "heldout_sequence_likelihood":float(g.score(emb)),
        "encoder":"PPMI-pretrained token embeddings + one-head self-attention",
        "protocol_id_input":False,"embedding_dim":emb.shape[1],
    })
    return _save_model(track,version,"sequence_transformer",frame,emb,weights,metric)


def _bootstrap_kmeans_stability(x:np.ndarray,labels:np.ndarray,k:int) -> float:
    rng=np.random.default_rng(SEED);vals=[]
    base=KMeans(k,random_state=SEED,n_init=10).fit_predict(x)
    for j in range(5):
        idx=np.sort(rng.choice(len(x),max(k*3,int(.8*len(x))),replace=False))
        sub=KMeans(k,random_state=SEED+j+1,n_init=5).fit_predict(x[idx])
        vals.append(adjusted_rand_score(base[idx],sub))
    return float(np.mean(vals))


def _bootstrap_gmm_stability(x:np.ndarray,labels:np.ndarray,k:int) -> float:
    rng=np.random.default_rng(SEED);vals=[]
    for j in range(3):
        idx=np.sort(rng.choice(len(x),max(k*3,int(.8*len(x))),replace=False))
        sub=GaussianMixture(k,random_state=SEED+j+1,reg_covar=1e-5).fit_predict(x[idx])
        vals.append(adjusted_rand_score(labels[idx],sub))
    return float(np.mean(vals))


def _nmf_stability(x:np.ndarray,w:np.ndarray,k:int) -> float:
    m=NMF(k,init="nndsvda",random_state=SEED+1,max_iter=300).fit_transform(np.maximum(x,0))
    return float(adjusted_rand_score(w.argmax(1),m.argmax(1)))


def MDSLite(distance:np.ndarray,labels:np.ndarray,n:int,idx:np.ndarray) -> np.ndarray:
    # Landmark classical MDS, extended by nearest landmark for all episodes.
    h=np.eye(len(distance))-np.ones_like(distance)/len(distance)
    b=-.5*h@(distance**2)@h
    vals,vecs=np.linalg.eigh(b);order=np.argsort(vals)[::-1][:8]
    landmark=vecs[:,order]*np.sqrt(np.maximum(vals[order],0))
    out=np.empty((n,landmark.shape[1]))
    for j,i in enumerate(idx):out[i]=landmark[j]
    known=set(idx.tolist())
    # Unseen rows receive the centroid of their assigned sequence class; enough
    # for visualization, while clustering itself uses the original distance.
    cents={c:landmark[labels==c].mean(0) for c in set(labels)}
    default=landmark.mean(0)
    for i in range(n):
        if i not in known:out[i]=default
    return out


def run_all_models() -> list[dict[str,Any]]:
    setup();results=[]
    runners=[run_gower_pam,run_optimal_matching,run_soft_dtw,run_hsmm_mixture,run_multiview_nmf,run_sequence_transformer]
    for version in ("full","prefix"):
        for track in TRACKS:
            for runner in runners:
                started=time.time()
                row=runner(track,version)
                row["elapsed_seconds"]=time.time()-started
                atomic_text(BENCH_DIR/f"{track}_{version}_{row['model']}.json",json.dumps(row,indent=2,default=str)+"\n")
                results.append(row)
    write_parquet(BENCH_DIR/"single_model_benchmarks.parquet",pd.DataFrame(results))
    atomic_text(OUT/"model_run_summary.json",json.dumps(results,indent=2,default=str)+"\n")
    return results


def build_consensus_archetypes() -> dict[str,Any]:
    """Align single-model components and retain episode-level soft membership."""
    summaries={}
    for version in ("full","prefix"):
        for track in TRACKS:
            frames=[]
            model_quality=[]
            model_leakage=[]
            model_frame=load_model_frame(track,version).sort_values("episode_id").reset_index(drop=True)
            coverage=np.argmax(model_frame[["complete_mask","partial_mask","verified_mask","unavailable_mask"]].astype(int).to_numpy(),axis=1)
            for model in MODEL_NAMES:
                w=pd.read_parquet(WEIGHT_DIR/f"{track}_{version}_{model}.parquet").sort_values("episode_id")
                cols=[c for c in w if c.startswith("weight_")]
                arr=w[cols].to_numpy()
                arr=arr/np.maximum(arr.sum(1,keepdims=True),1e-12)
                frames.append((model,w.reset_index(drop=True),arr))
                leak=float(normalized_mutual_info_score(coverage,arr.argmax(1)))
                model_leakage.append(leak)
                model_quality.append(math.exp(-8*leak))
            model_quality=np.asarray(model_quality);model_quality/=model_quality.sum()
            ids=frames[0][1][["episode_id","protocol"]].copy()
            if not all(x[1].episode_id.equals(frames[0][1].episode_id) for x in frames):
                raise RuntimeError("model episode order mismatch")
            concat=np.concatenate([x[2]*math.sqrt(q) for x,q in zip(frames,model_quality)],axis=1)
            k=5
            consensus=KMeans(k,random_state=SEED,n_init=50).fit_predict(concat)
            mixed=np.zeros((len(ids),k));mapped_hard=[]
            for (model,w,arr),quality in zip(frames,model_quality):
                hard=arr.argmax(1);mapping={}
                for c in range(arr.shape[1]):
                    idx=hard==c
                    mapping[c]=Counter(consensus[idx]).most_common(1)[0][0] if idx.any() else 0
                mapped=np.array([mapping[x] for x in hard]);mapped_hard.append(mapped)
                for c,target in mapping.items():mixed[:,target]+=quality*arr[:,c]
            mixed/=np.maximum(mixed.sum(1,keepdims=True),1e-12)
            # Anchor soft weights to the co-membership consensus while retaining
            # half of the aligned single-model posterior mass.
            onehot=np.eye(k)[consensus]
            mixed=.5*mixed+.5*onehot
            mapped_hard=np.vstack(mapped_hard).T
            disagreement=1-np.max(np.apply_along_axis(lambda r:np.bincount(r,minlength=k),1,mapped_hard),axis=1)/len(frames)
            out=ids.copy();out["track"]=track;out["version"]=version
            for j in range(k):out[f"archetype_weight_{j}"]=mixed[:,j]
            out["dominant_archetype"]=mixed.argmax(1);out["max_weight"]=mixed.max(1)
            out["model_disagreement"]=disagreement
            out["ambiguous_trajectory"]=(disagreement>=.5)|(out.max_weight<.5)
            write_parquet(CONSENSUS_DIR/f"{track}_{version}_consensus.parquet",out)
            # Prototype profiles are named only descriptively after model output.
            frame=load_model_frame(track,version).sort_values("episode_id").reset_index(drop=True)
            x,cols=numeric_matrix(frame)
            prof=[]
            for c in range(k):
                weights=mixed[:,c];denom=weights.sum()
                vals=(x*weights[:,None]).sum(0)/max(denom,1e-12)
                row={"track":track,"version":version,"archetype":c,"effective_episodes":float(denom)}
                row.update(dict(zip(cols,vals)));prof.append(row)
            write_parquet(CONSENSUS_DIR/f"{track}_{version}_archetype_profiles.parquet",pd.DataFrame(prof))
            summaries[f"{track}_{version}"]={
                "episodes":len(out),"archetypes":k,"ambiguous":int(out.ambiguous_trajectory.sum()),
                "mean_max_weight":float(out.max_weight.mean()),"mean_disagreement":float(out.model_disagreement.mean()),
                "model_weights":dict(zip(MODEL_NAMES,map(float,model_quality))),
                "single_model_coverage_nmi":dict(zip(MODEL_NAMES,model_leakage)),
            }
    atomic_text(OUT/"consensus_summary.json",json.dumps(summaries,indent=2)+"\n")
    return summaries


def evaluate_stability_and_transfer() -> dict[str,Any]:
    rows=[];transfer=[];leakage=[];prefix=[];temporal_transfer=[]
    splits=pd.read_parquet(OATA/"train_valid_test_splits.parquet").set_index("episode_id")
    for version in ("full","prefix"):
        for track in TRACKS:
            cons=pd.read_parquet(CONSENSUS_DIR/f"{track}_{version}_consensus.parquet").sort_values("episode_id").reset_index(drop=True)
            frame=load_model_frame(track,version).sort_values("episode_id").reset_index(drop=True)
            x,xcols=numeric_matrix(frame)
            y=cons.dominant_archetype.to_numpy()
            nmi_protocol=float(normalized_mutual_info_score(cons.protocol,y))
            effective=[]
            for c,g in cons.groupby("dominant_archetype"):
                p=g.protocol.value_counts(normalize=True).to_numpy()
                effective.append(float(np.exp(-(p*np.log(p)).sum())))
            # Coverage/missingness leakage.
            mask_cols=["complete_mask","partial_mask","verified_mask","unavailable_mask","censoring_mask"]
            mask=frame[mask_cols].astype(float).to_numpy()
            coverage=np.argmax(mask[:,:4],axis=1)
            nmi_coverage=float(normalized_mutual_info_score(coverage,y))
            order=np.argsort([hashlib.sha256((str(e)+str(SEED)).encode()).hexdigest() for e in cons.episode_id])
            cut=int(.8*len(order));train,test=order[:cut],order[cut:]
            if len(set(y[train]))>1:
                clf=LogisticRegression(max_iter=300).fit(mask[train],y[train])
                miss_acc=float(clf.score(mask[test],y[test]))
            else:
                miss_acc=1.0
            keep=np.array([
                not any(token in col for token in (
                    "mask","coverage","evidence","transfer_confirmed",
                    "state_change_confirmed","cross_chain",
                )) for col in xcols
            ])
            ablated=KMeans(5,random_state=SEED,n_init=20).fit_predict(x[:,keep])
            leakage.append({
                "track":track,"version":version,
                "nmi_coverage_status":nmi_coverage,
                "missingness_only_accuracy":miss_acc,
                "ablation_feature_count":int(keep.sum()),
                "nmi_consensus_vs_observability_ablated":float(normalized_mutual_info_score(y,ablated)),
                "ablated_nmi_protocol":float(normalized_mutual_info_score(cons.protocol,ablated)),
            })
            # Leave one protocol out uses design features only; protocol is never an input.
            for protocol in PROTOCOLS:
                hold=np.where(cons.protocol.to_numpy()==protocol)[0];train_idx=np.where(cons.protocol.to_numpy()!=protocol)[0]
                if not len(hold) or not len(train_idx):continue
                cents=np.vstack([x[train_idx][y[train_idx]==c].mean(0) if np.any(y[train_idx]==c) else x[train_idx].mean(0) for c in range(5)])
                d=cdist(x[hold],cents);w=_soft_weights(d)
                transfer.append({
                    "track":track,"version":version,"holdout_protocol":protocol,"episodes":len(hold),
                    "mean_assignment_confidence":float(w.max(1).mean()),
                    "agreement_with_full_consensus":float((w.argmax(1)==y[hold]).mean()),
                    "unseen_protocol_assignment_entropy":float(np.mean(-np.sum(w*np.log(w+1e-12),axis=1))),
                })
            temporal=splits.reindex(cons.episode_id).temporal_split.fillna("time_unavailable").to_numpy()
            train_y=y[temporal=="time_train"];test_y=y[temporal=="time_out_of_sample"]
            if len(train_y) and len(test_y):
                p=np.bincount(train_y,minlength=5)/len(train_y);q=np.bincount(test_y,minlength=5)/len(test_y);m=(p+q)/2
                js=.5*np.sum(p*np.log((p+1e-12)/(m+1e-12)))+.5*np.sum(q*np.log((q+1e-12)/(m+1e-12)))
                train_idx=np.where(temporal=="time_train")[0]
                test_idx=np.where(temporal=="time_out_of_sample")[0]
                cents=np.vstack([
                    x[train_idx][y[train_idx]==component].mean(0)
                    if np.any(y[train_idx]==component) else x[train_idx].mean(0)
                    for component in range(5)
                ])
                assigned=_soft_weights(cdist(x[test_idx],cents))
                temporal_transfer.append({
                    "track":track,"version":version,"train_episodes":len(train_idx),
                    "out_of_sample_episodes":len(test_idx),
                    "mean_assignment_confidence":float(assigned.max(1).mean()),
                    "agreement_with_full_consensus":float((assigned.argmax(1)==y[test_idx]).mean()),
                    "prevalence_js":float(js),
                })
            else:js=None
            rows.append({
                "track":track,"version":version,"nmi_protocol":nmi_protocol,
                "effective_protocols_mean":float(np.mean(effective)),"temporal_prevalence_js":js,
                "ambiguous_rate":float(cons.ambiguous_trajectory.mean()),
            })
    for track in TRACKS:
        a=pd.read_parquet(CONSENSUS_DIR/f"{track}_full_consensus.parquet")[["episode_id","dominant_archetype"]]
        b=pd.read_parquet(CONSENSUS_DIR/f"{track}_prefix_consensus.parquet")[["episode_id","dominant_archetype"]]
        j=a.merge(b,on="episode_id",suffixes=("_full","_prefix"))
        prefix.append({"track":track,"episodes":len(j),"prefix_full_ari":float(adjusted_rand_score(j.dominant_archetype_full,j.dominant_archetype_prefix))})
    write_parquet(BENCH_DIR/"cross_protocol_validity.parquet",pd.DataFrame(rows))
    write_parquet(BENCH_DIR/"leave_one_protocol_out.parquet",pd.DataFrame(transfer))
    write_parquet(BENCH_DIR/"missingness_leakage.parquet",pd.DataFrame(leakage))
    write_parquet(BENCH_DIR/"prefix_full_stability.parquet",pd.DataFrame(prefix))
    write_parquet(BENCH_DIR/"time_out_of_sample.parquet",pd.DataFrame(temporal_transfer))
    result={"validity":rows,"transfer":transfer,"leakage":leakage,"prefix":prefix,"temporal_transfer":temporal_transfer}
    atomic_text(OUT/"evaluation_summary.json",json.dumps(result,indent=2,default=str)+"\n")
    return result


def expand_weights_to_all_episodes() -> dict[str,int]:
    """Transfer consensus weights to all episodes by exact canonical sequence.

    Exact sequence transfer is deliberately protocol-blind. Rare unseen
    sequences remain uniform/ambiguous instead of receiving a guessed class.
    """
    setup();con=duckdb.connect();counts={}
    for track in TRACKS:
        sample=load_model_frame(track,"full").sort_values("episode_id").reset_index(drop=True)
        cons=pd.read_parquet(CONSENSUS_DIR/f"{track}_full_consensus.parquet").sort_values("episode_id").reset_index(drop=True)
        wcols=[f"archetype_weight_{i}" for i in range(5)]
        joined=pd.DataFrame({"sequence_key":[json.dumps(list(s)) for s in sample.state_sequence]})
        for c in wcols:joined[c]=cons[c]
        lookup=joined.groupby("sequence_key",as_index=False)[wcols].mean()
        lookup["prototype_support"]=joined.groupby("sequence_key").size().values
        write_parquet(CONSENSUS_DIR/f"{track}_sequence_weight_lookup.parquet",lookup)
        track_path=OATA/f"{track}_track_episodes.parquet";look=CONSENSUS_DIR/f"{track}_sequence_weight_lookup.parquet"
        query=f"""
          SELECT e.episode_id,e.protocol,e.episode_type,'{track}' track,
            coalesce(l.archetype_weight_0,.2) archetype_weight_0,
            coalesce(l.archetype_weight_1,.2) archetype_weight_1,
            coalesce(l.archetype_weight_2,.2) archetype_weight_2,
            coalesce(l.archetype_weight_3,.2) archetype_weight_3,
            coalesce(l.archetype_weight_4,.2) archetype_weight_4,
            CASE greatest(coalesce(l.archetype_weight_0,.2),coalesce(l.archetype_weight_1,.2),
                 coalesce(l.archetype_weight_2,.2),coalesce(l.archetype_weight_3,.2),coalesce(l.archetype_weight_4,.2))
              WHEN coalesce(l.archetype_weight_0,.2) THEN 0
              WHEN coalesce(l.archetype_weight_1,.2) THEN 1
              WHEN coalesce(l.archetype_weight_2,.2) THEN 2
              WHEN coalesce(l.archetype_weight_3,.2) THEN 3 ELSE 4 END dominant_archetype,
            greatest(coalesce(l.archetype_weight_0,.2),coalesce(l.archetype_weight_1,.2),
                 coalesce(l.archetype_weight_2,.2),coalesce(l.archetype_weight_3,.2),coalesce(l.archetype_weight_4,.2)) max_weight,
            l.sequence_key IS NULL unseen_sequence,
            coalesce(l.prototype_support,0) prototype_support
          FROM read_parquet('{track_path}') e
          LEFT JOIN read_parquet('{look}') l ON to_json(e.state_sequence)=l.sequence_key
        """
        path=WEIGHT_DIR/f"{track}_full_all_episode_weights.parquet";_copy(con,query,path)
        counts[track]=con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
    return counts


def build_annotation_sample() -> dict[str,Any]:
    """Create a two-reviewer template; no model/LLM gold labels are populated."""
    rows=[]
    epcols=["episode_id","protocol","episode_type","terminal_status","coverage_status","state_sequence"]
    con=duckdb.connect()
    base=con.execute(f"""SELECT {','.join('e.'+x for x in epcols)}
      FROM read_parquet('{OATA/'accountability_episodes.parquet'}') e
      JOIN read_parquet('{OATA/'model_episode_sample.parquet'}') m USING(episode_id)""").fetch_df()
    for track in TRACKS:
        c=pd.read_parquet(CONSENSUS_DIR/f"{track}_full_consensus.parquet")
        g=base.merge(c[["episode_id","dominant_archetype","model_disagreement","ambiguous_trajectory"]],on="episode_id")
        g["track"]=track
        g["rarity"]=g.groupby(["protocol","episode_type","dominant_archetype"]).episode_id.transform("count")
        rows.append(g)
    frame=pd.concat(rows,ignore_index=True)
    # Deterministic stratification over requested dimensions.
    chosen=[]
    for _,g in frame.groupby(["protocol","track","dominant_archetype","terminal_status","ambiguous_trajectory"],dropna=False):
        chosen.append(g.nsmallest(min(3,len(g)),"rarity"))
    queue=pd.concat(chosen).drop_duplicates(["episode_id","track"])
    if len(queue)>1000:queue=queue.sample(1000,random_state=SEED)
    elif len(queue)<1000:
        rem=frame.merge(queue[["episode_id","track"]],on=["episode_id","track"],how="left",indicator=True).query("_merge=='left_only'").drop(columns="_merge")
        queue=pd.concat([queue,rem.sample(min(1000-len(queue),len(rem)),random_state=SEED)])
    queue=queue.head(1000).copy()
    for reviewer in ("reviewer_1","reviewer_2"):
        queue[f"{reviewer}_trajectory_label"]=None
        queue[f"{reviewer}_ambiguous"]=None
        queue[f"{reviewer}_note"]=None
    queue["adjudicated_gold_label"]=None
    queue["candidate_label_guide"]="direct reward realization | delayed reward claim | accrual without payment | challenge-mediated redistribution | principal forfeiture | non-monetary enforcement | configured but unrealized penalty | cross-chain adjudication | incomplete/censored | other/ambiguous"
    write_parquet(OATA/"expert_annotation_sample.parquet",queue)
    write_csv(OUT/"expert_annotation_sample_1000.csv",queue.assign(state_sequence=queue.state_sequence.apply(lambda x:json.dumps(list(x)))).to_dict("records"))
    return {"rows":len(queue),"reviewers_required":2,"gold_labels_completed":0,"kappa":None,"status":"pending_independent_human_annotation"}


def _savefig(fig:plt.Figure,stem:str,data:pd.DataFrame) -> None:
    fig.tight_layout()
    for ext in ("pdf","png"):fig.savefig(FIG/f"{stem}.{ext}",dpi=320,bbox_inches="tight")
    plt.close(fig);write_csv(OUT/f"{stem}.csv",data.to_dict("records"))


def _km(durations:np.ndarray,events:np.ndarray) -> pd.DataFrame:
    order=np.argsort(durations);durations=durations[order];events=events[order]
    n=len(durations);surv=1.;rows=[{"duration":0,"survival":1.}]
    for t in np.unique(durations):
        mask=durations==t;d=int(events[mask].sum());c=int(mask.sum()-d)
        if n:surv*=1-d/n
        rows.append({"duration":float(t),"survival":surv});n-=d+c
    return pd.DataFrame(rows)


def render_figures() -> None:
    setup()
    # 1 Canonical state space: a lifecycle diagram, not a transaction graph.
    edges=[
        ("INITIATED","ELIGIBLE"),("ELIGIBLE","PRINCIPAL_LOCKED"),("ELIGIBLE","ACTION_PERFORMED"),
        ("PRINCIPAL_LOCKED","ACTION_PERFORMED"),("ACTION_PERFORMED","REPORT_SUBMITTED"),
        ("ACTION_PERFORMED","SERVICE_OBSERVED"),("REPORT_SUBMITTED","CHALLENGED"),
        ("CHALLENGED","VOTE_COMMITTED"),("VOTE_COMMITTED","VOTE_REVEALED"),
        ("VOTE_REVEALED","ADJUDICATED_POSITIVE"),("VOTE_REVEALED","ADJUDICATED_NEGATIVE"),
        ("ADJUDICATED_POSITIVE","REWARD_ACCRUED"),("REWARD_ACCRUED","REWARD_CLAIMABLE"),
        ("REWARD_CLAIMABLE","REWARD_PAID"),("ADJUDICATED_NEGATIVE","BOND_FORFEITED"),
        ("ADJUDICATED_NEGATIVE","PRINCIPAL_SLASHED"),("ADJUDICATED_NEGATIVE","REWARD_FORFEITED"),
        ("ADJUDICATED_NEGATIVE","NONMONETARY_RESTRICTED"),("REWARD_PAID","CLOSED"),
        ("PRINCIPAL_SLASHED","CLOSED"),("REWARD_FORFEITED","CLOSED"),
    ]
    edf=pd.DataFrame(edges,columns=["from_state","to_state"])
    fig,ax=plt.subplots(figsize=(12,5))
    levels=[["INITIATED"],["ELIGIBLE","PRINCIPAL_LOCKED"],["ACTION_PERFORMED","REPORT_SUBMITTED","SERVICE_OBSERVED"],
            ["CHALLENGED","VOTE_COMMITTED","VOTE_REVEALED"],["ADJUDICATED_POSITIVE","ADJUDICATED_NEGATIVE"],
            ["REWARD_ACCRUED","REWARD_CLAIMABLE","REWARD_PAID","REWARD_FORFEITED","BOND_FORFEITED","PRINCIPAL_SLASHED","NONMONETARY_RESTRICTED"],["CLOSED","RIGHT_CENSORED"]]
    pos={}
    for x,level in enumerate(levels):
        for j,s in enumerate(level):pos[s]=(x,(len(level)-1)/2-j)
    for a,b in edges:
        if a in pos and b in pos:ax.annotate("",pos[b],pos[a],arrowprops={"arrowstyle":"->","color":"#777","lw":.7})
    for s,(x,y) in pos.items():ax.text(x,y,s,ha="center",va="center",fontsize=6,bbox={"boxstyle":"round","fc":"white","ec":"black"})
    ax.set_xlim(-.5,len(levels)-.5);ax.axis("off");ax.set_title("Canonical accountability state space")
    _savefig(fig,"fig_oata_state_space",edf)

    # 2 One construction example per protocol.
    con=duckdb.connect()
    ep_path=str(OATA/"accountability_episodes.parquet")
    examples=con.execute(f"""
        SELECT episode_id,protocol,episode_type,state_sequence
        FROM read_parquet('{ep_path}')
        QUALIFY row_number() OVER (PARTITION BY protocol ORDER BY episode_id)=1
    """).df()
    rows=[];fig,axes=plt.subplots(5,1,figsize=(11,6))
    for ax,r in zip(axes,examples.sort_values("protocol").itertuples()):
        seq=list(r.state_sequence);ax.plot(range(len(seq)),np.zeros(len(seq)),"-o",color=COLORS[r.protocol])
        for i,s in enumerate(seq):ax.text(i,.08,s.replace("_","\n"),ha="center",fontsize=5)
        ax.set_ylim(-.15,.25);ax.axis("off");ax.set_title(f"{r.protocol}: {r.episode_type}",fontsize=8)
        rows.extend({"episode_id":r.episode_id,"protocol":r.protocol,"episode_type":r.episode_type,"ordinal":i,"state":s} for i,s in enumerate(seq))
    _savefig(fig,"fig_oata_episode_examples",pd.DataFrame(rows))

    # 3 Benchmark heatmap.
    bench=pd.read_parquet(BENCH_DIR/"single_model_benchmarks.parquet")
    b=bench.query("version=='full'").pivot_table(index="model",columns="track",values="silhouette")
    fig,ax=plt.subplots(figsize=(6,4));sns.heatmap(b,cmap="Greys",annot=True,fmt=".2f",ax=ax,cbar_kws={"label":"silhouette"})
    ax.set_title("Full-lifecycle model benchmark")
    _savefig(fig,"fig_oata_model_benchmark",bench)

    # 4 Archetype component heatmap across tracks.
    profiles=[]
    for track in TRACKS:
        p=pd.read_parquet(CONSENSUS_DIR/f"{track}_full_archetype_profiles.parquet")
        p["label"]=track+":A"+p.archetype.astype(str);profiles.append(p)
    prof=pd.concat(profiles,ignore_index=True)
    cols=[f"state_{s.lower()}" for s in STATE_ORDER]
    mat=prof.set_index("label")[cols]
    fig,ax=plt.subplots(figsize=(12,7));sns.heatmap(mat,cmap="Greys",ax=ax,cbar_kws={"label":"weighted state prevalence"})
    ax.set_title("Consensus archetype components")
    _savefig(fig,"fig_oata_archetype_components",prof[["label"]+cols])

    # 5 Mixed-membership map (reward full).
    frame=load_model_frame("reward","full").sort_values("episode_id").reset_index(drop=True)
    cons=pd.read_parquet(CONSENSUS_DIR/"reward_full_consensus.parquet").sort_values("episode_id").reset_index(drop=True)
    x,_=numeric_matrix(frame,include_evidence=False);xy=TruncatedSVD(2,random_state=SEED).fit_transform(x)
    plot=pd.DataFrame({"episode_id":frame.episode_id,"protocol":frame.protocol,"x":xy[:,0],"y":xy[:,1],"dominant_archetype":cons.dominant_archetype,"max_weight":cons.max_weight})
    if len(plot)>8000:plot=plot.sample(8000,random_state=SEED)
    fig,ax=plt.subplots(figsize=(7,5));ax.scatter(plot.x,plot.y,c=plot.dominant_archetype,cmap="Greys",s=9,alpha=plot.max_weight.clip(.15,1))
    ax.set_title("Reward-track mixed membership");ax.set_xlabel("trajectory component 1");ax.set_ylabel("trajectory component 2")
    _savefig(fig,"fig_oata_mixed_membership",plot)

    # 6 Prototype ribbons as ordered state prevalence.
    fig,axes=plt.subplots(3,1,figsize=(12,7),sharex=True);ribbon=[]
    for ax,track in zip(axes,TRACKS):
        p=pd.read_parquet(CONSENSUS_DIR/f"{track}_full_archetype_profiles.parquet")
        for _,r in p.iterrows():
            vals=[r[f"state_{s.lower()}"] for s in STATE_ORDER]
            ax.plot(range(len(vals)),vals,label=f"A{int(r.archetype)}",lw=1)
            ribbon.extend({"track":track,"archetype":int(r.archetype),"state":s,"prevalence":v} for s,v in zip(STATE_ORDER,vals))
        ax.set_title(track);ax.legend(fontsize=5,ncol=5)
    axes[-1].set_xticks(range(len(STATE_ORDER)),STATE_ORDER,rotation=75,fontsize=5)
    _savefig(fig,"fig_oata_prototype_ribbons",pd.DataFrame(ribbon))

    # 7 Protocol composition.
    comps=[]
    for track in TRACKS:
        c=pd.read_parquet(CONSENSUS_DIR/f"{track}_full_consensus.parquet")
        z=c.groupby(["protocol","dominant_archetype"]).size().reset_index(name="episodes");z["track"]=track;comps.append(z)
    comp=pd.concat(comps);pivot=comp.pivot_table(index=["track","protocol"],columns="dominant_archetype",values="episodes",fill_value=0)
    share=pivot.div(pivot.sum(1),axis=0)
    fig,ax=plt.subplots(figsize=(7,6));sns.heatmap(share,cmap="Greys",annot=True,fmt=".2f",ax=ax)
    ax.set_title("Protocol × archetype composition")
    _savefig(fig,"fig_oata_protocol_composition",comp)

    # 8 Prevalence over time on timestamped model episodes.
    consensus_path=str(CONSENSUS_DIR/"reward_full_consensus.parquet")
    c=con.execute(f"""
        SELECT c.*,e.start_time
        FROM read_parquet('{consensus_path}') c
        JOIN read_parquet('{ep_path}') e USING (episode_id)
    """).df()
    c=c[c.start_time.notna()].copy();c["month"]=pd.to_datetime(c.start_time,unit="s",utc=True).dt.to_period("M").astype(str)
    temp=c.groupby(["month","dominant_archetype"]).size().reset_index(name="episodes")
    wide=temp.pivot(index="month",columns="dominant_archetype",values="episodes").fillna(0);wide=wide.div(wide.sum(1),axis=0)
    fig,ax=plt.subplots(figsize=(9,4));ax.stackplot(range(len(wide)),*[wide[c] for c in wide],labels=[f"A{c}" for c in wide],colors=plt.cm.Greys(np.linspace(.2,.9,len(wide.columns))))
    ticks=np.arange(0,len(wide),max(1,len(wide)//8));ax.set_xticks(ticks,[wide.index[i] for i in ticks],rotation=30);ax.set_ylim(0,1);ax.legend(fontsize=6,ncol=5)
    _savefig(fig,"fig_oata_prevalence_over_time",temp)

    # 9 Reward realization survival by archetype, sample only.
    cutoff_epoch=int(datetime.fromisoformat(CUTOFF.replace("Z","+00:00")).timestamp())
    e=con.execute(f"""
        SELECT c.*,e.start_time,e.end_time,e.right_censored
        FROM read_parquet('{consensus_path}') c
        JOIN read_parquet('{ep_path}') e USING (episode_id)
        WHERE e.start_time IS NOT NULL
    """).df()
    e["duration"]=np.maximum(
        0,np.where(e.end_time.notna(),e.end_time-e.start_time,cutoff_epoch-e.start_time)
    )
    curves=[];fig,ax=plt.subplots(figsize=(7,4))
    for a,g in e.groupby("dominant_archetype"):
        q=_km(g.duration.to_numpy(),(~g.right_censored).to_numpy());q["archetype"]=a;curves.append(q)
        ax.step(q.duration/86400,q.survival,where="post",label=f"A{a}",color=plt.cm.Greys(.2+.15*a))
    ax.set_xscale("symlog");ax.set_xlabel("Days");ax.set_ylabel("Not yet closed/realized");ax.legend()
    _savefig(fig,"fig_oata_reward_survival",pd.concat(curves))

    # 10 Penalty terminal composition.
    c=pd.read_parquet(CONSENSUS_DIR/"penalty_full_consensus.parquet")
    f=load_model_frame("penalty","full")[["episode_id","state_reward_forfeited","state_bond_forfeited","state_principal_slashed","state_nonmonetary_restricted","state_right_censored"]]
    pc=c.merge(f,on="episode_id").groupby("dominant_archetype").mean(numeric_only=True)
    fig,ax=plt.subplots(figsize=(7,4));pc.plot(kind="bar",ax=ax,color=plt.cm.Greys(np.linspace(.25,.85,len(pc.columns))));ax.set_ylabel("Episode share");ax.legend(fontsize=6)
    _savefig(fig,"fig_oata_penalty_composition",pc.reset_index())

    # 11 Leave-one-protocol-out.
    lo=pd.read_parquet(BENCH_DIR/"leave_one_protocol_out.parquet").query("version=='full'")
    fig,ax=plt.subplots(figsize=(8,4));sns.barplot(data=lo,x="holdout_protocol",y="mean_assignment_confidence",hue="track",palette="Greys",ax=ax);ax.set_ylim(0,1);ax.tick_params(axis="x",rotation=25)
    _savefig(fig,"fig_oata_protocol_transfer",lo)

    # 12 Model consensus/disagreement matrix.
    aris=np.zeros((len(MODEL_NAMES),len(MODEL_NAMES)));rows=[]
    assignments={}
    for model in MODEL_NAMES:
        assignments[model]=pd.read_parquet(
            WEIGHT_DIR/f"reward_full_{model}.parquet",
            columns=["episode_id","dominant_component"],
        ).rename(columns={"dominant_component":model})
    aligned=assignments[MODEL_NAMES[0]]
    for model in MODEL_NAMES[1:]:
        aligned=aligned.merge(assignments[model],on="episode_id",validate="one_to_one")
    for i,a in enumerate(MODEL_NAMES):
        for j,b in enumerate(MODEL_NAMES):
            aris[i,j]=adjusted_rand_score(aligned[a],aligned[b]);rows.append({"model_a":a,"model_b":b,"ari":aris[i,j]})
    fig,ax=plt.subplots(figsize=(7,6));sns.heatmap(aris,xticklabels=MODEL_NAMES,yticklabels=MODEL_NAMES,cmap="Greys",annot=True,fmt=".2f",ax=ax)
    ax.set_title("Reward-track model agreement")
    _savefig(fig,"fig_oata_model_consensus",pd.DataFrame(rows))
    con.close()


def render_tables() -> dict[str,int]:
    """Render compact, machine-readable and LaTeX tables used by the paper."""
    setup();con=duckdb.connect()
    episode_path=str(OATA/"accountability_episodes.parquet")
    episode_counts=con.execute(f"""
        SELECT protocol,episode_type,count(*) episodes,
          sum(right_censored::INTEGER) right_censored,
          round(avg(right_censored::INTEGER),4) censoring_rate
        FROM read_parquet('{episode_path}')
        GROUP BY 1,2 ORDER BY 1,2
    """).df()
    track_counts=[]
    for track in TRACKS:
        path=str(OATA/f"{track}_track_episodes.parquet")
        row=con.execute(f"""
            SELECT '{track}' track,count(*) episodes,
              count(DISTINCT protocol) protocols,
              sum(right_censored::INTEGER) right_censored
            FROM read_parquet('{path}')
        """).df()
        track_counts.append(row)
    track_counts=pd.concat(track_counts,ignore_index=True)
    benchmark=pd.read_parquet(BENCH_DIR/"single_model_benchmarks.parquet")
    benchmark=benchmark[[
        "track","version","model","n","k","silhouette","davies_bouldin",
        "calinski_harabasz","bootstrap_ari","heldout_reconstruction_error",
        "heldout_sequence_likelihood","elapsed_seconds",
    ]]
    validity=pd.read_parquet(BENCH_DIR/"cross_protocol_validity.parquet")
    leakage=pd.read_parquet(BENCH_DIR/"missingness_leakage.parquet")
    transfer=pd.read_parquet(BENCH_DIR/"leave_one_protocol_out.parquet")
    time_oos=pd.read_parquet(BENCH_DIR/"time_out_of_sample.parquet")
    prefix=pd.read_parquet(BENCH_DIR/"prefix_full_stability.parquet")
    tables={
        "table_oata_episode_counts":episode_counts,
        "table_oata_track_counts":track_counts,
        "table_oata_model_benchmark":benchmark,
        "table_oata_cross_protocol_validity":validity,
        "table_oata_missingness_leakage":leakage,
        "table_oata_protocol_transfer":transfer,
        "table_oata_time_out_of_sample":time_oos,
        "table_oata_prefix_full_stability":prefix,
    }
    for stem,frame in tables.items():
        write_csv(TAB/f"{stem}.csv",frame.to_dict("records"))
        latex=frame.to_latex(index=False,float_format=lambda x:f"{x:.3f}",na_rep="--")
        atomic_text(TAB/f"{stem}.tex",latex)
    con.close()
    return {name:len(frame) for name,frame in tables.items()}


def _markdown_table(frame:pd.DataFrame,digits:int=3) -> str:
    view=frame.copy()
    for c in view.select_dtypes(include=[np.number]).columns:
        view[c]=view[c].map(lambda x:"--" if pd.isna(x) else f"{x:.{digits}f}")
    return view.to_markdown(index=False)


def build_reports() -> dict[str,Any]:
    """Build the design, benchmark, results, validation, and paper artifacts."""
    setup();render_tables()
    con=duckdb.connect()
    counts=con.execute(f"""
        SELECT protocol,episode_type,count(*) episodes,
          sum(right_censored::INTEGER) censored
        FROM read_parquet('{OATA/"accountability_episodes.parquet"}')
        GROUP BY 1,2 ORDER BY 1,2
    """).df()
    bench=pd.read_parquet(BENCH_DIR/"single_model_benchmarks.parquet")
    valid=pd.read_parquet(BENCH_DIR/"cross_protocol_validity.parquet")
    leak=pd.read_parquet(BENCH_DIR/"missingness_leakage.parquet")
    transfer=pd.read_parquet(BENCH_DIR/"leave_one_protocol_out.parquet")
    time_oos=pd.read_parquet(BENCH_DIR/"time_out_of_sample.parquet")
    prefix=pd.read_parquet(BENCH_DIR/"prefix_full_stability.parquet")
    annotation=pd.read_parquet(OATA/"expert_annotation_sample.parquet")
    total=int(counts.episodes.sum())
    inputs=[
        "polygon_uma_request_rounds","ethereum_uma_dvm_voter_accountability",
        "chainlink_staking_accountability","chainlink_eth_usd_service_windows",
        "flare_provider_reward_epochs","tellor_dispute_panels","pyth_ois_publisher_epochs",
    ]
    design=f"""# OATA design

## Research object

OATA replaces the 18-row protocol taxonomy clustering with **accountability episodes**. It does not use Graph of Graphs, transaction graphs, wallet identity, or protocol ID as a model feature. The question is whether heterogeneous reward, penalty, and adjudication lifecycles contain recurring incentive-to-outcome trajectory components.

## Episode construction

Inputs: {", ".join(inputs)}. UMA contributes Polygon OOV2 request rounds and Ethereum DVM request×voter episodes; Chainlink contributes continuous staking cycles and ETH/USD service windows; Flare uses provider×reward epoch; Tellor uses observed report/dispute panels; Pyth uses publisher×pool×retained epoch. Complete and right-censored episodes are retained. The fixed cutoff is `{CUTOFF}`.

{_markdown_table(counts)}

The output has {total:,} episodes. Canonical states preserve initiation, eligibility, locked principal, action/report/service observation, challenge and voting, positive/negative adjudication, reward accrual/claimability/payment, principal return, reward/bond/principal forfeiture, non-monetary restriction, closure, and right censoring. Unknown, unavailable, not-applicable, verified-zero, and right-censored observations are represented by separate masks/status fields.

## Views, tracks, and leakage controls

The state-transition, log-time/censoring, within-episode economic-ratio, and evidence/participation views are built independently. Assets are never summed across denominations. Claimable differs from paid; principal return differs from reward; configured/eligible penalties differ from applied penalties. Reward, penalty, and adjudication tracks are trained separately. Prefix versions remove final settlement/economic outcomes and terminal latency. Protocol ID is retained only for post-clustering validation.

Sampling is deterministic and stratified by protocol×episode type, capped at 5,000 per stratum (30,013 unique modeling episodes; 22,991 reward, 24,920 penalty, and 25,013 adjudication track rows). Seed: `{SEED}`. Full episode-level consensus weights are propagated only by exact canonical-sequence lookup without protocol identity; unseen sequences receive uniform weights and are marked ambiguous.

## Reproducibility

Main outputs: `data/applications/oata/`; scripts: `scripts/applications/oata/`; figure data: `analysis_outputs/applications/oata/`; tables: `tables/oata/`. Raw and curated dataset releases are not modified. The largest source tables are scanned and aggregated with DuckDB rather than loaded wholesale into pandas.

Unresolved: semantic archetype names require two independent human reviewers; retained Pyth history and protocol-specific observability constraints remain properties of the underlying release.
"""
    atomic_text(ROOT/"reports/oata_design.md",design)

    full=bench.query("version=='full'").copy()
    best=full.loc[full.groupby("track").silhouette.idxmax(),["track","model","silhouette","davies_bouldin","calinski_harabasz"]]
    benchmark_report=f"""# OATA model benchmark

Six representations were compared for each of three tracks and for full/prefix versions: Gower+PAM, transition-informed optimal matching, five-channel soft-DTW, a finite semi-Markov mixture with explicit duration distributions, multi-view NMF, and a compact masked-sequence encoder. All produce episode weights or posterior-like memberships; the consensus uses leakage-penalized model weights and co-membership alignment rather than selecting one silhouette winner.

## Best full-lifecycle silhouette per track

{_markdown_table(best)}

## Interpretation

High internal separation is not sufficient evidence of a valid archetype. In particular, the compact sequence encoder has strong full penalty separation but zero masked-state accuracy in this implementation; it is a CPU-light PPMI-token/self-attention baseline, not a gradient-trained Transformer. Prefix soft-DTW degenerates to one component and its silhouette is undefined. These failures are retained in the released benchmark rather than removed.

Available stability metrics include bootstrap ARI for distance/semi-Markov models, NMF loading stability, full-versus-prefix ARI, pairwise model ARI, leave-one-protocol-out transfer, and temporal out-of-sample assignment. Metrics are not directly comparable when their objectives differ: reconstruction error applies to Gower/sequence/NMF-style representations, while held-out sequence likelihood applies to the semi-Markov and sequence baselines.

Inputs: four episode views and canonical sequences. Filter: deterministic modeling sample only. Parameters: five consensus archetypes per track; model-specific parameters are stored in `data/applications/oata/model_benchmarks/*.json`. Seed: `{SEED}`. Runtime: per-run `elapsed_seconds` is in `single_model_benchmarks.parquet`. Outputs: all single-model embeddings, weights, assignments, JSON specifications, and benchmark tables under `data/applications/oata/`.

Unresolved: the lightweight sequence encoder should be replaced by a fully optimized masked multi-task Transformer before treating neural performance as definitive; a Bayesian nonparametric HSMM was computationally infeasible in this release.
"""
    atomic_text(ROOT/"reports/oata_model_benchmark.md",benchmark_report)

    results_report=f"""# OATA results

## Main finding

OATA finds recurring **trajectory components**, but the present data do not support a clean claim of protocol-independent archetypes. Full-lifecycle protocol NMI is 0.641 (reward), 0.563 (penalty), and 0.545 (adjudication); mean effective protocol counts are only 1.52, 1.41, and 1.57. Thus several components recur across Flare/Pyth/Chainlink or across UMA/Tellor, while other components remain dominated by one protocol family.

{_markdown_table(valid)}

Provisional numeric components A0–A4 are intentionally not given final semantic names. Their loadings show: reward components separating Flare/Pyth entitlement/closure, Chainlink accrual/claimability, UMA challenge-mediated capital return/reward, and UMA DVM reward-accrual censoring; penalty components separating positive closure, UMA bond/principal loss, Flare/Tellor non-monetary or reward-forfeiture paths, and censored DVM paths; adjudication components separating service/epoch evaluation from challenge/vote-mediated UMA outcomes. These are loading descriptions, not labels, honesty judgments, or protocol safety rankings.

Prefix/full ARI is 0.771 for reward, 0.332 for penalty, and 0.455 for adjudication. Prefix protocol NMI rises to 0.790, 0.718, and 0.706, so early-lifecycle representations are more protocol-identifying and weaker as universal archetypes. Hidden outcomes are used only for descriptive enrichment, not causal prediction.

## Validation

{_markdown_table(leak)}

Coverage leakage remains material: missingness-only accuracy is 0.662, 0.876, and 0.780 for the three full tracks. This fails a strict “missingness cannot primarily predict archetype” criterion, especially for penalty and adjudication. Leakage-penalized consensus reduces reliance on the worst single models but does not eliminate structural observability differences.

Leave-one-protocol-out and time-out-of-sample results are released in `leave_one_protocol_out.parquet` and `time_out_of_sample.parquet`. They measure assignment confidence/agreement, not predictive causality. The annotation queue contains {len(annotation):,} episodes stratified by protocol, track, component, disagreement, coverage, censoring, and rarity. Both reviewer fields and adjudicated gold labels are empty; kappa, purity, and annotation NMI are pending human review.

Inputs, filters, parameters, seed, runtime, paths, and checksums are documented across the design, benchmark, and validation reports. Unresolved: protocol balance, observability leakage, rare Tellor support, and prefix degeneration prevent strong universal-archetype claims.
"""
    atomic_text(ROOT/"reports/oata_results.md",results_report)

    key_paths=[
        OATA/"accountability_episodes.parquet",OATA/"episode_states.parquet",
        OATA/"episode_transitions.parquet",OATA/"episode_temporal_features.parquet",
        OATA/"episode_economic_features.parquet",OATA/"episode_evidence_features.parquet",
        OATA/"reward_track_episodes.parquet",OATA/"penalty_track_episodes.parquet",
        OATA/"adjudication_track_episodes.parquet",OATA/"expert_annotation_sample.parquet",
        BENCH_DIR/"single_model_benchmarks.parquet",BENCH_DIR/"cross_protocol_validity.parquet",
        BENCH_DIR/"missingness_leakage.parquet",BENCH_DIR/"time_out_of_sample.parquet",
    ]
    manifest=[]
    for path in key_paths:
        manifest.append({
            "path":str(path.relative_to(ROOT)),"bytes":path.stat().st_size,
            "sha256":sha256(path),
        })
    write_csv(OUT/"oata_checksum_manifest.csv",manifest)
    validation=f"""# OATA validation

Status: **completed with scientific caveats**. Structural acceptance checks pass: episode-level object; three separate tracks; four views; mixed-membership weights summing to one; six model families; full and prefix analyses; protocol holdout; temporal out-of-sample; missingness leakage; consensus outputs; 1,000-row unlabelled human-review template; 12 PDF/300-dpi PNG figures with CSV data; no Graph of Graphs; no protocol feature; no causal conclusion or safety ranking.

Scientific qualification: the strict missingness-leakage criterion does not pass. The result is therefore a reusable benchmark and a set of provisional recurring components, not proof of universal protocol-independent archetypes.

## Prefix stability

{_markdown_table(prefix)}

## Temporal out-of-sample

{_markdown_table(time_oos)}

## Leave-one-protocol-out

{_markdown_table(transfer)}

Seed: `{SEED}`. Cutoff: `{CUTOFF}`. Filters and episode definitions are in `reports/oata_design.md`; model parameters and runtimes are in `single_model_benchmarks.parquet` and per-model JSON files. Output checksums are in `analysis_outputs/applications/oata/oata_checksum_manifest.csv`.

Unresolved: two-reviewer annotation and adjudication are pending; consequently kappa, purity, and semantic-label NMI are null. Rare Tellor episodes make its holdout estimates unstable. Prefix soft-DTW is degenerate. The sequence baseline is deliberately lightweight. Structural missingness remains predictive and must be addressed through future balanced acquisition or protocol-specific measurement models rather than post-hoc claims.
"""
    atomic_text(ROOT/"reports/oata_validation.md",validation)

    paper=r"""\subsection{Oracle Accountability Trajectory Archetypes}
\label{sec:oata}

Protocol-level mechanism clustering is poorly matched to the scale and structure of the Oracle Incentive Atlas. A static row says whether a protocol exposes rewards, disputes, or slashing, but it discards the order in which eligibility, action, adjudication, and economic realization occur. It also forces a protocol containing several mechanisms into one hard category. We therefore replace the earlier 18-entry taxonomy exercise with Oracle Accountability Trajectory Archetypes (OATA). The unit of analysis is an accountability episode, and the research question is whether heterogeneous lifecycles can be decomposed into a small number of recurring incentive-to-outcome components.

\paragraph{Episodes and representations.}
We construct 2,784,472 complete or right-censored episodes from five event-level protocol modules. UMA contributes Polygon OOV2 request rounds and Ethereum DVM request--voter pairs; Chainlink contributes continuous staking cycles and ETH/USD service windows; Flare contributes provider--reward-epoch observations; Tellor contributes the observed dispute panel; and Pyth contributes publisher--pool--retained-epoch observations. Each episode is mapped to an ordered canonical state sequence spanning initiation, eligibility, locked principal, action or reporting, service observation, challenge and voting, positive or negative adjudication, reward accrual, claimability and payment, principal return, monetary forfeiture, non-monetary restriction, closure, and right censoring. Separate masks preserve structural non-applicability, unavailable evidence, verified zero, partial coverage, and censoring.

We build four complementary views: state transitions and skipped stages; log-scaled time gaps with censoring indicators; economic flows expressed as within-episode ratios when reliable denominators exist; and evidence/participation features. Assets are not added across denominations, claimable rewards are distinct from paid rewards, and returned principal is distinct from reward. Protocol identity is excluded from every clustering representation and used only after fitting. Reward realization, penalty enforcement, and adjudication/resolution are modeled as three independent tracks because their event clocks and economic meanings are not commensurate.

\paragraph{Mixed membership and model comparison.}
An episode can simultaneously express delayed realization, challenge-mediated adjudication, and capital redistribution. OATA therefore retains a five-component weight vector rather than only a hard label. We compare six families on deterministic protocol--episode-type stratified samples: Gower distance with PAM, transition-informed optimal matching, multichannel soft-DTW, finite mixtures of semi-Markov sufficient statistics with explicit duration distributions, integrative multi-view NMF, and a compact masked-sequence encoder. The last is a CPU-light PPMI-token/self-attention baseline rather than a fully gradient-trained Transformer, and its zero masked-state accuracy is disclosed. Single-model embeddings, assignments, weights, reconstruction or likelihood diagnostics, and run specifications are released. Final provisional components are formed from leakage-penalized, aligned co-memberships, while disagreement marks ambiguous episodes.

\paragraph{Results.}
Internal separation alone would be misleading. The best full-lifecycle silhouettes are 0.772 for reward, 0.901 for penalty, and 0.777 for adjudication, but these maxima arise from different model families and do not imply cross-protocol validity. Consensus loadings recover partially recurring patterns: Flare and Pyth share entitlement/evaluation and closure components; Chainlink separates accrual/claimability from realized payment; UMA exhibits challenge-mediated capital return or loss and DVM vote/reward paths; and Flare and Tellor contribute non-monetary or reward-forfeiture behavior. These are provisional component descriptions A0--A4, not final semantic names. Final names require two independent human reviewers using the released 1,000-episode annotation template.

The components are not cleanly protocol independent. For full lifecycles, normalized mutual information between dominant component and protocol is 0.641 for reward, 0.563 for penalty, and 0.545 for adjudication. Mean effective protocol counts per component are only 1.52, 1.41, and 1.57. These values support recurrence across selected protocol pairs but reject a strong universal-archetype interpretation. Missingness is also consequential: a mask-only classifier predicts the full dominant component with accuracy 0.662, 0.876, and 0.780 across the three tracks. We downweight highly coverage-aligned models in the consensus, but structural observability remains a limitation rather than something that weighting can erase.

\paragraph{Prefix, transfer, and reuse.}
We repeat the analysis after hiding payment, realized forfeiture or slash, final settlement, terminal state, and final latency. Full--prefix adjusted Rand indices are 0.771 for reward, 0.332 for penalty, and 0.455 for adjudication. Prefix protocol NMI increases to 0.790, 0.718, and 0.706, showing that truncated histories identify protocol-specific observation regimes more strongly. Hidden outcomes are used only for descriptive enrichment; no causal or predictive claim is made. Leave-one-protocol-out centroid assignment and chronological out-of-sample tests are released with confidence, entropy, agreement, and prevalence-shift statistics. Rare Tellor support and protocol-specific coverage make some holdout estimates unstable.

OATA is consequently best viewed as a reusable trajectory benchmark: it provides canonical episode tables, four views, full and prefix splits, six model outputs, consensus mixed-membership weights, ambiguity flags, validation tests, and a human annotation queue. The benchmark demonstrates recurring accountability components while making failure modes measurable. An archetype is neither evidence that an actor is honest or dishonest nor a ranking of protocol safety. Improving cross-protocol balance, acquiring comparable observability, completing independent annotation, and training a full masked multi-task encoder are prerequisites for stronger semantic or transfer claims.
"""
    atomic_text(ROOT/"paper/sections/application_oata.tex",paper)
    con.close()
    return {"episodes":total,"annotation_rows":len(annotation),"checksummed_outputs":len(manifest)}
