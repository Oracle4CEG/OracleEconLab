"""Rebuilt Applications analysis: mechanism space, economics, and reference coverage."""
from __future__ import annotations

import json
import math
import re
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.cluster.hierarchy import dendrogram, leaves_list, linkage
from scipy.spatial.distance import squareform
from sklearn.cluster import AgglomerativeClustering
from sklearn.manifold import MDS
from sklearn.metrics import adjusted_rand_score, silhouette_score

from scripts.applications.common import (
    APP, CUTOFF, FIG, MANIFESTS, OUT, PARQUET, ROOT, SEED, TAB,
    atomic_text, latex_escape, pq, qdf, release_checks, sha256, setup,
    write_csv, write_parquet,
)

MECH = APP / "mechanism_space"
ECON = APP / "accountability_economics"
GEO = APP / "geographic_semantic"
APPFIG = ROOT / "figures/applications"
APPOUT = OUT / "rebuilt"
STATUS = ("observed_yes", "observed_no", "not_applicable", "unknown", "structurally_unobservable")
PROTOCOL_COLORS = {
    "UMA": "#1b1b1b", "Chainlink": "#555555", "Flare_FTSOv2": "#888888",
    "Flare": "#888888", "Tellor": "#b0b0b0", "Pyth": "#d0d0d0",
    "Chronicle": "#737373", "RedStone": "#a0a0a0",
}

FAMILIES = {
    "delivery": ["push", "pull", "request_response", "optimistic", "epoch_based", "internal_twap", "cross_chain_relay"],
    "data_source": ["first_party_publisher", "third_party_node", "exchange_aggregation", "protocol_vote", "external_adjudication", "deterministic_onchain"],
    "aggregation": ["median", "weighted_median", "ocr_offchain", "optimistic_acceptance", "token_holder_vote", "provider_epoch_score", "single_source"],
    "subject": ["reporter", "proposer", "disputer", "voter", "staker", "operator", "provider", "publisher_pool", "delegator"],
    "reward": ["explicit_report_reward", "base_staking_emission", "delegation_reward", "accuracy_band_reward", "tip", "dispute_reward", "alert_reward", "voter_redistribution", "finalization_signature_reward", "none_documented"],
    "penalty": ["principal_slash", "bond_forfeiture", "fee_forfeiture", "reward_forfeiture", "loss_of_eligibility", "jail", "chill", "ban", "no_documented_penalty"],
    "truth_basis": ["external_reference", "consensus_median", "optimistic_acceptance", "protocol_vote", "service_availability", "deterministic_onchain_value"],
    "temporal": ["per_report", "per_request", "per_dispute", "per_round", "per_epoch", "per_service_window"],
}
COMPONENTS = [x for values in FAMILIES.values() for x in values]

# This catalog freezes only mechanisms supported by the project's cited official
# documentation/source snapshots. Protocols absent from it remain unknown.
CATALOG = {
    "Chainlink": {"push","third_party_node","exchange_aggregation","ocr_offchain","staker","operator","delegator","base_staking_emission","delegation_reward","alert_reward","principal_slash","reward_forfeiture","service_availability","per_service_window"},
    "Pyth": {"pull","epoch_based","cross_chain_relay","first_party_publisher","exchange_aggregation","weighted_median","publisher_pool","delegator","base_staking_emission","delegation_reward","principal_slash","external_reference","per_epoch"},
    "UMA": {"request_response","optimistic","cross_chain_relay","external_adjudication","protocol_vote","optimistic_acceptance","token_holder_vote","proposer","disputer","voter","explicit_report_reward","dispute_reward","voter_redistribution","bond_forfeiture","fee_forfeiture","principal_slash","optimistic_acceptance","protocol_vote","per_request","per_dispute","per_round"},
    "Tellor": {"push","request_response","third_party_node","protocol_vote","weighted_median","token_holder_vote","reporter","voter","tip","dispute_reward","principal_slash","fee_forfeiture","jail","protocol_vote","per_report","per_dispute"},
    "Flare_FTSOv2": {"push","epoch_based","first_party_publisher","exchange_aggregation","weighted_median","provider_epoch_score","provider","accuracy_band_reward","finalization_signature_reward","reward_forfeiture","loss_of_eligibility","chill","ban","consensus_median","per_round","per_epoch"},
    "Chronicle": {"push","third_party_node","exchange_aggregation","median","operator","dispute_reward","reward_forfeiture","external_reference","per_report"},
    "RedStone": {"push","pull","cross_chain_relay","first_party_publisher","exchange_aggregation","median","operator","no_documented_penalty","external_reference","per_report"},
    "Band": {"push","request_response","cross_chain_relay","third_party_node","weighted_median","reporter","staker","delegator","base_staking_emission","delegation_reward","principal_slash","consensus_median","per_report"},
    "API3": {"push","request_response","first_party_publisher","single_source","provider","base_staking_emission","principal_slash","external_reference","per_report"},
    "DIA": {"push","first_party_publisher","exchange_aggregation","median","provider","base_staking_emission","principal_slash","external_reference","per_report"},
    "Switchboard": {"pull","request_response","third_party_node","median","operator","staker","base_staking_emission","principal_slash","consensus_median","per_report"},
    "Witnet": {"push","request_response","third_party_node","weighted_median","reporter","base_staking_emission","principal_slash","consensus_median","per_report"},
    "Internal": {"internal_twap","deterministic_onchain","deterministic_onchain_value","single_source","not_applicable","per_report"},
    "TWAP": {"internal_twap","deterministic_onchain","deterministic_onchain_value","single_source","not_applicable","per_report"},
    "Uniswap": {"internal_twap","deterministic_onchain","deterministic_onchain_value","single_source","not_applicable","per_report"},
    "Curve": {"internal_twap","deterministic_onchain","deterministic_onchain_value","single_source","not_applicable","per_report"},
    "Binance Oracle": {"push","first_party_publisher","single_source","provider","external_reference","per_report"},
    "Supra": {"push","cross_chain_relay","third_party_node","weighted_median","operator","staker","base_staking_emission","principal_slash","consensus_median","per_report"},
}
COMPLETE_CATALOG = set(CATALOG)


def ensure_dirs() -> None:
    setup()
    for p in (MECH, ECON, GEO, APPFIG, APPOUT):
        p.mkdir(parents=True, exist_ok=True)


def registry_rows() -> list[dict[str, Any]]:
    return [json.loads(x) for x in (ROOT / "registry/oracle_observability_scores.jsonl").open()]


def _family(component: str) -> str:
    return next(k for k, values in FAMILIES.items() if component in values)


def build_mechanism_space() -> dict[str, Any]:
    """Build independent design and observability spaces without unknown=false."""
    ensure_dirs()
    design = []
    missing = []
    for row in registry_rows():
        name = row["oracle_network"]
        known = CATALOG.get(name, set())
        complete = name in COMPLETE_CATALOG
        for component in COMPONENTS:
            if component == "not_applicable":
                continue
            family=_family(component)
            if name in {"Internal","TWAP","Uniswap","Curve"} and family in {"subject","reward","penalty"}:
                value = "not_applicable"
            elif component in known:
                value = "not_applicable" if component == "not_applicable" else "observed_yes"
            elif complete:
                # Negative values are asserted only inside the frozen documented catalog.
                value = "observed_no"
            else:
                value = "unknown"
            design.append({
                "oracle_network": name, "feature_family": family,
                "component": component, "mechanism_feature_status": value,
                "source_class": "frozen_official_documentation_catalog_v1" if complete else "registry_only",
            })
        n_unknown = sum(x["mechanism_feature_status"] == "unknown" for x in design if x["oracle_network"] == name)
        missing.append({
            "oracle_network": name, "unknown_components": n_unknown,
            "total_components": len(COMPONENTS),
            "unknown_share": n_unknown / len(COMPONENTS),
            "primary_eligible": n_unknown / len(COMPONENTS) <= .40,
            "exclusion_reason": None if n_unknown / len(COMPONENTS) <= .40 else "core_design_unknown_share_gt_0.40",
        })
    design_df = pd.DataFrame(design)
    missing_df = pd.DataFrame(missing)
    write_parquet(MECH / "mechanism_design_features.parquet", design_df)
    write_parquet(MECH / "primary_clustering_sample.parquet", missing_df)
    write_parquet(MECH / "mechanism_missingness_matrix.parquet", missing_df)

    obs = []
    for r in registry_rows():
        name = r["oracle_network"]
        structural = name in {"Chronicle", "RedStone"}
        obs.append({
            "oracle_network": name,
            "actor_observable": str(r["publisher_level_observable"]),
            "amount_observable": int(r["reward_observability_score"] >= 3),
            "asset_observable": int(r["reward_observability_score"] >= 3),
            "transaction_observable": int(r["reward_observability_score"] >= 4),
            "state_change_observable": int(r["penalty_observability_score"] >= 4),
            "historical_completeness": int(r["historical_depth_score"]),
            "version_completeness": int(r["historical_depth_score"] >= 4),
            "cross_chain_linkability": int(r["truth_linkability_score"]),
            "payment_observability": int(r["reward_observability_score"]),
            "penalty_observability": int(r["penalty_observability_score"]),
            "structural_unobservability": structural,
            "deep_panel_status": r["deep_panel_status"],
        })
    write_parquet(MECH / "observability_features.parquet", pd.DataFrame(obs))
    return {
        "registry": len(missing_df), "primary_sample": int(missing_df.primary_eligible.sum()),
        "excluded_unknown": int((~missing_df.primary_eligible).sum()),
    }


def _design_matrix() -> tuple[pd.DataFrame, pd.DataFrame]:
    long = pd.read_parquet(MECH / "mechanism_design_features.parquet")
    eligible = pd.read_parquet(MECH / "primary_clustering_sample.parquet").query("primary_eligible")
    sub = long[long.oracle_network.isin(eligible.oracle_network)]
    matrix = sub.assign(value=(sub.mechanism_feature_status == "observed_yes").astype(int)).pivot_table(
        index="oracle_network", columns="component", values="value", aggfunc="max"
    ).fillna(0).astype(int)
    return matrix, eligible


def _jaccard_distance(x: np.ndarray) -> np.ndarray:
    inter = x @ x.T
    sums = x.sum(1)
    union = sums[:, None] + sums[None, :] - inter
    return 1 - np.divide(inter, union, out=np.zeros_like(inter, dtype=float), where=union != 0)


def _kmedoids(distance: np.ndarray, k: int) -> np.ndarray:
    medoids = [int(np.argmin(distance.sum(1)))]
    while len(medoids) < k:
        nearest = distance[:, medoids].min(1)
        nearest[medoids] = -1
        medoids.append(int(np.argmax(nearest)))
    labels = np.argmin(distance[:, medoids], axis=1)
    for _ in range(50):
        old = medoids.copy()
        for c in range(k):
            members = np.where(labels == c)[0]
            if len(members):
                medoids[c] = int(members[np.argmin(distance[np.ix_(members, members)].sum(1))])
        labels = np.argmin(distance[:, medoids], axis=1)
        if old == medoids:
            break
    return labels


def cluster_mechanisms() -> dict[str, Any]:
    matrix, _ = _design_matrix()
    distance = _jaccard_distance(matrix.to_numpy())
    rng = np.random.default_rng(SEED)
    metrics, labels_by_k = [], {}
    for k in range(2, min(7, len(matrix))):
        labels = AgglomerativeClustering(n_clusters=k, metric="precomputed", linkage="average").fit_predict(distance)
        pam = _kmedoids(distance, k)
        stability = []
        for _ in range(50):
            idx = np.sort(rng.choice(len(matrix), size=max(k + 2, math.ceil(.8 * len(matrix))), replace=False))
            sub = AgglomerativeClustering(n_clusters=k, metric="precomputed", linkage="average").fit_predict(distance[np.ix_(idx, idx)])
            stability.append(adjusted_rand_score(labels[idx], sub))
        metrics.append({
            "k": k, "silhouette": float(silhouette_score(distance, labels, metric="precomputed")),
            "bootstrap_ari": float(np.mean(stability)), "pam_agreement_ari": float(adjusted_rand_score(labels, pam)),
            "min_cluster_size": int(min(Counter(labels).values())),
        })
        labels_by_k[k] = labels
    # Prefer stable, interpretable solutions without singleton clusters.
    candidates = [x for x in metrics if x["min_cluster_size"] >= 2]
    chosen = max(candidates or metrics, key=lambda x: x["silhouette"] + x["bootstrap_ari"] + x["pam_agreement_ari"])
    labels = labels_by_k[chosen["k"]]
    out = pd.DataFrame({"oracle_network": matrix.index, "cluster_id": labels})
    names = {}
    for c, members in out.groupby("cluster_id"):
        profile = matrix.loc[members.oracle_network].mean().sort_values(ascending=False)
        top = profile[profile > .49].index[:3].tolist()
        names[int(c)] = " + ".join(x.replace("_", " ") for x in top) or "mixed mechanism family"
    out["mechanism_family"] = out.cluster_id.map(names)
    out["observability_grade"] = out.oracle_network.map(
        pd.read_parquet(MECH / "observability_features.parquet").set_index("oracle_network").historical_completeness
    )
    write_parquet(MECH / "mechanism_clusters.parquet", out)
    write_parquet(MECH / "cluster_stability.parquet", pd.DataFrame(metrics))
    np.save(MECH / "mechanism_jaccard_distance.npy", distance)
    profile = matrix.assign(cluster_id=labels).groupby("cluster_id").mean().reset_index()
    write_parquet(MECH / "cluster_feature_enrichment.parquet", profile)
    result = {
        "sample_size": len(matrix), "chosen_k": int(chosen["k"]),
        "silhouette": chosen["silhouette"], "bootstrap_ari": chosen["bootstrap_ari"],
        "pam_agreement_ari": chosen["pam_agreement_ari"], "cluster_sizes": dict(Counter(map(int, labels))),
        "cluster_names": names,
    }
    atomic_text(APPOUT / "mechanism_summary.json", json.dumps(result, indent=2) + "\n")
    return result


def build_mechanism_network() -> dict[str, Any]:
    design = pd.read_parquet(MECH / "mechanism_design_features.parquet")
    edges = design.query("mechanism_feature_status == 'observed_yes'")[["oracle_network", "component", "feature_family"]].copy()
    degree = edges.groupby("component").size()
    edges["component_degree"] = edges.component.map(degree)
    edges["rare_component"] = edges.component_degree <= 2
    write_parquet(MECH / "mechanism_bipartite_edges.parquet", edges)
    graph = nx.Graph()
    for r in edges.itertuples(index=False):
        graph.add_node(r.oracle_network, node_type="protocol")
        graph.add_node(r.component, node_type="component")
        graph.add_edge(r.oracle_network, r.component)
    communities = list(nx.algorithms.community.greedy_modularity_communities(graph))
    community = {node: i for i, nodes in enumerate(communities) for node in nodes}
    layout = nx.spring_layout(graph, seed=SEED, iterations=200)
    nodes = pd.DataFrame([{
        "node": n, "node_type": graph.nodes[n]["node_type"], "degree": graph.degree(n),
        "community": community[n], "layout_x": float(layout[n][0]), "layout_y": float(layout[n][1]),
        "layout_seed": SEED,
    } for n in graph])
    write_parquet(MECH / "mechanism_bipartite_nodes.parquet", nodes)
    return {"nodes": len(nodes), "edges": len(edges), "rare_components": int(degree.le(2).sum()), "communities": len(communities)}


def analyze_mechanism_outliers() -> dict[str, Any]:
    clusters = pd.read_parquet(MECH / "mechanism_clusters.parquet")
    matrix, _ = _design_matrix()
    distance = np.load(MECH / "mechanism_jaccard_distance.npy")
    rows = []
    for c, group in clusters.groupby("cluster_id"):
        idx = np.array([matrix.index.get_loc(x) for x in group.oracle_network])
        medoid = idx[np.argmin(distance[np.ix_(idx, idx)].sum(1))]
        for i in idx:
            rows.append({
                "oracle_network": matrix.index[i], "cluster_id": int(c),
                "cluster_medoid": matrix.index[medoid], "outlier_score": float(distance[i, medoid]),
                "rare_component_count": int(sum(matrix.iloc[i][x] for x in matrix.columns if matrix[x].sum() <= 2)),
            })
    out = pd.DataFrame(rows).sort_values(["outlier_score", "rare_component_count"], ascending=False)
    write_parquet(MECH / "mechanism_outliers.parquet", out)
    return {"top_outlier": out.iloc[0].oracle_network, "top_outlier_score": float(out.iloc[0].outlier_score)}


def _stage_sql() -> str:
    return """CASE
      WHEN economic_kind='parameter' OR realization_status LIKE 'parameter%' OR realization_status LIKE 'configured%' THEN 'designed_configured'
      WHEN economic_evidence_class LIKE '%eligibility%' OR realization_status='finalized' THEN 'eligible_adjudicated'
      WHEN realization_status LIKE 'accrued%' THEN 'accrued'
      WHEN realization_status LIKE 'claimable%' THEN 'claimable'
      WHEN realization_status IN ('paid','paid_or_wrapped','paid_to_stake','paid_to_stake_custody') THEN 'paid_applied'
      WHEN realization_status IN ('applied','realized') THEN 'paid_applied'
      WHEN realization_status='not_paid' OR economic_evidence_class LIKE '%forfeiture%' THEN 'forfeited'
      ELSE 'unavailable_other' END"""


def build_accountability_conversion() -> dict[str, Any]:
    """Build a count funnel while preserving asset-specific raw integer totals."""
    ensure_dirs()
    con = duckdb.connect()
    sem = pq("economic_semantics_events")
    stage = _stage_sql()
    funnel = qdf(con, f"""
      SELECT CASE WHEN oracle_network='Flare' THEN 'Flare_FTSOv2' ELSE oracle_network END protocol,
             mechanism, coalesce(asset,'unavailable') asset,
             coalesce(asset_decimals,-1) asset_decimals,
             CASE WHEN economic_kind IN ('slash','penalty') THEN 'penalty'
                  WHEN economic_kind='reward' THEN 'reward' ELSE economic_kind END side,
             {stage} stage, economic_evidence_class, realization_status,
             count(*) event_count, count(DISTINCT actor) actor_count,
             CASE WHEN count(*) FILTER (amount_raw IS NOT NULL AND try_cast(amount_raw AS HUGEINT) IS NULL)=0
                  THEN sum(try_cast(amount_raw AS HUGEINT))::VARCHAR END amount_raw_sum,
             bool_and(cashflow_verified OR state_delta_verified) verified_all,
             any_value(source_table) protocol_native_source,
             any_value(do_not_sum_group) do_not_sum_group
      FROM {sem}
      WHERE oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth')
      GROUP BY 1,2,3,4,5,6,7,8
    """)
    funnel["coverage_status"] = np.where(funnel.verified_all, "verified", np.where(
        funnel.stage.isin(["accrued","claimable","designed_configured"]), "observable_noncash_stage", "partial_or_unavailable"
    ))
    write_parquet(ECON / "conversion_funnel.parquet", funnel)

    matrix = funnel.groupby(["protocol","mechanism","side","stage"], as_index=False).event_count.sum()
    write_parquet(ECON / "designed_realized_matrix.parquet", matrix)

    # Only aligned protocol/asset/beneficiary/entitlement definitions may produce
    # amount ratios. The current Flare component entitlement cannot be aligned to
    # aggregate claims, so it remains null instead of becoming a misleading ratio.
    ratio_rows = []
    for (protocol, mechanism, asset), g in funnel.groupby(["protocol","mechanism","asset"], dropna=False):
        claim = g[g.stage == "claimable"]
        paid = g[g.stage == "paid_applied"]
        aligned = False
        ratio_rows.append({
            "protocol": protocol, "mechanism": mechanism, "asset": asset,
            "claimable_event_count": int(claim.event_count.sum()),
            "paid_event_count": int(paid.event_count.sum()),
            "claim_realization_ratio": None,
            "ratio_available": aligned,
            "unavailable_reason": "beneficiary/entitlement definition not aligned at aggregate stage" if len(claim) else "no claimable stage",
        })
    write_parquet(ECON / "claim_realization_metrics.parquet", pd.DataFrame(ratio_rows))
    return {"funnel_rows": len(funnel), "matrix_rows": len(matrix), "aligned_claim_ratios": 0}


def analyze_capital_lock() -> dict[str, Any]:
    con = duckdb.connect()
    cutoff = int(datetime.fromisoformat(CUTOFF.replace("Z", "+00:00")).timestamp())
    capital = qdf(con, f"""
      WITH settle AS (
        SELECT oo_request_id, max(block_time) settlement_time
        FROM {pq('polygon_oov2_events')} WHERE event='Settle' GROUP BY 1
      )
      SELECT 'UMA' protocol, r.oo_request_id accountability_unit,
             r.currency asset, 6 asset_decimals,
             r.effective_bond_raw principal_locked_raw,
             r.principal_returned_raw,
             r.bond_forfeited_raw principal_forfeited_raw,
             try_cast(r.request_time AS BIGINT) lock_start_time,
             s.settlement_time lock_end_time,
             coalesce(s.settlement_time,{cutoff})-try_cast(r.request_time AS BIGINT) capital_lock_duration_seconds,
             s.settlement_time IS NULL right_censored,
             'request_to_settlement; one-side effective bond, not cross-asset summed' measurement_scope
      FROM {pq('polygon_uma_request_rounds')} r LEFT JOIN settle s USING(oo_request_id)
      WHERE try_cast(r.request_time AS BIGINT) <= {cutoff}
    """)
    write_parquet(ECON / "capital_lock.parquet", capital)
    complete = capital[(~capital.right_censored) & capital.principal_locked_raw.notna()].copy()
    complete["token_days_locked_raw"] = [
        str(Decimal(str(amount)) * Decimal(int(seconds)) / Decimal(86400))
        for amount, seconds in zip(complete.principal_locked_raw, complete.capital_lock_duration_seconds)
    ]
    write_parquet(ECON / "capital_lock_complete_token_days.parquet", complete)
    return {
        "rows": len(capital), "right_censored": int(capital.right_censored.sum()),
        "complete_principal_duration": len(complete),
    }


def analyze_accountability_latency() -> dict[str, Any]:
    con = duckdb.connect()
    cutoff = int(datetime.fromisoformat(CUTOFF.replace("Z", "+00:00")).timestamp())
    latency = qdf(con, f"""
      WITH uma_settle AS (
        SELECT oo_request_id,max(block_time) end_time FROM {pq('polygon_oov2_events')}
        WHERE event='Settle' GROUP BY 1
      ), uma AS (
        SELECT 'UMA' protocol, r.oo_request_id::VARCHAR accountability_unit,
          'request_to_settlement' latency_type, try_cast(r.request_time AS BIGINT) start_time,
          s.end_time, s.end_time IS NOT NULL completed
        FROM {pq('polygon_uma_request_rounds')} r LEFT JOIN uma_settle s USING(oo_request_id)
        WHERE try_cast(r.request_time AS BIGINT)<={cutoff}
      ), flare AS (
        SELECT 'Flare_FTSOv2', c.source_tx::VARCHAR,
          'entitlement_to_claim', e.epoch_end_time_unix, c.block_time_unix, true
        FROM {pq('flare_reward_claim_events')} c JOIN {pq('flare_reward_epochs')} e USING(reward_epoch_id)
        WHERE c.block_time_unix<={cutoff}
      )
      SELECT *, coalesce(end_time,{cutoff})-start_time observed_duration_seconds,
        NOT completed right_censored
      FROM (SELECT * FROM uma UNION ALL SELECT * FROM flare)
      WHERE start_time IS NOT NULL AND coalesce(end_time,{cutoff})>=start_time
    """)
    write_parquet(ECON / "accountability_latency.parquet", latency)
    summary = latency.groupby(["protocol","latency_type","right_censored"]).observed_duration_seconds.agg(
        ["count","median",lambda x:x.quantile(.75),lambda x:x.quantile(.90),lambda x:x.quantile(.99)]
    ).reset_index()
    summary.columns = ["protocol","latency_type","right_censored","n","median_seconds","p75_seconds","p90_seconds","p99_seconds"]
    write_parquet(ECON / "latency_summary.parquet", summary)
    return {"rows": len(latency), "right_censored": int(latency.right_censored.sum())}


def _gini(shares: list[Decimal]) -> float:
    if not shares:
        return float("nan")
    x = sorted(shares)
    n = len(x)
    return float(sum(Decimal(2*i-n-1) * v for i, v in enumerate(x, 1)) / (Decimal(n) * sum(x))) if sum(x) else 0.0


def analyze_concentration() -> dict[str, Any]:
    con = duckdb.connect()
    real = pq("realized_reward_slash_events")
    actors = qdf(con, f"""
      SELECT CASE WHEN oracle_network='Flare' THEN 'Flare_FTSOv2' ELSE oracle_network END protocol,
             coalesce(asset,'unavailable') asset, coalesce(asset_decimals,-1) asset_decimals,
             coalesce(mechanism,'unavailable') actor_role, actor,
             sum(try_cast(amount_raw AS HUGEINT))::VARCHAR amount_raw,
             count(*) event_count
      FROM {real}
      WHERE include_in_realized_reward AND actor IS NOT NULL
        AND oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth')
        AND try_cast(amount_raw AS HUGEINT)>0
      GROUP BY 1,2,3,4,5
    """)
    rows, metrics = [], []
    for key, g in actors.groupby(["protocol","asset","asset_decimals","actor_role"], dropna=False):
        total = sum(Decimal(x) for x in g.amount_raw)
        local = []
        for r in g.to_dict("records"):
            share = Decimal(r["amount_raw"]) / total if total else Decimal(0)
            r["share"] = str(share); rows.append(r); local.append(share)
        desc = sorted(local, reverse=True)
        metrics.append({
            "protocol": key[0], "asset": key[1], "asset_decimals": key[2], "actor_role": key[3],
            "reward_recipient_count": len(local), "top1": float(sum(desc[:1])),
            "top5": float(sum(desc[:5])), "top10": float(sum(desc[:10])),
            "hhi": float(sum(x*x for x in local)), "gini": _gini(local),
        })
    detail = pd.DataFrame(rows)
    metric_df = pd.DataFrame(metrics)
    write_parquet(ECON / "reward_concentration.parquet", detail)
    write_parquet(ECON / "reward_concentration_metrics.parquet", metric_df)

    sample_b = pq("sample_b_observable_accountability")
    penalty = qdf(con, f"""
      SELECT oracle_network protocol,
        count(DISTINCT accountability_unit_id) eligible_accountability_units,
        count(DISTINCT accountability_unit_id) FILTER (
          penalty_class IS NOT NULL OR nonmonetary_penalty IS NOT NULL) realized_penalty_units,
        count(DISTINCT actor) FILTER (
          penalty_class IS NOT NULL OR nonmonetary_penalty IS NOT NULL) penalty_subject_count
      FROM {sample_b} GROUP BY 1
    """)
    penalty["count_enforcement_frequency"] = penalty.realized_penalty_units / penalty.eligible_accountability_units
    penalty["amount_enforcement_ratio"] = None
    penalty["amount_ratio_status"] = "unavailable_slashable_principal_denominator"
    penalty.loc[penalty.protocol == "Chainlink", "coverage_interpretation"] = "verified_zero_realized_slash_in_observed_window; forfeiture evidence separate"
    penalty.loc[penalty.protocol == "Pyth", "coverage_interpretation"] = "verified_zero_only_in_retained_durable_state"
    write_parquet(ECON / "penalty_frequency.parquet", penalty)

    signed = qdf(con, f"""
      SELECT evidence_id,
        CASE WHEN oracle_network='Flare' THEN 'Flare_FTSOv2' ELSE oracle_network END protocol,
        asset,asset_decimals,actor,economic_evidence_class,realization_status,
        coalesce(signed_amount_raw,
          CASE WHEN include_in_realized_slash THEN '-'||amount_raw ELSE amount_raw END) signed_amount_raw,
        CASE WHEN include_in_realized_slash THEN 'negative'
             WHEN include_in_realized_reward THEN 'positive' ELSE 'zero_or_accounting' END sign_class,
        source_table,do_not_sum_group
      FROM {real}
      WHERE oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth')
    """)
    write_parquet(ECON / "signed_economic_outcomes.parquet", signed)
    return {"actor_strata": len(detail), "concentration_strata": len(metric_df), "penalty_protocols": len(penalty), "signed_rows": len(signed)}


DOMAIN_RULES = [
    ("politics_election", r"\b(election|president|senate|congress|vote|prime minister|governor)\b"),
    ("sports", r"\b(nfl|nba|mlb|nhl|uefa|fifa|match|tournament|super bowl|world cup|championship)\b"),
    ("weather_climate", r"\b(weather|temperature|hurricane|storm|rainfall|climate|snow)\b"),
    ("legal_regulatory_event", r"\b(court|lawsuit|legal|regulat|sec |approved|ban|indict|convict)\b"),
    ("corporate_event", r"\b(company|ceo|earnings|merger|acquisition|bankrupt|ipo)\b"),
    ("macroeconomic_indicator", r"\b(gdp|inflation|cpi|unemployment|interest rate|fed rate|recession)\b"),
    ("fiat_fx", r"\b(fx|forex|usd|eur|gbp|jpy|currency|exchange rate)\b"),
    ("commodity", r"\b(oil|gold|silver|gas|commodity|wheat|corn)\b"),
    ("equity_rwa", r"\b(stock|share price|equity|bond yield|treasury|real estate)\b"),
    ("insurance", r"\b(insurance|insured|claim event)\b"),
    ("crypto_price", r"\b(bitcoin|btc|ethereum|eth|crypto|token|solana|sol|spotprice|spot price)\b"),
]


def _domain(text: str) -> tuple[str, str]:
    low = text.lower()
    for domain, pattern in DOMAIN_RULES:
        if re.search(pattern, low):
            return domain, pattern
    return "unknown", "no_deterministic_rule_match"


def build_semantic_domain_labels() -> dict[str, Any]:
    """Build a multi-protocol semantic layer without loading >100M rows."""
    con = duckdb.connect()
    gamma = qdf(con, f"""
      SELECT id::VARCHAR source_record_id,'UMA' protocol,
        concat_ws(' ',coalesce(question,''),coalesce(description,''),coalesce(category,''),coalesce(resolutionSource,'')) source_text,
        try_cast(createdAt AS TIMESTAMP) event_time,
        'event_level' coverage_status
      FROM {pq('polymarket_gamma_markets')}
      WHERE coalesce(created_before_cutoff,'true')='true'
    """)
    gamma[["semantic_domain","match_rule"]] = gamma.source_text.apply(lambda x: pd.Series(_domain(str(x))))
    tellor = qdf(con, f"""
      SELECT 'tellor_query_type:'||coalesce(query_type,'unknown') source_record_id,
        'Tellor' protocol, coalesce(query_type,'unknown') source_text,
        to_timestamp(min(timestamp_ms)/1000) event_time,
        'query_type_aggregate' coverage_status, count(*) represented_records
      FROM {pq('tellor_micro_reports')}
      GROUP BY query_type
    """)
    tellor[["semantic_domain","match_rule"]] = tellor.source_text.apply(lambda x: pd.Series(_domain(str(x))))
    meta = pd.DataFrame([
        ("chainlink:eth_usd","Chainlink","ETH / USD price feed","crypto_price","official_feed_metadata","service_window"),
        ("pyth:ois_products","Pyth","Pyth price publisher pools","crypto_price","official_product_scope","registry_layer"),
        ("flare:scaling_feeds","Flare_FTSOv2","FTSOv2 scaling price feeds","crypto_price","official_feed_scope","provider_epoch"),
        ("chronicle:registry","Chronicle","Chronicle data feeds","unknown","registry_scope","registry_layer"),
        ("redstone:registry","RedStone","RedStone data feeds","unknown","registry_scope","registry_layer"),
    ], columns=["source_record_id","protocol","source_text","semantic_domain","match_rule","coverage_status"])
    meta["event_time"] = pd.NaT
    meta["represented_records"] = 1
    gamma["represented_records"] = 1
    labels = pd.concat([gamma, tellor, meta], ignore_index=True, sort=False)
    labels["geographic_scope"] = np.where(labels.semantic_domain.isin(["crypto_price","fiat_fx","commodity","equity_rwa","macroeconomic_indicator"]), "global_or_nonspatial", "unknown_pending_geography")
    write_parquet(GEO / "semantic_domain_labels.parquet", labels)
    write_parquet(GEO / "oracle_reference_entities.parquet", labels[[
        "source_record_id","protocol","source_text","event_time","semantic_domain","geographic_scope","coverage_status","match_rule","represented_records"
    ]])
    summary = labels.groupby(["protocol","semantic_domain","coverage_status"], as_index=False).represented_records.sum()
    write_parquet(GEO / "protocol_domain_coverage.parquet", summary)
    return {"reference_rows": len(labels), "represented_records": int(labels.represented_records.sum()), "protocols": labels.protocol.nunique(), "domains": labels.semantic_domain.nunique()}


def build_geographic_labels() -> dict[str, Any]:
    """Promote existing deterministic evidence into a preliminary joint cube."""
    old = pd.read_parquet(APP / "geography/oracle_geographic_entities.parquet").copy()
    old = old.rename(columns={
        "record_id": "geographic_label_id", "native_record_id": "source_record_id",
        "location_text": "location_surface", "match_method": "match_rule",
    })
    old["protocol"] = old.pop("oracle_network")
    old["confidence"] = old.pop("match_confidence")
    old["ambiguity_reason"] = np.where(old.geographic_scope == "ambiguous", "multiple_country_matches", None)
    old["gazetteer_version"] = "embedded_atlas_gazetteer_v1"
    keep = [
        "geographic_label_id","source_record_id","protocol","source_text","location_surface",
        "location_type","country_code","admin1","city","latitude","longitude","geonames_id",
        "wikidata_id","geographic_scope","match_rule","confidence","manual_review_status",
        "ambiguity_reason","match_evidence","gazetteer_version",
    ]
    labels = old[keep]
    write_parquet(GEO / "geographic_labels.parquet", labels)

    semantic = pd.read_parquet(GEO / "semantic_domain_labels.parquet")
    spatial = labels.merge(
        semantic[["source_record_id","semantic_domain","event_time","represented_records"]],
        on="source_record_id", how="left",
    )
    cube = spatial.groupby(
        ["protocol","semantic_domain","country_code","location_type","confidence"], dropna=False
    ).agg(entity_rows=("geographic_label_id","count"), source_records=("source_record_id","nunique")).reset_index()
    write_parquet(GEO / "country_domain_cube.parquet", cube)

    edges = spatial.query("confidence == 'high' and country_code == country_code").groupby(
        ["protocol","country_code"]
    ).agg(records=("geographic_label_id","count"), source_records=("source_record_id","nunique")).reset_index()
    write_parquet(GEO / "protocol_country_edges.parquet", edges)

    temporal_base = semantic[semantic.event_time.notna()].copy()
    temporal_base["month"] = pd.to_datetime(temporal_base.event_time, utc=True).dt.to_period("M").astype(str)
    temporal = temporal_base.groupby(["month","protocol","semantic_domain"], as_index=False).represented_records.sum()
    first_domain = temporal_base.groupby("semantic_domain", as_index=False).event_time.min().rename(columns={"event_time":"first_appearance"})
    write_parquet(GEO / "temporal_coverage.parquet", temporal)
    write_parquet(GEO / "semantic_first_appearance.parquet", first_domain)

    # Country-level concentration is preliminary because external human
    # validation has not crossed the publication threshold.
    concentration = []
    for protocol, g in edges.groupby("protocol"):
        shares = g.records / g.records.sum()
        concentration.append({
            "protocol": protocol, "countries": len(g), "country_hhi": float((shares**2).sum()),
            "country_entropy": float(-(shares * np.log(shares)).sum()),
            "top_country_share": float(shares.max()),
            "validation_status": "preliminary_pending_external_human_review",
        })
    write_parquet(GEO / "geographic_concentration.parquet", pd.DataFrame(concentration))

    uncertainty = pd.DataFrame([
        {"scope":"high_confidence_spatial","records":int((labels.confidence=="high").sum())},
        {"scope":"ambiguous","records":int((labels.geographic_scope=="ambiguous").sum())},
        {"scope":"unmatched_or_global_nonspatial","records":int(len(semantic)-labels.source_record_id.nunique())},
    ])
    write_parquet(GEO / "geographic_uncertainty.parquet", uncertainty)
    return {
        "geographic_entity_rows": len(labels), "high_confidence_rows": int((labels.confidence=="high").sum()),
        "ambiguous_rows": int((labels.geographic_scope=="ambiguous").sum()),
        "countries": int(labels.loc[labels.confidence=="high","country_code"].nunique()),
        "validated_precision": None, "publication_threshold": .90,
        "status": "preliminary_pending_external_human_review",
    }


def validate_geographic_labels() -> dict[str, Any]:
    labels = pd.read_parquet(GEO / "geographic_labels.parquet")
    semantic = pd.read_parquet(GEO / "semantic_domain_labels.parquet")
    positives = labels.copy()
    positives["stratum"] = positives[["protocol","location_type","confidence"]].astype(str).agg("|".join, axis=1)
    selected = []
    for _, g in positives.groupby("stratum"):
        selected.append(g.sample(min(len(g), max(2, math.ceil(800 * len(g) / len(positives)))), random_state=SEED))
    queue = pd.concat(selected).drop_duplicates("geographic_label_id").head(800)
    if len(queue) < 800:
        rem = positives[~positives.geographic_label_id.isin(queue.geographic_label_id)]
        queue = pd.concat([queue, rem.sample(min(800-len(queue),len(rem)), random_state=SEED)])
    # Add 200 candidate-negative records to permit recall-on-candidate-mentions.
    negative = semantic[~semantic.source_record_id.isin(labels.source_record_id)].sample(
        min(200, len(semantic)-labels.source_record_id.nunique()), random_state=SEED
    ).copy()
    neg = pd.DataFrame({
        "geographic_label_id": "candidate_negative:" + negative.source_record_id.astype(str),
        "source_record_id": negative.source_record_id, "protocol": negative.protocol,
        "source_text": negative.source_text, "location_surface": None, "location_type": "unmatched_candidate",
        "country_code": None, "admin1": None, "city": None, "latitude": None, "longitude": None,
        "geonames_id": None, "wikidata_id": None, "geographic_scope": "unmatched",
        "match_rule": "candidate_negative_sample", "confidence": "unmatched",
        "manual_review_status": "pending", "ambiguity_reason": None,
        "match_evidence": None, "gazetteer_version": "embedded_atlas_gazetteer_v1",
    })
    queue = pd.concat([queue[labels.columns], neg[labels.columns]], ignore_index=True).head(1000)
    queue["gold_has_location_mention"] = None
    queue["gold_match_correct"] = None
    queue["gold_country_code"] = None
    queue["gold_location_type"] = None
    queue["gold_hierarchy_correct"] = None
    queue["reviewer_id"] = None
    queue["review_note"] = None
    write_parquet(GEO / "annotation_review_queue.parquet", queue)
    write_parquet(GEO / "annotation_gold_template.parquet", queue)
    write_csv(APPOUT / "annotation_gold_template_1000.csv", queue.to_dict("records"))
    status = {
        "queue_rows": len(queue), "precision": None, "candidate_recall": None,
        "false_country_rate": None, "hierarchy_error_rate": None,
        "ambiguity_rate": float((labels.geographic_scope=="ambiguous").mean()),
        "unmatched_rate": float((len(semantic)-labels.source_record_id.nunique())/len(semantic)),
        "status": "pending_external_human_review",
    }
    atomic_text(APPOUT / "geographic_validation.json", json.dumps(status, indent=2) + "\n")
    return status


def _savefig(fig: plt.Figure, stem: str, data: pd.DataFrame) -> None:
    fig.tight_layout()
    for ext in ("pdf","png"):
        fig.savefig(APPFIG / f"{stem}.{ext}", dpi=320, bbox_inches="tight")
    plt.close(fig)
    write_csv(APPOUT / f"{stem}.csv", data.to_dict("records"))


def render_mechanism_figures() -> None:
    matrix, _ = _design_matrix()
    clusters = pd.read_parquet(MECH / "mechanism_clusters.parquet").set_index("oracle_network")
    distance = np.load(MECH / "mechanism_jaccard_distance.npy")
    z = linkage(squareform(distance, checks=False), method="average")
    order = leaves_list(z)
    ordered = matrix.iloc[order]
    fig, ax = plt.subplots(figsize=(11, 6))
    sns.heatmap(ordered, cmap="Greys", cbar_kws={"label":"documented component"}, ax=ax)
    ax.set_xlabel("Mechanism component"); ax.set_ylabel("Primary-sample Oracle")
    _savefig(fig, "fig_mechanism_heatmap", ordered.reset_index())

    edges = pd.read_parquet(MECH / "mechanism_bipartite_edges.parquet")
    nodes = pd.read_parquet(MECH / "mechanism_bipartite_nodes.parquet")
    top_components = set(edges.groupby("component").size().nlargest(18).index) | set(edges.query("rare_component").component)
    shown = edges[edges.component.isin(top_components)]
    g = nx.from_pandas_edgelist(shown, "oracle_network", "component")
    pos = {r.node:(r.layout_x,r.layout_y) for r in nodes.itertuples() if r.node in g}
    fig, ax = plt.subplots(figsize=(10, 7))
    pnodes=[n for n in g if n in set(shown.oracle_network)]
    cnodes=[n for n in g if n not in pnodes]
    nx.draw_networkx_edges(g,pos,ax=ax,width=.5,alpha=.35,edge_color="black")
    nx.draw_networkx_nodes(g,pos,nodelist=pnodes,node_shape="s",node_color="white",edgecolors="black",node_size=220,ax=ax)
    nx.draw_networkx_nodes(g,pos,nodelist=cnodes,node_shape="o",node_color="#bdbdbd",edgecolors="black",node_size=90,ax=ax)
    nx.draw_networkx_labels(g,pos,labels={n:n.replace("_"," ") for n in g},font_size=6,ax=ax)
    ax.axis("off"); ax.set_title("Oracle--mechanism component network (primary documented systems)")
    _savefig(fig, "fig_mechanism_bipartite_network", shown)

    coords = MDS(n_components=2, dissimilarity="precomputed", random_state=SEED).fit_transform(distance)
    emb = pd.DataFrame({"oracle_network":matrix.index,"x":coords[:,0],"y":coords[:,1]}).join(clusters[["cluster_id","observability_grade"]],on="oracle_network")
    fig, ax = plt.subplots(figsize=(7,5))
    for grade, marker in [(0,"x"),(1,"o"),(2,"s"),(3,"^"),(4,"D"),(5,"P")]:
        s=emb[emb.observability_grade==grade]
        if len(s): ax.scatter(s.x,s.y,c=s.cluster_id,cmap="Greys",vmin=0,vmax=max(1,emb.cluster_id.max()),marker=marker,s=55,edgecolors="black",label=f"depth {grade}")
    for r in emb.itertuples(): ax.annotate(r.oracle_network,(r.x,r.y),fontsize=6,xytext=(2,2),textcoords="offset points")
    ax.legend(fontsize=6,title="Observability border/marker"); ax.set_title("Mechanism design embedding; color is design cluster")
    _savefig(fig, "fig_mechanism_embedding", emb)

    metrics = pd.read_parquet(MECH / "cluster_stability.parquet")
    fig, ax = plt.subplots(figsize=(6,4))
    ax.plot(metrics.k,metrics.silhouette,"o-",label="silhouette")
    ax.plot(metrics.k,metrics.bootstrap_ari,"s--",label="bootstrap ARI")
    ax.plot(metrics.k,metrics.pam_agreement_ari,"^:",label="PAM agreement ARI")
    ax.set_xlabel("Number of clusters");ax.set_ylim(-.1,1.05);ax.legend()
    _savefig(fig, "fig_mechanism_cluster_stability", metrics)

    obs = pd.read_parquet(MECH / "observability_features.parquet")
    obs_cols=["amount_observable","transaction_observable","state_change_observable","historical_completeness","cross_chain_linkability","payment_observability","penalty_observability"]
    om=obs.set_index("oracle_network")[obs_cols].astype(float)
    om=(om-om.min())/(om.max()-om.min()).replace(0,1)
    fig,ax=plt.subplots(figsize=(8,10));sns.heatmap(om,cmap="Greys",ax=ax,cbar_kws={"label":"normalized observability"})
    ax.set_title("Observability-only map (not mechanism taxonomy)")
    _savefig(fig,"fig_observability_map",om.reset_index())

    out=pd.read_parquet(MECH/"mechanism_outliers.parquet").sort_values("outlier_score")
    fig,ax=plt.subplots(figsize=(7,5));ax.barh(out.oracle_network,out.outlier_score,color="#777");ax.set_xlabel("Distance to cluster medoid");ax.set_title("Mechanism outlier score")
    _savefig(fig,"fig_mechanism_outliers",out)


def _km_curve(durations: np.ndarray, completed: np.ndarray) -> pd.DataFrame:
    order=np.argsort(durations);durations=durations[order];completed=completed[order]
    at_risk=len(durations);survival=1.0;rows=[{"duration_seconds":0,"survival":1.0,"at_risk":at_risk}]
    for t in np.unique(durations):
        mask=durations==t;d=int(completed[mask].sum());c=int(mask.sum()-d)
        if d and at_risk: survival*=1-d/at_risk
        rows.append({"duration_seconds":int(t),"survival":survival,"at_risk":at_risk,"events":d,"censored":c})
        at_risk-=d+c
    return pd.DataFrame(rows)


def _km_curve_grouped(grouped: pd.DataFrame) -> pd.DataFrame:
    grouped=grouped.sort_values("observed_duration_seconds")
    at_risk=int(grouped.n.sum());survival=1.0
    rows=[{"duration_seconds":0,"survival":1.0,"at_risk":at_risk,"events":0,"censored":0}]
    for r in grouped.itertuples(index=False):
        d=int(r.events);c=int(r.n-r.events)
        if d and at_risk:survival*=1-d/at_risk
        rows.append({"duration_seconds":int(r.observed_duration_seconds),"survival":survival,"at_risk":at_risk,"events":d,"censored":c})
        at_risk-=d+c
    return pd.DataFrame(rows)


def render_economic_figures() -> None:
    funnel=pd.read_parquet(ECON/"conversion_funnel.parquet")
    stages=["designed_configured","eligible_adjudicated","accrued","claimable","paid_applied","forfeited"]
    counts=funnel.groupby(["protocol","side","stage"],as_index=False).event_count.sum()
    protocols=["UMA","Chainlink","Flare_FTSOv2","Tellor","Pyth"]
    fig,axes=plt.subplots(1,5,figsize=(13,3.4),sharey=False)
    flow_rows=[]
    for ax,p in zip(axes,protocols):
        g=counts[counts.protocol==p]
        for side,style in [("reward","-"),("penalty","--")]:
            vals=[int(g[(g.side==side)&(g.stage==s)].event_count.sum()) for s in stages]
            denom=max(vals) or 1;norm=[v/denom for v in vals]
            ax.plot(range(len(stages)),norm,style,marker="o",color=PROTOCOL_COLORS[p],label=side)
            ax.fill_between(range(len(stages)),0,norm,color=PROTOCOL_COLORS[p],alpha=.13 if side=="reward" else .06)
            flow_rows.extend({"protocol":p,"side":side,"stage":s,"event_count":v,"within_side_normalized":n} for s,v,n in zip(stages,vals,norm))
        ax.set_title(p,fontsize=8);ax.set_xticks(range(len(stages)),["D","E","A","C","P","F"],fontsize=6);ax.set_ylim(-.03,1.05)
    axes[0].set_ylabel("Within protocol-side normalized count");axes[-1].legend(fontsize=6)
    _savefig(fig,"fig_accountability_sankey",pd.DataFrame(flow_rows))

    matrix=counts.pivot_table(index=["protocol","side"],columns="stage",values="event_count",fill_value=0)
    matrix=matrix.reindex(columns=stages,fill_value=0)
    log=np.log10(matrix+1)
    fig,ax=plt.subplots(figsize=(8,5));sns.heatmap(log,cmap="Greys",annot=matrix.map(lambda x:f"{int(x):,}"),fmt="",ax=ax,cbar_kws={"label":"log10(count+1)"})
    ax.set_title("Designed-to-realized accountability matrix")
    _savefig(fig,"fig_designed_realized_matrix",matrix.reset_index())

    con=duckdb.connect()
    lat=qdf(con,f"""SELECT protocol,observed_duration_seconds,count(*) n,
                    count(*) FILTER(completed) events
                    FROM read_parquet('{ECON/"accountability_latency.parquet"}')
                    GROUP BY 1,2 ORDER BY 1,2""")
    fig,axes=plt.subplots(1,2,figsize=(9,3.8))
    surv_rows=[]
    for ax,(p,g) in zip(axes,lat.groupby("protocol")):
        curve=_km_curve_grouped(g)
        if len(curve)>5000:
            keep=np.unique(np.r_[np.linspace(0,len(curve)-1,5000,dtype=int),len(curve)-1])
            curve=curve.iloc[keep].reset_index(drop=True)
        curve["protocol"]=p;surv_rows.append(curve)
        ax.step(curve.duration_seconds/86400,curve.survival,where="post",color=PROTOCOL_COLORS[p])
        ax.set_xscale("log");ax.set_title(p);ax.set_xlabel("Days (log scale)");ax.set_ylabel("Not yet realized")
    _savefig(fig,"fig_accountability_survival",pd.concat(surv_rows,ignore_index=True))

    reward=pd.read_parquet(ECON/"reward_concentration.parquet")
    fig,ax=plt.subplots(figsize=(7,5));lorenz=[]
    for (p,a,role),g in reward.groupby(["protocol","asset","actor_role"]):
        if len(g)<2: continue
        vals=np.sort(np.array([float(Decimal(x)) for x in g.share]));y=np.r_[0,np.cumsum(vals)];x=np.linspace(0,1,len(y))
        label=f"{p}:{str(a)[:8]}" if len(lorenz)<200000 else p
        ax.plot(x,y,color=PROTOCOL_COLORS.get(p,"#777"),alpha=.45,lw=.8,label=label if p not in [z.get("protocol") for z in lorenz[:1]] else None)
        lorenz.extend({"protocol":p,"asset":a,"actor_role":role,"actor_share":xx,"reward_share":yy} for xx,yy in zip(x,y))
    ax.plot([0,1],[0,1],"k--",lw=.8);ax.set_xlabel("Recipient share");ax.set_ylabel("Reward share");ax.set_title("Reward Lorenz curves by protocol--asset--role")
    _savefig(fig,"fig_reward_lorenz",pd.DataFrame(lorenz))

    signed_path=ECON/"signed_economic_outcomes.parquet"
    plot=qdf(con,f"""
      SELECT * EXCLUDE(rn) FROM (
        SELECT protocol,sign_class,signed_amount_raw,
          row_number() OVER (
            PARTITION BY protocol,sign_class
            ORDER BY hash(evidence_id,{SEED})) rn
        FROM read_parquet('{signed_path}')
        WHERE signed_amount_raw IS NOT NULL
      ) WHERE rn<=50000
    """)
    plot["magnitude_log10"]=plot.signed_amount_raw.apply(lambda x: math.log10(abs(int(x))+1))
    sample=plot
    fig,ax=plt.subplots(figsize=(7,4))
    for (p,sign),g in sample.groupby(["protocol","sign_class"]):
        x=np.sort(g.magnitude_log10.to_numpy());y=np.arange(1,len(x)+1)/len(x)
        ax.plot(x,y,color=PROTOCOL_COLORS.get(p,"#777"),ls="-" if sign=="positive" else "--",alpha=.7,label=f"{p} {sign}")
    ax.set_xlabel("log10(|raw signed amount|+1)");ax.set_ylabel("ECDF");ax.legend(fontsize=5,ncol=2)
    _savefig(fig,"fig_signed_outcomes",sample[["protocol","sign_class","magnitude_log10"]])

    cap=pd.read_parquet(ECON/"capital_lock.parquet")
    c=cap[cap.principal_locked_raw.notna()].copy()
    c["principal_log10"]=c.principal_locked_raw.apply(lambda x:math.log10(abs(int(x))+1))
    if len(c)>50000:c=c.sample(50000,random_state=SEED)
    fig,ax=plt.subplots(figsize=(6,4));ax.scatter(c.capital_lock_duration_seconds/86400,c.principal_log10,s=5,c=np.where(c.right_censored,.75,.25),cmap="Greys",alpha=.35)
    ax.set_xscale("log");ax.set_xlabel("Capital-lock duration, days (log)");ax.set_ylabel("log10(raw principal+1)");ax.set_title("UMA capital lock and settlement latency")
    _savefig(fig,"fig_capital_lock_latency",c[["accountability_unit","capital_lock_duration_seconds","principal_locked_raw","right_censored"]])


def render_geographic_figures() -> None:
    cube=pd.read_parquet(GEO/"country_domain_cube.parquet")
    country=cube.query("confidence == 'high'").groupby("country_code",as_index=False).source_records.sum()
    # Coordinate evidence is shown on a world-frame scatter because this release
    # intentionally does not add an unversioned third-party polygon basemap.
    labels=pd.read_parquet(GEO/"geographic_labels.parquet")
    points=labels.query("confidence == 'high' and latitude == latitude").groupby(["country_code","latitude","longitude"],as_index=False).size()
    fig,ax=plt.subplots(figsize=(9,4.5));ax.set_xlim(-180,180);ax.set_ylim(-60,85);ax.grid(color="#ddd",lw=.4)
    ax.scatter(points.longitude,points.latitude,s=np.sqrt(points["size"])*3,facecolors="white",edgecolors="black")
    for r in points.itertuples():ax.annotate(r.country_code,(r.longitude,r.latitude),fontsize=6)
    ax.set_xlabel("Longitude");ax.set_ylabel("Latitude");ax.set_title("Preliminary high-confidence oracle-referenced geography (not actors)")
    _savefig(fig,"fig_world_oracle_coverage",points)

    domains=pd.read_parquet(GEO/"protocol_domain_coverage.parquet")
    dm=domains.pivot_table(index="protocol",columns="semantic_domain",values="represented_records",aggfunc="sum",fill_value=0)
    shares=dm.div(dm.sum(1),axis=0)
    fig,ax=plt.subplots(figsize=(9,4));sns.heatmap(shares,cmap="Greys",ax=ax,cbar_kws={"label":"within-protocol share"})
    ax.set_title("Protocol × semantic-domain coverage")
    _savefig(fig,"fig_protocol_domain_heatmap",domains)

    edges=pd.read_parquet(GEO/"protocol_country_edges.parquet")
    shown=edges.sort_values("records",ascending=False).head(25)
    g=nx.from_pandas_edgelist(shown,"protocol","country_code",edge_attr="records")
    pos=nx.spring_layout(g,seed=SEED)
    fig,ax=plt.subplots(figsize=(7,5));nx.draw_networkx_edges(g,pos,width=[.5+math.log10(g.edges[e]["records"]+1) for e in g.edges],edge_color="#aaa",ax=ax)
    nx.draw_networkx_nodes(g,pos,nodelist=list(set(shown.protocol)),node_shape="s",node_color="white",edgecolors="black",node_size=400,ax=ax)
    countries=[n for n in g if n not in set(shown.protocol)];nx.draw_networkx_nodes(g,pos,nodelist=countries,node_color="#aaa",node_size=180,ax=ax)
    nx.draw_networkx_labels(g,pos,font_size=7,ax=ax);ax.axis("off");ax.set_title("Top protocol--country edges; preliminary labels")
    _savefig(fig,"fig_protocol_country_network",shown)

    temp=pd.read_parquet(GEO/"temporal_coverage.parquet")
    uma=temp[temp.protocol=="UMA"].copy()
    wide=uma.pivot_table(index="month",columns="semantic_domain",values="represented_records",aggfunc="sum",fill_value=0).sort_index()
    cumulative=(wide>0).cumsum()
    fig,ax=plt.subplots(figsize=(9,4))
    for col in cumulative.columns:ax.plot(cumulative.index,cumulative[col],lw=1,label=col)
    ticks=np.arange(0,len(cumulative),max(1,len(cumulative)//8));ax.set_xticks(ticks,[cumulative.index[i] for i in ticks],rotation=30);ax.set_ylabel("Cumulative active month-domain incidences");ax.legend(fontsize=5,ncol=3)
    ax.set_title("Temporal expansion of semantic coverage (timestamped UMA metadata)")
    _savefig(fig,"fig_geographic_expansion",cumulative.reset_index())

    conc=pd.read_parquet(GEO/"geographic_concentration.parquet")
    fig,axes=plt.subplots(1,2,figsize=(7,3.5))
    axes[0].bar(conc.protocol,conc.country_hhi,color="#777");axes[0].set_title("Country HHI")
    axes[1].bar(conc.protocol,conc.country_entropy,color="#aaa",edgecolor="black");axes[1].set_title("Country entropy")
    _savefig(fig,"fig_geographic_entropy",conc)

    unc=pd.read_parquet(GEO/"geographic_uncertainty.parquet")
    fig,ax=plt.subplots(figsize=(6,3.5));total=unc.records.sum();left=0
    for i,r in unc.iterrows():
        ax.barh(["reference scope"],[r.records/total],left=left,color=["#333","#999","#eee"][i],edgecolor="black",label=r.scope);left+=r.records/total
    ax.set_xlim(0,1);ax.set_xlabel("Share");ax.legend(fontsize=6);ax.set_title("Geographic coverage uncertainty")
    _savefig(fig,"fig_geographic_uncertainty",unc)


def _latex_table(path: Path, caption: str, label: str, frame: pd.DataFrame) -> None:
    headers=" & ".join(latex_escape(x) for x in frame.columns)
    rows="\n".join(" & ".join(latex_escape(x) for x in row)+" \\\\" for row in frame.astype(str).values.tolist())
    text=(f"\\begin{{table*}}[t]\\centering\\small\\caption{{{caption}}}\\label{{{label}}}"
          f"\\resizebox{{\\textwidth}}{{!}}{{\\begin{{tabular}}{{{'l'*len(frame.columns)}}}"
          f"\\toprule {headers} \\\\\\midrule\n{rows}\n\\bottomrule\\end{{tabular}}}}\\end{{table*}}\n")
    atomic_text(path,text)


def render_application_tables() -> None:
    stability=pd.read_parquet(MECH/"cluster_stability.parquet")
    _latex_table(TAB/"table_mechanism_stability.tex","Mechanism clustering diagnostics; observability is excluded from the distance.","tab:mech-stability",stability.round(3))
    penalty=pd.read_parquet(ECON/"penalty_frequency.parquet")[["protocol","eligible_accountability_units","realized_penalty_units","count_enforcement_frequency","amount_ratio_status"]]
    penalty["count_enforcement_frequency"]=penalty.count_enforcement_frequency.map(lambda x:f"{x:.6f}")
    _latex_table(TAB/"table_enforcement_gap.tex","Count-based enforcement and unavailable amount denominators.","tab:enforcement-gap",penalty)
    domain=pd.read_parquet(GEO/"protocol_domain_coverage.parquet")
    domain=domain.sort_values("represented_records",ascending=False).groupby("protocol").head(3)
    _latex_table(TAB/"table_semantic_coverage.tex","Largest deterministic semantic domains by protocol and coverage layer.","tab:semantic",domain)
    validation=json.loads((APPOUT/"geographic_validation.json").read_text())
    vf=pd.DataFrame([validation])
    _latex_table(TAB/"table_geographic_validation_rebuilt.tex","Preliminary geographic-label validation status.","tab:geo-rebuilt",vf)


def _figure_tex(stem: str, caption: str, label: str) -> str:
    return (f"\\begin{{figure*}}[t]\\centering\n"
            f"\\includegraphics[width=0.94\\textwidth]{{figures/applications/{stem}.pdf}}\n"
            f"\\caption{{{caption}}}\\label{{{label}}}\\end{{figure*}}\n")


def build_reports(
    release:dict[str,int], mechanism:dict[str,Any], network:dict[str,Any], outliers:dict[str,Any],
    conversion:dict[str,Any], capital:dict[str,Any], latency:dict[str,Any], concentration:dict[str,Any],
    semantic:dict[str,Any], geography:dict[str,Any], geo_validation:dict[str,Any],
) -> None:
    penalty=pd.read_parquet(ECON/"penalty_frequency.parquet")
    lat_sum=pd.read_parquet(ECON/"latency_summary.parquet")
    conc=pd.read_parquet(ECON/"reward_concentration_metrics.parquet")
    stages=pd.read_parquet(ECON/"designed_realized_matrix.parquet").groupby(["protocol","stage"],as_index=False).event_count.sum()
    domains=pd.read_parquet(GEO/"protocol_domain_coverage.parquet")
    mech_names="; ".join(f"{k}: {v}" for k,v in mechanism["cluster_names"].items())
    high_top=conc.query("reward_recipient_count >= 10").sort_values("top10",ascending=False).head(1).iloc[0]
    lat_text="; ".join(
        f"{r.protocol} {r.latency_type}: median {r.median_seconds/86400:.2f} days, p90 {r.p90_seconds/86400:.2f} days"
        for r in lat_sum.query("right_censored == False").itertuples()
    )
    penalty_text="; ".join(
        f"{r.protocol} {r.realized_penalty_units:,}/{r.eligible_accountability_units:,} units ({r.count_enforcement_frequency:.4%})"
        for r in penalty.itertuples()
    )
    report=f"""# Applications of Our Dataset

## 1. Application Design Audit

The redesign audit is frozen in `reports/applications_redesign_audit.md`. It
recomputed {release['registry']} Registry entries, {release['accountability']:,}
unified accountability rows, {release['sample_b']:,} Sample-B rows,
{release['sample_c']:,} Sample-C rows, and {release['manifest_tables']} curated
tables with {release['manifest_rows']:,} rows. It removes the old 51/56
low-observability “mechanism cluster,” the count-only claim bar, and the
UMA-only map as an ecosystem result.

Evidence: `registry/oracle_observability_scores.jsonl`,
`accountability_events.parquet`, Samples B/C, and
`data/manifests/curated_parquet.json`; fields are row IDs, protocol, actor,
accountability unit, economic semantics, observability, and native source.
Script: `audit_current_applications.py`. Filter: fixed cutoff
`{CUTOFF}`. Output: `reports/applications_redesign_audit.md`.

## 2. Mechanism-Space Construction

Mechanism design and observability are now independent tables. Design uses
delivery, source, aggregation, accountable subject, reward, penalty, truth
basis, and temporal-unit components. Every cell is status-aware; Registry-only
rows stay `unknown` and are never converted to false. A protocol enters the
primary space only when unknown core-design share is at most 40%. This leaves
{mechanism['sample_size']} of 56 systems in the primary analysis and retains the
other {56-mechanism['sample_size']} in the observability map.

Evidence: Registry fields and frozen official-documentation catalog;
denominator: {len(COMPONENTS)} design components per system. Script:
`build_mechanism_space.py`. Outputs:
`mechanism_design_features.parquet`, `observability_features.parquet`, and
`primary_clustering_sample.parquet`.

## 3. Mechanism Clustering and Outliers

Jaccard/Gower-equivalent binary design distance, average-linkage hierarchy, and
PAM robustness select {mechanism['chosen_k']} families. The chosen silhouette is
{mechanism['silhouette']:.3f}, subsample bootstrap ARI is
{mechanism['bootstrap_ari']:.3f}, and hierarchy--PAM agreement is
{mechanism['pam_agreement_ari']:.3f}. Family profiles are `{mech_names}`.
The network contains {network['edges']} documented protocol--component edges,
{network['rare_components']} components of degree at most two, and
{network['communities']} graph communities. The largest medoid-distance
boundary case is {outliers['top_outlier']} ({outliers['top_outlier_score']:.3f}).
These are design-space diagnostics, not rankings.

Evidence: only `observed_yes/observed_no` primary cells; denominator is the
eligible protocol set, with market size and observability excluded. Scripts:
`cluster_mechanisms.py`, `build_mechanism_network.py`,
`analyze_mechanism_outliers.py`. Outputs: cluster, stability, network-node,
network-edge, enrichment, and outlier Parquet files.

## 4. Accountability Conversion

The conversion model separates designed/configured, eligible/adjudicated,
accrued, claimable, paid/applied, forfeited, and unavailable stages by
protocol, mechanism, asset, evidence class, and realization status. It produces
{conversion['funnel_rows']:,} evidence strata. No aggregate claim-realization
amount ratio is published because the current claimable and paid rows cannot be
aligned on beneficiary and entitlement definition; the explicit null is the
result, not a failure to calculate. Principal return is never reward, and
configured slash is never applied loss.

Evidence: `economic_semantics_events.parquet`; fields include protocol,
mechanism, asset/decimals, actor, amount, realization status, verification,
native source, and `do_not_sum_group`. Script:
`build_accountability_conversion.py`. Denominator: event counts within each
protocol--mechanism--asset stage. Outputs: `conversion_funnel.parquet`,
`designed_realized_matrix.parquet`, and `claim_realization_metrics.parquet`.

## 5. Capital Lock and Latency

UMA contributes {capital['rows']:,} request-level capital-lock observations,
including {capital['right_censored']:,} right-censored requests and
{capital['complete_principal_duration']:,} rows with both principal and
duration. Token-days are computed in raw units inside the request asset only.
The combined latency table has {latency['rows']:,} observations and
{latency['right_censored']:,} censored rows. Completed distributions are:
{lat_text}. Kaplan--Meier curves retain incomplete units.

Evidence: Polygon UMA request rounds/OOV2 settlements and Flare epoch/claim
events; fields are start/end time, completion, raw principal, returned and
forfeited principal. Scripts: `analyze_capital_lock.py` and
`analyze_accountability_latency.py`. Denominator: lifecycle units within each
protocol and latency type. Outputs: capital, token-days, latency, and summary
Parquet files.

## 6. Reward and Penalty Concentration

Reward concentration is calculated for {concentration['actor_strata']:,}
protocol--asset--role--actor strata and {concentration['concentration_strata']:,}
separate concentration groups. The highest observed Top-10 share is
{high_top.top10:.3f} in {high_top.protocol}/{high_top.asset}/{high_top.actor_role};
this does not compare token magnitudes. Count enforcement is: {penalty_text}.
Amount enforcement remains unavailable without reconstructable slashable
principal. Chainlink's zero is restricted to its observed service window,
Pyth's to retained durable state, and Tellor to the observed dispute panel.

Evidence: transaction-gated realized events and Sample B; fields are asset,
decimals, actor, mechanism/role, raw amount, penalty class, and accountability
unit. Script: `analyze_concentration.py`. Denominators are rewards within each
protocol--asset--role group and eligible accountability units within protocol.
Outputs: reward detail/metrics, penalty frequency, and signed outcomes.

## 7. Semantic-Domain Coverage

Deterministic rules label {semantic['reference_rows']:,} source/aggregate
records representing {semantic['represented_records']:,} observations across
{semantic['protocols']} protocols and {semantic['domains']} observed domains.
UMA contributes event text, Tellor contributes query-type aggregates rather
than loading 82 million reports into memory, and Chainlink, Pyth, Flare,
Chronicle, and RedStone contribute explicit feed/product/Registry scope.
Protocol-by-domain shares therefore expose coverage layer as well as domain.

Evidence: Gamma question/description/category/resolution source, Tellor query
type, and frozen structured protocol metadata. Script:
`build_semantic_domain_labels.py`. Denominator: represented records within each
protocol; Registry-layer entries are marked and not treated as event panels.
Outputs: reference entities, semantic labels, and protocol-domain coverage.

## 8. Geographic Coverage

The preliminary extension contains {geography['geographic_entity_rows']:,}
location entities, {geography['high_confidence_rows']:,} high-confidence rows,
{geography['ambiguous_rows']:,} ambiguous rows, and {geography['countries']}
countries. Only deterministic high-confidence coordinates enter the map;
global/nonspatial and unmatched references remain outside it. The joint
country-domain cube and protocol-country edge table preserve semantic domain,
confidence, and coverage layer. No wallet, node, publisher, or operator
location is inferred.

Evidence: retained source text and gazetteer match evidence joined to semantic
labels. Script: `build_geographic_labels.py`. Denominator: source reference
records, not actors. Outputs: geographic labels, cube, edges, temporal
coverage, concentration, and uncertainty.

## 9. Validation and Uncertainty

The review package contains {geo_validation['queue_rows']} records stratified
over protocol, domain/scope, location type, confidence, rule, and candidate
negatives. Precision, recall, false-country rate, and hierarchy error remain
null until independent humans fill the gold template. The automatic ambiguity
rate is {geo_validation['ambiguity_rate']:.3%}; unmatched/global coverage is
reported separately. Geography is preliminary and cannot become a main
population estimate until external precision reaches 0.90.

## 10. Safe Interpretation

All results are descriptive reuse demonstrations. Missing is not zero,
claimable is not paid, payout is not reward, observability is not design, and
raw assets are not added across protocols. Sparse enforcement can represent a
verified zero, partial coverage, or unavailable evidence; it does not prove
deterrence or safety.

## 11. Figures Selected for the Main Paper

- A1 mechanism heatmap and A2 bipartite network.
- B1 accountability conversion and B3 survival curves.
- C2 protocol-domain heatmap and C6 geographic uncertainty.

## 12. Supplementary Figures

Mechanism embedding/stability/observability/outliers; designed-realized matrix,
Lorenz, signed-outcome ECDF, capital-lock joint plot; preliminary world map,
protocol-country network, temporal expansion, and geographic entropy.
"""
    atomic_text(ROOT/"reports/applications_of_dataset_rebuilt.md",report)

    # Three 450--650-word subsections; common interpretation limits appear once.
    tex=f"""\\section{{Applications of Our Dataset}}
These applications demonstrate reuse of the fixed-cutoff Atlas rather than estimate causal effects or rank protocols. Mechanism design is separated from evidence observability, economic quantities are compared only inside aligned protocol--asset definitions, and geographic labels never describe actors or infrastructure. Missing, unavailable, and verified zero are distinct throughout.

\\subsection{{Oracle Mechanism Space and Outlier Discovery}}
Can heterogeneous Oracle systems be represented in a common accountability-mechanism space without allowing missingness or market size to dominate the taxonomy? We answer this by replacing the earlier observability-driven clustering with two independent matrices. The mechanism-design matrix records delivery mode, data source, aggregation rule, accountable subject, reward, penalty, truth basis, and temporal unit. Every component carries one of five states: observed yes, observed no, not applicable, unknown, or structurally unobservable. Negative entries are asserted only for systems covered by the frozen documentation catalog; Registry-only entries remain unknown. Systems with more than 40 percent unknown core components are excluded from primary clustering but remain visible in the Registry and observability map. This produces a primary sample of {mechanism['sample_size']} of 56 systems and prevents the other {56-mechanism['sample_size']} from becoming a spurious mechanism family.

We cluster the primary binary design matrix with Jaccard distance and average-linkage hierarchy, and compare it with PAM medoids. Candidate solutions are assessed by silhouette, 50-fold subsample stability, minimum cluster size, and agreement with PAM. The selected {mechanism['chosen_k']}-family solution has silhouette {mechanism['silhouette']:.3f}, bootstrap adjusted-Rand stability {mechanism['bootstrap_ari']:.3f}, and hierarchy--PAM agreement {mechanism['pam_agreement_ari']:.3f}. Figure~\\ref{{fig:mechanism-heatmap}} shows the clustered component matrix; unknown Registry systems are absent from this primary heatmap rather than coded as zeros. The families describe recurring component bundles such as request/dispute voting, epoch-scored publisher systems, service-window staking, and deterministic on-chain sources. They are design descriptions, not security grades.

The complementary bipartite graph in Figure~\\ref{{fig:mechanism-network}} contains {network['edges']} documented protocol--component edges. Degree analysis identifies {network['rare_components']} components occurring in at most two catalogued systems, while greedy modularity yields {network['communities']} graph communities. Medoid distance provides an outlier score; {outliers['top_outlier']} is the largest observed boundary case at {outliers['top_outlier_score']:.3f}. Rare components and outliers reveal where otherwise similar systems change accountable subject, truth basis, or penalty form. A separate observability-only map records payment, penalty, transaction, state-change, history, version, and cross-chain evidence. Thus a poorly documented system can be an observability outlier without being assigned a mechanism identity.

The component matrix also makes co-occurrence directly testable. Researchers can ask whether principal slashing usually accompanies staking, whether optimistic acceptance co-occurs with bond forfeiture, or whether epoch scoring substitutes for report-level adjudication. Because cluster membership is fitted without TVL, integration count, or evidence-depth scores, commercially large systems receive no mechanical advantage in defining a family. Sensitivity outputs expose how assignments change with $k$, while the PAM comparison tests whether the hierarchy is being driven by a few pairwise distances. The moderate silhouette should be read as overlapping design bundles, not as evidence for perfectly separated natural kinds.

The released status matrix, missingness matrix, distance matrix, medoids, hierarchical assignments, stability diagnostics, network nodes and seeded layout permit alternative thresholds or taxonomies. Actor clustering is retained only as a protocol-internal supplement for UMA and Chainlink; it is not used to define ecosystem families and its labels do not describe identity, skill, or honesty.

{_figure_tex('fig_mechanism_heatmap','Documented mechanism components for the completeness-gated primary sample. Unknown Registry entries are retained outside the clustering rather than encoded as component absence.','fig:mechanism-heatmap')}
{_figure_tex('fig_mechanism_bipartite_network','Protocol--component network. Squares are Oracle systems and circles are mechanism components; node size does not encode market size.','fig:mechanism-network')}

\\subsection{{Accountability Conversion and Financial Frictions}}
How do designed incentives and penalties become accrued, claimable, paid, forfeited, or applied economic outcomes? We construct a conversion object keyed by protocol, mechanism, asset, evidence class, realization status, and native source. Reward stages run from designed/configured through eligible, accrued, claimable, and paid/applied; the penalty side runs from configured through trigger eligibility, adjudication, application, and transfer, burn, or redistribution. The rebuilt table contains {conversion['funnel_rows']:,} stage-specific evidence strata. Figure~\\ref{{fig:conversion}} normalizes event counts separately inside each protocol and economic side, preventing Flare's claim volume or Tellor's report history from visually overwhelming smaller mechanisms. Principal deposits and returns are excluded from reward flows.

The first finding is an evidence-conversion gap rather than a universal payment ratio. Designed parameters, accounting accruals, entitlements, cash payments, forfeitures, and applied stake changes are observable at different resolutions. No aggregate claim-realization amount ratio is released because the current claimable and paid records cannot always be matched on protocol, asset, beneficiary, entitlement definition, and time scope. In particular, Flare's provider-component conditions cannot be imputed from aggregate RewardClaimed transfers. Chainlink configuration is kept separate from its observation-window verified-zero realized slash, and Pyth's zero is restricted to retained durable state. Null ratios therefore locate an identification break instead of silently becoming zero.

Capital and latency evidence closes more completely for selected lifecycles. UMA contributes {capital['rows']:,} request-level lock observations, of which {capital['right_censored']:,} are right-censored and {capital['complete_principal_duration']:,} contain both raw principal and duration. Token-days remain asset-specific. Combined UMA settlement and Flare entitlement-to-claim data contain {latency['rows']:,} observations with {latency['right_censored']:,} censored units. Figure~\\ref{{fig:survival}} uses Kaplan--Meier curves rather than deleting incomplete requests; logarithmic time reveals the long tail without forcing both protocols onto a linear scale.

The conversion figure is deliberately an evidence-flow profile rather than a claim that every stage contains the same cohort. Event counts are normalized separately on the reward and penalty sides, and the underlying table retains native source and coverage status. This reveals where a protocol exposes only configuration, where accounting accrual is visible without a cash transfer, and where a verified payment or applied balance change closes the lifecycle. Signed-outcome ECDFs preserve positive redistribution and negative forfeiture/slash separately; an ordinary raw-amount histogram would be dominated by token decimals and extreme values. Capital-lock joint plots are restricted to units with both principal and duration evidence.

Concentration is also defined within protocol--asset--role groups. The release contains {concentration['actor_strata']:,} actor strata and {concentration['concentration_strata']:,} concentration groups with Top-1/5/10, HHI, Gini, and Lorenz inputs. The largest observed Top-10 share is {high_top.top10:.3f} for {high_top.protocol}/{high_top.asset}/{high_top.actor_role}. Count-based enforcement uses eligible accountability units, whereas amount-based enforcement remains unavailable when slashable principal cannot be reconstructed. These outputs support studies of payment friction, capital exposure, tail latency, and economic concentration, but do not show that rewards cause participation or penalties cause accuracy.

{_figure_tex('fig_accountability_sankey','Within-protocol conversion profiles. Counts are normalized separately by protocol and reward/penalty side; principal movements are excluded from reward.','fig:conversion')}
{_figure_tex('fig_accountability_survival','Kaplan--Meier curves for UMA request settlement and Flare entitlement-to-claim time. Unfinished UMA requests are right-censored.','fig:survival')}

\\subsection{{Geographic and Semantic Coverage of Oracle-Referenced Reality}}
What parts of reality are represented by Oracle-mediated on-chain information, across which semantic domains and protocols, and where are the observable gaps? Because the previous UMA-only map could not answer that question, the rebuilt application makes semantic coverage primary and geography a validation-gated extension. A four-dimensional cube links protocol, deterministic semantic domain, geographic scope, and time. Domains include crypto price, fiat/FX, commodities, equity/RWA, macroeconomic indicators, politics/elections, sports, weather/climate, insurance, corporate events, legal/regulatory events, global/nonspatial, and unknown.

The source layer now spans multiple protocols at their defensible resolution. UMA contributes market question, description, category, and resolution-source text. Tellor contributes query-type aggregates, avoiding an in-memory scan of more than 82 million report rows. Chainlink contributes explicit ETH/USD feed metadata; Pyth and Flare contribute documented price-product/feed scope; Chronicle and RedStone enter only at the Registry metadata layer. The resulting {semantic['reference_rows']:,} source or aggregate rows represent {semantic['represented_records']:,} observations across {semantic['protocols']} protocols and {semantic['domains']} observed domains. Figure~\\ref{{fig:domains}} reports within-protocol shares and labels the coverage layer, so a Registry entry cannot be mistaken for an event panel.

Geography is derived only from explicit source text and a versioned deterministic gazetteer. Global assets are not assigned to issuer or exchange countries, and ambiguous multi-country text receives no coordinates. The preliminary extension contains {geography['geographic_entity_rows']:,} entities, including {geography['high_confidence_rows']:,} high-confidence matches, {geography['ambiguous_rows']:,} ambiguous matches, and {geography['countries']} countries. Country--domain combinations, protocol--country edges, entropy, HHI, first appearance, and monthly semantic expansion are released, but current spatial coverage remains concentrated in UMA because equivalent structured geographic metadata are absent elsewhere.

The cube separates breadth from volume. A protocol may cover several domains through a small metadata Registry while another contributes millions of observations to one query type; both the represented-record count and coverage layer remain available. Cross-protocol Jaccard comparisons can therefore be computed on domain presence without mistaking report repetition for greater breadth. Timestamped UMA metadata support monthly first-appearance and cumulative-domain analysis, whereas undated Registry scope is excluded from temporal expansion. Similarly, country entropy and HHI use only published high-confidence spatial labels and are accompanied by the uncertainty denominator rather than treating unmatched references as if they had no geographic meaning.

Figure~\\ref{{fig:geo-uncertainty}} therefore emphasizes uncertainty rather than presenting a polished country map as a population result. The review package expands to {geo_validation['queue_rows']} records stratified by protocol, semantic/scope class, location type, confidence, rule, and candidate negatives. Precision, candidate-mention recall, false-country rate, and hierarchy error remain null until independent reviewers complete the gold template. Main geographic inference is disabled until precision reaches 0.90; Codex output is not ground truth. The semantic cube is ready for cross-protocol coverage and overlap studies, while country concentration and temporal geography remain preliminary extensions. No wallet, node, publisher, or operator location is inferred.

{_figure_tex('fig_protocol_domain_heatmap','Within-protocol semantic-domain shares. Registry-only and aggregate coverage remain distinguished from event-level records.','fig:domains')}
{_figure_tex('fig_geographic_uncertainty','Shares of high-confidence spatial, ambiguous, and unmatched/global/nonspatial reference coverage. Geography remains preliminary pending independent review.','fig:geo-uncertainty')}
"""
    atomic_text(ROOT/"paper/sections/applications_of_dataset.tex",tex)


def rebuild_validation(release:dict[str,int], summaries:dict[str,Any]) -> None:
    bases=[MECH,ECON,GEO,APPFIG,APPOUT,TAB,ROOT/"reports",ROOT/"paper/sections",ROOT/"scripts/applications",ROOT/"paper/build"]
    files=[]
    for base in bases:
        if not base.exists():continue
        for p in sorted(base.rglob("*")):
            if p.is_file() and p.name not in {"applications_rebuild_manifest.json","applications_rebuild_validation.md"}:
                if ("application" in p.name or base in (MECH,ECON,GEO,APPFIG,APPOUT,TAB) or p.suffix in {".py",".tex",".pdf",".png",".csv",".parquet",".npy"}):
                    files.append({"path":str(p),"sha256":sha256(p),"bytes":p.stat().st_size})
    manifest={
        "version":"2.0.0","generated_at_utc":datetime.now(UTC).isoformat(),"cutoff":CUTOFF,
        "seed":SEED,"release":release,"release_manifest_sha256":sha256(MANIFESTS/"oracle_dataset_release.json"),
        "summaries":summaries,
        "inputs":[
            "registry/oracle_observability_scores.jsonl","accountability_events.parquet",
            "sample_b_observable_accountability.parquet","sample_c_strict_honesty_events.parquet",
            "economic_semantics_events.parquet","realized_reward_slash_events.parquet",
            "polygon_uma_request_rounds.parquet","polygon_oov2_events.parquet",
            "flare_reward_claim_events.parquet","flare_reward_epochs.parquet",
            "polymarket_gamma_markets.parquet","tellor_micro_reports.parquet",
        ],
        "unresolved":[
            "Geographic precision/recall/false-country/hierarchy metrics await independent completion of the 1,000-row gold template.",
            "Geography remains preliminary and spatially concentrated in UMA source text.",
            "Claim realization amount ratios remain null where beneficiary and entitlement definitions cannot be aligned.",
            "Amount enforcement ratios remain null where observable slashable principal is unavailable.",
            "Mechanism catalog is documentation-bounded; Registry-only systems are retained as unknown and excluded above 40% unknown share.",
        ],
        "outputs":files,
    }
    atomic_text(APPOUT/"applications_rebuild_manifest.json",json.dumps(manifest,indent=2,default=str)+"\n")
    rows="\n".join(f"| `{x['path']}` | `{x['sha256']}` | {x['bytes']:,} |" for x in files)
    m=summaries["mechanism"];g=summaries["geography"];v=summaries["geographic_validation"]
    text=f"""# Applications Rebuild Validation

- Release version: `2.0.0`.
- Release manifest SHA-256: `{manifest['release_manifest_sha256']}`.
- Fixed cutoff: `{CUTOFF}`.
- Inputs: `{json.dumps(manifest['inputs'])}`.
- Recomputed release: `{json.dumps(release,sort_keys=True)}`.
- New feature tables: mechanism design/status, observability, missingness,
  conversion stages, capital lock, latency, concentration, signed outcomes,
  semantic labels, geographic labels, country-domain cube, network nodes/edges,
  temporal coverage, and annotation templates.
- Primary mechanism sample: **{m['sample_size']} / 56**; threshold: unknown core
  share at most 40%.
- Mechanism stability: k={m['chosen_k']}, silhouette={m['silhouette']:.4f},
  bootstrap ARI={m['bootstrap_ari']:.4f}, PAM agreement={m['pam_agreement_ari']:.4f}.
- Conversion metrics: `{json.dumps(summaries['conversion'],sort_keys=True)}`.
- Capital lock/right censoring: `{json.dumps(summaries['capital'],sort_keys=True)}`.
- Latency/right censoring: `{json.dumps(summaries['latency'],sort_keys=True)}`.
- Concentration: `{json.dumps(summaries['concentration'],sort_keys=True)}`.
- Semantic labels: `{json.dumps(summaries['semantic'],sort_keys=True)}`.
- Geographic labels: `{json.dumps(g,sort_keys=True)}`.
- Geographic validation precision: **unavailable/pending external review**;
  publication threshold 0.90; queue rows={v['queue_rows']}.

## Main-paper number provenance

| Number | Script | Output |
|---|---|---|
| Registry/release row counts | `audit_current_applications.py` | `reports/applications_redesign_audit.md` |
| Primary sample and missingness | `build_mechanism_space.py` | `primary_clustering_sample.parquet` |
| k, silhouette, stability, PAM agreement | `cluster_mechanisms.py` | `cluster_stability.parquet` |
| Network edges, rare components, communities | `build_mechanism_network.py` | `mechanism_bipartite_nodes/edges.parquet` |
| Conversion strata and gaps | `build_accountability_conversion.py` | `conversion_funnel.parquet` |
| Capital lock and censoring | `analyze_capital_lock.py` | `capital_lock.parquet` |
| Latency and censoring | `analyze_accountability_latency.py` | `accountability_latency.parquet` |
| Concentration and enforcement frequency | `analyze_concentration.py` | `reward_concentration_metrics.parquet`, `penalty_frequency.parquet` |
| Protocol/domain counts | `build_semantic_domain_labels.py` | `protocol_domain_coverage.parquet` |
| Geographic entities/uncertainty | `build_geographic_labels.py` | `geographic_labels.parquet`, `geographic_uncertainty.parquet` |

## Unresolved issues

{chr(10).join('- '+x for x in manifest['unresolved'])}

## Output checksums

| File | SHA-256 | Bytes |
|---|---|---:|
{rows}
"""
    atomic_text(ROOT/"reports/applications_rebuild_validation.md",text)


def run_all() -> dict[str,Any]:
    ensure_dirs()
    con=duckdb.connect();release=release_checks(con)
    mech_base=build_mechanism_space()
    mechanism=cluster_mechanisms();mechanism.update(mech_base)
    network=build_mechanism_network();outliers=analyze_mechanism_outliers()
    conversion=build_accountability_conversion();capital=analyze_capital_lock()
    latency=analyze_accountability_latency();concentration=analyze_concentration()
    semantic=build_semantic_domain_labels();geography=build_geographic_labels()
    geo_validation=validate_geographic_labels()
    render_mechanism_figures();render_economic_figures();render_geographic_figures()
    render_application_tables()
    build_reports(release,mechanism,network,outliers,conversion,capital,latency,concentration,semantic,geography,geo_validation)
    summaries={"mechanism":mechanism,"network":network,"outliers":outliers,"conversion":conversion,"capital":capital,"latency":latency,"concentration":concentration,"semantic":semantic,"geography":geography,"geographic_validation":geo_validation}
    rebuild_validation(release,summaries)
    return summaries


if __name__=="__main__":
    print(json.dumps(run_all(),indent=2,default=str))
