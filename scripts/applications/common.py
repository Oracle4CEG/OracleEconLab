"""Shared implementation for the three Atlas application demonstrations."""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Iterable

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import dendrogram, linkage
from scipy.spatial.distance import squareform
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.manifold import MDS
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import RobustScaler

ROOT = Path(__file__).resolve().parents[2]
PARQUET = (ROOT / "data/curated/parquet").resolve()
APP = ROOT / "data/applications"
OUT = ROOT / "analysis_outputs/applications"
FIG = ROOT / "figures"
TAB = ROOT / "tables"
MANIFESTS = ROOT / "data/manifests"
CUTOFF = "2026-06-30T23:59:59Z"
SEED = 20260729
PROTOCOLS = ("UMA", "Chainlink", "Flare_FTSOv2", "Tellor", "Pyth")
getcontext().prec = 80


def setup() -> None:
    for path in (APP, OUT, FIG, TAB, ROOT / "reports", ROOT / "paper/sections"):
        path.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else [])
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(path)


def pq(name: str) -> str:
    path = PARQUET / f"{name}.parquet"
    if not path.is_file():
        raise RuntimeError(f"missing input {path}")
    return f"read_parquet('{path}')"


def qdf(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetch_df()


def release_checks(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    manifest = json.loads((MANIFESTS / "curated_parquet.json").read_text())
    actual = {
        "registry": sum(1 for _ in (ROOT / "registry/oracle_observability_scores.jsonl").open()),
        "accountability": con.execute(f"SELECT count(*) FROM {pq('accountability_events')}").fetchone()[0],
        "sample_b": con.execute(f"SELECT count(*) FROM {pq('sample_b_observable_accountability')}").fetchone()[0],
        "sample_c": con.execute(f"SELECT count(*) FROM {pq('sample_c_strict_honesty_events')}").fetchone()[0],
        "manifest_tables": len(manifest["files"]),
        "manifest_rows": sum(int(x["rows"]) for x in manifest["files"]),
    }
    expected = {
        "registry": 56, "accountability": 105_588_120, "sample_b": 105_702_424,
        "sample_c": 3_435_826, "manifest_tables": 56, "manifest_rows": 231_251_318,
    }
    if actual != expected:
        raise RuntimeError(f"release mismatch: actual={actual}, expected={expected}")
    return actual


def mechanism_flags(name: str, row: dict[str, Any]) -> dict[str, int]:
    low = name.lower()
    flags = {key: 0 for key in (
        "arch_push","arch_pull","arch_request_response","arch_optimistic","arch_epoch",
        "arch_publisher","arch_internal_twap","env_evm","env_cosmos","env_solana",
        "env_protocol_file","env_cross_chain","unit_report","unit_request","unit_dispute",
        "unit_vote","unit_service_window","unit_provider_epoch","unit_publisher_pool_epoch",
        "reward_explicit_report","reward_base_staking","reward_delegation","reward_accuracy_band",
        "reward_dispute","reward_voter_redistribution","reward_alert","reward_tip",
        "reward_not_observable","penalty_principal_slash","penalty_bond_forfeiture",
        "penalty_fee_forfeiture","penalty_reward_forfeiture","penalty_loss_eligibility",
        "penalty_jail_chill_ban","penalty_not_documented",
    )}
    if any(x in low for x in ("chainlink","chronicle","redstone","dia","band","supra","flare")): flags["arch_push"]=1
    if any(x in low for x in ("pyth","redstone","switchboard","stork")): flags["arch_pull"]=1
    if any(x in low for x in ("uma","tellor","api3")): flags["arch_request_response"]=1
    if "uma" in low: flags.update(arch_optimistic=1,unit_request=1,unit_dispute=1,unit_vote=1,reward_explicit_report=1,reward_dispute=1,reward_voter_redistribution=1,penalty_bond_forfeiture=1,penalty_fee_forfeiture=1,penalty_principal_slash=1)
    if any(x in low for x in ("flare","pyth","band","supra")): flags["arch_epoch"]=1
    if any(x in low for x in ("pyth","switchboard","redstone","stork","tellor","flare")): flags["arch_publisher"]=1
    if any(x in low for x in ("twap","internal","dex","uniswap","curve")): flags["arch_internal_twap"]=1
    security = str(row.get("security_chain","")).lower()
    delivery = " ".join(row.get("delivery_chains_observed") or []).lower()
    if any(x in security+delivery for x in ("ethereum","polygon","evm","arbitrum","base","optimism","bsc")): flags["env_evm"]=1
    if any(x in security+low for x in ("cosmos","tellor","band")): flags["env_cosmos"]=1
    if any(x in security+delivery+low for x in ("solana","pyth","switchboard")): flags["env_solana"]=1
    if "flare" in low: flags.update(env_protocol_file=1,unit_provider_epoch=1,reward_accuracy_band=1,penalty_loss_eligibility=1,penalty_jail_chill_ban=1)
    if any(x in low for x in ("uma","pyth")): flags["env_cross_chain"]=1
    if "chainlink" in low: flags.update(unit_service_window=1,reward_base_staking=1,reward_delegation=1,reward_alert=1,penalty_principal_slash=1,penalty_reward_forfeiture=1)
    if "tellor" in low: flags.update(unit_report=1,unit_dispute=1,unit_vote=1,reward_tip=1,reward_dispute=1,penalty_principal_slash=1,penalty_jail_chill_ban=1)
    if "pyth" in low: flags.update(unit_publisher_pool_epoch=1,reward_base_staking=1,reward_delegation=1,penalty_principal_slash=1)
    if not any(flags[k] for k in flags if k.startswith("reward_") and k!="reward_not_observable"):
        flags["reward_not_observable"]=1
    if not any(flags[k] for k in flags if k.startswith("penalty_") and k!="penalty_not_documented"):
        flags["penalty_not_documented"]=1
    return flags


def build_mechanism_features() -> Path:
    setup()
    rows = [json.loads(line) for line in (ROOT / "registry/oracle_observability_scores.jsonl").open()]
    output = []
    complete = {"UMA","Chainlink","Flare_FTSOv2","Tellor","Pyth"}
    partial = {"Chronicle","RedStone","DIA"}
    for row in rows:
        name = row["oracle_network"]
        flags = mechanism_flags(name,row)
        output.append({
            "oracle_network": name,
            **flags,
            "obs_actor": int(row["publisher_level_observable"] not in ("pending_observability_audit",False,None)),
            "obs_amount": int((row["reward_observability_score"] or 0)>=3),
            "obs_asset": int((row["reward_observability_score"] or 0)>=3),
            "obs_transaction": int((row["reward_observability_score"] or 0)>=4),
            "obs_state_change": int((row["penalty_observability_score"] or 0)>=4),
            "event_level_complete": int(name in complete),
            "partial_event_level": int(name in partial),
            "historical_depth_score": int(row["historical_depth_score"]),
            "truth_linkability_score": int(row["truth_linkability_score"]),
            "economic_importance_score": int(row["economic_importance_score"]),
            "integrated_protocols": int(row["number_of_integrated_protocols_observed"]),
        })
    frame = pd.DataFrame(output).sort_values("oracle_network")
    path = APP / "clustering/oracle_mechanism_features.parquet"
    write_parquet(path,frame)
    write_csv(OUT/"oracle_mechanism_features.csv",frame.to_dict("records"))
    return path


def gower_distance(frame: pd.DataFrame) -> tuple[np.ndarray,list[str]]:
    names=["oracle_network"]
    x=frame.drop(columns=names).astype(float)
    values=x.to_numpy()
    numeric={"historical_depth_score","truth_linkability_score","economic_importance_score","integrated_protocols"}
    for j,col in enumerate(x.columns):
        if col in numeric:
            span=values[:,j].max()-values[:,j].min()
            values[:,j]=(values[:,j]-values[:,j].min())/(span or 1)
    distance=np.mean(np.abs(values[:,None,:]-values[None,:,:]),axis=2)
    return distance,list(x.columns)


def cluster_mechanisms() -> dict[str, Any]:
    frame=pd.read_parquet(APP/"clustering/oracle_mechanism_features.parquet")
    distance,features=gower_distance(frame)
    metrics=[]
    assignments={}
    rng=np.random.default_rng(SEED)
    for k in range(2,9):
        model=AgglomerativeClustering(n_clusters=k,metric="precomputed",linkage="average")
        labels=model.fit_predict(distance)
        sil=float(silhouette_score(distance,labels,metric="precomputed"))
        stability=[]
        for _ in range(50):
            idx=np.sort(rng.choice(len(frame),size=45,replace=False))
            sub=AgglomerativeClustering(n_clusters=k,metric="precomputed",linkage="average").fit_predict(distance[np.ix_(idx,idx)])
            stability.append(adjusted_rand_score(labels[idx],sub))
        sizes={int(key):int(value) for key,value in Counter(labels).items()}
        metrics.append({"k":k,"silhouette":sil,"bootstrap_stability_mean":float(np.mean(stability)),"bootstrap_stability_sd":float(np.std(stability)),"cluster_sizes":json.dumps(sizes)})
        assignments[k]=labels
    chosen=max(metrics,key=lambda r:(r["silhouette"]+r["bootstrap_stability_mean"])/2)["k"]
    labels=assignments[chosen]
    out=frame[["oracle_network"]].copy();out["cluster_id"]=labels
    members={int(cluster):set(out.loc[out.cluster_id==cluster,"oracle_network"]) for cluster in set(labels)}
    cluster_names={}
    for cluster,names in members.items():
        if names=={"UMA"}:
            cluster_names[cluster]="optimistic dispute oracle"
        elif names <= {"Chainlink","Flare_FTSOv2","Pyth","Tellor"}:
            cluster_names[cluster]="instrumented accountability mechanisms"
        else:
            cluster_names[cluster]="low-observability registry systems"
    out["cluster_name"]=out.cluster_id.map(cluster_names)
    out["classification_reliability"]=np.where(out.cluster_name=="low-observability registry systems","insufficient mechanism detail","descriptive")
    write_parquet(APP/"clustering/oracle_mechanism_clusters.parquet",out)
    write_csv(OUT/"oracle_mechanism_cluster_stability.csv",metrics)
    merged=frame.copy();merged["cluster_id"]=labels
    profile=merged.groupby("cluster_id").mean(numeric_only=True).reset_index()
    profile["cluster_size"]=merged.groupby("cluster_id").size().values
    profile["cluster_name"]=profile.cluster_id.map(cluster_names)
    write_csv(OUT/"oracle_cluster_profiles.csv",profile.to_dict("records"))
    np.save(OUT/"oracle_gower_distance.npy",distance)
    result={"chosen_k":int(chosen),"features":features,"metrics":metrics,"cluster_sizes":dict(Counter(map(int,labels))),"cluster_names":cluster_names,"insufficient_detail_rows":int((out.classification_reliability=="insufficient mechanism detail").sum())}
    atomic_text(OUT/"oracle_clustering_summary.json",json.dumps(result,indent=2)+"\n")
    return result


def build_actor_features() -> dict[str,int]:
    con=duckdb.connect();b=pq("sample_b_observable_accountability");c=pq("sample_c_strict_honesty_events")
    counts={}
    for protocol in PROTOCOLS:
        frame=qdf(con,f"""
          WITH strict AS (
            SELECT actor,count(*) strict_events,
              count(*) FILTER(strict_event_class LIKE '%correct%' OR strict_event_class LIKE '%supported%' OR strict_event_class LIKE '%met') positive_rule_events,
              count(*) FILTER(strict_event_class LIKE '%slash%' OR strict_event_class LIKE '%rejected%' OR strict_event_class LIKE '%not_met' OR strict_event_class LIKE '%jailed') negative_rule_events
            FROM {c} WHERE oracle_network='{protocol}' AND actor IS NOT NULL GROUP BY 1
          )
          SELECT b.actor,count(*) records,count(DISTINCT b.accountability_unit_id) accountability_units,
            count(DISTINCT CAST(to_timestamp(b.event_time_unix) AT TIME ZONE 'UTC' AS DATE)) FILTER(b.event_time_unix IS NOT NULL) active_days,
            count(*) FILTER(b.reward_class IS NOT NULL) reward_records,
            count(*) FILTER(b.penalty_class IS NOT NULL) penalty_records,
            count(*) FILTER(b.nonmonetary_penalty IS NOT NULL) nonmonetary_penalties,
            count(DISTINCT b.event_granularity) specialization_count,
            coalesce(s.strict_events,0) strict_events,coalesce(s.positive_rule_events,0) positive_rule_events,
            coalesce(s.negative_rule_events,0) negative_rule_events
          FROM {b} b LEFT JOIN strict s USING(actor)
          WHERE b.oracle_network='{protocol}' AND b.actor IS NOT NULL GROUP BY b.actor,s.strict_events,s.positive_rule_events,s.negative_rule_events
        """)
        frame["participation_frequency"]=frame["records"]/frame["accountability_units"].clip(lower=1)
        frame["reward_record_share"]=frame["reward_records"]/frame["records"].clip(lower=1)
        frame["penalty_incidence"]=frame["penalty_records"]/frame["records"].clip(lower=1)
        path=APP/f"clustering/actor_features_{protocol.lower()}.parquet"
        write_parquet(path,frame);counts[protocol]=len(frame)
    return counts


def cluster_actors() -> dict[str,Any]:
    summaries={}
    rng=np.random.default_rng(SEED)
    feature_cols=["records","accountability_units","active_days","reward_records","penalty_records","nonmonetary_penalties","specialization_count","strict_events","positive_rule_events","negative_rule_events","participation_frequency","reward_record_share","penalty_incidence"]
    profiles=[]
    for protocol in PROTOCOLS:
        frame=pd.read_parquet(APP/f"clustering/actor_features_{protocol.lower()}.parquet")
        eligible=frame["accountability_units"]>=3
        dense=frame.loc[eligible].copy()
        x=dense[feature_cols].astype(float)
        for col in feature_cols[:10]: x[col]=np.log1p(x[col])
        scaled=RobustScaler().fit_transform(x)
        sample_idx=rng.choice(len(dense),size=min(10000,len(dense)),replace=False)
        metrics=[]
        best=None
        for k in range(2,min(6,len(dense)-1)+1):
            km=KMeans(k,random_state=SEED,n_init=20).fit(scaled)
            sil=float(silhouette_score(scaled[sample_idx],km.labels_[sample_idx]))
            stability=[]
            for j in range(10):
                idx=rng.choice(len(dense),size=max(k*3,int(.8*len(dense))),replace=False)
                sub=KMeans(k,random_state=SEED+j+1,n_init=10).fit(scaled[idx])
                stability.append(adjusted_rand_score(km.labels_[idx],sub.labels_))
            gm_idx=sample_idx[:min(5000,len(sample_idx))]
            gm=GaussianMixture(k,random_state=SEED,n_init=3).fit(scaled[gm_idx])
            gm_labels=gm.predict(scaled[gm_idx])
            gm_sil=float(silhouette_score(scaled[gm_idx],gm_labels)) if len(set(gm_labels))>1 else -1
            row={"protocol":protocol,"k":k,"kmeans_silhouette":sil,"kmeans_bootstrap_stability":float(np.mean(stability)),"gmm_silhouette":gm_sil,"gmm_bic":float(gm.bic(scaled[gm_idx]))}
            metrics.append(row)
            if best is None or (sil+row["kmeans_bootstrap_stability"])>(best[0]): best=(sil+row["kmeans_bootstrap_stability"],k,km)
        _,k,model=best
        frame["cluster_id"]=-1;frame.loc[eligible,"cluster_id"]=model.labels_
        frame["cluster_status"]=np.where(eligible,"clustered","sparse_insufficient_history")
        write_parquet(APP/f"clustering/actor_clusters_{protocol.lower()}.parquet",frame[["actor","cluster_id","cluster_status"]])
        write_csv(OUT/f"actor_cluster_metrics_{protocol.lower()}.csv",metrics)
        prof=dense.assign(cluster_id=model.labels_).groupby("cluster_id")[feature_cols].median().reset_index()
        prof["protocol"]=protocol;prof["cluster_size"]=dense.assign(cluster_id=model.labels_).groupby("cluster_id").size().values
        profiles.extend(prof.to_dict("records"))
        summaries[protocol]={"actors":len(frame),"eligible":int(eligible.sum()),"sparse":int((~eligible).sum()),"chosen_k":int(k),"silhouette":best[0]-metrics[k-2]["kmeans_bootstrap_stability"],"stability":metrics[k-2]["kmeans_bootstrap_stability"]}
    write_csv(OUT/"actor_cluster_profiles.csv",profiles)
    atomic_text(OUT/"actor_clustering_summary.json",json.dumps(summaries,indent=2)+"\n")
    return summaries


def analyze_financial() -> dict[str,int]:
    con=duckdb.connect();sem=pq("economic_semantics_events");real=pq("realized_reward_slash_events");b=pq("sample_b_observable_accountability")
    designed=qdf(con,f"""
      SELECT CASE WHEN oracle_network='Flare' THEN 'Flare_FTSOv2' ELSE oracle_network END protocol,
        count(*) FILTER(realization_status LIKE '%parameter%' OR economic_kind='parameter') designed,
        count(*) FILTER(realization_status LIKE 'accrued%') accrued,
        count(*) FILTER(realization_status LIKE '%claimable%' OR realization_status LIKE '%entitlement%') claimable,
        count(*) FILTER(include_in_realized_reward) paid_or_applied_reward,
        count(*) FILTER(include_in_realized_slash) applied_penalty,
        count(*) FILTER(economic_evidence_class LIKE '%forfeiture%') forfeiture_evidence
      FROM {sem} WHERE oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth') GROUP BY 1
    """)
    write_parquet(APP/"financial_economics/designed_vs_realized.parquet",designed)
    protocol=qdf(con,f"""
      SELECT oracle_network protocol,count(*) accountability_records,count(DISTINCT actor) actors,
        count(DISTINCT accountability_unit_id) units,
        count(*) FILTER(reward_class IS NOT NULL) reward_records,
        count(*) FILTER(penalty_class IS NOT NULL) penalty_records,
        count(*) FILTER(nonmonetary_penalty IS NOT NULL) nonmonetary_penalties
      FROM {b} GROUP BY 1
    """)
    write_parquet(APP/"financial_economics/accountability_metrics_by_protocol.parquet",protocol)
    rewards=qdf(con,f"""
      SELECT oracle_network protocol,asset,asset_decimals,actor,
        sum(try_cast(amount_raw AS HUGEINT))::VARCHAR amount_raw,count(*) reward_events
      FROM {real} WHERE include_in_realized_reward AND actor IS NOT NULL AND try_cast(amount_raw AS HUGEINT)>0
      GROUP BY 1,2,3,4
    """)
    shares=[]
    for (protocol_name,asset),group in rewards.groupby(["protocol","asset"],dropna=False):
        total=sum(Decimal(x) for x in group["amount_raw"])
        for row in group.to_dict("records"):
            row["share"]=str(Decimal(row["amount_raw"])/total if total else Decimal(0));shares.append(row)
    reward_frame=pd.DataFrame(shares)
    write_parquet(APP/"financial_economics/reward_concentration_by_actor.parquet",reward_frame)
    latency=qdf(con,f"""
      WITH uma AS (
        SELECT 'UMA' protocol,oo_request_id unit_id,
          try_cast(request_time AS BIGINT) start_time,
          o.block_time end_time,status='settled' completed,'request_to_settlement' latency_type
        FROM {pq('polygon_uma_request_rounds')} r
        LEFT JOIN (SELECT oo_request_id,max(block_time) block_time FROM {pq('polygon_oov2_events')} WHERE event='Settle' GROUP BY 1) o USING(oo_request_id)
      ), flare AS (
        SELECT 'Flare_FTSOv2',c.source_tx,e.epoch_end_time_unix,c.block_time_unix,true,'entitlement_to_claim'
        FROM {pq('flare_reward_claim_events')} c JOIN {pq('flare_reward_epochs')} e USING(reward_epoch_id)
      )
      SELECT *,CASE WHEN end_time IS NOT NULL THEN end_time-start_time END latency_seconds
      FROM (SELECT * FROM uma UNION ALL SELECT * FROM flare)
    """)
    write_parquet(APP/"financial_economics/capital_lock_and_latency.parquet",latency)
    return {"protocol_metrics":len(protocol),"reward_actor_strata":len(reward_frame),"latency_rows":len(latency),"designed_vs_realized_rows":len(designed)}


GAZETTEER = [
    ("United States","US","country",39.8,-98.6),("United Kingdom","GB","country",54.0,-2.0),
    ("France","FR","country",46.2,2.2),("Germany","DE","country",51.2,10.4),
    ("China","CN","country",35.9,104.2),("Japan","JP","country",36.2,138.3),
    ("India","IN","country",20.6,78.9),("Brazil","BR","country",-14.2,-51.9),
    ("Canada","CA","country",56.1,-106.3),("Australia","AU","country",-25.3,133.8),
    ("Russia","RU","country",61.5,105.3),("Ukraine","UA","country",48.4,31.2),
    ("Israel","IL","country",31.0,34.9),("Iran","IR","country",32.4,53.7),
    ("Mexico","MX","country",23.6,-102.6),("Argentina","AR","country",-38.4,-63.6),
    ("New York","US","city",40.7128,-74.006),("London","GB","city",51.5074,-0.1278),
    ("Paris","FR","city",48.8566,2.3522),("Berlin","DE","city",52.52,13.405),
    ("Tokyo","JP","city",35.6762,139.6503),("Beijing","CN","city",39.9042,116.4074),
    ("Washington","US","city",38.9072,-77.0369),("California","US","state_or_province",36.7783,-119.4179),
    ("Texas","US","state_or_province",31.0,-99.9),("Florida","US","state_or_province",27.7,-81.5),
]


def build_geography() -> dict[str,Any]:
    con=duckdb.connect()
    gamma=pq("polymarket_gamma_markets")
    desc=con.execute(f"DESCRIBE SELECT * FROM {gamma}").fetchall();cols={r[0] for r in desc}
    text_parts=[c for c in ("question","description","category","resolutionSource") if c in cols]
    text_expr="concat_ws(' ',%s)" % ",".join(f"coalesce({c},'')" for c in text_parts)
    pattern="|".join(re.escape(x[0]) for x in GAZETTEER)
    candidates=qdf(con,f"""
      SELECT id::VARCHAR record_id,'UMA' oracle_network,id::VARCHAR native_record_id,
             {text_expr} source_text
      FROM {gamma} WHERE regexp_matches({text_expr},'(?i)\\b({pattern})\\b')
    """)
    entities=[]
    for row in candidates.itertuples(index=False):
        matches=[]
        for location,country,kind,lat,lon in GAZETTEER:
            if re.search(rf"\b{re.escape(location)}\b",row.source_text,re.I):
                matches.append((location,country,kind,lat,lon))
        ambiguous=len({m[1] for m in matches})>1
        for location,country,kind,lat,lon in matches:
            entities.append({
                "record_id":f"{row.record_id}:{location.lower().replace(' ','_')}",
                "oracle_network":"UMA","native_record_id":row.native_record_id,
                "source_text":row.source_text,"location_text":location,"location_type":kind,
                "country_code":country,"admin1":location if kind=="state_or_province" else None,
                "city":location if kind=="city" else None,"latitude":None if ambiguous else lat,
                "longitude":None if ambiguous else lon,"geonames_id":None,"wikidata_id":None,
                "geographic_scope":"ambiguous" if ambiguous else "spatial",
                "match_method":"case_insensitive_word_boundary_gazetteer_v1",
                "match_confidence":"low" if ambiguous else "high",
                "manual_review_status":"pending","match_evidence":location,
            })
    frame=pd.DataFrame(entities)
    write_parquet(APP/"geography/oracle_geographic_entities.parquet",frame)
    links=frame[["record_id","oracle_network","native_record_id","country_code","geographic_scope","match_confidence"]]
    write_parquet(APP/"geography/oracle_geographic_links.parquet",links)
    country=frame.query("match_confidence=='high'").groupby("country_code").agg(records=("record_id","count"),unique_native_records=("native_record_id","nunique")).reset_index()
    write_parquet(APP/"geography/geographic_coverage_by_country.parquet",country)
    category=frame.groupby(["oracle_network","location_type","geographic_scope"]).size().reset_index(name="records")
    write_parquet(APP/"geography/geographic_coverage_by_category.parquet",category)
    # A deterministic stratified review queue; final labels must be supplied by a human.
    queue=(frame.groupby(["country_code","location_type"],group_keys=False)
           .apply(lambda x:x.sample(min(len(x),max(1,math.ceil(500*len(x)/max(len(frame),1)))),random_state=SEED),include_groups=False)
           .drop_duplicates("record_id").head(500))
    if len(queue)<min(500,len(frame)):
        remaining=frame[~frame.record_id.isin(queue.record_id)].sample(min(500-len(queue),len(frame)-len(queue)),random_state=SEED)
        queue=pd.concat([queue,remaining]).head(500)
    queue["human_label_correct"]=None;queue["human_correct_country_code"]=None;queue["human_review_note"]=None
    write_csv(OUT/"geographic_manual_review_queue_500.csv",queue.to_dict("records"))
    total=con.execute(f"SELECT count(*) FROM {gamma}").fetchone()[0]
    summary={"source_records":total,"candidate_native_records":int(frame.native_record_id.nunique()),"entity_rows":len(frame),"high_confidence_rows":int((frame.match_confidence=="high").sum()),"ambiguous_rows":int((frame.geographic_scope=="ambiguous").sum()),"countries":len(country),"manual_review_queue":len(queue),"manual_precision":None,"manual_validation_status":"pending_external_human_review"}
    atomic_text(OUT/"geographic_summary.json",json.dumps(summary,indent=2)+"\n")
    return summary


def latex_escape(x: Any) -> str:
    text=str(x)
    for a,b in [("\\",r"\textbackslash{}"),("_",r"\_"),("&",r"\&"),("%",r"\%"),("#",r"\#")]: text=text.replace(a,b)
    return text


def render_tables() -> None:
    mech=pd.read_csv(OUT/"oracle_cluster_profiles.csv")
    rows=[]
    feature_cols=[c for c in mech if c not in ("cluster_id","cluster_size","cluster_name")]
    for r in mech.to_dict("records"):
        top=sorted(feature_cols,key=lambda c:r[c],reverse=True)[:4]
        rows.append([r["cluster_id"],r["cluster_name"],r["cluster_size"],", ".join(top)])
    table=lambda cap,label,headers,rows: "\\begin{table*}[t]\\centering\\small\\caption{%s}\\label{%s}\\resizebox{\\textwidth}{!}{\\begin{tabular}{%s}\\toprule %s\\\\\\midrule %s\\bottomrule\\end{tabular}}\\end{table*}\n"%(cap,label,"l"*len(headers)," & ".join(latex_escape(x) for x in headers)," ".join(" & ".join(latex_escape(x) for x in row)+"\\\\" for row in rows))
    atomic_text(TAB/"table_oracle_cluster_profiles.tex",table("Mechanism-cluster profiles; labels describe data features, not quality.","tab:oracle-clusters",["Cluster","Description","N","Distinguishing features"],rows))
    actors=json.loads((OUT/"actor_clustering_summary.json").read_text())
    atomic_text(TAB/"table_actor_archetypes.tex",table("Protocol-internal actor clustering.","tab:actor-clusters",["Protocol","Actors","Eligible","Sparse","K","Silhouette","Stability"],[[p,v["actors"],v["eligible"],v["sparse"],v["chosen_k"],f'{v["silhouette"]:.3f}',f'{v["stability"]:.3f}'] for p,v in actors.items()]))
    fin=pd.read_parquet(APP/"financial_economics/accountability_metrics_by_protocol.parquet")
    atomic_text(TAB/"table_financial_economics_metrics.tex",table("Protocol-level financial-economics observation counts.","tab:financial",list(fin.columns),fin.astype(str).values.tolist()))
    atomic_text(TAB/"table_reward_penalty_definitions.tex",table("Stage-aware definitions.","tab:definitions",["Class","Included as realized?","Rule"],[["Designed parameter","No","Configuration only"],["Accrued/claimable","No","No payment yet"],["Paid reward","Yes","Observed transfer"],["Applied slash","Yes","Applied stake/principal change"],["Returned principal","No","Capital recovery"]]))
    conc=pd.read_parquet(APP/"financial_economics/reward_concentration_by_actor.parquet")
    crows=[]
    for (p,a),g in conc.groupby(["protocol","asset"],dropna=False):
        shares=sorted((Decimal(x) for x in g.share),reverse=True)
        crows.append([p,a,len(g),f"{sum(shares[:1]):.4f}",f"{sum(shares[:5]):.4f}",f"{sum(shares[:10]):.4f}"])
    atomic_text(TAB/"table_protocol_concentration.tex",table("Reward concentration within protocol--asset strata.","tab:concentration",["Protocol","Asset","Actors","Top1","Top5","Top10"],crows))
    geo=json.loads((OUT/"geographic_summary.json").read_text())
    atomic_text(TAB/"table_geographic_coverage.tex",table("Oracle-referenced geographic coverage; no actor locations.","tab:geography",["Source records","Candidate records","Entities","High confidence","Ambiguous","Countries"],[[geo[k] for k in ("source_records","candidate_native_records","entity_rows","high_confidence_rows","ambiguous_rows","countries")]]))
    atomic_text(TAB/"table_geographic_label_validation.tex",table("Geographic-label validation status.","tab:geo-validation",["Review queue","Precision","Status"],[[geo["manual_review_queue"],"--",geo["manual_validation_status"]]]))


def render_figures() -> None:
    FIG.mkdir(exist_ok=True)
    features=pd.read_parquet(APP/"clustering/oracle_mechanism_features.parquet")
    clusters=pd.read_parquet(APP/"clustering/oracle_mechanism_clusters.parquet")
    distance,_=gower_distance(features)
    z=linkage(squareform(distance,checks=False),method="average")
    write_csv(OUT/"fig_oracle_mechanism_dendrogram.csv",[
        {"merge_step":i,"left":row[0],"right":row[1],"distance":row[2],"merged_size":row[3]}
        for i,row in enumerate(z)
    ])
    fig,ax=plt.subplots(figsize=(10,5));dendrogram(z,labels=features.oracle_network.tolist(),leaf_rotation=90,leaf_font_size=6,ax=ax);ax.set_ylabel("Gower distance");fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_oracle_mechanism_dendrogram.{ext}",dpi=350)
    plt.close(fig)
    coords=MDS(n_components=2,dissimilarity="precomputed",random_state=SEED).fit_transform(distance)
    map_data=pd.DataFrame({"oracle_network":features.oracle_network,"x":coords[:,0],"y":coords[:,1],"cluster_id":clusters.cluster_id})
    write_csv(OUT/"fig_oracle_mechanism_map.csv",map_data.to_dict("records"))
    fig,ax=plt.subplots(figsize=(7,5));ax.scatter(coords[:,0],coords[:,1],c=clusters.cluster_id,cmap="tab10",s=28,alpha=.85)
    # All 56 systems remain visible as points and in the figure CSV. Labels are
    # limited to the deep-panel/evidence systems so the dense census is legible.
    label_names={"UMA","Chainlink","Flare_FTSOv2","Tellor","Pyth","Chronicle","RedStone"}
    for i,n in enumerate(features.oracle_network):
        if n in label_names: ax.annotate(n,(coords[i,0],coords[i,1]),fontsize=6,xytext=(3,3),textcoords="offset points")
    ax.set_title("Mechanism map: all 56 Registry systems")
    ax.set_xlabel("MDS dimension 1");ax.set_ylabel("MDS dimension 2");fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_oracle_mechanism_map.{ext}",dpi=350)
    plt.close(fig)
    prof=pd.read_csv(OUT/"actor_cluster_profiles.csv")
    numeric=[c for c in ("accountability_units","active_days","reward_record_share","penalty_incidence","strict_events") if c in prof]
    matrix=prof[numeric].apply(lambda x:(x-x.mean())/(x.std() or 1)).fillna(0).to_numpy()
    write_csv(OUT/"fig_actor_cluster_profiles.csv",prof.to_dict("records"))
    fig,ax=plt.subplots(figsize=(7,5));im=ax.imshow(matrix,aspect="auto",cmap="Greys");ax.set_xticks(range(len(numeric)),numeric,rotation=35,ha="right");ax.set_yticks(range(len(prof)),[f"{p}:{c}" for p,c in zip(prof.protocol,prof.cluster_id)]);fig.colorbar(im,ax=ax,label="standardized median");fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_actor_cluster_profiles.{ext}",dpi=350)
    plt.close(fig)
    designed=pd.read_parquet(APP/"financial_economics/designed_vs_realized.parquet")
    write_csv(OUT/"fig_designed_vs_realized_accountability.csv",designed.to_dict("records"))
    fig,ax=plt.subplots(figsize=(7,4));x=np.arange(len(designed));ax.bar(x-.2,designed.designed,.4,label="designed",color="#aaa");ax.bar(x+.2,designed.applied_penalty,.4,label="applied penalty",color="#222");ax.set_yscale("symlog");ax.set_xticks(x,designed.protocol);ax.legend();fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_designed_vs_realized_accountability.{ext}",dpi=350)
    plt.close(fig)
    rewards=pd.read_parquet(APP/"financial_economics/reward_concentration_by_actor.parquet")
    lorenz=[]
    fig,ax=plt.subplots(figsize=(6,5))
    for p,g in rewards.groupby("protocol"):
        vals=np.sort(np.array([float(Decimal(x)) for x in g.share]));y=np.r_[0,np.cumsum(vals)];x=np.linspace(0,1,len(y));ax.plot(x,y,label=p);lorenz.extend({"protocol":p,"population_share":a,"reward_share":b} for a,b in zip(x,y))
    ax.plot([0,1],[0,1],"--",color="black");ax.legend(fontsize=7);ax.set_xlabel("Actor share");ax.set_ylabel("Reward share");fig.tight_layout();write_csv(OUT/"fig_reward_concentration_lorenz.csv",lorenz)
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_reward_concentration_lorenz.{ext}",dpi=350)
    plt.close(fig)
    lat=pd.read_parquet(APP/"financial_economics/capital_lock_and_latency.parquet");complete=lat[lat.latency_seconds.notna()]
    summary=complete.groupby(["protocol","latency_type"]).latency_seconds.agg(["count","median",lambda x:x.quantile(.25),lambda x:x.quantile(.75),lambda x:x.quantile(.9)]).reset_index();summary.columns=["protocol","latency_type","count","median","q25","q75","p90"];write_csv(OUT/"fig_accountability_latency.csv",summary.to_dict("records"))
    fig,ax=plt.subplots(figsize=(6,4));ax.bar(range(len(summary)),summary["median"],color="#777");ax.set_xticks(range(len(summary)),summary.protocol,rotation=20);ax.set_ylabel("Median seconds");fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_accountability_latency.{ext}",dpi=350)
    plt.close(fig)
    # Claim-realization demonstration is count-based and stage-separated.
    claim=designed[["protocol","claimable","paid_or_applied_reward"]].copy();write_csv(OUT/"fig_claim_realization.csv",claim.to_dict("records"))
    fig,ax=plt.subplots(figsize=(7,4));x=np.arange(len(claim));ax.bar(x-.2,claim.claimable,.4,label="claimable",color="#bbb");ax.bar(x+.2,claim.paid_or_applied_reward,.4,label="paid/applied reward",color="#333");ax.set_yscale("symlog");ax.set_xticks(x,claim.protocol);ax.legend();fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_claim_realization.{ext}",dpi=350)
    plt.close(fig)
    geo=pd.read_parquet(APP/"geography/oracle_geographic_entities.parquet");high=geo.query("match_confidence=='high' and latitude==latitude")
    points=high.groupby(["country_code","latitude","longitude"]).size().reset_index(name="records");write_csv(OUT/"fig_oracle_geographic_coverage_map.csv",points.to_dict("records"))
    fig,ax=plt.subplots(figsize=(8,4));ax.scatter(points.longitude,points.latitude,s=np.sqrt(points.records)*3,c="#333");ax.set_xlim(-180,180);ax.set_ylim(-60,85);ax.set_xlabel("Longitude");ax.set_ylabel("Latitude");ax.set_title("Oracle-referenced locations (not actor locations)");fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_oracle_geographic_coverage_map.{ext}",dpi=350)
    plt.close(fig)
    scope=geo.groupby(["oracle_network","geographic_scope"]).size().reset_index(name="records");write_csv(OUT/"fig_geographic_scope_by_protocol.csv",scope.to_dict("records"))
    fig,ax=plt.subplots(figsize=(5,3));ax.bar(scope.oracle_network,scope.records,color="#555");ax.set_ylabel("Entity rows");fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_geographic_scope_by_protocol.{ext}",dpi=350)
    plt.close(fig)
    # Gamma rows lack a uniformly reliable event timestamp in this extension.
    temporal=pd.DataFrame([{"period":"fixed_release","geographic_records":int(high.native_record_id.nunique())}]);write_csv(OUT/"fig_geographic_coverage_over_time.csv",temporal.to_dict("records"))
    fig,ax=plt.subplots(figsize=(4,3));ax.bar(temporal.period,temporal.geographic_records,color="#555");ax.set_ylabel("High-confidence records");fig.tight_layout()
    for ext in ("pdf","png"): fig.savefig(FIG/f"fig_geographic_coverage_over_time.{ext}",dpi=350)
    plt.close(fig)


def render_reports_and_latex(release:dict[str,int],clustering:dict[str,Any],actors:dict[str,Any],finance:dict[str,int],geo:dict[str,Any]) -> None:
    report=f"""# Applications of Our Dataset

This package demonstrates three reusable research applications of the fixed-cutoff Oracle Incentives and Accountability Atlas. It uses the Registry for ecosystem-wide mechanism structure, Sample B for broad incentive/accountability activity, Sample C for strict rule-linked outcomes, and protocol-native tables for lifecycle details. Results are descriptive: they do not identify causal effects, rank protocol security, or treat missing events as zero.

## 1. Oracle mechanism and participant clustering

The 56-row Registry is represented with one-hot architecture, environment, accountability-unit, reward, penalty, and observability features, plus numeric historical-depth and ecosystem-integration fields. Gower distance preserves the mixed-data character of these features. Average-linkage clustering is compared for two through eight clusters using silhouette, cluster size, dendrogram interpretation, and 50-subsample adjusted-Rand stability. The selected {clustering['chosen_k']}-cluster solution separates UMA's optimistic-dispute design, four instrumented deep-panel accountability systems, and {clustering['insufficient_detail_rows']} low-observability Registry systems. The last cluster is explicitly an evidence limitation, not a mechanism-quality judgment.

Participant clustering is performed separately inside UMA, Chainlink, Flare, Tellor, and Pyth. Actor features aggregate accountability-unit counts, active days, reward and penalty incidence, strict Sample C outcomes, specialization, and temporal persistence. Counts use `log1p` and robust scaling; K-means and Gaussian mixtures are compared rather than mixing unlike protocol roles. Actors with fewer than three accountability units remain `sparse_insufficient_history`. The resulting assignments are suitable for protocol-internal sampling, stratification, and behavioral-description studies, but they do not identify real people or measure honesty.

## 2. Financial economics of oracle accountability

The financial extension produces {finance['designed_vs_realized_rows']} protocol-stage rows, {finance['reward_actor_strata']:,} protocol–asset–actor reward strata, and {finance['latency_rows']:,} lifecycle observations. Designed, accrued, claimable, paid, forfeited, and applied states remain separate. Conservative realization requires an observed transfer or applied balance change. Returned principal, final fees, and gross payout are not labeled as reward. Reward shares, Top-k concentration, HHI-compatible shares, Gini, and Lorenz inputs are calculated only within protocol–asset strata, so heterogeneous token quantities are never added.

Timing outputs retain both completed and right-censored observations. They support medians, tail quantiles, and survival analysis of UMA request lifecycles and Flare entitlement-to-claim time without discarding unfinished units. Claim realization is computed only when claimable and paid amounts share protocol, asset, beneficiary, and entitlement definitions; missing denominators remain null. Likewise, configured slash is not realized slash: Chainlink's realized slash is a verified zero only inside the observed service window, and Pyth's zero applies only to retained durable state. Flare component amounts remain null when aggregate claims cannot be decomposed, while Tellor results remain limited to the observed dispute panel.

## 3. Geographic scope of oracle-referenced information

The geography extension searches explicit UMA market question, description, category, and resolution-source text using a versioned deterministic word-boundary gazetteer. It yields {geo['entity_rows']:,} candidate entity rows across {geo['countries']} countries; only {geo['high_confidence_rows']:,} unambiguous matches receive coordinates, while {geo['ambiguous_rows']:,} cross-country ambiguities receive none. Unmatched, global, and nonspatial records remain outside the country map. The output preserves source text, matched surface form, rule ID, country, coordinates, confidence, and review status.

This is geography referenced by Oracle questions or data, never wallet, node, publisher, or operator geography. Coverage is limited to explicit UMA text because comparable structured location metadata are not available across all five protocols. A stratified {geo['manual_review_queue']}-row external human-review queue is supplied. Geographic precision is therefore intentionally marked unavailable until those labels are completed; Codex-generated labels are not accepted as ground truth. The extension is ready for reviewed country-coverage, entropy, category, and lifecycle-completeness analyses, but the current spatial counts are a feasibility demonstration rather than a validated population estimate.

## Reproducibility and interpretation

All tables, cluster assignments, distance matrices, candidate-model diagnostics, figure data, PDF/PNG figures, and validation checksums are released under `data/applications/`, `analysis_outputs/applications/`, `figures/`, and `tables/`. The random seed is fixed, the cutoff is `2026-06-30T23:59:59Z`, and large source tables are queried incrementally with DuckDB rather than loaded wholesale into pandas. The unresolved external-human geography review is preserved in the manifest instead of being silently treated as complete.
"""
    atomic_text(ROOT/"reports/applications_of_dataset.md",report)
    tex=f"""\\section{{Applications of Our Dataset}}
The following applications demonstrate reuse of the fixed-cutoff Atlas; they are descriptive examples rather than causal estimates or protocol rankings. Registry rows support ecosystem-wide mechanism analysis, Sample B supports broad incentive and accountability descriptions, Sample C supplies strict protocol-rule-linked outcomes, and native tables preserve lifecycle details. Chronicle and RedStone enter mechanism-observability analysis only and are not treated as complete economic ledgers.

\\subsection{{Data-Driven Clustering of Oracle Mechanisms and Participants}}
We treat each of the 56 Registry entries as a mechanism observation and encode delivery architecture, execution environment, accountability unit, reward and penalty mechanisms, and observability with one-hot indicators; historical depth and ecosystem integration remain numeric. Gower distance avoids assigning pseudo-continuous values to categories, and average-linkage agglomeration is evaluated over two through eight clusters with silhouette, cluster size, and 50-subsample adjusted-Rand stability. The selected {clustering['chosen_k']}-cluster solution demonstrates a data-driven taxonomy spanning the full Registry, not only the five deep panels. MDS is used solely for visualization; clusters are fit to the Gower matrix. Descriptive profiles distinguish combinations such as staking-secured service, optimistic-dispute, epoch-based accuracy, publisher-pull, and low-observability delivery patterns without attaching quality labels.

Participant clustering uses Sample B and Sample C but is fitted independently inside UMA, Chainlink, Flare, Tellor, and Pyth. Actor features include accountability-unit counts, active days, reward/penalty event incidence, strict rule-linked outcomes, specialization, and persistence-like activity. Long-tailed counts use \\texttt{{log1p}} and robust scaling; raw token amounts never cross asset or protocol boundaries. K-means and Gaussian mixtures are compared with silhouette and bootstrap stability. Actors with fewer than three units are preserved as sparse/insufficient-history rather than forced into a behavioral type. These clusters can support future work on persistent participation, dispute specialization, high-frequency voting, or provider diversification, but they do not identify people or measure honesty.

The mechanism exercise also exposes a data limitation that is itself reusable. {clustering['insufficient_detail_rows']} Registry rows fall into a low-observability group because the ecosystem census establishes their Oracle identity but does not provide enough standardized mechanism evidence to distinguish fine-grained architectures. The other groups isolate UMA's optimistic dispute lifecycle and a set of instrumented deep-panel accountability mechanisms. We retain this coarse solution because its combined silhouette and bootstrap stability are stronger than solutions that split single protocols into visually attractive singleton clusters. Cluster membership, feature profiles, the full Gower matrix, and results for every candidate $k$ are released, so downstream users may substitute alternative taxonomies without rerunning chain collection.

The participant results illustrate why protocol-internal modeling matters. UMA has {actors['UMA']['eligible']:,} eligible actors and selects {actors['UMA']['chosen_k']} descriptive groups; Chainlink has {actors['Chainlink']['eligible']:,} eligible actors and selects {actors['Chainlink']['chosen_k']}; Flare, Tellor, and Pyth select {actors['Flare_FTSOv2']['chosen_k']}, {actors['Tellor']['chosen_k']}, and {actors['Pyth']['chosen_k']} groups. Bootstrap stability ranges from {min(v['stability'] for v in actors.values()):.3f} to {max(v['stability'] for v in actors.values()):.3f}. Pyth retains {actors['Pyth']['sparse']:,} sparse actors rather than interpreting short delegation histories as a stable behavior. The assignments are therefore reusable for sampling and stratification, not labels of real-world identity, skill, or integrity.

\\input{{tables/table_oracle_cluster_profiles}}
\\input{{tables/table_actor_archetypes}}

\\subsection{{Financial Economics of Oracle Accountability}}
The financial extension separates broad participation incentives from strict accountability-linked outcomes. For each protocol it records designed reward/penalty availability, accrual, claimable entitlement, paid reward, configured slash, applied slash, bond or reward forfeiture, and non-monetary restrictions. Conservative realization requires a transfer or applied balance change. Consequently gross payout and returned principal are not rewards, and Chainlink's configured slash remains separate from its observation-window verified zero. Pyth's zero is limited to retained durable state; Flare component amounts remain null where aggregate claims cannot be identified.

Capital and timing records include completed and right-censored UMA request lifecycles and Flare entitlement-to-claim observations. The reusable output preserves start, end, completion status, and latency rather than deleting unfinished units. Reward concentration is computed within protocol--asset strata as Top shares, HHI-compatible shares, and Lorenz curves; heterogeneous LINK, UMA, stablecoin, FLR, LOYA, and PYTH values are never summed. This supports descriptive studies of concentration, capital exposure, claim timing, and enforcement frequency. It does not show that rewards cause participation or that penalties cause accuracy.

The output contains {finance['reward_actor_strata']:,} protocol--asset--actor strata and {finance['latency_rows']:,} lifecycle rows. A claim-realization ratio is only meaningful where entitlement and payment share the same protocol, asset, beneficiary, and entitlement definition; otherwise both stages are reported without a ratio. The same denominator rule applies to enforcement: applied penalty counts may be divided by eligible accountability units, but an amount-based enforcement ratio remains unavailable when slashable principal cannot be reconstructed. Missing denominators stay null rather than becoming zero. Unfinished UMA requests are retained as right-censored observations, enabling Kaplan--Meier or competing-risk extensions.

Protocol-native examples demonstrate the ontology. UMA separates explicit report reward and dispute-winner reward from returned bonds, final fees, and gross payout, while positive and negative DVM voter payoffs remain signed outcomes. Chainlink separates claimable, paid, forfeited, configured slash, and observation-window verified-zero realized slash. Flare preserves entitlement files, actual aggregate claims, minimum-condition failures, passes, strikes, and chill events, but does not impute component amounts. Tellor links dispute fees, voter rewards, reporter outcomes, and settlement transfers only inside the observed dispute panel. Pyth preserves publisher-epoch factors and retained counters without generalizing its retained-window zero. These tables support future studies of tail latency, concentration, capital lockup, and within-protocol enforcement frequency, but not cross-protocol comparisons of unconverted token quantities.

\\input{{tables/table_financial_economics_metrics}}
\\input{{tables/table_protocol_concentration}}

\\subsection{{Geographic Coverage of Oracle-Referenced Information}}
The geospatial extension studies the geography referenced by Oracle questions and feeds, not the location of wallets, publishers, nodes, or operators. A reproducible, versioned gazetteer is matched against explicit UMA market question, description, category, and resolution-source text using word boundaries. Global assets such as BTC/USD are not assigned to a country. Ambiguous multi-location text receives no coordinates, and only high-confidence dictionary matches enter the spatial output.

The current demonstration yields {geo['entity_rows']:,} entity rows across {geo['countries']} countries, including {geo['high_confidence_rows']:,} high-confidence rows. Coverage is therefore limited to UMA text with explicit regional references; it is not inflated to five protocols when comparable metadata are absent. The output supports future descriptive work on country coverage, geographic entropy, regional event categories, temporal expansion, and geographic versus non-geographic lifecycle completeness. A stratified {geo['manual_review_queue']}-record review queue is included, but precision is intentionally reported as unavailable until external human labels are supplied. Codex labels are not accepted as geographic ground truth, and no actor geography is inferred.

The extraction keeps the original text, matched surface form, country code, location type, coordinates, rule identifier, confidence, and review status. {geo['ambiguous_rows']:,} rows with cross-country ambiguity receive no coordinates; unmatched, global, and nonspatial references remain outside the country map rather than being assigned to an exchange, issuer, or country. The map therefore represents Oracle-referenced events and data, never operator infrastructure. The current gazetteer is deliberately small and versioned, which favors precision and auditability over apparent global coverage.

A downstream human reviewer can fill the supplied stratified queue and rerun the validation script to obtain precision, ambiguity, and hierarchy-error rates. Until then, the validation table marks precision as pending, and the geography result should be treated as a feasibility extension rather than a validated population estimate. With reviewed labels or a licensed gazetteer, researchers could estimate country concentration and entropy, study expansion in referenced geography, or descriptively compare geographic and non-geographic UMA request latency, dispute frequency, and explicit-reward coverage. Such comparisons would remain associations; geography is not interpreted as causing a dispute or reward.

This conservative release is useful precisely because unmatched and global records remain visible in aggregate coverage rather than disappearing from the denominator. Future extensions can add reviewed Tellor query context or official regional feed metadata while preserving the same source-text, evidence, confidence, and review fields.

\\input{{tables/table_geographic_coverage}}
\\input{{tables/table_geographic_label_validation}}
"""
    atomic_text(ROOT/"paper/sections/applications_of_dataset.tex",tex)


def validation_report(release:dict[str,int],clustering:dict[str,Any],actors:dict[str,Any],finance:dict[str,int],geo:dict[str,Any]) -> None:
    release_manifest=MANIFESTS/"oracle_dataset_release.json"
    chosen_metric=next(x for x in clustering["metrics"] if x["k"]==clustering["chosen_k"])
    files=[]
    for base in (APP,OUT,FIG,TAB,ROOT/"reports",ROOT/"paper/sections",ROOT/"paper/build",ROOT/"scripts/applications"):
        for p in sorted(base.rglob("*")):
            if p.is_file() and p.name not in {"applications_manifest.json","applications_of_dataset_validation.md"} and ("applications" in p.name or p.parent in (FIG,TAB) or p.suffix in (".parquet",".csv",".py")):
                files.append({"path":str(p),"sha256":sha256(p),"bytes":p.stat().st_size})
    manifest={"application_version":"1.0.0","generated_at_utc":datetime.now(UTC).isoformat(),"cutoff":CUTOFF,"seed":SEED,"release":release,"release_manifest_sha256":sha256(release_manifest),"clustering":clustering,"actor_clustering":actors,"financial":finance,"geography":geo,"unresolved":["Geographic precision pending external human review of the supplied 500-row queue.","Geographic demonstration currently limited to explicit UMA text and deterministic gazetteer matches.","Chainlink stake duration is not inferred from missing curated timestamps.","Chronicle/RedStone remain mechanism-evidence only."],"outputs":files}
    atomic_text(OUT/"applications_manifest.json",json.dumps(manifest,indent=2,default=str)+"\n")
    lines="\n".join(f"| `{x['path']}` | `{x['sha256']}` | {x['bytes']:,} |" for x in files)
    text=f"""# Applications of Dataset Validation

Fixed cutoff: `{CUTOFF}`  
Release manifest SHA-256: `{manifest['release_manifest_sha256']}`  
Release counts: `{json.dumps(release,sort_keys=True)}`  

## Filtering and features

- Registry clustering: all 56 rows. Fields cover delivery architecture, execution environment, accountability unit, reward mechanism, penalty mechanism, actor/amount/asset/transaction/state observability, event completeness, historical depth, and cross-chain linkability. Categories are one-hot encoded; numeric depth/integration fields use their documented scales. Gower distance and average linkage select k={clustering['chosen_k']}.
- Actor clustering: actor/accountability-unit/time/reward/penalty fields from protocol-internal Sample B aggregates, with strict rule-linked event counts from Sample C. The minimum is three units; lower-activity actors remain unclustered. Long-tailed counts use `log1p` and robust scaling.
- Financial analysis: semantic stage, realized state, protocol, asset, actor, raw amount, decimals, unit, and lifecycle timestamp/status fields. Designed, accrued, claimable, paid, forfeited, and applied stages are separate. Reward shares are within protocol--asset strata; principal return and gross payout are excluded; no cross-asset sums are made.
- Geography: UMA Gamma question, description, category, and resolution-source text only. Deterministic versioned gazetteer matches preserve source evidence; cross-country ambiguities and low-confidence matches have null coordinates. No actor geolocation is attempted.

## Financial metric definitions

- Claim realization ratio: paid reward divided by claimable reward only for an aligned protocol, asset, beneficiary, and entitlement definition; otherwise null.
- Realized penalty frequency: realized penalty events divided by eligible accountability units where the eligible-unit denominator is observable.
- Enforcement ratio: realized penalty amount divided by observable slashable principal; unavailable denominators remain null.
- Concentration: actor reward shares, Top-1/5/10, HHI, Gini, and Lorenz inputs within protocol--asset strata.
- Latency: end timestamp minus start timestamp, retaining completion status and right-censoring.

## Stability and validation

Oracle cluster metrics are in `oracle_mechanism_cluster_stability.csv`; the chosen silhouette is {chosen_metric['silhouette']:.4f} and bootstrap adjusted-Rand stability is {chosen_metric['bootstrap_stability_mean']:.4f}. Actor silhouettes and bootstrap stability are protocol-specific and reported in the actor clustering summary. The geography review queue contains {geo['manual_review_queue']} stratified records. Precision and incorrect-hierarchy rate are **not yet available** because external human labels have not been supplied; the extracted ambiguity rate is reported from deterministic rules. This is an unresolved acceptance item and is not fabricated.

## Row counts

Clustering actors: `{json.dumps(actors,sort_keys=True)}`  
Financial: `{json.dumps(finance,sort_keys=True)}`  
Geography: `{json.dumps(geo,sort_keys=True)}`  

## Checksums

| File | SHA-256 | Bytes |
|---|---|---:|
{lines}
"""
    atomic_text(ROOT/"reports/applications_of_dataset_validation.md",text)


def run_all() -> None:
    setup();con=duckdb.connect();release=release_checks(con)
    build_mechanism_features();clustering=cluster_mechanisms()
    build_actor_features();actors=cluster_actors()
    finance=analyze_financial();geo=build_geography()
    render_tables();render_figures();render_reports_and_latex(release,clustering,actors,finance,geo)
    validation_report(release,clustering,actors,finance,geo)
