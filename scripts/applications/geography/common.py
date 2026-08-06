#!/usr/bin/env python3
"""Reality-grounding benchmark preparation and silver-label diagnostics.

No function in this module creates a human gold label.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/applications/reality_grounding"
FIG = ROOT / "figures"
CUTOFF = "2026-06-30 23:59:59+00"
SEED = 20260729
DOMAINS = [
    "crypto_price", "fiat_fx", "commodity", "equity_rwa",
    "macroeconomic_indicator", "politics_election", "sports",
    "weather_climate", "insurance", "corporate_event",
    "legal_regulatory_event", "other",
]


def _p(path: Path) -> str:
    return str(path).replace("'", "''")


def build_objects() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    sem = ROOT / "data/applications/geographic_semantic/semantic_domain_labels.parquet"
    geo = ROOT / "data/applications/geographic_semantic/geographic_labels.parquet"
    con = duckdb.connect()
    target = OUT / "native_oracle_objects.parquet"
    con.execute(
        f"""
        COPY (
          WITH s AS (
            SELECT
              protocol || ':' || source_record_id AS source_object_id,
              protocol,
              arg_max(source_text, length(coalesce(source_text,''))) AS source_text,
              NULL::VARCHAR AS metadata,
              string_agg(DISTINCT coverage_status, ',') AS evidence_resolution,
              min(event_time) AS first_seen,
              max(event_time) AS last_seen,
              sum(represented_records) AS event_count_represented,
              CASE WHEN arg_max(semantic_domain, event_time)='unknown'
                   THEN 'other' ELSE arg_max(semantic_domain, event_time) END
                AS silver_semantic_domain,
              string_agg(DISTINCT match_rule, ';') AS silver_domain_rule
            FROM read_parquet('{_p(sem)}')
            WHERE event_time <= TIMESTAMPTZ '{CUTOFF}'
            GROUP BY 1,2
          ), g AS (
            SELECT protocol || ':' || source_record_id AS source_object_id,
                   arg_max(location_surface,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS location_surface,
                   arg_max(location_type,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS location_type,
                   arg_max(country_code,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS iso_country,
                   arg_max(admin1,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS admin1,
                   arg_max(city,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS city,
                   arg_max(latitude,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS latitude,
                   arg_max(longitude,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS longitude,
                   arg_max(geonames_id,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS geonames_id,
                   arg_max(wikidata_id,
                           CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                     AS wikidata_id,
                   max(confidence) AS automatic_location_confidence,
                   count(*) AS automatic_location_candidates
            FROM read_parquet('{_p(geo)}') GROUP BY 1
          )
          SELECT s.*,
                 CASE
                   WHEN g.source_object_id IS NULL
                        AND strpos(evidence_resolution,'global_or_nonspatial')>0
                     THEN 'nonspatial'
                   WHEN g.location_type='country' THEN 'country'
                   WHEN g.city IS NOT NULL THEN 'city'
                   WHEN g.admin1 IS NOT NULL THEN 'subnational'
                   WHEN g.source_object_id IS NOT NULL THEN 'ambiguous'
                   ELSE 'ambiguous'
                 END AS silver_geographic_scope,
                 g.location_surface AS silver_location_span,
                 g.geonames_id AS silver_geonames_id,
                 g.wikidata_id AS silver_wikidata_id,
                 g.iso_country AS silver_iso_country,
                 g.admin1 AS silver_admin1,
                 g.city AS silver_city,
                 g.latitude AS silver_latitude,
                 g.longitude AS silver_longitude,
                 g.automatic_location_confidence,
                 coalesce(g.automatic_location_candidates,0) AS automatic_location_candidates,
                 false AS human_validated
          FROM s LEFT JOIN g USING (source_object_id)
        ) TO '{_p(target)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.close()


def build_annotation_package(n: int = 3000) -> None:
    objects = pd.read_parquet(OUT / "native_oracle_objects.parquet")
    objects["auto_confidence_stratum"] = np.where(
        objects.automatic_location_confidence.eq("high"), "auto_high",
        np.where(objects.silver_semantic_domain.eq("other"), "unmatched_or_other", "auto_other")
    )
    strata = ["protocol", "silver_semantic_domain", "silver_geographic_scope",
              "auto_confidence_stratum"]
    # Keep every rare protocol/object class, then hash-order within strata.
    objects["_hash"] = objects.source_object_id.map(
        lambda x: int(hashlib.sha256(f"{SEED}:{x}".encode()).hexdigest()[:16], 16)
    )
    objects = objects.sort_values(strata + ["_hash"])
    groups = list(objects.groupby(strata, dropna=False, sort=False))
    quota = max(1, n // max(1, len(groups)))
    selected = pd.concat([g.head(quota) for _, g in groups], ignore_index=True)
    if len(selected) < n:
        rest = objects[~objects.source_object_id.isin(selected.source_object_id)]
        selected = pd.concat([selected, rest.sort_values("_hash").head(n-len(selected))])
    selected = selected.head(n).drop(columns="_hash")
    selected["reviewer_1_domain"] = pd.NA
    selected["reviewer_1_scope"] = pd.NA
    selected["reviewer_1_location_span"] = pd.NA
    selected["reviewer_1_entity_id"] = pd.NA
    selected["reviewer_2_domain"] = pd.NA
    selected["reviewer_2_scope"] = pd.NA
    selected["reviewer_2_location_span"] = pd.NA
    selected["reviewer_2_entity_id"] = pd.NA
    selected["adjudicated_domain"] = pd.NA
    selected["adjudicated_scope"] = pd.NA
    selected["adjudicated_geonames_id"] = pd.NA
    selected["adjudicated_wikidata_id"] = pd.NA
    selected["adjudication_notes"] = pd.NA
    selected["annotation_status"] = "pending_two_independent_human_reviews"
    selected.to_parquet(OUT / "human_annotation_queue_3000.parquet", index=False)
    selected.to_csv(OUT / "human_annotation_queue_3000.csv", index=False)
    pd.DataFrame([{
        "n_selected": len(selected),
        "n_reviewer_1_complete": 0,
        "n_reviewer_2_complete": 0,
        "n_adjudicated": 0,
        "cohens_kappa": np.nan,
        "krippendorff_alpha": np.nan,
        "disagreement_rate": np.nan,
        "ambiguous_rate": np.nan,
        "publishable_gold_metrics": False,
    }]).to_parquet(OUT / "human_annotation_status.parquet", index=False)


def build_splits() -> None:
    con = duckdb.connect()
    src = OUT / "native_oracle_objects.parquet"
    con.execute(
        f"""
        COPY (
          WITH x AS (
            SELECT *,
              abs(hash(source_object_id || ':{SEED}'))%10000 h,
              row_number() OVER (ORDER BY first_seen,source_object_id) rn,
              count(*) OVER () n
            FROM read_parquet('{_p(src)}')
          )
          SELECT source_object_id, protocol, first_seen,
            CASE WHEN h<7000 THEN 'train' WHEN h<8500 THEN 'validation' ELSE 'test' END
              AS random_stratified_split,
            CASE WHEN rn<=floor(n*.70) THEN 'train'
                 WHEN rn<=floor(n*.85) THEN 'validation' ELSE 'test' END
              AS chronological_split,
            'protocol:'||protocol AS lopo_group,
            CASE WHEN silver_semantic_domain IN ('insurance','equity_rwa',
                                                  'legal_regulatory_event')
                 THEN 'open_set_test' ELSE 'known_domain_development' END
              AS unseen_domain_split
          FROM x
        ) TO '{_p(OUT / "splits.parquet")}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.close()


def run_silver_domain_baseline(max_rows: int = 80000) -> None:
    """Diagnostic only: evaluate text models against deterministic silver labels."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score, confusion_matrix, f1_score, log_loss, recall_score,
    )
    from sklearn.pipeline import Pipeline

    objects = pd.read_parquet(OUT / "native_oracle_objects.parquet")
    splits = pd.read_parquet(OUT / "splits.parquet")
    d = objects.merge(splits, on=["source_object_id","protocol","first_seen"])
    if len(d) > max_rows:
        sampled = []
        for _, group in d.groupby("silver_semantic_domain", sort=False):
            sampled.append(group.sample(
                min(len(group), max(20, max_rows//len(DOMAINS))),
                random_state=SEED,
            ))
        d = pd.concat(sampled, ignore_index=True)
    train = d[d.random_stratified_split == "train"]
    test = d[d.random_stratified_split == "test"]
    model = Pipeline([
        ("tfidf", TfidfVectorizer(ngram_range=(1,2), min_df=2, max_features=50000,
                                  sublinear_tf=True)),
        ("clf", LogisticRegression(max_iter=250, class_weight="balanced",
                                   random_state=SEED, n_jobs=min(8,os.cpu_count() or 1))),
    ])
    model.fit(train.source_text.fillna(""), train.silver_semantic_domain)
    pred = model.predict(test.source_text.fillna(""))
    prob = model.predict_proba(test.source_text.fillna(""))
    classes = model.named_steps["clf"].classes_
    rows = pd.DataFrame({
        "source_object_id": test.source_object_id,
        "protocol": test.protocol,
        "silver_target": test.silver_semantic_domain,
        "prediction": pred,
        "confidence": prob.max(axis=1),
        "is_human_gold": False,
        "evaluation_status": "development_only_against_deterministic_silver_labels",
    })
    rows.to_parquet(OUT / "silver_predictions.parquet", index=False)
    metrics = pd.DataFrame([{
        "task": "G1_domain",
        "model": "TFIDF_logistic_regression",
        "split": "random_stratified",
        "n_train": len(train), "n_test": len(test),
        "macro_f1_silver": f1_score(test.silver_semantic_domain, pred, average="macro"),
        "micro_f1_silver": f1_score(test.silver_semantic_domain, pred, average="micro"),
        "accuracy_silver": accuracy_score(test.silver_semantic_domain, pred),
        "log_loss_silver": log_loss(test.silver_semantic_domain, prob, labels=classes),
        "human_gold_metric": False,
    }])
    metrics.to_parquet(OUT / "silver_metrics.parquet", index=False)
    cm = confusion_matrix(test.silver_semantic_domain, pred, labels=classes)
    pd.DataFrame(cm, index=classes, columns=classes).rename_axis(
        "silver_target"
    ).reset_index().melt("silver_target", var_name="prediction",
                        value_name="count").to_parquet(
        OUT / "silver_domain_confusion.parquet", index=False
    )
    recalls = recall_score(test.silver_semantic_domain, pred, labels=classes,
                           average=None, zero_division=0)
    pd.DataFrame({"class": classes, "recall_silver": recalls}).to_parquet(
        OUT / "silver_per_class_recall.parquet", index=False
    )
    pd.DataFrame([
        ["keyword_gazetteer_rules", "available_no_valid_gold_evaluation",
         "source of silver labels; self-evaluation prohibited"],
        ["TFIDF_logistic_regression", "executed_silver_development_only",
         "not a human-gold benchmark result"],
        ["SentenceBERT_linear", "blocked_pending_human_gold", ""],
        ["DeBERTa_finetune", "blocked_pending_human_gold", ""],
        ["token_classification_NER", "blocked_pending_span_gold", ""],
        ["bi_encoder_entity_retrieval", "blocked_pending_entity_gold", ""],
        ["cross_encoder_entity_reranking", "blocked_pending_entity_gold", ""],
        ["open_set_abstention", "blocked_pending_gold_and_open_set_review", ""],
    ], columns=["model","status","note"]).to_parquet(OUT / "model_registry.parquet",
                                                    index=False)


def _save(fig, stem: str, data: pd.DataFrame) -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / f"{stem}.pdf", bbox_inches="tight")
    fig.savefig(FIG / f"{stem}.png", dpi=300, bbox_inches="tight")
    data.to_csv(FIG / f"{stem}.csv", index=False)
    data.to_parquet(FIG / f"{stem}.parquet", index=False)


def render_figures() -> None:
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set_theme(style="whitegrid", context="paper")
    blue, teal, orange, red, gray = "#3264C8", "#2A9D8F", "#F4A261", "#D1495B", "#7A8491"
    reg = pd.read_parquet(OUT / "model_registry.parquet")
    reg["ready"] = reg.status.str.startswith(("executed","available")).astype(int)
    fig, ax = plt.subplots(figsize=(7.2,3.7))
    ax.barh(reg.model, reg.ready, color=[teal if x else gray for x in reg.ready])
    ax.set_xlim(0,1.05); ax.set_xticks([0,1],["pending human gold","development available"])
    ax.set_title("Reality-grounding benchmark readiness—not model accuracy")
    _save(fig, "fig_grounding_model_benchmark", reg)
    plt.close(fig)

    cm = pd.read_parquet(OUT / "silver_domain_confusion.parquet")
    mat = cm.pivot(index="silver_target",columns="prediction",values="count").fillna(0)
    fig, ax = plt.subplots(figsize=(8.2,6.6))
    sns.heatmap(mat, cmap="YlGnBu", square=True, cbar_kws={"label":"objects"}, ax=ax)
    ax.set_title("Development diagnostic against silver labels (not gold)")
    _save(fig, "fig_domain_confusion_matrix", cm)
    plt.close(fig)

    objects = pd.read_parquet(OUT / "native_oracle_objects.parquet")
    scope = objects.groupby(["silver_geographic_scope"],as_index=False).size()
    # A real confusion matrix requires two human-reviewed dimensions.
    scope["human_gold_available"] = False
    fig, ax = plt.subplots(figsize=(6.4,3.6))
    ax.bar(scope.silver_geographic_scope, scope["size"], color=orange)
    ax.tick_params(axis="x",rotation=25)
    ax.set_title("Automatic scope inventory; confusion matrix pending gold")
    ax.set_ylabel("Distinct native objects")
    _save(fig, "fig_scope_confusion_matrix", scope)
    plt.close(fig)

    bubble = objects.groupby(["protocol","silver_semantic_domain"],as_index=False).size()
    fig, ax = plt.subplots(figsize=(8.2,4.4))
    protocols = list(bubble.protocol.unique())
    domains = list(bubble.silver_semantic_domain.unique())
    for _, r in bubble.iterrows():
        ax.scatter(domains.index(r.silver_semantic_domain),
                   protocols.index(r.protocol), s=8+15*np.log1p(r["size"]),
                   color=blue, alpha=.7)
    ax.set_xticks(range(len(domains)),domains,rotation=40,ha="right")
    ax.set_yticks(range(len(protocols)),protocols)
    ax.set_title("Silver semantic coverage by distinct native object")
    _save(fig, "fig_protocol_domain_bubble_matrix", bubble)
    plt.close(fig)

    world = objects.groupby("silver_iso_country",dropna=False,as_index=False).size()
    world["validated"] = False
    fig, ax = plt.subplots(figsize=(7.0,3.5))
    ax.axis("off")
    ax.text(.5,.63,"COUNTRY COVERAGE WITHHELD",ha="center",va="center",
            fontsize=17,weight="bold",color=red,transform=ax.transAxes)
    ax.text(.5,.42,"3,000-object double-human review is pending.\\n"
                   "Automatic high-confidence matches are not treated as truth.",
            ha="center",va="center",fontsize=10,transform=ax.transAxes)
    _save(fig, "fig_validated_world_coverage", world)
    plt.close(fig)

    temp = objects.assign(year=pd.to_datetime(objects.first_seen,utc=True).dt.year)
    temp = temp.groupby(["year","silver_semantic_domain"],as_index=False).size()
    fig, ax = plt.subplots(figsize=(7.0,3.6))
    for i,(domain,g) in enumerate(temp.groupby("silver_semantic_domain")):
        ax.plot(g.year,g["size"],label=domain,alpha=.75)
    ax.set_yscale("symlog",linthresh=1); ax.set_ylabel("First-seen native objects")
    ax.set_title("Preliminary semantic expansion; geography not validated")
    _save(fig, "fig_geographic_expansion", temp)
    plt.close(fig)

    pred = pd.read_parquet(OUT / "silver_predictions.parquet").sort_values(
        "confidence",ascending=False
    )
    pred["coverage"] = np.arange(1,len(pred)+1)/len(pred)
    pred["cumulative_silver_error"] = (
        (pred.prediction != pred.silver_target).cumsum()/np.arange(1,len(pred)+1)
    )
    fig, ax = plt.subplots(figsize=(6.4,3.6))
    ax.plot(pred.coverage,pred.cumulative_silver_error,color=teal,lw=2)
    ax.set_xlabel("Coverage after confidence-based abstention")
    ax.set_ylabel("Cumulative error vs silver labels")
    ax.set_title("Open-set risk–coverage diagnostic; human gold pending")
    _save(fig, "fig_open_set_risk_coverage", pred)
    plt.close(fig)


def write_metadata() -> None:
    config = {
        "cutoff": CUTOFF, "random_seed": SEED,
        "observation_unit": "distinct native oracle object",
        "event_count_used_as_training_weight": False,
        "human_gold_generated_by_codex": False,
        "actor_geography_inferred": False,
        "country_level_findings_publishable": False,
        "domain_classes": DOMAINS,
    }
    (OUT/"benchmark_config.json").write_text(
        json.dumps(config,indent=2,sort_keys=True),encoding="utf-8"
    )
    rows=[]
    for p in sorted(OUT.iterdir()):
        if p.is_file() and p.name not in {"manifest.json", "checksums.csv"}:
            h=hashlib.sha256()
            with p.open("rb") as f:
                for chunk in iter(lambda:f.read(1024*1024),b""): h.update(chunk)
            rows.append({"file":p.name,"bytes":p.stat().st_size,"sha256":h.hexdigest()})
    pd.DataFrame(rows).to_csv(OUT/"checksums.csv",index=False)
    (OUT/"manifest.json").write_text(json.dumps({
        "application":"Oracle Reality Grounding Benchmark",
        "human_gold_status":"pending",
        "files":rows,
    },indent=2,sort_keys=True),encoding="utf-8")


def run_all() -> None:
    build_objects()
    build_annotation_package()
    build_splits()
    run_silver_domain_baseline()
    render_figures()
    write_metadata()


if __name__ == "__main__":
    run_all()
