"""Second-stage UMA analysis on the fixed 2026-06-30 data release.

The models are descriptive associations.  They do not identify causal effects
of rewards, bonds, adapter versions, or participant experience.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm


ROOT = Path(__file__).resolve().parents[1]
MAIN_START = 1_680_307_200
MAIN_END = 1_782_863_999
FIXED_CUTOFF = "2026-06-30T23:59:59Z"


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
    connection.execute(
        f"COPY ({query}) TO '{temporary.as_posix()}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
    )
    temporary.replace(output)


def wilson_interval(successes: int, observations: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if observations == 0:
        return (math.nan, math.nan)
    p = successes / observations
    denominator = 1 + z * z / observations
    centre = (p + z * z / (2 * observations)) / denominator
    margin = z * math.sqrt(p * (1 - p) / observations + z * z / (4 * observations * observations)) / denominator
    return centre - margin, centre + margin


def gini(values: pd.Series) -> float:
    array = np.sort(values.to_numpy(dtype=float))
    if len(array) == 0 or array.sum() == 0:
        return 0.0
    index = np.arange(1, len(array) + 1)
    return float((2 * np.dot(index, array) / array.sum() - (len(array) + 1)) / len(array))


def concentration(values: pd.Series) -> dict[str, float | int]:
    values = values.astype(float)
    total = float(values.sum())
    shares = values.sort_values(ascending=False) / total if total else values
    return {
        "participants": int(len(values)),
        "top1_share": float(shares.head(1).sum()) if total else 0.0,
        "top10_share": float(shares.head(10).sum()) if total else 0.0,
        "top100_share": float(shares.head(100).sum()) if total else 0.0,
        "hhi": float((shares * shares).sum()) if total else 0.0,
        "gini": gini(values),
    }


def fit_logit(
    frame: pd.DataFrame,
    outcome: str,
    predictors: list[str],
    cluster: str,
    model_name: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    design = sm.add_constant(frame[predictors].astype(float), has_constant="add")
    model = sm.GLM(frame[outcome].astype(float), design, family=sm.families.Binomial())
    result = model.fit(cov_type="cluster", cov_kwds={"groups": frame[cluster]})
    rows: list[dict[str, Any]] = []
    confidence = result.conf_int()
    for name in design.columns:
        coefficient = float(result.params[name])
        rows.append({
            "model": model_name,
            "outcome": outcome,
            "term": name,
            "coefficient_log_odds": coefficient,
            "cluster_robust_se": float(result.bse[name]),
            "z": float(result.tvalues[name]),
            "p_value": float(result.pvalues[name]),
            "odds_ratio": math.exp(coefficient),
            "odds_ratio_ci_low": math.exp(float(confidence.loc[name, 0])),
            "odds_ratio_ci_high": math.exp(float(confidence.loc[name, 1])),
        })
    metadata = {
        "model": model_name,
        "outcome": outcome,
        "observations": int(result.nobs),
        "clusters": int(frame[cluster].nunique()),
        "log_likelihood": float(result.llf),
        "deviance": float(result.deviance),
        "pseudo_r2_cs": float(result.pseudo_rsquared(kind="cs")),
        "covariance": f"cluster-robust by {cluster}",
        "predictors": predictors,
    }
    return metadata, rows


def percent(value: float) -> str:
    return f"{100 * value:.3f}%"


def markdown_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    header = "| " + " | ".join(label for _, label in columns) + " |"
    divider = "|" + "|".join("---" for _ in columns) + "|"
    body = ["| " + " | ".join(str(row.get(key, "")) for key, _ in columns) + " |" for row in rows]
    return "\n".join([header, divider, *body])


def build_report(summary: dict[str, Any], coefficients: list[dict[str, Any]]) -> str:
    request = summary["request_panel"]
    economics = summary["reward_and_bond"]
    participants = summary["participant_concentration"]
    dvm = summary["dvm_negative_slash"]
    groups = []
    for row in summary["request_groups"]:
        groups.append({
            "group": f"{row['dimension']}={row['value']}",
            "settled": row["settled"],
            "disputed": row["disputed"],
            "rate": percent(row["dispute_rate"]),
            "disputer_win": "—" if row["disputer_win_rate"] is None else percent(row["disputer_win_rate"]),
        })
    model_rows = []
    for row in coefficients:
        if row["term"] == "const":
            continue
        model_rows.append({
            "model": row["model"],
            "term": row["term"],
            "or": f"{row['odds_ratio']:.3f}",
            "ci": f"[{row['odds_ratio_ci_low']:.3f}, {row['odds_ratio_ci_high']:.3f}]",
            "p": f"{row['p_value']:.4g}",
        })
    return f"""# UMA 第二阶段统计研究

生成时间：{summary['generated_at_utc']}  
固定截止：{FIXED_CUTOFF}  
主窗口：2023-04-01 至 2026-06-30  
研究性质：观察性关联分析，不作因果识别。

## 样本与核心结果

- 主样本包含 {request['settled']:,} 个已结算 Polygon–UMA request，其中 {request['disputed']:,} 个发生争议；争议率为 {percent(request['dispute_rate'])}，Wilson 95% CI 为 [{percent(request['dispute_rate_ci_low'])}, {percent(request['dispute_rate_ci_high'])}]。
- 已结算争议中，disputer 胜 {request['disputer_wins']:,}/{request['disputed']:,}，胜率 {percent(request['disputer_win_rate'])}。
- 未争议显式报告奖励合计 {economics['explicit_report_reward_units']} USDC units；争议胜方奖励 {economics['dispute_winner_reward_units']}；败方 bond 与 final fee 损失 {economics['loser_bond_and_fee_units']}。
- 最终 DVM round 的负向 slash 中，no-valid-reveal 为 {dvm['no_valid_reveal_events']:,} 条、{percent(dvm['no_valid_reveal_event_share'])}；wrong-vote 为 {dvm['wrong_vote_events']:,} 条、{percent(dvm['wrong_vote_event_share'])}。这是按最终 resolved round 对齐后的分类。

## 分组描述

{markdown_table(groups, [('group','分组'),('settled','已结算'),('disputed','争议'),('rate','争议率'),('disputer_win','Disputer 胜率')])}

分组差异同时包含时间、市场构成、参与者和合约版本差异，不能解释为 adapter 升级的因果效果。

## Reward / bond

- 未争议 request 的 reward/bond 中位数：{economics['undisputed_reward_to_bond_median']:.6f}。
- 有争议 request 的 reward/bond 中位数：{economics['disputed_reward_to_bond_median']:.6f}。
- 有争议 request 的有效 bond 中位数：{economics['disputed_bond_median_units']:.3f} USDC units。
- 争议胜方平均奖励：{economics['mean_dispute_winner_reward_units']:.3f} USDC units；败方平均 bond+fee 损失：{economics['mean_loser_bond_and_fee_units']:.3f}。

## 参与者集中度

- Proposer：{participants['proposer_rounds']['participants']:,} 个地址；头部 1/10 地址占 rounds 的 {percent(participants['proposer_rounds']['top1_share'])} / {percent(participants['proposer_rounds']['top10_share'])}，HHI={participants['proposer_rounds']['hhi']:.4f}，Gini={participants['proposer_rounds']['gini']:.4f}。
- Disputer：{participants['disputer_events']['participants']:,} 个地址；头部 1/10 地址占 disputes 的 {percent(participants['disputer_events']['top1_share'])} / {percent(participants['disputer_events']['top10_share'])}，HHI={participants['disputer_events']['hhi']:.4f}，Gini={participants['disputer_events']['gini']:.4f}。
- DVM 负向 slash 金额：{participants['dvm_negative_amount']['participants']:,} 个 voter；头部 10 地址占 {percent(participants['dvm_negative_amount']['top10_share'])}，HHI={participants['dvm_negative_amount']['hhi']:.4f}。

## Logistic 关联模型

{markdown_table(model_rows, [('model','模型'),('term','变量'),('or','Odds ratio'),('ci','95% CI'),('p','p-value')])}

模型一研究争议是否发生，并按 proposer 聚类稳健标准误；模型二仅研究已争议 request 的 disputer 是否胜出，并按 disputer 聚类。`log_reward_to_bond_pct` 是 `log(1 + 100 × reward/bond)`，`log_bond` 和历史参与次数均使用 `log1p`。年度项与 adapter 项控制可观测 cohort 差异，但不能消除选择偏差。

## DVM no-vote 与 wrong-vote

- no-valid-reveal slash：{dvm['no_valid_reveal_events']:,} 条，金额 {dvm['no_valid_reveal_amount_uma']} UMA。
- wrong-vote slash：{dvm['wrong_vote_events']:,} 条，金额 {dvm['wrong_vote_amount_uma']} UMA。
- Grade-A Polymarket 子样本负向 slash：{dvm['grade_a_polymarket_negative_events']:,} 条，其中 no-valid-reveal 占 {percent(dvm['grade_a_no_valid_reveal_event_share'])}。
- `VoterSlashApplied` 未进入本表，所有金额仍只来自一次 `VoterSlashed`。

## 结论边界

现有数据支持“争议是低频、高金额问责尾部”以及“DVM 已实现负向 slash 主要对应最终轮无有效 reveal”的描述。Logistic 系数反映条件相关性，不证明提高 bond、改变 reward 或升级 adapter 会因果地改变争议或裁决结果。未争议接受也不等于外部客观真值。

## 复现

```bash
python3 scripts/build_research_samples.py
python3 scripts/analyze_uma_phase2.py
```
"""


def analyze(
    parquet_dir: Path,
    output_parquet_dir: Path,
    analysis_dir: Path,
    report_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    required = {
        name: parquet_dir / f"{name}.parquet"
        for name in (
            "polygon_uma_request_rounds",
            "uma_dvm_voter_payoffs",
            "uma_dvm_votes_events",
            "uma_dvm_requests",
            "uma_polygon_ethereum_grade_a_links",
        )
    }
    missing = [str(path) for path in required.values() if not path.is_file()]
    if missing:
        raise RuntimeError(f"missing fixed-cutoff UMA inputs: {missing}")
    connection = duckdb.connect()
    source = lambda name: f"read_parquet('{required[name].as_posix()}')"
    request_panel = output_parquet_dir / "uma_phase2_request_panel.parquet"
    dvm_panel = output_parquet_dir / "uma_phase2_dvm_negative_slash_panel.parquet"

    request_query = f"""
      WITH base AS (
        SELECT
          oo_request_id, question_id, adapter_version, requester, proposer, disputer,
          try_cast(request_time AS BIGINT) request_time_unix,
          source_block, transaction_index, log_index, proposal_block,
          economic_status,
          CASE WHEN economic_status LIKE 'settled_disputed_%' THEN 1 ELSE 0 END disputed,
          CASE WHEN economic_status='settled_disputed_disputer_wins' THEN 1
               WHEN economic_status LIKE 'settled_disputed_%' THEN 0 ELSE NULL END disputer_win,
          reward_raw, effective_bond_raw, explicit_report_reward_raw,
          dispute_winner_reward_raw, bond_forfeited_raw, final_fee_forfeited_raw,
          try_cast(reward_raw AS DOUBLE)/1e6 reward_units,
          try_cast(effective_bond_raw AS DOUBLE)/1e6 bond_units,
          try_cast(reward_raw AS DOUBLE)/nullif(try_cast(effective_bond_raw AS DOUBLE),0) reward_to_bond_ratio
        FROM {source('polygon_uma_request_rounds')}
        WHERE sample_tier='primary' AND status='settled'
          AND try_cast(request_time AS BIGINT) BETWEEN {MAIN_START} AND {MAIN_END}
      ), proposer_history AS (
        SELECT *,
          row_number() OVER (
            PARTITION BY proposer ORDER BY request_time_unix, source_block, transaction_index, log_index
          ) - 1 proposer_prior_rounds,
          coalesce(sum(disputed) OVER (
            PARTITION BY proposer ORDER BY request_time_unix, source_block, transaction_index, log_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ), 0) proposer_prior_disputes
        FROM base
      )
      SELECT *,
        CASE WHEN disputer IS NOT NULL THEN row_number() OVER (
          PARTITION BY disputer ORDER BY request_time_unix, source_block, transaction_index, log_index
        ) - 1 ELSE NULL END disputer_prior_disputes,
        year(to_timestamp(request_time_unix)) request_year,
        ln(1 + bond_units) log_bond,
        ln(1 + 100 * reward_to_bond_ratio) log_reward_to_bond_pct,
        ln(1 + proposer_prior_rounds) log_proposer_prior_rounds,
        CASE WHEN adapter_version='adapter_v3_current' THEN 1 ELSE 0 END adapter_v3,
        CASE WHEN year(to_timestamp(request_time_unix))=2024 THEN 1 ELSE 0 END year_2024,
        CASE WHEN year(to_timestamp(request_time_unix))>=2025 THEN 1 ELSE 0 END year_2025_plus
      FROM proposer_history
    """
    atomic_parquet(connection, request_query, request_panel)

    dvm_query = f"""
      WITH final_reveals AS (
        SELECT v.dvm_request_id, v.voter,
          bool_or(v.revealed_price_raw=q.resolved_price_raw) reveal_matches_resolved,
          count(*) valid_reveals
        FROM {source('uma_dvm_votes_events')} v
        JOIN {source('uma_dvm_requests')} q
          ON v.dvm_request_id=q.dvm_request_id AND v.round_id=q.round_id
        WHERE v.revealed AND v.revealed_price_raw IS NOT NULL
        GROUP BY 1,2
      ), grade_a AS (
        SELECT DISTINCT dvm_request_id
        FROM {source('uma_polygon_ethereum_grade_a_links')}
        WHERE cross_chain_match_grade='A' AND sample_tier='primary'
          AND try_cast(dvm_time AS BIGINT) >= {MAIN_START}
      )
      SELECT
        p.dvm_request_id, p.request_index, p.voter, p.source_tx, p.source_block, p.log_index,
        p.wrong_or_no_vote_slash_raw penalty_amount_raw,
        try_cast(p.wrong_or_no_vote_slash_raw AS DOUBLE)/1e18 penalty_amount_uma,
        q.request_time, q.round_id, year(to_timestamp(try_cast(q.request_time AS BIGINT))) request_year,
        CASE
          WHEN coalesce(r.valid_reveals,0)=0 THEN 'no_valid_reveal_slash'
          WHEN r.reveal_matches_resolved THEN 'matching_reveal_negative_slash_anomaly'
          ELSE 'wrong_vote_slash'
        END negative_slash_class,
        coalesce(r.valid_reveals,0) final_round_valid_reveals,
        coalesce(r.reveal_matches_resolved,false) reveal_matches_resolved,
        g.dvm_request_id IS NOT NULL grade_a_polymarket
      FROM {source('uma_dvm_voter_payoffs')} p
      LEFT JOIN {source('uma_dvm_requests')} q USING (dvm_request_id)
      LEFT JOIN final_reveals r USING (dvm_request_id, voter)
      LEFT JOIN grade_a g USING (dvm_request_id)
      WHERE p.classification_rule_id='DVM_NEGATIVE_SLASH'
    """
    atomic_parquet(connection, dvm_query, dvm_panel)

    requests = connection.execute(f"SELECT * FROM read_parquet('{request_panel.as_posix()}')").fetchdf()
    penalties = connection.execute(f"SELECT * FROM read_parquet('{dvm_panel.as_posix()}')").fetchdf()
    requests["log_disputer_prior_disputes"] = np.log1p(requests["disputer_prior_disputes"].fillna(0).astype(float))

    models: list[dict[str, Any]] = []
    coefficients: list[dict[str, Any]] = []
    metadata, rows = fit_logit(
        requests,
        "disputed",
        ["log_bond", "log_reward_to_bond_pct", "log_proposer_prior_rounds", "adapter_v3", "year_2024", "year_2025_plus"],
        "proposer",
        "dispute_occurrence",
    )
    models.append(metadata)
    coefficients.extend(rows)
    disputed = requests[requests["disputed"] == 1].copy()
    metadata, rows = fit_logit(
        disputed,
        "disputer_win",
        ["log_bond", "log_reward_to_bond_pct", "log_proposer_prior_rounds", "log_disputer_prior_disputes", "adapter_v3", "year_2024", "year_2025_plus"],
        "disputer",
        "disputer_win_conditional_on_dispute",
    )
    models.append(metadata)
    coefficients.extend(rows)

    group_rows: list[dict[str, Any]] = []
    for dimension, column in (("adapter", "adapter_version"), ("year", "request_year")):
        for value, group in requests.groupby(column, dropna=False):
            dispute_count = int(group["disputed"].sum())
            wins = int(group["disputer_win"].fillna(0).sum())
            group_rows.append({
                "dimension": dimension,
                "value": str(value),
                "settled": int(len(group)),
                "disputed": dispute_count,
                "dispute_rate": dispute_count / len(group),
                "disputer_wins": wins,
                "disputer_win_rate": wins / dispute_count if dispute_count else None,
            })

    total = len(requests)
    dispute_count = int(requests["disputed"].sum())
    disputer_wins = int(requests["disputer_win"].fillna(0).sum())
    low, high = wilson_interval(dispute_count, total)
    exact = connection.execute(f"""
      SELECT
        sum(try_cast(explicit_report_reward_raw AS HUGEINT)) FILTER (WHERE disputed=0),
        sum(try_cast(dispute_winner_reward_raw AS HUGEINT)) FILTER (WHERE disputed=1),
        sum(try_cast(bond_forfeited_raw AS HUGEINT)+try_cast(final_fee_forfeited_raw AS HUGEINT)) FILTER (WHERE disputed=1)
      FROM read_parquet('{request_panel.as_posix()}')
    """).fetchone()

    proposer_counts = requests.groupby("proposer").size()
    disputer_counts = disputed.groupby("disputer").size()
    voter_amounts = penalties.groupby("voter")["penalty_amount_uma"].sum()
    dvm_counts = penalties["negative_slash_class"].value_counts()
    dvm_amounts = penalties.groupby("negative_slash_class")["penalty_amount_uma"].sum()
    no_vote = int(dvm_counts.get("no_valid_reveal_slash", 0))
    wrong_vote = int(dvm_counts.get("wrong_vote_slash", 0))
    anomaly = int(dvm_counts.get("matching_reveal_negative_slash_anomaly", 0))
    grade_a = penalties[penalties["grade_a_polymarket"]]
    grade_no_vote = int((grade_a["negative_slash_class"] == "no_valid_reveal_slash").sum())
    no_vote_raw, wrong_vote_raw = connection.execute(f"""
      SELECT
        sum(try_cast(penalty_amount_raw AS HUGEINT)) FILTER (WHERE negative_slash_class='no_valid_reveal_slash'),
        sum(try_cast(penalty_amount_raw AS HUGEINT)) FILTER (WHERE negative_slash_class='wrong_vote_slash')
      FROM read_parquet('{dvm_panel.as_posix()}')
    """).fetchone()

    summary: dict[str, Any] = {
        "analysis_version": "2.0.0",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fixed_cutoff": FIXED_CUTOFF,
        "study_design": "observational_descriptive_and_associational_not_causal",
        "request_panel": {
            "settled": total,
            "disputed": dispute_count,
            "dispute_rate": dispute_count / total,
            "dispute_rate_ci_low": low,
            "dispute_rate_ci_high": high,
            "disputer_wins": disputer_wins,
            "disputer_win_rate": disputer_wins / dispute_count,
        },
        "request_groups": group_rows,
        "reward_and_bond": {
            "explicit_report_reward_raw": str(exact[0]),
            "explicit_report_reward_units": f"{int(exact[0]) / 1e6:.6f}".rstrip("0").rstrip("."),
            "dispute_winner_reward_raw": str(exact[1]),
            "dispute_winner_reward_units": f"{int(exact[1]) / 1e6:.6f}".rstrip("0").rstrip("."),
            "loser_bond_and_fee_raw": str(exact[2]),
            "loser_bond_and_fee_units": f"{int(exact[2]) / 1e6:.6f}".rstrip("0").rstrip("."),
            "undisputed_reward_to_bond_median": float(requests.loc[requests.disputed == 0, "reward_to_bond_ratio"].median()),
            "disputed_reward_to_bond_median": float(disputed["reward_to_bond_ratio"].median()),
            "disputed_bond_median_units": float(disputed["bond_units"].median()),
            "mean_dispute_winner_reward_units": int(exact[1]) / 1e6 / dispute_count,
            "mean_loser_bond_and_fee_units": int(exact[2]) / 1e6 / dispute_count,
        },
        "participant_concentration": {
            "proposer_rounds": concentration(proposer_counts),
            "disputer_events": concentration(disputer_counts),
            "dvm_negative_amount": concentration(voter_amounts),
        },
        "dvm_negative_slash": {
            "events": int(len(penalties)),
            "no_valid_reveal_events": no_vote,
            "wrong_vote_events": wrong_vote,
            "matching_reveal_negative_anomalies": anomaly,
            "no_valid_reveal_event_share": no_vote / len(penalties),
            "wrong_vote_event_share": wrong_vote / len(penalties),
            "no_valid_reveal_amount_raw": str(no_vote_raw),
            "wrong_vote_amount_raw": str(wrong_vote_raw),
            "no_valid_reveal_amount_uma": f"{int(no_vote_raw) / 1e18:.6f}",
            "wrong_vote_amount_uma": f"{int(wrong_vote_raw) / 1e18:.6f}",
            "no_valid_reveal_amount_share": float(dvm_amounts.get("no_valid_reveal_slash", 0) / penalties.penalty_amount_uma.sum()),
            "grade_a_polymarket_negative_events": int(len(grade_a)),
            "grade_a_no_valid_reveal_events": grade_no_vote,
            "grade_a_no_valid_reveal_event_share": grade_no_vote / len(grade_a),
        },
        "models": models,
        "interpretation_guards": [
            "Associations are not causal effects.",
            "accepted_undisputed is not external objective truth.",
            "wrong/no-vote uses only valid reveals in the final resolved DVM round.",
            "VoterSlashApplied is reconciliation-only and is excluded from monetary totals.",
            "USDC and USDC.e are reported as protocol units without assuming a constant fiat price.",
        ],
        "outputs": {
            "request_panel": str(request_panel),
            "dvm_negative_slash_panel": str(dvm_panel),
        },
    }

    analysis_dir.mkdir(parents=True, exist_ok=True)
    summary_path = analysis_dir / "uma_phase2_summary.json"
    coefficient_path = analysis_dir / "uma_phase2_model_coefficients.csv"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pd.DataFrame(coefficients).to_csv(coefficient_path, index=False)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(build_report(summary, coefficients), encoding="utf-8")

    manifest = {
        "dataset": "UMA phase-two fixed-cutoff statistical analysis",
        "analysis_version": "2.0.0",
        "fixed_cutoff": FIXED_CUTOFF,
        "generated_at_utc": summary["generated_at_utc"],
        "inputs": {name: {"path": str(path), "sha256": sha256_file(path)} for name, path in required.items()},
        "outputs": {
            "request_panel": {"path": str(request_panel), "rows": total, "sha256": sha256_file(request_panel)},
            "dvm_negative_slash_panel": {"path": str(dvm_panel), "rows": len(penalties), "sha256": sha256_file(dvm_panel)},
            "summary": {"path": str(summary_path), "sha256": sha256_file(summary_path)},
            "model_coefficients": {"path": str(coefficient_path), "sha256": sha256_file(coefficient_path)},
            "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        },
        "assertions": {
            "request_rows_match_primary_settled": total == 40534,
            "disputed_rows_match_phase1": dispute_count == 821,
            "dvm_negative_rows_match_ledger": len(penalties) == 947128,
            "negative_slash_classification_exhaustive": no_vote + wrong_vote + anomaly == len(penalties),
            "matching_final_reveal_negative_anomalies_zero": anomaly == 0,
            "both_models_completed": len(models) == 2 and len(coefficients) == 15,
        },
    }
    manifest["all_required_assertions_pass"] = all(manifest["assertions"].values())
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"summary": summary, "manifest": manifest}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-dir", type=Path, default=ROOT / "data/curated/parquet")
    parser.add_argument("--output-parquet-dir", type=Path, default=ROOT / "data/curated/parquet")
    parser.add_argument("--analysis-dir", type=Path, default=ROOT / "data/analysis")
    parser.add_argument("--report", type=Path, default=ROOT / "reports/uma_phase2_research.md")
    parser.add_argument("--manifest", type=Path, default=ROOT / "data/manifests/uma_phase2_analysis.json")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    result = analyze(
        arguments.parquet_dir.resolve(),
        arguments.output_parquet_dir.resolve(),
        arguments.analysis_dir.resolve(),
        arguments.report.resolve(),
        arguments.manifest.resolve(),
    )
    print(json.dumps(result["manifest"], ensure_ascii=False, indent=2, sort_keys=True))
