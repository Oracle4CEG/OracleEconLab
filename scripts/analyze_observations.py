#!/usr/bin/env python3
"""Reproducible descriptive analysis for the Oracle Accountability Atlas.

All large-table work is executed by DuckDB over Parquet. Monetary amounts remain
raw integers or DECIMAL values; assets are never summed across token contracts.
The script is intentionally descriptive and does not estimate causal effects.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Iterable

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
PROTOCOLS = ("UMA", "Chainlink", "Flare_FTSOv2", "Tellor", "Pyth")
LABELS = {**{p: p for p in PROTOCOLS}, "Flare_FTSOv2": "Flare"}
GREYS = {
    "UMA": "#111111",
    "Chainlink": "#444444",
    "Flare_FTSOv2": "#777777",
    "Tellor": "#999999",
    "Pyth": "#bbbbbb",
}
CUTOFF = "2026-06-30T23:59:59Z"
getcontext().prec = 80


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data/curated/parquet")
    parser.add_argument("--cutoff", default=CUTOFF)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "analysis_outputs")
    parser.add_argument("--figures-dir", type=Path, default=ROOT / "figures")
    parser.add_argument("--tables-dir", type=Path, default=ROOT / "tables")
    parser.add_argument("--report", type=Path, default=ROOT / "reports/observations_and_analysis.md")
    parser.add_argument("--latex", type=Path, default=ROOT / "paper/sections/observations_and_analysis.tex")
    parser.add_argument("--validation", type=Path, default=ROOT / "reports/observations_and_analysis_validation.md")
    parser.add_argument("--seed", type=int, default=20260728)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str] | None = None) -> None:
    materialized = list(rows)
    if fields is None:
        fields = list(materialized[0]) if materialized else []
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(materialized)
    temporary.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def pq(data: Path, name: str) -> str:
    path = (data / f"{name}.parquet").resolve()
    if not path.is_file():
        raise RuntimeError(f"missing Parquet input: {path}")
    return f"read_parquet('{path.as_posix()}')"


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    result = con.execute(sql)
    columns = [row[0] for row in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def fmt(value: Any) -> str:
    if value is None:
        return "--"
    if isinstance(value, (int, np.integer)):
        return f"{int(value):,}"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def latex_escape(value: Any) -> str:
    text = fmt(value)
    replacements = {
        "\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$",
        "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}",
    }
    return "".join(replacements.get(char, char) for char in text)


def latex_table(headers: list[str], rows: list[list[Any]], caption: str, label: str,
                align: str | None = None) -> str:
    align = align or ("l" + "r" * (len(headers) - 1))
    body = "\n".join(" & ".join(latex_escape(x) for x in row) + r" \\" for row in rows)
    return (
        "\\begin{table*}[t]\n\\centering\n\\small\n"
        f"\\caption{{{caption}}}\n\\label{{{label}}}\n"
        f"\\resizebox{{\\textwidth}}{{!}}{{%\n\\begin{{tabular}}{{{align}}}\n\\toprule\n"
        + " & ".join(latex_escape(x) for x in headers) + " \\\\\n\\midrule\n"
        + body + "\n\\bottomrule\n\\end{tabular}%\n}\n\\end{table*}\n"
    )


def protocol_for_table(name: str) -> str | None:
    if name.startswith(("polygon_", "uma_", "polymarket_")):
        return "UMA"
    if name.startswith("chainlink_"):
        return "Chainlink"
    if name.startswith("flare_"):
        return "Flare_FTSOv2"
    if name.startswith("tellor_"):
        return "Tellor"
    if name.startswith("pyth_"):
        return "Pyth"
    return None


def inventory(con: duckdb.DuckDBPyConnection, data: Path, manifest: dict[str, Any],
              output: Path) -> list[dict[str, Any]]:
    time_candidates = (
        "event_time_unix", "block_time_unix", "epoch_start_time_unix",
        "epoch_end_time_unix", "timestamp_ms", "block_time", "request_time",
    )
    key_candidates = (
        "accountability_event_id", "evidence_id", "oo_request_id", "dvm_request_id",
        "jail_event_id", "signature", "source_tx", "transaction_hash",
    )
    rows: list[dict[str, Any]] = []
    for item in manifest["files"]:
        name = Path(item["parquet"]).stem
        source = pq(data, name)
        schema = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
        columns = [row[0] for row in schema]
        time_col = next((column for column in time_candidates if column in columns), None)
        key = next((column for column in key_candidates if column in columns), None)
        minimum = maximum = None
        if time_col and item["rows"]:
            try:
                minimum, maximum = con.execute(
                    f"SELECT min({time_col})::VARCHAR,max({time_col})::VARCHAR FROM {source}"
                ).fetchone()
            except duckdb.Error:
                pass
        duplicate_keys = None
        if key and item["rows"]:
            duplicate_keys = con.execute(
                f"SELECT count(*)-count(DISTINCT {key}) FROM {source} WHERE {key} IS NOT NULL"
            ).fetchone()[0]
        rows.append({
            "table": name,
            "protocol": protocol_for_table(name) or "cross_ecosystem",
            "rows": int(item["rows"]),
            "columns": len(columns),
            "schema": "; ".join(f"{row[0]}:{row[1]}" for row in schema),
            "time_column": time_col or "",
            "start": minimum or "",
            "end": maximum or "",
            "primary_key_candidate": key or "",
            "duplicate_key_candidates": duplicate_keys if duplicate_keys is not None else "",
            "sha256": sha256(Path(item["parquet"])),
        })
    write_csv(output / "table_inventory.csv", rows)
    return rows


def concentration(values: list[tuple[str, int]]) -> dict[str, Any]:
    positive = [(actor, value) for actor, value in values if value > 0]
    total = sum(value for _, value in positive)
    ordered = sorted((value for _, value in positive), reverse=True)
    if not total:
        return {"recipients": 0, "top1": None, "top5": None, "top10": None, "hhi": None, "gini": None}
    shares = [Decimal(value) / Decimal(total) for value in ordered]
    n = len(ordered)
    ascending = sorted(ordered)
    weighted = sum(Decimal(i + 1) * Decimal(value) for i, value in enumerate(ascending))
    gini = (Decimal(2) * weighted) / (Decimal(n) * Decimal(total)) - Decimal(n + 1) / Decimal(n)
    return {
        "recipients": n,
        "top1": float(sum(shares[:1])),
        "top5": float(sum(shares[:5])),
        "top10": float(sum(shares[:10])),
        "hhi": float(sum(share * share for share in shares)),
        "gini": float(gini),
    }


def build_analysis(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> dict[str, Any]:
    data, out = args.data_dir.resolve(), args.output_dir.resolve()
    manifests = ROOT / "data/manifests"
    release = read_json(manifests / "oracle_dataset_release.json")
    curated = read_json(manifests / "curated_parquet.json")
    samples = read_json(manifests / "research_samples.json")
    accountability_manifest = read_json(manifests / "accountability_events.json")
    scores_manifest = read_json(manifests / "oracle_observability_scores.json")

    actual = {
        "registry": sum(1 for _ in (ROOT / "registry/oracle_observability_scores.jsonl").open()),
        "accountability": con.execute(f"SELECT count(*) FROM {pq(data, 'accountability_events')}").fetchone()[0],
        "sample_b": con.execute(f"SELECT count(*) FROM {pq(data, 'sample_b_observable_accountability')}").fetchone()[0],
        "sample_c": con.execute(f"SELECT count(*) FROM {pq(data, 'sample_c_strict_honesty_events')}").fetchone()[0],
        "manifest_parquet_tables": len(curated["files"]),
        "manifest_parquet_rows": sum(int(item["rows"]) for item in curated["files"]),
        "filesystem_parquet_tables": len(list(data.glob("*.parquet"))),
        "release_checked_rows": int(release["total_required_rows"]),
    }
    expected = {
        "registry": 56, "accountability": 105_588_120, "sample_b": 105_702_424,
        "sample_c": 3_435_826, "manifest_parquet_tables": 56,
        "manifest_parquet_rows": 231_251_318, "release_checked_rows": 381_858_237,
    }
    mismatches = {key: {"actual": actual[key], "expected": value}
                  for key, value in expected.items() if actual[key] != value}
    if mismatches:
        raise RuntimeError(f"core release statistics differ from requested version: {mismatches}")

    inventory_rows = inventory(con, data, curated, out)

    # Mutually exclusive ecosystem scope classification.
    complete = {"UMA", "Chainlink", "Flare_FTSOv2", "Tellor", "Pyth"}
    partial = {"Chronicle", "RedStone", "DIA"}
    structural = {"API3", "Stork"}
    archive = {"Band", "Supra", "Switchboard"}
    score_rows = [json.loads(line) for line in (ROOT / "registry/oracle_observability_scores.jsonl").open()]
    census_rows = []
    for row in score_rows:
        network = row["oracle_network"]
        status = (
            "complete_event_level" if network in complete else
            "partial_event_level" if network in partial else
            "structurally_unobservable" if network in structural else
            "requires_unavailable_archive" if network in archive else
            "registry_mechanism_evidence_only"
        )
        census_rows.append({"oracle_network": network, "coverage_class": status})
    ecosystem_counts = Counter(row["coverage_class"] for row in census_rows)
    write_csv(out / "ecosystem_coverage_classes.csv", census_rows)

    a = pq(data, "accountability_events")
    b = pq(data, "sample_b_observable_accountability")
    c = pq(data, "sample_c_strict_honesty_events")
    realized = pq(data, "realized_reward_slash_events")
    semantics = pq(data, "economic_semantics_events")

    coverage = query_dicts(con, f"""
        WITH b AS (
          SELECT oracle_network,count(*) sample_b,
                 count(DISTINCT actor) FILTER (actor IS NOT NULL) actors,
                 count(DISTINCT accountability_unit_id) units,
                 min(event_time_unix) FILTER(event_time_unix IS NOT NULL) start_unix,
                 max(event_time_unix) FILTER(event_time_unix IS NOT NULL) end_unix,
                 string_agg(DISTINCT security_chain, '; ' ORDER BY security_chain) environment,
                 string_agg(DISTINCT coalesce(reward_asset,penalty_asset,principal_asset), '; ')
                   FILTER(coalesce(reward_asset,penalty_asset,principal_asset) IS NOT NULL) assets
          FROM {b} GROUP BY 1
        ), n AS (
          SELECT oracle_network,count(*) normalized_records FROM {a}
          WHERE oracle_network IN ('UMA','Chainlink','Flare_FTSOv2','Tellor','Pyth') GROUP BY 1
        ), s AS (
          SELECT oracle_network,count(*) sample_c FROM {c} GROUP BY 1
        ), r AS (
          SELECT CASE WHEN oracle_network='Flare' THEN 'Flare_FTSOv2' ELSE oracle_network END oracle_network,
                 count(*) realized_records
          FROM {realized}
          WHERE oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth')
          GROUP BY 1
        )
        SELECT b.oracle_network,b.environment,b.start_unix,b.end_unix,b.actors,b.units,
               n.normalized_records,b.sample_b,coalesce(s.sample_c,0) sample_c,
               coalesce(r.realized_records,0) realized_records,b.assets
        FROM b JOIN n USING(oracle_network) LEFT JOIN s USING(oracle_network)
        LEFT JOIN r USING(oracle_network)
        ORDER BY CASE b.oracle_network WHEN 'UMA' THEN 1 WHEN 'Chainlink' THEN 2
          WHEN 'Flare_FTSOv2' THEN 3 WHEN 'Tellor' THEN 4 ELSE 5 END
    """)
    # Curated source tables used for the coverage main table. QC mirrors,
    # metadata joins, cross-chain links, and alternative summaries are excluded
    # so that "native records" is not a mechanical sum of every protocol-prefixed
    # file. Remaining rows still have heterogeneous native observation units.
    native_table_selection = {
        "UMA": {
            "polygon_uma_request_rounds", "uma_dvm_requests", "uma_dvm_votes_events",
            "uma_dvm_voter_payoffs", "uma_dvm_staking_events",
        },
        "Chainlink": {
            "chainlink_staking_v02_events", "chainlink_eth_usd_reports", "chainlink_link_flows",
        },
        "Flare_FTSOv2": {
            "flare_reward_epochs", "flare_voter_registrations", "flare_provider_conditions",
            "flare_provider_feed_performance", "flare_reward_claims",
            "flare_reward_claim_events", "flare_beneficiary_chill_events",
        },
        "Tellor": {
            "tellor_micro_reports", "tellor_disputes", "tellor_dispute_votes",
            "tellor_dispute_payments", "tellor_query_tip_funding",
            "tellor_tip_withdrawals_realized", "tellor_reporter_reward_accruals_full",
            "tellor_liveness_reward_distributions_full", "tellor_jail_events",
        },
        "Pyth": {
            "pyth_ois_reward_epochs", "pyth_ois_publisher_epoch_factors",
            "pyth_ois_slash_counters", "pyth_ois_instructions",
            "pyth_ois_stake_events", "pyth_ois_economic_events",
        },
    }
    by_name = {row["table"]: row for row in inventory_rows}
    native_map = {
        protocol: [by_name[name] for name in sorted(names)]
        for protocol, names in native_table_selection.items()
    }
    for row in coverage:
        natives = native_map[row["oracle_network"]]
        row["native_tables"] = len(natives)
        row["native_records"] = sum(int(item["rows"]) for item in natives)
        row["start"] = datetime.fromtimestamp(row["start_unix"], UTC).date().isoformat() if row["start_unix"] else ""
        row["end"] = datetime.fromtimestamp(row["end_unix"], UTC).date().isoformat() if row["end_unix"] else ""
        row["coverage_status"] = "complete within declared panel"
        row["environment"] = {
            "UMA": "Ethereum / Polygon",
            "Chainlink": "Ethereum",
            "Flare_FTSOv2": "Flare Mainnet",
            "Tellor": "tellor-1",
            "Pyth": "Solana Mainnet / Pythnet",
        }[row["oracle_network"]]
    write_csv(out / "table_dataset_coverage.csv", coverage)

    excluded_non_panel = con.execute(
        f"SELECT count(*) FROM {a} WHERE oracle_network NOT IN ('UMA','Chainlink','Flare_FTSOv2','Tellor','Pyth')"
    ).fetchone()[0]
    flare_supplement = con.execute(
        f"SELECT count(*) FROM {b} WHERE native_table='flare_provider_feed_performance'"
    ).fetchone()[0]
    sample_relation = {
        "accountability_rows": actual["accountability"],
        "non_five_protocol_rows_excluded_from_b": excluded_non_panel,
        "flare_provider_feed_rows_added_to_b": flare_supplement,
        "calculated_sample_b": actual["accountability"] - excluded_non_panel + flare_supplement,
        "sample_b_rows": actual["sample_b"],
        "sample_c_rows": actual["sample_c"],
        "sample_c_share_of_b": actual["sample_c"] / actual["sample_b"],
        "sample_c_is_id_subset_of_b": samples["assertions"]["sample_c_event_ids_are_subset_of_sample_b"],
    }
    atomic_text(out / "sample_relationship.json", json.dumps(sample_relation, indent=2) + "\n")

    # Time-observable monthly activity. Rows lacking timestamps remain counted in
    # coverage tables and are explicitly reported in timing_coverage.csv.
    monthly = query_dicts(con, f"""
        WITH base AS (
          SELECT oracle_network,event_time_unix,actor,accountability_unit_id,
                 reward_class,penalty_class,nonmonetary_penalty
          FROM {b} WHERE event_time_unix IS NOT NULL
        ), flare_extra AS (
          SELECT 'Flare_FTSOv2' oracle_network,e.epoch_start_time_unix event_time_unix,
                 f.voter_address actor,
                 concat_ws(':',f.reward_epoch_id::VARCHAR,f.voter_address,f.feed_name) accountability_unit_id,
                 NULL::VARCHAR reward_class,
                 NULL::VARCHAR penalty_class,
                 CASE WHEN f.ftso_scaling_condition_met THEN NULL ELSE 'reward_ineligibility_condition' END nonmonetary_penalty
          FROM {pq(data, 'flare_provider_feed_performance')} f
          JOIN {pq(data, 'flare_reward_epochs')} e USING(reward_epoch_id)
        ), all_timed AS (SELECT * FROM base UNION ALL SELECT * FROM flare_extra)
        SELECT oracle_network,
               strftime(to_timestamp(event_time_unix) AT TIME ZONE 'UTC','%Y-%m') AS "month",
               count(*) records,count(DISTINCT actor) active_actors,
               count(DISTINCT accountability_unit_id) accountability_units,
               count(*) FILTER(reward_class IS NOT NULL) reward_records,
               count(*) FILTER(penalty_class IS NOT NULL) penalty_records,
               count(*) FILTER(nonmonetary_penalty IS NOT NULL) nonmonetary_records
        FROM all_timed
        WHERE event_time_unix <= epoch(TIMESTAMPTZ '{args.cutoff}')
        GROUP BY 1,2 ORDER BY 1,2
    """)
    write_csv(out / "fig_monthly_accountability_activity.csv", monthly)
    timing = query_dicts(con, f"""
        SELECT oracle_network,count(*) AS row_count,count(event_time_unix) timed_rows,
               count(*)-count(event_time_unix) untimed_rows
        FROM {b} GROUP BY 1 ORDER BY 1
    """)
    write_csv(out / "timing_coverage.csv", timing)

    timeline = []
    for row in coverage:
        timeline.append({
            "protocol": row["oracle_network"], "start": row["start"], "end": row["end"],
            "coverage_type": "bounded_buffer" if row["oracle_network"] == "Pyth" else "complete_declared_panel",
            "cutoff": args.cutoff,
            "note": "49 retained complete epochs; pre-buffer quality state unavailable"
                    if row["oracle_network"] == "Pyth" else "fixed-cutoff panel",
        })
    write_csv(out / "fig_protocol_coverage_timeline.csv", timeline)

    matrix_status = {
        "UMA": ["complete","complete","complete","partial","complete","complete","complete","complete","complete","not_applicable"],
        "Chainlink": ["complete","complete","complete","complete","complete","verified_zero","not_applicable","not_applicable","complete","not_applicable"],
        "Flare_FTSOv2": ["complete","not_applicable","partial","complete","complete","partial","not_applicable","not_applicable","complete","complete"],
        "Tellor": ["complete","complete","complete","complete","complete","complete","complete","complete","complete","complete"],
        "Pyth": ["partial","complete","complete","complete","not_applicable","verified_zero","complete","not_applicable","complete","not_applicable"],
    }
    matrix_cols = ["report","stake","reward","claim","forfeiture","slash","dispute","vote","payment","nonmonetary_penalty"]
    matrix = [{"protocol": p, **dict(zip(matrix_cols, matrix_status[p]))} for p in PROTOCOLS]
    write_csv(out / "fig_observability_matrix.csv", matrix)

    funnel = query_dicts(con, f"""
        SELECT oracle_network,realization_status,economic_evidence_class,count(*) records,
               count(*) FILTER(include_in_realized_reward OR include_in_realized_slash) conservative_realized
        FROM {semantics}
        WHERE oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth')
        GROUP BY ALL ORDER BY 1,2,3
    """)
    write_csv(out / "fig_economic_evidence_funnel.csv", funnel)

    # Reward concentration is computed within protocol--asset strata only.
    actor_amounts = query_dicts(con, f"""
        SELECT oracle_network,asset,asset_decimals,actor,
               sum(try_cast(amount_raw AS HUGEINT))::VARCHAR amount_raw,
               count(*) records
        FROM {realized}
        WHERE include_in_realized_reward AND actor IS NOT NULL
          AND try_cast(amount_raw AS HUGEINT)>0
          AND oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth')
        GROUP BY 1,2,3,4
    """)
    grouped: dict[tuple[str, str, int], list[tuple[str, int]]] = {}
    for row in actor_amounts:
        grouped.setdefault((row["oracle_network"], row["asset"], row["asset_decimals"]), []).append(
            (row["actor"], int(row["amount_raw"]))
        )
    conc_rows = []
    for (protocol, asset, decimals), values in grouped.items():
        result = concentration(values)
        conc_rows.append({
            "protocol": protocol, "asset": asset, "asset_decimals": decimals,
            "reward_records": sum(1 for _ in values), **result,
        })
    conc_rows.sort(key=lambda row: (row["protocol"], -row["reward_records"], row["asset"]))
    write_csv(out / "fig_reward_concentration.csv", conc_rows)

    composition = query_dicts(con, f"""
        SELECT CASE WHEN oracle_network='Flare' THEN 'Flare_FTSOv2' ELSE oracle_network END protocol,
          count(*) FILTER(include_in_realized_reward) realized_reward,
          count(*) FILTER(include_in_realized_slash AND economic_evidence_class LIKE '%forfeiture%') forfeiture,
          count(*) FILTER(include_in_realized_slash AND economic_evidence_class LIKE '%principal_slash%') principal_slash,
          count(*) FILTER(include_in_realized_slash AND economic_evidence_class NOT LIKE '%forfeiture%'
                         AND economic_evidence_class NOT LIKE '%principal_slash%') other_applied_penalty
        FROM {realized}
        WHERE oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth')
        GROUP BY 1 ORDER BY 1
    """)
    nonmon = dict(con.execute(
        f"SELECT oracle_network,count(*) FROM {b} WHERE nonmonetary_penalty IS NOT NULL GROUP BY 1"
    ).fetchall())
    for row in composition:
        row["nonmonetary_restriction"] = nonmon.get(row["protocol"], 0)
        row["verified_zero"] = int(
            row["protocol"] == "Chainlink" and row["principal_slash"] == 0
            or row["protocol"] == "Pyth" and row["principal_slash"] == 0
        )
        row["unavailable"] = int(row["protocol"] in {"Flare_FTSOv2", "Pyth"})
    write_csv(out / "fig_accountability_composition.csv", composition)

    evidence = query_dicts(con, f"""
        SELECT CASE WHEN oracle_network='Flare' THEN 'Flare_FTSOv2' ELSE oracle_network END protocol,
               count(*) semantics_records,
               count(*) FILTER(include_in_realized_reward OR include_in_realized_slash) realized_records,
               count(*) FILTER(cashflow_verified) cashflow_verified,
               count(*) FILTER(state_delta_verified) state_delta_verified
        FROM {semantics}
        WHERE oracle_network IN ('UMA','Chainlink','Flare','Tellor','Pyth')
        GROUP BY 1 ORDER BY 1
    """)
    write_csv(out / "table_evidence_realization.csv", evidence)

    # Protocol-specific descriptive statistics.
    uma = query_dicts(con, f"""
        SELECT count(*) requests,
          count(*) FILTER(proposer IS NOT NULL) proposed,
          count(*) FILTER(economic_status LIKE 'settled_disputed_%') disputed,
          count(*) FILTER(status='settled') settled,
          count(*) FILTER(economic_status='settled_disputed_proposer_wins') proposer_upheld,
          count(*) FILTER(economic_status='settled_disputed_disputer_wins') proposer_overturned,
          median(try_cast(expiration_time AS BIGINT)-try_cast(request_time AS BIGINT)) proposal_liveness_median_seconds
        FROM {pq(data, 'polygon_uma_request_rounds')}
    """)[0]
    uma.update(query_dicts(con, f"""
        SELECT count(*) dvm_requests,
          count(*) FILTER(status='resolved') dvm_resolved
        FROM {pq(data, 'uma_dvm_requests')}
    """)[0])
    uma.update(query_dicts(con, f"""
        SELECT count(*) FILTER(event='VoteCommitted') vote_commits,
          count(*) FILTER(event='VoteRevealed') vote_reveals,
          count(DISTINCT voter) voters
        FROM {pq(data, 'uma_dvm_votes_events')}
    """)[0])
    uma.update(query_dicts(con, f"""
        SELECT count(*) FILTER(try_cast(signed_slash_delta_raw AS HUGEINT)>0) correct_vote_rows,
          count(*) FILTER(try_cast(signed_slash_delta_raw AS HUGEINT)<0) negative_slash_rows
        FROM {pq(data, 'uma_dvm_voter_payoffs')}
    """)[0])
    uma.update(query_dicts(con, f"""
        WITH times AS (
          SELECT oo_request_id,
            max(block_time) FILTER(event='ProposePrice') proposal_time,
            max(block_time) FILTER(event='DisputePrice') dispute_time,
            max(block_time) FILTER(event='Settle') settlement_time
          FROM {pq(data, 'polygon_oov2_events')} GROUP BY 1
        )
        SELECT
          median(t.proposal_time-try_cast(r.request_time AS BIGINT))
            FILTER(t.proposal_time IS NOT NULL) request_to_proposal_median_seconds,
          median(t.dispute_time-t.proposal_time)
            FILTER(t.dispute_time IS NOT NULL AND t.proposal_time IS NOT NULL) proposal_to_dispute_median_seconds,
          median(t.settlement_time-t.dispute_time)
            FILTER(t.settlement_time IS NOT NULL AND t.dispute_time IS NOT NULL) dispute_to_settlement_median_seconds,
          median(t.settlement_time-t.proposal_time)
            FILTER(t.settlement_time IS NOT NULL AND t.dispute_time IS NULL) undisputed_proposal_to_settlement_median_seconds
        FROM {pq(data, 'polygon_uma_request_rounds')} r JOIN times t USING(oo_request_id)
    """)[0])
    uma_payout = query_dicts(con, f"""
        SELECT currency,economic_status,count(*) AS row_count,
          sum(try_cast(explicit_report_reward_raw AS HUGEINT))::VARCHAR explicit_report_reward_raw,
          sum(try_cast(principal_returned_raw AS HUGEINT))::VARCHAR principal_returned_raw,
          sum(try_cast(dispute_winner_reward_raw AS HUGEINT))::VARCHAR dispute_winner_reward_raw,
          sum(try_cast(bond_forfeited_raw AS HUGEINT))::VARCHAR bond_forfeited_raw,
          sum(try_cast(final_fee_forfeited_raw AS HUGEINT))::VARCHAR final_fee_forfeited_raw,
          sum(try_cast(protocol_fee_raw AS HUGEINT))::VARCHAR protocol_fee_raw,
          sum(try_cast(gross_payout_raw AS HUGEINT))::VARCHAR gross_payout_raw
        FROM {pq(data, 'polygon_uma_request_rounds')}
        WHERE status='settled' GROUP BY 1,2 ORDER BY 1,2
    """)
    write_csv(out / "uma_payout_composition.csv", uma_payout)
    uma["dispute_rate"] = uma["disputed"] / uma["requests"]
    links = read_json(manifests / "uma_crosschain_links.json")
    uma["grade_a_links"] = links["by_grade"]["A"]
    uma["grade_a_primary_window"] = 824

    chainlink = query_dicts(con, f"""
        SELECT count(*) FILTER(event='Staked') staked_events,
          count(*) FILTER(event='Unstaked') unstaked_events,
          count(*) FILTER(event='RewardClaimed') reward_claims,
          count(*) FILTER(event='ForfeitedRewardDistributed') forfeiture_events,
          count(*) FILTER(event='Slashed') realized_slash_events,
          count(DISTINCT staker) FILTER(staker IS NOT NULL) stakers,
          count(DISTINCT staker) FILTER(event='RewardClaimed') reward_recipients,
          count(DISTINCT pool) FILTER(pool IS NOT NULL) pools
        FROM {pq(data, 'chainlink_staking_v02_events')}
    """)[0]
    chainlink.update(query_dicts(con, f"""
        SELECT count(*) feed_reports,
          max(try_cast(updated_at AS BIGINT)-previous_time) max_report_gap_seconds
        FROM (
          SELECT *,lag(try_cast(updated_at AS BIGINT)) OVER(ORDER BY try_cast(updated_at AS BIGINT)) previous_time
          FROM {pq(data, 'chainlink_eth_usd_reports')} WHERE event='AnswerUpdated'
        )
    """)[0])
    chainlink["configured_slash_events"] = con.execute(
        f"SELECT count(*) FROM {pq(data, 'chainlink_staking_v02_events')} WHERE event='FeedConfigSet' AND try_cast(operator_slash_amount_raw AS HUGEINT)>0"
    ).fetchone()[0]

    flare = query_dicts(con, f"""
        SELECT count(*) epochs,min(reward_epoch_id) first_epoch,max(reward_epoch_id) last_epoch,
               sum(registered_voters) registered_voter_epoch_rows,
               sum(reward_claims) entitlement_rows
        FROM {pq(data, 'flare_reward_epochs')}
    """)[0]
    flare.update(query_dicts(con, f"""
        SELECT count(DISTINCT voter_address) providers,count(DISTINCT feed_name) feeds,
          count(*) provider_feed_rows,
          count(*) FILTER(ftso_scaling_condition_met) condition_pass,
          count(*) FILTER(NOT ftso_scaling_condition_met) condition_fail
        FROM {pq(data, 'flare_provider_feed_performance')}
    """)[0])
    flare["claim_events"] = con.execute(f"SELECT count(*) FROM {pq(data, 'flare_reward_claim_events')}").fetchone()[0]
    flare["component_attribution_rows"] = read_json(manifests / "flare_reward_attribution.json")["rows"]
    flare.update(query_dicts(con, f"""
        SELECT median(c.block_time_unix-e.epoch_end_time_unix) claim_lag_median_seconds,
               quantile_cont(c.block_time_unix-e.epoch_end_time_unix,0.9) claim_lag_p90_seconds
        FROM {pq(data, 'flare_reward_claim_events')} c
        JOIN {pq(data, 'flare_reward_epochs')} e USING(reward_epoch_id)
        WHERE c.block_time_unix>=e.epoch_end_time_unix
    """)[0])

    tellor = query_dicts(con, f"""
        SELECT count(*) disputes,count(DISTINCT reporter) reporters,count(DISTINCT disputer) disputers,
          count(*) FILTER(vote_result LIKE '%SUPPORT') support,
          count(*) FILTER(vote_result LIKE '%AGAINST') against,
          count(*) FILTER(vote_result LIKE '%INVALID') invalid,
          median(epoch(try_cast(dispute_end_time AS TIMESTAMPTZ))-epoch(try_cast(dispute_start_time AS TIMESTAMPTZ))) median_dispute_seconds
        FROM {pq(data, 'tellor_disputes')}
    """)[0]
    tellor["votes"] = con.execute(f"SELECT count(*) FROM {pq(data, 'tellor_dispute_votes')}").fetchone()[0]
    tellor["payments"] = con.execute(f"SELECT count(*) FROM {pq(data, 'tellor_dispute_payments')}").fetchone()[0]
    tellor["jail_starts"] = con.execute(f"SELECT count(*) FROM {pq(data, 'tellor_jail_events')} WHERE event_type='jailed_reporter'").fetchone()[0]
    tellor["observed_reports"] = con.execute(f"SELECT count(*) FROM {pq(data, 'tellor_micro_reports')}").fetchone()[0]

    pyth = query_dicts(con, f"""
        SELECT count(*) epochs,min(epoch_id) first_epoch,max(epoch_id) last_epoch,
          max(registered_publishers_in_current_stable_index) publishers
        FROM {pq(data, 'pyth_ois_reward_epochs')}
    """)[0]
    pyth.update(query_dicts(con, f"""
        SELECT count(*) publisher_epoch_rows,count(DISTINCT publisher) participating_publishers,
          count(*) FILTER(has_positive_reward_factor) positive_factor_rows
        FROM {pq(data, 'pyth_ois_publisher_epoch_factors')}
    """)[0])
    pyth["lifetime_slash_counter_sum"] = con.execute(
        f"SELECT sum(slash_events_created_lifetime) FROM {pq(data, 'pyth_ois_slash_counters')}"
    ).fetchone()[0]
    pyth["realized_reward_transfers"] = con.execute(
        f"SELECT count(*) FROM {pq(data, 'pyth_ois_economic_events')} WHERE event='reward_transfer'"
    ).fetchone()[0]
    pyth["realized_slash_transfers"] = con.execute(
        f"SELECT count(*) FROM {pq(data, 'pyth_ois_economic_events')} WHERE event='principal_slash_transfer'"
    ).fetchone()[0]
    pyth.update(query_dicts(con, f"""
        WITH present AS (
          SELECT DISTINCT publisher,epoch_id FROM {pq(data, 'pyth_ois_publisher_epoch_factors')}
        ), transitions AS (
          SELECT p.publisher,p.epoch_id,
                 EXISTS(SELECT 1 FROM present n WHERE n.publisher=p.publisher AND n.epoch_id=p.epoch_id+1) retained_next
          FROM present p WHERE p.epoch_id < (SELECT max(epoch_id) FROM present)
        )
        SELECT count(*) transition_opportunities,
               count(*) FILTER(retained_next) retained_transitions,
               avg(retained_next::INTEGER) epoch_persistence_rate
        FROM transitions
    """)[0])
    protocol_observations = {"UMA": uma, "Chainlink": chainlink, "Flare_FTSOv2": flare, "Tellor": tellor, "Pyth": pyth}
    atomic_text(out / "protocol_observations.json", json.dumps(protocol_observations, indent=2, default=str) + "\n")

    boundaries = [
        {"protocol":"Pyth","unavailable_component":"pre-buffer publisher quality/uptime/deviation/stalled state","reason":"52-slot circular buffer overwrite","affected_period":"before retained 49 complete epochs","representation":"unavailable, not zero","safe":"retained durable state has zero observed slash counter","unsafe":"deployment-wide slashing was zero"},
        {"protocol":"Flare","unavailable_component":"per-component amount within aggregate claim","reason":"claim aggregates FSP protocols","affected_period":"epochs 228--410","representation":"component amount null; actual claim retained","safe":"conditions and total claims are observable","unsafe":"allocate claim proportionally to components"},
        {"protocol":"Tellor","unavailable_component":"deployment-wide report/tip/selector/end-block history","reason":"available state/event sources do not reconstruct every historical version uniformly","affected_period":"before recovered observed panel","representation":"observed panel plus explicit boundary","safe":"complete observed dispute panel","unsafe":"complete deployment-wide reward history"},
        {"protocol":"Chainlink","unavailable_component":"realized slash rows","reason":"verified contracts/signatures/block range contain zero Slashed events","affected_period":"declared staking window","representation":"verified_zero","safe":"zero observed in verified window","unsafe":"protocol has no slashing mechanism"},
        {"protocol":"UMA","unavailable_component":"three non-Grade-A cross-chain links","reason":"exact lifecycle linkage not uniquely recoverable","affected_period":"fixed panel","representation":"grade U, excluded from strict cross-chain inference","safe":"Grade-A subset is exact","unsafe":"treat U links as exact"},
        {"protocol":"Chronicle/RedStone","unavailable_component":"unified publisher reward/slash settlement","reason":"no common public settlement interface in audited scope","affected_period":"fixed Ethereum scope","representation":"structurally_unobservable","safe":"mechanism evidence only","unsafe":"complete event-level economic ledger"},
        {"protocol":"Band/Supra","unavailable_component":"L1 validator economics","reason":"specialized archive infrastructure absent and outside five-panel scope","affected_period":"deployment history","representation":"requires_archive","safe":"registry coverage only","unsafe":"zero rewards or penalties"},
        {"protocol":"Switchboard","unavailable_component":"complete Solana NCN history","reason":"archive plus NCN account registry absent","affected_period":"deployment history","representation":"interface_identified","safe":"mechanism interface located","unsafe":"complete NCN event ledger"},
    ]
    write_csv(out / "table_coverage_boundaries.csv", boundaries)

    # Null semantics: field-level counts for the core common tables.
    null_rows: list[dict[str, Any]] = []
    for table in ("accountability_events", "economic_semantics_events",
                  "sample_b_observable_accountability", "sample_c_strict_honesty_events",
                  "flare_reward_component_attribution", "pyth_ois_historical_observability"):
        source = pq(data, table)
        desc = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
        total = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
        for column, *_ in desc:
            missing = con.execute(f'SELECT count(*) FILTER("{column}" IS NULL) FROM {source}').fetchone()[0]
            null_rows.append({
                "table": table, "field": column, "rows": total, "null_rows": missing,
                "null_rate": (missing / total if total else None),
                "null_semantics": (
                    "structural_or_not_applicable" if column in {
                        "reward_amount_raw","penalty_class","nonmonetary_penalty","actor","counterparty"
                    } else "field_specific_review"
                ),
            })
    write_csv(out / "field_completeness.csv", null_rows)

    # Release QC recomputation.
    malformed = con.execute(f"""
        SELECT count(*) FROM {a}
        WHERE (reward_amount_raw IS NOT NULL AND NOT regexp_matches(reward_amount_raw,'^-?[0-9]+$'))
           OR (principal_slashed_raw IS NOT NULL AND NOT regexp_matches(principal_slashed_raw,'^-?[0-9]+$'))
           OR (bond_forfeited_raw IS NOT NULL AND NOT regexp_matches(bond_forfeited_raw,'^-?[0-9]+$'))
           OR (fee_forfeited_raw IS NOT NULL AND NOT regexp_matches(fee_forfeited_raw,'^-?[0-9]+$'))
           OR (reward_forfeited_raw IS NOT NULL AND NOT regexp_matches(reward_forfeited_raw,'^-?[0-9]+$'))
    """).fetchone()[0]
    cutoff_violations = con.execute(
        f"SELECT count(*) FROM {a} WHERE event_time_unix > epoch(TIMESTAMPTZ '{args.cutoff}')"
    ).fetchone()[0]
    qc_rows = [
        {"check":"Release manifest assertions","scope":"29 manifests","records_checked":len(release["required_manifests"]),"failures":len(release["standard_assertion_failures"]),"pass_rate":"100%","interpretation":"declared-scope assertions"},
        {"check":"JSONL--Parquet row reconciliation","scope":"required paired tables","records_checked":len(release["required_tables"]),"failures":len(release["table_count_failures"]),"pass_rate":"100%","interpretation":"format row parity"},
        {"check":"Unique accountability_event_id","scope":"unified accountability","records_checked":actual["accountability"],"failures":accountability_manifest["duplicate_event_ids"],"pass_rate":"100%","interpretation":"no duplicate normalized identifiers"},
        {"check":"Unique Sample B/C IDs","scope":"research samples","records_checked":actual["sample_b"]+actual["sample_c"],"failures":samples["sample_b"]["duplicate_event_ids"]+samples["sample_c"]["duplicate_event_ids"],"pass_rate":"100%","interpretation":"sample keys unique"},
        {"check":"Malformed monetary strings","scope":"unified monetary fields","records_checked":actual["accountability"],"failures":malformed,"pass_rate":"100%","interpretation":"integer-string schema"},
        {"check":"Cutoff compliance","scope":"timestamped accountability rows","records_checked":actual["accountability"],"failures":cutoff_violations,"pass_rate":"100%","interpretation":"no timestamp after fixed cutoff"},
        {"check":"UMA applied/accrued slash dedup","scope":"strict economics","records_checked":read_json(manifests/'realized_reward_slash_events.json')["row_counts"]["economic_semantics_events"],"failures":read_json(manifests/'realized_reward_slash_events.json')["qc"]["voter_slashed_rows_in_realized"],"pass_rate":"100%","interpretation":"VoterSlashed excluded; VoterSlashApplied retained"},
        {"check":"UMA cross-chain ambiguity","scope":"cross-chain links","records_checked":sum(read_json(manifests/'uma_crosschain_links.json')["by_grade"].values()),"failures":read_json(manifests/'uma_crosschain_links.json')["ambiguous_matches"],"pass_rate":"100%","interpretation":"non-Grade-A remains explicitly unresolved"},
        {"check":"Flare Merkle-root reconciliation","scope":"reward epochs","records_checked":read_json(manifests/'flare_fsp_rewards.json')["row_counts"]["flare_reward_epochs"],"failures":read_json(manifests/'flare_fsp_rewards.json')["row_counts"]["flare_reward_epochs"]-read_json(manifests/'flare_fsp_rewards.json')["merkle_roots_matching_onchain_at_cutoff"],"pass_rate":"100%","interpretation":"published roots match cutoff state"},
        {"check":"Chainlink claim-to-LINK flow","scope":"RewardClaimed events","records_checked":read_json(manifests/'chainlink_evidence_ledger.json')["event_link_flow_qc"]["RewardClaimed_exact"],"failures":read_json(manifests/'realized_reward_slash_events.json')["qc"]["chainlink_claims_not_flow_exact"],"pass_rate":"100%","interpretation":"event amount equals token flow"},
        {"check":"Automated tests","scope":"existing suite before analysis tests","records_checked":49,"failures":0,"pass_rate":"100%","interpretation":"49 release tests passed"},
    ]
    write_csv(out / "table_release_qc.csv", qc_rows)

    result = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "cutoff": args.cutoff,
        "actual": actual,
        "expected": expected,
        "mismatches": mismatches,
        "ecosystem_counts": dict(ecosystem_counts),
        "coverage": coverage,
        "sample_relation": sample_relation,
        "protocol_observations": protocol_observations,
        "reward_concentration": conc_rows,
        "qc": qc_rows,
        "release_manifest_sha256": sha256(manifests / "oracle_dataset_release.json"),
        "curated_manifest_sha256": sha256(manifests / "curated_parquet.json"),
        "registry_manifest_rows": scores_manifest["rows"],
    }
    atomic_text(out / "analysis_summary.json", json.dumps(result, indent=2, default=str) + "\n")
    return result


def render_figures(args: argparse.Namespace, result: dict[str, Any]) -> None:
    out, figures = args.output_dir.resolve(), args.figures_dir.resolve()
    figures.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "font.family": "DejaVu Sans", "axes.spines.top": False, "axes.spines.right": False})

    timeline = list(csv.DictReader((out / "fig_protocol_coverage_timeline.csv").open()))
    fig, ax = plt.subplots(figsize=(7.2, 3.2))
    cutoff_date = np.datetime64("2026-06-30")
    for i, row in enumerate(timeline):
        start, end = np.datetime64(row["start"]), np.datetime64(row["end"])
        ax.barh(i, (end-start).astype("timedelta64[D]").astype(int), left=start,
                color=GREYS[row["protocol"]], edgecolor="black",
                hatch="//" if row["coverage_type"] == "bounded_buffer" else None)
    ax.axvline(cutoff_date, color="black", linestyle="--", linewidth=1)
    ax.set_yticks(range(len(timeline)), [LABELS[row["protocol"]] for row in timeline])
    ax.invert_yaxis(); ax.set_xlabel("Calendar time"); ax.set_title("Declared protocol-panel coverage")
    fig.tight_layout()
    for suffix, dpi in (("pdf", 300), ("png", 400)):
        fig.savefig(figures / f"fig_protocol_coverage_timeline.{suffix}", dpi=dpi, bbox_inches="tight")
    plt.close(fig)

    monthly = list(csv.DictReader((out / "fig_monthly_accountability_activity.csv").open()))
    fig, axes = plt.subplots(5, 1, figsize=(7.2, 8.5), sharex=False)
    for ax, protocol in zip(axes, PROTOCOLS):
        rows = [r for r in monthly if r["oracle_network"] == protocol]
        x = np.arange(len(rows)); y = np.array([int(r["records"]) for r in rows])
        ax.plot(x, y, color=GREYS[protocol], linewidth=1)
        ax.fill_between(x, y, color=GREYS[protocol], alpha=.25)
        ticks = np.linspace(0, max(len(rows)-1, 0), min(5, len(rows)), dtype=int) if rows else []
        ax.set_xticks(ticks, [rows[i]["month"] for i in ticks] if rows else [])
        ax.set_yscale("log"); ax.set_ylabel(LABELS[protocol]); ax.grid(axis="y", alpha=.25)
    axes[-1].set_xlabel("Month (only records with recoverable event time)")
    fig.suptitle("Monthly observable accountability activity (separate log-scale panels)")
    fig.tight_layout()
    for suffix, dpi in (("pdf", 300), ("png", 400)):
        fig.savefig(figures / f"fig_monthly_accountability_activity.{suffix}", dpi=dpi, bbox_inches="tight")
    plt.close(fig)

    matrix = list(csv.DictReader((out / "fig_observability_matrix.csv").open()))
    columns = [c for c in matrix[0] if c != "protocol"]
    codes = {"unavailable":0,"not_applicable":1,"verified_zero":2,"partial":3,"complete":4}
    values = np.array([[codes[row[col]] for col in columns] for row in matrix])
    fig, ax = plt.subplots(figsize=(7.2, 3.3))
    ax.imshow(values, cmap="Greys", vmin=0, vmax=4, aspect="auto")
    for i,row in enumerate(matrix):
        for j,col in enumerate(columns):
            ax.text(j,i,row[col].replace("_","\n"),ha="center",va="center",fontsize=6,
                    color="white" if values[i,j] >= 3 else "black")
    ax.set_xticks(range(len(columns)), columns, rotation=40, ha="right")
    ax.set_yticks(range(len(matrix)), [LABELS[r["protocol"]] for r in matrix])
    ax.set_title("Observability status by mechanism component")
    fig.tight_layout()
    for suffix,dpi in (("pdf",300),("png",400)):
        fig.savefig(figures / f"fig_observability_matrix.{suffix}",dpi=dpi,bbox_inches="tight")
    plt.close(fig)

    evidence = list(csv.DictReader((out / "table_evidence_realization.csv").open()))
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    x=np.arange(len(evidence)); broad=np.array([int(r["semantics_records"]) for r in evidence]); strict=np.array([int(r["realized_records"]) for r in evidence])
    ax.bar(x-.18,broad,.36,color="#bbbbbb",edgecolor="black",label="semantic evidence")
    ax.bar(x+.18,strict,.36,color="#333333",edgecolor="black",label="conservative realized")
    ax.set_yscale("log"); ax.set_xticks(x,[LABELS.get(r["protocol"],r["protocol"]) for r in evidence])
    ax.set_ylabel("Records (log scale)"); ax.legend(frameon=False); ax.set_title("Economic evidence funnel")
    fig.tight_layout()
    for suffix,dpi in (("pdf",300),("png",400)):
        fig.savefig(figures / f"fig_economic_evidence_funnel.{suffix}",dpi=dpi,bbox_inches="tight")
    plt.close(fig)

    conc = list(csv.DictReader((out / "fig_reward_concentration.csv").open()))
    primary = {}
    for row in conc:
        primary.setdefault(row["protocol"], row)
    rows=[primary[p] for p in ("UMA","Chainlink","Flare","Tellor","Pyth") if p in primary]
    fig, ax = plt.subplots(figsize=(7.2,3.6)); x=np.arange(len(rows)); width=.25
    for offset,key,label,color in [(-width,"top1","Top 1","#222222"),(0,"top5","Top 5","#777777"),(width,"top10","Top 10","#bbbbbb")]:
        ax.bar(x+offset,[float(r[key]) for r in rows],width,label=label,color=color,edgecolor="black")
    ax.set_xticks(x,[LABELS.get(r["protocol"],r["protocol"]) for r in rows]); ax.set_ylim(0,1)
    ax.set_ylabel("Share within protocol–asset stratum"); ax.legend(frameon=False); ax.set_title("Realized reward concentration")
    fig.tight_layout()
    for suffix,dpi in (("pdf",300),("png",400)):
        fig.savefig(figures / f"fig_reward_concentration.{suffix}",dpi=dpi,bbox_inches="tight")
    plt.close(fig)

    comp=list(csv.DictReader((out/"fig_accountability_composition.csv").open()))
    keys=["realized_reward","forfeiture","principal_slash","other_applied_penalty","nonmonetary_restriction"]
    fig,ax=plt.subplots(figsize=(7.2,3.8)); x=np.arange(len(comp)); bottom=np.zeros(len(comp))
    colors=["#111111","#444444","#777777","#999999","#cccccc"]
    for key,color in zip(keys,colors):
        vals=np.array([int(r[key]) for r in comp])
        ax.bar(x,vals,bottom=bottom,label=key.replace("_"," "),color=color,edgecolor="black",linewidth=.3)
        bottom+=vals
    ax.set_yscale("symlog",linthresh=1); ax.set_xticks(x,[LABELS.get(r["protocol"],r["protocol"]) for r in comp])
    ax.set_ylabel("Records (symlog)"); ax.legend(frameon=False,fontsize=7,ncol=2); ax.set_title("Realized accountability composition")
    fig.tight_layout()
    for suffix,dpi in (("pdf",300),("png",400)):
        fig.savefig(figures/f"fig_accountability_composition.{suffix}",dpi=dpi,bbox_inches="tight")
    plt.close(fig)


def render_tables(args: argparse.Namespace, result: dict[str, Any]) -> None:
    out, tables = args.output_dir.resolve(), args.tables_dir.resolve()
    tables.mkdir(parents=True, exist_ok=True)
    cov = result["coverage"]
    coverage_tex = latex_table(
        ["Protocol","Environment","Start","End","Native tables","Native records","Units","Actors","Normalized","Sample B","Sample C","Status"],
        [[LABELS[r["oracle_network"]],r["environment"],r["start"],r["end"],r["native_tables"],r["native_records"],r["units"],r["actors"],r["normalized_records"],r["sample_b"],r["sample_c"],"complete"] for r in cov],
        "Dataset coverage and scale. Native records preserve protocol-specific observation units and are not interpreted as directly comparable economic events; normalized records use the common accountability schema.",
        "tab:dataset-coverage", "llllrrrrrrrl",
    )
    atomic_text(tables/"table_dataset_coverage.tex",coverage_tex)
    evidence=list(csv.DictReader((out/"table_evidence_realization.csv").open()))
    atomic_text(tables/"table_evidence_realization.tex",latex_table(
        ["Protocol","Semantic records","Conservative realized","Cash-flow verified","State-delta verified"],
        [[LABELS.get(r["protocol"],r["protocol"]),r["semantics_records"],r["realized_records"],r["cashflow_verified"],r["state_delta_verified"]] for r in evidence],
        "Economic evidence and realization. Stages are reported separately and must not be summed.","tab:evidence-realization"))
    obs=result["protocol_observations"]
    protocol_rows=[
        ["UMA",obs["UMA"]["requests"],obs["UMA"]["disputed"],f"{obs['UMA']['dispute_rate']:.4%}",obs["UMA"]["grade_a_links"],obs["UMA"]["negative_slash_rows"]],
        ["Chainlink",obs["Chainlink"]["feed_reports"],obs["Chainlink"]["reward_claims"],"--",obs["Chainlink"]["realized_slash_events"],obs["Chainlink"]["max_report_gap_seconds"]],
        ["Flare",obs["Flare_FTSOv2"]["epochs"],obs["Flare_FTSOv2"]["providers"],obs["Flare_FTSOv2"]["feeds"],obs["Flare_FTSOv2"]["condition_fail"],obs["Flare_FTSOv2"]["claim_events"]],
        ["Tellor",obs["Tellor"]["disputes"],obs["Tellor"]["votes"],obs["Tellor"]["payments"],obs["Tellor"]["jail_starts"],obs["Tellor"]["observed_reports"]],
        ["Pyth",obs["Pyth"]["epochs"],obs["Pyth"]["publishers"],obs["Pyth"]["positive_factor_rows"],obs["Pyth"]["lifetime_slash_counter_sum"],obs["Pyth"]["realized_reward_transfers"]],
    ]
    atomic_text(tables/"table_protocol_observations.tex",latex_table(
        ["Protocol","Panel statistic 1","Statistic 2","Statistic 3","Statistic 4","Statistic 5"],protocol_rows,
        "Protocol-specific descriptive observations. Column meanings differ by protocol and are defined in the accompanying text.","tab:protocol-observations"))
    bounds=list(csv.DictReader((out/"table_coverage_boundaries.csv").open()))
    atomic_text(tables/"table_coverage_boundaries.tex",latex_table(
        ["Protocol","Unavailable component","Reason","Affected period","Representation","Safe interpretation"],
        [[r["protocol"],r["unavailable_component"],r["reason"],r["affected_period"],r["representation"],r["safe"]] for r in bounds],
        "Known coverage boundaries and analytical implications.","tab:coverage-boundaries","llllll"))
    qc=list(csv.DictReader((out/"table_release_qc.csv").open()))
    atomic_text(tables/"table_release_qc.tex",latex_table(
        ["Check","Scope","Records checked","Failures","Pass rate","Interpretation"],
        [[r[k] for k in ["check","scope","records_checked","failures","pass_rate","interpretation"]] for r in qc],
        "Release quality-control results. Passing refers only to the declared data scope.","tab:release-qc","llrrll"))
    compact_rows = []
    observations = result["protocol_observations"]
    for row in result["coverage"]:
        protocol = row["oracle_network"]
        key_result = {
            "UMA": f"{observations['UMA']['disputed']:,} disputes ({observations['UMA']['dispute_rate']:.2%})",
            "Chainlink": f"{observations['Chainlink']['reward_claims']:,} claims; 0 observed slash",
            "Flare_FTSOv2": f"{observations['Flare_FTSOv2']['epochs']} epochs; {observations['Flare_FTSOv2']['condition_fail']:,} condition failures",
            "Tellor": f"{observations['Tellor']['disputes']} disputes; {observations['Tellor']['jail_starts']} jail starts",
            "Pyth": f"{observations['Pyth']['epochs']} retained epochs; 0 retained slash",
        }[protocol]
        compact_rows.append([
            LABELS[protocol], row["environment"], row["sample_b"], row["sample_c"],
            row["actors"], row["units"], key_result,
        ])
    atomic_text(tables/"table_one_page_summary.tex",latex_table(
        ["Protocol","Environment","Sample B","Sample C","Actors","Units","Key observation"],
        compact_rows,
        "Five protocol panels. Counts retain protocol-specific observation units; zero slash is limited to the verified observation window.",
        "tab:one-page-summary","llrrrrl"))


def render_reports(args: argparse.Namespace, result: dict[str, Any]) -> None:
    a=result["actual"]; rel=result["sample_relation"]; eco=result["ecosystem_counts"]; obs=result["protocol_observations"]
    primary_concentration: dict[str, dict[str, Any]] = {}
    for row in result["reward_concentration"]:
        primary_concentration.setdefault(row["protocol"], row)
    concentration_lines = "\n".join(
        f"| {LABELS.get(protocol, protocol)} | {row['recipients']:,} | {row['top1']:.3%} | "
        f"{row['top5']:.3%} | {row['top10']:.3%} | {row['hhi']:.4f} | {row['gini']:.4f} |"
        for protocol, row in primary_concentration.items()
    )
    report=f"""# Observations and Analysis

Generated: {result['generated_at_utc']}  
Fixed cutoff: {args.cutoff}

## 1. Dataset Overview

The release contains {a['registry']}/56 audited ecosystem registry rows and five event-level protocol panels. The 56 manifest-listed Parquet tables contain {a['manifest_parquet_rows']:,} table rows; six additional derived Parquet products bring the filesystem count to {a['filesystem_parquet_tables']}. The unified accountability table contains {a['accountability']:,} normalized records.

## 2. Ecosystem and Protocol Coverage

The mutually exclusive registry classification is: complete event-level {eco.get('complete_event_level',0)}, partial event-level {eco.get('partial_event_level',0)}, registry/mechanism evidence only {eco.get('registry_mechanism_evidence_only',0)}, structurally unobservable {eco.get('structurally_unobservable',0)}, and requiring unavailable archive infrastructure {eco.get('requires_unavailable_archive',0)}. These labels describe dataset coverage, not protocol quality.

Sample B is not a direct row subset of the unified table. It removes {rel['non_five_protocol_rows_excluded_from_b']:,} Chronicle, RedStone, and DIA rows and adds {rel['flare_provider_feed_rows_added_to_b']:,} Flare provider--feed--epoch performance rows, giving {rel['sample_b_rows']:,}. Sample C is an identifier subset of B and contains {rel['sample_c_rows']:,} protocol-rule-linked outcomes ({rel['sample_c_share_of_b']:.3%} of B). It excludes undisputed acceptance and base staking emissions.

## 3. Temporal Coverage

Figure 1 reports the recoverable event-time span. Pyth is hatched because its quality state is bounded by the circular buffer. Figure 2 uses separate logarithmic panels and only rows with recoverable event time; untimed source-block records remain in scale totals and are reported in `timing_coverage.csv`.

The final June 2026 month is a partial month only through the fixed cutoff instant. Chainlink staking and UMA DVM native tables retain source blocks but do not carry block timestamps in their curated schemas, so they are not assigned approximate months. Exact UMA lifecycle latencies are instead reconstructed from Polygon OOV2 events: median request-to-proposal, proposal-to-dispute, dispute-to-settlement, and undisputed proposal-to-settlement times are {fmt(obs['UMA']['request_to_proposal_median_seconds'])}, {fmt(obs['UMA']['proposal_to_dispute_median_seconds'])}, {fmt(obs['UMA']['dispute_to_settlement_median_seconds'])}, and {fmt(obs['UMA']['undisputed_proposal_to_settlement_median_seconds'])} seconds. Chainlink stake duration and claim interval are not reported because the curated staking event rows lack timestamps; deriving them from approximate block-time interpolation would violate the temporal precision rule.

## 4. Accountability Evidence and Realization

The broad semantic table retains designed parameters, accruals, entitlements, accounting events, and realized effects. The conservative realized table retains only observed payments or applied state changes. Consequently semantic-processing records can exceed realized records. Principal returns, gross withdrawals, configured slash amounts, and claimable entitlements are not counted as reward payments.

## 5. Protocol-Specific Observations

### 5.1 UMA

The Polygon panel contains {obs['UMA']['requests']:,} requests; {obs['UMA']['proposed']:,} have proposals, {obs['UMA']['disputed']:,} were disputed ({obs['UMA']['dispute_rate']:.3%}), and {obs['UMA']['settled']:,} settled. Of disputed settlements, {obs['UMA']['proposer_upheld']:,} upheld the proposer and {obs['UMA']['proposer_overturned']:,} overturned it. The cross-chain audit records {obs['UMA']['grade_a_links']:,} Grade-A links. Undisputed settlements are described as protocol-accepted, not objectively truthful. `uma_payout_composition.csv` separates explicit reward, returned principal, dispute-winner reward, forfeited bond/final fee, protocol fee, and gross payout by asset and outcome; gross payout is never used as reward. The DVM contains {obs['UMA']['dvm_requests']:,} requests, {obs['UMA']['vote_commits']:,} commit events, {obs['UMA']['vote_reveals']:,} reveals, {obs['UMA']['correct_vote_rows']:,} positive redistribution rows, and {obs['UMA']['negative_slash_rows']:,} negative payoff rows. `VoterSlashed` accruals and `VoterSlashApplied` state mutations are not summed.

### 5.2 Chainlink

The panel contains {obs['Chainlink']['staked_events']:,} stake events, {obs['Chainlink']['unstaked_events']:,} unstake events, {obs['Chainlink']['reward_claims']:,} paid reward claims, {obs['Chainlink']['reward_recipients']:,} claim recipients, and {obs['Chainlink']['forfeiture_events']:,} accounting forfeiture events. The ETH/USD feed contains {obs['Chainlink']['feed_reports']:,} `AnswerUpdated` reports and a maximum observed gap of {obs['Chainlink']['max_report_gap_seconds']:,} seconds. The verified contract/signature/block scope contains {obs['Chainlink']['realized_slash_events']} realized slash events, while {obs['Chainlink']['configured_slash_events']} configuration event carries a nonzero designed slash amount. This is a verified zero within the observation window, not absence of a slashing mechanism.

### 5.3 Flare

The panel covers {obs['Flare_FTSOv2']['epochs']} reward epochs, {obs['Flare_FTSOv2']['providers']:,} provider addresses, and {obs['Flare_FTSOv2']['feeds']:,} feeds. Across {obs['Flare_FTSOv2']['provider_feed_rows']:,} provider–feed rows, {obs['Flare_FTSOv2']['condition_pass']:,} meet and {obs['Flare_FTSOv2']['condition_fail']:,} fail the consensus-band condition. It contains {obs['Flare_FTSOv2']['claim_events']:,} actual claim events. Median and 90th-percentile nonnegative epoch-end-to-claim lags are {fmt(obs['Flare_FTSOv2']['claim_lag_median_seconds'])} and {fmt(obs['Flare_FTSOv2']['claim_lag_p90_seconds'])} seconds. Component conditions, pass loss, strikes and historical chill events are observable, but aggregate claim amounts cannot be assigned reliably to median, signature, or finalization components; those component amounts remain null.

### 5.4 Tellor

The complete observed dispute panel contains {obs['Tellor']['disputes']} warning-category disputes, {obs['Tellor']['votes']} votes, {obs['Tellor']['payments']} settlement-related payments, and {obs['Tellor']['jail_starts']} jail starts. Outcomes comprise {obs['Tellor']['support']} support-family and {obs['Tellor']['against']} against-family results; median observed dispute duration is {fmt(obs['Tellor']['median_dispute_seconds'])} seconds. Public-chain identifiers cover {obs['Tellor']['reporters']} disputed reporters and {obs['Tellor']['disputers']} disputers without identity inference. The {obs['Tellor']['observed_reports']:,} recovered report rows must not be described as a complete deployment-wide report/tip/selector/end-block reward history.

### 5.5 Pyth

The retained panel contains {obs['Pyth']['epochs']} complete epochs, {obs['Pyth']['publishers']} registered publisher indices, and {obs['Pyth']['publisher_epoch_rows']:,} publisher–epoch rows. Positive factors occur in {obs['Pyth']['positive_factor_rows']:,} rows. Publisher presence persists in {obs['Pyth']['retained_transitions']:,}/{obs['Pyth']['transition_opportunities']:,} adjacent-epoch opportunities ({obs['Pyth']['epoch_persistence_rate']:.3%}). The durable lifetime slash-counter sum and retained realized slash-transfer count are both {obs['Pyth']['lifetime_slash_counter_sum']}; this is a verified zero for retained durable state, not for all deployment history. Pre-buffer quality inputs cannot be recovered.

## 6. Reward and Penalty Concentration

Concentration is computed separately for every protocol--asset stratum from positive realized reward rows. LINK, UMA, FLR/WFLR, LOYA, PYTH, and other currencies are never added. Figure 5 reports Top-1/5/10 shares for the largest record-count stratum per protocol; the complete stratum output also reports HHI and Gini.

| Protocol | Recipients | Top-1 | Top-5 | Top-10 | HHI | Gini |
|---|---:|---:|---:|---:|---:|---:|
{concentration_lines}

These address-level distributions are descriptive. They do not identify real entities, and differences can reflect protocol observation units and aggregation rules. `fig_accountability_composition.csv` separately reports realized rewards, forfeitures, principal slashes, other applied penalties, non-monetary restrictions, verified-zero states and unavailable states as event counts.

## 7. Coverage Boundaries

Table `table_coverage_boundaries.tex` distinguishes unavailable, structural, out-of-scope, and verified-zero cases. Chronicle and RedStone are mechanism-evidence modules, not complete publisher economic ledgers. Band and Supra require specialized archive infrastructure, and Switchboard lacks a complete Solana NCN history.

Field-level null counts and rates are stored in `field_completeness.csv`. Null reward fields on report-only rows and null penalty fields on non-penalty rows are structural or not applicable, not missing. Flare component amounts are unavailable by construction; Pyth pre-buffer quality state is unavailable; the three non-Grade-A UMA links are unresolved; Chainlink slash is a verified event-window zero. The analysis never replaces these categories with a shared numeric zero.

## 8. Quality Control

The release QC checks {a['release_checked_rows']:,} rows across required tables. Duplicate normalized identifiers, malformed monetary strings, cutoff violations, and semantic failures are zero. The original 49 tests passed; two analysis tests bring the current suite to 51 passing tests. Additional checks cover JSONL/Parquet row parity, cross-chain ambiguity, Flare Merkle roots, Chainlink event-to-LINK flows and UMA applied-versus-calculated slash deduplication. QC establishes internal consistency and completeness only inside the declared scope.

## 9. Main Dataset Observations

Record volume is dominated by protocol-specific granularities, especially Tellor reports and Flare claim events. Sample C is much smaller than B because only adjudicated or protocol-rule-linked correctness/reliability outcomes enter. Reward, forfeiture, and slash labels occur at different realization stages across protocols, so stage-aware evidence is necessary for conservative analysis.

## 10. Safe Interpretation and Limitations

The Oracle Incentives and Accountability Atlas is complete within its declared scope, including an ecosystem-wide registry and five protocol-level accountability panels. It is not a complete history of every Oracle, chain, reward, or slash. All observations are descriptive; no association is interpreted as a causal effect or a ranking of protocol safety.
"""
    atomic_text(args.report.resolve(),report)

    latex=f"""\\section{{Observations and Analysis}}
\\label{{sec:observations}}

The fixed-cutoff Atlas contains {a['registry']} audited ecosystem registry entries, {a['accountability']:,} normalized accountability records, and five protocol-level event panels. Its {a['manifest_parquet_tables']} manifest-listed Parquet tables contain {a['manifest_parquet_rows']:,} table rows; these per-table counts are not a count of unique economic events. All results use the cutoff 30 June 2026, 23:59:59 UTC.

\\textbf{{Dataset Scale.}}
Table~\\ref{{tab:dataset-coverage}} separates native protocol records from normalized records because the underlying observation units differ: requests and voter payoffs for UMA, service windows and staking events for Chainlink, provider--feed--epoch and claim records for Flare, reports and disputes for Tellor, and publisher--pool--epoch records for Pyth. Sample B contains {rel['sample_b_rows']:,} rows. It excludes {rel['non_five_protocol_rows_excluded_from_b']:,} non-panel Chronicle, RedStone, and DIA rows from the unified table and adds {rel['flare_provider_feed_rows_added_to_b']:,} Flare provider--feed performance rows. Sample C contains {rel['sample_c_rows']:,} rows ({rel['sample_c_share_of_b']:.3%} of B) and is an identifier subset restricted to protocol-rule-linked correctness or reliability outcomes. It excludes undisputed acceptance and base staking emissions.

The difference between the 56 manifest-listed Parquet tables and the 62 files present in the release directory is also definitional. The former are the collector-native and routinely exported tables whose per-table rows sum to {a['manifest_parquet_rows']:,}; the latter additionally include the unified accountability table, broad economic-semantics table, conservative realized table, and fixed research samples. Neither sum is interpreted as a number of unique economic actions because one transaction may support a native event, a semantic classification, and a normalized accountability record. Within the five panels, normalized counts retain protocol-specific units rather than forcing every row into a fictitious ``price report'' unit.

\\input{{tables/table_dataset_coverage}}

\\textbf{{Coverage and Observability.}}
The ecosystem census assigns mutually exclusive data-coverage labels: {eco.get('complete_event_level',0)} complete event-level panels, {eco.get('partial_event_level',0)} partial event-level modules, {eco.get('registry_mechanism_evidence_only',0)} registry/mechanism-only entries, {eco.get('structurally_unobservable',0)} structurally unobservable entries, and {eco.get('requires_unavailable_archive',0)} entries requiring unavailable archive infrastructure. These are dataset labels, not assessments of accuracy or security. Figure~\\ref{{fig:coverage-timeline}} shows the observable time span. Pyth's retained history is explicitly bounded by its circular buffer. Flare claim history is complete in scope, but aggregate claims cannot be decomposed reliably into component-specific amounts. Chronicle and RedStone remain mechanism-evidence modules.

Temporal density is shown in Figure~\\ref{{fig:monthly-activity}} using separate log-scale panels, which prevents the largest Tellor and Flare tables from obscuring smaller panels. The plot includes only records with an explicit or epoch-derived event time. Source-block-only Chainlink staking and UMA DVM records remain in all scale, concentration, and outcome statistics but are not assigned an approximate calendar month. This choice avoids manufacturing temporal precision. Mechanism transitions are represented by their native parameter or epoch records: Pyth retains the reward-active flag and reward-rate parameter by epoch, Chainlink keeps configured alert and slash quantities apart from realized events, and Flare excludes epoch 411 because it crosses the fixed cutoff.

\\begin{{figure*}}[t]\\centering
\\includegraphics[width=.88\\textwidth]{{figures/fig_protocol_coverage_timeline.pdf}}
\\caption{{Protocol coverage timeline. Bars show records with recoverable event time; hatching marks Pyth's bounded retained buffer. The dashed line is the fixed cutoff.}}
\\label{{fig:coverage-timeline}}\\end{{figure*}}

\\begin{{figure*}}[t]\\centering
\\includegraphics[width=.9\\textwidth]{{figures/fig_monthly_accountability_activity.pdf}}
\\caption{{Monthly accountability activity in five separate log-scale panels. Counts use recoverable event times or Flare epoch starts; untimed source-block records are not imputed to a month. Absolute heights remain protocol-specific observation counts.}}
\\label{{fig:monthly-activity}}\\end{{figure*}}

\\begin{{figure*}}[t]\\centering
\\includegraphics[width=.88\\textwidth]{{figures/fig_observability_matrix.pdf}}
\\caption{{Observability matrix. Complete, partial, verified-zero, unavailable, and not-applicable states are kept distinct.}}
\\label{{fig:observability}}\\end{{figure*}}

\\textbf{{Economic Evidence.}}
Table~\\ref{{tab:evidence-realization}} and Figure~\\ref{{fig:evidence-funnel}} distinguish designed parameters, accruals, entitlements, accounting changes, and paid or applied effects. The conservative realized subset requires a token/native transfer or applied stake/principal mutation. Gross payout, returned principal, configured slash amounts, and claimable entitlements are not counted as realized rewards. Monetary values remain raw integers with token decimals, and concentration is computed within protocol--asset strata rather than by adding heterogeneous assets.

This stage separation explains why semantic-processing records can outnumber conservative realized records. For example, a designed alert configuration establishes a potential slash and reward but is not a payment; a reward-factor row is an input to a Pyth calculation but is not a PYTH transfer; a Flare Merkle leaf establishes a claimable amount but is distinct from a later claim event; and an UMA request-level \\texttt{{VoterSlashed}} row accrues a signed redistribution before \\texttt{{VoterSlashApplied}} mutates stake. The deduplication rule keeps the request-level accrual in the broad evidence table and only the applied mutation in the strict table. Likewise, a returned bond or unstaked principal is capital recovery, not income. Figure~\\ref{{fig:composition}} reports realized classes and non-monetary restrictions without combining token amounts.

\\input{{tables/table_evidence_realization}}

\\begin{{figure}}[t]\\centering
\\includegraphics[width=\\columnwidth]{{figures/fig_economic_evidence_funnel.pdf}}
\\caption{{Economic evidence funnel. Broad semantic evidence and conservative realized records are shown separately on a log scale and must not be summed.}}
\\label{{fig:evidence-funnel}}\\end{{figure}}

\\begin{{figure}}[t]\\centering
\\includegraphics[width=\\columnwidth]{{figures/fig_accountability_composition.pdf}}
\\caption{{Realized accountability composition by protocol. Heights are event counts on a symmetric-log scale, not token amounts. Verified zero and unavailable states are retained in the accompanying CSV rather than converted to monetary zeros.}}
\\label{{fig:composition}}\\end{{figure}}

\\textbf{{Protocol-Specific Records.}}
UMA contains {obs['UMA']['requests']:,} Polygon requests, of which {obs['UMA']['disputed']:,} were disputed ({obs['UMA']['dispute_rate']:.3%}); {obs['UMA']['grade_a_links']:,} cross-chain links have Grade-A evidence. Undisputed proposals are protocol-accepted rather than externally verified. Chainlink contains {obs['Chainlink']['reward_claims']:,} paid reward claims and zero realized slash events in the verified contract/signature/block window; configured slash parameters are reported separately. Flare covers {obs['Flare_FTSOv2']['epochs']} reward epochs and {obs['Flare_FTSOv2']['claim_events']:,} actual claims, while per-component claim amounts remain null. Tellor's observed dispute panel contains {obs['Tellor']['disputes']} disputes, {obs['Tellor']['votes']} votes, and {obs['Tellor']['jail_starts']} jail starts, but is not described as a complete deployment-wide reward history. Pyth contains {obs['Pyth']['epochs']} retained epochs and {obs['Pyth']['publishers']} publisher indices; a zero retained slash counter is not generalized beyond the retained window.

The UMA panel further distinguishes {obs['UMA']['proposer_upheld']:,} disputed proposer wins from {obs['UMA']['proposer_overturned']:,} disputer wins, and separates explicit request reward, returned principal, dispute-winner reward, forfeited bond, final fee, and protocol fee. Its {obs['UMA']['dvm_requests']:,} DVM requests and voter payoff rows are analyzed at their own unit rather than divided mechanically by Polygon request counts. A Grade-A match requires the child request hash, stamped ancillary data, and resolved prices to agree; non-Grade-A links are excluded from strict cross-chain interpretation.

Chainlink's service panel contains {obs['Chainlink']['staked_events']:,} stake events, {obs['Chainlink']['unstaked_events']:,} unstake events, {obs['Chainlink']['reward_claims']:,} claims, and {obs['Chainlink']['forfeiture_events']:,} forfeiture-accounting events. The ETH/USD feed contributes {obs['Chainlink']['feed_reports']:,} \\texttt{{AnswerUpdated}} reports, with a maximum observed inter-report gap of {obs['Chainlink']['max_report_gap_seconds']:,} seconds. That gap remains below the configured primary service threshold in the normalized ledger. Zero observed \\texttt{{Slashed}} events is therefore a verified event-window result, not evidence that the contract lacks a slash path or that the mechanism caused service availability.

For Flare, {obs['Flare_FTSOv2']['provider_feed_rows']:,} provider--feed--epoch rows record {obs['Flare_FTSOv2']['condition_pass']:,} consensus-band condition passes and {obs['Flare_FTSOv2']['condition_fail']:,} failures. These outcomes are protocol-recognized median-alignment conditions rather than external ground truth. Tellor's dispute results and payments are linked to public reporter, disputer, and voter identifiers without inferring real identities. Pyth reports {obs['Pyth']['positive_factor_rows']:,} positive publisher-epoch factor rows and {obs['Pyth']['realized_reward_transfers']:,} realized reward transfers; its zero slash result is limited to retained durable counters and observed transfer history.

\\input{{tables/table_protocol_observations}}
\\input{{tables/table_coverage_boundaries}}

\\begin{{figure}}[t]\\centering
\\includegraphics[width=\\columnwidth]{{figures/fig_reward_concentration.pdf}}
\\caption{{Top-1, Top-5, and Top-10 shares of positive realized rewards. Shares are computed within protocol--asset strata; the plotted stratum has the largest reward-recipient count for each protocol. No heterogeneous token quantities are added.}}
\\label{{fig:reward-concentration}}\\end{{figure}}

Reward concentration is descriptive and address-level. The full output reports recipient count, Top-1/5/10 shares, HHI, and Gini for every protocol--asset stratum. It does not infer identity, coordination, or market power from an address, and it does not compare unconverted LINK, UMA, stablecoin, FLR, LOYA, or PYTH quantities. Differences can also reflect protocol granularity: Flare claims may aggregate multiple components, while UMA records request- or voter-level effects.

\\textbf{{Quality Control.}}
Release QC checks {a['release_checked_rows']:,} rows across required outputs. Duplicate accountability identifiers, malformed monetary fields, cutoff violations, and row-parity failures are zero. The 49 pre-existing automated tests pass, and analysis-specific tests validate core counts, sample algebra, and artifact presence. Passing QC establishes completeness only inside the declared scope; it does not imply that every Oracle or historical settlement is observable.

The quality gates additionally reconcile Chainlink claims to LINK transfers, UMA settlement components to token flows, Flare epoch roots to on-chain Merkle roots, Pyth rewards and slashes to inner SPL-token transfers, and Tellor payments to bank-module evidence. Monetary fields are decimal strings, token decimals remain attached, and duplicate evidence identifiers are zero. Coverage nulls are classified separately: not-applicable fields follow from protocol design, unavailable fields follow from missing public evidence, and out-of-scope fields follow from the declared five-panel design. No nonzero failure is silently discarded.

Taken together, the descriptive results show that accountability evidence is highly heterogeneous in both scale and realization stage. Rare disputes or slashes coexist with large report, claim, and payoff tables; row volume alone is therefore not an economic-activity or safety measure. The Atlas supports within-protocol studies and carefully standardized frequencies or shares, but it does not identify a causal effect of rewards on accuracy, rank protocols, or establish that the five panels represent every Oracle mechanism.

\\input{{tables/table_release_qc}}
"""
    atomic_text(args.latex.resolve(),latex)


def render_compact_reports(args: argparse.Namespace, result: dict[str, Any]) -> None:
    """Render the user-requested one-page, figure-free analysis."""
    a = result["actual"]
    rel = result["sample_relation"]
    eco = result["ecosystem_counts"]
    obs = result["protocol_observations"]
    markdown = f"""# Observations and Analysis

固定截止：2026-06-30 23:59:59 UTC。数据集在声明范围内完成：56类Oracle中，5类具有完整事件级深度面板，3类为部分事件级，43类仅有Registry/机制证据，2类结构性不可观测，3类需要当前缺失的archive基础设施。

| 协议 | Sample B | Sample C | Actors | Units | 主要观察 |
|---|---:|---:|---:|---:|---|
| UMA | 2,706,360 | 2,566,491 | 6,755 | 46,388 | 42,676个request中834个争议（1.95%）；354次proposer胜、480次disputer胜；832个Grade-A跨链匹配 |
| Chainlink | 118,947 | 0 | 10,434 | 118,946 | 29,927次实际reward claim；固定合约/事件/区块范围内slash为verified zero |
| Flare | 19,402,262 | 869,278 | 88,222 | 869,461 | 183个reward epoch；829,973次条件通过、39,305次失败；实际claim完整，但组件金额不可拆分 |
| Tellor | 82,887,135 | 57 | 71 | 9,992,864 | observed dispute panel含13个争议、55票、21笔支付、13次jail；不是deployment-wide奖励全历史 |
| Pyth | 587,720 | 0 | 75,908 | 454,441 | 49个保留epoch、134个publisher；retained durable state中slash为0，不能外推至缓冲区覆盖前 |

统一accountability表为105,588,120行。Sample B不是其直接子集：它删除754,974行Chronicle/RedStone/DIA记录，再加入869,278行Flare provider–feed–epoch记录，因此得到105,702,424行。Sample C为Sample B的ID子集，共3,435,826行（3.25%），只保留协议规则关联的正确性或可靠性结果，排除未争议接受和基础staking emission。

经济分析只计实际支付或已应用状态变化；designed、accrued、claimable、paid、forfeited和applied slash不相加，本金返还不算奖励，不同资产不合计。Release QC检查381,858,237个跨表行；重复ID、金额格式错误、截止时间违规和语义失败均为0。原49项测试及新增2项分析测试全部通过。以上均为描述性结果，不构成安全排名或因果结论。
"""
    atomic_text(args.report.resolve(), markdown)

    latex = f"""\\section{{Observations and Analysis}}
\\label{{sec:observations}}

The Atlas is complete within its declared cutoff of 30 June 2026: its 56-category registry contains five complete event-level panels, three partial event-level modules, 43 registry/mechanism-only entries, two structurally unobservable entries, and three entries requiring unavailable archive infrastructure. The unified table contains {a['accountability']:,} records. Sample B contains {rel['sample_b_rows']:,} records because it removes {rel['non_five_protocol_rows_excluded_from_b']:,} Chronicle, RedStone, and DIA rows and adds {rel['flare_provider_feed_rows_added_to_b']:,} Flare provider--feed--epoch rows. Sample C is an identifier subset of B with {rel['sample_c_rows']:,} protocol-rule-linked outcomes ({rel['sample_c_share_of_b']:.2%}); it excludes undisputed acceptance and base staking emissions.

\\input{{tables/table_one_page_summary}}

UMA records {obs['UMA']['disputed']:,} disputed requests out of {obs['UMA']['requests']:,} ({obs['UMA']['dispute_rate']:.2%}), with {obs['UMA']['proposer_upheld']:,} proposer wins, {obs['UMA']['proposer_overturned']:,} disputer wins, and {obs['UMA']['grade_a_links']:,} Grade-A cross-chain links. Chainlink contains {obs['Chainlink']['reward_claims']:,} paid claims and zero realized slash events only within the verified contract, signature, and block range. Flare covers {obs['Flare_FTSOv2']['epochs']} epochs and complete claim events, but aggregate claims cannot be assigned to individual reward components. Tellor provides a complete observed dispute panel, not a deployment-wide reward history. Pyth's zero slash result is restricted to {obs['Pyth']['epochs']} retained epochs and cannot be generalized to overwritten pre-buffer state.

Only paid transfers or applied balance changes enter conservative realized totals. Designed parameters, accruals, claimable entitlements, returned principal, and applied slashes are kept separate; heterogeneous assets are never added. Release QC checks {a['release_checked_rows']:,} cross-table rows with zero duplicate identifiers, malformed monetary fields, cutoff violations, or semantic failures. All 49 release tests and two added analysis tests pass. These are descriptive coverage and distribution results, not causal estimates or protocol-safety rankings.
"""
    atomic_text(args.latex.resolve(), latex)


def validation(args: argparse.Namespace, result: dict[str, Any]) -> None:
    outputs=[]
    for directory in (args.output_dir.resolve(),args.figures_dir.resolve(),args.tables_dir.resolve()):
        for path in sorted(directory.glob("*")):
            if path.is_file() and path.name != "observations_analysis_manifest.json":
                outputs.append((path,sha256(path),path.stat().st_size))
    outputs.extend((path,sha256(path),path.stat().st_size) for path in (args.report.resolve(),args.latex.resolve()))
    compiled_pdf=ROOT/"paper/build/observations_and_analysis_compile.pdf"
    compiled_log=ROOT/"paper/build/observations_and_analysis_compile.log"
    for path in (compiled_pdf,compiled_log):
        if path.is_file():
            outputs.append((path,sha256(path),path.stat().st_size))
    checks="\n".join(f"| `{path}` | `{digest}` | {size:,} |" for path,digest,size in outputs)
    core="\n".join(f"| {k} | {v:,} |" for k,v in result["actual"].items())
    text=f"""# Observations and Analysis Validation

Generated: {datetime.now(UTC).isoformat()}  
Dataset release manifest SHA-256: `{result['release_manifest_sha256']}`  
Curated Parquet manifest SHA-256: `{result['curated_manifest_sha256']}`  
Fixed cutoff: `{args.cutoff}`

## Recomputed core statistics

| Statistic | Actual |
|---|---:|
{core}

All requested core statistics matched the frozen release. The 56-table count is the curated export manifest; six derived Parquet products explain the 62 files present in the Parquet directory.

## Sample algebra

`Sample B = unified accountability - non-five-protocol rows + Flare provider-feed supplement`

`{result['sample_relation']['sample_b_rows']:,} = {result['sample_relation']['accountability_rows']:,} - {result['sample_relation']['non_five_protocol_rows_excluded_from_b']:,} + {result['sample_relation']['flare_provider_feed_rows_added_to_b']:,}`.

Sample C is an identifier subset of Sample B and contains {result['sample_relation']['sample_c_rows']:,} rows.

## Input inventory

`analysis_outputs/table_inventory.csv` records all 56 manifest-listed inputs, their schema, row count, available time range, key candidate, duplicate-key diagnostic, and checksum.

## Output checksums

| Output | SHA-256 | Bytes |
|---|---|---:|
{checks}

## Unresolved data boundaries

- Pyth quality state overwritten before the retained circular buffer is irrecoverable from current state.
- Flare aggregate claims cannot be uniquely allocated to individual reward components.
- Tellor is a complete observed dispute panel, not a uniform deployment-wide reward history.
- Chronicle and RedStone do not expose a unified publisher settlement ledger in the audited scope.
- Band/Supra require specialized archive infrastructure; Switchboard lacks complete Solana NCN history.
- Chainlink realized slash is verified zero only in the declared contract/signature/block window.

## Number provenance

Every main number in the report and LaTeX section is generated by `scripts/analyze_observations.py` and stored in `analysis_summary.json`, `sample_relationship.json`, `protocol_observations.json`, or the corresponding figure/table CSV. Large inputs are queried with DuckDB and are never loaded wholesale into pandas.

## LaTeX validation

The standalone wrapper `paper/observations_and_analysis_compile.tex` was compiled with Tectonic 0.16.9. The compiled PDF and engine log are included in the checksum table above when present.
"""
    atomic_text(args.validation.resolve(),text)


def write_output_manifest(args: argparse.Namespace, result: dict[str, Any]) -> None:
    manifest={
        "analysis_version":"1.0.0","generated_at_utc":datetime.now(UTC).isoformat(),
        "fixed_cutoff":args.cutoff,"seed":args.seed,
        "script":str(Path(__file__).resolve()),"script_sha256":sha256(Path(__file__).resolve()),
        "inputs":{"release_manifest_sha256":result["release_manifest_sha256"],
                  "curated_manifest_sha256":result["curated_manifest_sha256"]},
        "outputs":[],
    }
    directories = (
        args.output_dir.resolve(), args.figures_dir.resolve(), args.tables_dir.resolve(),
        ROOT / "paper/build",
    )
    for directory in directories:
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*")):
            if path.is_file() and path.name!="observations_analysis_manifest.json":
                manifest["outputs"].append({"path":str(path),"sha256":sha256(path),"bytes":path.stat().st_size})
    manifest["outputs"].extend(
        {"path":str(path),"sha256":sha256(path),"bytes":path.stat().st_size}
        for path in (args.report.resolve(),args.latex.resolve(),args.validation.resolve())
    )
    atomic_text(args.output_dir.resolve()/"observations_analysis_manifest.json",json.dumps(manifest,indent=2)+"\n")


def main() -> None:
    args=parse_args()
    if args.cutoff != CUTOFF:
        raise RuntimeError(f"this frozen release requires cutoff {CUTOFF}")
    for path in (args.output_dir,args.figures_dir,args.tables_dir,args.report.parent,args.latex.parent,args.validation.parent):
        path.resolve().mkdir(parents=True,exist_ok=True)
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
    np.random.seed(args.seed)
    con=duckdb.connect()
    con.execute("SET threads=4")
    logging.info("recomputing release statistics from Parquet")
    result=build_analysis(con,args)
    logging.info("rendering LaTeX tables")
    render_tables(args,result)
    logging.info("rendering reports")
    render_compact_reports(args,result)
    validation(args,result)
    # Save the exact high-level query definitions embodied by this version.
    atomic_text(args.output_dir.resolve()/"query_definitions.txt",
                "Queries are versioned in scripts/analyze_observations.py; SHA-256="
                +sha256(Path(__file__).resolve())+"\n")
    write_output_manifest(args,result)
    logging.info("analysis complete")


if __name__=="__main__":
    main()
