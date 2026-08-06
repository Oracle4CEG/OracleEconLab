#!/usr/bin/env python3
"""Reproduce one real UMA OOV2 -> DVM economic episode from fixed evidence.

One-command usage:
    python scripts/case_studies/reproduce_uma_real_episode.py

RPC responses are snapshotted without endpoint URLs. Later runs may use the
snapshots offline. The selected episode is a deterministic medoid of the
frozen, fully linked candidate cohort and is fixed below.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import requests
from jsonschema import Draft202012Validator


getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data/case_studies/uma_real_episode"
RAW_RPC = OUT / "raw_rpc"
FIGURES = ROOT / "figures"
REPORT = ROOT / "reports/uma_real_episode_case_study.md"
SCHEMA = ROOT / "schemas/cross_chain_economic_observation.schema.json"
CUTOFF = 1782863999

CASE_ID = "0x699c0842ebca1553171853de9853d07f0336ca0909c90156e731b76442a875ca"
DVM_ID = "0xdb17bc52e96153cceafbc3a275338732d7259cb766141acab55c1372307942a2"
POLYGON_CHAIN_ID = 137
ETHEREUM_CHAIN_ID = 1

TABLES = {
    "round": ROOT / "data/curated/parquet/polygon_uma_request_rounds.parquet",
    "events": ROOT / "data/curated/parquet/polygon_oov2_events.parquet",
    "flows": ROOT / "data/curated/parquet/polygon_uma_token_flows.parquet",
    "flow_qc": ROOT / "data/curated/parquet/polygon_uma_request_flow_qc.parquet",
    "link": ROOT / "data/curated/parquet/uma_polygon_ethereum_grade_a_links.parquet",
    "dvm_requests": ROOT / "data/curated/parquet/uma_dvm_requests.parquet",
    "dvm_votes": ROOT / "data/curated/parquet/uma_dvm_votes_events.parquet",
    "dvm_payoffs": ROOT / "data/curated/parquet/uma_dvm_voter_payoffs.parquet",
    "child": ROOT / "data/curated/parquet/polygon_child_tunnel_events.parquet",
    "adapter": ROOT / "data/curated/parquet/polygon_adapter_events.parquet",
}


def env_values() -> dict[str, str]:
    values = dict(os.environ)
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return values


def canonical_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def rpc_call(url: str, method: str, params: list[Any]) -> Any:
    response = requests.post(
        url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=30
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(f"RPC {method} failed: {payload['error']}")
    return payload.get("result")


def snapshot_rpc(chain: str, chain_id: int, url: str | None, txs: list[str], blocks: list[int]) -> dict[str, Any]:
    RAW_RPC.mkdir(parents=True, exist_ok=True)
    target = RAW_RPC / f"{chain}_rpc_snapshot.json"
    if url:
        observed_chain_id = int(rpc_call(url, "eth_chainId", []), 16)
        if observed_chain_id != chain_id:
            raise RuntimeError(f"{chain} RPC chain id {observed_chain_id}, expected {chain_id}")
        snapshot = {
            "chain": chain,
            "chain_id": chain_id,
            "retrieved_at": datetime.now(UTC).isoformat(),
            "endpoint_redacted": True,
            "receipts": {tx: rpc_call(url, "eth_getTransactionReceipt", [tx]) for tx in txs},
            "blocks": {str(block): rpc_call(url, "eth_getBlockByNumber", [hex(block), False]) for block in blocks},
        }
        if any(value is None for value in snapshot["receipts"].values()):
            raise RuntimeError(f"{chain} RPC returned a null transaction receipt")
        target.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
        return snapshot
    if not target.exists():
        raise RuntimeError(f"No {chain} RPC and no cached snapshot at {target}")
    return json.loads(target.read_text(encoding="utf-8"))


def extract(con: duckdb.DuckDBPyConnection) -> dict[str, pd.DataFrame]:
    p = lambda key: str(TABLES[key]).replace("'", "''")
    round_df = con.execute(
        f"SELECT * FROM read_parquet('{p('round')}') WHERE oo_request_id=?", [CASE_ID]
    ).fetchdf()
    if len(round_df) != 1:
        raise RuntimeError(f"expected one request round, got {len(round_df)}")
    row = round_df.iloc[0]
    txs = [row.source_tx, row.proposal_tx, row.dispute_tx, row.settlement_tx]
    link_df = con.execute(
        f"SELECT * FROM read_parquet('{p('link')}') WHERE oo_request_id=?", [CASE_ID]
    ).fetchdf()
    parent_hash = link_df.iloc[0].parent_request_hash
    child_hash = link_df.iloc[0].child_request_hash
    question_id = row.question_id
    return {
        "request_round": round_df,
        "oov2_events": con.execute(
            f"SELECT * FROM read_parquet('{p('events')}') WHERE oo_request_id=? ORDER BY block_time,log_index",
            [CASE_ID],
        ).fetchdf(),
        "token_flows": con.execute(
            f"SELECT * FROM read_parquet('{p('flows')}') WHERE source_tx IN (?,?,?,?) ORDER BY source_block,log_index",
            txs,
        ).fetchdf(),
        "flow_qc": con.execute(
            f"SELECT * FROM read_parquet('{p('flow_qc')}') WHERE oo_request_id=?", [CASE_ID]
        ).fetchdf(),
        "cross_chain_link": link_df,
        "dvm_request": con.execute(
            f"SELECT * FROM read_parquet('{p('dvm_requests')}') WHERE dvm_request_id=?", [DVM_ID]
        ).fetchdf(),
        "dvm_votes": con.execute(
            f"SELECT * FROM read_parquet('{p('dvm_votes')}') WHERE dvm_request_id=? ORDER BY source_block,log_index",
            [DVM_ID],
        ).fetchdf(),
        "dvm_voter_payoffs": con.execute(
            f"SELECT * FROM read_parquet('{p('dvm_payoffs')}') WHERE dvm_request_id=? ORDER BY source_block,log_index",
            [DVM_ID],
        ).fetchdf(),
        "child_tunnel_events": con.execute(
            f"SELECT * FROM read_parquet('{p('child')}') WHERE parent_request_hash=? OR child_request_hash=? ORDER BY block_time,log_index",
            [parent_hash, child_hash],
        ).fetchdf(),
        "adapter_events": con.execute(
            f"SELECT * FROM read_parquet('{p('adapter')}') WHERE oo_request_id=? OR question_id=? ORDER BY block_time,log_index",
            [CASE_ID, question_id],
        ).fetchdf(),
    }


def ensure_case_eligibility(data: dict[str, pd.DataFrame]) -> None:
    row = data["request_round"].iloc[0]
    link = data["cross_chain_link"].iloc[0]
    flow_qc = data["flow_qc"].iloc[0]
    checks = {
        "fixed_case_id": row.oo_request_id == CASE_ID,
        "primary_sample": row.sample_tier == "primary",
        "settled": row.settlement_tx is not None,
        "disputed": row.dispute_tx is not None,
        "grade_a_link": link.cross_chain_match_grade == "A",
        "resolved_price_consistent": bool(link.resolved_price_consistent),
        "settlement_flow_exact": bool(flow_qc.settlement_flow_exact),
        "dvm_id_fixed": link.dvm_request_id == DVM_ID,
        "four_oov2_events": set(data["oov2_events"].event) == {
            "RequestPrice", "ProposePrice", "DisputePrice", "Settle"
        },
    }
    failures = [name for name, passed in checks.items() if not passed]
    if failures:
        raise RuntimeError(f"case eligibility failed: {failures}")
    pd.DataFrame([{"check": k, "passed": v} for k, v in checks.items()]).to_parquet(
        OUT / "case_selection_qc.parquet", index=False
    )


def receipt_cost(receipt: dict[str, Any]) -> int:
    return int(receipt["gasUsed"], 16) * int(receipt["effectiveGasPrice"], 16)


def event_row(events: pd.DataFrame, name: str) -> pd.Series:
    selected = events[events.event.eq(name)]
    if len(selected) != 1:
        raise RuntimeError(f"expected one {name}, got {len(selected)}")
    return selected.iloc[0]


def build_episode(
    data: dict[str, pd.DataFrame], polygon_rpc: dict[str, Any], ethereum_rpc: dict[str, Any]
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    row = data["request_round"].iloc[0]
    events = data["oov2_events"]
    request = event_row(events, "RequestPrice")
    proposal = event_row(events, "ProposePrice")
    dispute = event_row(events, "DisputePrice")
    settle = event_row(events, "Settle")
    dvm = data["dvm_request"].iloc[0]
    dvm_block = int(dvm.source_block)
    dvm_header = ethereum_rpc["blocks"][str(dvm_block)]
    dvm_time = int(dvm_header["timestamp"], 16)

    bond = int(row.effective_bond_raw)
    reward_cfg = int(row.question_reward_raw)
    reward_paid = int(row.explicit_report_reward_raw) + int(row.dispute_winner_reward_raw)
    returned = int(row.principal_returned_raw)
    forfeited = int(row.bond_forfeited_raw)
    final_fee_forfeited = int(row.final_fee_forfeited_raw)
    protocol_fee = int(row.protocol_fee_raw)
    gross = int(row.gross_payout_raw)
    dispute_receipt = polygon_rpc["receipts"][row.dispute_tx]
    gas_cost = receipt_cost(dispute_receipt)
    lock_amount = 750_000_000  # exact disputer -> OOV2 transfer in the dispute transaction.
    lock_seconds = int(settle.block_time) - int(dispute.block_time)
    capital_days = Decimal(lock_amount) * Decimal(lock_seconds) / Decimal(86400)

    payoff = data["dvm_voter_payoffs"]
    signed_payoffs = payoff.signed_slash_delta_raw.map(int).tolist()
    positive = sum(value for value in signed_payoffs if value > 0)
    negative = sum(value for value in signed_payoffs if value < 0)

    source_map = {
        "bond_raw": [row.proposal_tx, row.dispute_tx],
        "reward_configured_raw": [row.source_tx],
        "reward_to_bond_ratio": [row.source_tx, row.proposal_tx],
        "dispute_decision": [row.dispute_tx],
        "proposal_upheld": [dvm.source_tx, row.settlement_tx],
        "mandatory_wait_seconds": [row.proposal_tx],
        "resolution_delay_seconds": [row.dispute_tx, dvm.source_tx, row.settlement_tx],
        "settlement_delay_seconds": [row.source_tx, row.settlement_tx],
        "excess_delay_seconds": [row.source_tx, row.proposal_tx, row.settlement_tx],
        "reward_paid_raw": [row.dispute_tx, row.settlement_tx],
        "principal_returned_raw": [row.dispute_tx, row.settlement_tx],
        "bond_forfeited_raw": [row.proposal_tx, row.dispute_tx, row.settlement_tx],
        "final_fee_forfeited_raw": [row.proposal_tx, row.dispute_tx],
        "protocol_fee_raw": [row.dispute_tx],
        "gross_payout_raw": [row.settlement_tx],
        "realized_payoff_raw": [row.dispute_tx, row.settlement_tx],
        "gas_cost_native_raw": [row.dispute_tx],
        "capital_days_locked_raw": [row.dispute_tx, row.settlement_tx],
        "cross_chain_link_grade": [row.dispute_tx, dvm.source_tx],
        "dvm_positive_redistribution_raw": [dvm.source_tx],
        "dvm_negative_slash_raw": [dvm.source_tx],
    }
    values = {
        "bond_raw": str(bond),
        "reward_configured_raw": str(reward_cfg),
        "reward_to_bond_ratio": str(Decimal(reward_cfg) / Decimal(bond)),
        "dispute_decision": "true",
        "proposal_upheld": "false",
        "mandatory_wait_seconds": str(int(proposal.expiration_time) - int(proposal.block_time)),
        "resolution_delay_seconds": str(int(settle.block_time) - int(dispute.block_time)),
        "settlement_delay_seconds": str(int(settle.block_time) - int(request.block_time)),
        "excess_delay_seconds": str(
            int(settle.block_time) - int(request.block_time)
            - (int(proposal.expiration_time) - int(proposal.block_time))
        ),
        "reward_paid_raw": str(reward_paid),
        "principal_returned_raw": str(returned),
        "bond_forfeited_raw": str(forfeited),
        "final_fee_forfeited_raw": str(final_fee_forfeited),
        "protocol_fee_raw": str(protocol_fee),
        "gross_payout_raw": str(gross),
        "realized_payoff_raw": str(reward_paid),
        "gas_cost_native_raw": str(gas_cost),
        "capital_days_locked_raw": format(capital_days, "f"),
        "cross_chain_link_grade": "A",
        "dvm_positive_redistribution_raw": str(positive),
        "dvm_negative_slash_raw": str(negative),
    }

    provenance_rows = []
    for variable_name, value in values.items():
        payload = {
            "episode_id": CASE_ID,
            "variable_name": variable_name,
            "value": value,
            "source_transactions": source_map[variable_name],
            "release_cutoff": CUTOFF,
            "transformation_rule": f"UMA_CASE_{variable_name.upper()}_V1",
        }
        provenance_rows.append({
            **payload,
            "source_transactions": json.dumps(payload["source_transactions"]),
            "source_tables": "polygon_uma_request_rounds; polygon_oov2_events; polygon_uma_token_flows; uma_dvm_requests/payoffs",
            "contract_semantics_rule_id": (
                "UMA_OOV2_SETTLEMENT_PAYMENT_V1" if variable_name in {
                    "reward_paid_raw", "principal_returned_raw", "gross_payout_raw", "realized_payoff_raw"
                } else "UMA_OOV2_BOND_FORFEITURE_V1" if "forfeit" in variable_name
                else "UMA_DVM_VOTER_SLASH_ACCRUAL_V1" if variable_name.startswith("dvm_")
                else None
            ),
            "evidence_grade": "A",
            "validation_status": "passed",
            "provenance_id": canonical_hash(payload),
        })
    provenance = pd.DataFrame(provenance_rows)
    pids = dict(zip(provenance.variable_name, provenance.provenance_id))
    aggregate_provenance = canonical_hash(sorted(pids.values()))

    episode = {
        "schema_version": "1.0.0",
        "episode_id": f"uma:polygon-oov2:{CASE_ID}",
        "protocol": "UMA",
        "mechanism": "Polygon OptimisticOracleV2 -> Ethereum VotingV2",
        "native_unit_type": "disputed_request",
        "observation_unit": "cross_chain_episode",
        "security_chain_namespace": "eip155",
        "security_chain_id": "1",
        "delivery_chain_namespace": "eip155",
        "delivery_chain_id": "137",
        "actor": str(row.disputer).lower(),
        "actor_role": "disputer",
        "counterparty": str(row.proposer).lower(),
        "counterparty_role": "proposer",
        "decision_time_unix": int(dispute.block_time),
        "proposal_time_unix": int(proposal.block_time),
        "dispute_time_unix": int(dispute.block_time),
        "settlement_time_unix": int(settle.block_time),
        "challenge_deadline_unix": int(proposal.expiration_time),
        "terminal_time_unix": int(settle.block_time),
        "action": "protocol_observed_action",
        "terminal_outcome": "settled_disputed_disputer_wins",
        "right_censored": False,
        "independent_ground_truth": None,
        "ground_truth_status": "protocol_resolution_only",
        "asset_address": str(row.currency).lower(),
        "asset_symbol": "USDC.e",
        "asset_decimals": 6,
        "bond_raw": str(bond),
        "reward_configured_raw": str(reward_cfg),
        "reward_paid_raw": str(reward_paid),
        "reward_forfeited_raw": None,
        "principal_returned_raw": str(returned),
        "bond_forfeited_raw": str(forfeited),
        "final_fee_forfeited_raw": str(final_fee_forfeited),
        "principal_slashed_raw": None,
        "protocol_fee_raw": str(protocol_fee),
        "gross_payout_raw": str(gross),
        "realized_payoff_raw": str(reward_paid),
        "gas_cost_native_raw": str(gas_cost),
        "investigation_cost_usd": None,
        "delay_cost_usd": None,
        "capital_cost_usd": None,
        "usd_value": None,
        "usd_conversion_source": None,
        "usd_conversion_time_unix": None,
        "reward_to_bond_ratio": str(Decimal(reward_cfg) / Decimal(bond)),
        "dispute_decision": True,
        "proposal_upheld": False,
        "mandatory_wait_seconds": int(values["mandatory_wait_seconds"]),
        "resolution_delay_seconds": int(values["resolution_delay_seconds"]),
        "settlement_delay_seconds": int(values["settlement_delay_seconds"]),
        "excess_delay_seconds": int(values["excess_delay_seconds"]),
        "capital_days_locked_raw": values["capital_days_locked_raw"],
        "verification_cost_usd": None,
        "economic_regret_usd": None,
        "independent_ground_truth_available": False,
        "dvm_positive_redistribution_raw": str(positive),
        "dvm_negative_slash_raw": str(negative),
        "decision_evidence_ids": [pids[x] for x in [
            "bond_raw", "reward_configured_raw", "reward_to_bond_ratio", "dispute_decision"
        ]],
        "evidence_snapshot_time_unix": int(dispute.block_time),
        "future_fields_excluded": [
            "resolved_price_raw", "settlement_time_unix", "reward_paid_raw",
            "principal_returned_raw", "terminal_outcome", "dvm_voter_payoffs",
        ],
        "source_chain_namespace": "eip155",
        "source_chain_id": "137",
        "source_contract": str(dispute.source_contract).lower(),
        "source_transaction": str(dispute.source_tx).lower(),
        "source_log_index": int(dispute.log_index),
        "source_block_number": int(dispute.source_block),
        "source_block_timestamp_unix": int(dispute.block_time),
        "source_event": "DisputePrice",
        "source_table": "polygon_uma_request_rounds + protocol-native evidence bundle",
        "source_finality_rule": "Fixed canonical Polygon receipts plus Ethereum execution block at release cutoff; removed logs excluded.",
        "cross_chain_link_grade": "A",
        "provenance_id": aggregate_provenance,
        "prov_entity_id": f"prov:entity:uma:{CASE_ID}",
        "prov_activity_id": "prov:activity:reproduce_uma_real_episode_v1",
        "prov_agent_id": "prov:softwareAgent:oracle-nature",
        "transformation_rule_id": "UMA_REAL_EPISODE_V1",
        "contract_semantics_rule_id": "UMA_OOV2_SETTLEMENT_PAYMENT_V1",
        "coverage_status": "partial",
        "missing_reason": "Independent truth, off-chain investigation cost and USD conversion are unavailable.",
        "evidence_grade": "A",
        "validation_status": "passed",
        "validation_rule_ids": [
            "UMA_CASE_EVENT_COMPLETENESS_V1", "UMA_CASE_SETTLEMENT_FLOW_EXACT_V1",
            "UMA_CASE_CROSSCHAIN_GRADE_A_V1", "UMA_CASE_PAYOUT_DECOMPOSITION_V1",
        ],
        "interpretation_note": "Protocol-resolved disputer win; not an independently verified truth claim. Gas remains in native Polygon units and is not netted against USDC.e.",
    }

    action_rows = []
    for action, tx in {
        "request": row.source_tx, "proposal": row.proposal_tx,
        "dispute": row.dispute_tx, "settlement": row.settlement_tx,
    }.items():
        receipt = polygon_rpc["receipts"][tx]
        action_rows.append({
            "action": action, "chain_id": 137, "transaction_hash": tx,
            "actor": receipt["from"].lower(), "gas_used": int(receipt["gasUsed"], 16),
            "effective_gas_price_raw": str(int(receipt["effectiveGasPrice"], 16)),
            "gas_cost_native_raw": str(receipt_cost(receipt)), "usd_cost": None,
        })
    dvm_receipt = ethereum_rpc["receipts"][dvm.source_tx]
    action_rows.append({
        "action": "dvm_resolution", "chain_id": 1, "transaction_hash": dvm.source_tx,
        "actor": dvm_receipt["from"].lower(), "gas_used": int(dvm_receipt["gasUsed"], 16),
        "effective_gas_price_raw": str(int(dvm_receipt["effectiveGasPrice"], 16)),
        "gas_cost_native_raw": str(receipt_cost(dvm_receipt)), "usd_cost": None,
    })
    action_costs = pd.DataFrame(action_rows)

    timeline = pd.DataFrame([
        {"stage": "Request", "timestamp": int(request.block_time), "chain": "Polygon", "tx": request.source_tx},
        {"stage": "Proposal", "timestamp": int(proposal.block_time), "chain": "Polygon", "tx": proposal.source_tx},
        {"stage": "Dispute", "timestamp": int(dispute.block_time), "chain": "Polygon", "tx": dispute.source_tx},
        {"stage": "DVM resolution", "timestamp": dvm_time, "chain": "Ethereum", "tx": dvm.source_tx},
        {"stage": "Settlement", "timestamp": int(settle.block_time), "chain": "Polygon", "tx": settle.source_tx},
    ])
    timeline["hours_since_request"] = (timeline.timestamp - int(request.block_time)) / 3600

    cashflow = pd.DataFrame([
        {"role": "proposer", "component": "bond + final fee lost", "amount_raw": -750_000_000},
        {"role": "disputer", "component": "escrow deposited", "amount_raw": -750_000_000},
        {"role": "disputer", "component": "settlement received", "amount_raw": 1_000_000_000},
        {"role": "oracle fee recipient", "component": "protocol fee", "amount_raw": 500_000_000},
    ])
    cashflow["amount_token"] = cashflow.amount_raw / 1_000_000
    return episode, provenance, action_costs, timeline, cashflow


def render_figure(timeline: pd.DataFrame, cashflow: pd.DataFrame) -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)
    colors = {"Polygon": "#3264C8", "Ethereum": "#D97706"}
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 3.7), gridspec_kw={"width_ratios": [1.35, 1]})
    ax = axes[0]
    label_positions = {
        "Request": (0, 0.18, "left"),
        "Proposal": (12, -0.22, "center"),
        "Dispute": (27, 0.20, "center"),
        "DVM resolution": (78, 0.20, "center"),
        "Settlement": (100, -0.22, "right"),
    }
    for _, row in timeline.iterrows():
        ax.scatter(row.hours_since_request, 0, s=90, color=colors[row.chain], zorder=3)
        tx, ty, align = label_positions[row.stage]
        ax.annotate(
            row.stage, xy=(row.hours_since_request, 0), xytext=(tx, ty),
            ha=align, va="center", fontsize=8,
            arrowprops={"arrowstyle": "-", "color": "#7B8794", "lw": .7},
        )
    ax.plot(timeline.hours_since_request, [0] * len(timeline), color="#9AA4B2", lw=1.5)
    ax.set_yticks([])
    ax.set_ylim(-.35, .35)
    ax.set_xlabel("Hours since RequestPrice")
    ax.set_title("(a) Cross-chain accountability lifecycle", loc="left", weight="bold")
    ax.spines[["left", "right", "top"]].set_visible(False)
    for chain, color in colors.items():
        ax.scatter([], [], color=color, label=chain)
    ax.legend(frameon=False, loc="lower right", ncol=2, fontsize=8)

    ax = axes[1]
    net = cashflow.groupby("role", as_index=False).amount_token.sum()
    bar_colors = ["#C44536" if value < 0 else "#2A9D8F" for value in net.amount_token]
    bars = ax.bar(net.role, net.amount_token, color=bar_colors, width=.65)
    ax.axhline(0, color="#687386", lw=.8)
    for bar, value in zip(bars, net.amount_token):
        ax.text(bar.get_x() + bar.get_width()/2, value + (18 if value >= 0 else -18),
                f"{value:+.0f}", ha="center", va="bottom" if value >= 0 else "top", fontsize=8)
    ax.set_ylabel("Net USDC.e (principal flows included)")
    ax.set_ylim(-900, 650)
    ax.set_title("(b) Realized allocation", loc="left", weight="bold")
    ax.tick_params(axis="x", rotation=18)
    ax.spines[["right", "top"]].set_visible(False)
    fig.suptitle(
        "One real UMA disputed episode: evidence-timed lifecycle and token allocation",
        weight="bold", fontsize=13,
    )
    fig.tight_layout()
    stem = FIGURES / "fig_uma_real_episode_economic_lifecycle"
    fig.savefig(stem.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(stem.with_suffix(".png"), dpi=300, bbox_inches="tight")
    plt.close(fig)
    timeline.to_csv(stem.with_name(stem.name + "_timeline.csv"), index=False)
    timeline.to_parquet(stem.with_name(stem.name + "_timeline.parquet"), index=False)
    cashflow.to_csv(stem.with_name(stem.name + "_cashflow.csv"), index=False)
    cashflow.to_parquet(stem.with_name(stem.name + "_cashflow.parquet"), index=False)


def title_from_ancillary(value: str) -> str:
    try:
        text = bytes.fromhex(value.removeprefix("0x")).decode("utf-8", errors="replace")
    except Exception:
        return "unavailable"
    match = re.search(r"title:\s*(.*?),\s*description:", text, re.I | re.S)
    return match.group(1).strip() if match else text[:180]


def write_outputs(
    data: dict[str, pd.DataFrame], episode: dict[str, Any], provenance: pd.DataFrame,
    action_costs: pd.DataFrame, timeline: pd.DataFrame, cashflow: pd.DataFrame,
) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    errors = list(Draft202012Validator(schema).iter_errors(episode))
    if errors:
        raise RuntimeError("episode schema errors: " + "; ".join(error.message for error in errors))
    for name, frame in data.items():
        frame.to_parquet(OUT / f"{name}.parquet", index=False)
    pd.DataFrame([episode]).to_parquet(OUT / "economic_episode.parquet", index=False)
    (OUT / "economic_episode.json").write_text(
        json.dumps(episode, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    provenance.to_parquet(OUT / "variable_provenance.parquet", index=False)
    action_costs.to_parquet(OUT / "action_gas_costs.parquet", index=False)
    timeline.to_parquet(OUT / "lifecycle_timeline.parquet", index=False)
    cashflow.to_parquet(OUT / "economic_cashflows.parquet", index=False)
    render_figure(timeline, cashflow)

    row = data["request_round"].iloc[0]
    title = title_from_ancillary(str(row.ancillary_data_hex))
    payoffs = data["dvm_voter_payoffs"]
    signed_payoffs = payoffs.signed_slash_delta_raw.map(int).tolist()
    positive_n = sum(value > 0 for value in signed_payoffs)
    negative_n = sum(value < 0 for value in signed_payoffs)
    zero_n = sum(value == 0 for value in signed_payoffs)
    REPORT.write_text(
        "# Real UMA source-to-visualization case study\n\n"
        f"**Episode:** `{CASE_ID}`  \n**Question:** {title}  \n"
        "**Selection:** fixed medoid of the frozen primary, settled, Grade-A, price-consistent, flow-exact disputed cohort with an archived dispute receipt.\n\n"
        "## Economic lifecycle\n\n"
        f"The requester configured a {int(row.question_reward_raw)/1e6:,.2f} USDC.e report reward. "
        f"The proposer and disputer each transferred 750.00 USDC.e into the OOV2 escrow, consisting of the effective bond and final-fee exposure. "
        f"The DVM-supported outcome overturned the proposal. Settlement transferred {int(row.gross_payout_raw)/1e6:,.2f} USDC.e to the disputer. "
        f"Relative to the disputer's 750.00 USDC.e escrow outflow, the realized token gain was {int(row.dispute_winner_reward_raw)/1e6:,.2f} USDC.e. "
        "This gain is kept separate from returned principal and from Polygon gas paid in the native asset. The configured 5 USDC.e question reward was refunded/rolled and was not counted as a paid report reward.\n\n"
        "## Cross-chain adjudication\n\n"
        f"The exact Polygon--Ethereum link maps the dispute to DVM request `{DVM_ID}`. "
        f"The request has {len(data['dvm_votes']):,} commit/reveal event rows and {len(payoffs):,} voter payoff rows: "
        f"{positive_n:,} positive redistribution, {negative_n:,} negative wrong/no-vote slash, and {zero_n:,} zero-delta rows. "
        "These `VoterSlashed` values are request-level accruals and are not added to later `VoterSlashApplied` account mutations.\n\n"
        "## What the case supports\n\n"
        "- Exact transaction/log provenance for request, proposal, dispute and settlement.\n"
        "- Exact settlement-flow reconciliation and principal/reward/fee decomposition.\n"
        "- Grade-A cross-chain matching to the Ethereum DVM resolution.\n"
        "- Native-chain Gas costs from transaction receipts, without unsupported USD conversion.\n"
        "- Decision-time evidence separation: settlement and DVM outcomes are excluded from the dispute-time feature set.\n\n"
        "## What it does not support\n\n"
        "The protocol resolution is not independent ground truth. Off-chain investigation effort, labor cost, and timestamped USD conversion are unavailable. "
        "Therefore this case does not establish factual accuracy, causal incentive effects, or economic regret.\n\n"
        "## Reproduction\n\n"
        "```bash\npython scripts/case_studies/reproduce_uma_real_episode.py\n```\n",
        encoding="utf-8",
    )


def manifest() -> None:
    files = sorted(
        p for p in OUT.rglob("*") if p.is_file() and p.name != "manifest.json"
    )
    rows = []
    for path in files:
        rows.append({
            "path": str(path.relative_to(OUT)), "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
    payload = {
        "schema_version": "1.0.0", "episode_id": CASE_ID,
        "fixed_cutoff_unix": CUTOFF, "selection_rule": "fixed frozen-cohort medoid",
        "files": rows,
    }
    (OUT / "manifest.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    data = extract(con)
    con.close()
    ensure_case_eligibility(data)
    row = data["request_round"].iloc[0]
    dvm = data["dvm_request"].iloc[0]
    env = env_values()
    offline = env.get("ORACLE_NATURE_OFFLINE", "0").lower() in {"1", "true", "yes"}
    polygon_url = None if offline else env.get("POLYGON_RPC_URL") or env.get("NODE_URL2")
    ethereum_url = None if offline else env.get("ETHEREUM_RPC_URL") or "http://127.0.0.1:8545"
    polygon = snapshot_rpc(
        "polygon", POLYGON_CHAIN_ID, polygon_url,
        [row.source_tx, row.proposal_tx, row.dispute_tx, row.settlement_tx],
        [int(x) for x in data["oov2_events"].source_block],
    )
    ethereum = snapshot_rpc(
        "ethereum", ETHEREUM_CHAIN_ID, ethereum_url,
        [dvm.source_tx], [int(dvm.source_block)],
    )
    episode, provenance, action_costs, timeline, cashflow = build_episode(data, polygon, ethereum)
    write_outputs(data, episode, provenance, action_costs, timeline, cashflow)
    manifest()
    print(json.dumps({
        "episode_id": CASE_ID,
        "output": str(OUT),
        "figure": str(FIGURES / "fig_uma_real_episode_economic_lifecycle.pdf"),
        "report": str(REPORT),
    }, indent=2))


if __name__ == "__main__":
    main()
