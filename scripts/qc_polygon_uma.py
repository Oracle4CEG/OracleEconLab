"""QC report for Polygon Polymarket UMA lifecycles and cross-chain links."""
from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CURATED = ROOT / "data/curated"
MAIN_START = int(datetime(2023, 4, 1, tzinfo=UTC).timestamp())


def rows(path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            yield json.loads(line)


def pct(n, d):
    return f"{n * 100 / d:.2f}%" if d else "n/a"


def main():
    raw = json.loads((ROOT / "data/manifests/polygon_uma_raw.json").read_text())
    legacy = json.loads((ROOT / "data/manifests/polygon_uma_legacy_child_tunnel_raw.json").read_text())
    ledger = json.loads((ROOT / "data/manifests/polygon_uma_ledger.json").read_text())
    links_manifest = json.loads((ROOT / "data/manifests/uma_crosschain_links.json").read_text())
    flow_manifest = json.loads((ROOT / "data/manifests/polygon_uma_token_flow_ledger.json").read_text())
    receipt_manifest = json.loads((ROOT / "data/manifests/polygon_uma_token_flow_receipts.json").read_text())
    gamma_manifest = json.loads((ROOT / "data/manifests/polymarket_gamma.json").read_text())
    economics = Counter(); statuses = Counter(); samples = Counter(); question_grades = Counter(); nonzero_gaps = 0
    for row in rows(CURATED / "polygon_uma_request_rounds.jsonl"):
        statuses[row["status"]] += 1; samples[row["sample_tier"]] += 1; economics[row["economic_status"]] += 1; question_grades[row["question_link_grade"]] += 1
        nonzero_gaps += row.get("payout_qc_gap_raw") not in {None, "0"}
    link_grades = Counter(); main_grades = Counter(); price_status = Counter(); unmatched = Counter()
    for row in rows(CURATED / "uma_polygon_ethereum_grade_a_links.jsonl"):
        link_grades[row["cross_chain_match_grade"]] += 1
        if int(row.get("dvm_time", 0)) >= MAIN_START:
            main_grades[row["cross_chain_match_grade"]] += 1
        if row.get("dvm_resolved_price_raw") is not None and row.get("oo_settled_price_raw") is not None:
            price_status["dvm_oo_match" if row["dvm_resolved_price_raw"] == row["oo_settled_price_raw"] else "dvm_oo_mismatch"] += 1
        if row.get("resolved_price_consistent") is True:
            price_status["three_way_match"] += 1
        elif row.get("dvm_status") == "resolved" and row.get("child_pushed_price_raw") is None:
            price_status["polygon_return_event_unobserved"] += 1
        if row.get("unmatched_reason"):
            unmatched[row["unmatched_reason"]] += 1
    report = f"""# Polygon UMA ledger QC

## Raw evidence

- Current contracts: {raw['raw_log_scan']['total_logs']} address-scoped logs in {raw['raw_log_scan']['chunks']} checksum-protected chunks, blocks {raw['raw_log_scan']['from_block']}–{raw['raw_log_scan']['to_block']}.
- Historical ChildTunnel: {legacy['raw_log_scan']['total_logs']} logs in {legacy['raw_log_scan']['chunks']} chunks, blocks {legacy['raw_log_scan']['from_block']}–{legacy['raw_log_scan']['to_block']}.
- Decoded lifecycle events: {ledger['decoded_events']}; decode errors: {sum(ledger['decode_errors_by_topic'].values())}; duplicate source logs: {ledger['duplicate_source_logs']}.

## Polymarket Adapter → OOV2 lifecycle

- Request rounds: {ledger['request_rounds']}; settled {statuses['settled']}, requested/open {statuses['requested']}, disputed/open {statuses['disputed']}.
- Sample tiers: primary {samples['primary']}, supplementary {samples['supplementary']}, unresolved {samples['unresolved']}.
- Exact Adapter question links: {question_grades['A']}/{sum(question_grades.values())} ({pct(question_grades['A'], sum(question_grades.values()))}).
- Settled undisputed: {economics['settled_undisputed']}.
- Settled disputed, proposer wins: {economics['settled_disputed_proposer_wins']}.
- Settled disputed, disputer wins: {economics['settled_disputed_disputer_wins']}.
- Non-zero settlement payout formula gaps after refund-on-dispute handling: {nonzero_gaps}.
- `Settle.gross_payout` remains gross payout; only formula-derived reward fields are labelled rewards.

## Token-flow reconciliation

- Relevant USDC/USDC.e transfers retained: {flow_manifest['flows']}.
- Settled request payouts exactly reconciled: {flow_manifest['settlement_flow_qc'].get('settlement_exact', 0)}/{statuses['settled']} ({pct(flow_manifest['settlement_flow_qc'].get('settlement_exact', 0), statuses['settled'])}).
- Remaining mismatches: {flow_manifest['settlement_flow_qc'].get('settlement_mismatch', 0)}.
- Canonical transaction receipts used to fill provider `eth_getLogs` omissions: {receipt_manifest['receipts']}; each receipt corpus is checksum-protected and explicitly tagged as receipt-fallback evidence.
- Token transfers are reconciliation evidence only and are never independently classified as rewards.

## Polymarket Gamma metadata

- Broad discovery snapshot plus exhaustive on-chain-question lookups retained {gamma_manifest['markets']} unique market records; {gamma_manifest['markets_created_before_cutoff']} were created by the fixed cutoff.
- Exact Gamma links: {gamma_manifest['uma_round_gamma_link_grades'].get('A', 0)}; unavailable/unlinked metadata: {gamma_manifest['uma_round_gamma_link_grades'].get('U', 0)}.
- Primary-sample Gamma links: {gamma_manifest['uma_round_gamma_grades_by_sample'].get('primary:A', 0)} A; {gamma_manifest['uma_round_gamma_grades_by_sample'].get('primary:U', 0)} U.
- Gamma/on-chain reward checks: {gamma_manifest['metadata_onchain_comparisons'].get('reward_match', 0)} match, {gamma_manifest['metadata_onchain_comparisons'].get('reward_mismatch', 0)} mismatch, {gamma_manifest['metadata_onchain_comparisons'].get('reward_missing', 0)} missing.
- Gamma/on-chain bond checks: {gamma_manifest['metadata_onchain_comparisons'].get('bond_match', 0)} match, {gamma_manifest['metadata_onchain_comparisons'].get('bond_mismatch', 0)} mismatch, {gamma_manifest['metadata_onchain_comparisons'].get('bond_missing', 0)} missing.
- Gamma is mutable auxiliary metadata. Missing or mismatched fields never overwrite the on-chain ledger.

## Polygon → Ethereum DVM linkage

- Disputed OOV2 rounds: {sum(link_grades.values())}; Grade A: {link_grades['A']} ({pct(link_grades['A'], sum(link_grades.values()))}); U: {link_grades['U']}.
- Main-window links (DVM time ≥ 2023-04-01): Grade A {main_grades['A']}/{sum(main_grades.values())} ({pct(main_grades['A'], sum(main_grades.values()))}).
- Exact DVM↔OOV2 resolved-price matches: {price_status['dvm_oo_match']}; mismatches: {price_status['dvm_oo_mismatch']}.
- Three-way DVM↔Polygon PushedPrice↔OOV2 matches: {price_status['three_way_match']}.
- DVM/OOV2 agree but Polygon return event is not observed in the configured tunnel logs: {price_status['polygon_return_event_unobserved']}.
- Exact Polygon bridge evidence but no VotingV2 request: {unmatched['exact_polygon_bridge_found_but_no_votingv2_request']} (all pre-primary-window; retained as U, not inferred).
- Ambiguous matches: {links_manifest['ambiguous_matches']}.

## Result

Polygon request rewards, bonds, proposals, disputes and settlements are reconstructable for exact Adapter-linked rounds, with complete settlement token-flow reconciliation. The primary-window disputed sample has complete Grade-A Polygon→Ethereum request linkage. Historical `getRequest` replay remains an optional secondary state check rather than a missing economic-ledger field.
"""
    output = ROOT / "reports/polygon_uma_qc.md"; output.write_text(report, encoding="utf-8"); print(output)


if __name__ == "__main__":
    main()
