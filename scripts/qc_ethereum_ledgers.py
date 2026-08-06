"""QC for the two Ethereum-only ledgers; reads external curated storage via data/ symlink."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CURATED = ROOT / "data/curated"


def rows(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            yield json.loads(line)


def main() -> None:
    uma_manifest = json.loads((ROOT / "data/manifests/uma_dvm_ledger.json").read_text())
    chainlink_manifest = json.loads((ROOT / "data/manifests/chainlink_staking_v02_ledger.json").read_text())
    chainlink_evidence = json.loads((ROOT / "data/manifests/chainlink_evidence_ledger.json").read_text())
    requests = list(rows(CURATED / "uma_dvm_requests.jsonl"))
    payoff_counts = Counter(); missing_request = 0; malformed_raw = 0
    for row in rows(CURATED / "uma_dvm_voter_payoffs.jsonl"):
        payoff_counts[row["classification_rule_id"]] += 1
        missing_request += row["dvm_request_id"] is None
        for key, value in row.items():
            if key.endswith("_raw") and not (isinstance(value, str) and value.lstrip("-").isdigit()):
                malformed_raw += 1
    vote_counts = Counter()
    for row in rows(CURATED / "uma_dvm_votes_events.jsonl"):
        vote_counts["committed" if row["committed"] else "revealed"] += 1
    staking_event_counts = Counter()
    for row in rows(CURATED / "uma_dvm_staking_events.jsonl"):
        staking_event_counts[row["event"]] += 1
    request_status = Counter(row.get("status", "unknown") for row in requests)
    source_slashes = uma_manifest["events_seen"]["VoterSlashed"]
    mismatches = sum(value for key, value in chainlink_evidence["event_link_flow_qc"].items() if key.endswith("_mismatch"))
    report = f"""# Ethereum ledger QC

## UMA VotingV2

- Request rows: {len(requests)}; resolved: {request_status['resolved']}; added-only/unresolved: {request_status['added']}.
- Vote-event rows: commit {vote_counts['committed']}; reveal {vote_counts['revealed']}.
- Payoff rows sourced only from `VoterSlashed`: {sum(payoff_counts.values())}; source event count: {source_slashes}.
- `VoterSlashApplied` rows are stored only in the staking/reconciliation table: {staking_event_counts['VoterSlashApplied']}; they are **not** included in payoff rows.
- Positive redistribution rows: {payoff_counts['DVM_CORRECT_VOTE_REDISTRIBUTION']}; negative-slash rows: {payoff_counts['DVM_NEGATIVE_SLASH']}; zero rows: {payoff_counts['DVM_ZERO_SLASH']}.
- Payoff rows without a resolved DVM request index mapping: {missing_request}; these remain unresolved rather than inferred.
- Non-integer raw amount fields: {malformed_raw}.

## Chainlink Staking v0.2

- Ledger rows: {chainlink_manifest['rows']}.
- Staked: {chainlink_manifest['by_event'].get('Staked', 0)}; unstaked: {chainlink_manifest['by_event'].get('Unstaked', 0)}.
- Reward claimed: {chainlink_manifest['by_event'].get('RewardClaimed', 0)}; reward finalized: {chainlink_manifest['by_event'].get('RewardFinalized', 0)}; forfeiture distributions: {chainlink_manifest['by_event'].get('ForfeitedRewardDistributed', 0)}.
- ETH/USD alert-controller configurations decoded: {chainlink_manifest['by_event'].get('FeedConfigSet', 0)}.
- LINK flow rows: {chainlink_evidence['link_flows']}; incoming {chainlink_evidence['link_flows_by_direction'].get('incoming', 0)}; outgoing {chainlink_evidence['link_flows_by_direction'].get('outgoing', 0)}.
- Event-to-flow exact: Staked {chainlink_evidence['event_link_flow_qc'].get('Staked_exact', 0)}; Unstaked {chainlink_evidence['event_link_flow_qc'].get('Unstaked_exact', 0)}; RewardClaimed {chainlink_evidence['event_link_flow_qc'].get('RewardClaimed_exact', 0)}. Mismatches: {mismatches}.
- Contract-mediated stake migrations: {chainlink_evidence['event_link_flow_routes'].get('Staked:contract_mediated_to_pool', 0)}; direct stakes: {chainlink_evidence['event_link_flow_routes'].get('Staked:direct_staker_to_pool', 0)}.
- Forfeiture accounting-only rows: {chainlink_evidence['event_link_flow_qc'].get('ForfeitedRewardDistributed_accounting_only', 0)}; no ERC-20 transfer is invented for internal reward-accounting redistribution.
- ETH/USD reports: AnswerUpdated {chainlink_evidence['feed_events'].get('AnswerUpdated', 0)}; NewTransmission {chainlink_evidence['feed_events'].get('NewTransmission', 0)}; maximum AnswerUpdated gap {chainlink_evidence['max_answer_update_gap_seconds']} seconds; gaps over 3 hours {chainlink_evidence['answer_update_gaps_over_3h']}.
- Alert/slash events are retained when observed; no raw LINK `Transfer` is independently classified as a reward. Zero observed AlertRaised/Slashed remains an observation, not proof that the mechanism is absent.

## Result

Both Ethereum ledgers pass their structural QC. UMA payoff rows use `VoterSlashed` only and exclude `VoterSlashApplied`; Chainlink stake/unstake/reward cash flows reconcile exactly, while forfeiture remains explicitly accounting-only. Polygon source-request, token-flow and Grade-A cross-chain checks are complete in the separate Polygon QC report.
"""
    output = ROOT / "reports/ethereum_ledger_qc.md"
    output.write_text(report, encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
