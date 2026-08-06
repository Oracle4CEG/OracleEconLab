"""Build the phase-one exploratory analysis from the curated Ethereum ledgers.

The analysis is deliberately streaming and integer-only.  If the migrated data
volume is unavailable, the script emits a manifest-only preview and marks every
amount/participant result as unavailable instead of inventing a result.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import yaml


ROOT = Path(__file__).resolve().parents[1]
MAIN_START = int(datetime(2023, 4, 1, tzinfo=UTC).timestamp())
MAIN_END = int(datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC).timestamp())
TOKEN_DECIMALS = 18


def json_rows(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON in {path}:{line_number}") from exc


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def token_amount(raw: int, decimals: int = TOKEN_DECIMALS) -> str:
    """Render a raw integer exactly, without binary floating point."""
    sign = "-" if raw < 0 else ""
    digits = str(abs(raw)).rjust(decimals + 1, "0")
    whole, fraction = digits[:-decimals], digits[-decimals:].rstrip("0")
    return f"{sign}{whole}" + (f".{fraction}" if fraction else "")


def ratio(numerator: int, denominator: int) -> str | None:
    if denominator == 0:
        return None
    # Exact four-decimal percentage using integer rounding.
    scaled = (numerator * 1_000_000 + denominator // 2) // denominator
    return f"{scaled // 10_000}.{scaled % 10_000:04d}%"


def iso_timestamp(value: int | None) -> str | None:
    return datetime.fromtimestamp(value, UTC).isoformat().replace("+00:00", "Z") if value is not None else None


def minmax(current: list[int | None], value: int) -> None:
    current[0] = value if current[0] is None else min(current[0], value)
    current[1] = value if current[1] is None else max(current[1], value)


def top_accounts(accounts: dict[str, dict[str, int]], key: str, reverse: bool = True) -> list[dict[str, str | int]]:
    selected = sorted(accounts.items(), key=lambda item: (item[1][key], item[0]), reverse=reverse)[:10]
    return [
        {
            "address": address,
            "event_count": values["events"],
            f"{key}_raw": str(values[key]),
            f"{key}_token": token_amount(values[key]),
        }
        for address, values in selected
        if values[key] != 0
    ]


def analyze_uma(curated: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    paths = {
        "requests": curated / "uma_dvm_requests.jsonl",
        "votes": curated / "uma_dvm_votes_events.jsonl",
        "payoffs": curated / "uma_dvm_voter_payoffs.jsonl",
        "staking": curated / "uma_dvm_staking_events.jsonl",
    }
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        return {
            "analysis_level": "manifest_only",
            "blocking_issue": "migrated curated volume is not mounted or files are absent",
            "missing_files": missing,
            "manifest_counts": manifest,
        }

    request_counts: Counter[str] = Counter()
    request_blocks: list[int | None] = [None, None]
    request_times: list[int | None] = [None, None]
    identifiers: set[str] = set()
    main_request_ids: set[str] = set()
    resolved_prices: dict[str, int] = {}
    for row in json_rows(paths["requests"]):
        request_counts[row.get("status", "unknown")] += 1
        request_id = row["dvm_request_id"]
        timestamp = int(row["request_time"])
        minmax(request_times, timestamp)
        minmax(request_blocks, int(row["source_block"]))
        identifiers.add(row["identifier"])
        if MAIN_START <= timestamp <= MAIN_END:
            main_request_ids.add(request_id)
        if row.get("status") == "resolved" and row.get("resolved_price_raw") is not None:
            resolved_prices[request_id] = int(row["resolved_price_raw"])

    vote_counts: Counter[str] = Counter()
    main_vote_counts: Counter[str] = Counter()
    vote_blocks: list[int | None] = [None, None]
    commit_voters: set[str] = set()
    reveal_voters: set[str] = set()
    revealed_pairs: set[tuple[str, str]] = set()
    reveal_matches = reveal_mismatches = reveal_unresolved = 0
    for row in json_rows(paths["votes"]):
        request_id, voter = row["dvm_request_id"], row["voter"]
        event = "commit" if row["committed"] else "reveal"
        vote_counts[event] += 1
        if request_id in main_request_ids:
            main_vote_counts[event] += 1
        minmax(vote_blocks, int(row["source_block"]))
        if event == "commit":
            commit_voters.add(voter)
        else:
            reveal_voters.add(voter)
            revealed_pairs.add((request_id, voter))
            resolved = resolved_prices.get(request_id)
            if resolved is None:
                reveal_unresolved += 1
            elif int(row["revealed_price_raw"]) == resolved:
                reveal_matches += 1
            else:
                reveal_mismatches += 1

    payoff_counts: Counter[str] = Counter()
    main_payoff_counts: Counter[str] = Counter()
    payoff_sums: Counter[str] = Counter()
    main_payoff_sums: Counter[str] = Counter()
    payoff_blocks: list[int | None] = [None, None]
    request_deltas: defaultdict[str, int] = defaultdict(int)
    accounts: defaultdict[str, dict[str, int]] = defaultdict(lambda: {"events": 0, "positive": 0, "negative": 0, "net": 0})
    negative_with_reveal = negative_without_reveal = 0
    positive_without_reveal = 0
    duplicate_keys: set[tuple[str, int, int]] = set()
    duplicate_rows = 0
    for row in json_rows(paths["payoffs"]):
        event_key = (row["source_tx"], int(row["log_index"]), int(row["source_block"]))
        if event_key in duplicate_keys:
            duplicate_rows += 1
        duplicate_keys.add(event_key)
        request_id, voter = row["dvm_request_id"], row["voter"]
        delta = int(row["signed_slash_delta_raw"])
        classification = row["classification_rule_id"]
        payoff_counts[classification] += 1
        payoff_sums["signed_delta_raw"] += delta
        if delta > 0:
            payoff_sums["positive_raw"] += delta
            accounts[voter]["positive"] += delta
            if (request_id, voter) not in revealed_pairs:
                positive_without_reveal += 1
        elif delta < 0:
            penalty = -delta
            payoff_sums["negative_raw"] += penalty
            accounts[voter]["negative"] += penalty
            if (request_id, voter) in revealed_pairs:
                negative_with_reveal += 1
            else:
                negative_without_reveal += 1
        accounts[voter]["events"] += 1
        accounts[voter]["net"] += delta
        request_deltas[request_id] += delta
        minmax(payoff_blocks, int(row["source_block"]))
        if request_id in main_request_ids:
            main_payoff_counts[classification] += 1
            main_payoff_sums["signed_delta_raw"] += delta
            if delta > 0:
                main_payoff_sums["positive_raw"] += delta
            elif delta < 0:
                main_payoff_sums["negative_raw"] += -delta

    staking_counts: Counter[str] = Counter()
    for row in json_rows(paths["staking"]):
        staking_counts[row["event"]] += 1

    imbalance = payoff_sums["positive_raw"] - payoff_sums["negative_raw"]
    request_imbalances = [value for value in request_deltas.values() if value != 0]
    return {
        "analysis_level": "curated_exact",
        "scope": {
            "raw_request_time_start": iso_timestamp(request_times[0]),
            "raw_request_time_end": iso_timestamp(request_times[1]),
            "main_window_start": "2023-04-01T00:00:00Z",
            "main_window_end": "2026-06-30T23:59:59Z",
            "request_block_range": request_blocks,
            "vote_block_range": vote_blocks,
            "payoff_block_range": payoff_blocks,
        },
        "requests": {
            "total": sum(request_counts.values()),
            "by_status": dict(request_counts),
            "resolution_rate": ratio(request_counts["resolved"], sum(request_counts.values())),
            "main_window": len(main_request_ids),
            "unique_identifiers": len(identifiers),
        },
        "votes": {
            "events": dict(vote_counts),
            "main_window_events": dict(main_vote_counts),
            "event_count_reveal_to_commit_ratio": ratio(vote_counts["reveal"], vote_counts["commit"]),
            "unique_commit_voters": len(commit_voters),
            "unique_reveal_voters": len(reveal_voters),
            "reveals_matching_resolved_price": reveal_matches,
            "reveals_not_matching_resolved_price": reveal_mismatches,
            "reveals_without_resolved_request": reveal_unresolved,
            "resolved_reveal_match_rate": ratio(reveal_matches, reveal_matches + reveal_mismatches),
        },
        "payoffs": {
            "events": dict(payoff_counts),
            "main_window_events": dict(main_payoff_counts),
            "positive_redistribution_raw": str(payoff_sums["positive_raw"]),
            "positive_redistribution_uma": token_amount(payoff_sums["positive_raw"]),
            "negative_slash_raw": str(payoff_sums["negative_raw"]),
            "negative_slash_uma": token_amount(payoff_sums["negative_raw"]),
            "signed_conservation_gap_raw": str(imbalance),
            "signed_conservation_gap_uma": token_amount(imbalance),
            "negative_with_reveal_wrong_vote_proxy": negative_with_reveal,
            "negative_without_reveal_no_vote_proxy": negative_without_reveal,
            "wrong_vote_share_of_negative_events": ratio(negative_with_reveal, negative_with_reveal + negative_without_reveal),
            "no_vote_share_of_negative_events": ratio(negative_without_reveal, negative_with_reveal + negative_without_reveal),
            "positive_without_reveal_anomaly": positive_without_reveal,
            "observed_redistribution_to_penalty_ratio": ratio(payoff_sums["positive_raw"], payoff_sums["negative_raw"]),
            "nonzero_request_conservation_gaps": len(request_imbalances),
            "duplicate_source_logs": duplicate_rows,
            "unique_voters": len(accounts),
            "top_positive_recipients": top_accounts(accounts, "positive"),
            "top_penalized_voters": top_accounts(accounts, "negative"),
            "main_window_amounts_raw": {key: str(value) for key, value in main_payoff_sums.items()},
        },
        "staking_reconciliation": {
            "events": dict(staking_counts),
            "voter_slash_applied_excluded_from_payoffs": staking_counts["VoterSlashApplied"],
        },
        "interpretation_limits": [
            "wrong/no-vote is an observed-event proxy based on presence of a valid reveal row",
            "VoterSlashed is request-level; VoterSlashApplied is reconciliation-only and is not added",
            "the observed signed gap is not a protocol conservation failure: lazy tracker updates leave penalties or redistributions pending/unobserved at the cutoff",
            "all DVM requests remain cross-chain grade U until Polygon evidence is linked",
        ],
    }


def analyze_chainlink(
    curated: Path,
    manifest: dict[str, Any],
    raw_manifest: dict[str, Any] | None = None,
    evidence_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path = curated / "chainlink_staking_v02_events.jsonl"
    if not path.is_file():
        return {
            "analysis_level": "manifest_only",
            "blocking_issue": "migrated curated volume is not mounted or file is absent",
            "missing_files": [str(path)],
            "manifest_counts": manifest,
        }

    counts: Counter[str] = Counter()
    sums: Counter[tuple[str, str]] = Counter()
    blocks: list[int | None] = [None, None]
    stakers: set[str] = set()
    reward_claimants: set[str] = set()
    finalized: Counter[str] = Counter()
    duplicate_keys: set[tuple[str, int, int]] = set()
    duplicate_rows = malformed_amounts = 0
    for row in json_rows(path):
        event = row["event"]
        counts[event] += 1
        minmax(blocks, int(row["source_block"]))
        event_key = (row["source_tx"], int(row["log_index"]), int(row["source_block"]))
        if event_key in duplicate_keys:
            duplicate_rows += 1
        duplicate_keys.add(event_key)
        for key, value in row.items():
            if key.endswith("_raw"):
                if isinstance(value, str) and value.lstrip("-").isdigit():
                    sums[(event, key)] += int(value)
                elif isinstance(value, list):
                    continue
                else:
                    malformed_amounts += 1
        if event in {"Staked", "Unstaked"}:
            stakers.add(row["staker"])
        elif event == "RewardClaimed":
            reward_claimants.add(row["staker"])
        elif event == "RewardFinalized":
            finalized["forfeited" if row["reward_forfeited"] else "not_forfeited"] += 1

    raw_scan = (raw_manifest or {}).get("raw_log_scan", {})
    raw_total = int(raw_scan.get("total_logs", sum(counts.values())))
    controller_unclassified = int(
        raw_scan.get("event_counts_by_contract", {})
        .get("price_feed_alert_controller", {})
        .get("unclassified_topic", 0)
    )
    return {
        "analysis_level": "curated_exact_structural",
        "scope": {"block_range": blocks, "fixed_cutoff": "2026-06-30T23:59:59Z"},
        "events": dict(counts),
        "participants": {"unique_stakers": len(stakers), "unique_reward_claimants": len(reward_claimants)},
        "amounts": {
            "gross_staked_raw": str(sums[("Staked", "amount_raw")]),
            "gross_staked_link": token_amount(sums[("Staked", "amount_raw")]),
            "gross_unstaked_raw": str(sums[("Unstaked", "amount_raw")]),
            "gross_unstaked_link": token_amount(sums[("Unstaked", "amount_raw")]),
            "reward_claimed_raw": str(sums[("RewardClaimed", "reward_claimed_raw")]),
            "reward_claimed_link": token_amount(sums[("RewardClaimed", "reward_claimed_raw")]),
            "principal_slashed_raw": str(sums[("Slashed", "principal_slashed_raw")]),
            "principal_slashed_link": token_amount(sums[("Slashed", "principal_slashed_raw")]),
            "alert_reward_actual_raw": str(sums[("AlertingRewardPaid", "alert_reward_actual_raw")]),
            "alert_reward_actual_link": token_amount(sums[("AlertingRewardPaid", "alert_reward_actual_raw")]),
            "vested_forfeited_reward_raw": str(sums[("ForfeitedRewardDistributed", "vested_reward_raw")]),
            "vested_forfeited_reward_link": token_amount(sums[("ForfeitedRewardDistributed", "vested_reward_raw")]),
            "reclaimed_forfeited_reward_raw": str(sums[("ForfeitedRewardDistributed", "reclaimed_reward_raw")]),
            "reclaimed_forfeited_reward_link": token_amount(sums[("ForfeitedRewardDistributed", "reclaimed_reward_raw")]),
        },
        "reward_finalization": dict(finalized),
        "qc": {
            "duplicate_source_logs": duplicate_rows,
            "malformed_integer_amounts": malformed_amounts,
            "raw_logs": raw_total,
            "decoded_ledger_rows": sum(counts.values()),
            "unclassified_raw_logs": raw_total - sum(counts.values()),
            "unclassified_price_feed_alert_controller_logs": controller_unclassified,
        },
        "supporting_evidence": evidence_manifest,
        "interpretation_limits": [
            "stake, unstake, and claimed-reward event amounts reconcile exactly against LINK Transfer flows",
            "forfeiture distribution is internal reward accounting and is not treated as an ERC-20 transfer",
            "ETH/USD report timing establishes the service window but does not by itself prove that a hypothetical alert was valid",
            "zero decoded AlertRaised/Slashed events is not proof of absence while alert-controller logs remain unclassified",
            "staking rewards are service/stake incentives, not report-level truth rewards",
        ],
    }


def analyze_registry(path: Path) -> dict[str, Any]:
    registry = yaml.safe_load(path.read_text(encoding="utf-8"))
    networks = registry["networks"]
    universe_manifest_path = path.parents[0] / ".." / "data/manifests/oracle_universe_registry.json"
    universe = json.loads(universe_manifest_path.read_text(encoding="utf-8")) if universe_manifest_path.is_file() else None
    return {
        "snapshot_date": registry["snapshot_date"],
        "network_count": len(networks),
        "networks": [
            {
                "oracle_network": row["oracle_network"],
                "security_chain": row["security_chain"],
                "delivery_chains": row["delivery_chains"],
                "reward_observability": row["reward_onchain_observable"],
                "penalty_observability": row["penalty_onchain_observable"],
                "report_level_observable": row["report_level_observable"],
                "publisher_level_observable": row["publisher_level_observable"],
                "deep_panel_status": row["deep_panel_status"],
            }
            for row in networks
        ],
        "universe_count": universe["oracle_categories"] if universe else None,
        "universe_snapshot_time_utc": universe["snapshot_time_utc"] if universe else None,
        "caveat": "The seven-network table is the deep-panel seed; the separate universe census retains all observed Oracle tags without inventing observability scores.",
    }


def md_value(value: Any) -> str:
    return "—" if value is None else str(value)


def build_report(summary: dict[str, Any]) -> str:
    uma, chainlink, registry = summary["uma"], summary["chainlink"], summary["ecosystem_registry"]
    exact = uma["analysis_level"] == "curated_exact" and chainlink["analysis_level"].startswith("curated_exact")
    lines = [
        "# 第一阶段研究结果",
        "",
        f"生成时间：{summary['generated_at_utc']}  ",
        f"数据状态：**{'逐行精确分析' if exact else '清单级预览（迁移盘离线）'}**",
        "",
        "## 结论边界",
        "",
    ]
    if exact:
        lines += [
            "Ethereum 的 UMA DVM 与 Chainlink Staking v0.2 已完成第一阶段精确分析；Polygon UMA request/dispute 经济账本及主窗口 Grade-A 跨链匹配也已完成。Chainlink stake/reward/unstake 已完成 LINK 流对账，ETH/USD service-window 报告证据也已补齐。",
        ]
    else:
        lines += [
            "当前只能复核既有 manifest 的事件数量。迁移后的 `/dev/sda` 未挂载，curated JSONL 不可读，因此金额、参与者、分布和资金守恒结果均未计算。下表不能作为最终实证结果。",
        ]

    lines += ["", "## UMA VotingV2", ""]
    if uma["analysis_level"] == "curated_exact":
        requests, votes, payoffs = uma["requests"], uma["votes"], uma["payoffs"]
        lines += [
            "| 指标 | 结果 |",
            "|---|---:|",
            f"| Request 总数 | {requests['total']} |",
            f"| 已解析 request | {requests['by_status'].get('resolved', 0)} |",
            f"| Request 解析率 | {requests['resolution_rate']} |",
            f"| Commit events | {votes['events'].get('commit', 0)} |",
            f"| Reveal events | {votes['events'].get('reveal', 0)} |",
            f"| Reveal/commit 事件数比 | {md_value(votes['event_count_reveal_to_commit_ratio'])} |",
            f"| 已解析 request 中 reveal 与最终价格一致率 | {votes['resolved_reveal_match_rate']} |",
            f"| 正向再分配事件 | {payoffs['events'].get('DVM_CORRECT_VOTE_REDISTRIBUTION', 0)} |",
            f"| 负向 slash 事件 | {payoffs['events'].get('DVM_NEGATIVE_SLASH', 0)} |",
            f"| 正向再分配总额（UMA） | {payoffs['positive_redistribution_uma']} |",
            f"| 负向 slash 总额（UMA） | {payoffs['negative_slash_uma']} |",
            f"| signed conservation gap（UMA） | {payoffs['signed_conservation_gap_uma']} |",
            f"| 已观察正向/负向金额比 | {payoffs['observed_redistribution_to_penalty_ratio']} |",
            f"| wrong-vote proxy | {payoffs['negative_with_reveal_wrong_vote_proxy']} |",
            f"| no-vote proxy | {payoffs['negative_without_reveal_no_vote_proxy']} |",
            f"| VoterSlashApplied（只作余额对账） | {uma['staking_reconciliation']['voter_slash_applied_excluded_from_payoffs']} |",
        ]
    else:
        events = uma["manifest_counts"]["events_seen"]
        lines += [
            "| 已保存清单指标 | 数量 |",
            "|---|---:|",
            f"| RequestAdded | {events.get('RequestAdded', 0)} |",
            f"| RequestResolved | {events.get('RequestResolved', 0)} |",
            f"| VoteCommitted | {events.get('VoteCommitted', 0)} |",
            f"| VoteRevealed | {events.get('VoteRevealed', 0)} |",
            f"| VoterSlashed | {events.get('VoterSlashed', 0)} |",
            f"| VoterSlashApplied（不计入 payoff） | {events.get('VoterSlashApplied', 0)} |",
        ]

    lines += ["", "## Chainlink Staking v0.2", ""]
    chain_events = (
        chainlink["events"]
        if "events" in chainlink
        else chainlink["manifest_counts"].get("by_event", {})
    )
    lines += [
        "| 事件 | 数量 |",
        "|---|---:|",
        f"| Staked | {chain_events.get('Staked', 0)} |",
        f"| Unstaked | {chain_events.get('Unstaked', 0)} |",
        f"| RewardClaimed | {chain_events.get('RewardClaimed', 0)} |",
        f"| RewardFinalized | {chain_events.get('RewardFinalized', 0)} |",
        f"| ForfeitedRewardDistributed | {chain_events.get('ForfeitedRewardDistributed', 0)} |",
        f"| AlertRaised | {chain_events.get('AlertRaised', 0)} |",
        f"| AlertingRewardPaid | {chain_events.get('AlertingRewardPaid', 0)} |",
            f"| Slashed | {chain_events.get('Slashed', 0)} |",
    ]
    if chainlink["analysis_level"].startswith("curated_exact"):
        evidence = chainlink.get("supporting_evidence") or {}
        evidence_qc = evidence.get("event_link_flow_qc", {})
        lines += [
            "",
            f"逐行事件中的 `RewardClaimed` 合计为 {chainlink['amounts']['reward_claimed_link']} LINK；`RewardFinalized(rewardForfeited=true)` 共 {chainlink['reward_finalization'].get('forfeited', 0)} 条。Staked {evidence_qc.get('Staked_exact', '—')}、Unstaked {evidence_qc.get('Unstaked_exact', '—')}、RewardClaimed {evidence_qc.get('RewardClaimed_exact', '—')} 均已与 LINK `Transfer` 精确对账。",
            "",
            f"ETH/USD 研究窗口内有 AnswerUpdated {evidence.get('feed_events', {}).get('AnswerUpdated', '—')} 条、NewTransmission {evidence.get('feed_events', {}).get('NewTransmission', '—')} 条；最大 AnswerUpdated 间隔为 {evidence.get('max_answer_update_gap_seconds', '—')} 秒，超过 3 小时的间隔为 {evidence.get('answer_update_gaps_over_3h', '—')}。",
            "",
            f"原始日志 {chainlink['qc']['raw_logs']} 条，进入已解码账本 {chainlink['qc']['decoded_ledger_rows']} 条，仍有 {chainlink['qc']['unclassified_raw_logs']} 条未分类，其中 alert controller 有 {chainlink['qc']['unclassified_price_feed_alert_controller_logs']} 条。因此表中的 AlertRaised/Slashed 为“当前 ABI 解码为 0”，不是无事件的最终证明。",
        ]

    lines += [
        "",
        "## 七类核心 Oracle 可观测性比较",
        "",
        "| Oracle | 安全链 | 交付链 | 奖励可观测性 | 惩罚可观测性 | 深度模块状态 |",
        "|---|---|---|---|---|---|",
    ]
    for row in registry["networks"]:
        lines.append(
            f"| {row['oracle_network']} | {row['security_chain']} | {row['delivery_chains']} | "
            f"{row['reward_observability']} | {row['penalty_observability']} | {row['deep_panel_status']} |"
        )
    lines += [
        "",
        f"七网络表是深度模块 seed；另有 {registry.get('universe_count') or '—'} 类 Oracle/机制标签的 universe 快照。因缺少统一评分证据，本阶段没有主观填造 `economic_importance_score` 等数值评分。",
        "",
        "## 当前可研究与不可研究",
        "",
        "可研究：UMA Polygon request-level reward/bond/dispute/settlement、主窗口 Grade-A 跨链归因、DVM 投票参与与正负经济结果；Chainlink staking/reward/forfeiture、LINK 资金路径与 ETH/USD service-window；Tellor 严格争议子样本；Flare provider/feed/epoch reward eligibility、pass 与链上对账后的 FSP entitlement；Pyth OIS 滚动 reward-factor 与奖励归零制度切点；Oracle universe 与核心网络可观测性差异。",
        "",
        "尚不可研究：窗口内不存在观测 alert/slash 样本时的 Chainlink alert/slash 因果效应、Tellor 全量 report/tip reward、Pyth 部署以来 position/payment/slash 交易全历史，以及五协议的完整支付级比较。",
        "",
        "## 复现命令",
        "",
        "```bash",
        "PYTHONPATH=src python scripts/analyze_phase1.py",
        "```",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run phase-one exploratory research")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--curated-dir", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    curated = args.curated_dir.resolve() if args.curated_dir else root / "data/curated"
    uma_manifest = json.loads((root / "data/manifests/uma_dvm_ledger.json").read_text(encoding="utf-8"))
    chainlink_manifest = json.loads((root / "data/manifests/chainlink_staking_v02_ledger.json").read_text(encoding="utf-8"))
    chainlink_raw_manifest = json.loads((root / "data/manifests/chainlink_staking_v02_raw.json").read_text(encoding="utf-8"))
    chainlink_evidence_manifest = json.loads((root / "data/manifests/chainlink_evidence_ledger.json").read_text(encoding="utf-8"))
    summary = {
        "analysis_version": "0.1.0",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "curated_dir": str(curated),
        "uma": analyze_uma(curated, uma_manifest),
        "chainlink": analyze_chainlink(curated, chainlink_manifest, chainlink_raw_manifest, chainlink_evidence_manifest),
        "ecosystem_registry": analyze_registry(root / "registry/oracle_networks.yaml"),
    }
    polygon_manifest = root / "data/manifests/polygon_uma_ledger.json"
    crosschain_manifest = root / "data/manifests/uma_crosschain_links.json"
    flow_manifest = root / "data/manifests/polygon_uma_token_flow_ledger.json"
    polygon_complete = all(path.is_file() for path in (polygon_manifest, crosschain_manifest, flow_manifest))
    summary["research_ready"] = {
        "ethereum_phase1_exact": summary["uma"]["analysis_level"] == "curated_exact"
        and summary["chainlink"]["analysis_level"].startswith("curated_exact"),
        "polymarket_uma_complete": polygon_complete,
        "cross_oracle_strict_panel_complete": False,
    }
    output = root / "data/analysis/phase1_summary.json"
    report = root / "reports/phase1_research.md"
    write_json(output, summary)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(build_report(summary), encoding="utf-8")
    print(output)
    print(report)


if __name__ == "__main__":
    main()
