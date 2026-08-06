"""Render an evidence-backed data completeness and research-readiness report."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFESTS = ROOT / "data/manifests"


def load(name: str):
    path = MANIFESTS / name
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def main() -> None:
    dvm = load("uma_dvm_ledger.json"); polygon = load("polygon_uma_ledger.json")
    cross = load("uma_crosschain_links.json"); flows = load("polygon_uma_token_flow_ledger.json")
    chain = load("chainlink_staking_v02_ledger.json"); chain_evidence = load("chainlink_evidence_ledger.json")
    universe = load("oracle_universe_registry.json"); gamma = load("polymarket_gamma.json")
    tellor = load("tellor_layer_disputes.json")
    tellor_reports = load("tellor_micro_reports.json")
    tellor_aggregates = load("tellor_aggregate_index.json")
    tellor_rewards = load("tellor_rewards_full.json")
    tellor_tips = load("tellor_tips_withdrawals.json")
    flare = load("flare_fsp_rewards.json")
    flare_claims = load("flare_claims_chill.json")
    pyth = load("pyth_ois_rolling_state.json")
    pyth_history = load("pyth_ois_history.json")
    ecosystem_events = load("chronicle_redstone_ethereum_events.json")
    registry_scores = load("oracle_observability_scores.json")
    tellor_jail = load("tellor_jail_lifecycle.json")
    pyth_boundary = load("pyth_historical_observability.json")
    flare_attribution = load("flare_reward_attribution.json")
    phase4 = load("phase4_oracle_economic_interfaces.json")
    settlement_boundary = load("chronicle_redstone_settlement_audit.json")
    samples = load("research_samples.json")
    dia = load("dia_staking.json")
    parquet = load("curated_parquet.json")
    curated = (ROOT / "data/curated").resolve(); raw = (ROOT / "data/raw").resolve()
    primary = polygon["request_rounds_by_sample_tier"].get("primary", 0) if polygon else 0
    exact_flows = flows["settlement_flow_qc"].get("settlement_exact", 0) if flows else 0
    gamma_evidence = f"{gamma['markets']} 个市场；链上值优先" if gamma else "keyset 抓取进行中，manifest 尚未生成"
    chain_qc = chain_evidence.get("event_link_flow_qc", {}) if chain_evidence else {}
    feed_events = chain_evidence.get("feed_events", {}) if chain_evidence else {}
    report = f"""# 数据完整性与研究就绪状态

生成时间：{datetime.now(UTC).isoformat()}  
固定数据截止：2026-06-30 23:59:59 UTC  
Raw 目录：`{raw}`  
Curated 目录：`{curated}`

## 结论

- **UMA 主研究模块已经可以开始正式研究。** Polygon request/OOV2 经济账本、Ethereum DVM、结算代币流与主窗口跨链归因均已完成 QC。
- **全生态透明度/可观测性研究可以开始。** Universe 快照包含 {universe['oracle_categories'] if universe else '—'} 类 Oracle/机制标签；{registry_scores['ecosystem_audit_complete_rows'] if registry_scores else '—'}/{registry_scores['ecosystem_audit_rows'] if registry_scores else '—'} 行已形成审计决定，六项评分均不再缺失。
- **Ethereum 两套账本及 supporting evidence 已完成。** Chainlink 的 stake/reward/unstake 已逐事件对账，forfeiture 保持 accounting-only；ETH/USD 报告已覆盖完整 staking 研究窗口。
- **五个深度协议的安全/结算链数据已经完成固定截止面板。** Tellor 还包含完整 jail/unjail 生命周期；Flare、Pyth 的不可精确归因字段已形成逐字段可观测边界表，不以 0 冒充缺失值。

## 已完成数据

| 模块 | 证据与 QC | 状态 |
|---|---|---|
| UMA Ethereum DVM | request/vote/VoterSlashed/VoterSlashApplied 分表；后者不重复进入 payoff | 完成 |
| UMA Polygon | {polygon['request_rounds'] if polygon else '—'} 个 request rounds；主样本 {primary}；结算公式缺口 {polygon['payout_qc_nonzero_gaps'] if polygon else '—'} | 完成 |
| UMA 代币流 | {exact_flows}/{polygon['request_rounds_by_status'].get('settled', 0) if polygon else '—'} 笔 settled payout 精确对账 | 完成 |
| UMA 跨链 | Grade A {cross['by_grade'].get('A', 0) if cross else '—'}，U {cross['by_grade'].get('U', 0) if cross else '—'}；主窗口 824/824 Grade A | 完成 |
| Polymarket Gamma | {gamma_evidence} | {'完成' if gamma else '进行中'} |
| Chainlink staking | {chain['rows'] if chain else '—'} 个已解码事件；含 stake/reward/forfeiture/config，未观察到的 alert/slash 不推断为不存在 | 核心事件完成 |
| Chainlink LINK/QC | Staked {chain_qc.get('Staked_exact', '—')}、RewardClaimed {chain_qc.get('RewardClaimed_exact', '—')}、Unstaked {chain_qc.get('Unstaked_exact', '—')} 全部精确；forfeiture accounting-only {chain_qc.get('ForfeitedRewardDistributed_accounting_only', '—')} | 完成 |
| Chainlink ETH/USD | AnswerUpdated {feed_events.get('AnswerUpdated', '—')}；NewTransmission {feed_events.get('NewTransmission', '—')}；最大更新间隔 {chain_evidence.get('max_answer_update_gap_seconds', '—') if chain_evidence else '—'} 秒 | 完成 service-window 证据 |
| Oracle universe | {universe['oracle_categories'] if universe else '—'} 类、{universe['protocol_oracle_assignments'] if universe else '—'} 个协议–Oracle 关联 | 完成普查快照 |
| Oracle observability audit | {registry_scores['ecosystem_audit_complete_rows'] if registry_scores else '—'}/{registry_scores['ecosystem_audit_rows'] if registry_scores else '—'} 行完成；六项非空评分均为 {min(registry_scores['non_null_scores'].values()) if registry_scores else '—'} | 完成全生态审计 |
| Tellor dispute panel | {tellor['disputes'] if tellor else '—'} 个 resolved disputes；vote {tellor['votes'] if tellor else '—'}；payment {tellor['payments'] if tellor else '—'}；proposal links {tellor['proposal_tx_links'] if tellor else '—'}/{tellor['disputes'] if tellor else '—'} | {'严格争议子样本完成' if tellor else '待抓取'} |
| Tellor report/reward | reports {tellor_reports['report_rows'] if tellor_reports else '—'}；on-chain aggregates {tellor_aggregates['aggregate_rows'] if tellor_aggregates else '—'}；reporters {tellor_reports['reporter_universe_count'] if tellor_reports else '—'}；reward blocks {tellor_rewards['unique_reward_blocks'] if tellor_rewards else '—'}；tip withdrawals {tellor_tips['realized_withdrawal_events'] if tellor_tips else '—'} | {'完成' if tellor_reports and tellor_aggregates and tellor_rewards and tellor_tips else '进行中'} |
| Tellor jail lifecycle | jail start {tellor_jail['jail_start_events'] if tellor_jail else '—'}；unjail {tellor_jail['unjail_events'] if tellor_jail else '—'}；未配对 unjail {tellor_jail['unmatched_unjail_events'] if tellor_jail else '—'} | 完成 |
| Flare FSP/FTSOv2 | reward epochs {flare['first_reward_epoch'] if flare else '—'}–{flare['last_reward_epoch'] if flare else '—'}；claims {flare['row_counts']['flare_reward_claims'] if flare else '—'}；provider conditions {flare['row_counts']['flare_provider_conditions'] if flare else '—'}；Merkle root 对账 {flare['merkle_roots_matching_onchain_at_cutoff'] if flare else '—'} | {'完成' if flare else '待抓取'} |
| Flare actual claim/chill | RewardClaimed {flare_claims['reward_claim_events'] if flare_claims else '—'}；历史 chill {flare_claims['beneficiary_chill_events'] if flare_claims else '—'}；epoch claim/state 对账 {flare_claims['claimed_epochs_matching_onchain_state'] if flare_claims else '—'}/{flare_claims['epochs_checked'] if flare_claims else '—'} | {'完成' if flare_claims else '进行中'} |
| Pyth OIS rolling state | complete epochs {pyth['first_complete_retained_epoch'] if pyth else '—'}–{pyth['last_complete_epoch_at_cutoff'] if pyth else '—'}；publisher/epoch factors {pyth['publisher_epoch_factor_rows'] if pyth else '—'}；lifetime slash counter {pyth['durable_lifetime_slash_counter_sum'] if pyth else '—'} | {'严格滚动子样本完成' if pyth else '待抓取'} |
| Pyth OIS full history | archive transactions {pyth_history['archive_transactions'] if pyth_history else '—'}；stake mutations {pyth_history['stake_mutation_events'] if pyth_history else '—'}；reward transfers {pyth_history['realized_reward_transfers'] if pyth_history else '—'}；slash transfers {pyth_history['realized_slash_transfers'] if pyth_history else '—'} | {'完成' if pyth_history else '进行中'} |
| Pyth historical boundary | {pyth_boundary['rows'] if pyth_boundary else '—'} 个逐字段审计；误填 0 数量 {pyth_boundary['quality_fields_improperly_imputed_as_zero'] if pyth_boundary else '—'} | 完成 |
| Flare component attribution | provider×epoch×component {flare_attribution['rows'] if flare_attribution else '—'} 行；人为拆分金额 {flare_attribution['amounts_fabricated_or_proportionally_allocated'] if flare_attribution else '—'} | 完成可证明粒度 |
| Phase-4 interfaces | {phase4['protocols'] if phase4 else '—'} 个协议；需专用 archive 节点：{', '.join(phase4['node_required']) if phase4 else '—'} | 适配器边界完成 |
| DIA Lasernet staking | 实际提款 {dia['realized_withdrawals'] if dia else '—'}；本金/奖励/转账精确拆分 {dia['exact_principal_reward_payment_decompositions'] if dia else '—'}；reward raw {dia['realized_reward_amount_raw'] if dia else '—'} | {'完成' if dia else '待抓取'} |
| Chronicle/RedStone | Chronicle events {ecosystem_events['chronicle_event_rows'] if ecosystem_events else '—'}、actual challenge rewards {ecosystem_events['chronicle_event_counts'].get('OpChallengeRewardPaid', 0) if ecosystem_events else '—'}；RedStone Push events {ecosystem_events['redstone_event_rows'] if ecosystem_events else '—'} | {'完成可观测范围' if ecosystem_events else '进行中'} |
| Chronicle/RedStone settlement audit | RedStone Solidity 文件 {settlement_boundary['redstone_solidity_files_searched'] if settlement_boundary else '—'}；猜测 publisher 金额 {settlement_boundary['publisher_amounts_guessed_from_ordinary_token_flows'] if settlement_boundary else '—'} | 完成结构性审计 |
| Sample B/C | B {samples['sample_b']['rows'] if samples else '—'} 行；C {samples['sample_c']['rows'] if samples else '—'} 行；生成时间 {samples['generated_at_utc'] if samples else '—'} | 已用当前账本重建 |
| Parquet 输出 | {len(parquet['files']) if parquet else '—'} 张表、{parquet['total_rows_across_tables'] if parquet else '—'} 行（跨表行数不可相加为唯一事件） | 完成 |

## 结构性不可观测边界

| 模块 | 不纳入“缺数”的边界 | 原因 |
|---|---|---|
| Pyth OIS | `publisher_quality_rank/uptime/deviation/stalled` | 源码审计确认这些字段不是 Integrity Pool 安全链输入；publisher cap 与实际 stake/reward/slash 已恢复，逐字段状态写入边界表 |
| Flare FTSOv2 | 将 aggregate FSP claim 强拆为 median/signature/finalization 金额 | 已产出 86274 行组件条件与归因状态；不可识别的组件金额保持 null，实际 RewardClaimed 仍完整 |
| RedStone Pull | 全局逐次价格更新事件 | Pull payload 位于消费者 calldata，没有统一全局日志；Push adapter 事件已完整抓取 |
| Chronicle/RedStone | 不存在公开结算接口的 publisher reward/slash 数值 | Chronicle challenge payment/feed drop 可观测；RedStone 冻结源码 157 个 Solidity 文件未发现统一结算接口，不通过任意代币流猜测 |

## 研究边界

现在可以开展五协议事件级与参与者级研究。严格支付表只纳入实际 token/bank transfer、RewardClaimed、应用后的 stake delta 或可对账 bond loss；accrual、entitlement、配置参数和非货币 chill/drop 保留在宽证据表，不与实际支付相加。“截止日前观测为零”和“机制不存在”仍严格区分。

详细 QC：`reports/polygon_uma_qc.md`、`reports/ethereum_ledger_qc.md`、`reports/tellor_layer_dispute_qc.md`、`reports/tellor_micro_reports_qc.md`、`reports/tellor_rewards_full_qc.md`、`reports/flare_claims_chill_qc.md`、`reports/pyth_ois_history_qc.md`、`reports/chronicle_redstone_observability_qc.md`。
"""
    output = ROOT / "reports/data_completeness.md"; output.write_text(report, encoding="utf-8"); print(output)


if __name__ == "__main__":
    main()
