# Economic Variable Dictionary / 经济变量字典

This document defines every raw field, processed variable, and generated metric in the OracleEconLab teaching MVP. Exact formulas are paired with intuitive meanings, research roles, and measurement cautions.

> The bundled records are synthetic. The definitions are reusable; the displayed values are not empirical findings about UMA.

## Notation / 数学符号

Let $i=1,\ldots,N$ index complete assertion–challenge–settlement episodes and $j$ index raw event records.

- $t_i^A$: assertion time;
- $t_i^D$: dispute time when a dispute exists;
- $t_i^S$: settlement time;
- $L_i$: challenge window in hours;
- $b_i$: proposer bond in USD;
- $r_i$: explicit ex-ante reward in USD;
- $D_i$: valid-dispute indicator;
- $Y_i$: protocol-recorded final outcome;
- $T_i$: assertion-to-settlement duration in hours;
- $E_i$: complete-evidence indicator.

## A. Raw event-record fields / 原始事件字段

| Raw field | Formal definition | Intuitive definition | Research and governance meaning | Type/unit |
|---|---|---|---|---|
| `event_id` | Unique key $j$ | 一条来源事件记录的编号 | Prevents duplicate ingestion; identifier only, not an economic variable | string |
| `episode_id` | Join key $i(j)$ | 该记录属于哪一次完整经济过程 | Links assertion, optional dispute, and settlement; must follow a documented protocol identity rule | string |
| `protocol` | $P_j$ | 产生事件的制度/协议 | Identifies the rule system; with one protocol it has no explanatory variation | category |
| `event_type` | $Z_j\in\{A,D,S\}$ | 声明、争议或结算 | Determines the state-transition role of a record | category |
| `event_time_utc` | $t_j$ | 事件发生的 UTC 时间 | Orders states and enforces challenge deadlines; must come from the source, not retrieval time | UTC datetime |
| `actor_role` | $R_j\in\{\text{proposer,disputer,protocol}\}$ | 谁在该事件中行动 | A role, not yet a participant identity; address-level research needs a separate sourced field | category |
| `claim_text` | $c_i$ on the assertion record | 被提出、可能被挑战的声明 | Defines the information task; the MVP does not claim that it is fully normalized or independently labeled | text |
| `challenge_window_hours` | $L_i>0$ on the assertion record | 允许挑战的时间长度 | Measures time pressure and a mechanism-design parameter | hours |
| `bond_usd` | $b_i=q_i^Pp_i$ | 提议者锁定并可能损失的保证金 | Proposer-side capital at risk; not total disputed capital. Real data must retain token amount, price source, and valuation time | USD |
| `reward_usd` | $r_i=q_i^Rp_i$ | 决策前可见的明确奖励 | Gross information/participation incentive; not realized profit | USD |
| `final_outcome` | $Y_i$ on settlement record | 协议程序记录的最终状态 | `accepted`, `proposer_won`, or `disputer_won`; not independent ground truth | category |
| `evidence_status` | $Q_j\in\{complete,partial,unavailable\}$ | 该原始记录能否完整核验 | Data-quality state; `unavailable` must never mean “verified not to have happened” | ordered category |
| `source_id` | Foreign key to source registry | 该记录来自哪个已登记来源 | Enforces provenance registration and permits source-level audit | string |
| `source_ref` | Persistent record pointer $s_j$ | 回到具体交易、日志或证据的位置 | Audit metadata, not a regressor. The fixture uses `synthetic://` URIs; real data require transaction/event or archived-source references | URI/string |
| `retrieved_at_utc` | $t_j^R$ | 研究者何时取得该来源记录 | Supports versioning and reproducibility; it is distinct from event time | UTC datetime |

## B. Processed episode variables / 处理后事件变量

`data/processed/uma_economic_episodes.csv` has one row per valid episode.

| Processed variable | Exact construction | Intuitive definition | Economic meaning and cautions | Type/unit |
|---|---|---|---|---|
| `episode_id` | Unique group key $i$ | 一次完整“声明—挑战窗口—结算”过程 | Unit of observation; identifier only | string |
| `protocol` | Common $P_j$ within episode | 适用的协议规则 | Grouping/fixed-effect candidate only after multiple protocols or rule versions exist | category |
| `claim_text` | Assertion record's $c_i$ | 需要判断的声明 | Candidate AI task text; insufficient without decision-time context and truth label | text |
| `assertion_time_utc` | $t_i^A$ | 声明开始时间 | Beginning of the economic episode | UTC datetime |
| `challenge_deadline_utc` | $t_i^A+L_i$ | 最迟可发起有效挑战的时间 | Defines time pressure and prevents post-deadline actions from being treated as valid disputes | UTC datetime |
| `resolution_time_utc` | $t_i^S$ | 协议完成结算的时间 | End of observed procedure; unresolved cases in real data require censoring rather than an invented time | UTC datetime |
| `challenge_window_hours` | $L_i$ | 挑战窗口长度 | Mechanism parameter affecting monitoring opportunity and delay | hours |
| `bond_usd` | $b_i=q_i^Pp_i$ | 提议者保证金 | Measures proposer-side deterrence/capital at risk. Total disputed capital additionally requires disputer bond $b_i^D$: $K_i=b_i+D_ib_i^D$ | USD |
| `reward_usd` | $r_i=q_i^Rp_i$ | 明确奖励 | Gross incentive before risk, gas, evidence, delay, and opportunity cost | USD |
| `reward_to_bond_ratio` | $r_i/b_i$, requiring $b_i>0$ | 每一美元提议者保证金对应的明确奖励 | Scale-free gross incentive intensity; not ROI or expected return | ratio |
| `was_disputed` | $D_i=\mathbf{1}\{\exists\text{ valid dispute with }t_i^A<t_i^D\le t_i^A+L_i\}$ | 是否有人在期限内实际挑战 | Observed costly monitoring. $D_i=0$ can reflect correctness, deterrence, low reward, high cost, or inattention | binary |
| `final_outcome` | $Y_i\in\{accepted,proposer\_won,disputer\_won\}$ | 协议的程序性结论 | `accepted` is not verified truth; contested outcomes remain protocol rulings | category |
| `resolution_hours` | $T_i=(t_i^S-t_i^A)/3600$ | 从声明到结算的小时数 | Institutional delay and possible capital-locking friction | hours |
| `evidence_status` | $\max_j Q_j$ under `complete` < `partial` < `unavailable` | 构成该 episode 的最差证据状态 | Conservative auditability measure; not protocol quality or AI accuracy | ordered category |
| `source_count` | Number of distinct $s_j$ in episode $i$ | 该行保留了多少条具体来源引用 | Coverage indicator; more sources do not automatically mean higher truth quality | count |
| `source_refs` | Sorted join of distinct $s_j$ | 构成该行的全部来源引用 | Enables row-to-source auditing; not an explanatory variable | pipe-delimited strings |

## C. Generated summary metrics / 最终指标

`outputs/summary.csv` is regenerated from the processed table.

| Metric | Exact formula | Intuitive definition | Correct interpretation |
|---|---|---|---|
| `episodes` | $N$ | 完整经济事件数量 | Sample denominator; only a population count when the sampling window is complete and pre-specified |
| `disputed_episodes` | $N_D=\sum_iD_i$ | 被有效挑战的事件数量 | Quantity of observed costly monitoring |
| `dispute_rate` | $\hat d=N^{-1}\sum_iD_i$ | 每 100 个事件约有多少被挑战 | Monitoring incidence, not error rate and not a causal effect of incentives |
| `median_bond_usd` | $\operatorname{median}_i(b_i)$ | 典型提议者保证金 | Typical proposer-side capital commitment |
| `median_reward_usd` | $\operatorname{median}_i(r_i)$ | 典型明确奖励 | Typical gross incentive before costs and risk |
| `median_reward_to_bond_ratio` | $\operatorname{median}_i(r_i/b_i)$ | 典型单位保证金奖励 | Scale-free incentive intensity, not expected return |
| `median_challenge_window_hours` | $\operatorname{median}_i(L_i)$ | 典型挑战窗口 | Typical time available to investigate and act |
| `median_resolution_hours` | $\operatorname{median}_i(T_i)$ | 典型结算延迟 | Typical procedural delay; not a complete opportunity-cost estimate |
| `successful_challenge_rate` | $\frac{\sum_i\mathbf{1}\{Y_i=disputer\_won\}}{\sum_iD_i}$ | 已挑战事件中挑战者获协议支持的比例 | Protocol-based selectivity/effectiveness proxy; undefined when there are no disputes and not independent accuracy |
| `complete_evidence_rate` | $\hat e=N^{-1}\sum_i\mathbf{1}\{Q_i=complete\}$ | 可完整追溯的事件占比 | Evidentiary coverage of the research dataset; should accompany substantive statistics |

## D. Economics and Trustworthy-AI boundaries

The current schema represents three economics channels:

1. incentives and deterrence: $b_i$, $r_i$, and $r_i/b_i$;
2. costly monitoring and accountability: $D_i$ and $Y_i$; and
3. institutional friction: $L_i$ and $T_i$.

A real complete-window dataset could estimate associations among these variables. Causal claims need exogenous rule changes, a defensible structural model, or another identification strategy because bonds and rewards may respond to unobserved risk.

The schema is only an interface for future Trustworthy-AI work. A valid agent benchmark must add at least:

| Missing benchmark variable | Why it is necessary |
|---|---|
| `decision_time_utc` | Fix the moment at which the agent acts |
| timestamped evidence snapshot | Prove that every input was available before the decision |
| `independent_truth_label` | Separate objective/adjudicated truth from protocol outcome |
| `agent_action` and probability | Evaluate Accept/Investigate/Challenge/Abstain and calibration |
| action, gas, evidence, and delay costs | Calculate economic utility and regret |
| model/tool trace and citations | Audit evidence fidelity and accountability |
| repeat and perturbation IDs | Evaluate stability and adversarial robustness |
| human-escalation label | Evaluate appropriate oversight |

## References / 参考文献

- **R1.** UMA, [How does UMA work?](https://docs.uma.xyz/protocol-overview/how-does-umas-oracle-work).
- **R2.** UMA, [Setting Custom Bond and Liveness Parameters](https://docs.uma.xyz/developers/setting-custom-bond-and-liveness-parameters).
- **R3.** Townsend, R. M. (1979), “Optimal Contracts and Competitive Markets with Costly State Verification,” *Journal of Economic Theory*, 21(2), 265–293. [DOI](https://doi.org/10.1016/0022-0531(79)90031-0).
- **R4.** Grossman, S. J. and Stiglitz, J. E. (1980), “On the Impossibility of Informationally Efficient Markets,” *American Economic Review*, 70(3), 393–408. [JSTOR](https://www.jstor.org/stable/1805228).
- **R5.** Wilkinson, M. D. et al. (2016), “The FAIR Guiding Principles for Scientific Data Management and Stewardship,” *Scientific Data*, 3, 160018. [DOI](https://doi.org/10.1038/sdata.2016.18).
- **R6.** Gebru, T. et al. (2021), “Datasheets for Datasets,” *Communications of the ACM*, 64(12), 86–92. [DOI](https://doi.org/10.1145/3458723).
- **R7.** MLCommons, [Croissant specification](https://docs.mlcommons.org/croissant/docs/croissant-spec-1.0.html).
- **R8.** NIST, [AI Risk Management Framework](https://www.nist.gov/itl/ai-risk-management-framework).
- **R9.** Yao, S. et al. (2024), “$\tau$-bench: A Benchmark for Tool-Agent-User Interaction in Real-World Domains.” [arXiv](https://arxiv.org/abs/2406.12045).
