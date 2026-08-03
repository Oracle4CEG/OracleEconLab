# Economic Variable Dictionary / 经济变量字典

This document explains exactly what every episode-level field and every generated summary metric means. The five observations are synthetic teaching examples, so the values demonstrate computation only; they are **not empirical findings about UMA**.

本文逐项解释事件层字段和脚本最终生成的指标。当前五条记录均为合成教学样例，因此数值只演示计算方法，**不是关于 UMA 的实证结论**。

## Notation / 数学符号

Let $i=1,\ldots,N$ index complete assertion–challenge–resolution episodes in the selected sample.

- $b_i$: proposer bond in USD;
- $r_i$: explicit ex-ante reward in USD;
- $D_i$: dispute indicator;
- $Y_i$: protocol-recorded final outcome;
- $T_i$: hours from assertion to resolution;
- $E_i$: indicator that the evidence trail is complete.

## A. Episode-level variables / 事件层变量

| CSV variable | Mathematical or formal definition | Intuitive definition | Economic interpretation and cautions | Unit/type | Reference |
|---|---|---|---|---|---|
| `episode_id` | Unique key $i$; it has no cardinal meaning | 一次完整“提出—挑战窗口—结算”过程的编号 | Defines the unit of observation. It is an identifier, not an explanatory variable and should not be averaged or treated as economic magnitude. | string | [R5] |
| `protocol` | $P_i \in \{\text{UMA},\ldots\}$ | 产生该事件的协议 | Identifies the institutional rule system. It may be used as a grouping or fixed-effect variable after multiple protocols are added, but this one-protocol demo has no identifying variation. | category | [R1] |
| `bond_usd` | $b_i=q_i^{P}\,p_i$, where $q_i^{P}$ is the proposer bond in token units and $p_i$ is the documented USD conversion price at the chosen valuation time | 提议者必须锁定、并可能因错误裁决而损失的保证金 | Measures proposer-side capital at risk and deterrence strength. It is **not total disputed capital**. Total capital at risk would require proposer and disputer bonds: $K_i=b_i^{P}+D_i b_i^{D}$. The real-data extension must record token amount, token, price source, and valuation timestamp. | USD, nonnegative | [R1], [R2], [R3] |
| `reward_usd` | $r_i=q_i^{R}\,p_i$, the explicit reward known before the decision; principal returned and forfeited-bond transfers are excluded unless separately documented | 在作出参与或挑战决定前可见的明确奖励 | Captures the incentive to acquire information and participate. It is not realized profit because success probability, gas, opportunity cost, and bond transfers are absent. | USD, nonnegative | [R1], [R4] |
| `was_disputed` | $D_i=\mathbf{1}\{\text{a valid dispute was recorded before the deadline}\}$ | 是否有人实际付出资本发起有效挑战 | An observed costly-monitoring action. $D_i=0$ may reflect correctness, deterrence, low reward, high verification cost, or inattention; it does **not** prove the proposal was true. | binary | [R1], [R2], [R3], [R4] |
| `final_outcome` | $Y_i\in\{\text{accepted},\text{proposer\_won},\text{disputer\_won}\}$ | 协议最终记录的程序性结果 | `accepted` means an undisputed proposal became final; it is not independent ground-truth verification. `proposer_won` and `disputer_won` record the protocol ruling after a dispute. | category | [R1] |
| `resolution_hours` | $T_i=(t_i^{resolve}-t_i^{assert})/3600$ | 从提出声明到协议完成结算所需小时数 | Measures institutional delay and capital-locking friction. A welfare calculation would also need capital amount and an opportunity-cost rate. Unresolved cases require censoring fields and must not be coded as zero. | hours, nonnegative | [R1], [R2] |
| `evidence_status` | $E_i=\mathbf{1}\{\text{status is complete}\}$; raw state is `complete`, `partial`, or `unavailable` | 是否能从来源一路核验到该行及其结算结果 | Measures observability and research reliability, not protocol performance. `unavailable` must never be recoded as “did not happen.” | ordered data-quality category | [R5] |
| `source_ref` | $s_i$ is a persistent pointer to the underlying transaction/event evidence | 能让别人回到原始证据的引用 | Enables auditability and provenance. It is metadata, not an economic regressor. Real data should use transaction hashes or stable source URLs plus retrieval date. | string/URI | [R5] |

## B. Generated summary metrics / 最终生成指标

The script writes these variables to `outputs/summary.csv`.

| Output metric | Exact formula | Intuitive definition | Economic meaning and correct interpretation |
|---|---|---|---|
| `episodes` | $N$ | 样本中的完整经济事件数 | Descriptive sample size and denominator; not protocol activity unless the sample is a complete, pre-specified block/time window. |
| `disputed_episodes` | $N_D=\sum_{i=1}^{N}D_i$ | 实际进入争议程序的事件数 | Quantity of costly monitoring supplied in the observed sample. |
| `dispute_rate` | $\hat d=N^{-1}\sum_{i=1}^{N}D_i$ | 每 100 个事件中约有多少个被挑战 | Descriptive monitoring incidence. It is not an error rate and cannot by itself identify whether bonds or rewards caused disputes. |
| `median_bond_usd` | $\operatorname{median}_{i}(b_i)$ | 典型事件中提议者锁定的保证金 | Typical proposer-side capital commitment/deterrence. It is not the total capital locked in disputed cases. |
| `median_reward_usd` | $\operatorname{median}_{i}(r_i)$ | 典型事件提供的明确奖励 | Typical gross incentive to participate, before costs and risk. |
| `median_reward_to_bond_ratio` | $\operatorname{median}_{i}(r_i/b_i)$, requiring $b_i>0$ | 每锁定 1 美元保证金对应多少明确奖励 | A scale-free **gross incentive intensity**. It is not expected return or ROI because win probability, gas, bond redistribution, delay, and opportunity cost are omitted. |
| `median_resolution_hours` | $\operatorname{median}_{i}(T_i)$ | 典型事件需要多久完成结算 | A descriptive measure of procedural delay and capital illiquidity. |
| `successful_challenge_rate` | $\frac{\sum_i \mathbf{1}\{Y_i=\text{disputer\_won}\}}{\sum_i D_i}$, undefined when $N_D=0$ | 已发起挑战中，挑战者获协议裁决支持的比例 | A proxy for the selectivity/effectiveness of observed challenges. It is based on protocol rulings, not independently verified truth. |
| `complete_evidence_rate` | $\hat e=N^{-1}\sum_{i=1}^{N}E_i$ | 能完整追溯原始证据的事件比例 | Measures evidentiary coverage of the research dataset. It should accompany every substantive statistic but is not a welfare or accuracy measure. |

## C. What can and cannot be concluded / 可以与不可以得出的结论

These variables represent three economic channels:

1. **Incentives and deterrence:** $b_i$, $r_i$, and $r_i/b_i$.
2. **Costly monitoring and accountability:** $D_i$, $Y_i$, and the successful-challenge rate.
3. **Institutional friction:** $T_i$, with capital opportunity cost left for the real-data extension.

A real complete-window dataset could describe associations such as whether dispute incidence varies with bond or reward intensity. It still could **not** claim that changing the bond causes more or fewer disputes without an identification strategy, because protocol designers and users may set bonds and rewards in response to unobserved case risk.

## References / 参考文献

- **R1.** UMA, [How does UMA work?](https://docs.uma.xyz/protocol-overview/how-does-umas-oracle-work). Protocol roles, disputes, bonds, rewards, and settlement.
- **R2.** UMA, [Setting Custom Bond and Liveness Parameters](https://docs.uma.xyz/developers/setting-custom-bond-and-liveness-parameters). Economic examples of dispute bonds, rewards, and challenge windows.
- **R3.** Townsend, R. M. (1979), “Optimal Contracts and Competitive Markets with Costly State Verification,” *Journal of Economic Theory*, 21(2), 265–293. [DOI: 10.1016/0022-0531(79)90031-0](https://doi.org/10.1016/0022-0531(79)90031-0).
- **R4.** Grossman, S. J., and Stiglitz, J. E. (1980), “On the Impossibility of Informationally Efficient Markets,” *American Economic Review*, 70(3), 393–408. [JSTOR record](https://www.jstor.org/stable/1805228).
- **R5.** Wilkinson, M. D. et al. (2016), “The FAIR Guiding Principles for Scientific Data Management and Stewardship,” *Scientific Data*, 3, 160018. [DOI: 10.1038/sdata.2016.18](https://doi.org/10.1038/sdata.2016.18).
