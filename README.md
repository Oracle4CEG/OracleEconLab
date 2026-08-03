# OracleEconLab — Minimum Teaching Demo

This repository is intentionally tiny. It demonstrates two ideas before students build a larger dataset.

## 1. Turn records into economically meaningful data

The teaching file [`data/uma_demo_episodes.csv`](data/uma_demo_episodes.csv) uses **one row for one complete economic episode**, not one row for one blockchain log.

Each row records an economic mechanism: a proposer locks a bond, a possible reward motivates information production, a disputer may pay to challenge, the protocol reaches an outcome, and resolution takes time.

The five rows are **synthetic teaching examples**. They are not empirical UMA observations and must not be used to make claims about UMA.

### Economic definitions

The bilingual [Economic Variable Dictionary](docs/ECONOMIC_VARIABLE_DICTIONARY.md) defines **every input variable and every generated metric** with:

- an exact mathematical formula;
- an intuitive definition;
- its economic interpretation;
- unit/type and measurement cautions;
- protocol and academic references.

Three distinctions are essential:

1. `bond_usd` is proposer-side capital at risk, not total disputed capital.
2. `accepted` means an undisputed proposal became final; it does not prove external truth.
3. `successful_challenge_rate` uses the protocol ruling, not an independently verified truth label.

## 2. Make the result reproducible

Run one command from the repository root:

```bash
python src/reproduce.py
```

The script uses only Python's standard library, validates the input, and recreates [`outputs/summary.csv`](outputs/summary.csv).

| Generated metric | Value | Plain-language meaning |
|---|---:|---|
| `episodes` | 5 | Complete economic episodes in this teaching sample |
| `disputed_episodes` | 2 | Episodes with a valid challenge |
| `dispute_rate` | 0.400 | Share receiving costly monitoring |
| `median_bond_usd` | 150.0 | Typical proposer-side capital commitment |
| `median_reward_usd` | 20.0 | Typical explicit gross reward |
| `median_reward_to_bond_ratio` | 0.125 | Typical gross reward per dollar of proposer bond |
| `median_resolution_hours` | 4.0 | Typical procedural delay |
| `successful_challenge_rate` | 0.500 | Share of disputes in which the protocol ruled for the disputer |
| `complete_evidence_rate` | 0.800 | Share with a complete evidence trail |

These are descriptive teaching calculations. The synthetic values have no empirical or causal interpretation.

## What the student should do next

Replace the five synthetic rows with a larger set of **real, verifiable episodes from one fixed time or block window**. For every episode:

1. retain the transaction/source reference and retrieval date;
2. document token amounts, USD price source, and valuation timestamp;
3. distinguish “not observed” from “verified not to have happened”;
4. explain exclusions and missing values;
5. preserve the same one-command reproduction pattern; and
6. extend the variable dictionary whenever a new field or metric is added.

Large-scale scraping, multiple protocols, causal inference, and AI-agent evaluation are later extensions.

## 中文说明

这个最小案例只教两点：

1. 不要堆砌链上日志；一行应代表一次具有保证金、奖励、挑战、结算和证据状态的完整经济事件。
2. 每个最终变量都必须同时公开数学定义、直观含义、经济解释、测量限制和参考文献，并由代码一键复现。

完整定义见 [经济变量字典](docs/ECONOMIC_VARIABLE_DICTIONARY.md)。
