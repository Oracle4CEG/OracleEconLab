# OracleEconLab — Minimum Teaching Demo

This repository is intentionally tiny. It demonstrates only two ideas before students build a larger dataset.

## 1. Turn records into economically meaningful data

The teaching file [`data/uma_demo_episodes.csv`](data/uma_demo_episodes.csv) uses **one row for one complete economic episode**, not one row for one blockchain log.

Each row answers a small economic question: how much capital was at risk (`bond_usd`), whether someone challenged (`was_disputed`), how long resolution took (`resolution_hours`), and whether the supporting evidence is complete (`evidence_status`).

The five rows are **synthetic teaching examples**. They are not empirical UMA observations and must not be used to make claims about UMA.

## 2. Make the result reproducible

Run one command from the repository root:

```bash
python src/reproduce.py
```

The script uses only Python's standard library, validates the input, and recreates [`outputs/summary.csv`](outputs/summary.csv). Expected results:

| metric | value |
|---|---:|
| episodes | 5 |
| disputed_episodes | 2 |
| dispute_rate | 0.4 |
| median_bond_usd | 150.0 |
| median_resolution_hours | 4.0 |
| complete_evidence_rate | 0.8 |

## What the student should do next

Replace the five synthetic rows with a larger set of **real, verifiable episodes from one fixed time or block window**. For every episode, retain the transaction/source reference and retrieval date, explain any exclusions or missing values, and keep the same one-command reproduction pattern.

That is the complete assignment for now. Large-scale scraping, multiple protocols, causal inference, and AI-agent evaluation are later extensions.

## 中文说明

这个最小案例只教两点：

1. 不要堆砌链上日志；一行应代表一次具有保证金、挑战、结算和证据状态的完整经济事件。
2. 数据、代码和预期输出同时公开，使别人运行一条命令即可得到相同结果。
