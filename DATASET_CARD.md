# Dataset Card — OracleEconLab Teaching MVP

## Summary

| Item | Value |
|---|---|
| Dataset | OracleEconLab Teaching MVP |
| Version | 0.1.0 |
| Release date | 2026-08-03 |
| Unit of observation | One complete assertion–challenge-window–settlement episode |
| Raw fixture | 12 synthetic event records |
| Processed fixture | 5 synthetic economic episodes |
| Protocol represented | UMA-style optimistic-oracle mechanism |
| License | Original synthetic data: CC BY 4.0; code: MIT |
| Machine-readable metadata | `data/croissant.json` |

> Every amount, claim, timestamp, actor role, and outcome in the bundled event log is fictional. The fixture was authored to exercise the processing logic. It was not queried from a blockchain, indexer, API, explorer, or protocol database.

## Motivation

The dataset is a small teaching artifact for demonstrating how raw protocol events become economic observations. Its main pedagogical goals are to show that:

1. an event log is not automatically an economics dataset;
2. assertion, dispute, and settlement records must be linked under explicit state and time rules;
3. every variable needs a formula, intuitive meaning, unit, source, and limitation;
4. reproducibility includes processed data, metadata, integrity checks, and figures; and
5. Trustworthy-AI claims require decision-time evidence and independent labels that are not present here.

## Composition

### Raw layer

`data/raw/uma_demo_event_log.csv` contains 12 rows across three event types:

- 5 `ASSERTION` records;
- 2 `DISPUTE` records; and
- 5 `SETTLEMENT` records.

Each record has a stable `event_id`, join key `episode_id`, UTC event and retrieval timestamps, evidence status, registered source ID, and source reference.

### Processed layer

`data/processed/uma_economic_episodes.csv` contains five rows. Each row records a claim, challenge window, proposer bond, explicit reward, dispute indicator, protocol outcome, procedural duration, evidence status, and the source references used to construct the row.

All definitions and formulas are in `docs/ECONOMIC_VARIABLE_DICTIONARY.md`.

## Collection and source status

The bundled raw input is a **synthetic fixture** registered as `SRC-SYNTH-001` in `data/source_registry.csv`. UMA protocol documentation is registered separately as a conceptual reference and is not misrepresented as the source of the observations.

For a real-data extension, every raw row must point to a transaction, event log, stable API response, or archived off-chain source; record retrieval time; document coverage and exclusions; and comply with the relevant source terms.

## Processing

`python src/reproduce.py`:

1. validates source registration, timestamp format, IDs, event states, and evidence status;
2. groups raw records by `episode_id`;
3. requires exactly one assertion and one settlement and at most one dispute;
4. checks temporal order and that disputes occur inside the challenge window;
5. constructs the processed episode table;
6. calculates descriptive metrics;
7. generates the research-question registry, figures, Croissant metadata, and checksums.

Four contract tests verify linkage, formulas, temporal validity, and source preservation.

## Intended uses

- classroom demonstration of event-to-episode data construction;
- testing extensions of the schema and validation logic;
- teaching reproducible research and data governance;
- planning economics studies and Trustworthy-AI benchmarks; and
- serving as a tiny fixture for continuous-integration checks.

## Out-of-scope and prohibited interpretations

The fixture must not be used to:

- estimate any UMA population statistic;
- claim that bonds, rewards, or deadlines cause disputes;
- infer that `accepted` means objectively true;
- infer that `disputer_won` is an independent ground-truth label;
- evaluate or rank AI models;
- estimate fairness, market concentration, welfare, or real financial returns; or
- support investment, legal, compliance, or operational decisions.

## Known limitations and missing data

- no real on-chain observations;
- no proposer/disputer addresses or entity linkage;
- no disputer-side bond, gas cost, token price source, or realized transfer;
- no complete sampling frame or unresolved/censored cases;
- no independent truth labels;
- no timestamped evidence snapshots as visible to a decision-maker;
- no AI actions, confidence, tool traces, repeated runs, or adversarial variants; and
- no protected-group, language, claim-category, or social-harm labels.

## Ethical and Trustworthy-AI considerations

Blockchain addresses can become personal data when linked to identities or behavior. A real-data extension should minimize entity linkage, document its lawful and ethical basis, avoid publishing unnecessary sensitive attributes, and distinguish addresses from verified persons or organizations.

For AI evaluation, information created after the decision time must be excluded from the agent input. Protocol rulings and independent truth must remain separate labels. High-stakes claims should specify human escalation rules and the social cost of false acceptance, false challenge, delay, and abstention.

## Maintenance and versioning

Changes to raw or processed schemas require updates to the variable dictionary, provenance, Croissant metadata, checksums, tests, and changelog in the same commit. A real dataset should be released under a new version and clearly separated from the synthetic fixture.

Issues and contributions: <https://github.com/Oracle4CEG/OracleEconLab/issues>
