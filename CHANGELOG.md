# Changelog

All notable changes to this teaching artifact are documented here.

## [Unreleased] — real-release-v1.5

### Added

- reusable real-data collectors and protocol adapters under `src/oracle_ledger/`;
- reproducible ingestion, QC, case-study, and analysis entry points under `scripts/`;
- versioned accountability and economic-observation schemas under `schemas/`;
- a pinned economic-release environment and package metadata; and
- repository guards preventing credentials, mounted datasets, generated
  figures, reports, and paper sources from entering Git.

### Changed

- replaced server-specific default data paths with repository-relative paths
  configurable through environment variables;
- updated repository URLs from the former personal location to the Oracle4CEG
  organization; and
- retained the original teaching fixture as the lightweight CI contract while
  keeping large real datasets external to Git.

## [0.1.0] — 2026-08-03

### Added

- a 12-record synthetic raw event log and explicit source registry;
- deterministic assertion–dispute–settlement episode construction;
- a governed 5-row processed economic episode table;
- 10 formula-defined summary metrics and 8 research-question specifications;
- a teaching dashboard and Economics/Trustworthy-AI research map as SVG;
- dataset card, provenance, expanded variable dictionary, roadmap, Croissant metadata, checksums, citation metadata, and a separate data-license notice;
- four standard-library contract tests; and
- GitHub Actions reproduction and committed-output checks.

### Changed

- replaced the earlier episode-only demonstration with a complete source-record-to-research-artifact pipeline;
- clarified throughout that the fixture is synthetic and cannot support UMA empirical or AI-benchmark claims.
