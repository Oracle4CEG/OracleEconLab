"""Reproduce the complete OracleEconLab teaching MVP with no third-party packages.

Pipeline:
    source registry + raw event records
        -> validated economic episodes
        -> descriptive metrics + research-question registry
        -> machine-readable metadata + checksums + SVG figures

All bundled observations are synthetic teaching fixtures. They are deliberately
small so students can inspect every transformation before replacing the input
with a fixed window of real, independently traceable protocol records.
"""

from __future__ import annotations

import csv
import hashlib
import html
import json
import textwrap
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median


ROOT = Path(__file__).resolve().parents[1]
RAW_EVENTS = ROOT / "data" / "raw" / "uma_demo_event_log.csv"
SOURCE_REGISTRY = ROOT / "data" / "source_registry.csv"
PROCESSED_EPISODES = ROOT / "data" / "processed" / "uma_economic_episodes.csv"
SUMMARY = ROOT / "outputs" / "summary.csv"
RESEARCH_QUESTIONS = ROOT / "outputs" / "research_questions.csv"
CROISSANT = ROOT / "data" / "croissant.json"
CHECKSUMS = ROOT / "data" / "checksums.sha256"
FIGURE_DIR = ROOT / "figures"

RAW_REQUIRED = {
    "event_id",
    "episode_id",
    "protocol",
    "event_type",
    "event_time_utc",
    "actor_role",
    "claim_text",
    "challenge_window_hours",
    "bond_usd",
    "reward_usd",
    "final_outcome",
    "evidence_status",
    "source_id",
    "source_ref",
    "retrieved_at_utc",
}

PROCESSED_FIELDS = [
    "episode_id",
    "protocol",
    "claim_text",
    "assertion_time_utc",
    "challenge_deadline_utc",
    "resolution_time_utc",
    "challenge_window_hours",
    "bond_usd",
    "reward_usd",
    "reward_to_bond_ratio",
    "was_disputed",
    "final_outcome",
    "resolution_hours",
    "evidence_status",
    "source_count",
    "source_refs",
]

RESEARCH_ROWS = [
    {
        "research_id": "E1",
        "domain": "Economics",
        "topic": "Costly information production",
        "question": "When do rewards and capital at risk attract costly monitoring?",
        "outcome": "was_disputed",
        "core_exposure_or_action": "reward_to_bond_ratio",
        "additional_data_needed": "Gas cost; evidence-acquisition cost; complete sampling window",
        "minimum_design": "Descriptive association then policy-based identification",
        "mvp_status": "Structure only",
    },
    {
        "research_id": "E2",
        "domain": "Economics",
        "topic": "Mechanism design",
        "question": "Which bond reward and deadline rules minimize false acceptance and wasteful disputes?",
        "outcome": "Social welfare and independent truth",
        "core_exposure_or_action": "bond_usd; reward_usd; challenge_window_hours",
        "additional_data_needed": "Independent truth; rule changes; realized transfers; social-loss weights",
        "minimum_design": "Structural model or quasi-experimental policy comparison",
        "mvp_status": "Not identified",
    },
    {
        "research_id": "E3",
        "domain": "Economics",
        "topic": "Public goods and market structure",
        "question": "Is decentralized verification supplied by a concentrated group of specialists?",
        "outcome": "Participation and challenger concentration",
        "core_exposure_or_action": "Disputer participation",
        "additional_data_needed": "Addresses; actor history; transfers; entity-linkage policy",
        "minimum_design": "Concentration and network analysis",
        "mvp_status": "Needs actor identifiers",
    },
    {
        "research_id": "E4",
        "domain": "Economics",
        "topic": "Delay and opportunity cost",
        "question": "How much economic friction is created by dispute resolution and locked capital?",
        "outcome": "resolution_hours and opportunity cost",
        "core_exposure_or_action": "bond_usd; final_outcome",
        "additional_data_needed": "Token prices; capital amounts; discount rate; censoring",
        "minimum_design": "Survival analysis and calibrated cost accounting",
        "mvp_status": "Delay observable; cost incomplete",
    },
    {
        "research_id": "T1",
        "domain": "Trustworthy AI",
        "topic": "Economically rational decisions",
        "question": "Can an agent choose Accept Investigate Challenge or Abstain under real costs?",
        "outcome": "Utility regret and protocol outcome",
        "core_exposure_or_action": "Agent action",
        "additional_data_needed": "Decision-time evidence; action costs; independent truth",
        "minimum_design": "Leakage-controlled agent benchmark",
        "mvp_status": "Benchmark interface only",
    },
    {
        "research_id": "T2",
        "domain": "Trustworthy AI",
        "topic": "Calibration and abstention",
        "question": "Does the agent abstain when evidence is weak and stakes are high?",
        "outcome": "Brier score; calibration error; selective risk",
        "core_exposure_or_action": "Confidence and abstention",
        "additional_data_needed": "Probabilities; truth labels; stake and harm categories",
        "minimum_design": "Risk-stratified calibration audit",
        "mvp_status": "Needs model runs",
    },
    {
        "research_id": "T3",
        "domain": "Trustworthy AI",
        "topic": "Evidence fidelity and no leakage",
        "question": "Do cited sources support the action using only information available at decision time?",
        "outcome": "Citation support and temporal leakage",
        "core_exposure_or_action": "Evidence trace",
        "additional_data_needed": "Timestamped evidence snapshots; model tool traces",
        "minimum_design": "Evidence entailment and temporal audit",
        "mvp_status": "Source schema present; snapshots absent",
    },
    {
        "research_id": "T4",
        "domain": "Trustworthy AI",
        "topic": "Robustness and human oversight",
        "question": "Does the agent remain stable under conflicting evidence and escalate high-risk cases?",
        "outcome": "pass^k robustness and escalation quality",
        "core_exposure_or_action": "Perturbation and human escalation",
        "additional_data_needed": "Adversarial variants; repeated runs; escalation labels",
        "minimum_design": "Controlled perturbation and repeated-run evaluation",
        "mvp_status": "Future extension",
    },
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid ISO-8601 timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"Timestamp must be UTC and timezone-aware: {value}")
    return parsed.astimezone(timezone.utc)


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_sources() -> set[str]:
    rows = read_csv(SOURCE_REGISTRY)
    required = {"source_id", "canonical_url", "retrieved_at_utc", "status"}
    if not rows or not required.issubset(rows[0]):
        raise ValueError("Source registry is empty or missing required columns.")
    ids = [row["source_id"] for row in rows]
    if len(ids) != len(set(ids)) or any(not source_id for source_id in ids):
        raise ValueError("source_id must be present and unique.")
    for row in rows:
        parse_utc(row["retrieved_at_utc"])
        if not row["canonical_url"].startswith("https://"):
            raise ValueError("Each registered source needs an HTTPS canonical URL.")
    return set(ids)


def load_raw_events(source_ids: set[str]) -> list[dict[str, str]]:
    with RAW_EVENTS.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = RAW_REQUIRED - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Raw event log missing columns: {sorted(missing)}")
        rows = list(reader)
    if not rows:
        raise ValueError("Raw event log is empty.")
    event_ids = [row["event_id"] for row in rows]
    if len(event_ids) != len(set(event_ids)):
        raise ValueError("event_id must be unique.")
    for row in rows:
        if row["event_type"] not in {"ASSERTION", "DISPUTE", "SETTLEMENT"}:
            raise ValueError(f"Invalid event_type in {row['event_id']}.")
        if row["evidence_status"] not in {"complete", "partial", "unavailable"}:
            raise ValueError(f"Invalid evidence_status in {row['event_id']}.")
        if row["source_id"] not in source_ids:
            raise ValueError(f"Unregistered source_id in {row['event_id']}.")
        parse_utc(row["event_time_utc"])
        parse_utc(row["retrieved_at_utc"])
    return rows


def build_episodes(events: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for event in events:
        grouped[event["episode_id"]].append(event)

    evidence_rank = {"complete": 0, "partial": 1, "unavailable": 2}
    episodes: list[dict[str, str]] = []

    for episode_id in sorted(grouped):
        rows = sorted(grouped[episode_id], key=lambda row: parse_utc(row["event_time_utc"]))
        assertions = [row for row in rows if row["event_type"] == "ASSERTION"]
        disputes = [row for row in rows if row["event_type"] == "DISPUTE"]
        settlements = [row for row in rows if row["event_type"] == "SETTLEMENT"]
        if len(assertions) != 1 or len(settlements) != 1 or len(disputes) > 1:
            raise ValueError(
                f"{episode_id} needs one ASSERTION, one SETTLEMENT, and at most one DISPUTE."
            )

        assertion, settlement = assertions[0], settlements[0]
        protocols = {row["protocol"] for row in rows}
        if len(protocols) != 1:
            raise ValueError(f"Protocol changes within {episode_id}.")

        assertion_time = parse_utc(assertion["event_time_utc"])
        resolution_time = parse_utc(settlement["event_time_utc"])
        if resolution_time <= assertion_time:
            raise ValueError(f"Settlement must follow assertion in {episode_id}.")

        try:
            challenge_hours = float(assertion["challenge_window_hours"])
            bond = float(assertion["bond_usd"])
            reward = float(assertion["reward_usd"])
        except ValueError as exc:
            raise ValueError(f"Invalid economic magnitude in {episode_id}.") from exc
        if challenge_hours <= 0 or bond <= 0 or reward < 0:
            raise ValueError(f"Invalid challenge window, bond, or reward in {episode_id}.")

        deadline = assertion_time + timedelta(hours=challenge_hours)
        disputed = bool(disputes)
        outcome = settlement["final_outcome"]
        if disputed:
            dispute_time = parse_utc(disputes[0]["event_time_utc"])
            if not assertion_time < dispute_time <= deadline:
                raise ValueError(f"Dispute outside the valid window in {episode_id}.")
            if dispute_time >= resolution_time:
                raise ValueError(f"Dispute must precede settlement in {episode_id}.")
            if outcome not in {"proposer_won", "disputer_won"}:
                raise ValueError(f"Disputed {episode_id} needs a contested outcome.")
        elif outcome != "accepted":
            raise ValueError(f"Undisputed {episode_id} must have outcome accepted.")
        elif resolution_time < deadline:
            raise ValueError(f"Undisputed {episode_id} settles before its challenge deadline.")

        evidence_status = max(
            (row["evidence_status"] for row in rows), key=evidence_rank.__getitem__
        )
        source_refs = sorted({row["source_ref"] for row in rows})
        duration = (resolution_time - assertion_time).total_seconds() / 3600

        episodes.append(
            {
                "episode_id": episode_id,
                "protocol": assertion["protocol"],
                "claim_text": assertion["claim_text"],
                "assertion_time_utc": format_utc(assertion_time),
                "challenge_deadline_utc": format_utc(deadline),
                "resolution_time_utc": format_utc(resolution_time),
                "challenge_window_hours": f"{challenge_hours:.1f}",
                "bond_usd": f"{bond:.2f}",
                "reward_usd": f"{reward:.2f}",
                "reward_to_bond_ratio": f"{reward / bond:.4f}",
                "was_disputed": str(disputed).lower(),
                "final_outcome": outcome,
                "resolution_hours": f"{duration:.1f}",
                "evidence_status": evidence_status,
                "source_count": str(len(source_refs)),
                "source_refs": "|".join(source_refs),
            }
        )
    return episodes


def summarize(episodes: list[dict[str, str]]) -> list[dict[str, str]]:
    count = len(episodes)
    disputed = sum(row["was_disputed"] == "true" for row in episodes)
    disputer_wins = sum(row["final_outcome"] == "disputer_won" for row in episodes)
    complete = sum(row["evidence_status"] == "complete" for row in episodes)
    bonds = [float(row["bond_usd"]) for row in episodes]
    rewards = [float(row["reward_usd"]) for row in episodes]
    ratios = [float(row["reward_to_bond_ratio"]) for row in episodes]
    windows = [float(row["challenge_window_hours"]) for row in episodes]
    durations = [float(row["resolution_hours"]) for row in episodes]

    values = [
        ("episodes", str(count)),
        ("disputed_episodes", str(disputed)),
        ("dispute_rate", f"{disputed / count:.3f}"),
        ("median_bond_usd", f"{median(bonds):.1f}"),
        ("median_reward_usd", f"{median(rewards):.1f}"),
        ("median_reward_to_bond_ratio", f"{median(ratios):.3f}"),
        ("median_challenge_window_hours", f"{median(windows):.1f}"),
        ("median_resolution_hours", f"{median(durations):.1f}"),
        (
            "successful_challenge_rate",
            f"{disputer_wins / disputed:.3f}" if disputed else "NA",
        ),
        ("complete_evidence_rate", f"{complete / count:.3f}"),
    ]
    return [{"metric": metric, "value": value} for metric, value in values]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def svg_text(
    x: float,
    y: float,
    value: str,
    size: int = 18,
    color: str = "#172033",
    weight: int = 400,
    anchor: str = "start",
) -> str:
    return (
        f'<text x="{x}" y="{y}" font-family="Inter,Arial,sans-serif" '
        f'font-size="{size}" fill="{color}" font-weight="{weight}" '
        f'text-anchor="{anchor}">{html.escape(value)}</text>'
    )


def svg_rect(
    x: float,
    y: float,
    width: float,
    height: float,
    fill: str,
    radius: int = 18,
    stroke: str = "none",
) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="{radius}" '
        f'fill="{fill}" stroke="{stroke}"/>'
    )


def write_dashboard(episodes: list[dict[str, str]], summary_rows: list[dict[str, str]]) -> None:
    values = {row["metric"]: row["value"] for row in summary_rows}
    width, height = 1280, 800
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        svg_rect(0, 0, width, height, "#F4F7FB", 0),
        svg_text(52, 58, "OracleEconLab · Teaching MVP dashboard", 30, "#101828", 700),
        svg_text(52, 88, "Synthetic observations · computation demo only · not empirical UMA evidence", 16, "#B54708", 600),
    ]

    panels = [(48, 120), (660, 120), (48, 410), (660, 410)]
    for x, y in panels:
        parts.append(svg_rect(x, y, 572, 330 if y == 410 else 250, "#FFFFFF", 20, "#D8E2F0"))

    # Panel 1: costly monitoring supply.
    x, y = panels[0]
    parts += [
        svg_text(x + 28, y + 42, "1 · Costly monitoring supply", 20, "#172B4D", 700),
        svg_text(x + 28, y + 72, "Was a valid dispute recorded?", 15, "#667085"),
    ]
    dispute_rate = float(values["dispute_rate"])
    bar_x, bar_y, bar_w = x + 28, y + 108, 516
    parts.append(svg_rect(bar_x, bar_y, bar_w, 44, "#E8EDF5", 12))
    parts.append(svg_rect(bar_x, bar_y, bar_w * dispute_rate, 44, "#6C5CE7", 12))
    parts += [
        svg_text(bar_x + 12, bar_y + 29, f"Disputed {dispute_rate:.0%}", 15, "#FFFFFF", 700),
        svg_text(bar_x + bar_w - 12, bar_y + 29, f"Undisputed {1-dispute_rate:.0%}", 15, "#344054", 600, "end"),
        svg_text(x + 28, y + 194, f"{values['disputed_episodes']} of {values['episodes']} episodes received costly monitoring", 16, "#344054", 600),
        svg_text(x + 28, y + 222, "Dispute incidence is not an error rate.", 14, "#B54708", 600),
    ]

    # Panel 2: bond versus reward.
    x, y = panels[1]
    bond = float(values["median_bond_usd"])
    reward = float(values["median_reward_usd"])
    max_value = max(bond, reward) * 1.15
    base_y, chart_h = y + 210, 118
    parts += [
        svg_text(x + 28, y + 42, "2 · Capital and explicit incentive", 20, "#172B4D", 700),
        svg_text(x + 28, y + 72, "Sample medians in USD", 15, "#667085"),
    ]
    for bar_x, value, label, fill in [
        (x + 116, bond, "Proposer bond", "#176B87"),
        (x + 356, reward, "Reward", "#F4A261"),
    ]:
        bar_h = chart_h * value / max_value
        parts.append(svg_rect(bar_x, base_y - bar_h, 100, bar_h, fill, 10))
        parts.append(svg_text(bar_x + 50, base_y - bar_h - 12, f"${value:,.0f}", 19, "#172B4D", 700, "middle"))
        parts.append(svg_text(bar_x + 50, base_y + 25, label, 14, "#475467", 600, "middle"))

    # Panel 3: episode delay.
    x, y = panels[2]
    parts += [
        svg_text(x + 28, y + 42, "3 · Institutional delay", 20, "#172B4D", 700),
        svg_text(x + 28, y + 70, "Hours from assertion to protocol settlement", 15, "#667085"),
    ]
    max_duration = max(float(row["resolution_hours"]) for row in episodes)
    outcome_colors = {"accepted": "#36B37E", "proposer_won": "#176B87", "disputer_won": "#E05A47"}
    for index, row in enumerate(episodes):
        row_y = y + 108 + index * 42
        duration = float(row["resolution_hours"])
        parts.append(svg_text(x + 28, row_y + 17, row["episode_id"], 14, "#475467", 600))
        parts.append(svg_rect(x + 122, row_y, 370, 22, "#EDF1F7", 6))
        parts.append(svg_rect(x + 122, row_y, 370 * duration / max_duration, 22, outcome_colors[row["final_outcome"]], 6))
        parts.append(svg_text(x + 510, row_y + 17, f"{duration:.0f} h", 14, "#344054", 700, "end"))
    parts.append(svg_text(x + 28, y + 316, "Longer resolution can imply more capital lock-up.", 14, "#667085"))

    # Panel 4: auditability and benchmark readiness.
    x, y = panels[3]
    complete_rate = float(values["complete_evidence_rate"])
    parts += [
        svg_text(x + 28, y + 42, "4 · Trustworthy-AI readiness", 20, "#172B4D", 700),
        svg_text(x + 28, y + 70, "What this MVP can and cannot support", 15, "#667085"),
        svg_text(x + 28, y + 112, "Complete source trail", 15, "#344054", 600),
        svg_text(x + 512, y + 112, f"{complete_rate:.0%}", 15, "#067647", 700, "end"),
        svg_rect(x + 28, y + 126, 484, 18, "#E8EDF5", 6),
        svg_rect(x + 28, y + 126, 484 * complete_rate, 18, "#36B37E", 6),
    ]
    readiness = [
        ("Economic episode construction", "READY", "#067647", "#ECFDF3"),
        ("Decision-time evidence snapshots", "MISSING", "#B54708", "#FFFAEB"),
        ("Independent ground-truth labels", "MISSING", "#B42318", "#FEF3F2"),
    ]
    for index, (label, status, color, bg) in enumerate(readiness):
        row_y = y + 178 + index * 45
        parts.append(svg_text(x + 28, row_y + 18, label, 14, "#344054", 600))
        parts.append(svg_rect(x + 410, row_y, 102, 28, bg, 14))
        parts.append(svg_text(x + 461, row_y + 19, status, 12, color, 700, "middle"))
    parts.append(svg_text(x + 28, y + 316, "Therefore: pipeline-ready, not yet an AI benchmark.", 14, "#6941C6", 700))

    parts.append("</svg>")
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    (FIGURE_DIR / "demo_dashboard.svg").write_text("\n".join(parts) + "\n", encoding="utf-8")


def write_research_map() -> None:
    width, height = 1360, 1180
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        svg_rect(0, 0, width, height, "#F6F8FC", 0),
        svg_text(56, 60, "What research can grow from an economic episode table?", 31, "#101828", 700),
        svg_text(56, 92, "Each card states a question and the extra evidence required before making the claim.", 16, "#667085"),
        svg_rect(56, 120, 1248, 90, "#172B4D", 20),
        svg_text(680, 158, "Source-linked assertion → challenge → settlement episodes", 22, "#FFFFFF", 700, "middle"),
        svg_text(680, 186, "bonds · rewards · disputes · outcomes · delay · evidence status", 15, "#DDE7F5", 400, "middle"),
        svg_text(56, 256, "ECONOMICS", 20, "#176B87", 800),
        svg_text(716, 256, "TRUSTWORTHY AI", 20, "#6941C6", 800),
    ]

    for index, row in enumerate(RESEARCH_ROWS):
        left = row["domain"] == "Economics"
        col_index = index if left else index - 4
        x = 56 if left else 716
        y = 280 + col_index * 190
        accent = "#176B87" if left else "#6941C6"
        pale = "#EAF5F7" if left else "#F1EDFF"
        parts.append(svg_rect(x, y, 588, 164, "#FFFFFF", 18, "#D8E2F0"))
        parts.append(svg_rect(x, y, 10, 164, accent, 10))
        parts.append(svg_rect(x + 28, y + 20, 48, 28, pale, 14))
        parts.append(svg_text(x + 52, y + 40, row["research_id"], 13, accent, 800, "middle"))
        parts.append(svg_text(x + 88, y + 41, row["topic"], 18, "#172B4D", 700))
        question_lines = textwrap.wrap(row["question"], width=68)
        for line_index, line in enumerate(question_lines[:2]):
            parts.append(svg_text(x + 28, y + 76 + line_index * 22, line, 14, "#344054", 500))
        add_lines = textwrap.wrap("ADD: " + row["additional_data_needed"], width=72)
        for line_index, line in enumerate(add_lines[:2]):
            parts.append(svg_text(x + 28, y + 127 + line_index * 20, line, 12, "#B54708", 600))

    roadmap_y = 1080
    parts += [
        svg_text(56, roadmap_y - 45, "STUDENT ROADMAP", 18, "#344054", 800),
        svg_rect(56, roadmap_y, 285, 54, "#DCEBFF", 14),
        svg_rect(377, roadmap_y, 285, 54, "#DDF6EC", 14),
        svg_rect(698, roadmap_y, 285, 54, "#FFF0D9", 14),
        svg_rect(1019, roadmap_y, 285, 54, "#F0E8FF", 14),
        svg_text(198, roadmap_y + 34, "MVP · inspect every row", 14, "#175CD3", 700, "middle"),
        svg_text(519, roadmap_y + 34, "Real window · verify sources", 14, "#067647", 700, "middle"),
        svg_text(840, roadmap_y + 34, "Economics · identify effects", 14, "#B54708", 700, "middle"),
        svg_text(1162, roadmap_y + 34, "AI · benchmark decisions", 14, "#6941C6", 700, "middle"),
    ]
    for start in (341, 662, 983):
        parts.append(f'<path d="M {start} {roadmap_y + 27} L {start + 36} {roadmap_y + 27}" stroke="#98A2B3" stroke-width="3"/>')
        parts.append(f'<path d="M {start + 28} {roadmap_y + 20} L {start + 36} {roadmap_y + 27} L {start + 28} {roadmap_y + 34}" fill="none" stroke="#98A2B3" stroke-width="3"/>')

    parts.append(svg_text(680, 1164, "Synthetic teaching data demonstrate structure—not empirical findings.", 14, "#B42318", 700, "middle"))
    parts.append("</svg>")
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    (FIGURE_DIR / "research_map.svg").write_text("\n".join(parts) + "\n", encoding="utf-8")


def build_croissant() -> dict[str, object]:
    resources = [
        ("raw_events", RAW_EVENTS, "Raw synthetic event records"),
        ("source_registry", SOURCE_REGISTRY, "Source and provenance registry"),
        ("processed_episodes", PROCESSED_EPISODES, "Processed economic episode table"),
        ("summary", SUMMARY, "Reproduced descriptive metrics"),
        ("research_questions", RESEARCH_QUESTIONS, "Research-question and data-gap registry"),
    ]
    distributions = []
    for resource_id, path, description in resources:
        relative = path.relative_to(ROOT).as_posix()
        distributions.append(
            {
                "@type": "cr:FileObject",
                "@id": resource_id,
                "name": path.name,
                "description": description,
                "contentUrl": f"https://raw.githubusercontent.com/sunshineluyao/OracleEconLab/main/{relative}",
                "encodingFormat": "text/csv",
                "sha256": sha256(path),
            }
        )

    field_types = {
        "episode_id": "sc:Text",
        "protocol": "sc:Text",
        "claim_text": "sc:Text",
        "assertion_time_utc": "sc:DateTime",
        "challenge_deadline_utc": "sc:DateTime",
        "resolution_time_utc": "sc:DateTime",
        "challenge_window_hours": "sc:Float",
        "bond_usd": "sc:Float",
        "reward_usd": "sc:Float",
        "reward_to_bond_ratio": "sc:Float",
        "was_disputed": "sc:Boolean",
        "final_outcome": "sc:Text",
        "resolution_hours": "sc:Float",
        "evidence_status": "sc:Text",
        "source_count": "sc:Integer",
        "source_refs": "sc:Text",
    }
    fields = [
        {
            "@type": "cr:Field",
            "@id": f"episodes/{name}",
            "name": name,
            "dataType": data_type,
            "source": {
                "fileObject": {"@id": "processed_episodes"},
                "extract": {"column": name},
            },
        }
        for name, data_type in field_types.items()
    ]
    return {
        "@context": {
            "@language": "en",
            "@vocab": "https://schema.org/",
            "sc": "https://schema.org/",
            "cr": "http://mlcommons.org/croissant/",
        },
        "@type": "sc:Dataset",
        "name": "OracleEconLab Teaching MVP",
        "description": "Five synthetic UMA-style economic episodes for teaching reproducible data construction. Not empirical protocol data.",
        "url": "https://github.com/sunshineluyao/OracleEconLab",
        "license": "https://creativecommons.org/licenses/by/4.0/",
        "version": "0.1.0",
        "datePublished": "2026-08-03",
        "conformsTo": "http://mlcommons.org/croissant/1.0",
        "creator": {"@type": "sc:Organization", "name": "Oracle4CEG"},
        "distribution": distributions,
        "recordSet": [
            {
                "@type": "cr:RecordSet",
                "@id": "episodes",
                "name": "Economic episodes",
                "description": "One row per complete assertion-dispute-settlement episode.",
                "field": fields,
            }
        ],
    }


def write_checksums(paths: list[Path]) -> None:
    lines = [f"{sha256(path)}  {path.relative_to(ROOT).as_posix()}" for path in paths]
    CHECKSUMS.write_text("\n".join(lines) + "\n", encoding="utf-8")


def reproduce() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    source_ids = validate_sources()
    events = load_raw_events(source_ids)
    episodes = build_episodes(events)
    summary_rows = summarize(episodes)

    write_csv(PROCESSED_EPISODES, PROCESSED_FIELDS, episodes)
    write_csv(SUMMARY, ["metric", "value"], summary_rows)
    write_csv(RESEARCH_QUESTIONS, list(RESEARCH_ROWS[0]), RESEARCH_ROWS)
    write_dashboard(episodes, summary_rows)
    write_research_map()

    CROISSANT.write_text(
        json.dumps(build_croissant(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    write_checksums(
        [RAW_EVENTS, SOURCE_REGISTRY, PROCESSED_EPISODES, SUMMARY, RESEARCH_QUESTIONS]
    )
    return episodes, summary_rows


def main() -> None:
    episodes, summary_rows = reproduce()
    print(f"Stage 1/4: validated {sum(1 for _ in read_csv(RAW_EVENTS))} raw event records")
    print(f"Stage 2/4: built {len(episodes)} economic episodes")
    print(f"Stage 3/4: wrote {len(summary_rows)} metrics and {len(RESEARCH_ROWS)} research questions")
    print("Stage 4/4: wrote governance metadata, checksums, and 2 SVG figures")
    print("NOTE: bundled observations are synthetic teaching fixtures, not UMA findings.")


if __name__ == "__main__":
    main()
