"""Validate the tiny teaching dataset and reproduce its economic summary."""

from __future__ import annotations

import csv
from pathlib import Path
from statistics import median


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "uma_demo_episodes.csv"
OUTPUT = ROOT / "outputs" / "summary.csv"

REQUIRED_COLUMNS = {
    "episode_id",
    "protocol",
    "bond_usd",
    "reward_usd",
    "was_disputed",
    "final_outcome",
    "resolution_hours",
    "evidence_status",
    "source_ref",
}


def load_and_validate() -> list[dict[str, str]]:
    with INPUT.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")
        rows = list(reader)

    if not rows:
        raise ValueError("The dataset is empty.")
    if len({row["episode_id"] for row in rows}) != len(rows):
        raise ValueError("episode_id must be unique.")

    for row in rows:
        if row["was_disputed"] not in {"true", "false"}:
            raise ValueError("was_disputed must be 'true' or 'false'.")
        if row["evidence_status"] not in {"complete", "partial", "unavailable"}:
            raise ValueError("Invalid evidence_status.")
        if row["final_outcome"] not in {"accepted", "proposer_won", "disputer_won"}:
            raise ValueError("Invalid final_outcome.")

        disputed = row["was_disputed"] == "true"
        if disputed and row["final_outcome"] == "accepted":
            raise ValueError("A disputed episode cannot have final_outcome='accepted'.")
        if not disputed and row["final_outcome"] != "accepted":
            raise ValueError("An undisputed episode must have final_outcome='accepted'.")

        bond = float(row["bond_usd"])
        reward = float(row["reward_usd"])
        duration = float(row["resolution_hours"])
        if bond <= 0:
            raise ValueError("bond_usd must be positive for reward-to-bond ratios.")
        if reward < 0 or duration < 0:
            raise ValueError("reward_usd and resolution_hours cannot be negative.")
    return rows


def summarize(rows: list[dict[str, str]]) -> list[tuple[str, str]]:
    count = len(rows)
    disputed = sum(row["was_disputed"] == "true" for row in rows)
    complete = sum(row["evidence_status"] == "complete" for row in rows)
    disputer_wins = sum(row["final_outcome"] == "disputer_won" for row in rows)

    bonds = [float(row["bond_usd"]) for row in rows]
    rewards = [float(row["reward_usd"]) for row in rows]
    reward_to_bond = [
        float(row["reward_usd"]) / float(row["bond_usd"]) for row in rows
    ]

    return [
        ("episodes", str(count)),
        ("disputed_episodes", str(disputed)),
        ("dispute_rate", f"{disputed / count:.3f}"),
        ("median_bond_usd", f"{median(bonds):.1f}"),
        ("median_reward_usd", f"{median(rewards):.1f}"),
        ("median_reward_to_bond_ratio", f"{median(reward_to_bond):.3f}"),
        (
            "median_resolution_hours",
            f"{median(float(row['resolution_hours']) for row in rows):.1f}",
        ),
        (
            "successful_challenge_rate",
            f"{disputer_wins / disputed:.3f}" if disputed else "NA",
        ),
        ("complete_evidence_rate", f"{complete / count:.3f}"),
    ]


def main() -> None:
    summary = summarize(load_and_validate())
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\\n")
        writer.writerow(["metric", "value"])
        writer.writerows(summary)
    print(f"Reproduced {OUTPUT.relative_to(ROOT)} from {INPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
