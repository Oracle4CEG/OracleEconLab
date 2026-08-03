"""Contract tests for the source-to-episode teaching pipeline."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import reproduce  # noqa: E402


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        source_ids = reproduce.validate_sources()
        cls.events = reproduce.load_raw_events(source_ids)
        cls.episodes = reproduce.build_episodes(cls.events)
        cls.summary = {
            row["metric"]: row["value"] for row in reproduce.summarize(cls.episodes)
        }

    def test_raw_records_are_joined_into_complete_episodes(self) -> None:
        self.assertEqual(len(self.events), 12)
        self.assertEqual(len(self.episodes), 5)
        self.assertEqual(len({row["episode_id"] for row in self.episodes}), 5)

    def test_economic_metrics_match_the_documented_fixture(self) -> None:
        self.assertEqual(self.summary["dispute_rate"], "0.400")
        self.assertEqual(self.summary["median_bond_usd"], "150.0")
        self.assertEqual(self.summary["median_reward_to_bond_ratio"], "0.125")
        self.assertEqual(self.summary["successful_challenge_rate"], "0.500")

    def test_disputes_occur_before_deadline_and_resolution(self) -> None:
        grouped = {}
        for event in self.events:
            grouped.setdefault(event["episode_id"], []).append(event)
        for row in self.episodes:
            events = grouped[row["episode_id"]]
            disputes = [event for event in events if event["event_type"] == "DISPUTE"]
            if disputes:
                dispute_time = reproduce.parse_utc(disputes[0]["event_time_utc"])
                deadline = reproduce.parse_utc(row["challenge_deadline_utc"])
                resolution = reproduce.parse_utc(row["resolution_time_utc"])
                self.assertLessEqual(dispute_time, deadline)
                self.assertLess(dispute_time, resolution)

    def test_every_episode_preserves_traceable_source_references(self) -> None:
        for row in self.episodes:
            self.assertGreaterEqual(int(row["source_count"]), 2)
            self.assertIn("synthetic://", row["source_refs"])


if __name__ == "__main__":
    unittest.main()
