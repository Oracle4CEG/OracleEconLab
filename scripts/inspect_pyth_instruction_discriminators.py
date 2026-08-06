#!/usr/bin/env python3
"""Inventory Pyth integrity-pool instruction discriminators from raw history."""
from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path

from oracle_ledger.pyth_history import PROGRAM_ID
from oracle_ledger.pyth_ois import INSTRUCTION_BY_DISCRIMINATOR, base58_decode


ROOT = Path(__file__).resolve().parents[1]


def instruction_log_name(transaction: dict, outer_index: int) -> str | None:
    """Return the top-level Anchor instruction name for one outer instruction."""
    program_invocation = 0
    depth = 0
    for line in (transaction.get("meta") or {}).get("logMessages") or []:
        if line.startswith(f"Program {PROGRAM_ID} invoke [1]"):
            if depth == 0:
                program_invocation += 1
                depth = 1
            continue
        if depth and line == f"Program {PROGRAM_ID} success":
            depth = 0
            continue
        if depth and line.startswith("Program log: Instruction: "):
            if program_invocation - 1 == outer_index:
                return line.removeprefix("Program log: Instruction: ")
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=ROOT / "data/raw/pyth_ois/history/transactions",
    )
    args = parser.parse_args()

    counts: Counter[str] = Counter()
    body_lengths: dict[str, Counter[int]] = {}
    outcomes: dict[str, Counter[str]] = {}
    examples: dict[str, dict] = {}
    for path in sorted(args.raw_dir.glob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                envelope = json.loads(line)
                signature = envelope["signature_record"]["signature"]
                transaction = envelope["transaction"]
                message = transaction["transaction"]["message"]
                for outer_index, instruction in enumerate(message.get("instructions") or []):
                    if instruction.get("programId") != PROGRAM_ID or "data" not in instruction:
                        continue
                    raw = base58_decode(str(instruction["data"]))
                    discriminator = raw[:8].hex()
                    counts[discriminator] += 1
                    body_lengths.setdefault(discriminator, Counter())[len(raw) - 8] += 1
                    outcome = "success" if (transaction.get("meta") or {}).get("err") is None else "failed"
                    outcomes.setdefault(discriminator, Counter())[outcome] += 1
                    if discriminator not in examples:
                        examples[discriminator] = {
                            "known_name": INSTRUCTION_BY_DISCRIMINATOR.get(raw[:8]),
                            "log_name": instruction_log_name(transaction, outer_index),
                            "signature": signature,
                            "slot": transaction["slot"],
                            "block_time": transaction["blockTime"],
                            "outer_index": outer_index,
                            "body_hex": raw[8:].hex(),
                            "accounts": instruction.get("accounts") or [],
                        }

    for discriminator, count in counts.most_common():
        print(json.dumps({
            "discriminator": discriminator,
            "count": count,
            "body_lengths": dict(sorted(body_lengths[discriminator].items())),
            "outcomes": dict(sorted(outcomes[discriminator].items())),
            **examples[discriminator],
        }, sort_keys=True))


if __name__ == "__main__":
    main()
