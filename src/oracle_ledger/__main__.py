from __future__ import annotations

import argparse

from .ethereum_audit import register_subcommand
from .chainlink_staking import register_subcommand as register_chainlink_subcommand
from .polygon_uma import register_subcommand as register_polygon_uma_subcommand
from .chainlink_evidence import register_subcommand as register_chainlink_evidence_subcommand
from .polygon_uma_flows import register_subcommand as register_polygon_uma_flows_subcommand


def main() -> None:
    parser = argparse.ArgumentParser(prog="oracle-ledger")
    subparsers = parser.add_subparsers(dest="command", required=True)
    register_subcommand(subparsers)
    register_chainlink_subcommand(subparsers)
    register_polygon_uma_subcommand(subparsers)
    register_chainlink_evidence_subcommand(subparsers)
    register_polygon_uma_flows_subcommand(subparsers)
    args = parser.parse_args()
    report = args.handler(args)
    print(report)


if __name__ == "__main__":
    main()
