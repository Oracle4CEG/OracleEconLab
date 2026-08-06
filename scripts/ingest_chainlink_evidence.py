"""Chainlink LINK-flow and ETH/USD feed supporting evidence entry point."""

from oracle_ledger.__main__ import main


if __name__ == "__main__":
    import sys

    sys.argv.insert(1, "ingest-chainlink-evidence")
    main()
