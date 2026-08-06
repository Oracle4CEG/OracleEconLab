"""Protocol entry point: Chainlink Staking v0.2 raw Ethereum collection via reth."""

from oracle_ledger.__main__ import main


if __name__ == "__main__":
    import sys

    sys.argv.insert(1, "ingest-chainlink-staking-v02")
    main()
