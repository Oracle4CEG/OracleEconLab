"""Protocol entry point: UMA DVM 2.0 raw Ethereum collection via local reth."""

from oracle_ledger.__main__ import main


if __name__ == "__main__":
    import sys

    sys.argv.insert(1, "audit-ethereum")
    main()
