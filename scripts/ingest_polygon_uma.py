"""Protocol entry point: Polygon Polymarket Adapter, UMA OOV2 and ChildTunnel."""

from oracle_ledger.__main__ import main


if __name__ == "__main__":
    import sys

    sys.argv.insert(1, "ingest-polygon-uma")
    main()
