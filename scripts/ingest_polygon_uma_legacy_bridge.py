"""Collect the discovered historical Polygon UMA ChildTunnel."""

from oracle_ledger.__main__ import main


if __name__ == "__main__":
    import sys

    sys.argv.insert(1, "ingest-polygon-uma-legacy-bridge")
    main()
