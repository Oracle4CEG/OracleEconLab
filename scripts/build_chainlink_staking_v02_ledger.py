from pathlib import Path
from oracle_ledger.ledger_build import build_chainlink

if __name__ == "__main__":
    print(build_chainlink(Path(".")))
