from pathlib import Path
from oracle_ledger.ledger_build import build_uma

if __name__ == "__main__":
    print(build_uma(Path(".")))
