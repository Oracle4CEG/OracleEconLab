from pathlib import Path
from oracle_ledger.chainlink_evidence_build import build

if __name__ == "__main__":
    print(build(Path(".")))
