from pathlib import Path
from oracle_ledger.polygon_uma_flows import build

if __name__ == "__main__":
    print(build(Path(".")))
