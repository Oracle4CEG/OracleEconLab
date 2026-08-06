from pathlib import Path

from oracle_ledger.polygon_uma_ledger import build


if __name__ == "__main__":
    print(build(Path(".")))
