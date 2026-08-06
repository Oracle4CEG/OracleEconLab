from pathlib import Path

from oracle_ledger.uma_crosschain import build


if __name__ == "__main__":
    print(build(Path(".")))
