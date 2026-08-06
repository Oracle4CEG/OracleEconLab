.PHONY: reproduce compile-release test check

reproduce:
	python src/reproduce.py

test:
	python -m unittest discover -s tests -v

compile-release:
	python -m compileall -q src/oracle_ledger scripts

check: reproduce compile-release test
