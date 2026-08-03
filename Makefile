.PHONY: reproduce test check

reproduce:
	python src/reproduce.py

test:
	python -m unittest discover -s tests -v

check: reproduce test
