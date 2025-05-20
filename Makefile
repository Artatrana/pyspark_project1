.PHONY: install test run

install:
	pip install -r requirements.txt

test:
	python -m unittest discover -s tests

run:
	python src/main.py