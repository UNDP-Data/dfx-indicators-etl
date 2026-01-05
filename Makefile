install:
	pip install --upgrade pip && pip install -e ".[dev]"
format:
	isort . --profile black --multi-line 3 && black .
lint:
	pylint src
test:
	python -m pytest tests/
.PHONY: clean
clean:
	rm -rf dist build src/*.egg-info
