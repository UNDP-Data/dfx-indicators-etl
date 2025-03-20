install:
	pip install --upgrade pip && pip install -e ".[dev]"
format:
	isort . --profile black --multi-line 3 && black .
lint:
	pylint dfpp/
test:
	python -m pytest tests/
