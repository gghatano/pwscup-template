.PHONY: install setup test lint run clean generate-data docker-build

PYTHON := python3
VENV := .venv
PIP := $(VENV)/bin/pip
PWSCUP := $(VENV)/bin/pwscup
PYTEST := $(VENV)/bin/pytest

install:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -e ".[dev]"

setup: install generate-data
	@echo "セットアップ完了"

test:
	PYTHONPATH=. $(PYTEST) -v

lint:
	$(VENV)/bin/ruff check src/ tests/

generate-data:
	$(VENV)/bin/python scripts/generate_data.py --output-dir data/original --sample-size 1000 --qualifying-size 10000 --final-size 10000
	$(VENV)/bin/python scripts/generate_auxiliary.py --input data/original/sample.csv --output-dir data/auxiliary --prefix sample
	cp data/original/sample.csv data/sample/sample_original.csv
	cp data/schema/schema.json data/sample/sample_schema.json
	cp data/auxiliary/sample_auxiliary.csv data/sample/sample_auxiliary.csv
	@echo "データ生成完了"

run: setup
	@echo ""
	@echo "=== ベースライン匿名化の評価 ==="
	$(PWSCUP) evaluate anonymize examples/anonymize_example/ --data-dir data/sample --schema-path data/schema/schema.json
	@echo ""
	@echo "=== デモ完了 ==="

docker-build:
	docker build -t pwscup-sandbox -f docker/Dockerfile.sandbox .

clean:
	rm -rf $(VENV) *.egg-info dist build
	rm -rf data/original/*.csv data/auxiliary/*.csv data/auxiliary/*.json
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
