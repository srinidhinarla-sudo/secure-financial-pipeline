VENV   := .venv/bin
PYTHON := $(VENV)/python3.11
JAVA17 := /opt/homebrew/opt/openjdk@17

export JAVA_HOME         := $(JAVA17)
export PATH              := $(JAVA17)/bin:$(PATH)
export PYSPARK_PYTHON    := $(PYTHON)
export PYSPARK_DRIVER_PYTHON := $(PYTHON)
export PIPELINE_DATA_DIR := $(shell pwd)/data
export PIPELINE_DELTA_DIR:= $(shell pwd)/data/delta
export SLACK_WEBHOOK_URL :=

.PHONY: install test run lint docker-up docker-down clean

install:
	python3.11 -m venv .venv
	$(VENV)/pip install --upgrade pip
	$(VENV)/pip install -e ".[dev]"

test:
	$(VENV)/pytest tests/ -v --tb=short

lint:
	$(VENV)/ruff check src/ dags/ tests/ scripts/
	$(VENV)/ruff format --check src/ dags/ tests/ scripts/

run:
	mkdir -p data/delta
	$(PYTHON) scripts/run_pipeline.py

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	rm -rf data/delta __pycache__ .pytest_cache .ruff_cache
	find . -name "*.pyc" -delete
