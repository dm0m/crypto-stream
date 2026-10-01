# Developer checks. Run `make check` before committing or deploying.
# launch.sh stays a pure runtime entrypoint and never runs any of these.

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: help check check-all lint format format-check typecheck test test-integration test-all run

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

check: lint format-check typecheck test  ## Lint, format check, type check, unit tests (the pre-commit gate)

check-all: check test-integration  ## check plus the integration suite (needs Docker)

lint:  ## ruff check
	$(PYTHON) -m ruff check .

format:  ## ruff format (rewrites files)
	$(PYTHON) -m ruff format .

format-check:  ## ruff format --check (no rewrites)
	$(PYTHON) -m ruff format --check .

typecheck:  ## mypy strict on app and tests (paths from pyproject)
	$(PYTHON) -m mypy

test:  ## Unit tests (integration excluded by pyproject addopts)
	$(PYTHON) -m pytest

test-integration:  ## Integration tests via testcontainers (needs Docker)
	$(PYTHON) -m pytest -m integration tests/integration

test-all: test test-integration  ## Unit then integration

run:  ## Start compose, migrate, run both services
	./launch.sh

run-api:  ## API with reload, host and port from .env
	set -a; . ./.env; set +a; $(PYTHON) -m fastapi dev --host $$API_HOST --port $$API_PORT
 