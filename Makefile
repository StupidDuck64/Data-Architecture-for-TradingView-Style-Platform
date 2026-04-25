# ─────────────────────────────────────────────────────────────────────────────
# Makefile — convenience targets for development and production workflows
# ─────────────────────────────────────────────────────────────────────────────
.DEFAULT_GOAL := help

# ─── Development ─────────────────────────────────────────────────────────────

.PHONY: dev
dev: ## Start all services in development mode (hot-reload, no SSL)
	docker compose up -d

.PHONY: dev-build
dev-build: ## Rebuild and start in development mode
	docker compose up -d --build

.PHONY: dev-logs
dev-logs: ## Tail logs for all services
	docker compose logs -f

.PHONY: dev-down
dev-down: ## Stop all development services
	docker compose down

# ─── Production ──────────────────────────────────────────────────────────────

.PHONY: prod
prod: ## Start all services in production mode (SSL, multi-worker)
	docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

.PHONY: prod-build
prod-build: ## Rebuild and start in production mode
	docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

.PHONY: prod-down
prod-down: ## Stop all production services
	docker compose -f docker-compose.yml -f docker-compose.prod.yml down

# ─── Jobs ────────────────────────────────────────────────────────────────────

.PHONY: submit-jobs
submit-jobs: ## Submit Flink and Spark streaming jobs
	bash scripts/auto_submit_jobs.sh

# ─── Testing ─────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run unit and integration tests
	PYTHONPATH=. python -m pytest tests/ -m "unit or integration" -v

.PHONY: test-all
test-all: ## Run all tests including e2e (requires running services)
	PYTHONPATH=. python -m pytest tests/ -v

.PHONY: test-cov
test-cov: ## Run tests with coverage report
	PYTHONPATH=. python -m pytest tests/ -m "unit or integration" --cov=backend --cov-report=term-missing

# ─── Utilities ───────────────────────────────────────────────────────────────

.PHONY: status
status: ## Show status of all containers
	docker compose ps

.PHONY: clean
clean: ## Remove all containers, volumes, and networks
	docker compose down -v --remove-orphans

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
