# DuckDB API Service - Makefile
# ================================

.PHONY: help install dev test test-v test-cov e2e server server-bg lint format clean docker-build docker-up docker-down

# Default admin key for development
ADMIN_API_KEY ?= test-admin-key
PORT ?= 8000

# Colors for output
CYAN := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RESET := \033[0m

help: ## Show this help
	@echo "$(CYAN)DuckDB API Service$(RESET)"
	@echo "=================="
	@echo ""
	@echo "$(GREEN)Available targets:$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-15s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(GREEN)Examples:$(RESET)"
	@echo "  make install        # First time setup"
	@echo "  make test           # Run unit tests"
	@echo "  make server         # Start dev server"
	@echo "  make e2e            # Run e2e tests (server must be running)"

# ============================================
# Setup
# ============================================

install: ## Install dependencies (creates venv if needed)
	@if [ ! -d ".venv" ]; then \
		echo "$(YELLOW)Creating virtual environment...$(RESET)"; \
		python3 -m venv .venv; \
	fi
	@echo "$(YELLOW)Installing dependencies...$(RESET)"
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q -r requirements.txt
	@echo "$(GREEN)Done! Activate with: source .venv/bin/activate$(RESET)"

dev: install ## Install with dev dependencies
	.venv/bin/pip install -q pytest pytest-cov pytest-asyncio httpx ruff

# ============================================
# Testing
# ============================================

test: ## Run pytest tests
	.venv/bin/pytest tests/ -v

test-v: ## Run pytest tests with verbose output
	.venv/bin/pytest tests/ -v --tb=short

test-cov: ## Run tests with coverage report
	.venv/bin/pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html
	@echo "$(GREEN)Coverage report: htmlcov/index.html$(RESET)"

test-fast: ## Run tests without verbose (faster)
	.venv/bin/pytest tests/ -q

# ============================================
# Workflow E2E Tests
# ============================================

workflow: ## Run workflow E2E tests
	.venv/bin/pytest tests/test_workflows_e2e.py -v

workflow-log: ## Run workflow E2E tests with protocol logging
	@echo "$(CYAN)Running workflows with protocol logging...$(RESET)"
	WORKFLOW_LOG=1 .venv/bin/pytest tests/test_workflows_e2e.py -v -s

workflow-one: ## Run single workflow (usage: make workflow-one W=2)
	@if [ -z "$(W)" ]; then \
		echo "$(YELLOW)Usage: make workflow-one W=<number>$(RESET)"; \
		echo "  W=1  Project Lifecycle"; \
		echo "  W=2  Data Pipeline"; \
		echo "  W=3  Snapshot Recovery"; \
		echo "  W=4  Branch Development"; \
		echo "  W=5  Bucket Sharing"; \
		echo "  W=6  Workspace SQL"; \
		echo "  W=7  S3 Compatible"; \
		echo "  W=8  Files Management"; \
		echo "  W=9  Driver Bridge"; \
		echo "  W=10 PG Wire Sessions"; \
	else \
		WORKFLOW_LOG=1 .venv/bin/pytest tests/test_workflows_e2e.py -v -s -k "Workflow$(W)"; \
	fi

e2e: ## Run e2e tests (server must be running)
	@echo "$(YELLOW)Running E2E tests against http://localhost:$(PORT)$(RESET)"
	@echo "$(YELLOW)Make sure server is running: make server$(RESET)"
	@echo ""
	.venv/bin/python scripts/e2e_test.py --admin-key $(ADMIN_API_KEY) --base-url http://localhost:$(PORT)

e2e-v: ## Run e2e tests with verbose output
	.venv/bin/python scripts/e2e_test.py --admin-key $(ADMIN_API_KEY) --base-url http://localhost:$(PORT) -v

# ============================================
# Server
# ============================================

server: ## Start development server
	@echo "$(GREEN)Starting server on http://localhost:$(PORT)$(RESET)"
	@echo "$(CYAN)Admin API key: $(ADMIN_API_KEY)$(RESET)"
	@echo "$(CYAN)Docs: http://localhost:$(PORT)/docs$(RESET)"
	@echo ""
	ADMIN_API_KEY=$(ADMIN_API_KEY) .venv/bin/python -m src.main

server-prod: ## Start server in production mode (no debug, JSON logs)
	DEBUG=false ADMIN_API_KEY=$(ADMIN_API_KEY) .venv/bin/python -m src.main

# ============================================
# Code Quality
# ============================================

lint: ## Run linter (ruff)
	.venv/bin/ruff check src/ tests/

lint-fix: ## Run linter and fix issues
	.venv/bin/ruff check src/ tests/ --fix

format: ## Format code (ruff)
	.venv/bin/ruff format src/ tests/

check: lint ## Run all checks (lint)
	@echo "$(GREEN)All checks passed!$(RESET)"

# ============================================
# Docker
# ============================================

docker-build: ## Build Docker image
	docker build -t duckdb-api-service .

docker-up: ## Start with docker-compose
	docker-compose up -d
	@echo "$(GREEN)Service running at http://localhost:$(PORT)$(RESET)"

docker-down: ## Stop docker-compose
	docker-compose down

docker-logs: ## Show docker logs
	docker-compose logs -f

# ============================================
# Cleanup
# ============================================

clean: ## Remove build artifacts and cache
	rm -rf __pycache__ .pytest_cache .coverage htmlcov .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(GREEN)Cleaned!$(RESET)"

clean-data: ## Remove test data (careful!)
	@echo "$(YELLOW)This will remove all data in data/ directory!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] && rm -rf data/* || echo "Cancelled"

reset: clean clean-data ## Full reset (clean + remove data)
	@echo "$(GREEN)Reset complete!$(RESET)"

# ============================================
# Database
# ============================================

db-shell: ## Open DuckDB shell for metadata database
	@echo "$(CYAN)Opening metadata.duckdb...$(RESET)"
	@echo "$(YELLOW)Tip: .tables, .schema, SELECT * FROM projects;$(RESET)"
	duckdb data/metadata.duckdb

db-stats: ## Show database statistics
	@echo "$(CYAN)Database Statistics$(RESET)"
	@echo "==================="
	@.venv/bin/python -c "import duckdb; \
		conn = duckdb.connect('data/metadata.duckdb', read_only=True); \
		print('Projects:', conn.execute('SELECT COUNT(*) FROM projects').fetchone()[0]); \
		print('  - Active:', conn.execute(\"SELECT COUNT(*) FROM projects WHERE status='active'\").fetchone()[0]); \
		print('Files:', conn.execute('SELECT COUNT(*) FROM files').fetchone()[0]); \
		print('Snapshots:', conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0]); \
		print('API Keys:', conn.execute('SELECT COUNT(*) FROM api_keys').fetchone()[0]); \
		print('Bucket Shares:', conn.execute('SELECT COUNT(*) FROM bucket_shares').fetchone()[0]); \
		print('Bucket Links:', conn.execute('SELECT COUNT(*) FROM bucket_links').fetchone()[0]); \
		print('Operations Log:', conn.execute('SELECT COUNT(*) FROM operations_log').fetchone()[0]); \
		conn.close()"

# ============================================
# Utilities
# ============================================

routes: ## Show all API routes
	@.venv/bin/python -c "from src.main import app; \
		routes = sorted([(r.path, list(r.methods - {'HEAD', 'OPTIONS'})) for r in app.routes if hasattr(r, 'methods')]); \
		[print(f'{m[0]:7} {p}') for p, m in routes if m]"

req-update: ## Update requirements.txt from installed packages
	.venv/bin/pip freeze > requirements.txt
	@echo "$(GREEN)requirements.txt updated$(RESET)"
