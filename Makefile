# Ghana Export Commodity & Cedi Monitoring Platform
COMPOSE ?= docker compose
# Git Bash on Windows rewrites /opt/... into a Windows path before it reaches the
# container. Harmless everywhere else.
export MSYS_NO_PATHCONV = 1

.DEFAULT_GOAL := help
.PHONY: help up down build logs ps topics batch dashboards test dry-run psql clean tools cluster restart-stream

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

build: ## Build the poller and Spark images
	$(COMPOSE) build

up: ## Start the whole live pipeline
	$(COMPOSE) up -d --build
	@echo
	@echo "Grafana   http://localhost:3000  (admin/admin, or browse anonymously)"
	@echo "Spark UI  http://localhost:4040"

down: ## Stop everything (keeps volumes)
	$(COMPOSE) down

clean: ## Stop everything and delete all data volumes
	$(COMPOSE) down -v

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail logs from every service
	$(COMPOSE) logs -f --tail=80

stream-logs: ## Tail only the Spark streaming job
	$(COMPOSE) logs -f --tail=120 spark-streaming

restart-stream: ## Restart just the Spark streaming job
	$(COMPOSE) restart spark-streaming

topics: ## List Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list

consume: ## Tail a topic, e.g. make consume TOPIC=alerts.flagged
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:19092 --topic $(or $(TOPIC),commodity.prices.raw) --from-beginning --max-messages 10

batch: ## Run the nightly analytical job now
	$(COMPOSE) run --rm spark-batch

psql: ## Open a psql shell on the serving store
	$(COMPOSE) exec postgres psql -U ghana -d ghana_dep

tools: ## Start Kafka UI + the alerts notifier
	$(COMPOSE) --profile tools up -d

cluster: ## Start a real Spark standalone master + worker
	$(COMPOSE) --profile cluster up -d

dashboards: ## Regenerate the Grafana dashboard JSON
	python scripts/build_dashboards.py

test: ## Run the unit tests (no containers needed)
	python -m pytest tests -q

dry-run: ## Poll every live source once and print to stdout (no Kafka)
	python -m ingestion.run_poller --source commodities --once --dry-run
	python -m ingestion.run_poller --source fx --once --dry-run
	python -m ingestion.run_poller --source weather --once --dry-run
