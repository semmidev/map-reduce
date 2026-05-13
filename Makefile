.PHONY: up down logs status build clean analyze scale help

# ──────────────────────────────────────────────
# MapReduce Cluster — Management Commands
# ──────────────────────────────────────────────

COMPOSE = docker compose
CLUSTER_WORKERS = worker-jakarta-1 worker-jakarta-2 worker-singapore-1 worker-singapore-2 worker-us-east-1 worker-us-west-1

help: ## Show this help
	@echo ""
	@echo "  🔐 MapReduce Cluster — Cyber Log Analysis"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""

build: ## Build all Docker images
	$(COMPOSE) build

up: ## Start the full cluster (log-gen → master → workers)
	$(COMPOSE) up -d log-generator
	@echo "⏳ Waiting for log generation..."
	$(COMPOSE) wait log-generator
	$(COMPOSE) up -d master
	@echo "⏳ Waiting for master to be healthy..."
	@sleep 3
	$(COMPOSE) up -d $(CLUSTER_WORKERS)
	@echo ""
	@echo "✅ Cluster is running!"
	@echo "   → Master status : http://localhost:8080/status"
	@echo "   → View logs     : make logs"
	@echo ""

down: ## Stop and remove all containers
	$(COMPOSE) down -v

logs: ## Stream all container logs
	$(COMPOSE) logs -f --tail=50

logs-master: ## Stream master logs only
	$(COMPOSE) logs -f master

logs-workers: ## Stream all worker logs
	$(COMPOSE) logs -f $(CLUSTER_WORKERS)

status: ## Query master job status (via HTTP)
	@echo ""
	@curl -s http://localhost:8080/status | python3 -c "\
import sys, json; \
d = json.load(sys.stdin); \
print(f'  Phase     : {d[\"phase\"]}'); \
print(f'  Map       : {d[\"done_map_tasks\"]}/{d[\"total_map_tasks\"]} done'); \
print(f'  Reduce    : {d[\"done_reduce_tasks\"]}/{d[\"total_reduce_tasks\"]} done'); \
print(f'  Workers   : {len(d[\"workers\"])}'); \
[print(f'    - {w[\"id\"]} [{w[\"status\"]}] tasks_done={w[\"tasks_handled\"]}') for w in d['workers']]; \
print()"

analyze: ## Run result analyzer (shows threat intel report)
	$(COMPOSE) run --rm analyzer

clean: ## Remove build artifacts and volumes
	$(COMPOSE) down -v --rmi local
	docker volume prune -f

scale-workers: ## Add more workers (usage: make scale-workers N=8)
	N=$${N:-8}; \
	$(COMPOSE) up -d --scale worker-jakarta-1=$$N

ps: ## Show container status
	$(COMPOSE) ps
