.PHONY: help redis-up redis-down logs test

REDIS_PORT ?= 16379
REDIS_URL ?= redis://127.0.0.1:$(REDIS_PORT)/

COLOR_GREEN := \033[32m
COLOR_BLUE := \033[34m
COLOR_RESET := \033[0m

help: ## Show available commands
	@printf "$(COLOR_BLUE)Available commands:$(COLOR_RESET)\n"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "%s\t%s\n", $$1, $$2}' $(MAKEFILE_LIST) \
					| sort \
					| awk -F"\t" -v green="$(COLOR_GREEN)" -v blue="$(COLOR_BLUE)" -v reset="$(COLOR_RESET)" 'function line(prefix){printf "%s-------------------------------- %s --------------------------------%s\n", blue, prefix, reset} function order(p){return (p=="misc"?0:(p=="dev"?1:(p=="local"?2:(p=="test"?3:(p=="prod"?4:99)))))} {cmd=$$1; desc=$$2; prefix=cmd; if(index(cmd,"-")>0){sub(/-.*/,"",prefix)} else {prefix="misc"}; groups[prefix]=groups[prefix] sprintf("%s%-24s%s %s\n", green, cmd, reset, desc)} END{n=0; for(p in groups){keys[n++]=p}; for(i=0;i<n;i++){for(j=i+1;j<n;j++){if(order(keys[j])<order(keys[i]) || (order(keys[j])==order(keys[i]) && keys[j]<keys[i])){t=keys[i];keys[i]=keys[j];keys[j]=t}}}; for(i=0;i<n;i++){p=keys[i]; line(p); printf "%s", groups[p]}}'
	@echo
	@echo "Tip: use 'make <target>' (e.g. 'make dev-up')"

redis-up: ## Start redis
	@docker info >/dev/null 2>&1 || (echo "docker daemon not running" >&2; exit 1)
	@REDIS_PORT="$(REDIS_PORT)" docker compose up -d redis-test
	@sh -c 'for i in $$(seq 1 60); do \
		if docker compose exec -T redis-test redis-cli ping >/dev/null 2>&1; then exit 0; fi; \
		sleep 0.25; \
	done; \
	echo "redis did not become ready in time" >&2; exit 1'


redis-down: ## Stop local redis and remove volumes (both profiles)
	@docker compose down -v --remove-orphans

logs: ## Show logs
	@docker compose logs -f


test: ## Run tests
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo test

test-example: ## Run example tests
	REDIS_URL="$(REDIS_URL)" cargo test value_is_eventually_flushed_to_redis -- --show-output
