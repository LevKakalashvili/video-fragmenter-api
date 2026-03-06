DOCKER_COMPOSE_LOCAL=./docker-compose.local.yaml
ENV_LOCAL=.env

up-local:
	docker compose --env-file $(ENV_LOCAL) -f $(DOCKER_COMPOSE_LOCAL) up -d --build

format:
	ruff format --config=./pyproject.toml
	ruff check --fix --preview --unsafe-fixes --config=./pyproject.toml

CHANGED_PY_FILES = $(shell git diff --name-only --diff-filter=ACM HEAD | findstr /R "\.py")

format_changed_win:
	@for %%f in ($(CHANGED_PY_FILES)) do (\
	@echo %%f && \
	ruff check --fix --preview --unsafe-fixes %%f && \
	ruff format --config=./pyproject.toml %%f \
	)
