DOCKER_COMPOSE_LOCAL=./docker-compose.local.yaml
ENV_LOCAL=.env

up-local:
	docker compose --env-file $(ENV_LOCAL) -f $(DOCKER_COMPOSE_LOCAL) up -d --build

format:
	ruff check --fix-only --preview --unsafe-fixes --config=./pyproject.toml .
	ruff format --config=./pyproject.toml .

lint:
	ruff check --preview --unsafe-fixes --config=./pyproject.toml .
	ruff format --check --config=./pyproject.toml .

CHANGED_PY_FILES = $(shell git diff --name-only --diff-filter=ACM HEAD | findstr /R "\.py")

format-changed-win:
	@for %%f in ($(CHANGED_PY_FILES)) do (\
	@echo %%f && \
	ruff check --fix-only --preview --unsafe-fixes --config=./pyproject.toml %%f && \
	ruff format --config=./pyproject.toml %%f \
	)

ffmpeg-dev-win:
	powershell -NoProfile -NoExit -ExecutionPolicy Bypass -Command ". .\scripts\dev.ps1"
