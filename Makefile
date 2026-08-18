.PHONY: install lint format test test-unit test-integration redis-up redis-down build run up down clean

PYTHON := python3.13
IMAGE := care-metrics-exporter
COMPOSE := docker compose

install:
	pipenv install --dev

lint:
	ruff format --check .
	ruff check .

format:
	ruff format .
	ruff check --fix .

# Integration tests skip automatically when Redis is not running.
test:
	pytest

test-unit:
	pytest -m "not integration"

test-integration: redis-up
	pytest -m integration

redis-up:
	$(COMPOSE) up -d redis

redis-down:
	$(COMPOSE) down redis

build:
	docker build -f docker/prod.Dockerfile -t $(IMAGE):dev .

# Run the built image against a broker reachable from the container.
run:
	docker run --rm -p 8000:8000 \
	  -e CELERY_BROKER_URL=$${CELERY_BROKER_URL:-redis://host.docker.internal:6379/0} \
	  $(IMAGE):dev

up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down -v

clean:
	rm -rf .pytest_cache .ruff_cache **/__pycache__
