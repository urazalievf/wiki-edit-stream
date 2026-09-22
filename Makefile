# Wikipedia edit stream: Kafka + Flink SQL
SHELL := /bin/bash
PY := .venv/bin/python
DURATION ?= 150

# Kafka 4 and Flink 2 both need JDK 17 or 21; many machines default to newer.
SUPPORTED_JAVA := $(shell ./scripts/find-jdk.sh 2>/dev/null)
ifneq ($(strip $(SUPPORTED_JAVA)),)
JAVA_HOME := $(SUPPORTED_JAVA)
export JAVA_HOME
endif
export PYTHONPATH := $(CURDIR)

.DEFAULT_GOAL := help

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## create the virtualenv and fetch connector jars
	./scripts/install.sh
	./scripts/fetch-connectors.sh

doctor: ## check the toolchain can run Kafka and Flink
	@echo "python    : $$($(PY) -V 2>&1)"
	@echo "JAVA_HOME : $${JAVA_HOME:-(unset)}"
	@echo "java      : $$($${JAVA_HOME:+$$JAVA_HOME/bin/}java -version 2>&1 | head -1)"
	@echo "kafka     : $$(ls -d $${KAFKA_HOME:-/opt/homebrew/opt/kafka} 2>/dev/null || echo 'NOT FOUND - brew install kafka')"
	@echo "jars      : $$(ls lib/*.jar 2>/dev/null | wc -l | tr -d ' ') in lib/"

## --- kafka ---------------------------------------------------------------

up: ## start the local broker and create topics
	./scripts/kafka.sh start
	./scripts/create-topics.sh

down: ## stop the broker
	./scripts/kafka.sh stop

reset: ## stop the broker and wipe all topic data
	./scripts/kafka.sh reset

topics: ## list topics
	./scripts/kafka.sh topics

offsets: ## show how many records are in each topic
	./scripts/show-offsets.sh

## --- pipeline ------------------------------------------------------------

demo: ## full run: live ingest + all three jobs, concurrently
	./scripts/demo.sh $(DURATION)

ingest: ## stream live edits into Kafka until interrupted
	$(PY) -m ingest.main

jobs: ## list the Flink SQL jobs
	$(PY) -m flink_jobs.submit --list

clean-job: ## run the raw -> clean + DLQ job
	$(PY) -m flink_jobs.submit 01_clean.sql

stats-job: ## run the windowed per-wiki stats job
	$(PY) -m flink_jobs.submit 02_stats.sql

wars-job: ## run the edit-war detection job
	$(PY) -m flink_jobs.submit 03_edit_wars.sql

replay: ## re-run every job over existing data as a bounded source, then exit
	WES_BOUNDED=1 $(PY) -m flink_jobs.submit 01_clean.sql
	WES_BOUNDED=1 $(PY) -m flink_jobs.submit 02_stats.sql
	WES_BOUNDED=1 $(PY) -m flink_jobs.submit 03_edit_wars.sql

tail-stats: ## print recent windowed stats
	./scripts/kafka.sh tail wiki.stats.per_wiki_1m 5

tail-wars: ## print recent edit-war alerts
	./scripts/kafka.sh tail wiki.alerts.edit_wars 5

## --- quality -------------------------------------------------------------

test: ## run the unit tests (no cluster needed)
	$(PY) -m pytest -q

lint: ## ruff
	.venv/bin/ruff check ingest flink_jobs tests

format: ## apply ruff formatting
	.venv/bin/ruff format ingest flink_jobs tests
	.venv/bin/ruff check --fix ingest flink_jobs tests

.PHONY: help install doctor up down reset topics offsets demo ingest jobs \
	clean-job stats-job wars-job replay tail-stats tail-wars test lint format
