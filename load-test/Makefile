# Convenience wrapper. Use plain docker compose if you prefer.
# Always run from the load-test/ directory.

PROJECT_KAF   := kaf
PROJECT_KAFKA := kafka

.PHONY: help build up down logs status loadgen kafka-up kafka-down clean nuke

help:
	@echo "kaf-go load-test targets"
	@echo ""
	@echo "  make build         - Build the kaf-go and loadgen Docker images"
	@echo "  make up            - Start the kaf-go cluster + monitoring (detached)"
	@echo "  make down          - Stop the kaf-go cluster (volumes preserved)"
	@echo "  make nuke          - Stop the kaf-go cluster and DELETE volumes"
	@echo "  make logs          - Tail kaf-go broker logs"
	@echo "  make status        - Show running containers"
	@echo "  make loadgen       - Run a 60s baseline load test against kaf-go"
	@echo ""
	@echo "  make kafka-up      - Start the Apache Kafka comparison cluster"
	@echo "  make kafka-down    - Stop the Apache Kafka cluster"
	@echo "  make kafka-loadgen - Run the same baseline against Kafka"
	@echo ""
	@echo "  Grafana:    http://localhost:3000  (anonymous viewer)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  cAdvisor:   http://localhost:8080"

build:
	docker compose -p $(PROJECT_KAF) -f docker-compose.kaf.yml build

up:
	docker compose -p $(PROJECT_KAF) -f docker-compose.kaf.yml up -d --build kaf1 kaf2 kaf3 prometheus grafana cadvisor node_exporter

down:
	docker compose -p $(PROJECT_KAF) -f docker-compose.kaf.yml down

nuke:
	docker compose -p $(PROJECT_KAF) -f docker-compose.kaf.yml down -v

logs:
	docker compose -p $(PROJECT_KAF) -f docker-compose.kaf.yml logs -f kaf1 kaf2 kaf3

status:
	docker compose -p $(PROJECT_KAF) -f docker-compose.kaf.yml ps

loadgen:
	docker compose -p $(PROJECT_KAF) -f docker-compose.kaf.yml --profile bench run --rm --use-aliases --service-ports loadgen

kafka-up:
	docker compose -p $(PROJECT_KAFKA) -f docker-compose.kafka.yml up -d

kafka-down:
	docker compose -p $(PROJECT_KAFKA) -f docker-compose.kafka.yml down

kafka-nuke:
	docker compose -p $(PROJECT_KAFKA) -f docker-compose.kafka.yml down -v

kafka-loadgen:
	docker compose -p $(PROJECT_KAFKA) -f docker-compose.kafka.yml --profile bench run --rm --use-aliases --service-ports loadgen

clean:
	rm -rf loadgen/loadgen
