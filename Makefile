SHELL := /bin/sh
IMAGE ?= gpu-telemetry:dev
RELEASE ?= telemetry
NAMESPACE ?= telemetry
CHART := deploy/helm/telemetry

.PHONY: help build test race coverage integration check openapi openapi-check image up down demo helm-check k8s-install k8s-status
help:
	@printf '%s\n' 'build test race coverage integration check openapi image up down demo helm-check k8s-install k8s-status'
build:
	go build -trimpath -o bin/pipeline ./cmd/pipeline
test:
	go test ./...
race:
	go test -race ./...
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
integration:
	@test -n "$$TEST_DATABASE_URL" || (echo 'Set TEST_DATABASE_URL to a dedicated disposable database (tests truncate its pipeline tables).'; exit 1)
	go test -race -tags=integration -count=1 -coverprofile=coverage-integration.out ./internal/store
	go tool cover -func=coverage-integration.out
check:
	@test -z "$$(gofmt -l cmd internal)" || (echo 'Run gofmt on the listed Go source files'; gofmt -l cmd internal; exit 1)
	go vet ./...
	$(MAKE) race openapi-check helm-check
openapi:
	go run ./cmd/pipeline openapi > docs/openapi.json
openapi-check:
	@tmp=$$(mktemp); trap 'rm -f "$$tmp"' EXIT; go run ./cmd/pipeline openapi > "$$tmp" && diff -u docs/openapi.json "$$tmp"
image:
	docker build -t $(IMAGE) .
up:
	docker compose up --build -d
down:
	docker compose down
demo: image
	python3 scripts/demo.py
helm-check:
	helm lint $(CHART)
	helm template $(RELEASE) $(CHART) > /dev/null
k8s-install:
	helm upgrade --install $(RELEASE) $(CHART) --namespace $(NAMESPACE) --create-namespace --wait --wait-for-jobs --timeout 5m
k8s-status:
	kubectl get pods,deployments,services,pvc -n $(NAMESPACE)
