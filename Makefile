# JobCrawler — project automation
#
# Usage examples:
#   make build                    # compile all 4 binaries
#   make test                     # run tests with race detector
#   make dev                      # start local Docker stack
#   make dev-down                 # tear down and remove volumes
#   make docker-build             # build all Docker images
#   make k8s-deploy               # apply all Kubernetes manifests
#   make scale svc=crawler n=3    # scale a K8s deployment
#   make logs svc=api             # tail K8s pod logs

# ── Variables ──────────────────────────────────────────────────────────────────
BINARY_DIR   := ./bin
SERVICES     := api crawler processor scheduler
GO_FLAGS     := -ldflags="-s -w" -trimpath
BUILD_FLAGS  := CGO_ENABLED=0 GOOS=linux

# Docker image settings — override with: make push DOCKER_USER=myname
DOCKER_USER  := yourorg
IMAGE_TAG    ?= latest

# Kubernetes namespace
K8S_NS       := jobcrawler

# Database DSN for local migrations
LOCAL_DSN    ?= postgres://jobcrawler:secret@localhost:5432/jobcrawler?sslmode=disable

.PHONY: all build test lint clean docker-build push dev dev-stop dev-down migrate \
        k8s-deploy k8s-status k8s-delete scale logs help

# Default target
all: build

# ── Build ──────────────────────────────────────────────────────────────────────

## build: Compile all 4 binaries into ./bin/ (parallel, 1 thread per CPU core)
build:
	@echo "→ Building binaries..."
	@mkdir -p $(BINARY_DIR)
	@$(MAKE) -j$(shell nproc 2>/dev/null || sysctl -n hw.logicalcpu) \
		$(addprefix _build-,$(SERVICES))
	@echo "✓ All binaries built in $(BINARY_DIR)/"

_build-%:
	@echo "  compiling cmd/$*..."
	@$(BUILD_FLAGS) go build $(GO_FLAGS) -o $(BINARY_DIR)/$* ./cmd/$*

## build-local: Compile for the current OS (macOS-friendly, no CGO_ENABLED=0)
build-local:
	@mkdir -p $(BINARY_DIR)
	@for svc in $(SERVICES); do \
		echo "  compiling cmd/$$svc..."; \
		go build -o $(BINARY_DIR)/$$svc ./cmd/$$svc; \
	done
	@echo "✓ Local binaries built"

# ── Test ───────────────────────────────────────────────────────────────────────

## test: Run all tests with race detector and 60s timeout
test:
	@echo "→ Running tests..."
	go test ./... -race -timeout 60s -count=1
	@echo "✓ Tests passed"

## test-cover: Run tests and generate HTML coverage report
test-cover:
	go test ./... -race -timeout 60s -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html
	@echo "✓ Coverage report: coverage.html"

# ── Lint ───────────────────────────────────────────────────────────────────────

## lint: Run golangci-lint (install: brew install golangci-lint)
lint:
	@command -v golangci-lint >/dev/null 2>&1 || \
		{ echo "golangci-lint not found — install with: brew install golangci-lint"; exit 1; }
	golangci-lint run ./...
	@echo "✓ Lint passed"

## fmt: Format all Go source files
fmt:
	gofmt -w -s .
	@echo "✓ Formatted"

# ── Clean ─────────────────────────────────────────────────────────────────────

## clean: Remove compiled binaries and test artifacts
clean:
	rm -rf $(BINARY_DIR) coverage.txt coverage.html
	@echo "✓ Cleaned"

# ── Docker ────────────────────────────────────────────────────────────────────

## docker-build: Build all Docker images in parallel
docker-build:
	@echo "→ Building Docker images..."
	docker compose build --parallel
	@echo "✓ Images built"

## push: Tag and push all images to Docker Hub
push: docker-build
	@echo "→ Pushing images to $(DOCKER_USER)/..."
	@for svc in $(SERVICES); do \
		docker tag jobcrawler-jobcrawler-$$svc:latest $(DOCKER_USER)/jobcrawler-$$svc:$(IMAGE_TAG); \
		docker push $(DOCKER_USER)/jobcrawler-$$svc:$(IMAGE_TAG); \
		echo "  ✓ Pushed $(DOCKER_USER)/jobcrawler-$$svc:$(IMAGE_TAG)"; \
	done

# ── Local development ─────────────────────────────────────────────────────────

## dev: Start the full local stack with Docker Compose (builds app images from source)
dev:
	@echo "→ Starting local dev stack..."
	docker compose --profile dev up -d --build
	@echo ""
	@echo "✓ Stack started. Service URLs:"
	@echo "   API:           http://localhost:8080"
	@echo "   Metrics:       http://localhost:8080/metrics"
	@echo "   Elasticsearch: http://localhost:9200"
	@echo "   Kafka:         localhost:9092"
	@echo "   PostgreSQL:    localhost:5432"
	@echo "   Redis:         localhost:6379"

## dev-stop: Stop containers without removing them (resume with `make dev`)
dev-stop:
	@echo "→ Stopping local stack..."
	docker compose --profile dev stop
	@echo "✓ Stack stopped (containers preserved — run 'make dev' to resume)"

## dev-down: Stop and remove all containers and volumes
dev-down:
	@echo "→ Tearing down local stack..."
	docker compose --profile dev down -v
	@echo "✓ Stack removed (all volumes deleted)"

## dev-logs: Tail logs from all Docker Compose services
dev-logs:
	docker compose --profile dev logs -f --tail=50

## dev-ps: Show running Docker Compose services
dev-ps:
	docker compose --profile dev ps

# ── Database ──────────────────────────────────────────────────────────────────

## migrate: Run all pending database migrations against the local DB
migrate:
	@echo "→ Running migrations against $(LOCAL_DSN)..."
	@go run ./cmd/processor/main.go --migrate-only 2>/dev/null || \
		migrate -path ./migrations -database "$(LOCAL_DSN)" up
	@echo "✓ Migrations applied"

## migrate-down: Roll back the last migration
migrate-down:
	migrate -path ./migrations -database "$(LOCAL_DSN)" down 1
	@echo "✓ Last migration rolled back"

## migrate-status: Show current migration version
migrate-status:
	migrate -path ./migrations -database "$(LOCAL_DSN)" version

# ── Kubernetes ────────────────────────────────────────────────────────────────

## k8s-deploy: Apply all Kubernetes manifests in order
k8s-deploy:
	@echo "→ Deploying to Kubernetes (namespace: $(K8S_NS))..."
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/secret.yaml
	kubectl apply -f k8s/service.yaml
	kubectl apply -f k8s/api-deployment.yaml
	kubectl apply -f k8s/crawler-deployment.yaml
	kubectl apply -f k8s/processor-deployment.yaml
	kubectl apply -f k8s/api-hpa.yaml
	kubectl apply -f k8s/crawler-scaledobject.yaml
	@echo "✓ Manifests applied"

## k8s-status: Watch pod status in the jobcrawler namespace
k8s-status:
	kubectl get pods -n $(K8S_NS) -w

## k8s-delete: Delete all JobCrawler resources from the cluster
k8s-delete:
	@echo "→ Deleting JobCrawler resources..."
	kubectl delete -f k8s/ --ignore-not-found
	@echo "✓ Resources deleted"

## scale svc=<name> n=<count>: Scale a deployment (e.g. make scale svc=crawler n=3)
scale:
	@[ "$(svc)" ] && [ "$(n)" ] || \
		{ echo "Usage: make scale svc=<service> n=<replicas>"; exit 1; }
	kubectl scale deployment jobcrawler-$(svc) \
		--replicas=$(n) \
		-n $(K8S_NS)
	@echo "✓ jobcrawler-$(svc) scaled to $(n) replicas"

## logs svc=<name>: Tail logs from a Kubernetes deployment
logs:
	@[ "$(svc)" ] || { echo "Usage: make logs svc=<service>"; exit 1; }
	kubectl logs \
		-l app.kubernetes.io/component=$(svc) \
		-n $(K8S_NS) \
		--follow \
		--tail=100 \
		--all-containers

## rollout svc=<name> image=<tag>: Rolling update with new image tag
rollout:
	@[ "$(svc)" ] && [ "$(image)" ] || \
		{ echo "Usage: make rollout svc=<service> image=<tag>"; exit 1; }
	kubectl set image deployment/jobcrawler-$(svc) \
		$(svc)=$(DOCKER_USER)/jobcrawler-$(svc):$(image) \
		-n $(K8S_NS)
	kubectl rollout status deployment/jobcrawler-$(svc) -n $(K8S_NS)

## rollback svc=<name>: Roll back a deployment to the previous version
rollback:
	@[ "$(svc)" ] || { echo "Usage: make rollback svc=<service>"; exit 1; }
	kubectl rollout undo deployment/jobcrawler-$(svc) -n $(K8S_NS)
	kubectl rollout status deployment/jobcrawler-$(svc) -n $(K8S_NS)

# ── Help ───────────────────────────────────────────────────────────────────────

## help: Print this help message
help:
	@echo "JobCrawler — available make targets:"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /' | column -t -s ':'