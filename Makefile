# Common tasks. BROKER selects the broker for Compose: rabbitmq (default), sqs or pubsub.
BROKER    ?= rabbitmq
API_PORT  ?= 8080
COMPOSE    = API_PORT=$(API_PORT) docker compose -f compose.yaml -f compose.$(BROKER).yaml
K8S       ?= deploy/k8s/base   # or deploy/k8s/overlays/sqs
TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/hookgate_test?sslmode=disable

.PHONY: test up down logs demo k8s-up k8s-demo k8s-down kind-up kind-down

test: ## unit + integration tests (needs Postgres at TEST_DATABASE_URL)
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) go test -p 1 -race -count=1 ./...

up: ## Postgres + broker + api + 2 workers
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f api worker

demo: ## send signed webhooks (incl. duplicates and a failing one) and show the result
	scripts/demo.sh http://localhost:$(API_PORT)

# ---- Kubernetes on minikube
k8s-up:
	minikube status >/dev/null 2>&1 || minikube start --cpus=4 --memory=4g
	minikube image build -t hookgate:dev .
	kubectl apply -k $(K8S)
	kubectl -n hookgate rollout status statefulset/postgres --timeout=180s
	kubectl -n hookgate rollout status deploy --timeout=300s

k8s-demo: ## port-forward the API and run the demo against the cluster
	kubectl -n hookgate port-forward svc/hookgate-api $(API_PORT):8080 >/dev/null & \
	  pf=$$!; sleep 3; scripts/demo.sh http://localhost:$(API_PORT); kill $$pf

k8s-down:
	kubectl delete -k $(K8S) --ignore-not-found

# ---- Kubernetes on kind (what CI uses)
kind-up:
	kind get clusters | grep -qx hookgate || kind create cluster --name hookgate
	docker build -t hookgate:dev .
	kind load docker-image hookgate:dev --name hookgate
	kubectl apply -k $(K8S)
	kubectl -n hookgate rollout status statefulset/postgres --timeout=180s
	kubectl -n hookgate rollout status deploy --timeout=300s

kind-down:
	kind delete cluster --name hookgate
