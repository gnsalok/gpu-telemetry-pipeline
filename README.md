# GPU Telemetry Pipeline

A Go telemetry pipeline with a custom PostgreSQL-backed message queue. Streamers replay GPU measurements, collectors persist them idempotently, and a REST API exposes the resulting history. Every service runs from the same small image with a separate command and scales independently.

```mermaid
flowchart LR
    S[CSV streamers] --> B[Custom Go brokers]
    B <--> Q[(PostgreSQL queue schema)]
    B --> C[Collectors]
    C --> T[(PostgreSQL telemetry schema)]
    A[REST API] --> T
```

**Delivery contract:** durable acceptance, at-least-once delivery attempts, and one persisted observation per event ID. An unrecoverable record is retained in the dead-letter state. There is no claim of exactly-once transport or database high availability.

## Start locally

Prerequisites: Docker Desktop with its engine running. Go 1.25.6+ is needed for development; Python 3 for the acceptance scripts. Kubernetes and Helm 3 are needed only for cluster deployment.

```sh
make up
curl http://localhost:8080/api/v1/gpus
open http://localhost:8080/docs
```

This builds the image, starts PostgreSQL, runs migrations, and starts all four roles. Wait for `/readyz` to return 200 before querying. The source generates up to 100 observations/second and loops continuously. API and broker ports are bound to loopback.

```sh
docker compose stop streamer   # Stop new observations; collectors drain
curl http://localhost:8081/queue/v1/stats
make down                      # Retains the database volume
```

Default database credentials are for local development only. `docker compose down -v` explicitly deletes the local Compose database.

## A finite, self-checking demo

```sh
make demo
```

Creates a uniquely named temporary Compose stack on free local ports, streams one complete CSV pass, verifies all API pages and inclusive time boundaries, and removes only that test stack/volume. Expected: **247 GPUs, 2,470 distinct observations, zero ready/leased/dead messages**. It does not reuse or erase the development database.

## Query the API

```sh
curl 'http://localhost:8080/api/v1/gpus?limit=100'
curl 'http://localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?limit=100'
curl 'http://localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2026-09-17T00:00:00Z&end_time=2026-09-17T23:59:59Z'
```

- GPU identity is the CSV `uuid`, not the host-local `gpu_id`.
- Responses contain `items` and, when another page exists, `next_cursor`.
- For GPU pages, pass the cursor as `after`. For telemetry, pass it as `cursor` with the same time filters.
- Both boundaries are inclusive RFC3339 timestamps. URL-encode offsets containing `+`.
- Telemetry is ordered by `(timestamp, event_id)`. Time is assigned during the first committed collector insert, not copied from the CSV.
- Page size defaults to 100, maximum 1,000. Pagination is a live view, not a repeatable snapshot during concurrent ingestion.
- Unknown GPUs return 404; a known GPU with no matching observations returns an empty array. Malformed filters return 400; schema validation may return 422; unavailable storage returns 503.
- `/docs` serves interactive documentation; `/openapi.json` serves the generated contract. `make openapi` generates [docs/openapi.json](docs/openapi.json) offline from the same route types.

## Docker Desktop Kubernetes

Enable Kubernetes in Docker Desktop. Confirm the context before installation:

```sh
kubectl config current-context
kubectl get nodes
make image
make k8s-install
make k8s-status
kubectl port-forward -n telemetry svc/telemetry-telemetry-api 8080:8080
```

Stop/change the Compose API port if it already occupies 8080. Defaults: 1 streamer, 2 brokers, 2 collectors, 1 API, and 1 PostgreSQL StatefulSet with persistent storage. The initial migration Job may retry while PostgreSQL starts; readiness checks require schema installation.

```sh
kubectl scale deployment/telemetry-telemetry-collector -n telemetry --replicas=10
kubectl scale deployment/telemetry-telemetry-streamer -n telemetry --replicas=10
kubectl scale deployment/telemetry-telemetry-streamer -n telemetry --replicas=0
kubectl scale deployment/telemetry-telemetry-collector -n telemetry --replicas=2
python3 scripts/k8s_verify.py
```

The verification script creates its own release/namespace, tests finite 1/5/10 producer/collector workloads, kills a broker and collector, restarts PostgreSQL with queued work, checks counts, and removes its namespace. This verifies replicas on one node, not node failure or multi-node performance. See [verification results](docs/verification.md).

Manual scaling is intentional; HPA is not required to demonstrate elasticity. Each streamer independently replays the CSV, so replicas increase offered load. Collectors compete for messages; delivery is not broadcast.

For another cluster, publish the image to your registry and override `image.repository`, `image.tag`, and optionally `postgresql.storageClass`. Set `spreadAcrossNodes=true` for preferred spreading. No hard anti-affinity prevents single-node scheduling. For an external database, set `postgresql.enabled=false` and `externalDatabaseSecret` to an existing Secret with key `url`.

## Development and tests

```sh
make build
make test           # Unit tests, no Docker/database needed
make race           # Unit tests with Go race detector
make coverage       # Printed unit coverage and coverage.html
make check          # Formatting, vet, race, OpenAPI drift, Helm checks
docker compose up -d postgres
docker compose exec postgres createdb -U telemetry telemetry_test
TEST_DATABASE_URL='postgres://telemetry:telemetry@localhost:15432/telemetry_test?sslmode=disable' make integration
```

**Integration tests truncate pipeline tables in `TEST_DATABASE_URL`. Use a dedicated disposable database.** They never fall back to `DATABASE_URL`. Unit and real-PostgreSQL coverage are reported separately. CI runs both, validates the generated API/Helm, and builds the image.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | Required except streamer/OpenAPI | PostgreSQL connection string |
| `HTTP_ADDR` | `:8080` | Role HTTP server |
| `BROKER_URL` | `http://localhost:8081` | Queue endpoint |
| `BROKER_TOKEN` | Empty | Optional bearer token for queue endpoints |
| `CSV_PATH` | `data/telemetry.csv` | Source data |
| `RATE` | `100` | Maximum observations/second per streamer |
| `LOOPS` | `0` | Continuous replay; positive number gives finite passes |
| `WORKERS` | `2` | Collector workers per pod, maximum 32 |
| `DB_POOL_SIZE` | `4` | Connections per broker/collector/API process |
| `QUEUE_CAPACITY` | `100000` | Ready + leased + dead records; use same value on all brokers |

Use `bin/pipeline broker`, `collector`, `streamer`, `api`, or `migrate` for individual roles. Configuration is validated before serving. There is no Kafka, RabbitMQ, Redis Streams, or task-queue framework dependency.

## Observability and operations

Each service exposes `/healthz`, `/readyz`, and Prometheus text metrics at `/metrics`, with JSON logs. Collectors expose inserts, duplicate processing, failures, and ingestion-latency histograms. Brokers expose queue states and oldest active age. Heap/goroutine gauges help inspect resource usage. Cross-machine latency needs synchronized clocks.

Queue gauges describe shared state: **do not sum them across brokers**. Collector counters/histograms are per process, can be aggregated, and reset on restart. Completion gauges reflect retained tombstones; use collector counters for throughput.

```sh
curl http://localhost:8081/queue/v1/stats
curl http://localhost:8081/queue/v1/dead
curl -X POST http://localhost:8081/queue/v1/dead/EVENT_ID/replay
```

Dead-letter inspection returns the first 100 records. Replay retains ID/payload and resets attempts; use it after correcting the consumer/environment. A malformed payload fails again. Add `Authorization: Bearer TOKEN` when configured. In Helm, `brokerTokenSecret` names an existing Secret with key `token`.

Production ingress authentication, TLS policy, least-privilege database roles, database HA/backups, and retention policies remain deployment hardening work. The included database is a single instance, not an HA system.

## Design and AI workflow

- [Coding-agent guidance](AGENTS.md)
- [Architecture and guarantees](docs/architecture.md)
- [CSV analysis](docs/data-analysis.md)
- [Executed checks and measurements](docs/verification.md)
- [AI prompts, workflow, and corrections](docs/ai-workflow.md)

PostgreSQL is the shared throughput/availability boundary; adding brokers does not eliminate it. The implementation favors explicit correctness and bounded work over building a replicated log or consensus algorithm.
