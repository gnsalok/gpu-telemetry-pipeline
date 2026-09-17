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

Prerequisites: Git, Make, curl, and Docker Desktop with its engine running. Go 1.25.6+ is needed for development; Python 3 for the acceptance scripts. Kubernetes and Helm 3 are needed only for cluster deployment.

```sh
git clone https://github.com/gnsalok/gpu-telemetry-pipeline.git
cd gpu-telemetry-pipeline
make up
# Wait for API readiness, with at most 30 bounded attempts.
(
  attempt=0
  until curl --fail --silent --output /dev/null --max-time 2 http://localhost:8080/readyz; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
      echo 'API did not become ready; see Troubleshooting.' >&2
      exit 1
    fi
    sleep 2
  done
) && curl --fail http://localhost:8080/api/v1/gpus
```

Browse the interactive API documentation at http://localhost:8080/docs. Readiness confirms the schema is available; GPU inventory may briefly be empty until collectors persist observations.

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
```

An illustrative telemetry response is shown below. Event IDs and processing timestamps are generated during ingestion and will differ in your run. Optional empty workload fields are omitted; `next_cursor` appears only when another page exists.

```json
{
  "items": [
    {
      "event_id": "a1b2c3d4e5f60718293a4b5c6d7e8f90",
      "timestamp": "2026-09-18T10:00:00.123456Z",
      "gpu_uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
      "metric_name": "DCGM_FI_DEV_GPU_UTIL",
      "value": 0,
      "source": {
        "source_timestamp": "2025-07-18T20:42:34Z",
        "metric_name": "DCGM_FI_DEV_GPU_UTIL",
        "gpu_id": "0",
        "device": "nvidia0",
        "uuid": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
        "model": "NVIDIA H100 80GB HBM3",
        "host": "mtv5-dgx1-hgpu-031",
        "value": "0",
        "labels_raw": "DCGM_FI_DRIVER_VERSION=\"535.129.03\",Hostname=\"mtv5-dgx1-hgpu-031\",UUID=\"GPU-5fd4f087-86f3-7a43-b711-4771313afc50\",__name__=\"DCGM_FI_DEV_GPU_UTIL\",device=\"nvidia0\",gpu=\"0\",instance=\"mtv5-dgx1-hgpu-031:9400\",job=\"dgx_dcgm_exporter\",modelName=\"NVIDIA H100 80GB HBM3\""
      }
    }
  ]
}
```

To try inclusive time filters using an observation from your own run (requires Python 3), fetch its processing timestamp and use it as both boundaries:

```sh
gpu_uuid='GPU-5fd4f087-86f3-7a43-b711-4771313afc50'
observation_time=$(curl --fail --silent --show-error "http://localhost:8080/api/v1/gpus/$gpu_uuid/telemetry?limit=1" |
  python3 -c 'import json,sys; items=json.load(sys.stdin)["items"]; items or sys.exit("No observations yet; retry after ingestion."); print(items[0]["timestamp"])') &&
curl --fail --get "http://localhost:8080/api/v1/gpus/$gpu_uuid/telemetry" \
  --data-urlencode "start_time=$observation_time" \
  --data-urlencode "end_time=$observation_time"
```

Both boundaries include the selected observation. `--data-urlencode` also handles timezone offsets safely.

- GPU identity is the CSV `uuid`, not the host-local `gpu_id`.
- Responses contain `items` and, when another page exists, `next_cursor`.
- For GPU pages, pass the cursor as `after`. For telemetry, pass it as `cursor` with the same time filters.
- Both boundaries are inclusive RFC3339 timestamps. URL-encode offsets containing `+`.
- Telemetry is ordered by `(timestamp, event_id)`. *Time is assigned during the first committed collector insert, not copied from the CSV.*
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

## Project layout

```text
cmd/pipeline/    Application entry point and role selection
internal/
  worker/       CSV streamer and collector loops
  broker/       Queue HTTP protocol and client
  store/        Queue transactions, telemetry queries, and schema
  api/          Public REST API and generated OpenAPI
  model/        Event types and measurement validation
  config/       Configuration and bounds
deploy/helm/    Kubernetes deployment chart
scripts/        Docker and Kubernetes acceptance tests
data/           Supplied telemetry dataset
docs/           Architecture, analysis, OpenAPI, and verification
```

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

## Troubleshooting

- **Docker is unavailable:** start Docker Desktop and confirm `docker info` succeeds before building or starting services.
- **Ports are occupied:** choose free Compose ports, for example `API_PORT=18080 BROKER_PORT=18081 DB_PORT=25432 make up`. Query the API at `http://localhost:18080` and the broker at `http://localhost:18081`; repeat the same overrides for subsequent Compose commands. For Kubernetes, use `kubectl port-forward -n telemetry svc/telemetry-telemetry-api 18080:8080` to avoid an occupied local 8080.
- **Compose services are not ready:** inspect `docker compose ps --all` and `docker compose logs --tail=100 postgres migrate broker collector api`. Applications depend on successful migration; readiness requires the schema. Check database and migration errors before restarting services.
- **Kubernetes is unavailable or using the wrong context:** enable Kubernetes in Docker Desktop, check `kubectl config current-context`, select `kubectl config use-context docker-desktop` for the local demo, and confirm `kubectl get nodes` shows a Ready node.
- **Pods cannot find the image:** run `make image` before `make k8s-install` on Docker Desktop. For another cluster, publish the image and supply the registry repository/tag to Helm. Inspect the affected pod with `kubectl describe pod POD_NAME -n telemetry`.
- **Kubernetes pods are not ready:** inspect `make k8s-status`, `kubectl get jobs -n telemetry`, and `kubectl get events -n telemetry --sort-by=.metadata.creationTimestamp`. Use `kubectl logs -n telemetry job/JOB_NAME` for the migration Job named by the jobs command; it may retry while PostgreSQL starts. Collector logs are available with `kubectl logs -n telemetry -l app.kubernetes.io/component=collector --tail=100`.

## Design and AI workflow

- [Coding-agent guidance](AGENTS.md)
- [Architecture and guarantees](docs/architecture.md)
- [CSV data source analysis](docs/data-analysis.md)
- [Executed checks and measurements](docs/verification.md)

PostgreSQL is the shared throughput/availability boundary; adding brokers does not eliminate it. The implementation favors explicit correctness and bounded work over building a replicated log or consensus algorithm.

## Maintainer

[Alok Tripathi](https://github.com/gnsalok)
