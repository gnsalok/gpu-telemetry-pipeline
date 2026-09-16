# Verification record

Executed on 17 September 2026 (Asia/Kolkata). Results are measurements from this development environment, not production capacity guarantees.

## Environment

- macOS/Apple Silicon; Go 1.25.6.
- Docker Desktop engine 28.1.1; single-node Kubernetes v1.32.2.
- Docker VM: 11 CPUs and approximately 7.65 GiB RAM.
- PostgreSQL 18.6, Helm 3.16.1.
- Kubernetes verification: two broker replicas; two workers per collector; four DB connections per application process. Application limits 500m CPU/256 MiB each; database limit **2 CPUs/1 GiB** for the scaling test (chart default is 1 CPU/1 GiB).
- No Kubernetes metrics-server was installed. A Docker container snapshot during the 10-instance run showed streamers at approximately 8.2 MiB and sampled collectors at 8.0–8.6 MiB. This is a point-in-time observation, not a leak/soak-test result.

## Executed checks

| Check | Result |
|---|---|
| `go test ./...` | PASS |
| `go test -race ./...` | PASS |
| `go vet ./...` and formatting | PASS |
| Generated OpenAPI compared to committed output | PASS |
| Helm lint and template rendering | PASS |
| Multi-stage Docker image build | PASS |
| Real PostgreSQL integration tests with race detector | PASS |
| Isolated finite Docker end-to-end demo | PASS: 247 GPUs, 2,470 observations, queue drained, inclusive filters |
| Helm install on Docker Desktop Kubernetes | PASS |
| Concurrent 1/5/10 producer and collector workloads | PASS |
| Broker and collector pod crash recovery | PASS |
| PostgreSQL restart with 2,470 queued observations | PASS |
| Streamer Deployment scale 10 → 1 → 0 | PASS |

The final isolated cluster run reconciled **50,891 accepted messages = 50,891 acknowledged messages = 50,891 stored observations**, with no remaining ready/leased/dead records. This includes finite workloads, recovery workloads, and the variable-count continuous-source scaling exercise. The test namespace and its resources were removed after verification.

The PostgreSQL suite directly verifies publication deduplication/conflicts/capacity, concurrent claims, stale-token fencing, lease renewal/expiry, commit-before-ACK redelivery, timestamp preservation, temporary retry vs dead-letter behavior, replay, exhausted delivery retention, inclusive filters, pagination, and tombstone cleanup.

## Scaling measurements

Each producer Job reads the full supplied CSV exactly once at a requested maximum of 1,000 observations/second. Each stage waits for the expected persisted/acknowledged count and an empty outstanding queue. Stage time includes Job creation, scheduling, connection startup, publication, processing and drain. The CPU setting and query were changed together between runs, so neither change's individual effect is isolated.

| Producers | Collectors | Observations | Stage duration | Observations / stage second |
|---:|---:|---:|---:|---:|
| 1 | 1 | 2,470 | 8.00 s | 308.7 |
| 5 | 5 | 12,350 | 12.13 s | 1,018.5 |
| 10 | 10 | 24,700 | 24.82 s | 995.1 |

Scaling to five increased completed workload throughput; ten did not improve it further in this shared single-node environment. These are finite acceptance-workload rates, not steady-state maximum throughput. Broker/database admission, transaction work, CPU/disk limits and shared resource contention remain boundaries. A separate Docker acceptance test briefly overlapped the beginning of the run, so these are not isolated benchmark conditions.

The script also reports cumulative acceptance-to-ACK latency from durable queue timestamps. At the end of each stage, cumulative mean/p95 were respectively 3.760/4.477 s, 0.670/4.123 s and 0.273/3.278 s. They include all prior stages and therefore must not be interpreted as per-stage comparable percentiles.

## Coverage

`make coverage` produces console output, `coverage.out` and an HTML report. `make integration` reports real-database coverage separately.

| Package | Unit statement coverage |
|---|---:|
| API | 74.1% |
| Broker protocol/client | 90.4% |
| Configuration | 100.0% |
| Model | 94.4% |
| Workers | 59.9% |

The all-package unit-only total is **41.3%**, because process startup and PostgreSQL storage paths are not executed by unit tests. The PostgreSQL storage package reaches **73.8%** in its separate integration suite. Container/Kubernetes tests execute the application entry point but do not collect Go statement coverage. No higher combined coverage figure is claimed.

## Failed intermediate checks and corrections

1. The first 10-instance cluster workload exceeded its 180-second deadline with a one-CPU database limit. At the final sample there were 29,605 stored records, 29,599 ACKed records, 2,485 outstanding messages and no dead letters; that run was incomplete and is not counted as a pass. Expiry recovery and ready claims were separated so ready claims can use their ordered partial index, avoiding sorting all eligible work. The verification database limit was increased to two CPUs and its deadline to 600 seconds. The final 10-instance stage completed in 24.82 seconds.
2. An intermediate recovery script chose a collector already terminating after scale-down and got a NotFound error. Selection now excludes terminating/non-running pods. The subsequent full run actually killed a live collector and broker and completed successfully.

## Explicit limits

Not verified: multi-node performance, node/network partition failure, database HA/failover or lost-volume recovery, long-duration storage/heap stability, production security hardening, or maximum sustained throughput. CI configuration is included; remote GitHub Actions status must be checked separately after publishing. The source deliberately simulates observations rather than reading physical GPUs.

To reproduce: `make demo` and `python3 scripts/k8s_verify.py`. The latter creates and removes a dedicated namespace and requires the image to be available to the cluster. All results depend on local CPU/memory/disk allocation and concurrent activity.
