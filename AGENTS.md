# Coding-agent guidance

## Project and boundaries

- Read `README.md` and `docs/architecture.md` before changing pipeline behavior.
- This is a Go take-home project; keep changes focused, idiomatic, and reviewable.
- Streamers replay CSV records and publish observations; they do not own queue state.
- Brokers own publication, claims, leases, acknowledgments, retries, and dead letters.
- Collectors validate and persist observations before acknowledging deliveries.
- The REST API reads persisted telemetry; it does not consume queue messages.
- Implement queue behavior here; do not replace it with an existing MQ/framework.

## Correctness rules

- Keep the same event ID and payload across publication retries and redelivery.
- Give each new observation a new ID, including later CSV loops and other producers.
- Never deduplicate by equal values, source timestamps, or GPU/metric pairs.
- Identify GPUs by UUID; device indices are local to a host.
- Preserve the first committed processing timestamp when persistence is retried.
- Retain CSV timestamps as provenance, not as the API query timestamp.
- Enforce current lease tokens for ACK, NACK, and renewal; stale workers must be fenced.
- Distinguish dependency outages from malformed records; do not discard accepted work.
- Document changes to delivery, ordering, retention, or deduplication guarantees.

## Implementation conventions

- Use explicit errors, context cancellation, bounded deadlines, and structured logs.
- Keep workers, goroutines, buffering, request sizes, and connection pools bounded.
- Account for aggregate database connections when increasing replica counts.
- Keep SQL transitions transactional; do not hold transactions during processing.
- Format Go with `gofmt`; add tests for meaningful behavior and failure paths.
- Keep API definitions and generated OpenAPI consistent; avoid hand-editing the spec.

## Validation

- Run `make test` for affected application behavior and `make race` for concurrency.
- Run `make check` for formatting, vet, race, OpenAPI drift, and Helm validation.
- Run `make coverage` to report unit coverage; do not confuse it with integration coverage.
- Run `make openapi` after API changes, then review the generated diff.
- `make integration` requires a disposable `TEST_DATABASE_URL`; it truncates tables.
- Never point integration tests at a development, shared, or production database.
- Use `make demo` for the isolated Docker acceptance test.
- Use `python3 scripts/k8s_verify.py` for isolated cluster scaling/recovery checks.
- Documentation-only changes need link/command review and `git diff --check`, not application tests.

## Deployment and records

- Preserve Docker Desktop single-node Kubernetes support in the Helm chart.
- Keep replicas independent; avoid fixed pod addresses, sticky sessions, or required node separation.
- Distinguish application scaling from database HA and multi-node performance.
- Preserve `data/telemetry.csv`; do not repair or remove source rows without an explicit requirement.
- Report only executed checks and measured results in `docs/verification.md`.
- Do not invent human intervention, coverage figures, production readiness, or HA claims.
