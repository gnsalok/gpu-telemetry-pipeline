# AI-assisted development record

## Attribution

The candidate supplied the assignment/CSV, reviewed the architecture, selected PostgreSQL-backed brokers, required Docker Desktop Kubernetes testing and created the repository. Codex analyzed inputs, proposed designs, implemented initial code/tests/deployment/docs, executed checks and corrected observed problems. This implementation was heavily AI-assisted and should not be represented as manually authored code.

No human code corrections are claimed. Human interventions below are actual design feedback; automated corrections are labeled separately.

## Task prompts

The substantive user requests, in conversation order:

1. “Go through the PDF on requirement and csv file of date (timestamp in cvs is wrong and all are might be duplicate entry) but rest field you also analyse.”
2. “First make me understand this project and whille designing message queue please keep deduplication, guarantee, retry etc into consideration or suggest in case I miss something.”
3. “Goal here is to get clear this round. Design and DX bother matters to the project, specially if you see success criteria.”
4. “Also, how should I start planning this project and attach codebase repo so that you can start working. Or should I give prompt to codex separatly?”
5. Structured answers: “3–5 focused days”, “Strong in both” (Go/Kubernetes), “No repository yet”, and “PostgreSQL-backed brokers”.
6. “No, I want to review first and since elastic, scalable and stable telemetry is part of requirement, current design fulfil it or not?”
7. “Also make sure this implementation we can test using Docker Desktop single node k8 cluster but it should be scalable?”
8. “can you help me to understand streamer and collector how will you implement in demo, is it any open source pkg or how you originally planned?”
9. “What exactly REST API is doing here and can you help me to understand csv file data and what it will get from those API.”
10. “247 GPUs how do you know that how to verify?”
11. “Can you tell me the good name of this project repo and about section so that I can share the repo link here so that you can start working on the plan?”
12. With this repository URL: “I have added this repo, let's implement this?”

There were no separate user prompts for individual files/tests and no delegated subagents. Code, unit-test and build-environment bootstrapping followed the final implementation request with preceding constraints as context. This records task prompts and observable workflow, not hidden reasoning or platform system instructions.

## AI contributions and verification

| Area | AI contribution | Check |
|---|---|---|
| Requirements/data | Read PDF; count UUIDs, metrics, duplicates and missing fields | Independent row/UUID/host counts |
| Bootstrap | Go module, role binary, package boundaries, dependencies | Build, formatting, vet/race |
| Queue | SQL states, admission, identity, leases and fencing | Real PostgreSQL concurrency/recovery tests |
| Workers | Replay, bounded concurrency, retries, persistence | Unit tests and finite Docker demo |
| API | Typed handlers, generated OpenAPI | HTTP/pagination/filter tests, contract drift check |
| Deployment | Docker, Compose, Helm, CI | Image build, Helm checks, actual cluster installation |
| Documentation | Guarantees, workflows, limitations and prompt record | Compared claims with executed results |

## Where the first proposal fell short

- **Human feedback:** Codex finalized the initial plan before the candidate completed review. The candidate challenged elasticity/scalability/stability; the design was revised to require measured tests and distinguish replica scaling from infrastructure HA.
- **Human constraint:** single-node Docker Desktop became the test target; no required anti-affinity or multi-node performance claims.
- **Human choice:** Codex initially recommended an embedded-store single broker. The candidate chose PostgreSQL-backed brokers; the code implements that selection.
- **Evidence correction:** suspected duplicate CSV records were distinct GPU/metric observations. Deduplication uses event identity, not equal values or GPU/metric pairs.

## Automated corrections during implementation

- Go checksum cache writes initially targeted a restricted directory. Temporary caches were relocated without disabling checksum verification.
- Docker Buildx cache writes were similarly isolated rather than changing user settings.
- Readiness initially checked only database connectivity. Review caught the migration-startup gap; readiness now checks required schema tables too.
- Unit coverage showed database functions at zero because unit tests intentionally avoid PostgreSQL. Separate real-database coverage was added rather than claiming those paths were unit-tested.
- Dependency-failure NACKs were separated from poison handling so normal outages do not immediately exhaust message attempts.
- The first 10-instance Kubernetes verification exceeded a three-minute test deadline with the database capped at one CPU. The claim query combined ready and expired leases and sorted eligible work. Review separated indexed lease recovery from indexed ready claims, and the verification workload was rerun with a two-CPU database limit and a longer explicit deadline. The initial unsuccessful run is retained in the verification record rather than hidden.
- The recovery script initially selected a pod already terminating during scale-down and received a NotFound error. It now selects a running, non-terminating pod so the failure injection actually exercises a live worker.

See `verification.md` for executed checks and limits. Append subsequent prompts and actual human corrections as work continues; never invent manual intervention or test results retrospectively.
