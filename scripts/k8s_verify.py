#!/usr/bin/env python3
"""Run scaling/recovery checks in a newly created, disposable Kubernetes namespace.

Requires the gpu-telemetry:dev image to be available to the selected cluster.
Results include startup/drain overhead and are not maximum-throughput benchmarks.
"""
import json
import subprocess
import time
import uuid

namespace = "telemetry-verify-" + uuid.uuid4().hex[:8]
name = "verify-telemetry"
results = []


def run(*args, data=None):
    return subprocess.check_output(args, input=data, text=True).strip()


def k(*args, data=None):
    return run("kubectl", "-n", namespace, *args, data=data)


def sql(query):
    return k("exec", name + "-postgres-0", "--", "psql", "-U", "telemetry",
             "-d", "telemetry", "-Atc", query)


def snapshot():
    return [int(x) for x in sql("SELECT (SELECT count(*) FROM telemetry.observations),"
                              "(SELECT count(*) FROM queue.messages WHERE state='done'),"
                              "(SELECT outstanding FROM queue.capacity),"
                              "(SELECT count(*) FROM queue.messages WHERE state='dead')").split("|")]


def wait_drain(expected):
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        state = snapshot()
        if state == [expected, expected, 0, 0]:
            return
        if state[3] or state[0] > expected:
            raise RuntimeError(f"unexpected counts: {state}; expected {expected}")
        time.sleep(1)
    raise RuntimeError(f"did not drain: {snapshot()}")


def scale(role, replicas):
    k("scale", "deployment/" + name + "-" + role, "--replicas=" + str(replicas))
    k("rollout", "status", "deployment/" + name + "-" + role, "--timeout=120s")


def producers(stage, count):
    for i in range(count):
        obj = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": f"source-{stage}-{i}"},
               "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 600,
                        "template": {"spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
                        "containers": [{"name": "streamer", "image": "gpu-telemetry:dev", "imagePullPolicy": "IfNotPresent",
                                        "args": ["streamer"], "env": [
                                            {"name": "BROKER_URL", "value": "http://" + name + "-broker:8080"},
                                            {"name": "LOOPS", "value": "1"}, {"name": "RATE", "value": "1000"}],
                                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"},
                                                      "limits": {"cpu": "500m", "memory": "256Mi"}}}]}}}}
        k("apply", "-f", "-", data=json.dumps(obj))


def kill_one(role):
    pods = json.loads(k("get", "pods", "-l", "app.kubernetes.io/component=" + role, "-o", "json"))["items"]
    active = [pod for pod in pods if not pod["metadata"].get("deletionTimestamp")
              and pod.get("status", {}).get("phase") == "Running"]
    if not active:
        raise RuntimeError("no running, non-terminating " + role + " pod to crash")
    k("delete", "pod", active[0]["metadata"]["name"], "--grace-period=0", "--force", "--wait=false")


try:
    run("helm", "upgrade", "--install", "verify", "deploy/helm/telemetry", "-n", namespace,
        "--create-namespace", "--set", "streamer.replicas=0", "--set", "postgresql.resources.limits.cpu=2", "--wait", "--wait-for-jobs", "--timeout", "5m")
    expected = 0
    for replicas in [1, 5, 10]:
        scale("collector", replicas)
        started = time.monotonic()
        producers(str(replicas), replicas)
        expected += 2470 * replicas
        wait_drain(expected)
        elapsed = time.monotonic() - started
        latency = sql("SELECT round(avg(extract(epoch FROM (completed_at-created_at)))::numeric,3),"
                      "round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM (completed_at-created_at)))::numeric,3)"
                      " FROM queue.messages WHERE state='done'")
        results.append({"streamers": replicas, "collectors": replicas, "observations": 2470 * replicas,
                        "elapsed_seconds": round(elapsed, 2), "stage_observations_per_second": round(2470 * replicas / elapsed, 1),
                        "cumulative_accept_to_ack_mean_p95_seconds": latency})
        print(json.dumps(results[-1]), flush=True)
    scale("collector", 1)
    producers("recovery", 1)
    time.sleep(2)
    kill_one("collector")
    kill_one("broker")
    expected += 2470
    wait_drain(expected)
    print("Pod crash recovery: PASS", flush=True)
    # Persist backlog without collectors, restart PostgreSQL, then drain.
    scale("collector", 0)
    producers("database-restart", 1)
    k("wait", "--for=condition=complete", "job/source-database-restart-0", "--timeout=120s")
    kill_one("postgres")
    k("rollout", "status", "statefulset/" + name + "-postgres", "--timeout=120s")
    # StatefulSet status may briefly describe the previous pod; wait for SQL too.
    for attempt in range(60):
        try:
            sql("SELECT 1")
            break
        except subprocess.CalledProcessError:
            time.sleep(1)
    scale("collector", 2)
    expected += 2470
    wait_drain(expected)
    # Exercise actual Deployment scale-up/down with continuous producers too.
    scale("streamer", 10)
    time.sleep(3)
    scale("streamer", 1)
    time.sleep(2)
    scale("streamer", 0)
    for attempt in range(60):
        pods = json.loads(k("get", "pods", "-l", "app.kubernetes.io/component=streamer", "-o", "json"))["items"]
        if not pods:
            break
        time.sleep(1)
    else:
        raise RuntimeError("streamers did not terminate after scale-down")
    expected = int(sql("SELECT count(*) FROM queue.messages"))
    wait_drain(expected)
    print(json.dumps({"result": "PASS", "total_observations": expected,
                      "database_restart_with_backlog": "PASS", "streamer_deployment_scale_10_1_0": "PASS", "stages": results}, indent=2))
except BaseException:
    subprocess.run(["kubectl", "-n", namespace, "get", "pods,jobs"], check=False)
    subprocess.run(["kubectl", "-n", namespace, "logs", "-l", "app.kubernetes.io/component=broker", "--tail=20"], check=False)
    raise
finally:
    subprocess.run(["kubectl", "delete", "namespace", namespace, "--wait=false"], check=False)
