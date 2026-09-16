#!/usr/bin/env python3
"""Finite end-to-end acceptance test in an isolated, automatically cleaned stack."""
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return str(sock.getsockname()[1])


env = dict(os.environ, RATE="500", LOOPS="1", API_PORT=free_port(),
           BROKER_PORT=free_port(), DB_PORT=free_port())
project = "telemetry-test-" + uuid.uuid4().hex[:8]
compose = ["docker", "compose", "-p", project]
api = "http://127.0.0.1:" + env["API_PORT"]
broker = "http://127.0.0.1:" + env["BROKER_PORT"]


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


try:
    subprocess.run(compose + ["up", "-d", "--no-build"], env=env, check=True)
    deadline = time.monotonic() + 120
    while True:
        try:
            stats = get(broker + "/queue/v1/stats")
            if stats["done"] == 2470 and stats["ready"] == stats["leased"] == stats["dead"] == 0:
                break
        except (urllib.error.URLError, TimeoutError):
            pass
        if time.monotonic() > deadline:
            raise RuntimeError("pipeline did not drain 2470 observations in 120 seconds")
        time.sleep(1)
    gpus = []
    after = ""
    while True:
        page = get(api + "/api/v1/gpus?limit=100&after=" + urllib.parse.quote(after))
        gpus.extend(page["items"])
        after = page.get("next_cursor", "")
        if not after:
            break
    assert len(gpus) == 247, len(gpus)
    events = set()
    for gpu in gpus:
        items = get(api + "/api/v1/gpus/" + gpu["id"] + "/telemetry")["items"]
        assert len(items) == 10, (gpu["id"], len(items))
        assert len({item["metric_name"] for item in items}) == 10
        assert items == sorted(items, key=lambda item: (item["timestamp"], item["event_id"]))
        events.update(item["event_id"] for item in items)
    assert len(events) == 2470
    first = get(api + "/api/v1/gpus/" + gpus[0]["id"] + "/telemetry")["items"][0]
    query = urllib.parse.urlencode({"start_time": first["timestamp"], "end_time": first["timestamp"]})
    exact = get(api + "/api/v1/gpus/" + gpus[0]["id"] + "/telemetry?" + query)["items"]
    assert any(item["event_id"] == first["event_id"] for item in exact)
    print(json.dumps({"result": "PASS", "gpus": len(gpus), "observations": len(events),
                      "inclusive_filters": "PASS", "queue": stats}, indent=2))
except BaseException:
    subprocess.run(compose + ["logs", "--tail", "30"], env=env)
    raise
finally:
    # Only the uniquely named test project's containers and volume are removed.
    subprocess.run(compose + ["down", "-v"], env=env, check=False)
