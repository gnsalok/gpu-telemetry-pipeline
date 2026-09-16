# Supplied CSV analysis

Source: `dcgm_metrics_20250718_134233.csv`, copied unchanged into `data/telemetry.csv`.

The file contains 2,470 records, 12 columns, 247 distinct nonempty GPU UUIDs, 31 hostnames and 10 metric names. Each UUID occurs once per metric. Thirty hosts have eight GPUs represented; `mtv5-dgx1-hgpu-012` has seven (index 6 is absent). This describes file coverage, not the physical cluster's complete inventory.

There are no exact duplicate records, including after removing the timestamp. `(uuid,metric_name)` is unique in this file but must not become a database uniqueness key: later loops create new observations.

Four source timestamps span 2025-07-18T20:42:34Z through 20:42:37Z. The assignment requires processing time; original timestamps remain provenance, not query time.

| Metric suffix | Min | Max |
|---|---:|---:|
| GPU_UTIL | 0 | 100 |
| MEM_COPY_UTIL | 0 | 80 |
| POWER_USAGE | 66.781 | 689.457 |
| GPU_TEMP | 26 | 80 |
| FB_USED | 4 | 77413 |
| FB_FREE | 3594 | 81003 |
| DEC_UTIL | 0 | 0 |
| ENC_UTIL | 0 | 0 |
| SM_CLOCK | 345 | 1980 |
| MEM_CLOCK | 2619 | 2619 |

All values are finite/nonnegative; used plus free framebuffer equals 81,007 for every GPU. Runtime validation requires finite numeric values, not a universal percentage range. `gpu_id` and `device` are host-local. All models are NVIDIA H100 80GB HBM3. Workload columns (`container`, `pod`, `namespace`) are blank and remain optional. Raw labels agree with structured fields and include driver 535.129.03, exporter instance and job dgx_dcgm_exporter. Preserve raw labels instead of using a fragile comma split.

Verify independently:

```sh
python3 - <<'PY'
import csv
from collections import Counter
with open('data/telemetry.csv', newline='') as f:
    rows = list(csv.DictReader(f))
counts = Counter(row['uuid'] for row in rows)
print('rows:', len(rows))
print('GPU UUIDs:', len(counts))
print('rows per UUID:', Counter(counts.values()))
PY
```

Expected: 2,470 rows, 247 UUIDs, `{10: 247}`. The streamer test verifies two passes produce 4,940 distinct observations while a publication retry preserves identity.
