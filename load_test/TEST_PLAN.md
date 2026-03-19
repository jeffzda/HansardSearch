# Hansard Search — Load Test Plan

## Objective

Quantify the real-world performance of the Hansard Search webapp (Flask + SQLite FTS5,
single-worker gunicorn on a small VPS) under realistic and worst-case concurrent user loads
before the corpus grows or user numbers increase.

---

## System Under Test

- **App**: Flask 3.x, served by gunicorn (single worker + threads)
- **Search backend**: SQLite FTS5 index over ~1.4M rows (Senate + House + Committee)
- **Infrastructure**: Single VPS, limited RAM/CPU
- **Endpoint under test**: `POST /api/search` (primary), `POST /api/day_context` (secondary)

---

## Test Scenarios

| Scenario | Users | Duration | Delay | Query mix | Purpose |
|----------|-------|----------|-------|-----------|---------|
| `cache_friendly` | 20 | 5 min | 3–7s | 5 fixed easy queries | Warm cache; measure cached-hit baseline |
| `cache_unfriendly` | 20 | 5 min | 3–7s | Random unique queries | Stress FTS index; no cache benefit |
| `worst_case` | 10 | 3 min | 1–3s | 100% worst bucket | Find hard performance ceiling |
| `burst` | 50 | 30s | 0s | Easy + medium | Measure gunicorn saturation under spike |
| `steady_state` | 30 | 25 min | 4–8s | Default weights | Primary soak test; measure latency drift |

**Recommended execution order**: cache_friendly → cache_unfriendly → worst_case → steady_state → burst (last, to avoid starving other tests).

---

## Query Buckets

| Bucket | Description | FTS cost | Cache behaviour |
|--------|-------------|----------|-----------------|
| `easy` | Single common term | Low | Cache-friendly (repeats) |
| `medium` | 2-term OR or AND | Moderate | Semi-friendly |
| `heavy` | `(A OR B) AND (C OR D)` | Higher | Unfriendly |
| `worst` | 6-term OR, page 20+, `case_sensitive=True`, both chambers | Maximum | Always unfriendly |

Default weights: easy=40%, medium=30%, heavy=20%, worst=10%.

---

## Metrics & Pass/Warn/Fail Thresholds

| Metric | PASS | WARN | FAIL |
|--------|------|------|------|
| Error rate (non-200 or conn error) | < 0.5% | < 2% | ≥ 2% |
| Timeout rate (> `--timeout`) | < 0.5% | < 1% | ≥ 1% |
| Degraded rate (latency > 3s) | < 5% | < 15% | ≥ 15% |
| p50 latency | < 0.5s | < 1.5s | ≥ 1.5s |
| p95 latency | < 3s | < 6s | ≥ 6s |
| p99 latency | < 8s | < 15s | ≥ 15s |
| Latency drift (last-10% / first-10% p50) | < 1.20× | < 1.50× | ≥ 1.50× |

---

## Data Collected

Per-request CSV columns:

```
timestamp, scenario, bucket, expression, chamber, page,
status_code, latency_s, timed_out, response_valid, error_msg
```

---

## Output Artefacts

```
load_test/results/run_YYYYMMDD_HHMMSS/
├── requests.csv       # raw per-request data
├── summary.json       # computed stats + threshold results
├── summary.md         # human-readable markdown table
└── plots/             # (if matplotlib installed)
    ├── latency_over_time.png
    ├── latency_histogram.png
    └── throughput.png
```

---

## Server-Side Monitoring

Run these on the server concurrently with the load test to correlate with CSV timestamps:

```bash
# CPU + RAM every 5s
vmstat -t 5 | tee vmstat_log.txt

# Disk I/O
iostat -x 5 | tee iostat_log.txt

# Gunicorn logs
journalctl -u hansard -f

# Memory check
watch -n 5 "ps aux | grep gunicorn | awk '{print \$6/1024 \"MB\"}'"
```

---

## How to Run

```bash
cd /home/jeffzda/Hansard
pip install aiohttp numpy matplotlib   # matplotlib optional

# Quick smoke test (5 users, 60s)
python load_test/load_test.py --url http://localhost:5000 \
    --scenario cache_friendly --users 5 --duration 60

# Primary soak test
python load_test/load_test.py --url https://your-domain \
    --scenario steady_state

# Worst-case ceiling
python load_test/load_test.py --scenario worst_case --users 10 --duration 180

# Burst spike
python load_test/load_test.py --scenario burst --users 50 --duration 30
```

---

## Interpreting Results

| Observation | Likely cause | Action |
|-------------|-------------|--------|
| p95 < 3s, error rate < 0.5% | Healthy | No action needed |
| p95 3–6s or degraded > 5% | FTS contention or memory pressure | Consider query caching, index tuning |
| Latency drift > 50% | Memory leak or SQLite lock contention | Check RSS growth with vmstat; profile gunicorn |
| Burst fails, steady-state passes | Gunicorn worker/thread saturation | Review `--workers` / `--threads` gunicorn config |
| Worst-case p99 > 15s | Expected on small VPS | Document ceiling; consider connection pool limits |

---

## Dependencies

```
aiohttp       # async HTTP client (required)
numpy         # latency percentiles (required)
matplotlib    # plots (optional — silently skipped if absent)
```

Install: `pip install aiohttp numpy matplotlib`
