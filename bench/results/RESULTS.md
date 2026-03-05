# Barkeeper vs etcd Benchmark Results

Single-node, same hardware. All requests via HTTP/JSON gateway (`/v3/kv/*`).
Load generated with [oha](https://github.com/hatoo/oha).

## Write Throughput (PUT /v3/kv/put)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 4,970 req/s | 2,340 req/s | 2.12x |
| c=10 | 4,778 req/s | 10,132 req/s | 0.47x |
| c=50 | 4,698 req/s | 14,785 req/s | 0.32x |
| c=100 | 4,623 req/s | 16,210 req/s | 0.29x |
| c=500 | 4,437 req/s | 16,592 req/s | 0.27x |

## Read Throughput (POST /v3/kv/range, c=100, 30s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 32,985 | 16,149 |
| P50 | 2.91ms | 5.80ms |
| P95 | 5.19ms | 10.94ms |
| P99 | 6.59ms | 14.39ms |
| P99.9 | 8.93ms | 19.71ms |

## Mixed Workload (80% read / 20% write, 30s)

| Component | Metric | Barkeeper | etcd |
|---|---|---|---|
| Reads (c=80) | req/s | 22,363 | 11,207 |
| | P50 | 3.40ms | 6.67ms |
| | P99 | 7.84ms | 16.11ms |
| Writes (c=20) | req/s | 2,389 | 3,538 |
| | P50 | 8.64ms | 5.32ms |
| | P99 | 17.69ms | 13.16ms |

## Large Values (64KB PUT, c=10, 5s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 1,111 | 1,280 |
| P50 | 9.48ms | 7.13ms |
| P99 | 13.48ms | 18.67ms |
| Success | 100.0% | 100.0% |

## Connection Scaling (GET /v3/kv/range)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 6,479 req/s | 2,427 req/s | 2.67x |
| c=10 | 28,321 req/s | 10,871 req/s | 2.61x |
| c=50 | 32,378 req/s | 15,942 req/s | 2.03x |
| c=100 | 32,705 req/s | 16,781 req/s | 1.95x |
| c=500 | 30,086 req/s | 17,648 req/s | 1.70x |
| c=1000 | 27,222 req/s | 16,959 req/s | 1.61x |
