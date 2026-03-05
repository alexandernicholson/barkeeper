# Barkeeper vs etcd Benchmark Results

Single-node, same hardware. All requests via HTTP/JSON gateway (`/v3/kv/*`).
Load generated with [oha](https://github.com/hatoo/oha).

## Write Throughput (PUT /v3/kv/put)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 3,599 req/s | 2,340 req/s | 1.54x |
| c=10 | 4,829 req/s | 10,132 req/s | 0.48x |
| c=50 | 4,823 req/s | 14,785 req/s | 0.33x |
| c=100 | 4,713 req/s | 16,210 req/s | 0.29x |
| c=500 | 4,685 req/s | 16,592 req/s | 0.28x |

## Read Throughput (POST /v3/kv/range, c=100, 30s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 32,596 | 16,149 |
| P50 | 2.94ms | 5.80ms |
| P95 | 5.22ms | 10.94ms |
| P99 | 6.63ms | 14.39ms |
| P99.9 | 9.47ms | 19.71ms |

## Mixed Workload (80% read / 20% write, 30s)

| Component | Metric | Barkeeper | etcd |
|---|---|---|---|
| Reads (c=80) | req/s | 21,774 | 11,207 |
| | P50 | 3.52ms | 6.67ms |
| | P99 | 7.96ms | 16.11ms |
| Writes (c=20) | req/s | 2,371 | 3,538 |
| | P50 | 8.17ms | 5.32ms |
| | P99 | 15.29ms | 13.16ms |

## Large Values (64KB PUT, c=10, 5s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 1,061 | 1,280 |
| P50 | 9.31ms | 7.13ms |
| P99 | 16.83ms | 18.67ms |
| Success | 100.0% | 100.0% |

## Connection Scaling (GET /v3/kv/range)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 6,421 req/s | 2,427 req/s | 2.65x |
| c=10 | 27,590 req/s | 10,871 req/s | 2.54x |
| c=50 | 32,488 req/s | 15,942 req/s | 2.04x |
| c=100 | 32,186 req/s | 16,781 req/s | 1.92x |
| c=500 | 29,627 req/s | 17,648 req/s | 1.68x |
| c=1000 | 26,248 req/s | 16,959 req/s | 1.55x |
