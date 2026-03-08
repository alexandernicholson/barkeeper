# Barkeeper vs etcd Benchmark Results

Single-node, same hardware. All requests via HTTP/JSON gateway (`/v3/kv/*`).
Load generated with [oha](https://github.com/hatoo/oha).

## Write Throughput (PUT /v3/kv/put)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 5,284 req/s | 2,340 req/s | 2.26x |
| c=10 | 19,836 req/s | 10,132 req/s | 1.96x |
| c=50 | 27,684 req/s | 14,785 req/s | 1.87x |
| c=100 | 29,639 req/s | 16,210 req/s | 1.83x |
| c=500 | 28,765 req/s | 16,592 req/s | 1.73x |

## Read Throughput (POST /v3/kv/range, c=100, 30s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 33,512 | 16,149 |
| P50 | 2.85ms | 5.80ms |
| P95 | 5.28ms | 10.94ms |
| P99 | 6.85ms | 14.39ms |
| P99.9 | 9.70ms | 19.71ms |

## Mixed Workload (80% read / 20% write, 30s)

| Component | Metric | Barkeeper | etcd |
|---|---|---|---|
| Reads (c=80) | req/s | 24,091 | 11,207 |
| | P50 | 3.17ms | 6.67ms |
| | P99 | 7.52ms | 16.11ms |
| Writes (c=20) | req/s | 4,796 | 3,538 |
| | P50 | 3.99ms | 5.32ms |
| | P99 | 8.82ms | 13.16ms |

## Large Values (64KB PUT, c=10, 5s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 2,350 | 1,280 |
| P50 | 3.97ms | 7.13ms |
| P99 | 8.22ms | 18.67ms |
| Success | 100.0% | 100.0% |

## Connection Scaling (GET /v3/kv/range)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 7,101 req/s | 2,427 req/s | 2.93x |
| c=10 | 28,646 req/s | 10,871 req/s | 2.63x |
| c=50 | 34,554 req/s | 15,942 req/s | 2.17x |
| c=100 | 34,213 req/s | 16,781 req/s | 2.04x |
| c=500 | 30,980 req/s | 17,648 req/s | 1.76x |
| c=1000 | 27,633 req/s | 16,959 req/s | 1.63x |
