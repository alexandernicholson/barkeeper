# Barkeeper vs etcd Benchmark Results

Single-node, same hardware. All requests via HTTP/JSON gateway (`/v3/kv/*`).
Load generated with [oha](https://github.com/hatoo/oha).

## Write Throughput (PUT /v3/kv/put)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 5,618 req/s | 2,340 req/s | 2.40x |
| c=10 | 22,198 req/s | 10,132 req/s | 2.19x |
| c=50 | 32,303 req/s | 14,785 req/s | 2.18x |
| c=100 | 33,741 req/s | 16,210 req/s | 2.08x |
| c=500 | 32,249 req/s | 16,592 req/s | 1.94x |

## Read Throughput (POST /v3/kv/range, c=100, 30s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 34,765 | 16,149 |
| P50 | 2.76ms | 5.80ms |
| P95 | 5.06ms | 10.94ms |
| P99 | 6.43ms | 14.39ms |
| P99.9 | 8.84ms | 19.71ms |

## Mixed Workload (80% read / 20% write, 30s)

| Component | Metric | Barkeeper | etcd |
|---|---|---|---|
| Reads (c=80) | req/s | 24,281 | 11,207 |
| | P50 | 3.15ms | 6.67ms |
| | P99 | 7.40ms | 16.11ms |
| Writes (c=20) | req/s | 5,456 | 3,538 |
| | P50 | 3.54ms | 5.32ms |
| | P99 | 7.71ms | 13.16ms |

## Large Values (64KB PUT, c=10, 5s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 2,198 | 1,280 |
| P50 | 4.21ms | 7.13ms |
| P99 | 9.97ms | 18.67ms |
| Success | 100.0% | 100.0% |

## Connection Scaling (GET /v3/kv/range)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 7,207 req/s | 2,427 req/s | 2.97x |
| c=10 | 29,314 req/s | 10,871 req/s | 2.70x |
| c=50 | 35,096 req/s | 15,942 req/s | 2.20x |
| c=100 | 35,040 req/s | 16,781 req/s | 2.09x |
| c=500 | 32,010 req/s | 17,648 req/s | 1.81x |
| c=1000 | 29,090 req/s | 16,959 req/s | 1.72x |
