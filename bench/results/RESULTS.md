# Barkeeper vs etcd Benchmark Results

Single-node, same hardware. All requests via HTTP/JSON gateway (`/v3/kv/*`).
Load generated with [oha](https://github.com/hatoo/oha).

## Write Throughput (PUT /v3/kv/put)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 2,319 req/s | 2,340 req/s | 0.99x |
| c=10 | 4,605 req/s | 10,132 req/s | 0.45x |
| c=50 | 4,398 req/s | 14,785 req/s | 0.30x |
| c=100 | 4,351 req/s | 16,210 req/s | 0.27x |
| c=500 | 4,654 req/s | 16,592 req/s | 0.28x |

## Read Throughput (POST /v3/kv/range, c=100, 30s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 32,583 | 16,149 |
| P50 | 2.94ms | 5.80ms |
| P95 | 5.27ms | 10.94ms |
| P99 | 6.77ms | 14.39ms |
| P99.9 | 9.90ms | 19.71ms |

## Mixed Workload (80% read / 20% write, 30s)

| Component | Metric | Barkeeper | etcd |
|---|---|---|---|
| Reads (c=80) | req/s | 19,913 | 11,207 |
| | P50 | 3.83ms | 6.67ms |
| | P99 | 8.80ms | 16.11ms |
| Writes (c=20) | req/s | 2,377 | 3,538 |
| | P50 | 8.14ms | 5.32ms |
| | P99 | 15.58ms | 13.16ms |

## Large Values (64KB PUT, c=10, 5s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 392 | 1,280 |
| P50 | 23.31ms | 7.13ms |
| P99 | 37.29ms | 18.67ms |
| Success | 100.0% | 100.0% |

## Connection Scaling (GET /v3/kv/range)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 6,617 req/s | 2,427 req/s | 2.73x |
| c=10 | 28,507 req/s | 10,871 req/s | 2.62x |
| c=50 | 33,279 req/s | 15,942 req/s | 2.09x |
| c=100 | 32,865 req/s | 16,781 req/s | 1.96x |
| c=500 | 29,827 req/s | 17,648 req/s | 1.69x |
| c=1000 | 27,181 req/s | 16,959 req/s | 1.60x |
