# Barkeeper vs etcd Benchmark Results

Single-node, same hardware. All requests via HTTP/JSON gateway (`/v3/kv/*`).
Load generated with [oha](https://github.com/hatoo/oha).

## Write Throughput (PUT /v3/kv/put)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 1,964 req/s | 1,874 req/s | 1.05x |
| c=10 | 4,455 req/s | 6,697 req/s | 0.67x |
| c=50 | 4,274 req/s | 8,841 req/s | 0.48x |
| c=100 | 4,250 req/s | 9,611 req/s | 0.44x |
| c=500 | 4,349 req/s | 9,480 req/s | 0.46x |

## Read Throughput (POST /v3/kv/range, c=100, 30s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 19,138 | 10,015 |
| P50 | 5.00ms | 7.75ms |
| P95 | 9.02ms | 29.34ms |
| P99 | 11.69ms | 36.27ms |
| P99.9 | 16.68ms | 43.31ms |

## Mixed Workload (80% read / 20% write, 30s)

| Component | Metric | Barkeeper | etcd |
|---|---|---|---|
| Reads (c=80) | req/s | 10,679 | 7,006 |
| | P50 | 7.30ms | 8.97ms |
| | P99 | 15.21ms | 37.39ms |
| Writes (c=20) | req/s | 1,951 | 2,039 |
| | P50 | 9.92ms | 7.57ms |
| | P99 | 19.34ms | 35.01ms |

## Large Values (64KB PUT, c=50, 15s)

| Metric | Barkeeper | etcd |
|---|---|---|
| req/s | 61,119 | 15,546 |
| P50 | 159ms | 88.19ms |
| P99 | 386ms | 305ms |
| Success | 0.1% | 1.2% |

## Connection Scaling (GET /v3/kv/range)

| Concurrency | Barkeeper | etcd | Ratio |
|---|---|---|---|
| c=1 | 23,831 req/s | 23,791 req/s | 1.00x |
| c=10 | 80,966 req/s | 80,156 req/s | 1.01x |
| c=50 | 82,163 req/s | 81,528 req/s | 1.01x |
| c=100 | 80,656 req/s | 80,348 req/s | 1.00x |
| c=500 | 78,795 req/s | 78,729 req/s | 1.00x |
| c=1000 | 77,804 req/s | 78,419 req/s | 0.99x |
