#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$BENCH_DIR")"
RESULTS_BASE="$BENCH_DIR/results"

STACK="${1:?Usage: run.sh <barkeeper|etcd|all>}"

# Barkeeper HTTP gateway port (gRPC port + 1)
BARKEEPER_PORT=2380
# etcd client port (grpc-gateway is built-in on the same port)
ETCD_PORT=2379

run_stack() {
    local stack="$1"
    local compose_file="$BENCH_DIR/docker-compose.${stack}.yml"
    local results_dir="$RESULTS_BASE/$stack"

    if [ ! -f "$compose_file" ]; then
        echo "ERROR: $compose_file not found"
        return 1
    fi

    echo "======================================"
    echo "  Running benchmark: $stack"
    echo "======================================"

    mkdir -p "$results_dir"

    echo "Starting $stack..."
    docker compose -f "$compose_file" up -d --build

    # Determine the health check URL.
    local port
    if [ "$stack" = "barkeeper" ]; then
        port="$BARKEEPER_PORT"
    else
        port="$ETCD_PORT"
    fi

    echo "Waiting for $stack health on port $port..."
    local retries=60
    while [ $retries -gt 0 ]; do
        if curl -sf -X POST "http://localhost:${port}/v3/maintenance/status" -d '{}' > /dev/null 2>&1; then
            echo "$stack is healthy"
            break
        fi
        retries=$((retries - 1))
        sleep 2
    done

    if [ $retries -eq 0 ]; then
        echo "WARNING: Health check timed out"
        docker compose -f "$compose_file" logs
    fi

    local url="http://localhost:${port}"

    echo "Running scenarios against $url ..."

    # Pre-populate keys for read tests.
    echo "  Seeding 1000 keys..."
    for i in $(seq 1 1000); do
        key=$(printf "bench/key/%04d" "$i" | base64 -w0)
        value=$(printf "value-%04d-padding-to-increase-payload-size" "$i" | base64 -w0)
        curl -sf -X POST "${url}/v3/kv/put" \
            -d "{\"key\":\"${key}\",\"value\":\"${value}\"}" > /dev/null 2>&1 || true
    done

    # ── Scenario 1: Write Throughput Ramp ──────────────────────────────
    for C in 1 10 50 100 500; do
        echo "  Write throughput c=$C (15s)"
        key=$(echo -n "bench/write/ramp" | base64 -w0)
        value=$(echo -n "benchmark-payload-data" | base64 -w0)
        oha -c "$C" -z 15s --output-format json \
            -m POST -d "{\"key\":\"${key}\",\"value\":\"${value}\"}" \
            -T application/json \
            "${url}/v3/kv/put" > "$results_dir/write_c${C}.json" 2>/dev/null || true
    done

    # ── Scenario 2: Read Throughput ────────────────────────────────────
    echo "  Read throughput (c=100, 30s)"
    key=$(printf "bench/key/0001" | base64 -w0)
    oha -c 100 -z 30s --output-format json \
        -m POST -d "{\"key\":\"${key}\"}" \
        -T application/json \
        "${url}/v3/kv/range" > "$results_dir/read_c100.json" 2>/dev/null || true

    # ── Scenario 3: Mixed Workload (80% read / 20% write) ─────────────
    # oha doesn't natively support mixed workloads, so we run reads and
    # writes concurrently and combine in the report.
    echo "  Mixed workload: reads (c=80, 30s)"
    key=$(printf "bench/key/0500" | base64 -w0)
    oha -c 80 -z 30s --output-format json \
        -m POST -d "{\"key\":\"${key}\"}" \
        -T application/json \
        "${url}/v3/kv/range" > "$results_dir/mixed_read.json" 2>/dev/null &
    local read_pid=$!

    echo "  Mixed workload: writes (c=20, 30s)"
    wkey=$(echo -n "bench/mixed/write" | base64 -w0)
    wval=$(echo -n "mixed-payload" | base64 -w0)
    oha -c 20 -z 30s --output-format json \
        -m POST -d "{\"key\":\"${wkey}\",\"value\":\"${wval}\"}" \
        -T application/json \
        "${url}/v3/kv/put" > "$results_dir/mixed_write.json" 2>/dev/null &
    local write_pid=$!

    wait "$read_pid" || true
    wait "$write_pid" || true

    # ── Scenario 4: Large Values (64KB) ───────────────────────────────
    echo "  Large values 64KB (c=50, 15s)"
    lkey=$(echo -n "bench/large" | base64 -w0)
    lval=$(head -c 65536 /dev/urandom | base64 -w0)
    oha -c 50 -z 15s --output-format json \
        -m POST -d "{\"key\":\"${lkey}\",\"value\":\"${lval}\"}" \
        -T application/json \
        "${url}/v3/kv/put" > "$results_dir/large_values.json" 2>/dev/null || true

    # ── Scenario 5: Connection Scaling ────────────────────────────────
    for C in 1 10 50 100 500 1000; do
        echo "  Connection scaling c=$C (10s)"
        key=$(printf "bench/key/0001" | base64 -w0)
        oha -c "$C" -z 10s --output-format json \
            -m POST -d "{\"key\":\"${key}\"}" \
            -T application/json \
            "${url}/v3/kv/range" > "$results_dir/conn_c${C}.json" 2>/dev/null || true
    done

    echo "Stopping $stack..."
    docker compose -f "$compose_file" down

    echo "Results saved to $results_dir/"
    echo ""
}

if [ "$STACK" = "all" ]; then
    for s in barkeeper etcd; do
        run_stack "$s" || echo "WARNING: $s benchmark failed, continuing..."
    done
else
    run_stack "$STACK"
fi

echo "======================================"
echo "  Generating report..."
echo "======================================"

if command -v python3 &> /dev/null; then
    python3 "$SCRIPT_DIR/report.py" "$RESULTS_BASE"
else
    echo "Python3 not found, skipping report generation"
fi
