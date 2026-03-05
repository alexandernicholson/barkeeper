#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$BENCH_DIR")"
RESULTS_BASE="$BENCH_DIR/results"

STACK="${1:?Usage: run.sh <barkeeper|etcd|all> [--native]}"
NATIVE=false
if [[ "${2:-}" == "--native" ]]; then
    NATIVE=true
fi

# Barkeeper HTTP gateway port (gRPC port + 1)
BARKEEPER_PORT=2380
# etcd client port (grpc-gateway is built-in on the same port)
ETCD_PORT=2379

TMPFS_DIR=""
NATIVE_PID=""

cleanup_native() {
    if [ -n "$NATIVE_PID" ] && kill -0 "$NATIVE_PID" 2>/dev/null; then
        echo "Stopping native process (PID $NATIVE_PID)..."
        kill "$NATIVE_PID" 2>/dev/null || true
        wait "$NATIVE_PID" 2>/dev/null || true
    fi
    if [ -n "$TMPFS_DIR" ] && mountpoint -q "$TMPFS_DIR" 2>/dev/null; then
        echo "Unmounting tmpfs at $TMPFS_DIR..."
        sudo umount "$TMPFS_DIR" 2>/dev/null || true
        rmdir "$TMPFS_DIR" 2>/dev/null || true
    fi
}

start_native_barkeeper() {
    echo "Building barkeeper (release)..."
    (cd "$REPO_DIR" && cargo build --release 2>&1 | tail -3)

    TMPFS_DIR=$(mktemp -d)
    sudo mount -t tmpfs -o size=2G tmpfs "$TMPFS_DIR"
    echo "tmpfs mounted at $TMPFS_DIR"

    "$REPO_DIR/target/release/barkeeper" \
        --listen-client-urls=0.0.0.0:2379 \
        --data-dir="$TMPFS_DIR/data" &
    NATIVE_PID=$!
    echo "barkeeper started (PID $NATIVE_PID)"
}

start_native_etcd() {
    if ! command -v etcd &> /dev/null; then
        echo "ERROR: etcd not found in PATH. Install etcd or use Docker mode."
        return 1
    fi

    TMPFS_DIR=$(mktemp -d)
    sudo mount -t tmpfs -o size=2G tmpfs "$TMPFS_DIR"
    echo "tmpfs mounted at $TMPFS_DIR"

    etcd \
        --listen-client-urls=http://0.0.0.0:2379 \
        --advertise-client-urls=http://localhost:2379 \
        --data-dir="$TMPFS_DIR/data" &
    NATIVE_PID=$!
    echo "etcd started (PID $NATIVE_PID)"
}

run_scenarios() {
    local url="$1"
    local results_dir="$2"

    mkdir -p "$results_dir"

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
    echo "  Large values 64KB (c=10, 5s)"
    lkey=$(echo -n "bench/large" | base64 -w0)
    lval=$(head -c 65536 /dev/urandom | base64 -w0)
    oha -c 10 -z 5s --output-format json \
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
}

run_stack() {
    local stack="$1"
    local results_dir="$RESULTS_BASE/$stack"

    echo "======================================"
    echo "  Running benchmark: $stack ($( $NATIVE && echo native || echo docker ))"
    echo "======================================"

    local port
    if [ "$stack" = "barkeeper" ]; then
        port="$BARKEEPER_PORT"
    else
        port="$ETCD_PORT"
    fi

    if $NATIVE; then
        trap cleanup_native EXIT

        if [ "$stack" = "barkeeper" ]; then
            start_native_barkeeper
        else
            start_native_etcd
        fi

        echo "Waiting for $stack health on port $port..."
        local retries=30
        while [ $retries -gt 0 ]; do
            if curl -sf -X POST "http://localhost:${port}/v3/maintenance/status" -d '{}' > /dev/null 2>&1; then
                echo "$stack is healthy"
                break
            fi
            retries=$((retries - 1))
            sleep 1
        done

        if [ $retries -eq 0 ]; then
            echo "WARNING: Health check timed out, proceeding anyway..."
        fi

        run_scenarios "http://localhost:${port}" "$results_dir"

        cleanup_native
        trap - EXIT
    else
        local compose_file="$BENCH_DIR/docker-compose.${stack}.yml"
        if [ ! -f "$compose_file" ]; then
            echo "ERROR: $compose_file not found"
            return 1
        fi

        echo "Starting $stack..."
        docker compose -f "$compose_file" up -d --build

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

        run_scenarios "http://localhost:${port}" "$results_dir"

        echo "Stopping $stack..."
        docker compose -f "$compose_file" down
    fi

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
