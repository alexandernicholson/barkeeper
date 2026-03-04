#!/usr/bin/env bash
# barkeeper performance benchmark — throughput, latency, and mixed workload tests
# Uses the HTTP/JSON gateway (port = grpc_port + 1) with curl.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
RESULTS_FILE="$RESULTS_DIR/perf_results.txt"
DATA_DIR=$(mktemp -d)
BK_PID=""

# Random port base to avoid collisions
BASE_PORT=$((RANDOM + 10000))
GRPC_PORT=$BASE_PORT
HTTP_PORT=$((BASE_PORT + 1))
HTTP="http://127.0.0.1:$HTTP_PORT"

cleanup() {
    if [ -n "$BK_PID" ] && kill -0 "$BK_PID" 2>/dev/null; then
        kill "$BK_PID" 2>/dev/null || true
        wait "$BK_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# ── Helpers ───────────────────────────────────────────────────────────────

b64() {
    echo -n "$1" | base64 -w0
}

# Nanosecond timestamp
now_ns() {
    date +%s%N
}

# Duration in milliseconds from two nanosecond timestamps
duration_ms() {
    local start_ns=$1 end_ns=$2
    echo "scale=3; ($end_ns - $start_ns) / 1000000" | bc
}

# Duration in seconds from two nanosecond timestamps
duration_s() {
    local start_ns=$1 end_ns=$2
    echo "scale=6; ($end_ns - $start_ns) / 1000000000" | bc
}

# Ops per second from count and nanosecond duration
ops_per_sec() {
    local count=$1 start_ns=$2 end_ns=$3
    local elapsed_s
    elapsed_s=$(duration_s "$start_ns" "$end_ns")
    if [ "$(echo "$elapsed_s > 0" | bc)" -eq 1 ]; then
        echo "scale=1; $count / $elapsed_s" | bc
    else
        echo "N/A"
    fi
}

# Calculate percentile from a sorted file of nanosecond values
# Usage: percentile <file> <p> (p = 50, 95, 99, etc.)
percentile() {
    local file=$1 p=$2
    local count
    count=$(wc -l < "$file")
    if [ "$count" -eq 0 ]; then
        echo "0"
        return
    fi
    local index
    index=$(echo "scale=0; ($p * $count + 99) / 100" | bc)
    if [ "$index" -lt 1 ]; then index=1; fi
    if [ "$index" -gt "$count" ]; then index=$count; fi
    awk "NR==$index {print}" "$file"
}

# ── Build ─────────────────────────────────────────────────────────────────

echo "=========================================="
echo "  barkeeper performance benchmark"
echo "=========================================="
echo ""
echo "Building barkeeper in release mode..."
(cd "$PROJECT_DIR" && cargo build --release 2>&1 | tail -1)
BARKEEPER="$PROJECT_DIR/target/release/barkeeper"

if [ ! -f "$BARKEEPER" ]; then
    echo "ERROR: Release binary not found at $BARKEEPER"
    exit 1
fi

echo "Binary: $BARKEEPER"
echo ""

# ── Start barkeeper ───────────────────────────────────────────────────────

echo "Starting barkeeper on gRPC=$GRPC_PORT HTTP=$HTTP_PORT ..."
$BARKEEPER \
    --name perf-bench \
    --data-dir "$DATA_DIR" \
    --listen-client-urls "127.0.0.1:$GRPC_PORT" \
    --node-id 1 \
    &>/dev/null &
BK_PID=$!

# Wait for the HTTP gateway to be ready (up to 10 seconds)
echo -n "Waiting for HTTP gateway"
for i in $(seq 1 40); do
    if curl -s -o /dev/null -w '' "$HTTP/v3/maintenance/status" -d '{}' 2>/dev/null; then
        echo " ready!"
        break
    fi
    if [ "$i" -eq 40 ]; then
        echo " TIMEOUT"
        echo "ERROR: barkeeper failed to start within 10 seconds"
        exit 1
    fi
    echo -n "."
    sleep 0.25
done

# Verify process is running
if ! kill -0 "$BK_PID" 2>/dev/null; then
    echo "ERROR: barkeeper process died during startup"
    exit 1
fi

echo ""

# ── Prepare results ───────────────────────────────────────────────────────

mkdir -p "$RESULTS_DIR"
: > "$RESULTS_FILE"

log() {
    echo "$1" | tee -a "$RESULTS_FILE"
}

log "=========================================="
log "  barkeeper performance results"
log "  $(date '+%Y-%m-%d %H:%M:%S')"
log "=========================================="
log ""

# ── 1. Sequential Write Throughput ────────────────────────────────────────

log "--- Sequential Write Throughput (1000 PUTs) ---"

NUM_WRITES=1000
start=$(now_ns)
for i in $(seq 1 $NUM_WRITES); do
    key=$(b64 "bench/write/$i")
    val=$(b64 "value-$i")
    curl -s -o /dev/null "$HTTP/v3/kv/put" -d "{\"key\":\"$key\",\"value\":\"$val\"}"
done
end=$(now_ns)

write_elapsed_ms=$(duration_ms "$start" "$end")
write_ops=$(ops_per_sec $NUM_WRITES "$start" "$end")

log "  Operations:  $NUM_WRITES"
log "  Elapsed:     ${write_elapsed_ms} ms"
log "  Throughput:  ${write_ops} ops/sec"
log ""

# ── 2. Sequential Read Throughput ─────────────────────────────────────────

log "--- Sequential Read Throughput (1000 GETs) ---"

NUM_READS=1000
start=$(now_ns)
for i in $(seq 1 $NUM_READS); do
    key=$(b64 "bench/write/$i")
    curl -s -o /dev/null "$HTTP/v3/kv/range" -d "{\"key\":\"$key\"}"
done
end=$(now_ns)

read_elapsed_ms=$(duration_ms "$start" "$end")
read_ops=$(ops_per_sec $NUM_READS "$start" "$end")

log "  Operations:  $NUM_READS"
log "  Elapsed:     ${read_elapsed_ms} ms"
log "  Throughput:  ${read_ops} ops/sec"
log ""

# ── 3. Mixed Workload (70% reads / 30% writes, 5 seconds) ────────────────

log "--- Mixed Workload (70/30 read/write, 5 seconds) ---"

MIXED_DURATION_NS=$((5 * 1000000000))
mixed_reads=0
mixed_writes=0
mixed_ops=0
mix_counter=0

start=$(now_ns)
while true; do
    now=$(now_ns)
    elapsed=$((now - start))
    if [ "$elapsed" -ge "$MIXED_DURATION_NS" ]; then
        break
    fi

    mix_counter=$((mix_counter + 1))

    # 70% reads, 30% writes based on counter mod 10
    mod=$((mix_counter % 10))
    if [ "$mod" -lt 7 ]; then
        # Read a random key from the ones we wrote
        idx=$(( (RANDOM % NUM_WRITES) + 1 ))
        key=$(b64 "bench/write/$idx")
        curl -s -o /dev/null "$HTTP/v3/kv/range" -d "{\"key\":\"$key\"}"
        mixed_reads=$((mixed_reads + 1))
    else
        # Write a new key
        key=$(b64 "bench/mixed/$mix_counter")
        val=$(b64 "mixed-value-$mix_counter")
        curl -s -o /dev/null "$HTTP/v3/kv/put" -d "{\"key\":\"$key\",\"value\":\"$val\"}"
        mixed_writes=$((mixed_writes + 1))
    fi
    mixed_ops=$((mixed_ops + 1))
done
end=$(now_ns)

mixed_elapsed_ms=$(duration_ms "$start" "$end")
mixed_total_ops=$(ops_per_sec $mixed_ops "$start" "$end")

log "  Duration:    ${mixed_elapsed_ms} ms"
log "  Total ops:   $mixed_ops"
log "  Reads:       $mixed_reads"
log "  Writes:      $mixed_writes"
log "  Throughput:  ${mixed_total_ops} ops/sec"
log ""

# ── 4. Range Scan (100 keys) ─────────────────────────────────────────────

log "--- Range Scan (100 keys) ---"

# Pre-populate 100 keys with a known prefix for range scan
for i in $(seq 1 100); do
    key=$(printf "range/%04d" "$i")
    key_b64=$(b64 "$key")
    val_b64=$(b64 "range-val-$i")
    curl -s -o /dev/null "$HTTP/v3/kv/put" -d "{\"key\":\"$key_b64\",\"value\":\"$val_b64\"}"
done

# Range query: key="range/" range_end="range0" (prefix scan)
range_key=$(b64 "range/")
range_end=$(b64 "range0")

# Run 10 range scans and report average latency
RANGE_ITERS=10
range_total_ns=0
for i in $(seq 1 $RANGE_ITERS); do
    s=$(now_ns)
    result=$(curl -s "$HTTP/v3/kv/range" -d "{\"key\":\"$range_key\",\"range_end\":\"$range_end\",\"limit\":\"100\"}")
    e=$(now_ns)
    range_total_ns=$((range_total_ns + (e - s)))
done

range_avg_ms=$(echo "scale=3; $range_total_ns / $RANGE_ITERS / 1000000" | bc)
# Count how many keys were returned in the last result
range_count=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('kvs', [])))" 2>/dev/null || echo "?")

log "  Keys returned:    $range_count"
log "  Iterations:       $RANGE_ITERS"
log "  Avg latency:      ${range_avg_ms} ms"
log ""

# ── 5. Transaction Throughput (500 compare-and-swap) ──────────────────────

log "--- Transaction Throughput (500 CAS) ---"

# Seed the txn key
txn_key_b64=$(b64 "bench/txn/cas")
txn_val_b64=$(b64 "initial")
curl -s -o /dev/null "$HTTP/v3/kv/put" -d "{\"key\":\"$txn_key_b64\",\"value\":\"$txn_val_b64\"}"

NUM_TXNS=500
txn_succeeded=0
start=$(now_ns)
for i in $(seq 1 $NUM_TXNS); do
    # Each txn: compare value == current, then set to new value
    # We read current value first to build a valid CAS
    current=$(curl -s "$HTTP/v3/kv/range" -d "{\"key\":\"$txn_key_b64\"}" | python3 -c "import sys,json; d=json.load(sys.stdin); kvs=d.get('kvs',[]); print(kvs[0]['value'] if kvs else '')" 2>/dev/null || echo "")

    if [ -n "$current" ]; then
        new_val=$(b64 "txn-val-$i")
        resp=$(curl -s "$HTTP/v3/kv/txn" -d "{
            \"compare\":[{
                \"key\":\"$txn_key_b64\",
                \"target\":\"VALUE\",
                \"result\":\"EQUAL\",
                \"value\":\"$current\"
            }],
            \"success\":[{\"requestPut\":{\"key\":\"$txn_key_b64\",\"value\":\"$new_val\"}}],
            \"failure\":[]
        }")
        succeeded=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('succeeded', False))" 2>/dev/null || echo "")
        if [ "$succeeded" = "True" ]; then
            txn_succeeded=$((txn_succeeded + 1))
        fi
    fi
done
end=$(now_ns)

txn_elapsed_ms=$(duration_ms "$start" "$end")
txn_ops=$(ops_per_sec $NUM_TXNS "$start" "$end")

log "  Operations:  $NUM_TXNS"
log "  Succeeded:   $txn_succeeded"
log "  Elapsed:     ${txn_elapsed_ms} ms"
log "  Throughput:  ${txn_ops} ops/sec"
log ""

# ── 6. Latency Percentiles ───────────────────────────────────────────────

log "--- Latency Percentiles (100 ops each) ---"

LATENCY_OPS=100
PUT_LATENCIES=$(mktemp)
GET_LATENCIES=$(mktemp)

# PUT latencies
for i in $(seq 1 $LATENCY_OPS); do
    key=$(b64 "bench/latency/put/$i")
    val=$(b64 "lat-val-$i")
    s=$(now_ns)
    curl -s -o /dev/null "$HTTP/v3/kv/put" -d "{\"key\":\"$key\",\"value\":\"$val\"}"
    e=$(now_ns)
    echo $((e - s)) >> "$PUT_LATENCIES"
done

# GET latencies
for i in $(seq 1 $LATENCY_OPS); do
    key=$(b64 "bench/latency/put/$i")
    s=$(now_ns)
    curl -s -o /dev/null "$HTTP/v3/kv/range" -d "{\"key\":\"$key\"}"
    e=$(now_ns)
    echo $((e - s)) >> "$GET_LATENCIES"
done

# Sort latencies for percentile calculation
sort -n "$PUT_LATENCIES" -o "$PUT_LATENCIES"
sort -n "$GET_LATENCIES" -o "$GET_LATENCIES"

put_p50_ns=$(percentile "$PUT_LATENCIES" 50)
put_p95_ns=$(percentile "$PUT_LATENCIES" 95)
put_p99_ns=$(percentile "$PUT_LATENCIES" 99)

get_p50_ns=$(percentile "$GET_LATENCIES" 50)
get_p95_ns=$(percentile "$GET_LATENCIES" 95)
get_p99_ns=$(percentile "$GET_LATENCIES" 99)

# Convert ns to ms for display
fmt_ms() {
    echo "scale=3; $1 / 1000000" | bc
}

log "  PUT latency (ms):"
log "    p50:  $(fmt_ms "$put_p50_ns")"
log "    p95:  $(fmt_ms "$put_p95_ns")"
log "    p99:  $(fmt_ms "$put_p99_ns")"
log ""
log "  GET latency (ms):"
log "    p50:  $(fmt_ms "$get_p50_ns")"
log "    p95:  $(fmt_ms "$get_p95_ns")"
log "    p99:  $(fmt_ms "$get_p99_ns")"
log ""

# Clean up temp files
rm -f "$PUT_LATENCIES" "$GET_LATENCIES"

# ── Summary Table ─────────────────────────────────────────────────────────

log "=========================================="
log "  Summary"
log "=========================================="
log ""
printf "  %-30s %15s %15s\n" "Test" "Value" "Unit" | tee -a "$RESULTS_FILE"
printf "  %-30s %15s %15s\n" "------------------------------" "---------------" "---------------" | tee -a "$RESULTS_FILE"
printf "  %-30s %15s %15s\n" "Sequential PUT (1000)" "$write_ops" "ops/sec" | tee -a "$RESULTS_FILE"
printf "  %-30s %15s %15s\n" "Sequential GET (1000)" "$read_ops" "ops/sec" | tee -a "$RESULTS_FILE"
printf "  %-30s %15s %15s\n" "Mixed 70/30 (5s)" "$mixed_total_ops" "ops/sec" | tee -a "$RESULTS_FILE"
printf "  %-30s %15s %15s\n" "Range scan (100 keys)" "$range_avg_ms" "ms avg" | tee -a "$RESULTS_FILE"
printf "  %-30s %15s %15s\n" "CAS txn (500)" "$txn_ops" "ops/sec" | tee -a "$RESULTS_FILE"
printf "  %-30s %15s %15s\n" "PUT p50 / p95 / p99" "$(fmt_ms "$put_p50_ns") / $(fmt_ms "$put_p95_ns") / $(fmt_ms "$put_p99_ns")" "ms" | tee -a "$RESULTS_FILE"
printf "  %-30s %15s %15s\n" "GET p50 / p95 / p99" "$(fmt_ms "$get_p50_ns") / $(fmt_ms "$get_p95_ns") / $(fmt_ms "$get_p99_ns")" "ms" | tee -a "$RESULTS_FILE"
log ""
log "Results saved to: $RESULTS_FILE"
log ""
