#!/usr/bin/env bash
# barkeeper behavioral benchmark — captures output for comparison with etcd
set -euo pipefail

OUTDIR="$(dirname "$0")/results/barkeeper"
mkdir -p "$OUTDIR"
DATA_DIR=$(mktemp -d)
LISTEN_PORT=23791
HTTP_PORT=23792  # barkeeper uses grpc_port + 1

cleanup() {
    kill $BK_PID 2>/dev/null || true
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

BARKEEPER="$(dirname "$0")/../target/release/barkeeper"

echo "=== Starting barkeeper on port $LISTEN_PORT ==="
$BARKEEPER \
    --name test-barkeeper \
    --data-dir "$DATA_DIR" \
    --listen-client-urls "127.0.0.1:$LISTEN_PORT" \
    --node-id 1 \
    &>/dev/null &
BK_PID=$!
sleep 2

# Check if barkeeper is still running
if ! kill -0 $BK_PID 2>/dev/null; then
    echo "ERROR: barkeeper failed to start"
    exit 1
fi

EP="--endpoints=http://127.0.0.1:$LISTEN_PORT"
HTTP="http://127.0.0.1:$HTTP_PORT"

run_test() {
    local name="$1"
    local desc="$2"
    shift 2
    echo "--- TEST: $name ($desc)"
}

# ============================================================
# 1. KV PUT — basic put
# ============================================================
run_test "kv_put_basic" "Basic put returns OK"
etcdctl $EP put foo bar 2>&1 | tee "$OUTDIR/kv_put_basic.txt"

# ============================================================
# 2. KV GET — basic get
# ============================================================
run_test "kv_get_basic" "Basic get returns value"
etcdctl $EP get foo 2>&1 | tee "$OUTDIR/kv_get_basic.txt"

# ============================================================
# 3. KV GET — get with --print-value-only
# ============================================================
run_test "kv_get_value_only" "Get value only"
etcdctl $EP get foo --print-value-only 2>&1 | tee "$OUTDIR/kv_get_value_only.txt"

# ============================================================
# 4. KV GET — nonexistent key
# ============================================================
run_test "kv_get_nonexistent" "Get nonexistent key"
etcdctl $EP get nonexistent 2>&1 | tee "$OUTDIR/kv_get_nonexistent.txt"

# ============================================================
# 5. KV PUT — overwrite
# ============================================================
run_test "kv_put_overwrite" "Overwrite existing key"
etcdctl $EP put foo bar2 2>&1 | tee "$OUTDIR/kv_put_overwrite.txt"
etcdctl $EP get foo --print-value-only 2>&1 | tee "$OUTDIR/kv_put_overwrite_verify.txt"

# ============================================================
# 6. KV GET — range with prefix
# ============================================================
run_test "kv_range_prefix" "Range with prefix"
etcdctl $EP put myprefix/key1 val1 2>&1 >/dev/null
etcdctl $EP put myprefix/key2 val2 2>&1 >/dev/null
etcdctl $EP put myprefix/key3 val3 2>&1 >/dev/null
etcdctl $EP put other/key1 other1 2>&1 >/dev/null
etcdctl $EP get myprefix/ --prefix 2>&1 | tee "$OUTDIR/kv_range_prefix.txt"

# ============================================================
# 7. KV GET — range with limit
# ============================================================
run_test "kv_range_limit" "Range with limit"
etcdctl $EP get myprefix/ --prefix --limit=2 2>&1 | tee "$OUTDIR/kv_range_limit.txt"

# ============================================================
# 8. KV DELETE — single key
# ============================================================
run_test "kv_delete_single" "Delete single key"
etcdctl $EP del foo 2>&1 | tee "$OUTDIR/kv_delete_single.txt"
etcdctl $EP get foo 2>&1 | tee "$OUTDIR/kv_delete_single_verify.txt"

# ============================================================
# 9. KV DELETE — prefix
# ============================================================
run_test "kv_delete_prefix" "Delete with prefix"
etcdctl $EP del myprefix/ --prefix 2>&1 | tee "$OUTDIR/kv_delete_prefix.txt"
etcdctl $EP get myprefix/ --prefix 2>&1 | tee "$OUTDIR/kv_delete_prefix_verify.txt"

# ============================================================
# 10. KV DELETE — nonexistent key
# ============================================================
run_test "kv_delete_nonexistent" "Delete nonexistent key"
etcdctl $EP del nonexistent 2>&1 | tee "$OUTDIR/kv_delete_nonexistent.txt"

# ============================================================
# 11. KV PUT — with lease
# ============================================================
run_test "kv_put_lease" "Put with lease"
LEASE_OUT=$(etcdctl $EP lease grant 300 2>&1)
echo "$LEASE_OUT" | tee "$OUTDIR/kv_lease_grant.txt"
LEASE_ID=$(echo "$LEASE_OUT" | grep -oP 'lease \K[a-f0-9]+')
etcdctl $EP put leasekey leaseval --lease="$LEASE_ID" 2>&1 | tee "$OUTDIR/kv_put_lease.txt"

# ============================================================
# 12. HTTP API — PUT via grpc-gateway
# ============================================================
run_test "http_put" "HTTP PUT"
KEY_B64=$(echo -n "httpkey" | base64)
VAL_B64=$(echo -n "httpval" | base64)
curl -s -X POST "$HTTP/v3/kv/put" \
    -d "{\"key\":\"$KEY_B64\",\"value\":\"$VAL_B64\"}" 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_put.txt"

# ============================================================
# 13. HTTP API — GET via grpc-gateway
# ============================================================
run_test "http_get" "HTTP GET"
curl -s -X POST "$HTTP/v3/kv/range" \
    -d "{\"key\":\"$KEY_B64\"}" 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_get.txt"

# ============================================================
# 14. HTTP API — DELETE via grpc-gateway
# ============================================================
run_test "http_delete" "HTTP DELETE"
curl -s -X POST "$HTTP/v3/kv/deleterange" \
    -d "{\"key\":\"$KEY_B64\"}" 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_delete.txt"

# ============================================================
# 15. HTTP API — Range empty result
# ============================================================
run_test "http_range_empty" "HTTP Range empty"
curl -s -X POST "$HTTP/v3/kv/range" \
    -d "{\"key\":\"$(echo -n 'doesnotexist' | base64)\"}" 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_range_empty.txt"

# ============================================================
# 16. TXN — success path
# ============================================================
run_test "txn_success" "Txn success path"
etcdctl $EP put txnkey txnval 2>&1 >/dev/null
# txn: if value(txnkey) = txnval then put txnkey txnval_new else put txnkey txnval_fail
echo 'val("txnkey") = "txnval"' | etcdctl $EP txn --interactive <<EOF 2>&1 | tee "$OUTDIR/txn_success.txt"
val("txnkey") = "txnval"

put txnkey txnval_new

put txnkey txnval_fail

EOF
etcdctl $EP get txnkey --print-value-only 2>&1 | tee "$OUTDIR/txn_success_verify.txt"

# ============================================================
# 17. TXN — failure path
# ============================================================
run_test "txn_failure" "Txn failure path"
echo 'val("txnkey") = "wrong"' | etcdctl $EP txn --interactive <<EOF 2>&1 | tee "$OUTDIR/txn_failure.txt"
val("txnkey") = "wrong"

put txnkey should_not_be_this

put txnkey txnval_fallback

EOF
etcdctl $EP get txnkey --print-value-only 2>&1 | tee "$OUTDIR/txn_failure_verify.txt"

# ============================================================
# 18. LEASE — grant, timetolive, revoke
# ============================================================
run_test "lease_grant" "Lease grant"
LEASE2_OUT=$(etcdctl $EP lease grant 60 2>&1)
echo "$LEASE2_OUT" | tee "$OUTDIR/lease_grant.txt"
LEASE2_ID=$(echo "$LEASE2_OUT" | grep -oP 'lease \K[a-f0-9]+')

run_test "lease_timetolive" "Lease TTL"
etcdctl $EP lease timetolive "$LEASE2_ID" 2>&1 | tee "$OUTDIR/lease_timetolive.txt"

run_test "lease_revoke" "Lease revoke"
etcdctl $EP lease revoke "$LEASE2_ID" 2>&1 | tee "$OUTDIR/lease_revoke.txt"

run_test "lease_list" "Lease list"
etcdctl $EP lease list 2>&1 | tee "$OUTDIR/lease_list.txt"

# ============================================================
# 19. CLUSTER — member list
# ============================================================
run_test "member_list" "Member list"
etcdctl $EP member list 2>&1 | tee "$OUTDIR/member_list.txt"

# ============================================================
# 20. MAINTENANCE — status
# ============================================================
run_test "maintenance_status" "Maintenance status"
etcdctl $EP endpoint status --write-out=json 2>&1 | python3 -m json.tool | tee "$OUTDIR/maintenance_status.txt"

# ============================================================
# 21. HTTP API — Lease grant/list
# ============================================================
run_test "http_lease_grant" "HTTP Lease grant"
curl -s -X POST "$HTTP/v3/lease/grant" \
    -d '{"TTL":120}' 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_lease_grant.txt"

run_test "http_lease_list" "HTTP Lease list"
curl -s -X POST "$HTTP/v3/lease/leases" \
    -d '{}' 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_lease_list.txt"

# ============================================================
# 22. HTTP API — Cluster member list
# ============================================================
run_test "http_member_list" "HTTP Member list"
curl -s -X POST "$HTTP/v3/cluster/member/list" \
    -d '{}' 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_member_list.txt"

# ============================================================
# 23. HTTP API — Maintenance status
# ============================================================
run_test "http_maintenance_status" "HTTP Maintenance status"
curl -s -X POST "$HTTP/v3/maintenance/status" \
    -d '{}' 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_maintenance_status.txt"

# ============================================================
# 24. HTTP API — PUT with prev_kv
# ============================================================
run_test "http_put_prev_kv" "HTTP PUT with prev_kv"
curl -s -X POST "$HTTP/v3/kv/put" \
    -d "{\"key\":\"$(echo -n prevtest | base64)\",\"value\":\"$(echo -n val1 | base64)\"}" >/dev/null
curl -s -X POST "$HTTP/v3/kv/put" \
    -d "{\"key\":\"$(echo -n prevtest | base64)\",\"value\":\"$(echo -n val2 | base64)\",\"prev_kv\":true}" 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_put_prev_kv.txt"

# ============================================================
# 25. HTTP API — Range with range_end (prefix)
# ============================================================
run_test "http_range_prefix" "HTTP Range with prefix"
curl -s -X POST "$HTTP/v3/kv/put" \
    -d "{\"key\":\"$(echo -n prefix/a | base64)\",\"value\":\"$(echo -n 1 | base64)\"}" >/dev/null
curl -s -X POST "$HTTP/v3/kv/put" \
    -d "{\"key\":\"$(echo -n prefix/b | base64)\",\"value\":\"$(echo -n 2 | base64)\"}" >/dev/null
curl -s -X POST "$HTTP/v3/kv/put" \
    -d "{\"key\":\"$(echo -n prefix/c | base64)\",\"value\":\"$(echo -n 3 | base64)\"}" >/dev/null
# range_end = "prefix0" (prefix/ + 1 = prefix0)
curl -s -X POST "$HTTP/v3/kv/range" \
    -d "{\"key\":\"$(echo -n prefix/ | base64)\",\"range_end\":\"$(echo -n 'prefix0' | base64)\"}" 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_range_prefix.txt"

# ============================================================
# 26. HTTP API — Txn
# ============================================================
run_test "http_txn" "HTTP Txn"
curl -s -X POST "$HTTP/v3/kv/put" \
    -d "{\"key\":\"$(echo -n txnhttpkey | base64)\",\"value\":\"$(echo -n original | base64)\"}" >/dev/null
curl -s -X POST "$HTTP/v3/kv/txn" \
    -d "{
        \"compare\":[{
            \"key\":\"$(echo -n txnhttpkey | base64)\",
            \"target\":\"VALUE\",
            \"result\":\"EQUAL\",
            \"value\":\"$(echo -n original | base64)\"
        }],
        \"success\":[{\"requestPut\":{\"key\":\"$(echo -n txnhttpkey | base64)\",\"value\":\"$(echo -n updated | base64)\"}}],
        \"failure\":[{\"requestPut\":{\"key\":\"$(echo -n txnhttpkey | base64)\",\"value\":\"$(echo -n failed | base64)\"}}]
    }" 2>&1 | python3 -m json.tool | tee "$OUTDIR/http_txn.txt"

# ============================================================
# 27. Response header structure
# ============================================================
run_test "response_headers" "Response header fields"
curl -s -X POST "$HTTP/v3/kv/put" \
    -d "{\"key\":\"$(echo -n headertest | base64)\",\"value\":\"$(echo -n v | base64)\"}" 2>&1 | python3 -m json.tool | tee "$OUTDIR/response_headers.txt"

echo ""
echo "=== barkeeper benchmark complete. Results in $OUTDIR ==="
