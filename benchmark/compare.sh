#!/usr/bin/env bash
# Compare etcd and barkeeper benchmark results
set -uo pipefail

ETCD_DIR="$(dirname "$0")/results/etcd"
BK_DIR="$(dirname "$0")/results/barkeeper"

pass=0
fail=0
cosmetic=0

compare() {
    local name="$1"
    local desc="$2"
    local etcd_file="$ETCD_DIR/$name"
    local bk_file="$BK_DIR/$name"

    if [[ ! -f "$etcd_file" ]]; then
        echo "SKIP: $name — etcd result missing"
        return
    fi
    if [[ ! -f "$bk_file" ]]; then
        echo "FAIL: $name — barkeeper result missing"
        ((fail++))
        return
    fi

    if diff -q "$etcd_file" "$bk_file" &>/dev/null; then
        echo "PASS: $name ($desc)"
        ((pass++))
    else
        echo "DIFF: $name ($desc)"
        diff --unified "$etcd_file" "$bk_file" | head -20
        echo ""
        ((fail++))
    fi
}

compare_normalized() {
    local name="$1"
    local desc="$2"
    local etcd_file="$ETCD_DIR/$name"
    local bk_file="$BK_DIR/$name"

    if [[ ! -f "$etcd_file" || ! -f "$bk_file" ]]; then
        echo "SKIP: $name — file missing"
        return
    fi

    # Normalize: remove cluster_id, member_id, revision, raft_term, lease IDs
    # (these are instance-specific, not behavioral differences)
    normalize() {
        sed -E \
            -e 's/"cluster_id": "[0-9]+"/"cluster_id": "CLUSTER"/g' \
            -e 's/"member_id": "[0-9]+"/"member_id": "MEMBER"/g' \
            -e 's/"revision": "[0-9]+"/"revision": "REV"/g' \
            -e 's/"raft_term": "[0-9]+"/"raft_term": "TERM"/g' \
            -e 's/"ID": "[0-9]+"/"ID": "LEASE_ID"/g' \
            -e 's/"create_revision": "[0-9]+"/"create_revision": "REV"/g' \
            -e 's/"mod_revision": "[0-9]+"/"mod_revision": "REV"/g' \
            "$1"
    }

    if diff -q <(normalize "$etcd_file") <(normalize "$bk_file") &>/dev/null; then
        echo "PASS: $name ($desc) [normalized]"
        ((pass++))
    else
        echo "DIFF: $name ($desc) [normalized]"
        diff --unified <(normalize "$etcd_file") <(normalize "$bk_file") | head -20
        echo ""
        ((fail++))
    fi
}

echo "================================================================"
echo "  etcd vs barkeeper behavioral comparison"
echo "================================================================"
echo ""

echo "=== KV Operations (gRPC via etcdctl) ==="
compare "kv_put_basic.txt" "Put basic"
compare "kv_get_basic.txt" "Get basic"
compare "kv_get_value_only.txt" "Get value only"
compare "kv_get_nonexistent.txt" "Get nonexistent"
compare "kv_put_overwrite.txt" "Put overwrite"
compare "kv_put_overwrite_verify.txt" "Put overwrite verify"
compare "kv_range_prefix.txt" "Range prefix"
compare "kv_range_limit.txt" "Range limit"
compare "kv_delete_single.txt" "Delete single"
compare "kv_delete_single_verify.txt" "Delete single verify"
compare "kv_delete_prefix.txt" "Delete prefix"
compare "kv_delete_prefix_verify.txt" "Delete prefix verify"
compare "kv_delete_nonexistent.txt" "Delete nonexistent"
compare "kv_put_lease.txt" "Put with lease"
echo ""

echo "=== Txn Operations (gRPC via etcdctl) ==="
compare "txn_success.txt" "Txn success"
compare "txn_success_verify.txt" "Txn success verify"
compare "txn_failure.txt" "Txn failure"
compare "txn_failure_verify.txt" "Txn failure verify"
echo ""

echo "=== HTTP API (normalized — ignoring instance-specific IDs) ==="
compare_normalized "http_put.txt" "HTTP Put"
compare_normalized "http_get.txt" "HTTP Get"
compare_normalized "http_delete.txt" "HTTP Delete"
compare_normalized "http_range_empty.txt" "HTTP Range empty"
compare_normalized "http_put_prev_kv.txt" "HTTP Put prev_kv"
compare_normalized "http_range_prefix.txt" "HTTP Range prefix"
compare_normalized "http_txn.txt" "HTTP Txn"
compare_normalized "response_headers.txt" "Response headers"
compare_normalized "http_lease_grant.txt" "HTTP Lease grant"
compare_normalized "http_lease_list.txt" "HTTP Lease list"
compare_normalized "http_member_list.txt" "HTTP Member list"
compare_normalized "http_maintenance_status.txt" "HTTP Maintenance status"
echo ""

echo "================================================================"
echo "  Results: $pass passed, $fail differences"
echo "================================================================"
