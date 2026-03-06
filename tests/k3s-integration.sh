#!/usr/bin/env bash
# k3s integration test for barkeeper.
#
# Starts barkeeper as the etcd backend, boots k3s against it, deploys
# real workloads (pod, deployment, service), and verifies they work.
#
# Prerequisites: sudo access, curl, no existing k3s or barkeeper on
# ports 2379/6443.
#
# Usage: sudo bash tests/k3s-integration.sh

set -euo pipefail

# ── Configuration ───────────────────────────────────────────────────────────

BARKEEPER_BIN="./target/release/barkeeper"
BARKEEPER_PORT=2379
BARKEEPER_DATA_DIR=$(mktemp -d /tmp/barkeeper-test-XXXXXX)
BARKEEPER_LOG="${BARKEEPER_DATA_DIR}/barkeeper.log"
K3S_KUBECONFIG="/tmp/barkeeper-k3s.kubeconfig"
K3S_DATA_DIR=$(mktemp -d /tmp/k3s-test-XXXXXX)
TIMEOUT=120  # seconds to wait for each condition

BARKEEPER_PID=""
K3S_PID=""

# ── Colors ──────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; }
pass()  { echo -e "${GREEN}[PASS]${NC} $*"; }

# ── Cleanup ─────────────────────────────────────────────────────────────────

cleanup() {
    info "Cleaning up..."

    if [[ -n "$K3S_PID" ]] && kill -0 "$K3S_PID" 2>/dev/null; then
        info "Stopping k3s (PID $K3S_PID)"
        kill "$K3S_PID" 2>/dev/null || true
        wait "$K3S_PID" 2>/dev/null || true
    fi

    # k3s may leave behind a killall script
    if command -v k3s-killall.sh &>/dev/null; then
        k3s-killall.sh 2>/dev/null || true
    fi

    if [[ -n "$BARKEEPER_PID" ]] && kill -0 "$BARKEEPER_PID" 2>/dev/null; then
        info "Stopping barkeeper (PID $BARKEEPER_PID)"
        kill "$BARKEEPER_PID" 2>/dev/null || true
        wait "$BARKEEPER_PID" 2>/dev/null || true
    fi

    rm -rf "$BARKEEPER_DATA_DIR" "$K3S_DATA_DIR" "$K3S_KUBECONFIG" 2>/dev/null || true
    info "Cleanup complete."
}
trap cleanup EXIT

# ── Helpers ─────────────────────────────────────────────────────────────────

wait_for_port() {
    local port=$1
    local name=$2
    local deadline=$((SECONDS + TIMEOUT))
    info "Waiting for $name on port $port..."
    while ! ss -tlnp | grep -q ":${port} "; do
        if (( SECONDS > deadline )); then
            fail "$name did not start within ${TIMEOUT}s"
            return 1
        fi
        sleep 1
    done
    pass "$name is listening on port $port"
}

kc() {
    kubectl --kubeconfig="$K3S_KUBECONFIG" "$@"
}

dump_diagnostics() {
    fail "Test failed — dumping diagnostics"
    echo ""
    echo "=== barkeeper log (last 50 lines) ==="
    tail -50 "$BARKEEPER_LOG" 2>/dev/null || echo "(no log)"
    echo ""
    echo "=== k3s journal (last 50 lines) ==="
    journalctl -u k3s --no-pager -n 50 2>/dev/null || echo "(no journal)"
    echo ""
    echo "=== kubectl get events ==="
    kc get events --all-namespaces --sort-by='.lastTimestamp' 2>/dev/null | tail -30 || true
    echo ""
    echo "=== kubectl get all ==="
    kc get all --all-namespaces 2>/dev/null || true
    echo ""
    echo "=== kubectl describe failed pods ==="
    kc get pods --all-namespaces --field-selector=status.phase!=Running,status.phase!=Succeeded -o name 2>/dev/null | \
        while read -r pod; do
            echo "--- $pod ---"
            kc describe "$pod" --all-namespaces 2>/dev/null || true
        done
}

# ── Preflight ───────────────────────────────────────────────────────────────

info "=== k3s Integration Test for Barkeeper ==="
echo ""

# Check we're running as root (k3s needs it for kubelet).
if [[ $EUID -ne 0 ]]; then
    fail "This script must be run as root (k3s needs kubelet access)."
    fail "Usage: sudo bash tests/k3s-integration.sh"
    exit 1
fi

# Check barkeeper binary exists.
if [[ ! -x "$BARKEEPER_BIN" ]]; then
    info "Building barkeeper (release)..."
    cargo build --release 2>&1 | tail -5
    if [[ ! -x "$BARKEEPER_BIN" ]]; then
        fail "Failed to build barkeeper"
        exit 1
    fi
fi

# Install k3s if not present.
if ! command -v k3s &>/dev/null; then
    info "Installing k3s..."
    curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_START=true INSTALL_K3S_SKIP_ENABLE=true sh -
    if ! command -v k3s &>/dev/null; then
        fail "Failed to install k3s"
        exit 1
    fi
    pass "k3s installed"
fi

# Check ports are free.
for port in $BARKEEPER_PORT 6443; do
    if ss -tlnp | grep -q ":${port} "; then
        fail "Port $port is already in use"
        exit 1
    fi
done

# ── Step 1: Start Barkeeper ────────────────────────────────────────────────

info "Starting barkeeper on port $BARKEEPER_PORT..."
"$BARKEEPER_BIN" \
    --listen-client-urls "127.0.0.1:${BARKEEPER_PORT}" \
    --data-dir "$BARKEEPER_DATA_DIR" \
    --node-id 1 \
    > "$BARKEEPER_LOG" 2>&1 &
BARKEEPER_PID=$!
info "barkeeper PID: $BARKEEPER_PID"

wait_for_port "$BARKEEPER_PORT" "barkeeper"

# Quick health check — etcd /health equivalent.
sleep 1
if ! curl -sf "http://127.0.0.1:$((BARKEEPER_PORT+1))/health" -o /dev/null 2>/dev/null; then
    # Try the gRPC port with a simple range request via the HTTP gateway.
    if ! curl -sf "http://127.0.0.1:$((BARKEEPER_PORT+1))/v3/kv/range" \
        -d '{"key":"L2hlYWx0aA=="}' -o /dev/null 2>/dev/null; then
        warn "Health check inconclusive, continuing anyway..."
    fi
fi
pass "barkeeper is running"

# ── Step 2: Start k3s ──────────────────────────────────────────────────────

info "Starting k3s with barkeeper as datastore..."
k3s server \
    --datastore-endpoint="http://127.0.0.1:${BARKEEPER_PORT}" \
    --data-dir "$K3S_DATA_DIR" \
    --disable=traefik,servicelb,metrics-server \
    --write-kubeconfig-mode=644 \
    --write-kubeconfig="$K3S_KUBECONFIG" \
    > "${BARKEEPER_DATA_DIR}/k3s.log" 2>&1 &
K3S_PID=$!
info "k3s PID: $K3S_PID"

# Wait for kubeconfig to appear.
deadline=$((SECONDS + TIMEOUT))
while [[ ! -f "$K3S_KUBECONFIG" ]]; do
    if (( SECONDS > deadline )); then
        fail "k3s did not create kubeconfig within ${TIMEOUT}s"
        dump_diagnostics
        exit 1
    fi
    if ! kill -0 "$K3S_PID" 2>/dev/null; then
        fail "k3s process died"
        dump_diagnostics
        exit 1
    fi
    sleep 1
done
pass "k3s kubeconfig created"

# Wait for the node to become Ready.
info "Waiting for node to become Ready..."
deadline=$((SECONDS + TIMEOUT))
while true; do
    if kc get nodes 2>/dev/null | grep -q " Ready"; then
        break
    fi
    if (( SECONDS > deadline )); then
        fail "Node did not become Ready within ${TIMEOUT}s"
        dump_diagnostics
        exit 1
    fi
    if ! kill -0 "$K3S_PID" 2>/dev/null; then
        fail "k3s process died while waiting for node Ready"
        dump_diagnostics
        exit 1
    fi
    sleep 2
done
pass "Node is Ready"

# ── Step 3: Test Workloads ──────────────────────────────────────────────────

echo ""
info "=== Deploying Test Workloads ==="
echo ""

# Wait for the default service account to be created by controller-manager.
info "Waiting for default service account..."
deadline=$((SECONDS + TIMEOUT))
while ! kc get serviceaccount default -n default &>/dev/null; do
    if (( SECONDS > deadline )); then
        fail "Default service account not created within ${TIMEOUT}s"
        dump_diagnostics
        exit 1
    fi
    sleep 2
done
pass "Default service account exists"

# --- Test 1: Simple Pod ---
info "[1/3] Creating nginx pod..."
kc run nginx-test --image=nginx:alpine --restart=Never
if kc wait --for=condition=Ready pod/nginx-test --timeout="${TIMEOUT}s"; then
    pass "[1/3] Pod nginx-test is Ready"
else
    fail "[1/3] Pod nginx-test did not become Ready"
    kc describe pod/nginx-test
    dump_diagnostics
    exit 1
fi

# --- Test 2: Deployment with 3 replicas ---
info "[2/3] Creating deployment (3 replicas)..."
kc create deployment nginx-deploy --image=nginx:alpine --replicas=3
if kc wait --for=condition=Available deployment/nginx-deploy --timeout="${TIMEOUT}s"; then
    pass "[2/3] Deployment nginx-deploy is Available"
else
    fail "[2/3] Deployment nginx-deploy did not become Available"
    kc describe deployment/nginx-deploy
    kc get pods -l app=nginx-deploy
    dump_diagnostics
    exit 1
fi

# Verify all 3 replicas are running.
ready_replicas=$(kc get deployment nginx-deploy -o jsonpath='{.status.readyReplicas}')
if [[ "$ready_replicas" == "3" ]]; then
    pass "[2/3] All 3 replicas are ready"
else
    fail "[2/3] Expected 3 ready replicas, got $ready_replicas"
    dump_diagnostics
    exit 1
fi

# --- Test 3: Service + endpoint resolution ---
info "[3/3] Creating service for deployment..."
kc expose deployment nginx-deploy --port=80 --name=nginx-svc
sleep 3  # Give endpoints controller time to populate

endpoints=$(kc get endpoints nginx-svc -o jsonpath='{.subsets[0].addresses}' 2>/dev/null)
if [[ -n "$endpoints" && "$endpoints" != "null" ]]; then
    pass "[3/3] Service nginx-svc has endpoints"
else
    fail "[3/3] Service nginx-svc has no endpoints"
    kc describe endpoints nginx-svc
    kc describe service nginx-svc
    dump_diagnostics
    exit 1
fi

# ── Results ─────────────────────────────────────────────────────────────────

echo ""
echo "========================================"
pass "All k3s integration tests passed!"
echo "========================================"
echo ""
info "Workloads verified:"
info "  - Pod (nginx-test): Ready"
info "  - Deployment (nginx-deploy, 3 replicas): Available"
info "  - Service (nginx-svc): Endpoints populated"
echo ""
