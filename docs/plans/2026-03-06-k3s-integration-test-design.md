# k3s Integration Test Design

## Goal

Run real Kubernetes workloads (pods, deployments, services) against barkeeper as the backing etcd store, using k3s with `--datastore-endpoint`.

## Architecture

A shell script (`tests/k3s-integration.sh`) that:

1. Builds barkeeper in release mode
2. Starts barkeeper on localhost:2379
3. Starts k3s with `--datastore-endpoint=http://127.0.0.1:2379`
4. Waits for the cluster to become Ready
5. Deploys test workloads and verifies they reach Ready/Available
6. Captures diagnostic output on failure
7. Tears down everything (barkeeper + k3s)

## k3s Configuration

```bash
k3s server \
  --datastore-endpoint="http://127.0.0.1:2379" \
  --disable=traefik,servicelb,metrics-server \
  --write-kubeconfig-mode=644 \
  --write-kubeconfig=/tmp/barkeeper-k3s.kubeconfig
```

Disabling traefik/servicelb/metrics-server reduces noise — we only care about core K8s (apiserver, scheduler, controller-manager, kubelet, coredns).

## Test Workloads

Three levels of verification:

1. **Pod**: `kubectl run nginx --image=nginx` — proves basic object storage and watch
2. **Deployment** (3 replicas): proves controller-manager's watch + Txn loop (create, scale)
3. **Service + endpoint resolution**: proves endpoints controller watches pods and writes endpoint slices

Each workload gets `kubectl wait --for=condition=...` with a timeout.

## Failure Handling

On failure:
- Capture barkeeper stderr to a log file
- Dump k3s logs via `journalctl -u k3s`
- Dump `kubectl get events --all-namespaces`
- Dump `kubectl describe` of failed resources
- Script exits non-zero

## Prerequisites

- k3s binary installed (or auto-downloaded)
- sudo access (k3s needs root for kubelet)
- No existing k3s or barkeeper on ports 2379/6443

## Cleanup

Trap-based cleanup ensures both processes are killed on exit, success or failure. k3s data is written to a temp directory and removed.
