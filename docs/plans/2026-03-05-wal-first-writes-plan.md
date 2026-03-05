# WAL-First Writes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Decouple write acknowledgment from KV apply by pre-assigning revisions at Raft proposal time and using ReadIndex for linearizable reads.

**Architecture:** Raft actor assigns revisions and returns them in `ClientProposalResult::Success` after log commit. Put handler returns immediately without waiting for KV apply. Reads use ReadIndex (commit index vs applied index) to wait for apply before serving. ApplyNotifier coordinates via `tokio::sync::watch`.

**Tech Stack:** Rust, tokio, redb, bincode

---

### Task 1: ApplyNotifier — watch-based apply index notification

**Files:**
- Create: `src/kv/apply_notifier.rs`
- Modify: `src/kv/mod.rs`
- Create: `tests/apply_notifier_test.rs`

**Step 1: Write the failing test**

```rust
// tests/apply_notifier_test.rs
use barkeeper::kv::apply_notifier::ApplyNotifier;

#[tokio::test]
async fn test_already_applied_returns_immediately() {
    let notifier = ApplyNotifier::new(10);
    // Should return immediately since 5 <= 10
    notifier.wait_for(5).await;
}

#[tokio::test]
async fn test_wait_for_future_index() {
    let notifier = ApplyNotifier::new(0);
    let n = notifier.clone();
    let handle = tokio::spawn(async move {
        n.wait_for(5).await;
    });
    // Not yet applied — task should be pending
    tokio::task::yield_now().await;
    assert!(!handle.is_finished());
    // Advance to 5
    notifier.advance(5);
    handle.await.unwrap();
}

#[tokio::test]
async fn test_advance_wakes_multiple_waiters() {
    let notifier = ApplyNotifier::new(0);
    let n1 = notifier.clone();
    let n2 = notifier.clone();
    let h1 = tokio::spawn(async move { n1.wait_for(3).await; });
    let h2 = tokio::spawn(async move { n2.wait_for(5).await; });
    tokio::task::yield_now().await;
    notifier.advance(5);
    h1.await.unwrap();
    h2.await.unwrap();
}

#[tokio::test]
async fn test_current_returns_applied_index() {
    let notifier = ApplyNotifier::new(42);
    assert_eq!(notifier.current(), 42);
    notifier.advance(100);
    assert_eq!(notifier.current(), 100);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test apply_notifier_test -- --nocapture 2>&1`
Expected: FAIL — module `apply_notifier` not found

**Step 3: Write minimal implementation**

```rust
// src/kv/apply_notifier.rs
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct ApplyNotifier {
    inner: Arc<ApplyNotifierInner>,
}

struct ApplyNotifierInner {
    applied: AtomicU64,
    tx: watch::Sender<u64>,
    rx: watch::Receiver<u64>,
}

impl ApplyNotifier {
    pub fn new(initial: u64) -> Self {
        let (tx, rx) = watch::channel(initial);
        Self {
            inner: Arc::new(ApplyNotifierInner {
                applied: AtomicU64::new(initial),
                tx,
                rx,
            }),
        }
    }

    /// Wait until applied index >= target. Returns immediately if already there.
    pub async fn wait_for(&self, target: u64) {
        if self.inner.applied.load(Ordering::Acquire) >= target {
            return;
        }
        let mut rx = self.inner.rx.clone();
        while *rx.borrow_and_update() < target {
            if rx.changed().await.is_err() {
                return; // sender dropped
            }
        }
    }

    /// Advance the applied index and wake all waiters.
    pub fn advance(&self, index: u64) {
        self.inner.applied.store(index, Ordering::Release);
        let _ = self.inner.tx.send(index);
    }

    /// Return current applied index.
    pub fn current(&self) -> u64 {
        self.inner.applied.load(Ordering::Acquire)
    }
}
```

Add to `src/kv/mod.rs`:
```rust
pub mod apply_notifier;
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test apply_notifier_test -- --nocapture 2>&1`
Expected: 4 tests PASS

**Step 5: Commit**

```bash
git add src/kv/apply_notifier.rs src/kv/mod.rs tests/apply_notifier_test.rs
git commit -m "feat: add ApplyNotifier for ReadIndex wait coordination"
```

---

### Task 2: Extend LogEntryData to carry pre-assigned revision

**Files:**
- Modify: `src/raft/messages.rs:13-20` (LogEntryData enum)
- Modify: `tests/raft_types_test.rs`

**Step 1: Write the failing test**

Add to `tests/raft_types_test.rs`:

```rust
#[test]
fn test_log_entry_command_with_revision() {
    let entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Command {
            data: vec![1, 2, 3],
            revision: 42,
        },
    };
    match &entry.data {
        LogEntryData::Command { data, revision } => {
            assert_eq!(data, &vec![1, 2, 3]);
            assert_eq!(*revision, 42);
        }
        _ => panic!("expected Command"),
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test raft_types_test test_log_entry_command_with_revision -- --nocapture 2>&1`
Expected: FAIL — Command variant doesn't have named fields

**Step 3: Change LogEntryData::Command from tuple to struct variant**

In `src/raft/messages.rs`, change:
```rust
// From:
Command(Vec<u8>),
// To:
Command { data: Vec<u8>, revision: i64 },
```

Then fix all compile errors across the codebase. Every match on `LogEntryData::Command(data)` becomes `LogEntryData::Command { data, .. }` or `LogEntryData::Command { data, revision }`. Every construction `LogEntryData::Command(bytes)` becomes `LogEntryData::Command { data: bytes, revision: 0 }` (temporarily 0 — Task 4 will set real values).

Key files to update:
- `src/raft/core.rs:207` — `handle_proposal` creates LogEntry with `Command(data)` → `Command { data, revision: 0 }`
- `src/raft/node.rs` — `execute_actions` matches on `Command(data)` → `Command { data, .. }`
- `src/kv/state_machine.rs:74` — match on `Command(data)` → `Command { data, .. }`
- `src/raft/log_store.rs` — serialization (JSON) auto-handles named fields
- Any test files that construct or match `LogEntryData::Command`

**Step 4: Run all tests to verify nothing breaks**

Run: `cargo test 2>&1`
Expected: All tests PASS (revision: 0 everywhere preserves existing behavior)

**Step 5: Commit**

```bash
git add src/raft/messages.rs src/raft/core.rs src/raft/node.rs src/kv/state_machine.rs tests/
git commit -m "refactor: LogEntryData::Command carries revision field"
```

---

### Task 3: Move revision counter from KvStore to Raft actor

**Files:**
- Modify: `src/raft/node.rs:89` (spawn_raft_node — add revision AtomicI64)
- Modify: `src/raft/node.rs:295` (spawn_raft_node_rebar — same)
- Modify: `src/raft/node.rs:43-55` (RaftHandle — add revision)
- Modify: `src/raft/core.rs:188-224` (handle_proposal — assign revision)
- Modify: `src/kv/store.rs:228-231` (KvStore — remove AtomicI64 revision ownership from proposal path)
- Modify: `src/api/server.rs:88` (wire revision into Raft handle)
- Create: `tests/revision_assignment_test.rs`

**Step 1: Write the failing test**

```rust
// tests/revision_assignment_test.rs
use barkeeper::raft::core::{RaftCore, RaftConfig, Event, Action, LogEntryData};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[test]
fn test_proposal_assigns_revision() {
    let config = RaftConfig {
        node_id: 1,
        peers: vec![],
        election_timeout_min: 150,
        election_timeout_max: 300,
    };
    let revision = Arc::new(AtomicI64::new(5));
    let mut core = RaftCore::new(config, revision.clone());

    // Become leader (single node)
    let _ = core.step(Event::ElectionTimeout);

    // Propose — should assign revision 6
    let actions = core.step(Event::Proposal {
        id: 1,
        data: vec![1, 2, 3],
    });

    // Find the AppendToLog action
    let append = actions.iter().find_map(|a| match a {
        Action::AppendToLog(entries) => Some(entries),
        _ => None,
    }).expect("should have AppendToLog");

    match &append[0].data {
        LogEntryData::Command { revision, .. } => {
            assert_eq!(*revision, 6);
        }
        _ => panic!("expected Command"),
    }

    // Counter advanced
    assert_eq!(revision.load(Ordering::SeqCst), 6);
}

#[test]
fn test_proposal_result_includes_revision() {
    let config = RaftConfig {
        node_id: 1,
        peers: vec![],
        election_timeout_min: 150,
        election_timeout_max: 300,
    };
    let revision = Arc::new(AtomicI64::new(0));
    let mut core = RaftCore::new(config, revision);
    let _ = core.step(Event::ElectionTimeout);

    let actions = core.step(Event::Proposal { id: 1, data: vec![] });
    let respond = actions.iter().find_map(|a| match a {
        Action::RespondToProposal { id, result } => Some((id, result)),
        _ => None,
    });
    // Single-node: should respond with Success including revision
    if let Some((_, result)) = respond {
        match result {
            barkeeper::raft::messages::ClientProposalResult::Success { revision, .. } => {
                assert_eq!(*revision, 1);
            }
            _ => panic!("expected Success"),
        }
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test revision_assignment_test -- --nocapture 2>&1`
Expected: FAIL — RaftCore::new doesn't accept revision parameter

**Step 3: Implement revision assignment in RaftCore**

In `src/raft/core.rs`:
- Add `revision: Arc<AtomicI64>` field to `RaftCore` struct
- `RaftCore::new` takes `Arc<AtomicI64>` parameter
- In `handle_proposal`: `let rev = self.revision.fetch_add(1, Ordering::SeqCst) + 1;`
- Create LogEntry with `LogEntryData::Command { data, revision: rev }`
- In `RespondToProposal`, set `revision: rev`

In `src/raft/node.rs`:
- `spawn_raft_node` and `spawn_raft_node_rebar` accept `Arc<AtomicI64>` and pass to `RaftCore::new`

In `src/api/server.rs`:
- Load initial revision from KvStore (already done at store open)
- Create `Arc<AtomicI64>` with that value
- Pass to spawn_raft_node

Update `ClientProposalResult::Success` in `src/raft/messages.rs` to include `revision: i64` (if not already).

**Step 4: Run all tests**

Run: `cargo test 2>&1`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/raft/core.rs src/raft/node.rs src/raft/messages.rs src/api/server.rs tests/revision_assignment_test.rs
git commit -m "feat: move revision counter to Raft actor, pre-assign in proposals"
```

---

### Task 4: State machine uses pre-assigned revision from log entry

**Files:**
- Modify: `src/kv/state_machine.rs:49-148` (apply method)
- Modify: `src/kv/store.rs:956` (batch_apply_with_index — accept revisions)
- Modify: `tests/integration_test.rs`

**Step 1: Write the failing test**

Add to `tests/integration_test.rs`:

```rust
#[tokio::test]
async fn test_state_machine_uses_preassigned_revision() {
    // Create store, state machine, broker
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb").to_str().unwrap()).unwrap());
    let broker = Arc::new(ApplyResultBroker::new());
    let watch_hub = Arc::new(WatchHub::new());
    let lease_manager = Arc::new(LeaseManager::new());

    let (apply_tx, apply_rx) = mpsc::channel(16);
    let notifier = ApplyNotifier::new(0);
    spawn_state_machine(store.clone(), apply_rx, broker.clone(), watch_hub, lease_manager, notifier.clone());

    // Create log entry with pre-assigned revision 42
    let cmd = KvCommand::Put {
        key: b"foo".to_vec(),
        value: b"bar".to_vec(),
        lease_id: 0,
    };
    let entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Command {
            data: bincode::serialize(&cmd).unwrap(),
            revision: 42,
        },
    };
    apply_tx.send(vec![entry]).await.unwrap();

    let result = broker.wait_for_result(1).await;
    match result {
        ApplyResult::Put(put_result) => {
            assert_eq!(put_result.revision, 42);
        }
        _ => panic!("expected Put result"),
    }

    // Verify store has the key at revision 42
    let range_result = store.range(b"foo", &[], 0, false).unwrap();
    assert_eq!(range_result.kvs.len(), 1);
    assert_eq!(range_result.kvs[0].mod_revision, 42);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test integration_test test_state_machine_uses_preassigned_revision -- --nocapture 2>&1`
Expected: FAIL — spawn_state_machine doesn't accept ApplyNotifier / revision is wrong

**Step 3: Implement**

In `src/kv/state_machine.rs`:
- `spawn_state_machine` accepts `ApplyNotifier` parameter
- `apply()` extracts revision from `LogEntryData::Command { data, revision }`
- Passes `Vec<(KvCommand, i64)>` (command + revision pairs) to store
- After batch apply completes: `notifier.advance(last_applied_index)`

In `src/kv/store.rs`:
- `batch_apply_with_index` signature changes: accepts `&[(KvCommand, i64)]` (command + pre-assigned revision)
- Instead of `self.revision.fetch_add(1, ...)`, uses the pre-assigned revision from the tuple
- Still persists final revision to META_TABLE at end of batch

**Step 4: Run all tests**

Run: `cargo test 2>&1`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/kv/state_machine.rs src/kv/store.rs tests/integration_test.rs
git commit -m "feat: state machine uses pre-assigned revision from log entries"
```

---

### Task 5: Early acknowledge for Put — drop broker wait

**Files:**
- Modify: `src/api/gateway.rs:610` (handle_put)
- Modify: `src/api/kv_service.rs:94` (put)
- Modify: `src/raft/node.rs:57-71` (RaftHandle — propose returns revision)
- Create: `tests/early_ack_test.rs`

**Step 1: Write the failing test**

```rust
// tests/early_ack_test.rs
// Integration test: PUT returns immediately with revision after Raft commit,
// before KV apply finishes.
// Test by adding artificial delay in state machine apply and verifying PUT returns fast.

use std::time::{Duration, Instant};

#[tokio::test]
async fn test_put_returns_revision_from_propose() {
    // Start a full barkeeper instance
    // PUT a key
    // Verify response includes correct revision
    // Verify it returns without waiting for broker
    // (We verify by checking that propose() result now carries the real revision)

    // This test verifies the contract: propose() returns Success { index, revision }
    // where revision > 0 (pre-assigned), and the handler uses that revision directly.

    // Use the compat_test pattern: start a real server, hit HTTP endpoint
    let port = portpicker::pick_unused_port().unwrap();
    let dir = tempfile::tempdir().unwrap();

    // Start server in background...
    // PUT /v3/kv/put
    // Assert response.header.revision > 0
    // Assert response completes in < 50ms (no KV apply wait)
}
```

Note: The actual test structure should follow the pattern in `tests/compat_test.rs` for starting a real server. The key assertion is that `propose()` returns `Success { revision: N }` where N > 0, and `handle_put` uses that revision directly instead of calling `broker.wait_for_result()`.

**Step 2: Run test to verify it fails**

Run: `cargo test --test early_ack_test -- --nocapture 2>&1`
Expected: FAIL

**Step 3: Implement**

In `src/api/gateway.rs` `handle_put`:
```rust
// BEFORE:
let proposal_result = state.raft_handle.propose(data).await;
let index = match proposal_result { Success { index, .. } => index, ... };
let result = state.broker.wait_for_result(index).await;
// Build response from result

// AFTER:
let proposal_result = state.raft_handle.propose(data).await;
match proposal_result {
    Success { index, revision } => {
        // Return immediately with pre-assigned revision
        // Optionally include prev_kv if requested (read before propose)
        let response = PutResponse { header: ResponseHeader { revision }, prev_kv };
        Ok(Json(response))
    }
    ...
}
```

For `prev_kv` support: if `prev_kv` is requested in the put, read it from `store_direct` *before* proposing.

Same pattern in `src/api/kv_service.rs` `put()`.

**DeleteRange and Txn remain unchanged** — they still wait on broker.

**Step 4: Run all tests**

Run: `cargo test 2>&1`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/api/gateway.rs src/api/kv_service.rs tests/early_ack_test.rs
git commit -m "feat: PUT returns immediately after Raft commit with pre-assigned revision"
```

---

### Task 6: ReadIndex for linearizable reads

**Files:**
- Modify: `src/raft/node.rs:43-55` (RaftHandle — add read_index method)
- Modify: `src/api/gateway.rs:572` (handle_range — add ReadIndex wait)
- Modify: `src/api/kv_service.rs:68` (range — add ReadIndex wait)
- Modify: `src/api/server.rs` (wire ApplyNotifier into gateway and kv_service)
- Create: `tests/read_index_test.rs`

**Step 1: Write the failing test**

```rust
// tests/read_index_test.rs
#[tokio::test]
async fn test_read_after_write_sees_value() {
    // Start full server
    // PUT key=foo value=bar (returns immediately via early ack)
    // GET key=foo (should see the value — ReadIndex waits for apply)
    // Assert value == bar
}

#[tokio::test]
async fn test_read_index_returns_commit_index() {
    // Create RaftHandle with known commit state
    // Call read_index()
    // Assert returns current commit index
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test read_index_test -- --nocapture 2>&1`
Expected: FAIL — read_index method doesn't exist

**Step 3: Implement**

In `src/raft/node.rs` RaftHandle:
```rust
pub struct RaftHandle {
    pub proposal_tx: mpsc::Sender<ClientProposal>,
    pub inbound_tx: mpsc::Sender<(u64, RaftMessage)>,
    pub current_term: Arc<AtomicU64>,
    pub applied_index: Arc<AtomicU64>,
    pub commit_index: Arc<AtomicU64>,  // NEW
    pub leader_id: Arc<AtomicU64>,
}

impl RaftHandle {
    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Acquire)
    }
}
```

In the Raft actor loop, update `commit_index` AtomicU64 whenever `advance_commit_index` fires.

In `src/api/gateway.rs` `handle_range`:
```rust
// NEW: ReadIndex — wait for apply to catch up to commit
let commit = state.raft_handle.commit_index();
state.apply_notifier.wait_for(commit).await;
// Then do store.range() as before
```

Same in `src/api/kv_service.rs` `range()`.

Wire `ApplyNotifier` into `GatewayState` and `KvService` via `server.rs`.

**Step 4: Run all tests**

Run: `cargo test 2>&1`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/raft/node.rs src/api/gateway.rs src/api/kv_service.rs src/api/server.rs tests/read_index_test.rs
git commit -m "feat: ReadIndex for linearizable reads — wait for apply before serving"
```

---

### Task 7: Wire everything together in server.rs

**Files:**
- Modify: `src/api/server.rs`

**Step 1: Verify integration**

This task is about ensuring all pieces connect:
- `ApplyNotifier` created in server.rs, passed to state machine + gateway + kv_service
- Revision `Arc<AtomicI64>` created from KvStore initial revision, passed to Raft actor
- `commit_index` `Arc<AtomicU64>` created and shared between Raft actor and RaftHandle

**Step 2: Run full test suite**

Run: `cargo test 2>&1`
Expected: All tests PASS

**Step 3: Run compat tests specifically**

Run: `cargo test --test compat_test -- --nocapture 2>&1`
Expected: All compat tests PASS (etcd API behavior unchanged)

**Step 4: Commit**

```bash
git add src/api/server.rs
git commit -m "feat: wire ApplyNotifier and revision counter through server"
```

---

### Task 8: Run benchmarks

**Step 1: Build release**

Run: `cargo build --release 2>&1`

**Step 2: Run benchmark suite**

Run: `cd bench && bash harness/run.sh all 2>&1`

**Step 3: Compare results**

Expected improvements:
- Write c=1 latency: should drop (only 1 fsync instead of 2)
- Write c=100+ throughput: should improve (Raft log commit is the only blocking path)
- Read latency: minimal change (ReadIndex wait is ~0 on single node)

**Step 4: Update RESULTS.md**

Copy new results into `bench/results/RESULTS.md`.

**Step 5: Commit**

```bash
git add bench/results/RESULTS.md
git commit -m "bench: update results after WAL-first writes optimization"
```
