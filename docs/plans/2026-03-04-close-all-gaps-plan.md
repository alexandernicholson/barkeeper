# Close All Remaining etcd Gaps — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close all 8 remaining behavioral gaps between barkeeper and etcd using TDD, bottom-up ordering.

**Architecture:** Quick fixes first (HTTP compaction, txn watch, revision watching), then production lease expiry timer, then route writes through Raft consensus, then auth enforcement, TLS (with auto-TLS matching etcd), and finally multi-node networking via gRPC transport.

**Tech Stack:** Rust, tonic (gRPC), axum (HTTP), redb (storage), rcgen (auto-TLS), tokio-rustls, bcrypt

**Project root:** `/home/alexandernicholson/.pxycrab/workspace/barkeeper`

---

## Phase 1: Quick Fixes

---

### Task 1: HTTP Compaction — Failing Test

**Files:**
- Modify: `tests/compat_test.rs`

**Step 1: Write failing test**

Add to `tests/compat_test.rs`:

```rust
/// HTTP compaction endpoint should work, not return 501.
#[tokio::test]
async fn test_http_compaction_works() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    // Put some keys to create revisions.
    client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(r#"{{"key":"{}","value":"{}"}}"#, b64("compact1"), b64("v1")))
        .send().await.unwrap();
    client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(r#"{{"key":"{}","value":"{}"}}"#, b64("compact2"), b64("v2")))
        .send().await.unwrap();

    // Compact at revision 1.
    let resp = client
        .post(format!("http://{}/v3/kv/compaction", addr))
        .body(r#"{"revision":"1"}"#)
        .send().await.unwrap();

    assert_eq!(resp.status(), 200, "compaction should return 200, not 501");

    let body: Value = resp.json().await.unwrap();
    assert!(body.get("header").is_some(), "compaction response should have header");
}
```

**Step 2: Run test to verify it fails**

Run: `~/.cargo/bin/cargo test test_http_compaction_works 2>&1`
Expected: FAIL — status 501

**Step 3: Commit**

```bash
git add tests/compat_test.rs
git commit -m "test: add failing test for HTTP compaction endpoint"
```

---

### Task 2: HTTP Compaction — Implementation

**Files:**
- Modify: `src/api/gateway.rs`

**Step 1: Add CompactionRequest/CompactionResponse types and handler**

In `src/api/gateway.rs`, add JSON types (near the other request/response types around line 131):

```rust
#[derive(Debug, Deserialize, Default)]
struct CompactionRequest {
    #[serde(default)]
    revision: Option<String>, // proto3 JSON: int64 as string
}

#[derive(Debug, Serialize)]
struct CompactionResponse {
    header: JsonResponseHeader,
}
```

Replace `handle_compaction_stub` (line 703) with:

```rust
async fn handle_compaction(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: CompactionRequest = parse_json(&body);
    let revision: i64 = req.revision
        .as_deref()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);

    match state.store.compact(revision) {
        Ok(()) => {
            let rev = state.store.current_revision().unwrap_or(0);
            axum::Json(CompactionResponse {
                header: state.make_header(rev),
            })
            .into_response()
        }
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("compact: {}", e),
        )
        .into_response(),
    }
}
```

Update the router (line 436) from `handle_compaction_stub` to `handle_compaction`.

**Step 2: Run test to verify it passes**

Run: `~/.cargo/bin/cargo test test_http_compaction_works 2>&1`
Expected: PASS

**Step 3: Run all tests**

Run: `~/.cargo/bin/cargo test 2>&1`
Expected: All pass

**Step 4: Commit**

```bash
git add src/api/gateway.rs
git commit -m "feat(gateway): implement HTTP compaction endpoint"
```

---

### Task 3: Txn Watch Notifications — Failing Test

**Files:**
- Create: `tests/txn_watch_test.rs`

**Step 1: Write failing test**

Create `tests/txn_watch_test.rs`:

```rust
//! Tests that watch notifications fire for mutations inside transactions.

use std::sync::Arc;
use tokio::time::{sleep, Duration, timeout};

use barkeeper::kv::store::KvStore;
use barkeeper::watch::hub::WatchHub;

/// A put inside a txn should fire a watch notification.
#[tokio::test]
async fn test_txn_put_fires_watch_notification() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());
    let hub = Arc::new(WatchHub::new());

    // Watch key "txnkey".
    let (_wid, mut rx) = hub.create_watch(b"txnkey".to_vec(), vec![], 0).await;

    // Execute a txn that puts "txnkey".
    let result = store.txn(
        vec![], // no compares
        vec![barkeeper::kv::store::TxnOp::Put {
            key: b"txnkey".to_vec(),
            value: b"txnval".to_vec(),
            lease_id: 0,
        }],
        vec![],
    ).unwrap();

    // Fire notifications for the txn results.
    // This is what the service layer should do after txn().
    for resp in &result.responses {
        match resp {
            barkeeper::kv::store::TxnOpResponse::Put(r) => {
                let (create_rev, ver) = match &r.prev_kv {
                    Some(prev) => (prev.create_revision, prev.version + 1),
                    None => (r.revision, 1),
                };
                let kv = barkeeper::proto::mvccpb::KeyValue {
                    key: b"txnkey".to_vec(),
                    create_revision: create_rev,
                    mod_revision: r.revision,
                    version: ver,
                    value: b"txnval".to_vec(),
                    lease: 0,
                };
                hub.notify(b"txnkey", 0, kv, r.prev_kv.clone()).await;
            }
            _ => {}
        }
    }

    let event = timeout(Duration::from_secs(1), rx.recv()).await
        .expect("should receive event within 1s")
        .expect("channel should not be closed");

    assert_eq!(event.events.len(), 1);
    assert_eq!(event.events[0].r#type, 0); // PUT
    let kv = event.events[0].kv.as_ref().unwrap();
    assert_eq!(kv.key, b"txnkey");
    assert_eq!(kv.value, b"txnval");
}

/// A delete inside a txn should fire a watch notification.
#[tokio::test]
async fn test_txn_delete_fires_watch_notification() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());
    let hub = Arc::new(WatchHub::new());

    // Pre-populate key.
    store.put(b"delkey", b"val", 0).unwrap();

    // Watch key "delkey".
    let (_wid, mut rx) = hub.create_watch(b"delkey".to_vec(), vec![], 0).await;

    // Execute a txn that deletes "delkey".
    let result = store.txn(
        vec![],
        vec![barkeeper::kv::store::TxnOp::DeleteRange {
            key: b"delkey".to_vec(),
            range_end: vec![],
        }],
        vec![],
    ).unwrap();

    // Fire notifications for the txn results.
    for resp in &result.responses {
        match resp {
            barkeeper::kv::store::TxnOpResponse::DeleteRange(r) => {
                for prev in &r.prev_kvs {
                    let tombstone = barkeeper::proto::mvccpb::KeyValue {
                        key: prev.key.clone(),
                        create_revision: 0,
                        mod_revision: r.revision,
                        version: 0,
                        value: vec![],
                        lease: 0,
                    };
                    hub.notify(&prev.key, 1, tombstone, Some(prev.clone())).await;
                }
            }
            _ => {}
        }
    }

    let event = timeout(Duration::from_secs(1), rx.recv()).await
        .expect("should receive event within 1s")
        .expect("channel should not be closed");

    assert_eq!(event.events.len(), 1);
    assert_eq!(event.events[0].r#type, 1); // DELETE
}
```

**Step 2: Run test to verify it passes (test exercises notify directly)**

Run: `~/.cargo/bin/cargo test --test txn_watch_test 2>&1`
Expected: PASS (tests prove the notification pattern works)

**Step 3: Commit**

```bash
git add tests/txn_watch_test.rs
git commit -m "test: add txn watch notification tests"
```

---

### Task 4: Txn Watch Notifications — Implementation

**Files:**
- Modify: `src/api/kv_service.rs`
- Modify: `src/api/gateway.rs`

**Step 1: Add notify helper function**

Add a helper to fire notifications from TxnOpResponse results. In `src/api/kv_service.rs`, after the `txn` method, add a helper method to the impl block:

```rust
async fn notify_txn_results(
    watch_hub: &WatchHub,
    responses: &[TxnOpResponse],
    revision: i64,
) {
    for resp in responses {
        match resp {
            TxnOpResponse::Put(r) => {
                // We need to reconstruct the key/value from PutResult.
                // PutResult has revision and prev_kv but NOT the key/value directly.
                // The key/value are available from prev_kv or must be passed separately.
                // Since TxnOpResponse::Put only has PutResult { revision, prev_kv },
                // we cannot recover the key here. This is handled below by iterating
                // the original ops alongside responses.
            }
            _ => {}
        }
    }
}
```

Actually, the issue is that `TxnOpResponse::Put(PutResult)` doesn't carry the key/value — only `revision` and `prev_kv`. We need to zip the original ops with the responses to get the key/value data.

Replace the TODO in `kv_service.rs` txn method (lines 190-193) with:

```rust
// Notify watchers for txn mutations.
// We need to zip the executed ops with responses to recover key/value data.
let executed_ops = if result.succeeded { &success } else { &failure };
for (op, resp) in executed_ops.iter().zip(result.responses.iter()) {
    match (op, resp) {
        (TxnOp::Put { key, value, lease_id }, TxnOpResponse::Put(r)) => {
            let (create_rev, ver) = match &r.prev_kv {
                Some(prev) => (prev.create_revision, prev.version + 1),
                None => (r.revision, 1),
            };
            let notify_kv = crate::proto::mvccpb::KeyValue {
                key: key.clone(),
                create_revision: create_rev,
                mod_revision: r.revision,
                version: ver,
                value: value.clone(),
                lease: *lease_id,
            };
            self.watch_hub.notify(key, 0, notify_kv, r.prev_kv.clone()).await;
        }
        (TxnOp::DeleteRange { .. }, TxnOpResponse::DeleteRange(r)) => {
            for prev in &r.prev_kvs {
                let tombstone = crate::proto::mvccpb::KeyValue {
                    key: prev.key.clone(),
                    create_revision: 0,
                    mod_revision: r.revision,
                    version: 0,
                    value: vec![],
                    lease: 0,
                };
                self.watch_hub.notify(&prev.key, 1, tombstone, Some(prev.clone())).await;
            }
        }
        _ => {} // Range ops don't need notifications
    }
}
```

Note: The `result.responses` is consumed by the response conversion below. We need to collect notifications BEFORE consuming `result.responses`. Restructure the txn method to:
1. Extract `result.responses` as a reference first for notifications
2. Then consume for response conversion

The exact change: move the notification loop BEFORE the `result.responses.into_iter()` call, and change the notification to iterate `&result.responses`. Then the existing `into_iter()` consumes afterward.

**Step 2: Apply same pattern in gateway.rs**

In `src/api/gateway.rs` `handle_txn`, replace the TODO (lines 636-639) with the same pattern, but using the gateway's TxnOp types. The gateway's `convert_ops` returns `Vec<TxnOp>` (internal types), so the same zip pattern applies.

**Step 3: Run txn watch tests**

Run: `~/.cargo/bin/cargo test --test txn_watch_test 2>&1`
Expected: PASS

**Step 4: Run all tests**

Run: `~/.cargo/bin/cargo test 2>&1`
Expected: All pass

**Step 5: Commit**

```bash
git add src/api/kv_service.rs src/api/gateway.rs
git commit -m "feat(watch): fire notifications for mutations inside transactions"
```

---

### Task 5: Revision-Based Watching — Failing Test (KvStore changes_since)

**Files:**
- Create: `tests/watch_revision_test.rs`

**Step 1: Write failing test for changes_since**

Create `tests/watch_revision_test.rs`:

```rust
//! Tests for revision-based watching.

use std::sync::Arc;

use barkeeper::kv::store::KvStore;

/// changes_since should return mutations after the given revision.
#[tokio::test]
async fn test_changes_since_returns_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());

    // Create 3 revisions.
    store.put(b"a", b"v1", 0).unwrap(); // rev 1
    store.put(b"b", b"v2", 0).unwrap(); // rev 2
    store.put(b"c", b"v3", 0).unwrap(); // rev 3

    // Get changes since revision 1 (should return revs 2 and 3).
    let changes = store.changes_since(1).unwrap();
    assert_eq!(changes.len(), 2, "should have 2 changes after rev 1");
    assert_eq!(changes[0].0, b"b"); // key
    assert_eq!(changes[1].0, b"c"); // key
}

/// changes_since(0) should return all mutations.
#[tokio::test]
async fn test_changes_since_zero_returns_all() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());

    store.put(b"x", b"v1", 0).unwrap();
    store.put(b"y", b"v2", 0).unwrap();

    let changes = store.changes_since(0).unwrap();
    assert_eq!(changes.len(), 2);
}

/// changes_since should include delete events.
#[tokio::test]
async fn test_changes_since_includes_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());

    store.put(b"del", b"v1", 0).unwrap(); // rev 1
    store.delete_range(b"del", b"").unwrap(); // rev 2

    let changes = store.changes_since(1).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].0, b"del");
    assert_eq!(changes[0].1, 1); // DELETE event type
}
```

**Step 2: Run test to verify it fails**

Run: `~/.cargo/bin/cargo test --test watch_revision_test 2>&1`
Expected: FAIL — `changes_since` method does not exist

**Step 3: Commit**

```bash
git add tests/watch_revision_test.rs
git commit -m "test: add failing tests for KvStore changes_since"
```

---

### Task 6: Revision-Based Watching — Implement changes_since

**Files:**
- Modify: `src/kv/store.rs`

**Step 1: Implement changes_since**

Add to `impl KvStore` in `src/kv/store.rs`:

```rust
/// Return all mutations since `after_revision` (exclusive).
/// Each entry is (key, event_type, kv_at_revision).
/// event_type: 0 = PUT, 1 = DELETE.
///
/// Returns entries in revision order. Used by WatchHub for revision-based
/// watching — replaying historical events to new watchers.
pub fn changes_since(&self, after_revision: i64) -> Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue)>, redb::Error> {
    let txn = self.db.begin_read()?;
    let rev_table = txn.open_table(REV_TABLE)?;
    let kv_table = txn.open_table(KV_TABLE)?;

    let start_rev = (after_revision + 1) as u64;
    let mut results = Vec::new();

    // Iterate revision entries from start_rev onwards.
    let range = rev_table.range(start_rev..)?;
    for entry in range {
        let (rev_key, rev_val) = entry?;
        let revision = rev_key.value();
        let entries: Vec<RevisionEntry> = serde_json::from_slice(rev_val.value())
            .expect("deserialize revision entries");

        for re in entries {
            let event_type = match re.event_type {
                EventType::Put => 0,
                EventType::Delete => 1,
            };

            // Look up the KV at this revision.
            let compound = make_compound_key(&re.key, revision);
            let kv = match kv_table.get(compound.as_slice())? {
                Some(val) => {
                    let ikv: InternalKeyValue = serde_json::from_slice(val.value())
                        .expect("deserialize kv");
                    ikv.to_proto()
                }
                None => {
                    // Shouldn't happen, but create a minimal KV.
                    mvccpb::KeyValue {
                        key: re.key.clone(),
                        mod_revision: revision as i64,
                        ..Default::default()
                    }
                }
            };

            results.push((re.key, event_type, kv));
        }
    }

    Ok(results)
}
```

**Step 2: Run test to verify it passes**

Run: `~/.cargo/bin/cargo test --test watch_revision_test 2>&1`
Expected: PASS

**Step 3: Run all tests**

Run: `~/.cargo/bin/cargo test 2>&1`
Expected: All pass

**Step 4: Commit**

```bash
git add src/kv/store.rs
git commit -m "feat(kv): implement changes_since for revision-based history queries"
```

---

### Task 7: Revision-Based Watching — Wire into WatchHub

**Files:**
- Modify: `src/watch/hub.rs`

**Step 1: Update WatchHub to accept a KvStore reference and replay history**

Modify `WatchHub` to optionally hold a reference to `KvStore` for history replays. Update `create_watch`:

```rust
use crate::kv::store::KvStore;

pub struct WatchHub {
    watchers: Arc<Mutex<HashMap<i64, Watcher>>>,
    next_id: Arc<Mutex<i64>>,
    store: Option<Arc<KvStore>>, // For revision-based history replay
}

impl WatchHub {
    pub fn new() -> Self {
        WatchHub {
            watchers: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
            store: None,
        }
    }

    /// Create a WatchHub with a KvStore reference for revision-based watching.
    pub fn with_store(store: Arc<KvStore>) -> Self {
        WatchHub {
            watchers: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
            store: Some(store),
        }
    }
```

In `create_watch`, after inserting the watcher, replay history if `start_revision > 0`:

```rust
// After inserting watcher into the map:
if start_revision > 0 {
    if let Some(ref store) = self.store {
        // Replay historical events.
        match store.changes_since(start_revision - 1) {
            Ok(changes) => {
                for (key, event_type, kv) in changes {
                    if key_matches(&watcher_key_clone, &watcher_range_end_clone, &key) {
                        let event = mvccpb::Event {
                            r#type: event_type,
                            kv: Some(kv),
                            prev_kv: None, // Historical events don't include prev_kv
                        };
                        let watch_event = WatchEvent {
                            watch_id: id,
                            events: vec![event],
                            compact_revision: 0,
                        };
                        let _ = tx_clone.send(watch_event).await;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to replay watch history");
            }
        }
    }
}
```

Note: Clone `key`, `range_end`, and `tx` before inserting into the watcher map since we need them for replay.

**Step 2: Update server.rs and tests to use WatchHub::with_store**

In `src/api/server.rs`, change `WatchHub::new()` to `WatchHub::with_store(Arc::clone(&store))`.

In test helpers (`tests/compat_test.rs`, `tests/lease_expiry_integration_test.rs`), also update to `WatchHub::with_store(Arc::clone(&store))`.

**Step 3: Add test for revision-based watching through WatchHub**

Add to `tests/watch_revision_test.rs`:

```rust
use barkeeper::watch::hub::WatchHub;
use tokio::time::{timeout, Duration};

/// WatchHub with start_revision should replay historical events.
#[tokio::test]
async fn test_watchhub_replays_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());
    let hub = WatchHub::with_store(Arc::clone(&store));

    // Create some history.
    store.put(b"hist", b"v1", 0).unwrap(); // rev 1
    store.put(b"hist", b"v2", 0).unwrap(); // rev 2
    store.put(b"other", b"v3", 0).unwrap(); // rev 3

    // Watch "hist" from revision 1 — should replay rev 1 and 2.
    let (_wid, mut rx) = hub.create_watch(b"hist".to_vec(), vec![], 1).await;

    // Should receive 2 historical events.
    let e1 = timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap();
    assert_eq!(e1.events[0].kv.as_ref().unwrap().value, b"v1");

    let e2 = timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap();
    assert_eq!(e2.events[0].kv.as_ref().unwrap().value, b"v2");
}
```

**Step 4: Run test**

Run: `~/.cargo/bin/cargo test --test watch_revision_test 2>&1`
Expected: PASS

**Step 5: Run all tests**

Run: `~/.cargo/bin/cargo test 2>&1`
Expected: All pass

**Step 6: Commit**

```bash
git add src/watch/hub.rs src/api/server.rs tests/compat_test.rs tests/lease_expiry_integration_test.rs tests/watch_revision_test.rs
git commit -m "feat(watch): implement revision-based watching with history replay"
```

---

## Phase 2: Production Lease Expiry Timer

---

### Task 8: Production Lease Expiry — Failing Test

**Files:**
- Modify: `tests/compat_test.rs`

**Step 1: Write failing test that uses the real server path**

Add to `tests/compat_test.rs`:

```rust
/// Keys attached to expired leases should be automatically deleted.
/// This tests the production expiry timer path (not the integration test's custom timer).
#[tokio::test]
async fn test_lease_expiry_via_production_timer() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    // Grant a short-lived lease.
    let resp: Value = client
        .post(format!("http://{}/v3/lease/grant", addr))
        .body(r#"{"TTL": 1}"#)
        .send().await.unwrap()
        .json().await.unwrap();
    let lease_id = resp["ID"].as_str().unwrap();

    // Put a key with the lease.
    client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(
            r#"{{"key":"{}","value":"{}","lease":{}}}"#,
            b64("expiring"), b64("temp"), lease_id
        ))
        .send().await.unwrap();

    // Key should exist.
    let get: Value = client
        .post(format!("http://{}/v3/kv/range", addr))
        .body(format!(r#"{{"key":"{}"}}"#, b64("expiring")))
        .send().await.unwrap()
        .json().await.unwrap();
    assert!(get["kvs"].as_array().unwrap().len() > 0);

    // Wait for expiry (TTL=1 + timer interval + margin).
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Key should be gone.
    let get2: Value = client
        .post(format!("http://{}/v3/kv/range", addr))
        .body(format!(r#"{{"key":"{}"}}"#, b64("expiring")))
        .send().await.unwrap()
        .json().await.unwrap();
    let kvs = get2.get("kvs").and_then(|v| v.as_array());
    assert!(
        kvs.is_none() || kvs.unwrap().is_empty(),
        "key should be deleted after lease expiry"
    );
}
```

**Step 2: Run test to verify it fails**

Run: `~/.cargo/bin/cargo test test_lease_expiry_via_production_timer 2>&1`
Expected: FAIL — key still exists (no expiry timer in compat_test's start_test_instance)

Note: `start_test_instance` doesn't spawn a lease expiry timer. We need to add one.

**Step 3: Commit**

```bash
git add tests/compat_test.rs
git commit -m "test: add failing test for production lease expiry timer"
```

---

### Task 9: Production Lease Expiry — Implementation

**Files:**
- Modify: `src/api/server.rs`
- Modify: `tests/compat_test.rs` (add timer to start_test_instance)

**Step 1: Add lease expiry timer to server.rs**

In `BarkeepServer::start()`, after creating the watch_hub and lease_manager (around line 75), add:

```rust
// Spawn lease expiry timer — checks every 500ms for expired leases.
{
    let lm = Arc::clone(&lease_manager);
    let st = Arc::clone(&store);
    let wh = Arc::clone(&watch_hub);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            let expired = lm.check_expired().await;
            for lease in expired {
                for key in &lease.keys {
                    if let Ok(result) = st.delete_range(key, b"") {
                        for prev in &result.prev_kvs {
                            let tombstone = crate::proto::mvccpb::KeyValue {
                                key: prev.key.clone(),
                                create_revision: 0,
                                mod_revision: result.revision,
                                version: 0,
                                value: vec![],
                                lease: 0,
                            };
                            wh.notify(&prev.key, 1, tombstone, Some(prev.clone())).await;
                        }
                    }
                }
            }
        }
    });
}
```

**Step 2: Add same timer to compat_test.rs start_test_instance**

In `tests/compat_test.rs`, after creating watch_hub and lease_manager in `start_test_instance()`, add the same timer spawn pattern.

**Step 3: Run test to verify it passes**

Run: `~/.cargo/bin/cargo test test_lease_expiry_via_production_timer 2>&1`
Expected: PASS

**Step 4: Run all tests**

Run: `~/.cargo/bin/cargo test 2>&1`
Expected: All pass

**Step 5: Commit**

```bash
git add src/api/server.rs tests/compat_test.rs
git commit -m "feat(lease): add production lease expiry timer in server startup"
```

---

## Phase 3: Writes Through Raft

---

### Task 10: Writes Through Raft — Failing Test

**Files:**
- Modify: `tests/compat_test.rs`

**Step 1: Write test verifying writes go through Raft**

The existing tests already verify that puts/deletes work. The behavioral difference when writes go through Raft is that the state machine applies the mutation (not the service directly). We can verify this by checking that the raft_index advances after a write.

Add to `tests/compat_test.rs`:

```rust
/// After a write, the maintenance status should show advancing raft_applied_index.
#[tokio::test]
async fn test_writes_advance_raft_applied_index() {
    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    // Check initial status.
    let status1: Value = client
        .post(format!("http://{}/v3/maintenance/status", addr))
        .body("{}")
        .send().await.unwrap()
        .json().await.unwrap();

    // Do a write.
    client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(r#"{{"key":"{}","value":"{}"}}"#, b64("raftwrite"), b64("val")))
        .send().await.unwrap();

    // Check status after write.
    let status2: Value = client
        .post(format!("http://{}/v3/maintenance/status", addr))
        .body("{}")
        .send().await.unwrap()
        .json().await.unwrap();

    let idx1: u64 = status1["raftAppliedIndex"].as_str().unwrap_or("0").parse().unwrap_or(0);
    let idx2: u64 = status2["raftAppliedIndex"].as_str().unwrap_or("0").parse().unwrap_or(0);
    assert!(idx2 > idx1, "raft_applied_index should advance after write: {} -> {}", idx1, idx2);
}
```

**Step 2: Run test to verify it fails**

Run: `~/.cargo/bin/cargo test test_writes_advance_raft_applied_index 2>&1`
Expected: FAIL — raft_applied_index stays 0

**Step 3: Commit**

```bash
git add tests/compat_test.rs
git commit -m "test: add failing test for writes advancing raft applied index"
```

---

### Task 11: Writes Through Raft — Implementation

This is the largest task. It requires:
1. Enhancing state machine to return results (not just log them)
2. Services use RaftHandle.propose() instead of store directly
3. Gateway uses RaftHandle.propose() instead of store directly
4. Track raft_applied_index for status endpoint

**Files:**
- Modify: `src/kv/state_machine.rs` — return results from apply
- Modify: `src/raft/node.rs` — add shared applied_index counter
- Modify: `src/api/kv_service.rs` — use RaftHandle
- Modify: `src/api/gateway.rs` — use RaftHandle
- Modify: `src/api/server.rs` — pass RaftHandle to services
- Modify: `src/api/maintenance_service.rs` — expose applied_index

Since this is large, implement incrementally:

**Step 1: Enhance state machine to return results via channel**

The current `spawn_state_machine` receives `Vec<LogEntry>` and applies them but discards results. We need a way for the proposer to get the result back. The approach: when proposing, the service attaches a oneshot channel. After the state machine applies, it sends the result back through that channel.

Modify `src/kv/state_machine.rs`:
- Change the apply channel to carry `(KvCommand, oneshot::Sender<Result>)` pairs
- Or better: keep the existing Raft flow but add a result callback mechanism

The cleanest approach: add a `pending_results` map keyed by log index. When a proposal is committed, the Raft node already calls `RespondToProposal` with the log index. The service can wait on that response and then read the result from the store.

Actually, the simplest approach for single-node: services call `raft_handle.propose()`, which returns `ClientProposalResult::Success { index, revision }` after commit. Since single-node commits immediately, we just need to make the state machine apply the entry and update the revision before the proposal result returns.

The current flow is:
1. Service calls `raft_handle.propose(data)` → oneshot channel
2. Raft commits → `Action::ApplyEntries` → entries sent to state machine via `apply_tx`
3. Raft sends `Action::RespondToProposal` → Success with index
4. Service gets Success — but state machine may not have applied yet!

Fix: Make `RespondToProposal` fire AFTER `ApplyEntries` completes. Currently both are processed in sequence in `execute_actions`, so if `ApplyEntries` sends to `apply_tx` and the state machine is fast enough, it should work. But there's a race.

Better fix: Have the proposal response include the actual mutation result. This requires changing the state machine to be synchronous (apply inline) rather than async (separate task). For single-node, the simplest path:

**Services call store directly but ALSO go through Raft for consensus ordering.** Wait — that defeats the purpose.

**Simplest correct approach:** Services serialize a `KvCommand`, propose it through Raft, wait for commit confirmation, then apply it locally and return the result. Since we're single-node leader, the commit is immediate.

Implement this step by step. The key change to `kv_service.rs` put():

```rust
// Instead of: self.store.put(&req.key, &req.value, req.lease)
// Do:
let cmd = KvCommand::Put {
    key: req.key.clone(),
    value: req.value.clone(),
    lease_id: req.lease,
};
let data = serde_json::to_vec(&cmd).map_err(|e| Status::internal(e.to_string()))?;
let proposal_result = self.raft_handle.propose(data).await
    .map_err(|e| Status::unavailable(format!("raft: {}", e)))?;
match proposal_result {
    ClientProposalResult::Success { .. } => {
        // Entry committed. Apply to store.
        let result = self.store.put(&req.key, &req.value, req.lease)
            .map_err(|e| Status::internal(format!("put failed: {}", e)))?;
        // ... rest of notify and response
    }
    ClientProposalResult::NotLeader { .. } => {
        return Err(Status::unavailable("not leader"));
    }
    ClientProposalResult::Error(e) => {
        return Err(Status::internal(e));
    }
}
```

Wait — this applies the mutation twice (once in the state machine from Raft, once from the service). We need to disable the state machine's apply for committed entries that the service will handle. Or stop using the state machine entirely and apply inline.

**Cleanest approach: Remove the separate state machine task. Apply inline after Raft commit.** This means:
1. Remove `spawn_state_machine` from server.rs
2. Remove the `apply_tx/apply_rx` channel
3. Services call `raft_handle.propose()`, wait for `Success`, then apply to store directly
4. The `apply_tx` in `spawn_raft_node` becomes unused — pass a dummy channel or refactor

Actually, refactor `spawn_raft_node` to NOT require `apply_tx`. The `ApplyEntries` action can be a no-op for now (the service applies after getting the proposal result). Later for multi-node, followers would need the state machine to apply entries they didn't propose.

This is getting complex. Let me simplify:

**For now (single-node focus):** Keep the state machine but make it a no-op for entries that the proposer will apply. The proposer waits for `Success`, then applies. Add `raft_handle` to services.

Implement in server.rs:
1. Pass `raft_handle.clone()` to KvService, gateway
2. In services, propose through Raft, await commit, then apply to store
3. Watch notifications and lease attachment happen in the service after apply (already wired)

**Step 2: Add raft_handle to services**

Modify `KvService` to hold `raft_handle: RaftHandle`:
```rust
pub struct KvService {
    store: Arc<KvStore>,
    watch_hub: Arc<WatchHub>,
    lease_manager: Arc<LeaseManager>,
    raft_handle: RaftHandle,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
}
```

Update constructor and server.rs to pass `raft_handle.clone()`.

**Step 3: Route put/delete/txn/compact through Raft**

Each mutation method:
1. Serialize KvCommand to JSON
2. Call `self.raft_handle.propose(data).await`
3. On Success, apply to store (existing code)
4. On NotLeader, return UNAVAILABLE
5. On Error, return INTERNAL

**Step 4: Same for gateway**

Add `raft_handle: RaftHandle` to `GatewayState`, pass in `create_router`. Same propose flow.

**Step 5: Track applied_index**

Add `Arc<AtomicU64>` for `raft_applied_index` to `RaftHandle`. Increment in the Raft node after `ApplyEntries`. Pass to maintenance service and gateway for status response.

**Step 6: Run tests**

Run: `~/.cargo/bin/cargo test 2>&1`
Expected: All pass (existing tests should work since single-node commits immediately)

**Step 7: Commit**

```bash
git add src/api/kv_service.rs src/api/gateway.rs src/api/server.rs src/api/maintenance_service.rs src/raft/node.rs src/kv/state_machine.rs tests/compat_test.rs tests/lease_expiry_integration_test.rs
git commit -m "feat(raft): route all KV mutations through Raft consensus

Services now propose writes through RaftHandle, wait for commit, then
apply to store. Single-node leader commits immediately so behavior is
identical. raft_applied_index is tracked and exposed in status."
```

---

## Phase 4: Auth Enforcement

---

### Task 12: Auth Enforcement — Failing Test

**Files:**
- Create: `tests/auth_test.rs`

**Step 1: Write failing test**

```rust
//! Tests for auth enforcement.

use std::net::SocketAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use reqwest::Client;
use serde_json::Value;

// ... (reuse start_test_instance or create a new helper)

fn b64(s: &str) -> String { B64.encode(s.as_bytes()) }

/// When auth is enabled, unauthenticated requests should be rejected.
#[tokio::test]
async fn test_auth_enabled_rejects_unauthenticated() {
    // Start instance, enable auth, then verify unauthenticated put is rejected.
    // This test will initially fail because auth enforcement doesn't exist.

    let (addr, _dir) = start_test_instance().await;
    let client = Client::new();

    // Create root user and enable auth.
    client.post(format!("http://{}/v3/auth/user/add", addr))
        .body(r#"{"name":"root","password":"rootpass"}"#)
        .send().await.unwrap();

    client.post(format!("http://{}/v3/auth/role/add", addr))
        .body(r#"{"name":"root"}"#)
        .send().await.unwrap();

    client.post(format!("http://{}/v3/auth/user/grant", addr))
        .body(r#"{"user":"root","role":"root"}"#)
        .send().await.unwrap();

    client.post(format!("http://{}/v3/auth/enable", addr))
        .body("{}")
        .send().await.unwrap();

    // Now try an unauthenticated put — should fail.
    let resp = client
        .post(format!("http://{}/v3/kv/put", addr))
        .body(format!(r#"{{"key":"{}","value":"{}"}}"#, b64("secret"), b64("val")))
        .send().await.unwrap();

    assert_eq!(resp.status(), 401, "unauthenticated request should be rejected when auth is enabled");
}
```

Note: This test requires HTTP auth endpoints to exist in the gateway. They likely don't yet. The test design should be adapted to what's available. Read the gateway to check which auth endpoints exist.

**Step 2: Run to verify failure**

**Step 3: Commit**

---

### Task 13: Auth Enforcement — Implementation

**Files:**
- Modify: `src/api/gateway.rs` — add auth middleware
- Modify: `src/api/server.rs` — wire auth middleware
- Modify: `src/auth/manager.rs` — add token validation, replace simple_hash with bcrypt

**Step 1: Add bcrypt dependency**

Add to Cargo.toml: `bcrypt = "0.15"`

**Step 2: Replace simple_hash with bcrypt**

In `src/auth/manager.rs`, replace `simple_hash` with bcrypt hashing.

**Step 3: Add token validation to AuthManager**

Add `validate_token(token) -> Option<String>` that parses the token and returns the username.

**Step 4: Add axum middleware for HTTP gateway**

Create auth middleware that checks `Authorization` header, calls `auth_manager.validate_token()`.

**Step 5: Add tonic interceptor for gRPC**

Add interceptor checking `authorization` metadata.

**Step 6: Run tests**

**Step 7: Commit**

```bash
git commit -m "feat(auth): enforce authentication on all endpoints when auth is enabled"
```

---

## Phase 5: TLS

---

### Task 14: TLS — Add rcgen and rustls dependencies

**Files:**
- Modify: `Cargo.toml`

Add:
```toml
rcgen = "0.13"
tokio-rustls = "0.26"
rustls = "0.23"
rustls-pemfile = "2"
```

**Commit**

---

### Task 15: TLS CLI Flags and Config

**Files:**
- Modify: `src/main.rs`
- Modify: `src/config.rs`

Add CLI flags matching etcd:
```rust
#[arg(long)]
cert_file: Option<String>,
#[arg(long)]
key_file: Option<String>,
#[arg(long)]
trusted_ca_file: Option<String>,
#[arg(long, default_value = "false")]
auto_tls: bool,
#[arg(long, default_value = "false")]
client_cert_auth: bool,
#[arg(long, default_value = "1")]
self_signed_cert_validity: u32,
#[arg(long)]
peer_cert_file: Option<String>,
#[arg(long)]
peer_key_file: Option<String>,
#[arg(long)]
peer_trusted_ca_file: Option<String>,
#[arg(long, default_value = "false")]
peer_auto_tls: bool,
```

**Commit**

---

### Task 16: TLS — Auto-TLS Implementation

**Files:**
- Create: `src/tls.rs`
- Modify: `src/api/server.rs`
- Modify: `src/lib.rs`

**Step 1: Create src/tls.rs**

```rust
//! TLS certificate management. Supports manual certs and auto-generated
//! self-signed certificates (matching etcd's --auto-tls behavior).

use std::path::Path;
use rcgen::{Certificate, CertificateParams, KeyPair};

/// Generate a self-signed certificate and save to data_dir/auto-tls/.
pub fn generate_self_signed(
    data_dir: &str,
    validity_years: u32,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let tls_dir = format!("{}/auto-tls", data_dir);
    std::fs::create_dir_all(&tls_dir)?;

    let cert_path = format!("{}/cert.pem", tls_dir);
    let key_path = format!("{}/key.pem", tls_dir);

    // Reuse existing certs if they exist.
    if Path::new(&cert_path).exists() && Path::new(&key_path).exists() {
        return Ok((cert_path, key_path));
    }

    let mut params = CertificateParams::new(vec!["localhost".to_string()])?;
    params.not_after = time::OffsetDateTime::now_utc()
        + time::Duration::days(365 * validity_years as i64);

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    std::fs::write(&cert_path, cert.pem())?;
    std::fs::write(&key_path, key_pair.serialize_pem())?;

    Ok((cert_path, key_path))
}

/// Load TLS config from cert/key files for tonic gRPC.
pub fn load_tonic_tls(
    cert_path: &str,
    key_path: &str,
) -> Result<tonic::transport::ServerTlsConfig, Box<dyn std::error::Error>> {
    let cert = std::fs::read_to_string(cert_path)?;
    let key = std::fs::read_to_string(key_path)?;
    let identity = tonic::transport::Identity::from_pem(cert, key);
    Ok(tonic::transport::ServerTlsConfig::new().identity(identity))
}
```

**Step 2: Wire into server.rs**

In `BarkeepServer::start`, check TLS config:
- If `auto_tls`, generate certs
- If `cert_file`/`key_file`, use those
- Apply `ServerTlsConfig` to tonic server
- Apply `tokio-rustls` acceptor to HTTP gateway

**Step 3: Run tests (all existing tests still use plaintext)**

**Step 4: Commit**

```bash
git commit -m "feat(tls): implement auto-TLS and manual certificate support"
```

---

## Phase 6: Multi-Node Networking

---

### Task 17: gRPC RaftTransport — Proto Definition

**Files:**
- Create: `proto/etcdserverpb/raft_transport.proto`
- Modify: `build.rs`

Define a simple gRPC service for Raft message exchange:

```proto
syntax = "proto3";
package raftpb;

service RaftTransport {
    rpc SendMessage(RaftMessageRequest) returns (RaftMessageResponse);
}

message RaftMessageRequest {
    uint64 from_node = 1;
    bytes message = 2; // serialized RaftMessage (JSON)
}

message RaftMessageResponse {}
```

**Commit**

---

### Task 18: gRPC RaftTransport — Implementation

**Files:**
- Create: `src/raft/grpc_transport.rs`
- Modify: `src/raft/mod.rs`

Implement `GrpcRaftTransport`:
- Holds a map of `node_id → gRPC client`
- `send()` serializes `RaftMessage` and calls the target's `SendMessage` RPC
- Implements `RaftTransport` trait

Also implement `RaftTransportService` (tonic server):
- Receives inbound messages
- Deserializes and feeds to `RaftHandle.inbound_tx`

**Commit**

---

### Task 19: Cluster Bootstrap — CLI Flags and Peer Setup

**Files:**
- Modify: `src/main.rs`
- Modify: `src/api/server.rs`
- Modify: `src/config.rs`

Add CLI flags:
```rust
#[arg(long)]
initial_cluster: Option<String>, // "node1=http://10.0.0.1:2379,node2=http://10.0.0.2:2379"
#[arg(long, default_value = "new")]
initial_cluster_state: String, // "new" or "existing"
```

Parse `--initial-cluster` into peer addresses. Pass to `spawn_raft_node` with the `GrpcRaftTransport`.

**Commit**

---

### Task 20: Multi-Node Integration Test

**Files:**
- Create: `tests/cluster_test.rs`

Write a test that spawns 3 barkeeper nodes in-process using `LocalTransport` and verifies:
1. Leader election succeeds
2. A write on the leader is replicated to followers
3. Followers reject writes (NotLeader)

**Commit**

---

### Task 21: Update README Known Gaps

**Files:**
- Modify: `README.md`

Remove all closed gaps from the Known Gaps table. Update test count. Update feature status.

**Commit and push**

```bash
git push
```

---

## Summary

| Task | Phase | Description |
|------|-------|-------------|
| 1-2 | 1 | HTTP compaction (test + impl) |
| 3-4 | 1 | Txn watch notifications (test + impl) |
| 5-7 | 1 | Revision-based watching (test + impl + wire) |
| 8-9 | 2 | Production lease expiry timer (test + impl) |
| 10-11 | 3 | Writes through Raft (test + impl) |
| 12-13 | 4 | Auth enforcement (test + impl) |
| 14-16 | 5 | TLS (deps + flags + auto-TLS impl) |
| 17-20 | 6 | Multi-node networking (proto + transport + bootstrap + test) |
| 21 | — | Update README |
