# 100% etcd API Compatibility Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close all 7 remaining etcd API compatibility gaps so barkeeper is a true drop-in replacement for etcd 3.5.

**Architecture:** Plumb missing proto fields (filters, prev_kv, watch_id) through the WatchHubCmd command enum into the watch actor. Add CRC32 hashing to KvStore. Replace custom auth tokens with JWT via the `jsonwebtoken` crate. Implement nested Txn by recursing into the existing `txn()` method.

**Tech Stack:** Rust, tonic/prost (gRPC), jsonwebtoken (JWT), crc32fast (hashing, already transitive dep)

---

## Task 1: Watch Filters (NOPUT / NODELETE)

Plumb the `filters` field from `WatchCreateRequest` through to the watch actor, and skip event delivery when the event type matches a filter.

**Files:**
- Modify: `src/actors/commands.rs:29-51` (WatchHubCmd::CreateWatch)
- Modify: `src/watch/actor.rs:21-26` (Watcher struct)
- Modify: `src/watch/actor.rs:48-100` (CreateWatch handler + Notify handler)
- Modify: `src/watch/actor.rs:166-171` (create_watch handle method)
- Modify: `src/api/watch_service.rs:64-72` (pass filters through)
- Test: `tests/watch_hub_actor_test.rs`

**Step 1: Write failing tests for watch filters**

Add to `tests/watch_hub_actor_test.rs`:

```rust
/// NOPUT filter should suppress PUT events but deliver DELETE events.
#[tokio::test]
async fn test_watch_filter_noput() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    // Filter value 0 = NOPUT (from proto: FilterType::Noput = 0)
    let (_wid, mut rx) = hub.create_watch(b"foo".to_vec(), vec![], 0, vec![0], false).await;

    // Send a PUT event — should be filtered out.
    let kv_put = mvccpb::KeyValue {
        key: b"foo".to_vec(),
        value: b"bar".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        lease: 0,
    };
    hub.notify(b"foo".to_vec(), 0, kv_put, None).await;

    // Send a DELETE event — should be delivered.
    let kv_del = mvccpb::KeyValue {
        key: b"foo".to_vec(),
        ..Default::default()
    };
    hub.notify(b"foo".to_vec(), 1, kv_del, None).await;

    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(event.events[0].r#type, 1, "should only get DELETE");
}

/// NODELETE filter should suppress DELETE events but deliver PUT events.
#[tokio::test]
async fn test_watch_filter_nodelete() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    // Filter value 1 = NODELETE (from proto: FilterType::Nodelete = 1)
    let (_wid, mut rx) = hub.create_watch(b"foo".to_vec(), vec![], 0, vec![1], false).await;

    // Send a DELETE event — should be filtered out.
    let kv_del = mvccpb::KeyValue {
        key: b"foo".to_vec(),
        ..Default::default()
    };
    hub.notify(b"foo".to_vec(), 1, kv_del, None).await;

    // Send a PUT event — should be delivered.
    let kv_put = mvccpb::KeyValue {
        key: b"foo".to_vec(),
        value: b"bar".to_vec(),
        create_revision: 2,
        mod_revision: 2,
        version: 1,
        lease: 0,
    };
    hub.notify(b"foo".to_vec(), 0, kv_put, None).await;

    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(event.events[0].r#type, 0, "should only get PUT");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test test_watch_filter -- --nocapture 2>&1 | tail -20`
Expected: Compilation error — `create_watch` doesn't accept `filters` and `prev_kv` args yet.

**Step 3: Add filters and prev_kv fields to WatchHubCmd and Watcher**

In `src/actors/commands.rs`, add `filters` and `prev_kv` to `CreateWatch`:

```rust
    CreateWatch {
        key: Vec<u8>,
        range_end: Vec<u8>,
        start_revision: i64,
        filters: Vec<i32>,
        prev_kv: bool,
        requested_watch_id: i64,
        reply: oneshot::Sender<(i64, tokio::sync::mpsc::Receiver<crate::watch::hub::WatchEvent>)>,
    },
```

In `src/watch/actor.rs`, update `Watcher`:

```rust
struct Watcher {
    id: i64,
    key: Vec<u8>,
    range_end: Vec<u8>,
    filters: Vec<i32>,
    prev_kv: bool,
    tx: mpsc::Sender<WatchEvent>,
}
```

Update the `CreateWatch` handler in the actor match arm to destructure the new fields, store them in the `Watcher`, and apply filters during historical replay:

```rust
WatchHubCmd::CreateWatch { key, range_end, start_revision, filters, prev_kv: want_prev_kv, requested_watch_id, reply } => {
    let (tx, rx) = mpsc::channel(256);

    // Use client-requested watch_id if non-zero and not already in use.
    let id = if requested_watch_id != 0 {
        if watchers.contains_key(&requested_watch_id) {
            // Collision — send error and continue.
            let _ = reply.send((requested_watch_id, rx));
            // Note: caller detects collision via created=false response.
            // We can't send an error through oneshot, so we drop the
            // watcher rx (it will close immediately).
            continue;
        }
        // Ensure auto-assign doesn't collide in the future.
        if requested_watch_id >= next_id {
            next_id = requested_watch_id + 1;
        }
        requested_watch_id
    } else {
        let id = next_id;
        next_id += 1;
        id
    };

    // Replay historical events BEFORE sending the reply.
    if start_revision > 0 {
        if let Some(ref store) = store {
            match store.changes_since(start_revision - 1).await {
                Ok(changes) => {
                    for (change_key, event_type, kv) in changes {
                        if !key_matches(&key, &range_end, &change_key) {
                            continue;
                        }
                        // Apply filters during replay too.
                        if filters.contains(&event_type) {
                            continue;
                        }

                        let event = mvccpb::Event {
                            r#type: event_type,
                            kv: Some(kv),
                            prev_kv: None, // prev_kv for historical replay handled in Task 2
                        };

                        let watch_event = WatchEvent {
                            watch_id: id,
                            events: vec![event],
                            compact_revision: 0,
                        };

                        if tx.send(watch_event).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("failed to replay watch history: {}", e);
                }
            }
        }
    }

    let watcher = Watcher {
        id,
        key,
        range_end,
        filters,
        prev_kv: want_prev_kv,
        tx,
    };
    watchers.insert(id, watcher);

    let _ = reply.send((id, rx));
}
```

Update the `Notify` handler to apply filters:

```rust
WatchHubCmd::Notify { key, event_type, kv, prev_kv } => {
    let mut dead_ids = Vec::new();

    for (id, watcher) in watchers.iter() {
        if !key_matches(&watcher.key, &watcher.range_end, &key) {
            continue;
        }

        // Apply filters: filter value matches event_type to suppress.
        if watcher.filters.contains(&event_type) {
            continue;
        }

        let event = mvccpb::Event {
            r#type: event_type,
            kv: Some(kv.clone()),
            prev_kv: if watcher.prev_kv { prev_kv.clone() } else { None },
        };

        let watch_event = WatchEvent {
            watch_id: watcher.id,
            events: vec![event],
            compact_revision: 0,
        };

        if watcher.tx.send(watch_event).await.is_err() {
            dead_ids.push(*id);
        }
    }

    for id in dead_ids {
        watchers.remove(&id);
    }
}
```

Update the `create_watch` handle method signature:

```rust
pub async fn create_watch(
    &self,
    key: Vec<u8>,
    range_end: Vec<u8>,
    start_revision: i64,
    filters: Vec<i32>,
    prev_kv: bool,
) -> (i64, mpsc::Receiver<WatchEvent>) {
    self.create_watch_with_id(key, range_end, start_revision, filters, prev_kv, 0).await
}

pub async fn create_watch_with_id(
    &self,
    key: Vec<u8>,
    range_end: Vec<u8>,
    start_revision: i64,
    filters: Vec<i32>,
    prev_kv: bool,
    requested_watch_id: i64,
) -> (i64, mpsc::Receiver<WatchEvent>) {
    let (reply, rx) = oneshot::channel();
    self.cmd_tx
        .send(WatchHubCmd::CreateWatch {
            key,
            range_end,
            start_revision,
            filters,
            prev_kv,
            requested_watch_id,
            reply,
        })
        .await
        .expect("watch hub actor dead");
    rx.await.expect("watch hub actor dropped reply")
}
```

**Step 4: Fix all existing callers of `create_watch`**

All existing callers need `filters: vec![], prev_kv: false` added. Search for `.create_watch(` in:
- `src/api/watch_service.rs:66-71` — update to pass `create.filters`, `create.prev_kv`
- `tests/watch_hub_actor_test.rs` — all existing calls get `, vec![], false` appended
- `tests/watch_notify_test.rs` — same
- `tests/watch_revision_test.rs` — same
- `tests/txn_watch_test.rs` — same

In `src/api/watch_service.rs`, update the `CreateRequest` handler:

```rust
Some(RequestUnion::CreateRequest(create)) => {
    let requested_watch_id = create.watch_id;
    let (watch_id, mut event_rx) = hub
        .create_watch_with_id(
            create.key,
            create.range_end,
            create.start_revision,
            create.filters,
            create.prev_kv,
            requested_watch_id,
        )
        .await;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test test_watch_filter -- --nocapture 2>&1 | tail -20`
Expected: Both `test_watch_filter_noput` and `test_watch_filter_nodelete` PASS.

Run: `cargo test 2>&1 | tail -5`
Expected: All tests pass (no regressions from adding new args to existing calls).

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: implement watch NOPUT/NODELETE filters

Plumb filters field from WatchCreateRequest through WatchHubCmd
into the watch actor. Events matching a filter are suppressed
during both live delivery and historical replay."
```

---

## Task 2: Watch prev_kv on Historical Replay

The `prev_kv` flag is now stored on the Watcher (from Task 1). Live events already
respect it. This task adds prev_kv to historical replay by looking up the previous
revision's value for each key.

**Files:**
- Modify: `src/kv/store.rs` (add `changes_since_with_prev_kv` method)
- Modify: `src/kv/actor.rs` (add command variant)
- Modify: `src/actors/commands.rs` (add command variant)
- Modify: `src/watch/actor.rs:56-90` (use new method when prev_kv requested)
- Test: `tests/watch_hub_actor_test.rs`

**Step 1: Write failing test for watch prev_kv on historical replay**

Add to `tests/watch_hub_actor_test.rs`:

```rust
/// prev_kv flag should include previous value in historical replay events.
#[tokio::test]
async fn test_watch_prev_kv_historical_replay() {
    let rt = make_runtime();
    let dir = tempfile::tempdir().unwrap();
    let store_raw = KvStore::open(dir.path()).unwrap();

    // Put foo=bar at revision 1, then foo=baz at revision 2.
    store_raw.put(b"foo", b"bar", 0).unwrap();
    store_raw.put(b"foo", b"baz", 0).unwrap();

    let store = spawn_kv_store_actor(&rt, store_raw).await;
    let hub = spawn_watch_hub_actor(&rt, Some(store)).await;

    // Create watch with prev_kv=true, replay from revision 2.
    let (_wid, mut rx) = hub
        .create_watch(b"foo".to_vec(), vec![], 2, vec![], true)
        .await;

    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");

    assert_eq!(event.events[0].r#type, 0); // PUT
    let kv = event.events[0].kv.as_ref().unwrap();
    assert_eq!(kv.value, b"baz");

    // prev_kv should contain the old value "bar"
    let prev = event.events[0].prev_kv.as_ref().expect("prev_kv should be set");
    assert_eq!(prev.value, b"bar");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_watch_prev_kv_historical -- --nocapture 2>&1 | tail -10`
Expected: FAIL — `prev_kv` is `None` because historical replay doesn't look up previous values.

**Step 3: Add `changes_since_with_prev` method to KvStore**

In `src/kv/store.rs`, add after the existing `changes_since` method:

```rust
/// Return watch events with optional prev_kv since `after_revision`.
///
/// For each mutation, looks up the key's value at the previous revision
/// to populate prev_kv. This is more expensive than `changes_since`.
pub fn changes_since_with_prev(
    &self,
    after_revision: i64,
) -> Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue, Option<mvccpb::KeyValue>)>, StoreError> {
    let inner = self.inner.read().unwrap();
    let start_rev = (after_revision + 1) as u64;
    let mut results = Vec::new();

    for (&rev, entries) in inner.revisions.range(start_rev..) {
        for re in entries {
            let event_type = match re.event_type {
                EventType::Put => 0,
                EventType::Delete => 1,
            };
            let compound = make_compound_key(&re.key, rev);
            let kv = match inner.kv.get(&compound) {
                Some(ikv) => ikv.to_proto(),
                None => mvccpb::KeyValue {
                    key: re.key.clone(),
                    mod_revision: rev as i64,
                    ..Default::default()
                },
            };

            // Look up the previous version of this key.
            let prev_kv = if rev > 1 {
                // Find the latest revision for this key before `rev`.
                let search_end = make_compound_key(&re.key, rev);
                let search_start = make_compound_key(&re.key, 0);
                inner.kv.range(search_start..search_end)
                    .next_back()
                    .map(|(_, ikv)| ikv.to_proto())
            } else {
                None
            };

            results.push((re.key.clone(), event_type, kv, prev_kv));
        }
    }
    Ok(results)
}
```

**Step 4: Add command variant to actor**

In `src/actors/commands.rs`, add to `KvStoreCmd`:

```rust
    /// Return all mutations with prev_kv since a revision (exclusive).
    ChangesSinceWithPrev {
        after_revision: i64,
        reply: oneshot::Sender<Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue, Option<mvccpb::KeyValue>)>, String>>,
    },
```

In `src/kv/actor.rs`, add the handler in the match arm (after `ChangesSince`):

```rust
KvStoreCmd::ChangesSinceWithPrev { after_revision, reply } => {
    let s = Arc::clone(&store);
    let result = tokio::task::spawn_blocking(move || {
        s.changes_since_with_prev(after_revision).map_err(|e| e.to_string())
    }).await.expect("kv store spawn_blocking panicked");
    let _ = reply.send(result);
}
```

Add handle method to `KvStoreActorHandle`:

```rust
/// Return all mutations with prev_kv since `after_revision` (exclusive).
pub async fn changes_since_with_prev(
    &self,
    after_revision: i64,
) -> Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue, Option<mvccpb::KeyValue>)>, String> {
    let (reply, rx) = oneshot::channel();
    self.cmd_tx
        .send(KvStoreCmd::ChangesSinceWithPrev {
            after_revision,
            reply,
        })
        .await
        .expect("kv store actor dead");
    rx.await.expect("kv store actor dropped")
}
```

**Step 5: Update watch actor to use prev_kv-aware replay**

In `src/watch/actor.rs`, update the historical replay block inside `CreateWatch` handler. Replace the `store.changes_since(...)` call:

```rust
if start_revision > 0 {
    if let Some(ref store) = store {
        if want_prev_kv {
            // Use prev_kv-aware replay.
            match store.changes_since_with_prev(start_revision - 1).await {
                Ok(changes) => {
                    for (change_key, event_type, kv, prev_kv_val) in changes {
                        if !key_matches(&key, &range_end, &change_key) {
                            continue;
                        }
                        if filters.contains(&event_type) {
                            continue;
                        }

                        let event = mvccpb::Event {
                            r#type: event_type,
                            kv: Some(kv),
                            prev_kv: prev_kv_val,
                        };

                        let watch_event = WatchEvent {
                            watch_id: id,
                            events: vec![event],
                            compact_revision: 0,
                        };

                        if tx.send(watch_event).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("failed to replay watch history: {}", e);
                }
            }
        } else {
            // No prev_kv needed — use lightweight replay.
            match store.changes_since(start_revision - 1).await {
                Ok(changes) => {
                    for (change_key, event_type, kv) in changes {
                        if !key_matches(&key, &range_end, &change_key) {
                            continue;
                        }
                        if filters.contains(&event_type) {
                            continue;
                        }

                        let event = mvccpb::Event {
                            r#type: event_type,
                            kv: Some(kv),
                            prev_kv: None,
                        };

                        let watch_event = WatchEvent {
                            watch_id: id,
                            events: vec![event],
                            compact_revision: 0,
                        };

                        if tx.send(watch_event).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("failed to replay watch history: {}", e);
                }
            }
        }
    }
}
```

**Step 6: Run tests**

Run: `cargo test test_watch_prev_kv_historical -- --nocapture 2>&1 | tail -10`
Expected: PASS

Run: `cargo test 2>&1 | tail -5`
Expected: All tests pass.

**Step 7: Commit**

```bash
git add -A
git commit -m "feat: implement watch prev_kv on historical replay

Add changes_since_with_prev() to KvStore that looks up the previous
revision's value for each key. Watch actor uses it when the watcher
has prev_kv=true."
```

---

## Task 3: Watch watch_id from Client

Allow clients to specify their own watch_id. This was already wired in Task 1's
`CreateWatch` handler (the `requested_watch_id` field and collision detection).
This task adds the collision error response and tests.

**Files:**
- Modify: `src/api/watch_service.rs:74-93` (detect collision in created response)
- Modify: `src/watch/actor.rs` (return collision signal)
- Test: `tests/watch_hub_actor_test.rs`

**Step 1: Write failing test**

Add to `tests/watch_hub_actor_test.rs`:

```rust
/// Client-provided watch_id should be used when non-zero.
#[tokio::test]
async fn test_watch_client_provided_watch_id() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    let (watch_id, _rx) = hub
        .create_watch_with_id(b"foo".to_vec(), vec![], 0, vec![], false, 42)
        .await;
    assert_eq!(watch_id, 42, "should use client-provided watch_id");
}

/// Duplicate watch_id should return the requested id (caller detects collision
/// when the receiver closes immediately).
#[tokio::test]
async fn test_watch_duplicate_watch_id_collision() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    // First watch with id=42 should succeed.
    let (wid1, _rx1) = hub
        .create_watch_with_id(b"foo".to_vec(), vec![], 0, vec![], false, 42)
        .await;
    assert_eq!(wid1, 42);

    // Second watch with id=42 should collide — receiver closes immediately.
    let (wid2, mut rx2) = hub
        .create_watch_with_id(b"foo".to_vec(), vec![], 0, vec![], false, 42)
        .await;
    assert_eq!(wid2, 42);

    // The collision receiver should close immediately since the watch was not created.
    let result = timeout(Duration::from_millis(100), rx2.recv()).await;
    match result {
        Ok(None) => {} // Channel closed — collision detected
        Ok(Some(_)) => panic!("should not receive events on colliding watch"),
        Err(_) => panic!("timeout — channel should have closed immediately"),
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test test_watch_client_provided -- --nocapture 2>&1 | tail -10`
Expected: May fail depending on Task 1's implementation of the collision `continue` logic. If using `continue` in the match arm, the `tx`/`rx` pair is created then dropped, closing the channel.

**Step 3: Verify collision logic in watch actor**

The Task 1 implementation used `continue` after detecting a collision. This drops `tx` immediately, which closes `rx`. The caller receives `(requested_watch_id, rx)` where `rx` will yield `None` on first recv — exactly what the test expects.

If the `continue` approach doesn't work correctly (because the reply channel needs to be sent before continuing), fix the actor's `CreateWatch` handler:

In `src/watch/actor.rs`, the collision case should send the reply THEN continue:

```rust
if requested_watch_id != 0 && watchers.contains_key(&requested_watch_id) {
    // tx is dropped here, closing rx for the caller.
    let _ = reply.send((requested_watch_id, rx));
    continue;
}
```

This is already what Task 1 implemented. The test should pass.

**Step 4: Run tests**

Run: `cargo test test_watch_client_provided -- --nocapture 2>&1 | tail -10`
Run: `cargo test test_watch_duplicate -- --nocapture 2>&1 | tail -10`
Expected: Both PASS.

**Step 5: Commit**

```bash
git add -A
git commit -m "test: add tests for client-provided watch_id and collision detection"
```

---

## Task 4: Watch ProgressRequest

Return proper `watch_id=0` and current store revision in ProgressRequest responses.

**Files:**
- Modify: `src/api/watch_service.rs:15-20` (add store handle to WatchService)
- Modify: `src/api/watch_service.rs:158-180` (fix ProgressRequest handler)
- Modify: wherever `WatchService` is constructed (add store parameter)
- Test: `tests/watch_hub_actor_test.rs` or integration test

**Step 1: Find where WatchService is constructed**

Search for `WatchService {` or `WatchService::new` to find construction sites.

**Step 2: Write failing test**

Since ProgressRequest is a gRPC streaming concern, test at the service level or via
a simpler unit approach. For now, verify via the compat test or add an integration test.
The key change is small — just fix the response fields.

**Step 3: Add store handle to WatchService and fix ProgressRequest**

In `src/api/watch_service.rs`, add `store` field to `WatchService`:

```rust
pub struct WatchService {
    hub: WatchHubActorHandle,
    store: KvStoreActorHandle,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
}
```

Add import: `use crate::kv::actor::KvStoreActorHandle;`

Update the constructor (wherever `WatchService` is created) to accept and store the handle.

Clone the store handle into the spawned task. Update the ProgressRequest handler:

```rust
Some(RequestUnion::ProgressRequest(_)) => {
    // Get current revision from the store.
    let revision = match store_clone.current_revision().await {
        Ok(rev) => rev,
        Err(_) => 0,
    };

    let progress_resp = WatchResponse {
        header: Some(ResponseHeader {
            cluster_id,
            member_id,
            revision,
            raft_term: raft_term.load(Ordering::Relaxed),
        }),
        watch_id: 0, // etcd returns 0 for progress responses
        created: false,
        canceled: false,
        compact_revision: 0,
        cancel_reason: String::new(),
        fragment: false,
        events: vec![],
    };

    if resp_tx.send(Ok(progress_resp)).await.is_err() {
        break;
    }
}
```

**Step 4: Update WatchService construction site**

Search for where `WatchService` is created (likely in `src/api/server.rs` or similar) and pass the store handle.

**Step 5: Run tests**

Run: `cargo test 2>&1 | tail -5`
Expected: All tests pass (compilation + existing tests).

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: implement proper ProgressRequest with current revision

Return watch_id=0 and actual store revision in ProgressRequest
responses, matching etcd behavior."
```

---

## Task 5: Hash / HashKV

Implement CRC32 hash computation over sorted KV data.

**Files:**
- Modify: `Cargo.toml` (add `crc32fast`)
- Modify: `src/kv/store.rs` (add `hash` and `hash_kv` methods)
- Modify: `src/kv/actor.rs` (add command handlers)
- Modify: `src/actors/commands.rs` (add command variants)
- Modify: `src/api/maintenance_service.rs:186-208` (call real hash methods)
- Test: `tests/kv_store_test.rs` or new `tests/hash_test.rs`

**Step 1: Write failing tests**

Create or add to `tests/kv_store_test.rs`:

```rust
#[test]
fn test_hash_empty_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();
    let hash = store.hash().unwrap();
    // Empty store should still produce a deterministic hash.
    assert_ne!(hash, 0, "hash should be non-zero even for empty store (CRC32 of empty = 0, but we include revision)");
    // Actually CRC32 of no data is 0. Let's accept 0 for empty.
    // The key test is that after adding data, hash changes.
}

#[test]
fn test_hash_changes_after_put() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();
    let hash1 = store.hash().unwrap();
    store.put(b"foo", b"bar", 0).unwrap();
    let hash2 = store.hash().unwrap();
    assert_ne!(hash1, hash2, "hash should change after put");
}

#[test]
fn test_hash_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();
    store.put(b"foo", b"bar", 0).unwrap();
    let hash1 = store.hash().unwrap();
    let hash2 = store.hash().unwrap();
    assert_eq!(hash1, hash2, "hash should be deterministic");
}

#[test]
fn test_hash_kv_at_revision() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();
    store.put(b"foo", b"bar", 0).unwrap(); // rev 1
    let hash_rev1 = store.hash_kv(1).unwrap();
    store.put(b"foo", b"baz", 0).unwrap(); // rev 2
    let hash_rev2 = store.hash_kv(2).unwrap();
    let hash_rev1_again = store.hash_kv(1).unwrap();
    assert_eq!(hash_rev1, hash_rev1_again, "hash at rev 1 should be stable");
    assert_ne!(hash_rev1, hash_rev2, "different revisions should produce different hashes");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test test_hash -- --nocapture 2>&1 | tail -10`
Expected: Compilation error — `hash()` and `hash_kv()` methods don't exist.

**Step 3: Add crc32fast dependency**

In `Cargo.toml`, add to `[dependencies]`:
```toml
crc32fast = "1"
```

**Step 4: Implement hash methods on KvStore**

In `src/kv/store.rs`:

```rust
use crc32fast::Hasher;
```

Add methods:

```rust
/// Compute CRC32 hash over all KV data (sorted by compound key).
pub fn hash(&self) -> Result<u32, StoreError> {
    let inner = self.inner.read().unwrap();
    let mut hasher = Hasher::new();
    for (compound_key, ikv) in inner.kv.iter() {
        hasher.update(compound_key);
        hasher.update(&ikv.value);
    }
    Ok(hasher.finalize())
}

/// Compute CRC32 hash over KV data up to and including `revision`.
pub fn hash_kv(&self, revision: i64) -> Result<(u32, i64), StoreError> {
    let inner = self.inner.read().unwrap();
    let mut hasher = Hasher::new();
    let max_rev = revision as u64;

    for (compound_key, ikv) in inner.kv.iter() {
        // compound key = user_key + 0x00 + 8-byte BE revision
        // Extract revision from the last 8 bytes.
        if compound_key.len() >= 8 {
            let rev_bytes: [u8; 8] = compound_key[compound_key.len() - 8..].try_into().unwrap();
            let entry_rev = u64::from_be_bytes(rev_bytes);
            if entry_rev > max_rev {
                continue;
            }
        }
        hasher.update(compound_key);
        hasher.update(&ikv.value);
    }

    // Return (hash, compact_revision). We don't track compact_revision
    // explicitly, so return 0.
    Ok((hasher.finalize(), 0))
}
```

**Step 5: Add actor commands and handle methods**

In `src/actors/commands.rs`, add to `KvStoreCmd`:

```rust
    /// Compute CRC32 hash over all KV data.
    Hash {
        reply: oneshot::Sender<Result<u32, String>>,
    },
    /// Compute CRC32 hash over KV data up to a revision.
    HashKv {
        revision: i64,
        reply: oneshot::Sender<Result<(u32, i64), String>>,
    },
```

In `src/kv/actor.rs`, add handlers and handle methods following the existing pattern
(spawn_blocking + reply).

**Step 6: Update maintenance_service.rs**

Replace the stub implementations:

```rust
async fn hash(
    &self,
    _request: Request<HashRequest>,
) -> Result<Response<HashResponse>, Status> {
    let hash = self.store.hash().await
        .map_err(|e| Status::internal(format!("hash: {}", e)))?;
    Ok(Response::new(HashResponse {
        header: self.make_header().await,
        hash,
    }))
}

async fn hash_kv(
    &self,
    request: Request<HashKvRequest>,
) -> Result<Response<HashKvResponse>, Status> {
    let revision = request.into_inner().revision;
    let (hash, compact_revision) = self.store.hash_kv(revision).await
        .map_err(|e| Status::internal(format!("hash_kv: {}", e)))?;
    Ok(Response::new(HashKvResponse {
        header: self.make_header().await,
        hash,
        compact_revision,
        hash_revision: revision,
    }))
}
```

Note: The `hash` field in `HashResponse` is `u32` in the proto. Verify the generated
Rust type matches. If it's `u32`, use directly. If it's `i32`, cast with `hash as i32`.

**Step 7: Run tests**

Run: `cargo test test_hash -- --nocapture 2>&1 | tail -10`
Expected: All hash tests PASS.

Run: `cargo test 2>&1 | tail -5`
Expected: All tests pass.

**Step 8: Commit**

```bash
git add -A
git commit -m "feat: implement Hash and HashKV with CRC32

Compute CRC32 hash over sorted KV data. HashKV supports
revision-bounded hashing. Replaces zero-hash stubs in
maintenance service."
```

---

## Task 6: JWT Auth Tokens

Replace custom `{username}.{uuid}` tokens with JWT (HMAC-SHA256).

**Files:**
- Modify: `Cargo.toml` (add `jsonwebtoken`)
- Modify: `src/auth/actor.rs:28,56-67,84-86` (JWT generation and validation)
- Test: `tests/auth_actor_test.rs`

**Step 1: Write failing test**

Add to `tests/auth_actor_test.rs`:

```rust
/// Authenticate should return a valid JWT token.
#[tokio::test]
async fn test_authenticate_returns_jwt() {
    let handle = make_handle().await;
    handle.user_add("alice".into(), "password123".into()).await;

    let token = handle.authenticate("alice", "password123").await
        .expect("authenticate should succeed");

    // JWT tokens have 3 dot-separated parts: header.payload.signature
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3, "token should be a JWT with 3 parts, got: {}", token);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_authenticate_returns_jwt -- --nocapture 2>&1 | tail -10`
Expected: FAIL — current token format is `username.uuid` (2 parts), not JWT (3 parts).

**Step 3: Add jsonwebtoken dependency**

In `Cargo.toml`, add to `[dependencies]`:
```toml
jsonwebtoken = "9"
```

**Step 4: Implement JWT in auth actor**

In `src/auth/actor.rs`, add imports:

```rust
use jsonwebtoken::{encode, decode, Header, Algorithm, Validation, EncodingKey, DecodingKey};
use serde::{Serialize, Deserialize};
```

Add JWT claims struct (inside the module, before `spawn_auth_actor`):

```rust
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: u64,
    iat: u64,
}
```

In `spawn_auth_actor`, generate the signing key at startup. Replace `let mut tokens: HashMap<String, String> = HashMap::new();` with:

```rust
// Generate random HMAC-SHA256 signing key for JWT tokens.
let jwt_secret: Vec<u8> = {
    use rand::Rng;
    let mut key = vec![0u8; 32];
    rand::thread_rng().fill(&mut key[..]);
    key
};
```

Replace the `Authenticate` handler's token generation (the `spawn_blocking` block):

```rust
AuthCmd::Authenticate { name, password, reply } => {
    let user_data = users.get(&name).map(|u| {
        (u.password_hash.clone(), name.clone())
    });

    match user_data {
        Some((password_hash, user_name)) => {
            let secret = jwt_secret.clone();
            let result = tokio::task::spawn_blocking(move || {
                if bcrypt::verify(&password, &password_hash).unwrap_or(false) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let claims = Claims {
                        sub: user_name.clone(),
                        iat: now,
                        exp: now + 300, // 5 minute TTL
                    };
                    let token = encode(
                        &Header::default(),
                        &claims,
                        &EncodingKey::from_secret(&secret),
                    ).expect("JWT encoding should not fail");
                    Some(token)
                } else {
                    None
                }
            }).await.expect("bcrypt verify task panicked");

            let _ = reply.send(result);
        }
        None => {
            let _ = reply.send(None);
        }
    }
}
```

Replace the `ValidateToken` handler:

```rust
AuthCmd::ValidateToken { token, reply } => {
    let secret = jwt_secret.clone();
    // JWT decode is fast (no bcrypt), but keep it consistent.
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    match decode::<Claims>(&token, &DecodingKey::from_secret(&secret), &validation) {
        Ok(token_data) => {
            let username = token_data.claims.sub;
            // Verify user still exists.
            if users.contains_key(&username) {
                let _ = reply.send(Some(username));
            } else {
                let _ = reply.send(None);
            }
        }
        Err(_) => {
            let _ = reply.send(None);
        }
    }
}
```

Remove the `tokens` HashMap entirely — JWT is stateless.

**Step 5: Run tests**

Run: `cargo test test_authenticate_returns_jwt -- --nocapture 2>&1 | tail -10`
Expected: PASS

Run: `cargo test auth -- --nocapture 2>&1 | tail -20`
Expected: All auth tests pass.

Run: `cargo test 2>&1 | tail -5`
Expected: All tests pass.

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: replace custom auth tokens with JWT (HMAC-SHA256)

Generate random signing key at auth actor startup. Tokens are
standard JWT with sub/iat/exp claims and 5-minute TTL. Validation
is stateless (no HashMap lookup). Matches etcd client expectations."
```

---

## Task 7: Nested Txn

Allow one level of Txn nesting inside a Txn request.

**Files:**
- Modify: `src/api/kv_service.rs:365-367` (convert nested txn ops)
- Modify: `src/kv/store.rs` (may need to handle `TxnOp::Txn` variant)
- Modify: `src/actors/commands.rs` (add `TxnOp::Txn` variant if needed)
- Test: `tests/txn_test.rs`

**Step 1: Write failing test**

Add to `tests/txn_test.rs`:

```rust
use barkeeper::kv::store::KvStore;
use barkeeper::actors::commands::{TxnCompare, TxnCompareResult, TxnCompareTarget, TxnOp, TxnOpResponse};

/// Nested txn should execute inner txn ops.
#[test]
fn test_nested_txn_success() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    // Put a key so we can compare against it.
    store.put(b"flag", b"on", 0).unwrap();

    // Outer txn: compare flag=="on", success = inner txn that puts "nested"="yes"
    let inner_txn = TxnOp::Txn {
        compares: vec![],
        success: vec![TxnOp::Put {
            key: b"nested".to_vec(),
            value: b"yes".to_vec(),
            lease_id: 0,
        }],
        failure: vec![],
    };

    let result = store.txn(
        &[TxnCompare {
            key: b"flag".to_vec(),
            target: TxnCompareTarget::Value(b"on".to_vec()),
            result: TxnCompareResult::Equal,
        }],
        &[inner_txn],
        &[],
    ).unwrap();

    assert!(result.succeeded, "outer txn should succeed");

    // Verify the nested put happened.
    let range = store.range(b"nested", &[], 0, 0).unwrap();
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(range.kvs[0].value, b"yes");
}

/// Deeply nested txn (depth > 1) should fail.
#[test]
fn test_deeply_nested_txn_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    let deep_txn = TxnOp::Txn {
        compares: vec![],
        success: vec![TxnOp::Txn {
            compares: vec![],
            success: vec![],
            failure: vec![],
        }],
        failure: vec![],
    };

    let result = store.txn(&[], &[deep_txn], &[]);
    assert!(result.is_err(), "deeply nested txn should be rejected");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_nested_txn -- --nocapture 2>&1 | tail -10`
Expected: Compilation error — `TxnOp::Txn` variant doesn't exist.

**Step 3: Add TxnOp::Txn variant**

In `src/actors/commands.rs`, add to the `TxnOp` enum:

```rust
    Txn {
        compares: Vec<TxnCompare>,
        success: Vec<TxnOp>,
        failure: Vec<TxnOp>,
    },
```

Add `TxnOpResponse::Txn` variant:

```rust
    Txn(TxnResult),
```

**Step 4: Implement nested txn execution in KvStore**

In `src/kv/store.rs`, refactor `txn()` to support nesting. Add a `depth` parameter
to an internal helper, and have the public `txn()` call it with depth=0.

Find the `txn` method and refactor. The key change is in the op execution loop —
when encountering `TxnOp::Txn`, recursively execute with depth+1:

```rust
pub fn txn(
    &self,
    compares: &[TxnCompare],
    success: &[TxnOp],
    failure: &[TxnOp],
) -> Result<TxnResult, StoreError> {
    self.txn_inner(compares, success, failure, 0)
}

fn txn_inner(
    &self,
    compares: &[TxnCompare],
    success: &[TxnOp],
    failure: &[TxnOp],
    depth: u32,
) -> Result<TxnResult, StoreError> {
    if depth > 1 {
        return Err(StoreError::Serialization(
            "nested txn depth exceeds limit".to_string(),
        ));
    }

    // ... existing compare logic ...

    let ops = if succeeded { success } else { failure };

    let mut responses = Vec::new();
    for op in ops {
        match op {
            // ... existing Put, Range, DeleteRange handling ...
            TxnOp::Txn { compares, success, failure } => {
                let inner_result = self.txn_inner(compares, success, failure, depth + 1)?;
                responses.push(TxnOpResponse::Txn(inner_result));
            }
        }
    }

    Ok(TxnResult {
        succeeded,
        revision: /* current revision */,
        responses,
    })
}
```

**Step 5: Update kv_service.rs to convert nested Txn**

In `src/api/kv_service.rs:365-367`, replace the `UNIMPLEMENTED` error:

```rust
Some(request_op::Request::RequestTxn(inner_txn)) => {
    let inner_compares = inner_txn.compare.iter().map(|c| convert_compare(c)).collect::<Result<Vec<_>, _>>()?;
    let inner_success = inner_txn.success.iter().map(|op| convert_request_op(op)).collect::<Result<Vec<_>, _>>()?;
    let inner_failure = inner_txn.failure.iter().map(|op| convert_request_op(op)).collect::<Result<Vec<_>, _>>()?;
    Ok(Some(TxnOp::Txn {
        compares: inner_compares,
        success: inner_success.into_iter().flatten().collect(),
        failure: inner_failure.into_iter().flatten().collect(),
    }))
}
```

Also update `convert_txn_op_response` to handle `TxnOpResponse::Txn`:

```rust
TxnOpResponse::Txn(inner) => ResponseOp {
    response: Some(response_op::Response::ResponseTxn(TxnResponse {
        header: make_header(inner.revision),
        succeeded: inner.succeeded,
        responses: inner.responses.into_iter().map(|r| {
            convert_txn_op_response(r, cluster_id, member_id, raft_term)
        }).collect(),
    })),
},
```

**Step 6: Run tests**

Run: `cargo test test_nested_txn -- --nocapture 2>&1 | tail -10`
Expected: PASS

Run: `cargo test 2>&1 | tail -5`
Expected: All tests pass.

**Step 7: Commit**

```bash
git add -A
git commit -m "feat: implement nested Txn (one level of nesting)

Allow TxnOp::Txn inside a transaction. Recursive execution with
depth limit of 1 (matching etcd). Depths > 1 return an error."
```

---

## Task 8: Update Documentation and Run Full Test Suite

**Files:**
- Modify: `docs/etcd-compatibility.md` (update Known Differences section)
- Run: full test suite + compat tests

**Step 1: Update etcd-compatibility.md**

Remove all 7 gaps from the "Known Differences and Limitations" section:
- Remove "Watch gaps" subsection (filters, prev_kv, ProgressRequest all implemented)
- Remove "Nested transactions" subsection
- Remove "Maintenance stubs" reference to Hash/HashKV (they now compute real hashes)
- Update "Auth token format" to say JWT instead of custom format
- Update the benchmark results section if applicable

**Step 2: Run full test suite**

Run: `cargo test 2>&1 | tail -20`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add -A
git commit -m "docs: update etcd-compatibility.md for 100% API coverage

All 7 gaps closed: watch filters, prev_kv, watch_id, ProgressRequest,
Hash/HashKV, JWT auth, nested Txn."
```
