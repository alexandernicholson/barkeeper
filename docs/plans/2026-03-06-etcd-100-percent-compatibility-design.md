# Design: 100% etcd API Compatibility

**Date:** 2026-03-06
**Goal:** Close all 7 remaining etcd compatibility gaps so barkeeper is a true drop-in replacement.

---

## Current State

barkeeper passes 28/30 behavioral tests against etcd 3.5.17. The 2 remaining
differences are instance-specific (member IDs, versions). However, 7 API gaps
exist where features are parsed but not enforced or return stubs.

## Gaps to Close

### 1. Watch Filters (NOPUT / NODELETE)

**Problem:** `WatchCreateRequest.filters` is parsed in `watch_service.rs:65` but
never passed to the watch hub. All events are delivered regardless.

**Design:** Add `filters: Vec<i32>` to `WatchHubCmd::CreateWatch` and the internal
`Watcher` struct. In the `Notify` handler, skip delivery when `event_type` matches
a filter (filter value 0 = NOPUT, 1 = NODELETE). Also apply filters during
historical replay.

**Files:** `src/actors/commands.rs`, `src/watch/actor.rs`, `src/api/watch_service.rs`

### 2. Watch prev_kv on Historical Replay

**Problem:** `WatchCreateRequest.prev_kv` is parsed but ignored. Historical replay
always sets `prev_kv: None`. Live events already pass `prev_kv` through `Notify`.

**Design:** Add `prev_kv: bool` flag to `WatchHubCmd::CreateWatch` and `Watcher`.
For historical replay, the store's `changes_since` already returns revision entries.
Look up the previous revision's value for each key during replay. For live Notify,
only include `prev_kv` in events sent to watchers that requested it.

**Files:** `src/actors/commands.rs`, `src/watch/actor.rs`, `src/api/watch_service.rs`

### 3. Watch watch_id from Client

**Problem:** `WatchCreateRequest.watch_id` is never read. The hub always
auto-assigns sequential IDs starting from 1.

**Design:** Read `create.watch_id` in `watch_service.rs`. If non-zero, pass it
through `CreateWatch` command. The actor checks for collision — if the ID is
already in use, return an error response (created=false, canceled=true,
cancel_reason="watch ID already in use"). If zero, auto-assign as before.

**Files:** `src/actors/commands.rs`, `src/watch/actor.rs`, `src/api/watch_service.rs`

### 4. Watch ProgressRequest

**Problem:** ProgressRequest returns `watch_id: -1` and `revision: 0` — a stub.

**Design:** ProgressRequest should return `watch_id: 0` (etcd convention) and the
current store revision in the header. Add a `GetRevision` command to the KvStore
actor (or reuse existing mechanism). The watch_service needs access to the store
handle or current revision.

**Files:** `src/api/watch_service.rs` (needs store handle or revision source)

### 5. Hash / HashKV

**Problem:** Both RPCs return `hash: 0`.

**Design:** Implement CRC32 hash over sorted KV data, matching etcd's approach.
Add a `hash()` method to `KvStore` that iterates the `kv` BTreeMap in order,
feeding each key and value into a CRC32 hasher. `HashKV` additionally accepts a
`revision` parameter and should hash only entries up to that revision (use the
`revisions` BTreeMap). Return `compact_revision` and `hash_revision` fields.

**Files:** `src/kv/store.rs`, `src/kv/actor.rs`, `src/actors/commands.rs`,
`src/api/maintenance_service.rs`

### 6. JWT Auth Tokens

**Problem:** Tokens use `{username}.{uuid}` format. etcd uses JWT. Most clients
pass tokens opaquely, but some may validate JWT structure.

**Design:** Add `jsonwebtoken` crate. Generate a random HMAC-SHA256 signing key
at auth actor startup (in-memory, not persisted — tokens don't survive restart,
matching etcd's default simple token mode). `Authenticate` returns a JWT with
claims `{sub: username, iat: now, exp: now + 300s}` (5-minute TTL, matching
etcd default). `ValidateToken` decodes and verifies the JWT instead of HashMap
lookup. Remove the `tokens: HashMap<String, String>` from the auth actor.

The interceptor extracts the token from the `Authorization` header (unchanged)
and calls `validate_token` which now does JWT verification.

**Files:** `Cargo.toml`, `src/auth/actor.rs`, `src/auth/interceptor.rs`

### 7. Nested Txn

**Problem:** Nested `Txn` inside a `Txn` request op returns `UNIMPLEMENTED`.

**Design:** Allow one level of nesting (matching etcd's limit). In `kv_service.rs`,
the `convert_request_op` function currently returns `UNIMPLEMENTED` for
`RequestTxn`. Change it to recursively convert the inner txn's compares, success
ops, and failure ops. Execute the inner txn via the existing `txn()` method on
KvStore. Reject depth > 1 with UNIMPLEMENTED. Watch notifications already work
for txn mutations.

**Files:** `src/api/kv_service.rs`, `src/kv/store.rs` (may need recursive txn
helper), `src/kv/actor.rs`

---

## Dependencies

- `jsonwebtoken = "9"` — well-maintained, 50M+ downloads, HMAC-SHA256 support.
  No other new dependencies needed (CRC32 is in Rust stdlib via `std::hash`
  or we use the `crc32fast` crate which is already a transitive dependency).

## Testing Strategy

Each gap gets dedicated unit tests. The existing etcd compatibility test suite
(`bench/compat/`) should be extended with test cases for:
- Watch with filters (NOPUT, NODELETE)
- Watch with prev_kv flag
- Watch with explicit watch_id
- ProgressRequest with real revision
- Hash/HashKV returning non-zero values
- Auth with JWT token format validation
- Nested txn execution

## Implementation Order

1. Watch fixes (gaps 1-4) — all in the watch subsystem, can be done together
2. Hash/HashKV (gap 5) — isolated to KV store + maintenance service
3. JWT auth (gap 6) — isolated to auth actor + interceptor
4. Nested Txn (gap 7) — isolated to KV service + store

Gaps 1-4 can be parallelized. Gaps 5-7 are independent of each other.
