# Design: bkctl — Barkeeper TUI

**Date:** 2026-03-06

**Goal:** A standalone TUI for exploring barkeeper clusters — browse keys, monitor cluster status, and watch events in real time.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Packaging | Separate crate (`bkctl/`) | Keeps server binary lean |
| Protocol | gRPC | Native streaming for Watch, matches etcdctl |
| Framework | Ratatui + crossterm | Most popular Rust TUI, active ecosystem |
| Binary name | `bkctl` | Short, easy to type |

## Architecture

Separate crate in the barkeeper repo at `bkctl/`. Own `Cargo.toml`, own binary. Shares proto definitions from `proto/` to generate gRPC client stubs via `tonic-build` with `.build_client(true)`.

### Dependencies

- `ratatui` + `crossterm` — TUI rendering
- `tonic` + `prost` — gRPC client (generated from existing protos)
- `tokio` — async runtime
- `clap` — CLI args

### Layered Design

Three layers, each independently testable:

1. **Client layer** (`client.rs`) — thin async wrapper around tonic gRPC stubs
2. **State/model layer** (`app.rs`) — all TUI state as pure data + transition functions
3. **Rendering layer** (`ui.rs`) — Ratatui widget drawing, stateless functions over `&App`

## CLI Interface

```
bkctl [OPTIONS]

Options:
  --endpoints <URLS>   Comma-separated gRPC endpoints [default: 127.0.0.1:2379]
  --prefix <PREFIX>    Initial watch/browse prefix [default: /]
```

## Tab 1: Dashboard

```
┌─ Dashboard ──────────────────────────────────────────┐
│ Cluster: 3 nodes │ Leader: barkeeper-0 │ Raft term: 42│
├──────────────────────────────────────────────────────┤
│ Members                                              │
│  ID  │ Name         │ Status  │ Peer URL             │
│  1   │ barkeeper-0  │ Leader  │ http://10.0.0.1:2381 │
│  2   │ barkeeper-1  │ Follower│ http://10.0.0.2:2381 │
│  3   │ barkeeper-2  │ Follower│ http://10.0.0.3:2381 │
├──────────────────────────────────────────────────────┤
│ Watch Events (live)                    [prefix: /]   │
│  12:03:01 PUT  /registry/pods/nginx-abc  (1.2KB)    │
│  12:03:01 PUT  /registry/pods/nginx-def  (1.1KB)    │
│  12:03:02 DEL  /registry/leases/kube-sch             │
│  12:03:02 PUT  /registry/events/default  (0.8KB)    │
│  ...auto-scrolling...                                │
└──────────────────────────────────────────────────────┘
```

- **Top bar:** cluster summary from `Maintenance.Status` + `Cluster.MemberList` (polled every 2s)
- **Members table:** from `MemberList` RPC
- **Watch events:** live stream via `Watch` gRPC bidirectional streaming, prefix-filterable

## Tab 2: Keys

```
┌─ Keys ───────────────────────────────────────────────┐
│ Path: /registry/pods/                    [123 keys]  │
├──────────────┬───────────────────────────────────────┤
│ Prefixes     │ Value                                 │
│  default/    │ Key: /registry/pods/default/nginx-abc │
│  kube-system/│ Rev: 4521  Mod: 4519  Ver: 3         │
│  monitoring/ │ Lease: 0                              │
│              │                                       │
│ Keys         │ {                                     │
│ > nginx-abc  │   "apiVersion": "v1",                 │
│   nginx-def  │   "kind": "Pod",                      │
│   nginx-ghi  │   "metadata": {                       │
│              │     "name": "nginx-abc",               │
│              │     ...                                │
│              │   }                                    │
│              │ }                                      │
├──────────────┴───────────────────────────────────────┤
│ [p]ut [d]elete [/]search [r]efresh [q]uit            │
└──────────────────────────────────────────────────────┘
```

- **Left pane:** prefix tree navigation (like a file browser). `Enter` to descend, `Backspace` to go up.
- **Right pane:** selected key's metadata + value. Auto-detects JSON and pretty-prints.
- **Bottom:** key bindings. `p` opens put dialog, `d` deletes with confirmation.

## Keybindings

| Key | Action |
|-----|--------|
| `Tab` | Switch between Dashboard / Keys tabs |
| `j/k` or arrows | Navigate lists |
| `Enter` | Descend into prefix / select key |
| `Backspace` | Go up one prefix level |
| `p` | Put key (opens input dialog) |
| `d` | Delete key (with confirmation) |
| `/` | Search/filter keys |
| `r` | Refresh current view |
| `w` | Change watch prefix (Dashboard tab) |
| `q` / `Esc` | Quit |

## Testing Harness

All tests run against a real barkeeper instance — no server-level mocks.

### TestServer utility

Shared test infrastructure that starts a real barkeeper on a random port with a temp data dir. Returns the gRPC endpoint. Cleans up on drop.

```rust
struct TestServer {
    endpoint: String,
    _dir: TempDir,
    // server task handle for cleanup
}

impl TestServer {
    async fn start() -> Self { /* ... */ }
    fn endpoint(&self) -> &str { &self.endpoint }
}
```

### Layer 1: Client tests

Test the gRPC client wrapper against a real server. Cover all RPCs.

```rust
#[tokio::test]
async fn test_client_put_get() {
    let server = TestServer::start().await;
    let client = BkClient::connect(server.endpoint()).await.unwrap();
    client.put(b"key", b"val").await.unwrap();
    let kv = client.get(b"key").await.unwrap();
    assert_eq!(kv.value, b"val");
}
```

Tests: KV put/get/delete/prefix, Watch stream receives events, Lease grant/revoke/timetolive, Cluster member list, Maintenance status, connection error handling.

### Layer 2: State/model tests

Test all TUI state transitions as pure functions. No rendering, no I/O, no server.

```rust
#[test]
fn test_key_navigation_descend() {
    let mut app = App::new();
    app.set_keys(vec!["/a/1", "/a/2", "/b/1"]);
    app.set_prefix("/");
    assert_eq!(app.visible_prefixes(), vec!["a/", "b/"]);
    app.descend("a/");
    assert_eq!(app.current_prefix(), "/a/");
    assert_eq!(app.visible_keys(), vec!["1", "2"]);
}
```

Tests: tab switching, prefix navigation (descend/ascend), key selection, put/delete confirmation dialogs, watch event buffer (append, scroll, capacity limit), search filtering, scroll state tracking.

### Layer 3: Rendering tests

Use Ratatui's `TestBackend` to render to an in-memory buffer and assert on output.

```rust
#[test]
fn test_dashboard_renders_members() {
    let mut app = App::new();
    app.set_members(vec![member(1, "node-0", true), member(2, "node-1", false)]);
    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw_dashboard(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);
    assert!(content.contains("node-0"));
    assert!(content.contains("Leader"));
}
```

Tests: dashboard layout (members table, cluster summary), key explorer layout (prefix list, value pane, metadata), tab bar active indicator, status line, dialog rendering (put input, delete confirmation).

### Layer 4: End-to-end tests

Full integration: start barkeeper, connect the App model (no terminal), drive through scripted user flows.

```rust
#[tokio::test]
async fn test_e2e_put_and_browse() {
    let server = TestServer::start().await;
    let mut app = App::connect(server.endpoint()).await.unwrap();

    // Switch to Keys tab, put a key
    app.handle_key(KeyCode::Tab);
    app.handle_key(KeyCode::Char('p'));
    app.input_key("test/hello");
    app.input_value("world");
    app.confirm();
    app.tick().await; // process async responses

    // Verify it appears in the key list
    assert!(app.visible_keys().contains(&"hello".to_string()));

    // Select it, verify value panel
    app.handle_key(KeyCode::Enter);
    assert_eq!(app.selected_value(), Some(b"world".as_ref()));

    // Switch to dashboard, verify watch caught the PUT
    app.handle_key(KeyCode::Tab);
    assert!(app.watch_events().iter().any(|e| e.key == b"test/hello"));
}
```

Tests: browse → put → verify in key list, put → watch event appears on dashboard, delete with confirmation → key removed, prefix navigation across multiple levels, search filtering then selecting result, tab switching preserves state.

### Test coverage summary

| Layer | What | Server needed |
|-------|------|---------------|
| Client | All gRPC RPCs | Yes (real barkeeper) |
| State/model | All state transitions | No |
| Rendering | All UI layouts | No |
| End-to-end | Full user flows | Yes (real barkeeper) |
