# bkctl

Terminal UI for exploring barkeeper clusters. Browse keys, monitor cluster status, and watch events in real time.

Built with [Ratatui](https://ratatui.rs/) and connects via gRPC.

## Build

```bash
cargo build --release -p bkctl
```

The binary is at `target/release/bkctl`.

## Usage

```bash
# Connect to a local barkeeper instance
bkctl

# Connect to a specific endpoint
bkctl --endpoints http://10.0.0.1:2379

# Start browsing from a specific prefix
bkctl --prefix /registry/
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoints` | `http://127.0.0.1:2379` | gRPC endpoint |
| `--prefix` | `/` | Initial browse/watch prefix |

## Interface

bkctl has two tabs, switched with `Tab`:

### Dashboard

- Cluster status (leader, Raft term, node count)
- Members table (ID, name, leader/follower)
- Live watch event stream (PUT/DELETE with key and value size)

### Keys

Split pane layout:

- **Left**: prefix tree browser. Directories shown in cyan, leaf keys in white. Navigate with `j`/`k` or arrows, `Enter` to descend into a prefix, `Backspace` to go up.
- **Right**: value viewer for the selected key. JSON values are pretty-printed. Shows key metadata (revision, mod revision, version, lease).

## Keybindings

| Key | Action |
|-----|--------|
| `Tab` | Switch between Dashboard and Keys tabs |
| `j` / `Down` | Move selection down |
| `k` / `Up` | Move selection up |
| `Enter` | Descend into prefix / fetch key value |
| `Backspace` | Go up one level |
| `p` | Open put dialog (Keys tab) |
| `d` | Open delete confirmation (Keys tab) |
| `r` | Refresh data |
| `q` / `Esc` | Quit |
| `Ctrl+C` | Quit |

## Architecture

Three-layer design with full test coverage:

```
┌──────────────┐
│   main.rs    │  Event loop, terminal setup, async refresh
├──────────────┤
│   ui.rs      │  Stateless Ratatui rendering (Dashboard + Keys)
├──────────────┤
│   app.rs     │  Pure state/model (no I/O, no async)
├──────────────┤
│  client.rs   │  gRPC wrapper (tonic client stubs)
├──────────────┤
│  event.rs    │  Crossterm key → state transition mapping
└──────────────┘
```

## Testing

```bash
# Run all bkctl tests
cargo test -p bkctl
```

28 tests across 4 test files:

| File | Tests | What it covers |
|------|-------|----------------|
| `tests/app_test.rs` | 11 | Pure state transitions (no I/O) |
| `tests/client_test.rs` | 7 | gRPC RPCs against a real barkeeper |
| `tests/ui_test.rs` | 5 | Ratatui rendering via TestBackend |
| `tests/e2e_test.rs` | 5 | Full flows: put → browse → select → read value |

Client and e2e tests start a real barkeeper instance (Raft + state machine + gRPC server) on random ports using `portpicker`.
