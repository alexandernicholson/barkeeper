# barkeeper

An etcd-compatible distributed key-value store built on the [Rebar](https://github.com/alexandernicholson/rebar) actor runtime.

## Overview

barkeeper implements the etcd v3 gRPC API, providing a drop-in replacement for etcd powered by:

- **Rebar** — BEAM-inspired distributed actor runtime for Rust
- **Raft consensus** — built from scratch as Rebar actors
- **redb** — pure Rust embedded storage (no C dependencies)

## Status

Early development.

## License

Apache License 2.0 — see [LICENSE](LICENSE)

The protobuf definitions in `proto/` are vendored from [etcd](https://github.com/etcd-io/etcd) and are also licensed under Apache 2.0.
