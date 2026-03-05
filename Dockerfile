FROM rust:1.93-bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto/ proto/
COPY src/ src/
COPY benches/ benches/

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/barkeeper /usr/local/bin/barkeeper

ENTRYPOINT ["barkeeper"]
