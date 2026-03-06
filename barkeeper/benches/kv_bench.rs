use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use barkeeper::kv::store::{KvStore, TxnCompare, TxnCompareTarget, TxnCompareResult, TxnOp};
use tempfile::TempDir;

/// Create a fresh KvStore in a temporary directory.
fn new_store() -> (KvStore, TempDir) {
    let dir = TempDir::new().expect("create temp dir");
    let store = KvStore::open(dir.path().join("bench.db")).expect("open KvStore");
    (store, dir)
}

/// Benchmark: sequential puts to an empty store.
fn bench_put_sequential(c: &mut Criterion) {
    c.bench_function("put_sequential", |b| {
        b.iter_batched(
            || new_store(),
            |(store, _dir)| {
                for i in 0..1000u64 {
                    let key = format!("bench/seq/{}", i);
                    let value = format!("value-{}", i);
                    store.put(key.as_bytes(), value.as_bytes(), 0).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
}

/// Benchmark: range query returning 100 keys.
fn bench_range_100(c: &mut Criterion) {
    c.bench_function("range_100", |b| {
        b.iter_batched(
            || {
                let (store, dir) = new_store();
                // Pre-populate 100 keys under "range/" prefix.
                for i in 0..100u64 {
                    let key = format!("range/{:04}", i);
                    let value = format!("range-val-{}", i);
                    store.put(key.as_bytes(), value.as_bytes(), 0).unwrap();
                }
                (store, dir)
            },
            |(store, _dir)| {
                // Range scan: key="range/" range_end="range0" (prefix scan).
                let result = store
                    .range(b"range/", b"range0", 100, 0)
                    .unwrap();
                assert_eq!(result.kvs.len(), 100);
            },
            BatchSize::PerIteration,
        );
    });
}

/// Benchmark: compare-and-swap transaction.
fn bench_txn_cas(c: &mut Criterion) {
    c.bench_function("txn_cas", |b| {
        b.iter_batched(
            || {
                let (store, dir) = new_store();
                // Seed the key with an initial value.
                store.put(b"txn/key", b"initial", 0).unwrap();
                (store, dir)
            },
            |(store, _dir)| {
                // Run 100 CAS transactions, each reading current value and swapping.
                for i in 0..100u64 {
                    let new_value = format!("cas-{}", i);
                    // Read current value.
                    let range = store.range(b"txn/key", b"", 0, 0).unwrap();
                    let current_value = range.kvs[0].value.clone();

                    let compares = vec![TxnCompare {
                        key: b"txn/key".to_vec(),
                        range_end: vec![],
                        target: TxnCompareTarget::Value(current_value),
                        result: TxnCompareResult::Equal,
                    }];
                    let success = vec![TxnOp::Put {
                        key: b"txn/key".to_vec(),
                        value: new_value.into_bytes(),
                        lease_id: 0,
                    }];
                    let failure = vec![];

                    let result = store.txn(compares, success, failure).unwrap();
                    assert!(result.succeeded);
                }
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(benches, bench_put_sequential, bench_range_100, bench_txn_cas);
criterion_main!(benches);
