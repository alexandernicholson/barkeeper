use std::io::Write;
use tempfile::tempdir;

use barkeeper::kv::state_machine::KvCommand;
use barkeeper::kv::store::KvStore;
use barkeeper::kv::wal_replay::replay_wal;
use barkeeper::raft::log_store::LogStore;
use barkeeper::raft::messages::{LogEntry, LogEntryData};

/// Helper: create a LogEntry containing a KvCommand::Put serialized with bincode.
fn put_entry(index: u64, term: u64, key: &str, value: &str, revision: i64) -> LogEntry {
    let cmd = KvCommand::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        lease_id: 0,
    };
    LogEntry {
        term,
        index,
        data: LogEntryData::Command {
            data: bincode::serialize(&cmd).unwrap(),
            revision,
        },
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// A. Crash Recovery
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_torn_write_recovery() {
    let dir = tempdir().unwrap();

    // Write one valid entry.
    {
        let store = LogStore::open(dir.path()).unwrap();
        store.append(&[put_entry(1, 1, "k1", "v1", 1)]).unwrap();
        assert_eq!(store.len().unwrap(), 1);
    }

    // Manually append a torn frame: u32 length prefix claiming 1000 bytes, but only 10 bytes.
    {
        let wal_path = dir.path().join("raft.wal");
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        let fake_len: u32 = 1000;
        f.write_all(&fake_len.to_le_bytes()).unwrap();
        f.write_all(&[0xAB; 10]).unwrap();
        f.flush().unwrap();
    }

    // Reopen — only the valid entry should survive.
    {
        let store = LogStore::open(dir.path()).unwrap();
        assert_eq!(store.len().unwrap(), 1);
        assert_eq!(store.last_index().unwrap(), 1);

        // We can still append after recovery.
        store.append(&[put_entry(2, 1, "k2", "v2", 2)]).unwrap();
        assert_eq!(store.len().unwrap(), 2);
        assert_eq!(store.last_index().unwrap(), 2);
    }
}

#[test]
fn test_corrupt_payload_recovery() {
    let dir = tempdir().unwrap();

    // Write one valid entry.
    {
        let store = LogStore::open(dir.path()).unwrap();
        store.append(&[put_entry(1, 1, "k1", "v1", 1)]).unwrap();
    }

    // Append a frame with valid length (64 bytes) but garbage payload.
    {
        let wal_path = dir.path().join("raft.wal");
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        let payload_len: u32 = 64;
        f.write_all(&payload_len.to_le_bytes()).unwrap();
        f.write_all(&[0xFF; 64]).unwrap();
        f.flush().unwrap();
    }

    // Reopen — only the valid entry should survive.
    {
        let store = LogStore::open(dir.path()).unwrap();
        assert_eq!(store.len().unwrap(), 1);
        assert_eq!(store.last_index().unwrap(), 1);
        let entry = store.get(1).unwrap().unwrap();
        assert_eq!(entry.index, 1);
    }
}

#[test]
fn test_large_wal_reopen() {
    let dir = tempdir().unwrap();

    {
        let store = LogStore::open(dir.path()).unwrap();
        let entries: Vec<LogEntry> = (1..=1000)
            .map(|i| put_entry(i, 1, &format!("k{}", i), &format!("v{}", i), i as i64))
            .collect();
        store.append(&entries).unwrap();
    }

    // Reopen and verify.
    {
        let store = LogStore::open(dir.path()).unwrap();
        assert_eq!(store.len().unwrap(), 1000);
        assert_eq!(store.last_index().unwrap(), 1000);

        // Spot-check entries at index 1, 500, 1000.
        let e1 = store.get(1).unwrap().unwrap();
        assert_eq!(e1.index, 1);
        let e500 = store.get(500).unwrap().unwrap();
        assert_eq!(e500.index, 500);
        let e1000 = store.get(1000).unwrap().unwrap();
        assert_eq!(e1000.index, 1000);

        // Range check.
        let range = store.get_range(990, 1000).unwrap();
        assert_eq!(range.len(), 11);
    }
}

#[test]
fn test_truncate_to_zero() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    store
        .append(&[
            put_entry(1, 1, "a", "1", 1),
            put_entry(2, 1, "b", "2", 2),
            put_entry(3, 1, "c", "3", 3),
        ])
        .unwrap();

    store.truncate_after(0).unwrap();
    assert_eq!(store.len().unwrap(), 0);
    assert_eq!(store.last_index().unwrap(), 0);
    assert!(store.get(1).unwrap().is_none());

    // Append after truncate-to-zero should work.
    store.append(&[put_entry(1, 2, "x", "y", 1)]).unwrap();
    assert_eq!(store.len().unwrap(), 1);
    assert_eq!(store.last_index().unwrap(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════════
// B. Snapshot + WAL Replay
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_wal_replay_no_snapshot() {
    let dir = tempdir().unwrap();

    let log_store = LogStore::open(dir.path()).unwrap();
    let entries: Vec<LogEntry> = (1..=5)
        .map(|i| put_entry(i, 1, &format!("key{}", i), &format!("val{}", i), i as i64))
        .collect();
    log_store.append(&entries).unwrap();

    let kv_store = KvStore::open(dir.path()).unwrap();
    let applied = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied, 5);

    // Verify all 5 keys are present.
    for i in 1..=5 {
        let key = format!("key{}", i);
        let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "key {} should exist", key);
    }
}

#[test]
fn test_wal_replay_with_snapshot() {
    let dir = tempdir().unwrap();

    let log_store = LogStore::open(dir.path()).unwrap();
    let entries: Vec<LogEntry> = (1..=10)
        .map(|i| put_entry(i, 1, &format!("key{}", i), &format!("val{}", i), i as i64))
        .collect();
    log_store.append(&entries).unwrap();

    // Apply first 5 entries via batch_apply_with_index, then snapshot.
    {
        let kv_store = KvStore::open(dir.path()).unwrap();
        let cmds: Vec<(KvCommand, i64)> = (1..=5)
            .map(|i| {
                (
                    KvCommand::Put {
                        key: format!("key{}", i).into_bytes(),
                        value: format!("val{}", i).into_bytes(),
                        lease_id: 0,
                    },
                    i as i64,
                )
            })
            .collect();
        kv_store.batch_apply_with_index(&cmds, 5).unwrap();
        kv_store.snapshot().unwrap();
    }

    // Reopen KvStore from snapshot, then replay WAL for entries 6-10.
    {
        let kv_store = KvStore::open(dir.path()).unwrap();
        assert_eq!(kv_store.last_applied_raft_index().unwrap(), 5);

        let applied = replay_wal(&log_store, &kv_store).unwrap();
        assert_eq!(applied, 5);

        // All 10 keys should be present.
        for i in 1..=10 {
            let key = format!("key{}", i);
            let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
            assert_eq!(result.kvs.len(), 1, "key {} should exist", key);
        }
    }
}

#[test]
fn test_wal_replay_idempotency() {
    let dir = tempdir().unwrap();

    let log_store = LogStore::open(dir.path()).unwrap();
    log_store
        .append(&[put_entry(1, 1, "only_key", "only_val", 1)])
        .unwrap();

    let kv_store = KvStore::open(dir.path()).unwrap();

    // First replay.
    let applied1 = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied1, 1);
    let rev_after_first = kv_store.current_revision().unwrap();

    // Second replay — should be a no-op.
    let applied2 = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied2, 0);
    let rev_after_second = kv_store.current_revision().unwrap();

    assert_eq!(rev_after_first, rev_after_second);

    // Still exactly 1 key.
    let result = kv_store.range(b"only_key", b"", 0, 0).unwrap();
    assert_eq!(result.kvs.len(), 1);
}

#[test]
fn test_snapshot_then_crash_mid_wal() {
    let dir = tempdir().unwrap();

    // Write 5 entries, apply and snapshot at index 5.
    let log_store = LogStore::open(dir.path()).unwrap();
    let first_five: Vec<LogEntry> = (1..=5)
        .map(|i| put_entry(i, 1, &format!("k{}", i), &format!("v{}", i), i as i64))
        .collect();
    log_store.append(&first_five).unwrap();

    {
        let kv_store = KvStore::open(dir.path()).unwrap();
        let cmds: Vec<(KvCommand, i64)> = (1..=5)
            .map(|i| {
                (
                    KvCommand::Put {
                        key: format!("k{}", i).into_bytes(),
                        value: format!("v{}", i).into_bytes(),
                        lease_id: 0,
                    },
                    i as i64,
                )
            })
            .collect();
        kv_store.batch_apply_with_index(&cmds, 5).unwrap();
        kv_store.snapshot().unwrap();
    }

    // Write 3 more valid entries (index 6-8).
    let next_three: Vec<LogEntry> = (6..=8)
        .map(|i| put_entry(i, 1, &format!("k{}", i), &format!("v{}", i), i as i64))
        .collect();
    log_store.append(&next_three).unwrap();
    drop(log_store);

    // Append a torn frame to simulate crash mid-write of entry 9.
    {
        let wal_path = dir.path().join("raft.wal");
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        let fake_len: u32 = 500;
        f.write_all(&fake_len.to_le_bytes()).unwrap();
        f.write_all(&[0xDE; 7]).unwrap();
        f.flush().unwrap();
    }

    // Reopen both stores.
    let log_store = LogStore::open(dir.path()).unwrap();
    assert_eq!(log_store.last_index().unwrap(), 8, "torn entry 9 should be truncated");

    let kv_store = KvStore::open(dir.path()).unwrap();
    let applied = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied, 3, "entries 6-8 should be replayed");

    // All 8 keys should be present.
    for i in 1..=8 {
        let key = format!("k{}", i);
        let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
        assert_eq!(result.kvs.len(), 1, "key {} should exist", key);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// C. Revision Continuity
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_revisions_survive_restart() {
    let dir = tempdir().unwrap();

    let log_store = LogStore::open(dir.path()).unwrap();
    // Write entries 1-10 to WAL with revisions 1-10.
    let all_entries: Vec<LogEntry> = (1..=10)
        .map(|i| put_entry(i, 1, &format!("rk{}", i), &format!("rv{}", i), i as i64))
        .collect();
    log_store.append(&all_entries).unwrap();

    // Apply first 5 entries with batch_apply_with_index, then snapshot.
    {
        let kv_store = KvStore::open(dir.path()).unwrap();
        let cmds: Vec<(KvCommand, i64)> = (1..=5)
            .map(|i| {
                (
                    KvCommand::Put {
                        key: format!("rk{}", i).into_bytes(),
                        value: format!("rv{}", i).into_bytes(),
                        lease_id: 0,
                    },
                    i as i64,
                )
            })
            .collect();
        kv_store.batch_apply_with_index(&cmds, 5).unwrap();
        assert_eq!(kv_store.current_revision().unwrap(), 5);
        kv_store.snapshot().unwrap();
    }

    // Reopen KvStore from snapshot (revision=5), replay WAL for entries 6-10.
    {
        let kv_store = KvStore::open(dir.path()).unwrap();
        assert_eq!(kv_store.current_revision().unwrap(), 5);

        let applied = replay_wal(&log_store, &kv_store).unwrap();
        assert_eq!(applied, 5);
        assert_eq!(kv_store.current_revision().unwrap(), 10);

        // Verify all keys have the correct mod_revision.
        for i in 1..=10 {
            let key = format!("rk{}", i);
            let result = kv_store.range(key.as_bytes(), b"", 0, 0).unwrap();
            assert_eq!(result.kvs.len(), 1, "key {} should exist", key);
            assert_eq!(
                result.kvs[0].mod_revision, i as i64,
                "key {} should have mod_revision={}", key, i
            );
        }
    }
}

#[test]
fn test_revision_counter_matches_store() {
    let dir = tempdir().unwrap();

    let log_store = LogStore::open(dir.path()).unwrap();
    // Write 3 entries with revisions 42, 43, 44.
    log_store
        .append(&[
            put_entry(1, 1, "a", "x", 42),
            put_entry(2, 1, "b", "y", 43),
            put_entry(3, 1, "c", "z", 44),
        ])
        .unwrap();

    let kv_store = KvStore::open(dir.path()).unwrap();
    let applied = replay_wal(&log_store, &kv_store).unwrap();
    assert_eq!(applied, 3);

    assert_eq!(kv_store.current_revision().unwrap(), 44);
    assert_eq!(kv_store.last_applied_raft_index().unwrap(), 3);
}
