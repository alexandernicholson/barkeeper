use crate::proto::mvccpb;

/// Event to send to a watcher.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub watch_id: i64,
    pub events: Vec<mvccpb::Event>,
    pub compact_revision: i64,
}

/// Check whether a key matches a watcher's key/range_end filter.
///
/// - If `range_end` is empty, the watcher matches only the exact key.
/// - If `range_end` is `[0]` (`\x00`), the watcher matches all keys >= key.
/// - Otherwise the watcher matches keys in the half-open range [key, range_end).
pub fn key_matches(watcher_key: &[u8], range_end: &[u8], event_key: &[u8]) -> bool {
    if range_end.is_empty() {
        // Exact match.
        event_key == watcher_key
    } else if range_end == b"\x00" {
        // All keys >= watcher_key.
        event_key >= watcher_key
    } else {
        // Range [key, range_end).
        event_key >= watcher_key && event_key < range_end
    }
}
