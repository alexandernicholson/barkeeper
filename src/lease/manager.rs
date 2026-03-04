use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

/// A single lease entry tracked in memory.
struct LeaseEntry {
    #[allow(dead_code)]
    id: i64,
    ttl: i64, // seconds (the original granted TTL)
    granted_at: Instant,
    keys: Vec<Vec<u8>>, // keys attached to this lease
}

/// The LeaseManager tracks active leases with TTLs in memory.
///
/// For now, this does not run expiry timers (that will go through Raft
/// for consistency). It simply tracks leases and their attached keys.
pub struct LeaseManager {
    leases: Arc<Mutex<HashMap<i64, LeaseEntry>>>,
    next_id: Arc<Mutex<i64>>,
}

impl LeaseManager {
    pub fn new() -> Self {
        LeaseManager {
            leases: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
        }
    }

    /// Grant a new lease. If `id` is 0, a unique ID is generated.
    /// Returns the assigned lease ID.
    pub async fn grant(&self, id: i64, ttl: i64) -> i64 {
        let lease_id = if id == 0 {
            let mut next = self.next_id.lock().await;
            let assigned = *next;
            *next += 1;
            assigned
        } else {
            // Ensure next_id stays ahead of manually assigned IDs.
            let mut next = self.next_id.lock().await;
            if id >= *next {
                *next = id + 1;
            }
            id
        };

        let entry = LeaseEntry {
            id: lease_id,
            ttl,
            granted_at: Instant::now(),
            keys: Vec::new(),
        };

        self.leases.lock().await.insert(lease_id, entry);
        lease_id
    }

    /// Revoke a lease. Returns true if the lease existed and was removed.
    pub async fn revoke(&self, id: i64) -> bool {
        self.leases.lock().await.remove(&id).is_some()
    }

    /// Keep a lease alive by resetting its grant time. Returns the remaining
    /// TTL in seconds, or None if the lease does not exist.
    pub async fn keepalive(&self, id: i64) -> Option<i64> {
        let mut leases = self.leases.lock().await;
        if let Some(entry) = leases.get_mut(&id) {
            // Reset the grant time to extend the lease.
            entry.granted_at = Instant::now();
            Some(entry.ttl)
        } else {
            None
        }
    }

    /// Get time-to-live information for a lease.
    /// Returns (granted_ttl, remaining_ttl, keys) or None if not found.
    pub async fn time_to_live(&self, id: i64) -> Option<(i64, i64, Vec<Vec<u8>>)> {
        let leases = self.leases.lock().await;
        if let Some(entry) = leases.get(&id) {
            let elapsed = entry.granted_at.elapsed();
            let remaining = entry.ttl - elapsed.as_secs() as i64;
            let remaining = if remaining < 0 { 0 } else { remaining };
            Some((entry.ttl, remaining, entry.keys.clone()))
        } else {
            None
        }
    }

    /// List all active lease IDs.
    pub async fn list(&self) -> Vec<i64> {
        self.leases.lock().await.keys().copied().collect()
    }

    /// Attach a key to a lease. If the lease does not exist, this is a no-op.
    pub async fn attach_key(&self, lease_id: i64, key: Vec<u8>) {
        let mut leases = self.leases.lock().await;
        if let Some(entry) = leases.get_mut(&lease_id) {
            if !entry.keys.contains(&key) {
                entry.keys.push(key);
            }
        }
    }

    /// Check for expired leases. Returns expired leases and removes them.
    pub async fn check_expired(&self) -> Vec<ExpiredLease> {
        let mut leases = self.leases.lock().await;
        let mut expired = Vec::new();

        let expired_ids: Vec<i64> = leases
            .iter()
            .filter(|(_, entry)| {
                entry.granted_at.elapsed().as_secs() as i64 >= entry.ttl
            })
            .map(|(id, _)| *id)
            .collect();

        for id in expired_ids {
            if let Some(entry) = leases.remove(&id) {
                expired.push(ExpiredLease {
                    lease_id: id,
                    keys: entry.keys,
                });
            }
        }

        expired
    }
}

/// An expired lease and its attached keys.
pub struct ExpiredLease {
    pub lease_id: i64,
    pub keys: Vec<Vec<u8>>,
}
