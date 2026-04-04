//! Singleflight request coalescing for query deduplication.
//!
//! When multiple async tasks issue the SAME query (same sql_hash + same parameter
//! bytes) simultaneously, only one actually executes against PostgreSQL. The others
//! wait for the result and receive a shared copy via a broadcast channel.
//!
//! This is opt-in: enabled via `Pool::builder().singleflight(true)`.
//!
//! # Key design
//!
//! Key = hash of (sql_hash, parameter bytes). We use rapidhash to combine the
//! sql_hash with a hash of the parameter slice. If a request is already in-flight
//! with the same key, the caller subscribes to its broadcast channel instead of
//! executing a new query.
//!
//! # Limitations
//!
//! - Only coalesces `query_raw` and `query_raw_readonly` (not `execute_raw`).
//!   Writes must not be coalesced.
//! - The result is `Arc`-shared, so callers receive the same data (no mutations).
//! - Large result sets are shared by reference, reducing memory for hot reads.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::error::BsqlError;

/// Shared result type broadcast to waiting tasks.
type SharedResult = Arc<Result<Arc<OwnedResultSnapshot>, BsqlError>>;

/// A snapshot of query results that can be shared across tasks.
///
/// Unlike `OwnedResult`, this does not own an arena — the data has been
/// copied into owned `Vec<u8>` storage for safe sharing.
pub struct OwnedResultSnapshot {
    /// The query result metadata (column offsets, column descriptors).
    pub result: bsql_driver_postgres::QueryResult,
    /// Arena data copied into owned storage for sharing.
    pub arena: bsql_driver_postgres::Arena,
}

/// Singleflight coalescing layer.
///
/// Tracks in-flight queries by key. Concurrent identical queries share results.
pub struct Singleflight {
    /// In-flight queries: key -> broadcast sender.
    /// Uses std::sync::Mutex because the critical section is trivial
    /// (HashMap insert/remove — no I/O).
    in_flight: Mutex<HashMap<u64, broadcast::Sender<SharedResult>>>,
}

/// Result of attempting to join a singleflight group.
pub enum FlightResult {
    /// This task is the leader — it should execute the query.
    Leader(FlightLeader),
    /// Another task is already executing this query — wait for the result.
    Follower(broadcast::Receiver<SharedResult>),
}

/// Handle for the leader task that will execute the query and broadcast results.
pub struct FlightLeader {
    key: u64,
    tx: broadcast::Sender<SharedResult>,
}

impl FlightLeader {
    /// Broadcast the result to all waiting followers and remove from in-flight map.
    pub fn complete(self, sf: &Singleflight, result: SharedResult) {
        // Remove from in-flight first so new requests don't join a completed flight
        sf.in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
        // Broadcast to followers (ignore send errors — no receivers is fine)
        let _ = self.tx.send(result);
    }
}

impl Singleflight {
    /// Create a new singleflight coalescing layer.
    pub fn new() -> Self {
        Self {
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Try to join an in-flight query group, or become the leader.
    ///
    /// `key` should be a hash of (sql_hash, parameter bytes).
    pub fn try_join(&self, key: u64) -> FlightResult {
        let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(tx) = map.get(&key) {
            // Another task is already executing — subscribe
            FlightResult::Follower(tx.subscribe())
        } else {
            // We are the leader — create broadcast channel
            // Capacity 1: only one result will ever be sent
            let (tx, _) = broadcast::channel(1);
            map.insert(key, tx.clone());
            FlightResult::Leader(FlightLeader { key, tx })
        }
    }

    /// Compute a singleflight key from sql_hash and parameter data.
    ///
    /// Uses rapidhash to combine the sql_hash with a hash of all parameter
    /// bytes (including actual encoded values). Two queries with the same SQL
    /// and same parameter values produce the same key.
    pub fn compute_key(
        sql_hash: u64,
        params: &[&(dyn bsql_driver_postgres::Encode + Sync)],
    ) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = rapidhash::quality::RapidHasher::default();
        sql_hash.hash(&mut hasher);
        let mut scratch = Vec::with_capacity(64);
        // Hash each parameter's actual encoded bytes (not just type OID)
        for param in params {
            if param.is_null() {
                hasher.write_u8(0xFF); // NULL marker
            } else {
                scratch.clear();
                param.encode_binary(&mut scratch);
                hasher.write(&scratch);
            }
        }
        hasher.finish()
    }
}

impl Default for Singleflight {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singleflight_leader_when_empty() {
        let sf = Singleflight::new();
        let result = sf.try_join(42);
        assert!(matches!(result, FlightResult::Leader(_)));
    }

    #[test]
    fn singleflight_follower_when_in_flight() {
        let sf = Singleflight::new();
        let _leader = sf.try_join(42);
        let result = sf.try_join(42);
        assert!(matches!(result, FlightResult::Follower(_)));
    }

    #[test]
    fn singleflight_different_keys_both_leaders() {
        let sf = Singleflight::new();
        let r1 = sf.try_join(42);
        let r2 = sf.try_join(43);
        assert!(matches!(r1, FlightResult::Leader(_)));
        assert!(matches!(r2, FlightResult::Leader(_)));
    }

    #[test]
    fn singleflight_complete_removes_from_map() {
        let sf = Singleflight::new();
        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };
        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("test".into()));
        leader.complete(&sf, Arc::new(Err(err)));

        // After completion, same key should produce a new leader
        let result = sf.try_join(42);
        assert!(matches!(result, FlightResult::Leader(_)));
    }

    #[test]
    fn compute_key_same_inputs_same_key() {
        let k1 = Singleflight::compute_key(123, &[]);
        let k2 = Singleflight::compute_key(123, &[]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_different_sql_hash_different_key() {
        let k1 = Singleflight::compute_key(123, &[]);
        let k2 = Singleflight::compute_key(456, &[]);
        assert_ne!(k1, k2);
    }

    // --- compute_key with actual params ---

    #[test]
    fn compute_key_same_params_same_key() {
        let a = 42i32;
        let b = 42i32;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_different_params_different_key() {
        let a = 42i32;
        let b = 99i32;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn compute_key_different_sql_same_params_different_key() {
        let a = 42i32;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(200, &[&a]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn compute_key_null_param_handling() {
        // Option<i32> = None encodes as NULL
        let null_val: Option<i32> = None;
        let some_val: Option<i32> = Some(42);
        let k1 = Singleflight::compute_key(100, &[&null_val]);
        let k2 = Singleflight::compute_key(100, &[&some_val]);
        assert_ne!(k1, k2, "NULL and Some(42) should produce different keys");
    }

    #[test]
    fn compute_key_two_nulls_same_key() {
        let a: Option<i32> = None;
        let b: Option<i32> = None;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_multiple_params() {
        let a = 1i32;
        let b = "hello";
        let k1 = Singleflight::compute_key(100, &[&a, &b]);
        let k2 = Singleflight::compute_key(100, &[&a, &b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_param_order_matters() {
        let a = 1i32;
        let b = 2i32;
        let k1 = Singleflight::compute_key(100, &[&a, &b]);
        let k2 = Singleflight::compute_key(100, &[&b, &a]);
        assert_ne!(k1, k2);
    }

    // --- FlightLeader::complete broadcasts result ---

    #[tokio::test]
    async fn leader_complete_broadcasts_to_follower() {
        let sf = Singleflight::new();

        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };

        let mut rx = match sf.try_join(42) {
            FlightResult::Follower(rx) => rx,
            _ => panic!("expected follower"),
        };

        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("test".into()));
        leader.complete(&sf, Arc::new(Err(err)));

        let received = rx.recv().await.unwrap();
        assert!(received.is_err());
    }

    // --- Multiple followers receive same result ---

    #[tokio::test]
    async fn multiple_followers_receive_result() {
        let sf = Singleflight::new();

        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };

        let mut rx1 = match sf.try_join(42) {
            FlightResult::Follower(rx) => rx,
            _ => panic!("expected follower 1"),
        };
        let mut rx2 = match sf.try_join(42) {
            FlightResult::Follower(rx) => rx,
            _ => panic!("expected follower 2"),
        };

        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("done".into()));
        leader.complete(&sf, Arc::new(Err(err)));

        let r1 = rx1.recv().await.unwrap();
        let r2 = rx2.recv().await.unwrap();
        assert!(r1.is_err());
        assert!(r2.is_err());
    }

    // --- Drop leader without completing -> key stays in map ---

    #[test]
    fn drop_leader_without_complete_key_stays_in_map() {
        let sf = Singleflight::new();

        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };

        // Drop leader without calling complete.
        // The in-flight map still holds the broadcast sender (cloned on insert),
        // so the key is NOT removed. New joiners become followers.
        drop(leader);

        // A new try_join for the same key should still produce a follower
        // (the entry is still in the map).
        let result = sf.try_join(42);
        assert!(
            matches!(result, FlightResult::Follower(_)),
            "key should still be in map after leader drop without complete"
        );
    }

    // --- Concurrent stress test ---

    #[tokio::test]
    async fn concurrent_stress_test() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::task;

        let sf = Arc::new(Singleflight::new());
        let leader_count = Arc::new(AtomicUsize::new(0));
        let follower_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        // 10 tasks, 5 unique keys (2 tasks per key)
        for i in 0..10 {
            let sf = Arc::clone(&sf);
            let leaders = Arc::clone(&leader_count);
            let followers = Arc::clone(&follower_count);
            let key = (i % 5) as u64;

            handles.push(task::spawn(async move {
                match sf.try_join(key) {
                    FlightResult::Leader(leader) => {
                        leaders.fetch_add(1, Ordering::Relaxed);
                        // Complete immediately
                        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool(
                            "stress".into(),
                        ));
                        leader.complete(&sf, Arc::new(Err(err)));
                    }
                    FlightResult::Follower(_rx) => {
                        followers.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let total = leader_count.load(Ordering::Relaxed) + follower_count.load(Ordering::Relaxed);
        assert_eq!(total, 10, "all 10 tasks should participate");
        // At least 5 leaders (one per unique key)
        assert!(
            leader_count.load(Ordering::Relaxed) >= 5,
            "should have at least 5 leaders (one per key)"
        );
    }

    // --- Default trait ---

    #[test]
    fn singleflight_default() {
        let sf = Singleflight::default();
        // Should be able to use it
        let result = sf.try_join(1);
        assert!(matches!(result, FlightResult::Leader(_)));
    }

    // --- Send + Sync assertions ---

    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    #[test]
    fn singleflight_is_send_and_sync() {
        _assert_send::<Singleflight>();
        _assert_sync::<Singleflight>();
    }

    // --- compute_key with string params ---

    #[test]
    fn compute_key_string_params() {
        let a = "hello";
        let b = "world";
        let k1 = Singleflight::compute_key(100, &[&a, &b]);
        let k2 = Singleflight::compute_key(100, &[&a, &b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_empty_params_consistent() {
        let k1 = Singleflight::compute_key(0, &[]);
        let k2 = Singleflight::compute_key(0, &[]);
        assert_eq!(k1, k2);
    }

    // --- Leader complete with no followers ---

    #[test]
    fn leader_complete_with_no_followers() {
        let sf = Singleflight::new();
        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };
        // Complete without any followers. Should not panic.
        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("solo".into()));
        leader.complete(&sf, Arc::new(Err(err)));

        // Key should be removed
        let result = sf.try_join(42);
        assert!(matches!(result, FlightResult::Leader(_)));
    }
}
