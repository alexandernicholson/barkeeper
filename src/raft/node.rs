use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use rand::Rng;

use rebar_core::process::ProcessId;
use rebar_core::runtime::{ProcessContext, Runtime};
use rebar_cluster::registry::orset::Registry;

use super::core::{Action, Event, RaftCore};
use super::log_store::LogStore;
use super::messages::*;
use super::state::PersistentState;

use crate::kv::write_buffer::WriteBuffer;
use crate::kv::state_machine::KvCommand;
use crate::proto::mvccpb::KeyValue;

/// Configuration for the RaftNode.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: u64,
    pub data_dir: String,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub peers: Vec<u64>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            node_id: 1,
            data_dir: "data.barkeeper".into(),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            peers: vec![],
        }
    }
}

/// Handle to communicate with the RaftNode actor.
#[derive(Clone)]
pub struct RaftHandle {
    pub proposal_tx: mpsc::Sender<ClientProposal>,
    /// Channel for delivering inbound Raft messages from peers.
    /// The transport pushes `(from_node_id, message)` tuples here.
    pub inbound_tx: mpsc::Sender<(u64, RaftMessage)>,
    /// Current Raft term, updated by the actor after every state transition.
    pub current_term: Arc<AtomicU64>,
    /// Last applied Raft log index, updated after entries are sent to the state machine.
    pub applied_index: Arc<AtomicU64>,
    /// Current Raft leader ID (0 = unknown).
    pub leader_id: Arc<AtomicU64>,
    /// Last committed Raft log index. Updated when ApplyEntries actions fire.
    /// Used by ReadIndex to determine how far the state machine needs to catch up.
    pub commit_index: Arc<AtomicU64>,
}

impl RaftHandle {
    /// Submit a proposal and wait for the result.
    pub async fn propose(&self, data: Vec<u8>) -> Result<ClientProposalResult, String> {
        let (tx, rx) = oneshot::channel();
        let proposal = ClientProposal {
            id: rand::random(),
            data,
            response_tx: tx,
        };
        self.proposal_tx
            .send(proposal)
            .await
            .map_err(|_| "raft node stopped".to_string())?;
        rx.await.map_err(|_| "proposal dropped".to_string())
    }

    /// Returns the last applied Raft log index.
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Relaxed)
    }

    /// Returns the current Raft leader ID (0 = unknown).
    pub fn leader_id(&self) -> u64 {
        self.leader_id.load(Ordering::Relaxed)
    }

    /// Returns the last committed Raft log index.
    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Acquire)
    }
}

/// Spawn the RaftNode actor. Returns a handle for submitting proposals.
///
/// This spawns a single-node Raft actor using a raw `tokio::spawn`. For
/// multi-node clusters, use `spawn_raft_node_rebar` which uses the Rebar
/// runtime for inter-node communication.
pub async fn spawn_raft_node(
    config: RaftConfig,
    apply_tx: mpsc::Sender<Vec<LogEntry>>,
    revision: Arc<AtomicI64>,
    write_buffer: Arc<WriteBuffer>,
) -> RaftHandle {
    let (proposal_tx, proposal_rx) = mpsc::channel::<ClientProposal>(256);
    let (inbound_tx, inbound_rx) = mpsc::channel::<(u64, RaftMessage)>(256);
    let current_term = Arc::new(AtomicU64::new(0));
    let applied_index = Arc::new(AtomicU64::new(0));
    let leader_id_atomic = Arc::new(AtomicU64::new(0));
    let commit_index = Arc::new(AtomicU64::new(0));
    let term_ref = Arc::clone(&current_term);
    let applied_ref = Arc::clone(&applied_index);
    let leader_ref = Arc::clone(&leader_id_atomic);
    let commit_ref = Arc::clone(&commit_index);
    let handle = RaftHandle {
        proposal_tx,
        inbound_tx,
        current_term,
        applied_index,
        leader_id: leader_id_atomic,
        commit_index,
    };

    let data_dir = config.data_dir.clone();
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    let log_store =
        LogStore::open(&data_dir).expect("failed to open LogStore");

    let mut core = RaftCore::new(config.node_id, revision);

    // Initialize from persistent state
    let hard_state = log_store.load_hard_state().unwrap();
    let last_index = log_store.last_index().unwrap();
    let last_term = log_store.last_term().unwrap();

    let init_actions = core.step(Event::Initialize {
        peers: config.peers.clone(),
        hard_state,
        last_log_index: last_index,
        last_log_term: last_term,
    });

    // Spawn the actor as a tokio task
    tokio::spawn(async move {
        let mut proposal_rx = proposal_rx;
        let mut inbound_rx = inbound_rx;
        let mut election_timer = random_election_timeout(&config);
        let mut heartbeat_timer: Option<tokio::time::Interval> = None;
        let mut pending_responses: std::collections::HashMap<
            u64,
            oneshot::Sender<ClientProposalResult>,
        > = Default::default();

        // In-memory buffer for recently appended entries to avoid disk round-trips
        let mut recent_entries: Vec<LogEntry> = Vec::new();

        // Process init actions
        execute_actions(
            &init_actions,
            &log_store,
            &apply_tx,
            &mut pending_responses,
            &mut heartbeat_timer,
            &config,
            &applied_ref,
            &commit_ref,
            &mut recent_entries,
            &write_buffer,
        )
        .await;
        term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);

        loop {
            tokio::select! {
                // Election timeout
                _ = &mut election_timer => {
                    let actions = core.step(Event::ElectionTimeout);
                    execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &applied_ref, &commit_ref, &mut recent_entries, &write_buffer).await;
                    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
                    election_timer = random_election_timeout(&config);
                }

                // Heartbeat (leader only)
                _ = heartbeat_tick(&mut heartbeat_timer) => {
                    let actions = core.step(Event::HeartbeatTimeout);
                    execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &applied_ref, &commit_ref, &mut recent_entries, &write_buffer).await;
                    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
                }

                // Client proposals (group commit)
                Some(proposal) = proposal_rx.recv() => {
                    let mut wal_buffer = WalBuffer::new();
                    let mut batch = vec![proposal];
                    loop {
                        while batch.len() < 4096 {
                            match proposal_rx.try_recv() {
                                Ok(p) => batch.push(p),
                                Err(_) => break,
                            }
                        }
                        let mut all_actions = Vec::new();
                        for p in batch.drain(..) {
                            pending_responses.insert(p.id, p.response_tx);
                            let actions = core.step(Event::Proposal { id: p.id, data: p.data });
                            all_actions.extend(actions);
                        }
                        let merged = merge_log_actions(all_actions);
                        execute_actions_buffered(
                            &merged, &log_store, &apply_tx, &mut pending_responses,
                            &mut heartbeat_timer, &config, &applied_ref, &commit_ref,
                            &mut recent_entries, &write_buffer, &mut wal_buffer,
                        ).await;
                        match proposal_rx.try_recv() {
                            Ok(p) => {
                                batch.push(p);
                                continue;
                            }
                            Err(_) => break,
                        }
                    }
                    wal_buffer.flush(&log_store, &mut pending_responses).await;
                    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
                }

                // Inbound Raft messages from peers (via transport)
                Some((from, message)) = inbound_rx.recv() => {
                    let actions = core.step(Event::Message { from, message });
                    execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &applied_ref, &commit_ref, &mut recent_entries, &write_buffer).await;
                    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
                }
            }
        }
    });

    handle
}

fn random_election_timeout(config: &RaftConfig) -> Pin<Box<tokio::time::Sleep>> {
    let mut rng = rand::thread_rng();
    let ms = rng.gen_range(
        config.election_timeout_min.as_millis()..=config.election_timeout_max.as_millis(),
    );
    Box::pin(tokio::time::sleep(Duration::from_millis(ms as u64)))
}

async fn heartbeat_tick(timer: &mut Option<tokio::time::Interval>) {
    match timer {
        Some(ref mut hb) => {
            hb.tick().await;
        }
        None => {
            std::future::pending::<()>().await;
        }
    }
}

/// Buffered WAL state for group commit.
struct WalBuffer {
    entries: Vec<LogEntry>,
    hard_state: Option<PersistentState>,
    deferred_responses: Vec<(u64, ClientProposalResult)>,
}

impl WalBuffer {
    fn new() -> Self {
        WalBuffer {
            entries: Vec::new(),
            hard_state: None,
            deferred_responses: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.hard_state.is_none()
    }

    async fn flush(
        &mut self,
        log_store: &LogStore,
        pending_responses: &mut std::collections::HashMap<u64, oneshot::Sender<ClientProposalResult>>,
    ) {
        if self.is_empty() {
            for (id, result) in self.deferred_responses.drain(..) {
                if let Some(tx) = pending_responses.remove(&id) {
                    let _ = tx.send(result);
                }
            }
            return;
        }

        let ls = log_store.clone();
        let entries = std::mem::take(&mut self.entries);
        let hard_state = self.hard_state.take();
        tokio::task::spawn_blocking(move || {
            ls.flush(&entries, hard_state.as_ref()).unwrap()
        })
        .await
        .unwrap();

        for (id, result) in self.deferred_responses.drain(..) {
            if let Some(tx) = pending_responses.remove(&id) {
                let _ = tx.send(result);
            }
        }
    }
}

async fn execute_actions_buffered(
    actions: &[Action],
    log_store: &LogStore,
    apply_tx: &mpsc::Sender<Vec<LogEntry>>,
    pending_responses: &mut std::collections::HashMap<u64, oneshot::Sender<ClientProposalResult>>,
    heartbeat_timer: &mut Option<tokio::time::Interval>,
    config: &RaftConfig,
    applied_index: &Arc<AtomicU64>,
    commit_index: &Arc<AtomicU64>,
    recent_entries: &mut Vec<LogEntry>,
    write_buffer: &Arc<WriteBuffer>,
    wal_buffer: &mut WalBuffer,
) {
    for action in actions {
        match action {
            Action::PersistHardState(state) => {
                wal_buffer.hard_state = Some(state.clone());
            }
            Action::AppendToLog(entries) => {
                wal_buffer.entries.extend(entries.iter().cloned());
                recent_entries.extend(entries.iter().cloned());
            }
            Action::TruncateLogAfter(index) => {
                wal_buffer.flush(log_store, pending_responses).await;
                let ls = log_store.clone();
                let idx = *index;
                tokio::task::spawn_blocking(move || ls.truncate_after(idx).unwrap())
                    .await
                    .unwrap();
                recent_entries.retain(|e| e.index <= idx);
            }
            Action::ApplyEntries { from, to } => {
                let expected_count = (*to - *from + 1) as usize;
                let mem_entries: Vec<LogEntry> = recent_entries
                    .iter()
                    .filter(|e| e.index >= *from && e.index <= *to)
                    .cloned()
                    .collect();
                let entries = if mem_entries.len() == expected_count {
                    mem_entries
                } else {
                    let ls = log_store.clone();
                    let (f, t) = (*from, *to);
                    tokio::task::spawn_blocking(move || ls.get_range(f, t).unwrap())
                        .await
                        .unwrap()
                };
                for entry in &entries {
                    if let LogEntryData::Command { data, revision } = &entry.data {
                        if let Ok(cmd) = bincode::deserialize::<KvCommand>(data) {
                            match cmd {
                                KvCommand::Put { key, value, lease_id } => {
                                    let kv = KeyValue {
                                        key: key.clone(),
                                        value,
                                        create_revision: *revision,
                                        mod_revision: *revision,
                                        version: 1,
                                        lease: lease_id,
                                    };
                                    write_buffer.put(kv);
                                }
                                KvCommand::DeleteRange { key, range_end: _ } => {
                                    write_buffer.delete(&key, *revision);
                                }
                                _ => {}
                            }
                        }
                    }
                }
                recent_entries.retain(|e| e.index > *to);
                apply_tx.send(entries).await.ok();
                applied_index.store(*to, Ordering::Relaxed);
                commit_index.store(*to, Ordering::Release);
            }
            Action::RespondToProposal { id, result } => {
                let result_clone = match result {
                    ClientProposalResult::Success { index, revision } => {
                        ClientProposalResult::Success { index: *index, revision: *revision }
                    }
                    ClientProposalResult::NotLeader { leader_id } => {
                        ClientProposalResult::NotLeader { leader_id: *leader_id }
                    }
                    ClientProposalResult::Error(e) => ClientProposalResult::Error(e.clone()),
                };
                wal_buffer.deferred_responses.push((*id, result_clone));
            }
            Action::ResetElectionTimer => {}
            Action::StartHeartbeatTimer => {
                *heartbeat_timer = Some(tokio::time::interval(config.heartbeat_interval));
            }
            Action::StopHeartbeatTimer => {
                *heartbeat_timer = None;
            }
            Action::SendMessage { .. } => {} // No-op in single-node mode
        }
    }
}

async fn execute_actions_buffered_rebar(
    actions: &[Action],
    log_store: &LogStore,
    apply_tx: &mpsc::Sender<Vec<LogEntry>>,
    pending_responses: &mut std::collections::HashMap<u64, oneshot::Sender<ClientProposalResult>>,
    heartbeat_timer: &mut Option<tokio::time::Interval>,
    config: &RaftConfig,
    ctx: &ProcessContext,
    peers: &Arc<Mutex<HashMap<u64, ProcessId>>>,
    applied_index: &Arc<AtomicU64>,
    commit_index: &Arc<AtomicU64>,
    recent_entries: &mut Vec<LogEntry>,
    write_buffer: &Arc<WriteBuffer>,
    wal_buffer: &mut WalBuffer,
) -> bool {
    let mut should_reset_election_timer = false;
    for action in actions {
        match action {
            Action::PersistHardState(state) => {
                wal_buffer.hard_state = Some(state.clone());
            }
            Action::AppendToLog(entries) => {
                wal_buffer.entries.extend(entries.iter().cloned());
                recent_entries.extend(entries.iter().cloned());
            }
            Action::TruncateLogAfter(index) => {
                wal_buffer.flush(log_store, pending_responses).await;
                let ls = log_store.clone();
                let idx = *index;
                tokio::task::spawn_blocking(move || ls.truncate_after(idx).unwrap())
                    .await
                    .unwrap();
                recent_entries.retain(|e| e.index <= idx);
            }
            Action::ApplyEntries { from, to } => {
                let expected_count = (*to - *from + 1) as usize;
                let mem_entries: Vec<LogEntry> = recent_entries
                    .iter()
                    .filter(|e| e.index >= *from && e.index <= *to)
                    .cloned()
                    .collect();
                let entries = if mem_entries.len() == expected_count {
                    mem_entries
                } else {
                    let ls = log_store.clone();
                    let (f, t) = (*from, *to);
                    tokio::task::spawn_blocking(move || ls.get_range(f, t).unwrap())
                        .await
                        .unwrap()
                };
                for entry in &entries {
                    if let LogEntryData::Command { data, revision } = &entry.data {
                        if let Ok(cmd) = bincode::deserialize::<KvCommand>(data) {
                            match cmd {
                                KvCommand::Put { key, value, lease_id } => {
                                    let kv = KeyValue {
                                        key: key.clone(),
                                        value,
                                        create_revision: *revision,
                                        mod_revision: *revision,
                                        version: 1,
                                        lease: lease_id,
                                    };
                                    write_buffer.put(kv);
                                }
                                KvCommand::DeleteRange { key, range_end: _ } => {
                                    write_buffer.delete(&key, *revision);
                                }
                                _ => {}
                            }
                        }
                    }
                }
                recent_entries.retain(|e| e.index > *to);
                apply_tx.send(entries).await.ok();
                applied_index.store(*to, Ordering::Relaxed);
                commit_index.store(*to, Ordering::Release);
            }
            Action::RespondToProposal { id, result } => {
                let result_clone = match result {
                    ClientProposalResult::Success { index, revision } => {
                        ClientProposalResult::Success { index: *index, revision: *revision }
                    }
                    ClientProposalResult::NotLeader { leader_id } => {
                        ClientProposalResult::NotLeader { leader_id: *leader_id }
                    }
                    ClientProposalResult::Error(e) => ClientProposalResult::Error(e.clone()),
                };
                wal_buffer.deferred_responses.push((*id, result_clone));
            }
            Action::ResetElectionTimer => {
                should_reset_election_timer = true;
            }
            Action::StartHeartbeatTimer => {
                *heartbeat_timer = Some(tokio::time::interval(config.heartbeat_interval));
            }
            Action::StopHeartbeatTimer => {
                *heartbeat_timer = None;
            }
            Action::SendMessage { to, message } => {
                let peer_pid = {
                    let peers = peers.lock().unwrap();
                    peers.get(to).copied()
                };
                if let Some(pid) = peer_pid {
                    let message = match message {
                        RaftMessage::AppendEntriesReq(req) => {
                            let mut req = req.clone();
                            if req.prev_log_index > 0 {
                                req.prev_log_term = log_store
                                    .term_at(req.prev_log_index)
                                    .unwrap()
                                    .unwrap_or(0);
                            }
                            let last = log_store.last_index().unwrap_or(0);
                            let start = req.prev_log_index + 1;
                            if start <= last {
                                req.entries = log_store.get_range(start, last).unwrap_or_default();
                            }
                            RaftMessage::AppendEntriesReq(req)
                        }
                        other => other.clone(),
                    };
                    let payload = raft_message_to_value(&message);
                    if let Err(e) = ctx.send(pid, payload).await {
                        tracing::warn!(to = to, error = %e, "failed to send raft message via rebar");
                    }
                } else {
                    tracing::debug!(to = to, "no peer PID found for node (peer not yet discovered)");
                }
            }
        }
    }
    should_reset_election_timer
}

async fn execute_non_wal_action(
    action: &Action,
    apply_tx: &mpsc::Sender<Vec<LogEntry>>,
    pending_responses: &mut std::collections::HashMap<u64, oneshot::Sender<ClientProposalResult>>,
    heartbeat_timer: &mut Option<tokio::time::Interval>,
    config: &RaftConfig,
    applied_index: &Arc<AtomicU64>,
    commit_index: &Arc<AtomicU64>,
    recent_entries: &mut Vec<LogEntry>,
    write_buffer: &Arc<WriteBuffer>,
    log_store: &LogStore,
) {
    match action {
        Action::ApplyEntries { from, to } => {
            // Try in-memory first
            let expected_count = (*to - *from + 1) as usize;
            let mem_entries: Vec<LogEntry> = recent_entries
                .iter()
                .filter(|e| e.index >= *from && e.index <= *to)
                .cloned()
                .collect();

            let entries = if mem_entries.len() == expected_count {
                mem_entries
            } else {
                let ls = log_store.clone();
                let (f, t) = (*from, *to);
                tokio::task::spawn_blocking(move || ls.get_range(f, t).unwrap())
                    .await
                    .unwrap()
            };

            // Populate write buffer for immediate read visibility
            for entry in &entries {
                if let LogEntryData::Command { data, revision } = &entry.data {
                    if let Ok(cmd) = bincode::deserialize::<KvCommand>(data) {
                        match cmd {
                            KvCommand::Put { key, value, lease_id } => {
                                let kv = KeyValue {
                                    key: key.clone(),
                                    value,
                                    create_revision: *revision,
                                    mod_revision: *revision,
                                    version: 1,
                                    lease: lease_id,
                                };
                                write_buffer.put(kv);
                            }
                            KvCommand::DeleteRange { key, range_end: _ } => {
                                write_buffer.delete(&key, *revision);
                            }
                            _ => {} // Txn/Compact handled by broker path
                        }
                    }
                }
            }

            // Trim applied entries from buffer
            recent_entries.retain(|e| e.index > *to);

            apply_tx.send(entries).await.ok();
            applied_index.store(*to, Ordering::Relaxed);
            commit_index.store(*to, Ordering::Release);
        }
        Action::RespondToProposal { id, result } => {
            if let Some(tx) = pending_responses.remove(id) {
                let _ = tx.send(match result {
                    ClientProposalResult::Success { index, revision } => {
                        ClientProposalResult::Success {
                            index: *index,
                            revision: *revision,
                        }
                    }
                    ClientProposalResult::NotLeader { leader_id } => {
                        ClientProposalResult::NotLeader {
                            leader_id: *leader_id,
                        }
                    }
                    ClientProposalResult::Error(e) => ClientProposalResult::Error(e.clone()),
                });
            }
        }
        Action::ResetElectionTimer => {
            // Timer reset handled by the select loop
        }
        Action::StartHeartbeatTimer => {
            *heartbeat_timer = Some(tokio::time::interval(config.heartbeat_interval));
        }
        Action::StopHeartbeatTimer => {
            *heartbeat_timer = None;
        }
        Action::SendMessage { .. } => {
            // No-op: single-node mode has no peers to send to.
            // Multi-node clusters use spawn_raft_node_rebar with Rebar transport.
        }
        _ => {} // WAL actions handled by caller
    }
}

async fn execute_actions(
    actions: &[Action],
    log_store: &LogStore,
    apply_tx: &mpsc::Sender<Vec<LogEntry>>,
    pending_responses: &mut std::collections::HashMap<u64, oneshot::Sender<ClientProposalResult>>,
    heartbeat_timer: &mut Option<tokio::time::Interval>,
    config: &RaftConfig,
    applied_index: &Arc<AtomicU64>,
    commit_index: &Arc<AtomicU64>,
    recent_entries: &mut Vec<LogEntry>,
    write_buffer: &Arc<WriteBuffer>,
) {
    // First pass: collect WAL operations
    let mut wal_entries: Vec<LogEntry> = Vec::new();
    let mut hard_state: Option<PersistentState> = None;
    let mut has_truncate = false;

    for action in actions {
        match action {
            Action::PersistHardState(state) => {
                hard_state = Some(state.clone());
            }
            Action::AppendToLog(entries) => {
                wal_entries.extend(entries.iter().cloned());
            }
            Action::TruncateLogAfter(_) => {
                has_truncate = true;
                break;
            }
            _ => {}
        }
    }

    // If there's a truncate, fall back to sequential processing
    if has_truncate {
        for action in actions {
            match action {
                Action::PersistHardState(state) => {
                    let ls = log_store.clone();
                    let s = state.clone();
                    tokio::task::spawn_blocking(move || ls.save_hard_state(&s).unwrap())
                        .await
                        .unwrap();
                }
                Action::AppendToLog(entries) => {
                    let ls = log_store.clone();
                    let e = entries.clone();
                    tokio::task::spawn_blocking(move || ls.append(&e).unwrap())
                        .await
                        .unwrap();
                    recent_entries.extend(entries.iter().cloned());
                }
                Action::TruncateLogAfter(index) => {
                    let ls = log_store.clone();
                    let idx = *index;
                    tokio::task::spawn_blocking(move || ls.truncate_after(idx).unwrap())
                        .await
                        .unwrap();
                    recent_entries.retain(|e| e.index <= idx);
                }
                other => {
                    execute_non_wal_action(other, apply_tx, pending_responses, heartbeat_timer, config, applied_index, commit_index, recent_entries, write_buffer, log_store).await;
                }
            }
        }
        return;
    }

    // No truncate: flush entries + hard state in one transaction
    if !wal_entries.is_empty() || hard_state.is_some() {
        let ls = log_store.clone();
        let e = wal_entries.clone();
        let hs = hard_state;
        tokio::task::spawn_blocking(move || ls.flush(&e, hs.as_ref()).unwrap())
            .await
            .unwrap();
        recent_entries.extend(wal_entries);
    }

    // Second pass: process non-WAL actions
    for action in actions {
        match action {
            Action::PersistHardState(_) | Action::AppendToLog(_) => {} // Already flushed
            Action::TruncateLogAfter(_) => unreachable!(),
            other => {
                execute_non_wal_action(other, apply_tx, pending_responses, heartbeat_timer, config, applied_index, commit_index, recent_entries, write_buffer, log_store).await;
            }
        }
    }
}

/// Spawn the RaftNode actor using the Rebar runtime.
///
/// Like `spawn_raft_node`, but uses Rebar `runtime.spawn()` to create a
/// distributed actor instead of a raw `tokio::spawn`. The actor registers
/// itself in the global registry as `raft:{node_id}` and uses the Rebar
/// mailbox for inter-node Raft messages.
pub async fn spawn_raft_node_rebar(
    config: RaftConfig,
    apply_tx: mpsc::Sender<Vec<LogEntry>>,
    runtime: &Runtime,
    registry: Arc<Mutex<Registry>>,
    peers: Arc<Mutex<HashMap<u64, ProcessId>>>,
    revision: Arc<AtomicI64>,
    write_buffer: Arc<WriteBuffer>,
) -> RaftHandle {
    let (proposal_tx, proposal_rx) = mpsc::channel::<ClientProposal>(256);
    // The Rebar version receives messages via ctx.recv() from the actor
    // mailbox, not via the mpsc channel. Create a closed channel so that
    // sends to inbound_tx immediately fail (signaling callers).
    let (inbound_tx, inbound_rx) = mpsc::channel::<(u64, RaftMessage)>(1);
    drop(inbound_rx);
    let current_term = Arc::new(AtomicU64::new(0));
    let applied_index = Arc::new(AtomicU64::new(0));
    let leader_id_atomic = Arc::new(AtomicU64::new(0));
    let commit_index = Arc::new(AtomicU64::new(0));
    let term_ref = Arc::clone(&current_term);
    let applied_ref = Arc::clone(&applied_index);
    let leader_ref = Arc::clone(&leader_id_atomic);
    let commit_ref = Arc::clone(&commit_index);

    let handle = RaftHandle {
        proposal_tx,
        inbound_tx,
        current_term,
        applied_index,
        leader_id: leader_id_atomic,
        commit_index,
    };

    let data_dir = config.data_dir.clone();
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    let log_store =
        LogStore::open(&data_dir).expect("failed to open LogStore");

    let mut core = RaftCore::new(config.node_id, revision);

    // Initialize from persistent state
    let hard_state = log_store.load_hard_state().unwrap();
    let last_index = log_store.last_index().unwrap();
    let last_term = log_store.last_term().unwrap();

    let init_actions = core.step(Event::Initialize {
        peers: config.peers.clone(),
        hard_state,
        last_log_index: last_index,
        last_log_term: last_term,
    });

    let node_id = config.node_id;

    runtime
        .spawn(move |mut ctx| async move {
            // Register in global registry as "raft:{node_id}"
            {
                let name = format!("raft:{}", node_id);
                registry.lock().unwrap().register(
                    &name,
                    ctx.self_pid(),
                    node_id,
                    1, // timestamp
                );
            }

            let mut proposal_rx = proposal_rx;
            let mut election_timer = random_election_timeout(&config);
            let mut heartbeat_timer: Option<tokio::time::Interval> = None;
            let mut pending_responses: std::collections::HashMap<
                u64,
                oneshot::Sender<ClientProposalResult>,
            > = Default::default();

            // In-memory buffer for recently appended entries to avoid disk round-trips
            let mut recent_entries: Vec<LogEntry> = Vec::new();

            // Process init actions
            execute_actions_rebar(
                &init_actions,
                &log_store,
                &apply_tx,
                &mut pending_responses,
                &mut heartbeat_timer,
                &config,
                &ctx,
                &peers,
                &applied_ref,
                &commit_ref,
                &mut recent_entries,
                &write_buffer,
            )
            .await;
            term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);

            loop {
                tokio::select! {
                    // Election timeout
                    _ = &mut election_timer => {
                        let actions = core.step(Event::ElectionTimeout);
                        let _reset = execute_actions_rebar(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &ctx, &peers, &applied_ref, &commit_ref, &mut recent_entries, &write_buffer).await;
                        term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
                        // Always reset after election timeout fires (start_election returns ResetElectionTimer)
                        election_timer = random_election_timeout(&config);
                    }

                    // Heartbeat (leader only)
                    _ = heartbeat_tick(&mut heartbeat_timer) => {
                        let actions = core.step(Event::HeartbeatTimeout);
                        let reset = execute_actions_rebar(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &ctx, &peers, &applied_ref, &commit_ref, &mut recent_entries, &write_buffer).await;
                        term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
                        if reset { election_timer = random_election_timeout(&config); }
                    }

                    // Client proposals (group commit)
                    Some(proposal) = proposal_rx.recv() => {
                        let mut wal_buffer = WalBuffer::new();
                        let mut batch = vec![proposal];
                        let mut reset = false;
                        loop {
                            while batch.len() < 4096 {
                                match proposal_rx.try_recv() {
                                    Ok(p) => batch.push(p),
                                    Err(_) => break,
                                }
                            }
                            let mut all_actions = Vec::new();
                            for p in batch.drain(..) {
                                pending_responses.insert(p.id, p.response_tx);
                                let actions = core.step(Event::Proposal { id: p.id, data: p.data });
                                all_actions.extend(actions);
                            }
                            let merged = merge_log_actions(all_actions);
                            reset |= execute_actions_buffered_rebar(
                                &merged, &log_store, &apply_tx, &mut pending_responses,
                                &mut heartbeat_timer, &config, &ctx, &peers,
                                &applied_ref, &commit_ref, &mut recent_entries, &write_buffer,
                                &mut wal_buffer,
                            ).await;
                            match proposal_rx.try_recv() {
                                Ok(p) => {
                                    batch.push(p);
                                    continue;
                                }
                                Err(_) => break,
                            }
                        }
                        wal_buffer.flush(&log_store, &mut pending_responses).await;
                        term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                        leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
                        if reset { election_timer = random_election_timeout(&config); }
                    }

                    // Inbound Raft messages from peers via Rebar mailbox
                    Some(msg) = ctx.recv() => {
                        if let Ok(raft_msg) = raft_message_from_value(msg.payload()) {
                            let from = msg.from().node_id();
                            let actions = core.step(Event::Message { from, message: raft_msg });
                            let reset = execute_actions_rebar(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &ctx, &peers, &applied_ref, &commit_ref, &mut recent_entries, &write_buffer).await;
                            term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
                            if reset { election_timer = random_election_timeout(&config); }
                        }
                    }
                }
            }
        })
        .await;

    handle
}

/// Execute a non-WAL Raft action using the Rebar process context for message delivery.
///
/// Returns `true` if the election timer should be reset.
async fn execute_non_wal_action_rebar(
    action: &Action,
    apply_tx: &mpsc::Sender<Vec<LogEntry>>,
    pending_responses: &mut std::collections::HashMap<u64, oneshot::Sender<ClientProposalResult>>,
    heartbeat_timer: &mut Option<tokio::time::Interval>,
    config: &RaftConfig,
    ctx: &ProcessContext,
    peers: &Arc<Mutex<HashMap<u64, ProcessId>>>,
    applied_index: &Arc<AtomicU64>,
    commit_index: &Arc<AtomicU64>,
    recent_entries: &mut Vec<LogEntry>,
    write_buffer: &Arc<WriteBuffer>,
    log_store: &LogStore,
) -> bool {
    match action {
        Action::ApplyEntries { from, to } => {
            // Try in-memory first
            let expected_count = (*to - *from + 1) as usize;
            let mem_entries: Vec<LogEntry> = recent_entries
                .iter()
                .filter(|e| e.index >= *from && e.index <= *to)
                .cloned()
                .collect();

            let entries = if mem_entries.len() == expected_count {
                mem_entries
            } else {
                let ls = log_store.clone();
                let (f, t) = (*from, *to);
                tokio::task::spawn_blocking(move || ls.get_range(f, t).unwrap())
                    .await
                    .unwrap()
            };

            // Populate write buffer for immediate read visibility
            for entry in &entries {
                if let LogEntryData::Command { data, revision } = &entry.data {
                    if let Ok(cmd) = bincode::deserialize::<KvCommand>(data) {
                        match cmd {
                            KvCommand::Put { key, value, lease_id } => {
                                let kv = KeyValue {
                                    key: key.clone(),
                                    value,
                                    create_revision: *revision,
                                    mod_revision: *revision,
                                    version: 1,
                                    lease: lease_id,
                                };
                                write_buffer.put(kv);
                            }
                            KvCommand::DeleteRange { key, range_end: _ } => {
                                write_buffer.delete(&key, *revision);
                            }
                            _ => {} // Txn/Compact handled by broker path
                        }
                    }
                }
            }

            // Trim applied entries from buffer
            recent_entries.retain(|e| e.index > *to);

            apply_tx.send(entries).await.ok();
            applied_index.store(*to, Ordering::Relaxed);
            commit_index.store(*to, Ordering::Release);
            false
        }
        Action::RespondToProposal { id, result } => {
            if let Some(tx) = pending_responses.remove(id) {
                let _ = tx.send(match result {
                    ClientProposalResult::Success { index, revision } => {
                        ClientProposalResult::Success {
                            index: *index,
                            revision: *revision,
                        }
                    }
                    ClientProposalResult::NotLeader { leader_id } => {
                        ClientProposalResult::NotLeader {
                            leader_id: *leader_id,
                        }
                    }
                    ClientProposalResult::Error(e) => ClientProposalResult::Error(e.clone()),
                });
            }
            false
        }
        Action::ResetElectionTimer => {
            true
        }
        Action::StartHeartbeatTimer => {
            *heartbeat_timer = Some(tokio::time::interval(config.heartbeat_interval));
            false
        }
        Action::StopHeartbeatTimer => {
            *heartbeat_timer = None;
            false
        }
        Action::SendMessage { to, message } => {
            // Look up peer PID from the shared peers map
            let peer_pid = {
                let peers = peers.lock().unwrap();
                peers.get(to).copied()
            };
            if let Some(pid) = peer_pid {
                // For AppendEntries, fill in entries and prev_log_term
                // from the LogStore (the core leaves them as placeholders).
                let message = match message {
                    RaftMessage::AppendEntriesReq(req) => {
                        let mut req = req.clone();
                        if req.prev_log_index > 0 {
                            req.prev_log_term = log_store
                                .term_at(req.prev_log_index)
                                .unwrap()
                                .unwrap_or(0);
                        }
                        let last = log_store.last_index().unwrap_or(0);
                        let start = req.prev_log_index + 1;
                        if start <= last {
                            req.entries = log_store.get_range(start, last).unwrap_or_default();
                        }
                        RaftMessage::AppendEntriesReq(req)
                    }
                    other => other.clone(),
                };
                let payload = raft_message_to_value(&message);
                if let Err(e) = ctx.send(pid, payload).await {
                    tracing::warn!(to = to, error = %e, "failed to send raft message via rebar");
                }
            } else {
                tracing::debug!(to = to, "no peer PID found for node (peer not yet discovered)");
            }
            false
        }
        _ => false, // WAL actions handled by caller
    }
}

/// Execute Raft actions using the Rebar process context for message delivery.
///
/// This mirrors `execute_actions` but uses Rebar's `ProcessContext::send()`
/// and a peer PID lookup table for inter-node communication.
///
/// Returns `true` if the election timer should be reset.
async fn execute_actions_rebar(
    actions: &[Action],
    log_store: &LogStore,
    apply_tx: &mpsc::Sender<Vec<LogEntry>>,
    pending_responses: &mut std::collections::HashMap<u64, oneshot::Sender<ClientProposalResult>>,
    heartbeat_timer: &mut Option<tokio::time::Interval>,
    config: &RaftConfig,
    ctx: &ProcessContext,
    peers: &Arc<Mutex<HashMap<u64, ProcessId>>>,
    applied_index: &Arc<AtomicU64>,
    commit_index: &Arc<AtomicU64>,
    recent_entries: &mut Vec<LogEntry>,
    write_buffer: &Arc<WriteBuffer>,
) -> bool {
    // First pass: collect WAL operations
    let mut wal_entries: Vec<LogEntry> = Vec::new();
    let mut hard_state: Option<PersistentState> = None;
    let mut has_truncate = false;

    for action in actions {
        match action {
            Action::PersistHardState(state) => {
                hard_state = Some(state.clone());
            }
            Action::AppendToLog(entries) => {
                wal_entries.extend(entries.iter().cloned());
            }
            Action::TruncateLogAfter(_) => {
                has_truncate = true;
                break;
            }
            _ => {}
        }
    }

    let mut should_reset_election_timer = false;

    // If there's a truncate, fall back to sequential processing
    if has_truncate {
        for action in actions {
            match action {
                Action::PersistHardState(state) => {
                    let ls = log_store.clone();
                    let s = state.clone();
                    tokio::task::spawn_blocking(move || ls.save_hard_state(&s).unwrap())
                        .await
                        .unwrap();
                }
                Action::AppendToLog(entries) => {
                    let ls = log_store.clone();
                    let e = entries.clone();
                    tokio::task::spawn_blocking(move || ls.append(&e).unwrap())
                        .await
                        .unwrap();
                    recent_entries.extend(entries.iter().cloned());
                }
                Action::TruncateLogAfter(index) => {
                    let ls = log_store.clone();
                    let idx = *index;
                    tokio::task::spawn_blocking(move || ls.truncate_after(idx).unwrap())
                        .await
                        .unwrap();
                    recent_entries.retain(|e| e.index <= idx);
                }
                other => {
                    should_reset_election_timer |= execute_non_wal_action_rebar(other, apply_tx, pending_responses, heartbeat_timer, config, ctx, peers, applied_index, commit_index, recent_entries, write_buffer, log_store).await;
                }
            }
        }
        return should_reset_election_timer;
    }

    // No truncate: flush entries + hard state in one transaction
    if !wal_entries.is_empty() || hard_state.is_some() {
        let ls = log_store.clone();
        let e = wal_entries.clone();
        let hs = hard_state;
        tokio::task::spawn_blocking(move || ls.flush(&e, hs.as_ref()).unwrap())
            .await
            .unwrap();
        recent_entries.extend(wal_entries);
    }

    // Second pass: process non-WAL actions
    for action in actions {
        match action {
            Action::PersistHardState(_) | Action::AppendToLog(_) => {} // Already flushed
            Action::TruncateLogAfter(_) => unreachable!(),
            other => {
                should_reset_election_timer |= execute_non_wal_action_rebar(other, apply_tx, pending_responses, heartbeat_timer, config, ctx, peers, applied_index, commit_index, recent_entries, write_buffer, log_store).await;
            }
        }
    }

    should_reset_election_timer
}

/// Merge multiple `AppendToLog` actions into a single action for batching.
fn merge_log_actions(actions: Vec<Action>) -> Vec<Action> {
    let mut merged_entries = Vec::new();
    let mut other_actions = Vec::new();
    for action in actions {
        match action {
            Action::AppendToLog(entries) => merged_entries.extend(entries),
            other => other_actions.push(other),
        }
    }
    let mut result = Vec::new();
    if !merged_entries.is_empty() {
        result.push(Action::AppendToLog(merged_entries));
    }
    result.extend(other_actions);
    result
}
