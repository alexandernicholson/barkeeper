use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use rand::Rng;

use super::core::{Action, Event, RaftCore};
use super::log_store::LogStore;
use super::messages::*;
use super::transport::RaftTransport;

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
}

/// Spawn the RaftNode actor. Returns a handle for submitting proposals.
///
/// The `transport` parameter is used to send outbound Raft messages to peer
/// nodes. Inbound messages from peers should be sent to `RaftHandle::inbound_tx`.
pub async fn spawn_raft_node(
    config: RaftConfig,
    apply_tx: mpsc::Sender<Vec<LogEntry>>,
    transport: Option<Arc<dyn RaftTransport>>,
) -> RaftHandle {
    let (proposal_tx, proposal_rx) = mpsc::channel::<ClientProposal>(256);
    let (inbound_tx, inbound_rx) = mpsc::channel::<(u64, RaftMessage)>(256);
    let current_term = Arc::new(AtomicU64::new(0));
    let applied_index = Arc::new(AtomicU64::new(0));
    let term_ref = Arc::clone(&current_term);
    let applied_ref = Arc::clone(&applied_index);
    let handle = RaftHandle {
        proposal_tx,
        inbound_tx,
        current_term,
        applied_index,
    };

    let data_dir = config.data_dir.clone();
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    let log_store =
        LogStore::open(format!("{}/raft.redb", data_dir)).expect("failed to open LogStore");

    let mut core = RaftCore::new(config.node_id);

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

        // Process init actions
        execute_actions(
            &init_actions,
            &log_store,
            &apply_tx,
            &mut pending_responses,
            &mut heartbeat_timer,
            &config,
            &transport,
            &applied_ref,
        )
        .await;
        term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);

        loop {
            tokio::select! {
                // Election timeout
                _ = &mut election_timer => {
                    let actions = core.step(Event::ElectionTimeout);
                    execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &transport, &applied_ref).await;
                    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                    election_timer = random_election_timeout(&config);
                }

                // Heartbeat (leader only)
                _ = heartbeat_tick(&mut heartbeat_timer) => {
                    let actions = core.step(Event::HeartbeatTimeout);
                    execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &transport, &applied_ref).await;
                    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                }

                // Client proposals
                Some(proposal) = proposal_rx.recv() => {
                    let id = proposal.id;
                    pending_responses.insert(id, proposal.response_tx);
                    let actions = core.step(Event::Proposal { id, data: proposal.data });
                    execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &transport, &applied_ref).await;
                    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
                }

                // Inbound Raft messages from peers (via transport)
                Some((from, message)) = inbound_rx.recv() => {
                    let actions = core.step(Event::Message { from, message });
                    execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &transport, &applied_ref).await;
                    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
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

async fn execute_actions(
    actions: &[Action],
    log_store: &LogStore,
    apply_tx: &mpsc::Sender<Vec<LogEntry>>,
    pending_responses: &mut std::collections::HashMap<u64, oneshot::Sender<ClientProposalResult>>,
    heartbeat_timer: &mut Option<tokio::time::Interval>,
    config: &RaftConfig,
    transport: &Option<Arc<dyn RaftTransport>>,
    applied_index: &Arc<AtomicU64>,
) {
    for action in actions {
        match action {
            Action::PersistHardState(state) => {
                log_store.save_hard_state(state).unwrap();
            }
            Action::AppendToLog(entries) => {
                log_store.append(entries).unwrap();
            }
            Action::TruncateLogAfter(index) => {
                log_store.truncate_after(*index).unwrap();
            }
            Action::ApplyEntries { from, to } => {
                let entries = log_store.get_range(*from, *to).unwrap();
                apply_tx.send(entries).await.ok();
                applied_index.store(*to, Ordering::Relaxed);
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
            Action::SendMessage { to, message } => {
                if let Some(ref transport) = transport {
                    transport.send(*to, message.clone()).await;
                } else {
                    tracing::debug!(to = to, "would send raft message (no transport configured)");
                }
            }
        }
    }
}
