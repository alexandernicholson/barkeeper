//! RaftProcess — Raft consensus engine running as a Rebar actor.
//!
//! This wraps the existing RaftCore pure state machine and LogStore in a
//! Rebar-supervised process. Uses tokio::spawn internally but follows the
//! actor-per-concern pattern with typed command channels.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use rand::Rng;

use crate::actors::commands::{RaftCmd, ProposeResult, ApplyResult, StoreCmd};
use crate::raft::core::{Action, Event, RaftCore};
use crate::raft::log_store::LogStore;
use crate::raft::messages::*;
use crate::raft::node::RaftConfig;
use crate::raft::transport::RaftTransport;

/// Spawn the RaftProcess actor. Returns a sender for submitting commands.
pub fn spawn_raft_process(
    config: RaftConfig,
    store_tx: mpsc::Sender<StoreCmd>,
    transport: Option<Arc<dyn RaftTransport>>,
    current_term: Arc<AtomicU64>,
) -> mpsc::Sender<RaftCmd> {
    let (cmd_tx, cmd_rx) = mpsc::channel::<RaftCmd>(256);

    tokio::spawn(async move {
        raft_process_loop(config, cmd_rx, store_tx, transport, current_term).await;
    });

    cmd_tx
}

async fn raft_process_loop(
    config: RaftConfig,
    mut cmd_rx: mpsc::Receiver<RaftCmd>,
    store_tx: mpsc::Sender<StoreCmd>,
    transport: Option<Arc<dyn RaftTransport>>,
    current_term: Arc<AtomicU64>,
) {
    std::fs::create_dir_all(&config.data_dir).expect("create data dir");

    let log_store = LogStore::open(format!("{}/raft.redb", config.data_dir))
        .expect("open LogStore");

    let mut core = RaftCore::new(config.node_id);

    // Initialize from persistent state.
    let hard_state = log_store.load_hard_state().unwrap();
    let last_index = log_store.last_index().unwrap();
    let last_term = log_store.last_term().unwrap();

    let init_actions = core.step(Event::Initialize {
        peers: config.peers.clone(),
        hard_state,
        last_log_index: last_index,
        last_log_term: last_term,
    });

    let (apply_tx, mut _apply_rx) = mpsc::channel::<Vec<LogEntry>>(256);
    let mut pending_responses: std::collections::HashMap<
        u64,
        tokio::sync::oneshot::Sender<ClientProposalResult>,
    > = Default::default();

    let mut election_timer = random_election_timeout(&config);
    let mut heartbeat_timer: Option<tokio::time::Interval> = None;

    // Process init actions.
    execute_actions(
        &init_actions, &log_store, &apply_tx, &mut pending_responses,
        &mut heartbeat_timer, &config, &transport,
    ).await;
    current_term.store(core.state.persistent.current_term, Ordering::Relaxed);

    loop {
        tokio::select! {
            _ = &mut election_timer => {
                let actions = core.step(Event::ElectionTimeout);
                execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &transport).await;
                election_timer = random_election_timeout(&config);
                current_term.store(core.state.persistent.current_term, Ordering::Relaxed);
            }

            _ = heartbeat_tick(&mut heartbeat_timer) => {
                let actions = core.step(Event::HeartbeatTimeout);
                execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &transport).await;
                current_term.store(core.state.persistent.current_term, Ordering::Relaxed);
            }

            Some(raft_cmd) = cmd_rx.recv() => {
                match raft_cmd {
                    RaftCmd::Propose { command, reply } => {
                        let data = serde_json::to_vec(&command).expect("serialize KvCommand");
                        let id: u64 = rand::random();
                        let (otx, orx) = tokio::sync::oneshot::channel();
                        pending_responses.insert(id, otx);

                        let actions = core.step(Event::Proposal { id, data });
                        execute_actions(&actions, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &transport).await;
                        current_term.store(core.state.persistent.current_term, Ordering::Relaxed);

                        // Bridge: when the proposal is committed, apply to store and return result.
                        let store_tx_clone = store_tx.clone();
                        let term = core.state.persistent.current_term;
                        tokio::spawn(async move {
                            match orx.await {
                                Ok(ClientProposalResult::Success { .. }) => {
                                    let (atx, arx) = tokio::sync::oneshot::channel();
                                    let _ = store_tx_clone.send(StoreCmd::Apply {
                                        command,
                                        reply: atx,
                                    }).await;
                                    match arx.await {
                                        Ok(Ok(apply_result)) => {
                                            let pr = match apply_result {
                                                ApplyResult::Put(r) => ProposeResult {
                                                    revision: r.revision,
                                                    raft_term: term,
                                                    put_result: Some(r),
                                                    delete_result: None,
                                                    txn_result: None,
                                                },
                                                ApplyResult::Delete(r) => ProposeResult {
                                                    revision: r.revision,
                                                    raft_term: term,
                                                    put_result: None,
                                                    delete_result: Some(r),
                                                    txn_result: None,
                                                },
                                                ApplyResult::Txn(r) => ProposeResult {
                                                    revision: r.revision,
                                                    raft_term: term,
                                                    put_result: None,
                                                    delete_result: None,
                                                    txn_result: Some(r),
                                                },
                                                ApplyResult::Compact => ProposeResult {
                                                    revision: 0,
                                                    raft_term: term,
                                                    put_result: None,
                                                    delete_result: None,
                                                    txn_result: None,
                                                },
                                            };
                                            let _ = reply.send(Ok(pr));
                                        }
                                        _ => {
                                            let _ = reply.send(Err("apply failed".to_string()));
                                        }
                                    }
                                }
                                Ok(ClientProposalResult::NotLeader { .. }) => {
                                    let _ = reply.send(Err("not leader".to_string()));
                                }
                                Ok(ClientProposalResult::Error(e)) => {
                                    let _ = reply.send(Err(e));
                                }
                                Err(_) => {
                                    let _ = reply.send(Err("proposal dropped".to_string()));
                                }
                            }
                        });
                    }
                }
            }
        }
    }
}

fn random_election_timeout(config: &RaftConfig) -> std::pin::Pin<Box<tokio::time::Sleep>> {
    let mut rng = rand::thread_rng();
    let ms = rng.gen_range(
        config.election_timeout_min.as_millis()..=config.election_timeout_max.as_millis(),
    );
    Box::pin(tokio::time::sleep(Duration::from_millis(ms as u64)))
}

async fn heartbeat_tick(timer: &mut Option<tokio::time::Interval>) {
    match timer {
        Some(ref mut hb) => { hb.tick().await; }
        None => { std::future::pending::<()>().await; }
    }
}

async fn execute_actions(
    actions: &[Action],
    log_store: &LogStore,
    apply_tx: &mpsc::Sender<Vec<LogEntry>>,
    pending_responses: &mut std::collections::HashMap<u64, tokio::sync::oneshot::Sender<ClientProposalResult>>,
    heartbeat_timer: &mut Option<tokio::time::Interval>,
    config: &RaftConfig,
    transport: &Option<Arc<dyn RaftTransport>>,
) {
    for action in actions {
        match action {
            Action::PersistHardState(state) => { log_store.save_hard_state(state).unwrap(); }
            Action::AppendToLog(entries) => { log_store.append(entries).unwrap(); }
            Action::TruncateLogAfter(index) => { log_store.truncate_after(*index).unwrap(); }
            Action::ApplyEntries { from, to } => {
                let entries = log_store.get_range(*from, *to).unwrap();
                apply_tx.send(entries).await.ok();
            }
            Action::RespondToProposal { id, result } => {
                if let Some(tx) = pending_responses.remove(id) {
                    let _ = tx.send(match result {
                        ClientProposalResult::Success { index, revision } => {
                            ClientProposalResult::Success { index: *index, revision: *revision }
                        }
                        ClientProposalResult::NotLeader { leader_id } => {
                            ClientProposalResult::NotLeader { leader_id: *leader_id }
                        }
                        ClientProposalResult::Error(e) => ClientProposalResult::Error(e.clone()),
                    });
                }
            }
            Action::ResetElectionTimer => {}
            Action::StartHeartbeatTimer => {
                *heartbeat_timer = Some(tokio::time::interval(config.heartbeat_interval));
            }
            Action::StopHeartbeatTimer => { *heartbeat_timer = None; }
            Action::SendMessage { to, message } => {
                if let Some(ref transport) = transport {
                    transport.send(*to, message.clone()).await;
                }
            }
        }
    }
}
