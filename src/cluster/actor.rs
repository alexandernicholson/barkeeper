//! Rebar actor for cluster membership management.
//!
//! Converts the shared `ClusterManager` into a message-passing actor that
//! serializes all access through a `tokio::sync::mpsc` command channel,
//! eliminating the need for `Arc<Mutex<...>>`.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use rebar_core::runtime::Runtime;

use crate::actors::commands::ClusterCmd;
use crate::cluster::manager::Member;
use crate::cluster::membership_sync::MembershipSync;

/// Spawn the cluster membership actor on the Rebar runtime.
///
/// The actor owns all cluster state internally and processes commands
/// sequentially from the `cmd_rx` channel. Returns a lightweight handle
/// for sending commands.
pub async fn spawn_cluster_actor(runtime: &Runtime, cluster_id: u64) -> ClusterActorHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ClusterCmd>(256);

    runtime.spawn(move |mut ctx| async move {
        let mut members: HashMap<u64, Member> = HashMap::new();
        let mut next_id: u64 = 100;
        let mut membership_sync: Option<Arc<std::sync::Mutex<MembershipSync>>> = None;

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        ClusterCmd::ClusterId { reply } => {
                            let _ = reply.send(cluster_id);
                        }
                        ClusterCmd::AddInitialMember { id, name, peer_urls, client_urls, reply } => {
                            let member = Member {
                                id,
                                name,
                                peer_urls,
                                client_urls,
                                is_learner: false,
                            };
                            members.insert(id, member);
                            if id >= next_id {
                                next_id = id + 1;
                            }
                            let _ = reply.send(());
                        }
                        ClusterCmd::MemberList { reply } => {
                            let mut list: Vec<Member> = members.values().cloned().collect();

                            // Supplement with SWIM-discovered peers.
                            if let Some(ref sync) = membership_sync {
                                let sync = sync.lock().unwrap();
                                for (node_id, addr) in sync.peer_addrs() {
                                    if !list.iter().any(|m| m.id == *node_id) {
                                        list.push(Member {
                                            id: *node_id,
                                            name: String::new(),
                                            peer_urls: vec![format!("http://{}", addr)],
                                            client_urls: vec![],
                                            is_learner: false,
                                        });
                                    }
                                }
                            }

                            let _ = reply.send(list);
                        }
                        ClusterCmd::MemberAdd { peer_urls, is_learner, reply } => {
                            let id = next_id;
                            next_id += 1;

                            let member = Member {
                                id,
                                name: String::new(),
                                peer_urls,
                                client_urls: vec![],
                                is_learner,
                            };
                            members.insert(id, member.clone());
                            let _ = reply.send(member);
                        }
                        ClusterCmd::MemberRemove { id, reply } => {
                            let existed = members.remove(&id).is_some();
                            let _ = reply.send(existed);
                        }
                        ClusterCmd::MemberUpdate { id, peer_urls, reply } => {
                            let existed = if let Some(member) = members.get_mut(&id) {
                                member.peer_urls = peer_urls;
                                true
                            } else {
                                false
                            };
                            let _ = reply.send(existed);
                        }
                        ClusterCmd::MemberPromote { id, reply } => {
                            let promoted = if let Some(member) = members.get_mut(&id) {
                                if member.is_learner {
                                    member.is_learner = false;
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            };
                            let _ = reply.send(promoted);
                        }
                        ClusterCmd::SetMembershipSync { sync, reply } => {
                            membership_sync = Some(sync);
                            let _ = reply.send(());
                        }
                    }
                }
                // Also listen on Rebar mailbox for future distributed messages.
                Some(_msg) = ctx.recv() => {
                    // Reserved for future distributed cluster coordination.
                }
                else => break,
            }
        }
    }).await;

    ClusterActorHandle {
        cluster_id,
        cmd_tx,
    }
}

/// Lightweight handle for communicating with the cluster actor.
///
/// This replaces `Arc<ClusterManager>` throughout the codebase. It is
/// cheaply cloneable and exposes the same async API as the old manager.
#[derive(Clone)]
pub struct ClusterActorHandle {
    cluster_id: u64,
    cmd_tx: mpsc::Sender<ClusterCmd>,
}

impl ClusterActorHandle {
    /// Return the cluster ID (stored locally — no actor roundtrip needed).
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Add the local node as the initial cluster member.
    pub async fn add_initial_member(
        &self,
        id: u64,
        name: String,
        peer_urls: Vec<String>,
        client_urls: Vec<String>,
    ) {
        let (reply, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ClusterCmd::AddInitialMember {
            id,
            name,
            peer_urls,
            client_urls,
            reply,
        }).await;
        let _ = rx.await;
    }

    /// List all members, including SWIM-discovered peers.
    pub async fn member_list(&self) -> Vec<Member> {
        let (reply, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ClusterCmd::MemberList { reply }).await;
        rx.await.unwrap_or_default()
    }

    /// Add a new member. Returns the newly created member.
    pub async fn member_add(&self, peer_urls: Vec<String>, is_learner: bool) -> Member {
        let (reply, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ClusterCmd::MemberAdd {
            peer_urls,
            is_learner,
            reply,
        }).await;
        rx.await.expect("cluster actor dropped")
    }

    /// Remove a member by ID. Returns true if the member existed.
    pub async fn member_remove(&self, id: u64) -> bool {
        let (reply, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ClusterCmd::MemberRemove { id, reply }).await;
        rx.await.unwrap_or(false)
    }

    /// Update a member's peer URLs. Returns true if the member existed.
    pub async fn member_update(&self, id: u64, peer_urls: Vec<String>) -> bool {
        let (reply, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ClusterCmd::MemberUpdate {
            id,
            peer_urls,
            reply,
        }).await;
        rx.await.unwrap_or(false)
    }

    /// Promote a learner to a voting member. Returns true if the member
    /// existed and was a learner.
    pub async fn member_promote(&self, id: u64) -> bool {
        let (reply, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ClusterCmd::MemberPromote { id, reply }).await;
        rx.await.unwrap_or(false)
    }

    /// Attach a SWIM membership sync for peer discovery.
    pub async fn set_membership_sync(&self, sync: Arc<std::sync::Mutex<MembershipSync>>) {
        let (reply, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ClusterCmd::SetMembershipSync { sync, reply }).await;
        let _ = rx.await;
    }
}
