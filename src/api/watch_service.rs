use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::proto::etcdserverpb::watch_request::RequestUnion;
use crate::proto::etcdserverpb::watch_server::Watch;
use crate::proto::etcdserverpb::{ResponseHeader, WatchRequest, WatchResponse};
use crate::watch::actor::WatchHubActorHandle;

/// gRPC Watch service implementing the etcd Watch API.
pub struct WatchService {
    hub: WatchHubActorHandle,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
}

impl WatchService {
    pub fn new(hub: WatchHubActorHandle, cluster_id: u64, member_id: u64, raft_term: Arc<AtomicU64>) -> Self {
        WatchService {
            hub,
            cluster_id,
            member_id,
            raft_term,
        }
    }
}

#[tonic::async_trait]
impl Watch for WatchService {
    type WatchStream = ReceiverStream<Result<WatchResponse, Status>>;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut in_stream = request.into_inner();
        let (resp_tx, resp_rx) = mpsc::channel(256);

        let hub = self.hub.clone();
        let cluster_id = self.cluster_id;
        let member_id = self.member_id;
        let raft_term = Arc::clone(&self.raft_term);

        tokio::spawn(async move {
            // Track active watch IDs so we can spawn event-forwarding tasks.
            // Each watch gets its own forwarding task that reads from the hub
            // receiver and writes to resp_tx.
            let mut watch_tasks: Vec<(i64, tokio::task::JoinHandle<()>)> = Vec::new();

            while let Some(result) = in_stream.next().await {
                let req = match result {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("watch request stream error: {}", e);
                        break;
                    }
                };

                match req.request_union {
                    Some(RequestUnion::CreateRequest(create)) => {
                        let (watch_id, mut event_rx) = hub
                            .create_watch_with_id(
                                create.key,
                                create.range_end,
                                create.start_revision,
                                create.filters,
                                create.prev_kv,
                                create.watch_id,
                            )
                            .await;

                        // Detect collision: if the channel is already closed,
                        // the actor rejected the requested watch_id.
                        match event_rx.try_recv() {
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                let error_resp = WatchResponse {
                                    header: Some(ResponseHeader {
                                        cluster_id,
                                        member_id,
                                        revision: 0,
                                        raft_term: raft_term.load(Ordering::Relaxed),
                                    }),
                                    watch_id,
                                    created: false,
                                    canceled: true,
                                    compact_revision: 0,
                                    cancel_reason: "watch id already exists".to_string(),
                                    fragment: false,
                                    events: vec![],
                                };
                                if resp_tx.send(Ok(error_resp)).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            _ => {} // Empty or has data — watch was created successfully.
                        }

                        // Send the "created" response.
                        let created_resp = WatchResponse {
                            header: Some(ResponseHeader {
                                cluster_id,
                                member_id,
                                revision: 0,
                                raft_term: raft_term.load(Ordering::Relaxed),
                            }),
                            watch_id,
                            created: true,
                            canceled: false,
                            compact_revision: 0,
                            cancel_reason: String::new(),
                            fragment: false,
                            events: vec![],
                        };

                        if resp_tx.send(Ok(created_resp)).await.is_err() {
                            break;
                        }

                        // Spawn a task to forward events from this watch to the
                        // response stream.
                        let resp_tx_clone = resp_tx.clone();
                        let raft_term_clone = Arc::clone(&raft_term);
                        let task = tokio::spawn(async move {
                            while let Some(watch_event) = event_rx.recv().await {
                                let resp = WatchResponse {
                                    header: Some(ResponseHeader {
                                        cluster_id,
                                        member_id,
                                        revision: 0,
                                        raft_term: raft_term_clone.load(Ordering::Relaxed),
                                    }),
                                    watch_id: watch_event.watch_id,
                                    created: false,
                                    canceled: false,
                                    compact_revision: watch_event.compact_revision,
                                    cancel_reason: String::new(),
                                    fragment: false,
                                    events: watch_event.events,
                                };

                                if resp_tx_clone.send(Ok(resp)).await.is_err() {
                                    break;
                                }
                            }
                        });

                        watch_tasks.push((watch_id, task));
                    }
                    Some(RequestUnion::CancelRequest(cancel)) => {
                        let existed = hub.cancel_watch(cancel.watch_id).await;

                        // Abort the forwarding task for this watch.
                        watch_tasks.retain(|(id, task)| {
                            if *id == cancel.watch_id {
                                task.abort();
                                false
                            } else {
                                true
                            }
                        });

                        let canceled_resp = WatchResponse {
                            header: Some(ResponseHeader {
                                cluster_id,
                                member_id,
                                revision: 0,
                                raft_term: raft_term.load(Ordering::Relaxed),
                            }),
                            watch_id: cancel.watch_id,
                            created: false,
                            canceled: existed,
                            compact_revision: 0,
                            cancel_reason: String::new(),
                            fragment: false,
                            events: vec![],
                        };

                        if resp_tx.send(Ok(canceled_resp)).await.is_err() {
                            break;
                        }
                    }
                    Some(RequestUnion::ProgressRequest(_)) => {
                        // Progress requests are not yet implemented.
                        // Send an empty response.
                        let progress_resp = WatchResponse {
                            header: Some(ResponseHeader {
                                cluster_id,
                                member_id,
                                revision: 0,
                                raft_term: raft_term.load(Ordering::Relaxed),
                            }),
                            watch_id: -1,
                            created: false,
                            canceled: false,
                            compact_revision: 0,
                            cancel_reason: String::new(),
                            fragment: false,
                            events: vec![],
                        };

                        if resp_tx.send(Ok(progress_resp)).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        // Empty request, ignore.
                    }
                }
            }

            // Clean up: cancel all watches when the stream closes.
            for (id, task) in watch_tasks {
                hub.cancel_watch(id).await;
                task.abort();
            }
        });

        Ok(Response::new(ReceiverStream::new(resp_rx)))
    }
}
