//! Thin async wrapper around tonic gRPC stubs for barkeeper.

use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::proto::etcdserverpb::{
    self,
    kv_client::KvClient,
    watch_client::WatchClient,
    cluster_client::ClusterClient,
    maintenance_client::MaintenanceClient,
    RangeRequest, PutRequest, DeleteRangeRequest,
    WatchRequest, WatchCreateRequest, watch_request,
    MemberListRequest, StatusRequest,
};
use crate::proto::mvccpb::{KeyValue, Event};

/// gRPC client for barkeeper.
#[derive(Clone)]
pub struct BkClient {
    kv: KvClient<Channel>,
    watch: WatchClient<Channel>,
    cluster: ClusterClient<Channel>,
    maintenance: MaintenanceClient<Channel>,
}

impl BkClient {
    /// Connect to a barkeeper endpoint.
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        let channel = Channel::from_shared(endpoint.to_string())
            .unwrap()
            .connect()
            .await?;
        Ok(Self {
            kv: KvClient::new(channel.clone()),
            watch: WatchClient::new(channel.clone()),
            cluster: ClusterClient::new(channel.clone()),
            maintenance: MaintenanceClient::new(channel),
        })
    }

    /// Put a key-value pair.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), tonic::Status> {
        let mut kv = self.kv.clone();
        kv.put(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })
        .await?;
        Ok(())
    }

    /// Get a single key. Returns None if not found.
    pub async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>, tonic::Status> {
        let mut kv = self.kv.clone();
        let resp = kv
            .range(RangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(resp.kvs.into_iter().next())
    }

    /// Get all keys with the given prefix.
    pub async fn get_prefix(&self, prefix: &[u8]) -> Result<Vec<KeyValue>, tonic::Status> {
        let mut kv = self.kv.clone();
        let range_end = prefix_range_end(prefix);
        let resp = kv
            .range(RangeRequest {
                key: prefix.to_vec(),
                range_end,
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(resp.kvs)
    }

    /// Delete a single key. Returns the number of keys deleted.
    pub async fn delete(&self, key: &[u8]) -> Result<i64, tonic::Status> {
        let mut kv = self.kv.clone();
        let resp = kv
            .delete_range(DeleteRangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok(resp.deleted)
    }

    /// List cluster members.
    pub async fn member_list(
        &self,
    ) -> Result<Vec<etcdserverpb::Member>, tonic::Status> {
        let mut cluster = self.cluster.clone();
        let resp = cluster
            .member_list(MemberListRequest { linearizable: false })
            .await?
            .into_inner();
        Ok(resp.members)
    }

    /// Get cluster status.
    pub async fn status(&self) -> Result<etcdserverpb::StatusResponse, tonic::Status> {
        let mut maint = self.maintenance.clone();
        let resp = maint
            .status(StatusRequest {})
            .await?
            .into_inner();
        Ok(resp)
    }

    /// Start watching a prefix. Returns a channel that receives events.
    pub async fn watch(
        &self,
        prefix: &[u8],
    ) -> Result<mpsc::UnboundedReceiver<Event>, tonic::Status> {
        let mut watch = self.watch.clone();
        let (tx, rx) = mpsc::unbounded_channel();

        let create_req = WatchRequest {
            request_union: Some(watch_request::RequestUnion::CreateRequest(
                WatchCreateRequest {
                    key: prefix.to_vec(),
                    range_end: prefix_range_end(prefix),
                    ..Default::default()
                },
            )),
        };

        let (req_tx, req_rx) = mpsc::channel(1);
        req_tx.send(create_req).await.map_err(|_| {
            tonic::Status::internal("failed to send watch create request")
        })?;

        let req_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);
        let mut resp_stream = watch.watch(req_stream).await?.into_inner();

        tokio::spawn(async move {
            // Keep req_tx alive so the request stream stays open.
            let _keep_alive = req_tx;
            use tokio_stream::StreamExt;
            while let Some(Ok(resp)) = resp_stream.next().await {
                for event in resp.events {
                    if tx.send(event).is_err() {
                        return; // receiver dropped
                    }
                }
            }
        });

        Ok(rx)
    }
}

/// Compute the range end for a prefix scan (increment last byte).
fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // All 0xff — use empty (meaning no upper bound)
    vec![0]
}
