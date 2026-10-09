//! Small client that walks the node list until it finds the leader.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use thiserror::Error;
use tonic::transport::Endpoint;

use crate::pb;
use crate::pb::kv_client::KvClient;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("no node accepted the request before the deadline: {0}")]
    Unavailable(String),
    #[error("bad address {0}")]
    BadAddress(String),
}

pub struct KvCluster {
    addrs: Vec<SocketAddr>,
    last_good: usize,
}

impl KvCluster {
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        Self {
            addrs,
            last_good: 0,
        }
    }

    /// Retries across nodes until `deadline`. A write that timed out may still have committed.
    pub async fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        deadline: Duration,
    ) -> Result<pb::PutResponse, ClientError> {
        let start = Instant::now();
        let mut last_err = String::from("no attempt made");
        while start.elapsed() < deadline {
            for step in 0..self.addrs.len() {
                let i = (self.last_good + step) % self.addrs.len();
                let mut client = connect(self.addrs[i])?;
                let req = pb::PutRequest {
                    key: key.to_vec(),
                    value: value.to_vec(),
                };
                match client.put(req).await {
                    Ok(resp) => {
                        self.last_good = i;
                        return Ok(resp.into_inner());
                    }
                    Err(status) => last_err = format!("{}: {}", self.addrs[i], status.message()),
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(ClientError::Unavailable(last_err))
    }

    /// Reads one node's applied store. This is not a linearizable read.
    pub async fn get_from(
        &self,
        addr: SocketAddr,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let mut client = connect(addr)?;
        let resp = client
            .get(pb::GetRequest { key: key.to_vec() })
            .await
            .map_err(|s| ClientError::Unavailable(s.message().to_string()))?
            .into_inner();
        Ok(resp.found.then_some(resp.value))
    }
}

fn connect(addr: SocketAddr) -> Result<KvClient<tonic::transport::Channel>, ClientError> {
    let url = format!("http://{addr}");
    let endpoint = Endpoint::from_shared(url.clone())
        .map_err(|_| ClientError::BadAddress(url))?
        .connect_timeout(Duration::from_millis(300))
        .timeout(Duration::from_secs(5));
    Ok(KvClient::new(endpoint.connect_lazy()))
}
