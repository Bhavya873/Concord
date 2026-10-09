//! tonic handlers. They translate RPCs into driver events and wait for the reply.

use std::time::Duration;

use concord_raft::{Envelope, Message, NodeId};
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use crate::convert as cv;
use crate::driver::{Event, PutFail};
use crate::pb;
use crate::pb::kv_server::Kv;
use crate::pb::raft_server::Raft;

const PEER_WAIT: Duration = Duration::from_millis(800);
const PUT_WAIT: Duration = Duration::from_secs(4);

#[derive(Clone)]
pub(crate) struct Service {
    id: NodeId,
    tx: mpsc::Sender<Event>,
}

impl Service {
    pub fn new(id: NodeId, tx: mpsc::Sender<Event>) -> Self {
        Self { id, tx }
    }

    async fn peer_call(&self, from: NodeId, msg: Message) -> Result<Message, Status> {
        let (reply, rx) = oneshot::channel();
        let env = Envelope {
            from,
            to: self.id,
            msg,
        };
        self.tx
            .send(Event::Peer { env, reply })
            .await
            .map_err(|_| Status::unavailable("node is stopping"))?;
        match tokio::time::timeout(PEER_WAIT, rx).await {
            Ok(Ok(msg)) => Ok(msg),
            _ => Err(Status::deadline_exceeded("no response from raft core")),
        }
    }
}

#[tonic::async_trait]
impl Raft for Service {
    async fn request_vote(
        &self,
        request: Request<pb::RequestVoteRequest>,
    ) -> Result<Response<pb::RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let from = req.candidate_id;
        match self
            .peer_call(from, Message::RequestVote(cv::vote_from_pb(req)))
            .await?
        {
            Message::RequestVoteResp(resp) => Ok(Response::new(cv::vote_resp_to_pb(&resp))),
            _ => Err(Status::internal("unexpected reply kind")),
        }
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let from = req.leader_id;
        let msg = cv::append_from_pb(req)
            .ok_or_else(|| Status::invalid_argument("unknown entry kind"))?;
        match self.peer_call(from, Message::AppendEntries(msg)).await? {
            Message::AppendEntriesResp(resp) => Ok(Response::new(cv::append_resp_to_pb(&resp))),
            _ => Err(Status::internal("unexpected reply kind")),
        }
    }
}

#[tonic::async_trait]
impl Kv for Service {
    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::PutResponse>, Status> {
        let req = request.into_inner();
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Event::Put {
                key: req.key,
                value: req.value,
                reply,
            })
            .await
            .map_err(|_| Status::unavailable("node is stopping"))?;
        match tokio::time::timeout(PUT_WAIT, rx).await {
            Ok(Ok(Ok((index, term)))) => Ok(Response::new(pb::PutResponse {
                leader_id: self.id,
                index,
                term,
            })),
            Ok(Ok(Err(PutFail::NotLeader(leader)))) => Err(Status::failed_precondition(format!(
                "not leader; leader={}",
                leader.unwrap_or(0)
            ))),
            Ok(Ok(Err(PutFail::Superseded))) => Err(Status::aborted(
                "leadership lost before the write committed",
            )),
            _ => Err(Status::deadline_exceeded("write did not commit in time")),
        }
    }

    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Event::Get {
                key: request.into_inner().key,
                reply,
            })
            .await
            .map_err(|_| Status::unavailable("node is stopping"))?;
        let out = rx
            .await
            .map_err(|_| Status::unavailable("node is stopping"))?;
        Ok(Response::new(pb::GetResponse {
            found: out.value.is_some(),
            value: out.value.unwrap_or_default(),
            leader_id: out.leader.unwrap_or(0),
        }))
    }
}
