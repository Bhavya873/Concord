//! The single task that owns Raft, the WAL and the store.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use concord_kv::{encode_put, KvStore};
use concord_raft::{
    Config, EntryKind, Envelope, LogIndex, Message, NodeId, ProposeError, RaftError, RaftNode, Term,
};
use concord_storage::{Wal, WalError};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};

use crate::convert as cv;
use crate::pb;
use crate::pb::kv_server::KvServer;
use crate::pb::raft_client::RaftClient;
use crate::pb::raft_server::RaftServer;
use crate::service::Service;

const PEER_RPC_TIMEOUT: Duration = Duration::from_millis(300);
const MAX_WAITERS: usize = 64;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("raft: {0}")]
    Raft(#[from] RaftError),
    #[error("wal: {0}")]
    Wal(#[from] WalError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("bad peer address {0}")]
    BadPeer(String),
}

#[derive(Clone, Debug)]
pub struct NodeSpec {
    pub id: NodeId,
    /// Every voter, including this node, with the address its gRPC server listens on.
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub data_dir: PathBuf,
    pub tick: Duration,
    pub rng_seed: u64,
}

pub(crate) enum PutFail {
    NotLeader(Option<NodeId>),
    /// The slot was taken by a different entry: this node lost leadership first.
    Superseded,
}

type PendingPut = (Term, oneshot::Sender<Result<(LogIndex, Term), PutFail>>);

pub(crate) struct GetOut {
    pub value: Option<Vec<u8>>,
    pub leader: Option<NodeId>,
}

pub(crate) enum Event {
    Tick,
    Peer {
        env: Envelope,
        reply: oneshot::Sender<Message>,
    },
    PeerResp(Envelope),
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        reply: oneshot::Sender<Result<(LogIndex, Term), PutFail>>,
    },
    Get {
        key: Vec<u8>,
        reply: oneshot::Sender<GetOut>,
    },
    Status {
        reply: oneshot::Sender<String>,
    },
}

/// A running replica. Dropping the handle does not stop it; call [`NodeHandle::kill`].
pub struct NodeHandle {
    pub id: NodeId,
    pub addr: SocketAddr,
    tasks: Vec<JoinHandle<()>>,
    tx: mpsc::Sender<Event>,
}

impl NodeHandle {
    /// Stop serving and drop all RAM state. The WAL file stays as last synced.
    pub fn kill(&self) {
        for task in &self.tasks {
            task.abort();
        }
    }

    pub async fn status(&self) -> Option<String> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Event::Status { reply }).await.ok()?;
        rx.await.ok()
    }
}

/// Bind-first start: the caller passes an already-bound listener so tests can use port 0.
pub async fn start(spec: NodeSpec, listener: TcpListener) -> Result<NodeHandle, ServerError> {
    let addr = listener.local_addr()?;
    std::fs::create_dir_all(&spec.data_dir)?;
    let (wal, recovered) = Wal::open(spec.data_dir.join("wal.log"))?;
    let cfg = Config {
        id: spec.id,
        peers: spec.peers.keys().copied().collect(),
        election_tick_min: 10,
        election_tick_max: 20,
        heartbeat_ticks: 2,
        max_entries_per_msg: 64,
        rng_seed: spec.rng_seed,
    };
    // The store is empty, so nothing is applied yet. Committed entries replay into it.
    let raft = RaftNode::restore(cfg, recovered.hard_state, recovered.entries, 0)?;

    let mut clients = BTreeMap::new();
    for (peer, peer_addr) in &spec.peers {
        if *peer == spec.id {
            continue;
        }
        let url = format!("http://{peer_addr}");
        let endpoint = Endpoint::from_shared(url.clone()).map_err(|_| ServerError::BadPeer(url))?;
        clients.insert(*peer, RaftClient::new(endpoint.connect_lazy()));
    }

    let (tx, rx) = mpsc::channel(1024);
    let driver = Driver {
        id: spec.id,
        raft,
        wal,
        kv: KvStore::new(),
        clients,
        tx: tx.clone(),
        waiters: BTreeMap::new(),
        puts: BTreeMap::new(),
    };

    let tick_every = spec.tick;
    let ticker_tx = tx.clone();
    let ticker = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_every);
        loop {
            interval.tick().await;
            if ticker_tx.send(Event::Tick).await.is_err() {
                break;
            }
        }
    });
    let run = tokio::spawn(driver.run(rx));

    let svc = Service::new(spec.id, tx.clone());
    let serve = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(RaftServer::new(svc.clone()))
            .add_service(KvServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    Ok(NodeHandle {
        id: spec.id,
        addr,
        tasks: vec![ticker, run, serve],
        tx,
    })
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RespKind {
    Vote,
    Append,
}

struct Driver {
    id: NodeId,
    raft: RaftNode,
    wal: Wal,
    kv: KvStore,
    clients: BTreeMap<NodeId, RaftClient<Channel>>,
    tx: mpsc::Sender<Event>,
    /// Inbound RPCs waiting for the response the core will produce for that peer.
    waiters: BTreeMap<(NodeId, RespKind), VecDeque<oneshot::Sender<Message>>>,
    /// Client writes waiting for their log index to be applied.
    puts: BTreeMap<LogIndex, PendingPut>,
}

impl Driver {
    async fn run(mut self, mut rx: mpsc::Receiver<Event>) {
        while let Some(event) = rx.recv().await {
            self.handle(event);
            if let Err(err) = self.pump() {
                // A node that cannot persist must stop: it may not vote or acknowledge.
                eprintln!("node {} stopping: {err}", self.id);
                return;
            }
        }
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::Tick => self.raft.tick(),
            Event::Peer { env, reply } => {
                let kind = match env.msg {
                    Message::RequestVote(_) => RespKind::Vote,
                    Message::AppendEntries(_) => RespKind::Append,
                    _ => return,
                };
                let queue = self.waiters.entry((env.from, kind)).or_default();
                if queue.len() >= MAX_WAITERS {
                    queue.pop_front();
                }
                queue.push_back(reply);
                let _ = self.raft.step(env);
            }
            Event::PeerResp(env) => {
                let _ = self.raft.step(env);
            }
            Event::Put { key, value, reply } => {
                let Ok(data) = encode_put(&key, &value) else {
                    let _ = reply.send(Err(PutFail::NotLeader(None)));
                    return;
                };
                match self.raft.propose(data) {
                    Ok((index, term)) => {
                        self.puts.insert(index, (term, reply));
                    }
                    Err(ProposeError::NotLeader { leader }) => {
                        let _ = reply.send(Err(PutFail::NotLeader(leader)));
                    }
                }
            }
            Event::Get { key, reply } => {
                let _ = reply.send(GetOut {
                    value: self.kv.get(&key).map(<[u8]>::to_vec),
                    leader: self.raft.leader(),
                });
            }
            Event::Status { reply } => {
                let _ = reply.send(self.raft.debug_line());
            }
        }
    }

    /// Persist, advance, apply, then send. Nothing leaves the node before `persist` returns.
    fn pump(&mut self) -> Result<(), WalError> {
        while self.raft.has_ready() {
            let ready = self.raft.ready();
            self.wal
                .persist(ready.hard_state, ready.truncate_from, &ready.entries)?;
            let tip = self.raft.last_log_index();
            self.raft.advance(tip);

            for entry in &ready.committed {
                if entry.kind == EntryKind::Command {
                    if let Err(err) = self.kv.apply_data(&entry.data) {
                        eprintln!("node {} skipped bad entry {}: {err}", self.id, entry.index);
                    }
                }
                if let Some((term, reply)) = self.puts.remove(&entry.index) {
                    let outcome = if term == entry.term {
                        Ok((entry.index, entry.term))
                    } else {
                        Err(PutFail::Superseded)
                    };
                    let _ = reply.send(outcome);
                }
            }

            for env in ready.messages {
                self.route(env);
            }
        }
        Ok(())
    }

    fn route(&mut self, env: Envelope) {
        match env.msg {
            Message::RequestVoteResp(_) | Message::AppendEntriesResp(_) => {
                let kind = if matches!(env.msg, Message::RequestVoteResp(_)) {
                    RespKind::Vote
                } else {
                    RespKind::Append
                };
                if let Some(reply) = self
                    .waiters
                    .get_mut(&(env.to, kind))
                    .and_then(VecDeque::pop_front)
                {
                    let _ = reply.send(env.msg);
                }
            }
            Message::RequestVote(ref m) => {
                let Some(client) = self.clients.get(&env.to).cloned() else {
                    return;
                };
                let req = cv::vote_to_pb(m);
                let (id, peer, tx) = (self.id, env.to, self.tx.clone());
                tokio::spawn(async move {
                    let call = async {
                        let mut client = client;
                        client.request_vote(req).await
                    };
                    if let Ok(Ok(resp)) = tokio::time::timeout(PEER_RPC_TIMEOUT, call).await {
                        let msg =
                            Message::RequestVoteResp(cv::vote_resp_from_pb(resp.into_inner()));
                        let _ = tx
                            .send(Event::PeerResp(Envelope {
                                from: peer,
                                to: id,
                                msg,
                            }))
                            .await;
                    }
                });
            }
            Message::AppendEntries(ref m) => {
                let Some(client) = self.clients.get(&env.to).cloned() else {
                    return;
                };
                let req = cv::append_to_pb(m);
                let (id, peer, tx) = (self.id, env.to, self.tx.clone());
                tokio::spawn(async move {
                    let call = async {
                        let mut client = client;
                        client.append_entries(req).await
                    };
                    if let Ok(Ok(resp)) = tokio::time::timeout(PEER_RPC_TIMEOUT, call).await {
                        let msg =
                            Message::AppendEntriesResp(cv::append_resp_from_pb(resp.into_inner()));
                        let _ = tx
                            .send(Event::PeerResp(Envelope {
                                from: peer,
                                to: id,
                                msg,
                            }))
                            .await;
                    }
                });
            }
        }
    }
}

#[allow(dead_code)]
fn _assert_pb_linked(_: pb::GetRequest) {}
