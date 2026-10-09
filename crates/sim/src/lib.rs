//! Deterministic 3-node cluster for checking Raft safety.
//!
//! Time is logical ticks. The network is a delay queue. Disk is an in-memory image that the
//! driver updates from each `Ready` before it sends that `Ready`'s messages, exactly like the
//! real driver does with the WAL. Killing a node drops its RAM (role, volatile log tail, KV)
//! and keeps the synced image.
//!
//! Checked every tick:
//! - election safety: at most one leader per term;
//! - committed-prefix agreement: no two nodes ever hold different entries at a committed index;
//! - commit index never decreases on a live node.
//!
//! Checked on demand: every acknowledged write is still in the committed prefix, and after the
//! cluster settles the leader's store holds the last acknowledged value of every key.

#![forbid(unsafe_code)]

mod chaos;
pub mod demo_session;
mod json;

use std::collections::{BTreeMap, BTreeSet};

use concord_kv::{encode_put, KvStore};
use concord_raft::{
    Config, Entry, EntryKind, Envelope, HardState, LogIndex, NodeId, ProposeError, RaftNode, Role,
    Term,
};

pub use chaos::{chaos_run, chaos_run_with, ChaosReport};

/// What a node has on disk. Updated only from a `Ready`, before its messages leave.
#[derive(Clone, Debug, Default)]
pub struct Disk {
    pub hard: HardState,
    pub entries: Vec<Entry>,
}

impl Disk {
    fn apply(&mut self, hard: Option<HardState>, truncate: Option<LogIndex>, entries: &[Entry]) {
        if let Some(from) = truncate {
            let keep = usize::try_from(from.saturating_sub(1)).unwrap_or(usize::MAX);
            self.entries.truncate(keep);
        }
        self.entries.extend(entries.iter().cloned());
        if let Some(hs) = hard {
            self.hard = hs;
        }
    }
}

pub struct Member {
    pub id: NodeId,
    pub raft: Option<RaftNode>,
    pub disk: Disk,
    pub kv: KvStore,
    pub restarts: u64,
    prev_commit: LogIndex,
    checked_through: LogIndex,
}

impl Member {
    pub fn alive(&self) -> bool {
        self.raft.is_some()
    }
}

/// A client write that reached a leader's log. It is acknowledged only once that leader, still
/// in the same term, has committed the entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ticket {
    pub node: NodeId,
    pub index: LogIndex,
    pub term: Term,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TicketState {
    Pending,
    Acked,
    /// The proposer lost leadership first. The write may or may not have survived.
    Lost,
    /// The client was told the write was saved, and then a different entry committed in its
    /// slot. Only the `LeaderOnly` baseline can reach this state.
    ConfirmedLost,
}

/// When the client is told "saved". Raft itself runs the same way under both.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AckPolicy {
    /// Concord: once the entry is committed, i.e. on a majority.
    #[default]
    Majority,
    /// Baseline for the demo: as soon as the leader has the entry on its own disk.
    LeaderOnly,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PutError {
    NoLeader,
    NodeDown,
    NotLeader { leader: Option<NodeId> },
}

#[derive(Clone, Copy, Debug)]
struct InFlight {
    deliver_at: u64,
    sent_at: u64,
}

pub struct Cluster {
    seed: u64,
    policy: AckPolicy,
    ids: Vec<NodeId>,
    pub members: BTreeMap<NodeId, Member>,
    net: Vec<(InFlight, Envelope)>,
    isolated: BTreeSet<NodeId>,
    /// Severed links, stored as `(low, high)` node-id pairs.
    cut_links: BTreeSet<(NodeId, NodeId)>,
    tick: u64,
    leaders_by_term: BTreeMap<Term, NodeId>,
    committed: BTreeMap<LogIndex, (Term, Vec<u8>)>,
    tickets: Vec<(Ticket, TicketState)>,
    acked: Vec<Ticket>,
    violations: Vec<String>,
}

impl Cluster {
    pub fn new(seed: u64) -> Self {
        Self::with_policy(seed, AckPolicy::Majority)
    }

    pub fn with_policy(seed: u64, policy: AckPolicy) -> Self {
        let ids = vec![1, 2, 3];
        let mut members = BTreeMap::new();
        for id in &ids {
            let raft = RaftNode::new(Self::config(seed, *id, 0, &ids)).ok();
            members.insert(
                *id,
                Member {
                    id: *id,
                    raft,
                    disk: Disk::default(),
                    kv: KvStore::new(),
                    restarts: 0,
                    prev_commit: 0,
                    checked_through: 0,
                },
            );
        }
        Self {
            seed,
            policy,
            ids,
            members,
            net: Vec::new(),
            isolated: BTreeSet::new(),
            cut_links: BTreeSet::new(),
            tick: 0,
            leaders_by_term: BTreeMap::new(),
            committed: BTreeMap::new(),
            tickets: Vec::new(),
            acked: Vec::new(),
            violations: Vec::new(),
        }
    }

    fn config(seed: u64, id: NodeId, restarts: u64, ids: &[NodeId]) -> Config {
        Config {
            id,
            peers: ids.to_vec(),
            election_tick_min: 10,
            election_tick_max: 20,
            heartbeat_ticks: 2,
            max_entries_per_msg: 32,
            rng_seed: seed
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(id.wrapping_mul(1_000_003))
                .wrapping_add(restarts.wrapping_mul(7_919)),
        }
    }

    pub fn tick_count(&self) -> u64 {
        self.tick
    }

    pub fn violations(&self) -> &[String] {
        &self.violations
    }

    pub fn acked(&self) -> &[Ticket] {
        &self.acked
    }

    pub fn tickets(&self) -> &[(Ticket, TicketState)] {
        &self.tickets
    }

    /// Writes the client was told were saved that can never come back.
    pub fn confirmed_lost(&self) -> usize {
        self.tickets
            .iter()
            .filter(|(_, s)| *s == TicketState::ConfirmedLost)
            .count()
    }

    pub fn is_isolated(&self, id: NodeId) -> bool {
        self.isolated.contains(&id)
    }

    pub fn alive_ids(&self) -> Vec<NodeId> {
        self.members
            .values()
            .filter(|m| m.alive())
            .map(|m| m.id)
            .collect()
    }

    /// The live node that currently believes it is leader, highest term first.
    pub fn leader(&self) -> Option<NodeId> {
        self.members
            .values()
            .filter_map(|m| m.raft.as_ref().map(|r| (m.id, r)))
            .filter(|(_, r)| r.role() == Role::Leader)
            .max_by_key(|(_, r)| r.term())
            .map(|(id, _)| id)
    }

    /// A leader that can currently reach a majority. This is the node a client should use.
    pub fn serving_leader(&self) -> Option<NodeId> {
        let quorum = self.ids.len() / 2 + 1;
        let leader = self.leader()?;
        let reachable = self
            .alive_ids()
            .into_iter()
            .filter(|id| self.reachable(leader, *id))
            .count();
        (reachable >= quorum).then_some(leader)
    }

    /// Whether `a` and `b` can exchange messages right now (both directions).
    pub fn reachable(&self, a: NodeId, b: NodeId) -> bool {
        a == b
            || (!self.isolated.contains(&a)
                && !self.isolated.contains(&b)
                && !self.cut_links.contains(&link(a, b)))
    }

    fn violate(&mut self, text: String) {
        self.violations.push(format!("tick {}: {text}", self.tick));
    }

    // ---- fault injection -------------------------------------------------------------

    /// Drop RAM. The synced disk image stays. Messages addressed to the node are lost.
    pub fn kill(&mut self, id: NodeId) {
        if let Some(m) = self.members.get_mut(&id) {
            m.raft = None;
            m.kv = KvStore::new();
        }
    }

    /// Like `kill`, but the node dies before its network buffer flushes: messages it sent
    /// that have not been delivered yet are dropped too.
    pub fn crash_now(&mut self, id: NodeId) {
        self.net.retain(|(_, env)| env.from != id);
        self.kill(id);
    }

    /// Rebuild from the synced image and replay committed entries into a fresh store.
    pub fn restart(&mut self, id: NodeId) {
        let ids = self.ids.clone();
        let seed = self.seed;
        let Some(m) = self.members.get_mut(&id) else {
            return;
        };
        if m.raft.is_some() {
            return;
        }
        m.restarts += 1;
        let cfg = Self::config(seed, id, m.restarts, &ids);
        match RaftNode::restore(cfg, m.disk.hard, m.disk.entries.clone(), 0) {
            Ok(node) => {
                m.prev_commit = m.disk.hard.commit;
                m.checked_through = 0;
                m.raft = Some(node);
                m.kv = KvStore::new();
            }
            Err(err) => self.violate(format!("node {id} failed to restore: {err}")),
        }
    }

    pub fn isolate(&mut self, id: NodeId) {
        self.isolated.insert(id);
    }

    /// Sever the single link between `a` and `b`. Their other links keep working. Raft sends
    /// messages directly, so nothing is relayed through the third node.
    pub fn cut_link(&mut self, a: NodeId, b: NodeId) {
        if a != b && self.ids.contains(&a) && self.ids.contains(&b) {
            self.cut_links.insert(link(a, b));
        }
    }

    pub fn restore_link(&mut self, a: NodeId, b: NodeId) {
        self.cut_links.remove(&link(a, b));
    }

    /// Clear every node isolation and every cut link.
    pub fn heal(&mut self) {
        self.isolated.clear();
        self.cut_links.clear();
    }

    // ---- client ----------------------------------------------------------------------

    /// Propose on `node`. The entry lives only in that node's RAM until the next `step`.
    pub fn put_on(&mut self, node: NodeId, key: &[u8], value: &[u8]) -> Result<Ticket, PutError> {
        let Ok(data) = encode_put(key, value) else {
            return Err(PutError::NoLeader);
        };
        let Some(m) = self.members.get_mut(&node) else {
            return Err(PutError::NodeDown);
        };
        let Some(raft) = m.raft.as_mut() else {
            return Err(PutError::NodeDown);
        };
        match raft.propose(data) {
            Ok((index, term)) => {
                let ticket = Ticket {
                    node,
                    index,
                    term,
                    key: key.to_vec(),
                    value: value.to_vec(),
                };
                self.tickets.push((ticket.clone(), TicketState::Pending));
                Ok(ticket)
            }
            Err(ProposeError::NotLeader { leader }) => Err(PutError::NotLeader { leader }),
        }
    }

    /// Propose on whichever leader can reach a majority.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Ticket, PutError> {
        let leader = self.serving_leader().ok_or(PutError::NoLeader)?;
        self.put_on(leader, key, value)
    }

    pub fn ticket_state(&self, ticket: &Ticket) -> TicketState {
        self.tickets
            .iter()
            .find(|(t, _)| t == ticket)
            .map(|(_, s)| s.clone())
            .unwrap_or(TicketState::Lost)
    }

    pub fn get(&self, node: NodeId, key: &[u8]) -> Option<Vec<u8>> {
        self.members
            .get(&node)
            .and_then(|m| m.kv.get(key).map(<[u8]>::to_vec))
    }

    // ---- time ------------------------------------------------------------------------

    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step();
        }
    }

    /// Run until `cond` holds or `max` ticks pass. Returns whether it held.
    pub fn run_until(&mut self, max: u64, mut cond: impl FnMut(&Cluster) -> bool) -> bool {
        for _ in 0..max {
            if cond(self) {
                return true;
            }
            self.step();
        }
        cond(self)
    }

    pub fn step(&mut self) {
        self.tick += 1;
        self.deliver();
        let ids = self.ids.clone();
        for id in &ids {
            if let Some(r) = self.members.get_mut(id).and_then(|m| m.raft.as_mut()) {
                r.tick();
            }
        }
        for id in &ids {
            self.drive(*id);
        }
        self.check_invariants();
        self.update_tickets();
    }

    fn deliver(&mut self) {
        let now = self.tick;
        let pending = std::mem::take(&mut self.net);
        let mut keep = Vec::new();
        for (flight, env) in pending {
            if flight.deliver_at > now {
                keep.push((flight, env));
                continue;
            }
            if !self.reachable(env.from, env.to) {
                continue;
            }
            if let Some(r) = self.members.get_mut(&env.to).and_then(|m| m.raft.as_mut()) {
                let _ = r.step(env);
            }
        }
        // Anything sent during delivery cannot arrive before the next tick.
        keep.append(&mut self.net);
        self.net = keep;
    }

    /// Persist, advance, apply, then send. The order is the whole point of the driver.
    fn drive(&mut self, id: NodeId) {
        let tick = self.tick;
        for _ in 0..64 {
            let Some(m) = self.members.get_mut(&id) else {
                return;
            };
            let Some(raft) = m.raft.as_mut() else {
                return;
            };
            if !raft.has_ready() {
                return;
            }
            let ready = raft.ready();
            m.disk
                .apply(ready.hard_state, ready.truncate_from, &ready.entries);
            let tip = raft.last_log_index();
            raft.advance(tip);
            for entry in &ready.committed {
                if entry.kind == EntryKind::Command {
                    if let Err(err) = m.kv.apply_data(&entry.data) {
                        let msg = format!("node {id} cannot apply entry {}: {err}", entry.index);
                        self.violate(msg);
                        return;
                    }
                }
            }
            for env in ready.messages {
                self.net.push((
                    InFlight {
                        deliver_at: tick + 1,
                        sent_at: tick,
                    },
                    env,
                ));
            }
        }
    }

    fn check_invariants(&mut self) {
        let ids = self.ids.clone();
        let mut found: Vec<String> = Vec::new();
        for id in &ids {
            let Some(m) = self.members.get_mut(id) else {
                continue;
            };
            let Some(raft) = m.raft.as_ref() else {
                continue;
            };
            let role = raft.role();
            let term = raft.term();
            let commit = raft.commit_index();
            if role == Role::Leader {
                match self.leaders_by_term.get(&term) {
                    Some(other) if other != id => found.push(format!(
                        "election safety: nodes {other} and {id} both led term {term}"
                    )),
                    Some(_) => {}
                    None => {
                        self.leaders_by_term.insert(term, *id);
                    }
                }
            }
            if commit < m.prev_commit {
                found.push(format!(
                    "node {id} commit index moved back {} -> {commit}",
                    m.prev_commit
                ));
            }
            m.prev_commit = commit;
            let from = m.checked_through.saturating_add(1);
            for index in from..=commit {
                let Some(entry) = raft.entry(index) else {
                    found.push(format!("node {id} commit {commit} but no entry {index}"));
                    break;
                };
                let seen = (entry.term, entry.data.clone());
                match self.committed.get(&index) {
                    Some(prev) if *prev != seen => found.push(format!(
                        "committed entries differ at index {index} (node {id})"
                    )),
                    Some(_) => {}
                    None => {
                        self.committed.insert(index, seen);
                    }
                }
            }
            m.checked_through = m.checked_through.max(commit);
        }
        for text in found {
            self.violate(text);
        }
    }

    fn update_tickets(&mut self) {
        let mut newly_acked = Vec::new();
        let mut newly_lost = Vec::new();
        let policy = self.policy;
        for (ticket, state) in &mut self.tickets {
            if *state == TicketState::Acked {
                // A different entry committed in this slot: the confirmed write is gone for good.
                let replaced = self
                    .committed
                    .get(&ticket.index)
                    .is_some_and(|(term, _)| *term != ticket.term);
                if replaced {
                    *state = TicketState::ConfirmedLost;
                    newly_lost.push(ticket.clone());
                }
                continue;
            }
            if *state != TicketState::Pending {
                continue;
            }
            if policy == AckPolicy::LeaderOnly {
                let on_disk = self.members.get(&ticket.node).is_some_and(|m| {
                    m.alive()
                        && usize::try_from(ticket.index - 1)
                            .ok()
                            .and_then(|i| m.disk.entries.get(i))
                            .is_some_and(|e| e.term == ticket.term)
                });
                if on_disk {
                    *state = TicketState::Acked;
                    newly_acked.push(ticket.clone());
                    continue;
                }
            }
            let Some(raft) = self.members.get(&ticket.node).and_then(|m| m.raft.as_ref()) else {
                *state = TicketState::Lost;
                continue;
            };
            if raft.role() != Role::Leader || raft.term() != ticket.term {
                *state = TicketState::Lost;
                continue;
            }
            let same = raft
                .entry(ticket.index)
                .map(|e| e.term == ticket.term)
                .unwrap_or(false);
            if !same {
                *state = TicketState::Lost;
            } else if raft.commit_index() >= ticket.index {
                *state = TicketState::Acked;
                newly_acked.push(ticket.clone());
            }
        }
        self.acked.extend(newly_acked);
        // Expected under the baseline; a safety bug under Concord.
        if policy == AckPolicy::Majority {
            for ticket in newly_lost {
                self.violate(format!(
                    "{}={} was confirmed, but a different entry committed in slot {}.",
                    show(&ticket.key),
                    show(&ticket.value),
                    ticket.index
                ));
            }
        }
    }

    // ---- durability checks -------------------------------------------------------------

    /// Every acknowledged write must still be in the committed prefix with the same bytes.
    pub fn check_acked_in_committed_prefix(&self) -> Vec<String> {
        let mut bad = Vec::new();
        for t in &self.acked {
            let want = encode_put(&t.key, &t.value).unwrap_or_default();
            match self.committed.get(&t.index) {
                Some((term, data)) if *term == t.term && *data == want => {}
                _ => bad.push(format!(
                    "acked write {}={} (index {}, term {}) is not in the committed prefix",
                    show(&t.key),
                    show(&t.value),
                    t.index,
                    t.term
                )),
            }
        }
        bad
    }

    /// After the cluster settles, every live node's store must hold the last acked value per key.
    pub fn check_acked_in_stores(&self) -> Vec<String> {
        let mut last: BTreeMap<&[u8], &Ticket> = BTreeMap::new();
        for t in &self.acked {
            match last.get(t.key.as_slice()) {
                Some(prev) if prev.index >= t.index => {}
                _ => {
                    last.insert(t.key.as_slice(), t);
                }
            }
        }
        let mut bad = Vec::new();
        for m in self.members.values().filter(|m| m.alive()) {
            for (key, t) in &last {
                // A later unacked-but-committed write to the same key is legal; compare the
                // store with the committed log instead of only the ack.
                let expect = self.latest_committed_value(key);
                let got = m.kv.get(key).map(<[u8]>::to_vec);
                if got != expect || got.is_none() {
                    bad.push(format!(
                        "node {} has {:?} for {}, expected {:?} (acked index {})",
                        m.id,
                        got.as_deref().map(show),
                        show(key),
                        expect.as_deref().map(show),
                        t.index
                    ));
                }
            }
        }
        bad
    }

    fn latest_committed_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut out = None;
        for (_, data) in self.committed.values() {
            let mut kv = KvStore::new();
            if kv.apply_data(data).is_ok() {
                if let Some(v) = kv.get(key) {
                    out = Some(v.to_vec());
                }
            }
        }
        out
    }

    /// True when all live, connected nodes share a leader and the same commit index.
    pub fn converged(&self) -> bool {
        let Some(leader) = self.serving_leader() else {
            return false;
        };
        let Some(lr) = self.members.get(&leader).and_then(|m| m.raft.as_ref()) else {
            return false;
        };
        let commit = lr.commit_index();
        if !self.isolated.is_empty() || !self.cut_links.is_empty() {
            return false;
        }
        self.members
            .values()
            .filter_map(|m| m.raft.as_ref())
            .all(|r| r.leader() == Some(leader) && r.commit_index() == commit && commit > 0)
    }

    pub fn in_flight(&self) -> impl Iterator<Item = (&Envelope, u64)> {
        self.net.iter().map(|(f, e)| (e, f.sent_at))
    }

    pub fn to_json(&self) -> String {
        json::cluster(self)
    }
}

fn link(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    (a.min(b), a.max(b))
}

pub(crate) fn show(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
