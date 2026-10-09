//! Raft role state machine. Callers sync [`Ready`] before sending [`Ready::messages`].

use std::collections::{BTreeMap, BTreeSet};

use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};

use crate::commit::highest_committable;
use crate::log::RaftLog;
use crate::message::{
    AppendEntries, AppendEntriesResp, Envelope, Message, RequestVote, RequestVoteResp,
};
use crate::{
    Config, Entry, EntryKind, HardState, LogIndex, NodeId, ProposeError, RaftError, Role, Term,
};

/// Output of one step. Persist `hard_state`, `truncate_from`, and `entries` before sending `messages`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ready {
    pub hard_state: Option<HardState>,
    pub truncate_from: Option<LogIndex>,
    pub entries: Vec<Entry>,
    pub messages: Vec<Envelope>,
    pub committed: Vec<Entry>,
}

pub struct RaftNode {
    id: NodeId,
    peers: Vec<NodeId>,
    election_tick_min: u32,
    election_tick_max: u32,
    heartbeat_ticks: u32,
    max_entries_per_msg: usize,
    rng: ChaCha8Rng,

    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    log: RaftLog,
    commit_index: LogIndex,
    last_applied: LogIndex,
    /// Last index the caller has reported via [`RaftNode::advance`].
    persisted_through: LogIndex,
    /// Last index already handed to the caller in a [`Ready`].
    sent_to_storage: LogIndex,
    truncate_from: Option<LogIndex>,

    election_elapsed: u32,
    election_timeout: u32,
    heartbeat_elapsed: u32,

    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
    votes: BTreeSet<NodeId>,

    msgs: Vec<Envelope>,
    hs_dirty: bool,
    committed_buf: Vec<Entry>,
}

impl RaftNode {
    pub fn new(cfg: Config) -> Result<Self, RaftError> {
        Self::restore(cfg, HardState::default(), Vec::new(), 0)
    }

    /// Rebuild a follower from durable state. `last_applied` must already be reflected in the state machine.
    pub fn restore(
        cfg: Config,
        hard: HardState,
        entries: Vec<Entry>,
        last_applied: LogIndex,
    ) -> Result<Self, RaftError> {
        cfg.validate()?;
        let log = RaftLog::from_entries(entries)?;
        if hard.commit > log.last_index() {
            return Err(RaftError::CommitPastLog {
                commit: hard.commit,
            });
        }
        if last_applied > hard.commit {
            return Err(RaftError::AppliedPastCommit {
                applied: last_applied,
                commit: hard.commit,
            });
        }
        let mut peers = cfg.peers.clone();
        peers.sort_unstable();
        let last = log.last_index();
        let mut node = Self {
            id: cfg.id,
            peers,
            election_tick_min: cfg.election_tick_min,
            election_tick_max: cfg.election_tick_max,
            heartbeat_ticks: cfg.heartbeat_ticks,
            max_entries_per_msg: cfg.max_entries_per_msg,
            rng: ChaCha8Rng::seed_from_u64(cfg.rng_seed),
            role: Role::Follower,
            current_term: hard.term,
            voted_for: hard.voted_for,
            leader_id: None,
            log,
            commit_index: hard.commit,
            last_applied,
            persisted_through: last,
            sent_to_storage: last,
            truncate_from: None,
            election_elapsed: 0,
            election_timeout: cfg.election_tick_min,
            heartbeat_elapsed: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            votes: BTreeSet::new(),
            msgs: Vec::new(),
            hs_dirty: false,
            committed_buf: Vec::new(),
        };
        node.reset_election_timer();
        Ok(node)
    }

    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed = self.heartbeat_elapsed.saturating_add(1);
                if self.heartbeat_elapsed >= self.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.bcast_append();
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed = self.election_elapsed.saturating_add(1);
                if self.election_elapsed >= self.election_timeout {
                    self.start_election();
                }
            }
        }
    }

    pub fn step(&mut self, env: Envelope) -> Result<(), RaftError> {
        if env.to != self.id || env.from == self.id || !self.is_peer(env.from) {
            return Ok(());
        }
        match env.msg {
            Message::RequestVote(msg) => self.on_request_vote(env.from, msg),
            Message::RequestVoteResp(msg) => self.on_request_vote_resp(env.from, msg),
            Message::AppendEntries(msg) => self.on_append_entries(env.from, msg),
            Message::AppendEntriesResp(msg) => self.on_append_entries_resp(env.from, msg),
        }
        Ok(())
    }

    pub fn propose(&mut self, data: Vec<u8>) -> Result<(LogIndex, Term), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let index = self.append_entry(EntryKind::Command, data);
        self.bcast_append();
        Ok((index, self.current_term))
    }

    pub fn has_ready(&self) -> bool {
        self.hs_dirty
            || self.truncate_from.is_some()
            || self.sent_to_storage < self.log.last_index()
            || !self.msgs.is_empty()
            || !self.committed_buf.is_empty()
            || self.has_unapplied()
    }

    pub fn ready(&mut self) -> Ready {
        self.collect_committed();
        let hard_state = if self.hs_dirty {
            self.hs_dirty = false;
            Some(self.hard_state())
        } else {
            None
        };
        let truncate_from = self.truncate_from.take();
        let entries = self.log.entries_after(self.sent_to_storage);
        if let Some(last) = entries.last() {
            self.sent_to_storage = last.index;
        }
        Ready {
            hard_state,
            truncate_from,
            entries,
            messages: std::mem::take(&mut self.msgs),
            committed: std::mem::take(&mut self.committed_buf),
        }
    }

    /// Caller has synced every entry up through `persisted_through`.
    pub fn advance(&mut self, persisted_through: LogIndex) {
        if persisted_through > self.persisted_through {
            self.persisted_through = persisted_through;
        }
        if self.role == Role::Leader {
            self.maybe_commit();
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> Term {
        self.current_term
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader_id
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }

    pub fn entry(&self, index: LogIndex) -> Option<Entry> {
        self.log.get(index)
    }

    pub fn hard_state(&self) -> HardState {
        HardState {
            term: self.current_term,
            voted_for: self.voted_for,
            commit: self.commit_index,
        }
    }

    pub fn debug_line(&self) -> String {
        format!(
            "id={} role={} term={} leader={:?} commit={} applied={} last={} voted={:?}",
            self.id,
            self.role.as_str(),
            self.current_term,
            self.leader_id,
            self.commit_index,
            self.last_applied,
            self.log.last_index(),
            self.voted_for
        )
    }

    fn start_election(&mut self) {
        let Some(term) = self.current_term.checked_add(1) else {
            return;
        };
        self.current_term = term;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.hs_dirty = true;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer();
        if self.votes.len() >= self.quorum() {
            self.become_leader();
            return;
        }
        let last_index = self.log.last_index();
        let last_term = self.log.last_term();
        let term = self.current_term;
        let id = self.id;
        for peer in self.others() {
            self.msgs.push(Envelope {
                from: id,
                to: peer,
                msg: Message::RequestVote(RequestVote {
                    term,
                    candidate_id: id,
                    last_log_index: last_index,
                    last_log_term: last_term,
                }),
            });
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.votes.clear();
        // Paper §5.4.2: the noop is a current-term entry, so a later quorum commits the prefix.
        let noop_index = self.append_entry(EntryKind::Noop, Vec::new());
        for peer in self.others() {
            self.next_index.insert(peer, noop_index);
            self.match_index.insert(peer, 0);
        }
        self.bcast_append();
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.current_term {
            self.current_term = term;
            // A new term is the only time `voted_for` is cleared (election safety).
            self.voted_for = None;
            self.hs_dirty = true;
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.votes.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.reset_election_timer();
    }

    fn on_request_vote(&mut self, from: NodeId, msg: RequestVote) {
        if msg.candidate_id != from {
            return;
        }
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
        }
        let grant = msg.term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(msg.candidate_id))
            && self
                .log
                .is_up_to_date(msg.last_log_term, msg.last_log_index);
        if grant {
            self.voted_for = Some(msg.candidate_id);
            self.hs_dirty = true;
            self.reset_election_timer();
        }
        let term = self.current_term;
        self.msgs.push(Envelope {
            from: self.id,
            to: from,
            msg: Message::RequestVoteResp(RequestVoteResp {
                term,
                vote_granted: grant,
            }),
        });
    }

    fn on_request_vote_resp(&mut self, from: NodeId, msg: RequestVoteResp) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
            return;
        }
        if self.role != Role::Candidate || msg.term != self.current_term {
            return;
        }
        if msg.vote_granted {
            self.votes.insert(from);
            if self.votes.len() >= self.quorum() {
                self.become_leader();
            }
        }
    }

    fn on_append_entries(&mut self, from: NodeId, msg: AppendEntries) {
        if msg.leader_id != from {
            return;
        }
        if msg.term > self.current_term {
            self.become_follower(msg.term, Some(msg.leader_id));
        } else if msg.term < self.current_term {
            self.send_ae_resp(from, false, 0, 0, 0);
            return;
        }

        // Same-term AppendEntries to a leader would mean two leaders. Reject it.
        if self.role == Role::Leader {
            self.send_ae_resp(from, false, 0, 0, 0);
            return;
        }
        self.become_follower(self.current_term, Some(msg.leader_id));

        if msg.prev_log_index > self.log.last_index() {
            let conflict_index = self.log.last_index().saturating_add(1);
            self.send_ae_resp(from, false, 0, conflict_index, 0);
            return;
        }
        match self.log.term(msg.prev_log_index) {
            Some(term) if term == msg.prev_log_term => {}
            Some(term) => {
                let conflict_index = self
                    .log
                    .first_index_of_term(term)
                    .unwrap_or(msg.prev_log_index);
                self.send_ae_resp(from, false, 0, conflict_index, term);
                return;
            }
            None => {
                let conflict_index = self.log.last_index().saturating_add(1);
                self.send_ae_resp(from, false, 0, conflict_index, 0);
                return;
            }
        }

        if !self.append_resolved_entries(msg.prev_log_index, &msg.entries) {
            self.send_ae_resp(from, false, 0, self.log.last_index().saturating_add(1), 0);
            return;
        }

        let matched = msg
            .prev_log_index
            .saturating_add(u64_len(msg.entries.len()));
        if msg.leader_commit > self.commit_index {
            let new_commit = msg.leader_commit.min(matched).min(self.log.last_index());
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
                self.hs_dirty = true;
                self.collect_committed();
            }
        }
        self.send_ae_resp(from, true, matched, 0, 0);
    }

    /// Append the suffix of `entries` that is not already present with the same term.
    /// Returns false when the batch is illegal. A `false` return does not truncate.
    fn append_resolved_entries(&mut self, prev: LogIndex, entries: &[Entry]) -> bool {
        let mut append_from: Option<usize> = None;
        for (offset, entry) in entries.iter().enumerate() {
            let Some(delta) = u64::try_from(offset).ok() else {
                return false;
            };
            let expected = prev.saturating_add(delta).saturating_add(1);
            if entry.index != expected || entry.term == 0 {
                return false;
            }
            match self.log.term(entry.index) {
                Some(term) if term == entry.term => {}
                Some(_) => {
                    if entry.index <= self.commit_index {
                        return false;
                    }
                    append_from = Some(offset);
                    break;
                }
                None => {
                    if entry.index != self.log.last_index().saturating_add(1) {
                        return false;
                    }
                    append_from = Some(offset);
                    break;
                }
            }
        }
        let Some(offset) = append_from else {
            return true;
        };
        let Some(first) = entries.get(offset).cloned() else {
            return false;
        };
        if self.log.term(first.index).is_some() && self.truncate_log(first.index).is_err() {
            return false;
        }
        if self.log.last_index().saturating_add(1) != first.index {
            return false;
        }
        if self.log.append(first).is_err() {
            return false;
        }
        for entry in entries.iter().skip(offset.saturating_add(1)) {
            if self.log.append(entry.clone()).is_err() {
                return false;
            }
        }
        true
    }

    fn on_append_entries_resp(&mut self, from: NodeId, msg: AppendEntriesResp) {
        if msg.term > self.current_term {
            self.become_follower(msg.term, None);
            return;
        }
        if self.role != Role::Leader || msg.term != self.current_term {
            return;
        }
        if msg.success {
            let prev = self.match_index.get(&from).copied().unwrap_or(0);
            if msg.match_index > prev {
                self.match_index.insert(from, msg.match_index);
                self.next_index
                    .insert(from, msg.match_index.saturating_add(1));
                self.maybe_commit();
                let next = self.next_index.get(&from).copied().unwrap_or(1);
                if next <= self.log.last_index() {
                    self.send_append(from);
                }
            }
        } else {
            let matched = self.match_index.get(&from).copied().unwrap_or(0);
            let next = self.backtrack_next(from, &msg, matched);
            self.next_index.insert(from, next);
            self.send_append(from);
        }
    }

    fn backtrack_next(&self, from: NodeId, msg: &AppendEntriesResp, matched: LogIndex) -> LogIndex {
        let hinted = if msg.conflict_term == 0 {
            msg.conflict_index
        } else if let Some(last) = self.log.last_index_of_term(msg.conflict_term) {
            last.saturating_add(1)
        } else {
            msg.conflict_index.max(1)
        };
        let floor = matched.saturating_add(1);
        let mut next = hinted.max(floor).max(1);
        let current = self.next_index.get(&from).copied().unwrap_or(1).max(floor);
        if next >= current && current > floor {
            next = current.saturating_sub(1).max(floor);
        }
        next
    }

    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let matched = self.matched_indexes();
        let quorum = self.quorum();
        let new_commit = highest_committable(
            self.commit_index,
            self.current_term,
            matched,
            quorum,
            |index| self.log.term(index),
        );
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.hs_dirty = true;
            self.collect_committed();
            self.bcast_append();
        }
    }

    fn matched_indexes(&self) -> Vec<LogIndex> {
        self.peers
            .iter()
            .map(|id| {
                if *id == self.id {
                    self.persisted_through
                } else {
                    self.match_index.get(id).copied().unwrap_or(0)
                }
            })
            .collect()
    }

    fn bcast_append(&mut self) {
        for peer in self.others() {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let last = self.log.last_index();
        let mut next = self.next_index.get(&to).copied().unwrap_or(1).max(1);
        let end = last.saturating_add(1);
        if next > end {
            next = end;
            self.next_index.insert(to, next);
        }
        let prev = next.saturating_sub(1);
        let prev_term = self.log.term(prev).unwrap_or(0);
        let entries = self.log.slice_from(next, self.max_entries_per_msg);
        let term = self.current_term;
        let leader_id = self.id;
        let leader_commit = self.commit_index;
        self.msgs.push(Envelope {
            from: self.id,
            to,
            msg: Message::AppendEntries(AppendEntries {
                term,
                leader_id,
                prev_log_index: prev,
                prev_log_term: prev_term,
                entries,
                leader_commit,
            }),
        });
    }

    fn send_ae_resp(
        &mut self,
        to: NodeId,
        success: bool,
        match_index: LogIndex,
        conflict_index: LogIndex,
        conflict_term: Term,
    ) {
        let term = self.current_term;
        self.msgs.push(Envelope {
            from: self.id,
            to,
            msg: Message::AppendEntriesResp(AppendEntriesResp {
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
            }),
        });
    }

    fn append_entry(&mut self, kind: EntryKind, data: Vec<u8>) -> LogIndex {
        let index = self.log.last_index().saturating_add(1);
        let entry = Entry {
            index,
            term: self.current_term,
            kind,
            data,
        };
        if self.log.append(entry).is_err() {
            return self.log.last_index();
        }
        index
    }

    fn truncate_log(&mut self, from: LogIndex) -> Result<(), RaftError> {
        if from == 0 || from <= self.commit_index {
            return Err(RaftError::TruncateCommitted { index: from });
        }
        self.log.truncate_from(from);
        self.truncate_from = Some(match self.truncate_from {
            Some(existing) => existing.min(from),
            None => from,
        });
        if self.sent_to_storage >= from {
            self.sent_to_storage = from.saturating_sub(1);
        }
        if self.persisted_through >= from {
            self.persisted_through = from.saturating_sub(1);
        }
        Ok(())
    }

    fn collect_committed(&mut self) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied.saturating_add(1);
            let Some(entry) = self.log.get(next) else {
                break;
            };
            self.last_applied = next;
            self.committed_buf.push(entry);
        }
    }

    fn has_unapplied(&self) -> bool {
        let next = self.last_applied.saturating_add(1);
        next <= self.commit_index && self.log.get(next).is_some()
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.draw_timeout();
    }

    fn draw_timeout(&mut self) -> u32 {
        let min = self.election_tick_min;
        let max = self.election_tick_max;
        if min >= max {
            return min;
        }
        let span = max.saturating_sub(min).saturating_add(1);
        if span == 0 {
            return min;
        }
        min.saturating_add(self.rng.next_u32() % span)
    }

    fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    fn others(&self) -> Vec<NodeId> {
        self.peers
            .iter()
            .copied()
            .filter(|id| *id != self.id)
            .collect()
    }

    fn is_peer(&self, id: NodeId) -> bool {
        self.peers.binary_search(&id).is_ok()
    }
}

fn u64_len(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::RequestVote;
    use std::collections::BTreeMap;

    fn cfg(id: u64, min: u32, max: u32) -> Config {
        Config {
            id,
            peers: vec![1, 2, 3],
            election_tick_min: min,
            election_tick_max: max,
            heartbeat_ticks: 2,
            max_entries_per_msg: 32,
            rng_seed: id,
        }
    }

    fn vote(from: u64, to: u64, term: u64, last_index: u64, last_term: u64) -> Envelope {
        Envelope {
            from,
            to,
            msg: Message::RequestVote(RequestVote {
                term,
                candidate_id: from,
                last_log_index: last_index,
                last_log_term: last_term,
            }),
        }
    }

    fn take_vote_resp(ready: &Ready) -> RequestVoteResp {
        match ready.messages.first().map(|env| env.msg.clone()) {
            Some(Message::RequestVoteResp(resp)) => resp,
            other => panic!("expected vote response, got {other:?}"),
        }
    }

    #[test]
    fn grants_vote_when_candidate_log_is_up_to_date() {
        let mut node = RaftNode::new(cfg(2, 10, 10)).unwrap();
        node.step(vote(1, 2, 1, 0, 0)).unwrap();
        let ready = node.ready();
        assert_eq!(ready.hard_state.map(|hs| hs.voted_for), Some(Some(1)));
        let resp = take_vote_resp(&ready);
        assert!(resp.vote_granted);
        assert_eq!(resp.term, 1);
    }

    #[test]
    fn rejects_vote_when_candidate_log_is_older() {
        let mut node = RaftNode::new(cfg(2, 10, 10)).unwrap();
        node.step(append(1, 2, 1, 0, 0, vec![cmd(1, 1, b"a")], 0))
            .unwrap();
        let _ = node.ready();
        node.advance(node.last_log_index());
        node.step(vote(3, 2, 2, 0, 0)).unwrap();
        let ready = node.ready();
        let resp = take_vote_resp(&ready);
        assert!(!resp.vote_granted);
        assert_eq!(node.term(), 2);
        assert_eq!(node.hard_state().voted_for, None);
    }

    #[test]
    fn rejects_second_vote_in_the_same_term() {
        let mut node = RaftNode::new(cfg(2, 10, 10)).unwrap();
        node.step(vote(1, 2, 1, 0, 0)).unwrap();
        let _ = node.ready();
        node.step(vote(3, 2, 1, 0, 0)).unwrap();
        let ready = node.ready();
        assert!(!take_vote_resp(&ready).vote_granted);
        assert_eq!(node.hard_state().voted_for, Some(1));
    }

    #[test]
    fn steps_down_on_higher_term() {
        let mut node = RaftNode::new(cfg(1, 1, 1)).unwrap();
        node.tick();
        assert_eq!(node.role(), Role::Candidate);
        assert_eq!(node.term(), 1);
        node.step(vote(2, 1, 4, 0, 0)).unwrap();
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 4);
    }

    #[test]
    fn stale_append_does_not_truncate() {
        let mut node = RaftNode::new(cfg(2, 50, 50)).unwrap();
        node.step(append(
            1,
            2,
            1,
            0,
            0,
            vec![cmd(1, 1, b"a"), cmd(2, 1, b"b")],
            0,
        ))
        .unwrap();
        let _ = node.ready();
        node.advance(node.last_log_index());
        node.step(append(1, 2, 1, 0, 0, vec![cmd(1, 1, b"a")], 0))
            .unwrap();
        let ready = node.ready();
        assert!(ready.truncate_from.is_none());
        assert_eq!(node.entry(2).map(|e| e.data), Some(b"b".to_vec()));
    }

    #[test]
    fn three_nodes_elect_a_leader_append_noop_and_commit_a_write() {
        let mut group = Group::new();
        group.pump(10);
        let leaders: Vec<_> = group
            .nodes
            .values()
            .filter(|node| node.role() == Role::Leader)
            .map(|node| node.id)
            .collect();
        assert_eq!(leaders, vec![1], "expected node 1 to win the short timeout");
        let leader = group.nodes.get(&1).unwrap();
        assert_eq!(leader.entry(1).map(|e| e.kind), Some(EntryKind::Noop));
        assert_eq!(leader.entry(1).map(|e| e.term), Some(leader.term()));

        let (index, term) = group
            .nodes
            .get_mut(&1)
            .unwrap()
            .propose(b"k=v".to_vec())
            .unwrap();
        group.exchange();
        for node in group.nodes.values() {
            assert!(node.commit_index() >= index, "{}", node.debug_line());
            let entry = node.entry(index).expect("replicated");
            assert_eq!(entry.term, term);
            assert_eq!(entry.data, b"k=v");
            assert_eq!(entry.kind, EntryKind::Command);
        }
    }

    fn cmd(index: u64, term: u64, data: &[u8]) -> Entry {
        Entry {
            index,
            term,
            kind: EntryKind::Command,
            data: data.to_vec(),
        }
    }

    fn append(
        from: u64,
        to: u64,
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    ) -> Envelope {
        Envelope {
            from,
            to,
            msg: Message::AppendEntries(AppendEntries {
                term,
                leader_id: from,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit,
            }),
        }
    }

    struct Group {
        nodes: BTreeMap<NodeId, RaftNode>,
    }

    impl Group {
        fn new() -> Self {
            let mut nodes = BTreeMap::new();
            for id in 1..=3 {
                let timeout = if id == 1 { 3 } else { 40 };
                nodes.insert(id, RaftNode::new(cfg(id, timeout, timeout)).unwrap());
            }
            Self { nodes }
        }

        fn pump(&mut self, ticks: u32) {
            for _ in 0..ticks {
                let ids: Vec<_> = self.nodes.keys().copied().collect();
                for id in ids {
                    if let Some(node) = self.nodes.get_mut(&id) {
                        node.tick();
                    }
                }
                self.exchange();
            }
        }

        fn exchange(&mut self) {
            for _ in 0..64 {
                let mut outbound = Vec::new();
                let ids: Vec<_> = self.nodes.keys().copied().collect();
                let mut progress = false;
                for id in ids {
                    while let Some(node) = self.nodes.get_mut(&id) {
                        if !node.has_ready() {
                            break;
                        }
                        progress = true;
                        let ready = node.ready();
                        let tip = node.last_log_index();
                        outbound.extend(ready.messages);
                        node.advance(tip);
                    }
                }
                if outbound.is_empty() {
                    if !progress {
                        break;
                    }
                    continue;
                }
                for env in outbound {
                    let to = env.to;
                    if let Some(node) = self.nodes.get_mut(&to) {
                        node.step(env).unwrap();
                    }
                }
            }
        }
    }
}
