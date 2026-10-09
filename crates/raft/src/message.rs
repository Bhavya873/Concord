use crate::{Entry, LogIndex, NodeId, Term};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    RequestVote(RequestVote),
    RequestVoteResp(RequestVoteResp),
    AppendEntries(AppendEntries),
    AppendEntriesResp(AppendEntriesResp),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestVoteResp {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Entry>,
    pub leader_commit: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
    /// On success, `prev_log_index + entries.len()`.
    pub match_index: LogIndex,
    /// On failure, first index the follower wants the leader to retry from.
    pub conflict_index: LogIndex,
    /// On failure, `0` means the follower's log is shorter than `prev_log_index`.
    pub conflict_term: Term,
}
