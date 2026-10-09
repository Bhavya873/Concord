//! Raft core types <-> protobuf messages.

use concord_raft::{
    AppendEntries, AppendEntriesResp, Entry, EntryKind, RequestVote, RequestVoteResp,
};

use crate::pb;

pub fn entry_to_pb(e: &Entry) -> pb::LogEntry {
    pb::LogEntry {
        index: e.index,
        term: e.term,
        kind: e.kind.as_u32(),
        data: e.data.clone(),
    }
}

pub fn entry_from_pb(e: pb::LogEntry) -> Option<Entry> {
    Some(Entry {
        index: e.index,
        term: e.term,
        kind: EntryKind::from_u32(e.kind)?,
        data: e.data,
    })
}

pub fn vote_to_pb(m: &RequestVote) -> pb::RequestVoteRequest {
    pb::RequestVoteRequest {
        term: m.term,
        candidate_id: m.candidate_id,
        last_log_index: m.last_log_index,
        last_log_term: m.last_log_term,
    }
}

pub fn vote_from_pb(m: pb::RequestVoteRequest) -> RequestVote {
    RequestVote {
        term: m.term,
        candidate_id: m.candidate_id,
        last_log_index: m.last_log_index,
        last_log_term: m.last_log_term,
    }
}

pub fn vote_resp_to_pb(m: &RequestVoteResp) -> pb::RequestVoteResponse {
    pb::RequestVoteResponse {
        term: m.term,
        vote_granted: m.vote_granted,
    }
}

pub fn vote_resp_from_pb(m: pb::RequestVoteResponse) -> RequestVoteResp {
    RequestVoteResp {
        term: m.term,
        vote_granted: m.vote_granted,
    }
}

pub fn append_to_pb(m: &AppendEntries) -> pb::AppendEntriesRequest {
    pb::AppendEntriesRequest {
        term: m.term,
        leader_id: m.leader_id,
        prev_log_index: m.prev_log_index,
        prev_log_term: m.prev_log_term,
        entries: m.entries.iter().map(entry_to_pb).collect(),
        leader_commit: m.leader_commit,
    }
}

/// `None` when an entry has an unknown kind: the whole request is rejected.
pub fn append_from_pb(m: pb::AppendEntriesRequest) -> Option<AppendEntries> {
    let entries = m
        .entries
        .into_iter()
        .map(entry_from_pb)
        .collect::<Option<Vec<_>>>()?;
    Some(AppendEntries {
        term: m.term,
        leader_id: m.leader_id,
        prev_log_index: m.prev_log_index,
        prev_log_term: m.prev_log_term,
        entries,
        leader_commit: m.leader_commit,
    })
}

pub fn append_resp_to_pb(m: &AppendEntriesResp) -> pb::AppendEntriesResponse {
    pb::AppendEntriesResponse {
        term: m.term,
        success: m.success,
        match_index: m.match_index,
        conflict_index: m.conflict_index,
        conflict_term: m.conflict_term,
    }
}

pub fn append_resp_from_pb(m: pb::AppendEntriesResponse) -> AppendEntriesResp {
    AppendEntriesResp {
        term: m.term,
        success: m.success,
        match_index: m.match_index,
        conflict_index: m.conflict_index,
        conflict_term: m.conflict_term,
    }
}
