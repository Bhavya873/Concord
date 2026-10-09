use thiserror::Error;

use crate::{LogIndex, NodeId};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftError {
    #[error("invalid raft config: {0}")]
    BadConfig(&'static str),
    #[error("log gap: expected index {expected}, got {got}")]
    LogGap { expected: LogIndex, got: LogIndex },
    #[error("refusing to truncate committed index {index}")]
    TruncateCommitted { index: LogIndex },
    #[error("commit index {commit} is past the end of the log")]
    CommitPastLog { commit: LogIndex },
    #[error("applied index {applied} is past commit {commit}")]
    AppliedPastCommit { applied: LogIndex, commit: LogIndex },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProposeError {
    #[error("not the leader")]
    NotLeader { leader: Option<NodeId> },
}
