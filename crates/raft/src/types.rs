//! Identifiers and log records shared with storage and the server.

/// Voter id. `0` is never a node; callers use it only as "none" on the wire.
pub type NodeId = u64;

/// Raft term. Monotonic for a node except across a restore of older durable state, which does not happen.
pub type Term = u64;

/// 1-based log index. `0` means "before the first entry".
pub type LogIndex = u64;

/// What an entry carries. A new leader appends [`EntryKind::Noop`] in its term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryKind {
    Noop = 1,
    Command = 2,
}

impl EntryKind {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::Noop),
            2 => Some(Self::Command),
            _ => None,
        }
    }

    pub fn as_u32(self) -> u32 {
        match self {
            Self::Noop => 1,
            Self::Command => 2,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub kind: EntryKind,
    pub data: Vec<u8>,
}

/// Durable node metadata. `commit` is a hint; safety comes from the log on a quorum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub commit: LogIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Follower => "follower",
            Self::Candidate => "candidate",
            Self::Leader => "leader",
        }
    }
}
