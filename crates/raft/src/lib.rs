//! Deterministic Raft core. Logical ticks only: no I/O, no Tokio, no wall clock.
//!
//! Drivers persist [`Ready`] (fsync) before sending [`Ready::messages`].

#![forbid(unsafe_code)]

mod commit;
mod config;
mod error;
mod log;
mod message;
mod node;
mod types;

pub use commit::highest_committable;
pub use config::Config;
pub use error::{ProposeError, RaftError};
pub use message::{
    AppendEntries, AppendEntriesResp, Envelope, Message, RequestVote, RequestVoteResp,
};
pub use node::{RaftNode, Ready};
pub use types::{Entry, EntryKind, HardState, LogIndex, NodeId, Role, Term};
