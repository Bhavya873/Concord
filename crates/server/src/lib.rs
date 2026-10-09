//! gRPC front end for one Concord replica.
//!
//! One driver task owns the Raft core, the WAL and the store. RPC handlers only send it events.
//! The driver persists each `Ready` (fsync) before it sends that `Ready`'s messages.

#![forbid(unsafe_code)]

mod client;
mod convert;
mod driver;
mod service;

pub mod pb {
    tonic::include_proto!("concord");
}

pub use client::{ClientError, KvCluster};
pub use driver::{start, NodeHandle, NodeSpec, ServerError};
