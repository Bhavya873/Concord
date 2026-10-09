//! One-file write-ahead log.
//!
//! [`Wal::persist`] writes the records and calls `File::sync_data` before it returns.
//! Vote grants and AppendEntries successes must not be sent until that call returns.

#![forbid(unsafe_code)]

mod wal;

pub use wal::{Wal, WalError};
