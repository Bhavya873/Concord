use crate::{NodeId, RaftError};

/// Static cluster. Membership does not change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub id: NodeId,
    /// Every voter, including `id`.
    pub peers: Vec<NodeId>,
    pub election_tick_min: u32,
    pub election_tick_max: u32,
    pub heartbeat_ticks: u32,
    pub max_entries_per_msg: usize,
    /// Seed for election-timeout draws. Same seed and same inputs => same timeouts.
    pub rng_seed: u64,
}

impl Config {
    pub fn validate(&self) -> Result<(), RaftError> {
        if self.id == 0 {
            return Err(RaftError::BadConfig("node id 0 is reserved"));
        }
        if self.peers.is_empty() {
            return Err(RaftError::BadConfig("peer set is empty"));
        }
        if !self.peers.contains(&self.id) {
            return Err(RaftError::BadConfig("peer set must include this node"));
        }
        if self.peers.contains(&0) {
            return Err(RaftError::BadConfig("peer id 0 is reserved"));
        }
        let mut sorted = self.peers.clone();
        sorted.sort_unstable();
        for pair in sorted.windows(2) {
            if pair[0] == pair[1] {
                return Err(RaftError::BadConfig("duplicate peer id"));
            }
        }
        if self.election_tick_min == 0 {
            return Err(RaftError::BadConfig("election_tick_min must be >= 1"));
        }
        if self.election_tick_max < self.election_tick_min {
            return Err(RaftError::BadConfig(
                "election_tick_max < election_tick_min",
            ));
        }
        if self.heartbeat_ticks == 0 {
            return Err(RaftError::BadConfig("heartbeat_ticks must be >= 1"));
        }
        if self.max_entries_per_msg == 0 {
            return Err(RaftError::BadConfig("max_entries_per_msg must be >= 1"));
        }
        Ok(())
    }
}
