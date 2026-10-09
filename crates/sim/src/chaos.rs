//! Seeded fault schedules. Same seed, same schedule, same result.

use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};

use crate::{AckPolicy, Cluster, TicketState};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChaosReport {
    pub seed: u64,
    pub ticks: u64,
    pub kills: u32,
    pub restarts: u32,
    pub partitions: u32,
    pub heals: u32,
    pub puts_proposed: u32,
    pub puts_acked: u32,
    pub puts_lost: u32,
    /// Writes the client was told were saved that later vanished. Always 0 under `Majority`.
    pub confirmed_lost: u32,
    pub leaders_seen: usize,
    pub violations: Vec<String>,
}

/// Inject random kills, restarts, partitions and writes for `ticks`, then heal, restart
/// everything, let the cluster settle, and check durability. At most one node is down or
/// isolated at a time, so a majority always exists and acknowledged writes must survive.
pub fn chaos_run(seed: u64, ticks: u64) -> ChaosReport {
    chaos_run_with(seed, ticks, AckPolicy::Majority)
}

/// The same schedule under either ack policy. Under `LeaderOnly` the durability checks are
/// expected to fail, so they are reported as `confirmed_lost` instead of as violations; the Raft
/// invariants are still checked.
pub fn chaos_run_with(seed: u64, ticks: u64, policy: AckPolicy) -> ChaosReport {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xC4A0_5EED);
    let mut c = Cluster::with_policy(seed, policy);
    let mut rep = ChaosReport {
        seed,
        ticks,
        ..ChaosReport::default()
    };
    let mut down: Option<u64> = None;
    let mut cut: Option<u64> = None;
    let mut n = 0u32;

    for _ in 0..ticks {
        let roll = rng.next_u32() % 100;
        let pick = u64::from(rng.next_u32() % 3) + 1;
        match roll {
            0..=1 if down.is_none() && cut.is_none() => {
                c.kill(pick);
                down = Some(pick);
                rep.kills += 1;
            }
            2..=4 if down.is_some() => {
                if let Some(id) = down.take() {
                    c.restart(id);
                    rep.restarts += 1;
                }
            }
            5..=6 if down.is_none() && cut.is_none() => {
                c.isolate(pick);
                cut = Some(pick);
                rep.partitions += 1;
            }
            7..=9 if cut.is_some() => {
                c.heal();
                cut = None;
                rep.heals += 1;
            }
            10..=24 => {
                // Half the writes go to any leader, even one stuck behind a partition, so
                // some of them can never be acknowledged.
                let target = if roll % 2 == 0 {
                    c.leader()
                } else {
                    c.serving_leader()
                };
                if let Some(leader) = target {
                    n += 1;
                    let key = format!("k{}", n % 5);
                    let val = format!("v{n}");
                    if c.put_on(leader, key.as_bytes(), val.as_bytes()).is_ok() {
                        rep.puts_proposed += 1;
                    }
                }
            }
            _ => {}
        }
        c.step();
    }

    if let Some(id) = down.take() {
        c.restart(id);
    }
    c.heal();
    let settled = c.run_until(600, |c| c.converged());
    if !settled {
        rep.violations
            .push("cluster did not converge after healing".to_string());
    }
    c.run(30);

    rep.puts_acked = u32::try_from(c.acked().len()).unwrap_or(u32::MAX);
    rep.puts_lost = u32::try_from(c.acked_lost_count()).unwrap_or(u32::MAX);
    rep.leaders_seen = c.leaders_seen();
    rep.confirmed_lost = u32::try_from(c.confirmed_lost()).unwrap_or(u32::MAX);
    rep.violations.extend(c.violations().iter().cloned());
    if policy == AckPolicy::LeaderOnly {
        return rep;
    }
    rep.violations.extend(c.check_acked_in_committed_prefix());
    if settled {
        rep.violations.extend(c.check_acked_in_stores());
    }
    rep
}

impl Cluster {
    pub(crate) fn acked_lost_count(&self) -> usize {
        self.tickets
            .iter()
            .filter(|(_, s)| *s == TicketState::Lost)
            .count()
    }

    pub(crate) fn leaders_seen(&self) -> usize {
        self.leaders_by_term.len()
    }
}
