//! Per-visitor sessions for the public web demo.
//!
//! Each visitor gets their own intersection (Concord's Raft cluster and the baseline
//! controllers) so strangers never see or disturb each other. Sessions advance lazily: a request runs the ticks that are due
//! since the last one, capped, so an idle tab costs nothing and there is no global ticker.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::intersection::Intersection;

/// Live sessions kept at most. The least recently seen one is dropped to make room.
pub const MAX_SESSIONS: usize = 200;
/// A session nobody has touched for this long is discarded.
pub const IDLE_TTL: Duration = Duration::from_secs(15 * 60);
/// Most ticks one request will catch up. A tab that was in the background resumes from "now"
/// instead of fast-forwarding through minutes of simulated time.
pub const MAX_CATCH_UP_TICKS: u64 = 40;
/// Simulated tick length. One fixed speed, slow enough to follow messages by eye.
pub const TICK_MS: u64 = 220;

/// One visitor's intersection and the wall clock it runs on.
pub struct Session {
    pub intersection: Intersection,
    last_seen: Instant,
    /// Wall-clock time up to which ticks have been run.
    clock: Instant,
}

impl Session {
    pub fn new(seed: u64, now: Instant) -> Self {
        Self {
            intersection: Intersection::new(seed),
            last_seen: now,
            clock: now,
        }
    }

    pub fn step(&mut self) {
        self.intersection.step();
    }

    /// Run the ticks that are due by `now`. Returns how many ran.
    pub fn catch_up(&mut self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.clock);
        let due = u64::try_from(elapsed.as_millis() / u128::from(TICK_MS)).unwrap_or(u64::MAX);
        let run = due.min(MAX_CATCH_UP_TICKS);
        for _ in 0..run {
            self.step();
        }
        if due > run {
            self.clock = now;
        } else {
            self.clock += Duration::from_millis(run * TICK_MS);
        }
        run
    }
}

/// All live sessions, keyed by the visitor's session id.
pub struct Sessions {
    map: BTreeMap<String, Session>,
    max: usize,
    ttl: Duration,
}

impl Sessions {
    pub fn new(max: usize, ttl: Duration) -> Self {
        Self {
            map: BTreeMap::new(),
            max: max.max(1),
            ttl,
        }
    }

    /// The session for `sid`, or a new one stored under `fresh_id()` when `sid` is missing,
    /// unknown or expired. Returns the id the visitor should keep using and whether it is new.
    pub fn touch(
        &mut self,
        sid: Option<&str>,
        fresh_id: impl FnOnce() -> String,
        now: Instant,
    ) -> (String, bool, &mut Session) {
        self.expire(now);
        let known = sid
            .filter(|id| self.map.contains_key(*id))
            .map(str::to_owned);
        let (id, created) = match known {
            Some(id) => (id, false),
            None => {
                if self.map.len() >= self.max {
                    self.evict_oldest();
                }
                (fresh_id(), true)
            }
        };
        let session = self
            .map
            .entry(id.clone())
            .or_insert_with(|| Session::new(1, now));
        session.last_seen = now;
        (id, created, session)
    }

    fn expire(&mut self, now: Instant) {
        let ttl = self.ttl;
        self.map
            .retain(|_, s| now.saturating_duration_since(s.last_seen) < ttl);
    }

    fn evict_oldest(&mut self) {
        let oldest = self
            .map
            .iter()
            .min_by_key(|(_, s)| s.last_seen)
            .map(|(id, _)| id.clone());
        if let Some(id) = oldest {
            self.map.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> impl FnMut() -> String {
        let mut n = 0;
        move || {
            n += 1;
            format!("sid{n}")
        }
    }

    #[test]
    fn unknown_or_missing_ids_get_a_new_session() {
        let mut s = Sessions::new(10, IDLE_TTL);
        let mut next = ids();
        let now = Instant::now();
        let (a, created_a, _) = s.touch(None, &mut next, now);
        let (b, created_b, _) = s.touch(Some("forged"), &mut next, now);
        let (again, created_again, _) = s.touch(Some(&a), &mut next, now);
        assert!(created_a && created_b && !created_again);
        assert_ne!(a, b);
        assert_eq!(again, a);
        assert_eq!(s.map.len(), 2);
    }

    #[test]
    fn visitors_do_not_share_intersections() {
        let mut s = Sessions::new(10, IDLE_TTL);
        let mut next = ids();
        let mut now = Instant::now();
        let (a, _, _) = s.touch(None, &mut next, now);
        let (b, _, _) = s.touch(None, &mut next, now);
        for _ in 0..60 {
            now += Duration::from_millis(TICK_MS);
            s.touch(Some(&a), &mut next, now).2.catch_up(now);
            s.touch(Some(&b), &mut next, now).2.catch_up(now);
        }
        s.touch(Some(&a), &mut next, now)
            .2
            .intersection
            .toggle_cable(1);
        let (_, _, sa) = s.touch(Some(&a), &mut next, now);
        assert!(sa.intersection.concord.is_isolated(1));
        let (_, _, sb) = s.touch(Some(&b), &mut next, now);
        assert!(
            !sb.intersection.concord.is_isolated(1),
            "b saw a's cable cut"
        );
    }

    #[test]
    fn idle_sessions_expire() {
        let mut s = Sessions::new(10, Duration::from_secs(60));
        let mut next = ids();
        let t0 = Instant::now();
        let (a, _, _) = s.touch(None, &mut next, t0);
        let (again, created, _) = s.touch(Some(&a), &mut next, t0 + Duration::from_secs(61));
        assert!(created, "an expired id must not come back");
        assert_ne!(again, a);
        assert_eq!(s.map.len(), 1);
    }

    #[test]
    fn the_least_recently_seen_session_is_evicted_when_full() {
        let mut s = Sessions::new(2, IDLE_TTL);
        let mut next = ids();
        let t0 = Instant::now();
        let (a, _, _) = s.touch(None, &mut next, t0);
        let (b, _, _) = s.touch(None, &mut next, t0 + Duration::from_secs(1));
        s.touch(Some(&a), &mut next, t0 + Duration::from_secs(2));
        let (_, created, _) = s.touch(None, &mut next, t0 + Duration::from_secs(3));
        assert!(created);
        assert_eq!(s.map.len(), 2);
        let (_, a_new, _) = s.touch(Some(&a), &mut next, t0 + Duration::from_secs(4));
        assert!(!a_new, "a was seen more recently than b and must survive");
        let (_, b_new, _) = s.touch(Some(&b), &mut next, t0 + Duration::from_secs(5));
        assert!(b_new, "b was the oldest and must have been evicted");
    }

    #[test]
    fn catch_up_runs_due_ticks_and_caps_long_gaps() {
        let t0 = Instant::now();
        let mut s = Session::new(1, t0);
        let ran = s.catch_up(t0 + Duration::from_millis(TICK_MS * 5 + 10));
        assert_eq!(ran, 5);
        assert_eq!(s.intersection.tick(), 5);
        let ran = s.catch_up(t0 + Duration::from_secs(600));
        assert_eq!(ran, MAX_CATCH_UP_TICKS);
        // After a capped gap the clock resumes from "now", not from the backlog.
        let ran = s.catch_up(t0 + Duration::from_secs(600) + Duration::from_millis(TICK_MS));
        assert_eq!(ran, 1);
    }
}
