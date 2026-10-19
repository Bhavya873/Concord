//! The traffic-intersection demo, built on the simulator.
//!
//! Two intersections with the same hardware: controller 1 drives the North–South lights,
//! controller 2 the East–West lights, controller 3 is a backup. One controller at a time leads
//! and plans the cycle as time-boxed green *grants* ("NS green from tick 120 to 138").
//!
//! - **Concord** runs the three controllers as a Raft cluster. A grant is a log entry, and a
//!   light shows a grant only once that controller knows it is committed. While the leader
//!   can't reach both light controllers, the majority agrees on flashing red for everyone.
//! - **Baseline** is the same three controllers without Raft: each acts on the newest plan it
//!   heard, and one that hears nobody keeps its own road green.
//!
//! Time is the simulator tick, which stands in for the GPS-synchronised clock real controllers
//! share.

use std::fmt::Write;

use concord_kv::{decode, Command};
use concord_raft::{NodeId, RaftNode};

use crate::{AckPolicy, Cluster};

/// Green plus amber, in ticks.
const GREEN: u64 = 18;
/// The last ticks of a green show amber.
const AMBER: u64 = 4;
/// All-red clearance between grants.
const GAP: u64 = 8;
/// The leader plans the next grant this many ticks before the current one ends.
const LOOK: u64 = 10;
/// A baseline controller that hears nobody for this long acts on its own.
const ALONE_AFTER: u64 = 6;
/// How far back from the newest entry a controller looks for grants.
const SCAN: u64 = 24;
/// Raft node ids for the controllers. The light controllers are the first two.
const CONTROLLERS: [NodeId; 3] = [1, 2, 3];
const GRANT_KEY: &[u8] = b"g";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dir {
    NorthSouth,
    EastWest,
}

impl Dir {
    fn other(self) -> Self {
        match self {
            Dir::NorthSouth => Dir::EastWest,
            Dir::EastWest => Dir::NorthSouth,
        }
    }

    /// The direction a controller drives, if any.
    fn of(id: NodeId) -> Option<Self> {
        match id {
            1 => Some(Dir::NorthSouth),
            2 => Some(Dir::EastWest),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Light {
    Go,
    Amber,
    Stop,
    /// Flashing red: an all-way stop.
    Flash,
    /// No power.
    Dark,
}

impl Light {
    pub fn is_green(self) -> bool {
        matches!(self, Light::Go | Light::Amber)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Light::Go => "go",
            Light::Amber => "amber",
            Light::Stop => "stop",
            Light::Flash => "flash",
            Light::Dark => "dark",
        }
    }
}

/// A planned interval: a green for one direction, or flashing red for everyone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Grant {
    flash: bool,
    dir: Dir,
    start: u64,
    end: u64,
}

impl Grant {
    fn green(dir: Dir, start: u64) -> Self {
        Self {
            flash: false,
            dir,
            start,
            end: start + GREEN,
        }
    }

    fn flash(start: u64) -> Self {
        Self {
            flash: true,
            dir: Dir::NorthSouth,
            start,
            end: start + GREEN,
        }
    }

    fn encode(&self) -> String {
        let tag = match (self.flash, self.dir) {
            (true, _) => "FL",
            (false, Dir::NorthSouth) => "NS",
            (false, Dir::EastWest) => "EW",
        };
        format!("{tag}:{}-{}", self.start, self.end)
    }

    fn parse(text: &str) -> Option<Self> {
        let (tag, span) = text.split_once(':')?;
        let (start, end) = span.split_once('-')?;
        let (start, end) = (start.parse().ok()?, end.parse().ok()?);
        let (flash, dir) = match tag {
            "FL" => (true, Dir::NorthSouth),
            "NS" => (false, Dir::NorthSouth),
            "EW" => (false, Dir::EastWest),
            _ => return None,
        };
        Some(Self {
            flash,
            dir,
            start,
            end,
        })
    }

    fn covers(&self, t: u64) -> bool {
        self.start <= t && t < self.end
    }

    /// What a controller driving `own` shows while this grant covers `t`.
    fn light_for(&self, own: Dir, t: u64) -> Light {
        if self.flash {
            Light::Flash
        } else if self.dir != own {
            Light::Stop
        } else if t + AMBER >= self.end {
            Light::Amber
        } else {
            Light::Go
        }
    }
}

/// The next green after `last`, starting no earlier than `earliest`.
fn next_green(last: Option<Grant>, earliest: u64) -> Grant {
    match last {
        Some(g) => Grant::green(g.dir.other(), (g.end + GAP).max(earliest)),
        None => Grant::green(Dir::NorthSouth, earliest),
    }
}

// ---- Concord --------------------------------------------------------------------------

fn grant_at(raft: &RaftNode, index: u64) -> Option<Grant> {
    let entry = raft.entry(index)?;
    match decode(&entry.data).ok()? {
        Command::Put { key, value } if key == GRANT_KEY => {
            Grant::parse(std::str::from_utf8(&value).ok()?)
        }
        _ => None,
    }
}

/// Grants in `raft`'s log from `top` down, newest first.
fn grants_from(raft: &RaftNode, top: u64) -> impl Iterator<Item = Grant> + '_ {
    let low = top.saturating_sub(SCAN).max(1);
    (low..=top).rev().filter_map(move |i| grant_at(raft, i))
}

/// What controller `id` shows: only grants it knows are committed. The newest committed grant
/// covering `t` wins, so an agreed flash overrides an earlier agreed green.
fn concord_light(c: &Cluster, id: NodeId, t: u64) -> Light {
    let (Some(own), Some(raft)) = (
        Dir::of(id),
        c.members.get(&id).and_then(|m| m.raft.as_ref()),
    ) else {
        return Light::Dark;
    };
    let mut future = false;
    for g in grants_from(raft, raft.commit_index()) {
        if g.covers(t) {
            return g.light_for(own, t);
        }
        future |= g.start > t;
    }
    if future {
        Light::Stop
    } else {
        Light::Flash
    }
}

/// The serving leader plans ahead. Each new green starts after every green already in its log,
/// and its log holds every committed entry, so committed greens never overlap in time.
///
/// Whether the light controllers are reachable is read from the simulated network, standing in
/// for the leader noticing that its messages to them go unanswered.
fn concord_plan(c: &mut Cluster, t: u64) {
    let Some(leader) = c.serving_leader() else {
        return;
    };
    let drivers_ok = [1, 2]
        .iter()
        .all(|&d| c.members.get(&d).is_some_and(|m| m.alive()) && c.reachable(leader, d));
    let Some(raft) = c.members.get(&leader).and_then(|m| m.raft.as_ref()) else {
        return;
    };
    let grants: Vec<Grant> = grants_from(raft, raft.last_log_index()).collect();
    let newest = grants.first().copied();
    let last_green = grants.iter().copied().find(|g| !g.flash);
    let green_end = grants
        .iter()
        .filter(|g| !g.flash)
        .map(|g| g.end)
        .max()
        .unwrap_or(0);
    let proposal = if drivers_ok {
        (green_end <= t + LOOK).then(|| next_green(last_green, (green_end + GAP).max(t + 4)))
    } else {
        match newest {
            Some(g) if g.flash && g.end > t + LOOK => None,
            Some(g) if g.flash => Some(Grant::flash(g.end.max(t + 1))),
            _ => Some(Grant::flash(t + 1)),
        }
    };
    if let Some(g) = proposal {
        let _ = c.put_on(leader, GRANT_KEY, g.encode().as_bytes());
    }
}

/// Some two controllers are alive and can talk.
fn has_majority(c: &Cluster) -> bool {
    let live: Vec<NodeId> = c.alive_ids();
    live.iter()
        .any(|&a| live.iter().any(|&b| a < b && c.reachable(a, b)))
}

// ---- Baseline ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct BaseCtrl {
    alive: bool,
    cut: bool,
    plan: Vec<Grant>,
    heard: u64,
    alone: bool,
}

/// Three controllers without consensus.
#[derive(Clone, Debug)]
struct Baseline {
    ctrls: [BaseCtrl; 3],
    leader: usize,
}

impl Baseline {
    fn new() -> Self {
        let c = BaseCtrl {
            alive: true,
            cut: false,
            plan: Vec::new(),
            heard: 0,
            alone: false,
        };
        Self {
            ctrls: [c.clone(), c.clone(), c],
            leader: 1,
        }
    }

    fn reach(&self, i: usize) -> bool {
        self.ctrls[i].alive && !self.ctrls[i].cut
    }

    fn push(&mut self, i: usize, g: Grant) {
        let plan = &mut self.ctrls[i].plan;
        plan.push(g);
        if plan.len() > 8 {
            plan.remove(0);
        }
    }

    /// The leader sends its plan to everyone it can reach. A controller that hears nobody for
    /// a while assumes the others are gone and keeps planning for its own road.
    fn step(&mut self, t: u64) {
        let lead = self.leader;
        let ok = self.reach(lead);
        if ok {
            let last = self.ctrls[lead].plan.last().copied();
            if last.is_none_or(|g| t + LOOK >= g.end) {
                let earliest = last.map_or(t + 2, |g| (g.end + GAP).max(t + 2));
                self.push(lead, next_green(last, earliest));
            }
        }
        for i in 0..3 {
            if !self.ctrls[i].alive || (ok && i == lead) {
                continue;
            }
            if ok && self.reach(i) {
                self.ctrls[i].plan = self.ctrls[lead].plan.clone();
                self.ctrls[i].heard = t;
                self.ctrls[i].alone = false;
                continue;
            }
            if t.saturating_sub(self.ctrls[i].heard) <= ALONE_AFTER {
                continue;
            }
            if !ok && self.reach(i) {
                // Lowest-numbered controller still on the network takes over planning.
                if let Some(next) = (0..3).find(|&j| self.reach(j)) {
                    self.leader = next;
                    self.ctrls[next].heard = t;
                }
                return;
            }
            if self.ctrls[i].cut {
                if let Some(own) = Dir::of(i as NodeId + 1) {
                    let last = self.ctrls[i].plan.last().copied();
                    if !self.ctrls[i].alone {
                        self.ctrls[i].alone = true;
                        self.push(i, Grant::green(own, t + 1));
                    } else if last.is_none_or(|g| t + LOOK >= g.end) {
                        let start = last.map_or(t + 1, |g| (g.end + GAP).max(t + 1));
                        self.push(i, Grant::green(own, start));
                    }
                }
            }
        }
    }

    /// The newest plan that covers `t`; otherwise hold the last instruction.
    fn light(&self, i: usize, t: u64) -> Light {
        let n = &self.ctrls[i];
        let Some(own) = Dir::of(i as NodeId + 1) else {
            return Light::Dark;
        };
        if !n.alive {
            return Light::Dark;
        }
        if let Some(g) = n.plan.iter().rev().find(|g| g.covers(t)) {
            return g.light_for(own, t);
        }
        if n.plan.iter().any(|g| g.start > t) {
            return Light::Stop;
        }
        match n.plan.last() {
            Some(g) if g.dir == own => Light::Go,
            _ => Light::Stop,
        }
    }

    fn adopt(&mut self, i: usize, t: u64) {
        let lead = self.leader;
        if lead != i && self.reach(lead) {
            self.ctrls[i].plan = self.ctrls[lead].plan.clone();
        }
        self.ctrls[i].heard = t;
        self.ctrls[i].alone = false;
    }
}

// ---- both intersections -----------------------------------------------------------------

/// What one intersection's lights show.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lights {
    pub north_south: Light,
    pub east_west: Light,
}

impl Lights {
    pub fn conflict(&self) -> bool {
        self.north_south.is_green() && self.east_west.is_green()
    }
}

pub struct Intersection {
    pub concord: Cluster,
    baseline: Baseline,
    tick: u64,
}

impl Intersection {
    pub fn new(seed: u64) -> Self {
        Self {
            concord: Cluster::with_policy(seed, AckPolicy::Majority),
            baseline: Baseline::new(),
            tick: 0,
        }
    }

    pub fn tick(&self) -> u64 {
        self.tick
    }

    pub fn step(&mut self) {
        self.tick += 1;
        self.concord.step();
        concord_plan(&mut self.concord, self.tick);
        self.baseline.step(self.tick);
    }

    pub fn concord_lights(&self) -> Lights {
        Lights {
            north_south: concord_light(&self.concord, 1, self.tick),
            east_west: concord_light(&self.concord, 2, self.tick),
        }
    }

    pub fn baseline_lights(&self) -> Lights {
        Lights {
            north_south: self.baseline.light(0, self.tick),
            east_west: self.baseline.light(1, self.tick),
        }
    }

    /// Cut or reconnect controller `id`'s cable on both intersections.
    pub fn toggle_cable(&mut self, id: NodeId) {
        let Some(i) = index(id) else { return };
        if !self.baseline.ctrls[i].alive {
            return;
        }
        if self.baseline.ctrls[i].cut {
            self.baseline.ctrls[i].cut = false;
            self.baseline.adopt(i, self.tick);
            self.concord.rejoin(id);
        } else {
            self.baseline.ctrls[i].cut = true;
            self.concord.isolate(id);
        }
    }

    /// Turn controller `id` off or back on, on both intersections.
    pub fn toggle_power(&mut self, id: NodeId) {
        let Some(i) = index(id) else { return };
        if self.baseline.ctrls[i].alive {
            self.baseline.ctrls[i].alive = false;
            self.concord.crash_now(id);
        } else {
            self.baseline.ctrls[i].alive = true;
            self.baseline.ctrls[i].cut = false;
            self.baseline.adopt(i, self.tick);
            self.concord.rejoin(id);
            self.concord.restart(id);
        }
    }

    pub fn to_json(&self, tick_ms: u64) -> String {
        let mut out = String::new();
        let (b, c) = (self.baseline_lights(), self.concord_lights());
        let _ = write!(out, "{{\"tick\":{},\"tick_ms\":{tick_ms},\"baseline\":{{\"ns\":\"{}\",\"ew\":\"{}\",\"leader\":{},\"ctrls\":[",
            self.tick, b.north_south.as_str(), b.east_west.as_str(), self.baseline.leader + 1);
        for (i, n) in self.baseline.ctrls.iter().enumerate() {
            let sep = if i > 0 { "," } else { "" };
            let _ = write!(
                out,
                "{sep}{{\"alive\":{},\"cut\":{},\"alone\":{}}}",
                n.alive, n.cut, n.alone
            );
        }
        let leader = self
            .concord
            .serving_leader()
            .map_or("null".to_string(), |l| l.to_string());
        let _ = write!(out, "]}},\"concord\":{{\"ns\":\"{}\",\"ew\":\"{}\",\"leader\":{leader},\"majority\":{},\"ctrls\":[",
            c.north_south.as_str(), c.east_west.as_str(), has_majority(&self.concord));
        for (i, id) in CONTROLLERS.iter().enumerate() {
            let sep = if i > 0 { "," } else { "" };
            let alive = self.concord.members.get(id).is_some_and(|m| m.alive());
            let _ = write!(
                out,
                "{sep}{{\"alive\":{alive},\"cut\":{}}}",
                self.concord.is_isolated(*id)
            );
        }
        out.push_str("]}}");
        out
    }
}

fn index(id: NodeId) -> Option<usize> {
    CONTROLLERS.iter().position(|&c| c == id)
}

#[cfg(test)]
mod tests {
    use rand_chacha::ChaCha8Rng;
    use rand_core::{Rng, SeedableRng};

    use super::*;

    fn run(x: &mut Intersection, ticks: u64, mut check: impl FnMut(&Intersection)) {
        for _ in 0..ticks {
            x.step();
            check(x);
        }
    }

    #[test]
    fn grants_round_trip() {
        for g in [
            Grant::green(Dir::NorthSouth, 5),
            Grant::green(Dir::EastWest, 90),
            Grant::flash(12),
        ] {
            assert_eq!(Grant::parse(&g.encode()), Some(g));
        }
        assert_eq!(Grant::parse("XX:1-2"), None);
    }

    #[test]
    fn healthy_intersections_give_both_roads_green_and_never_conflict() {
        let mut x = Intersection::new(3);
        let (mut ns, mut ew) = (false, false);
        run(&mut x, 400, |x| {
            for l in [x.concord_lights(), x.baseline_lights()] {
                assert!(!l.conflict(), "tick {}: {l:?}", x.tick());
            }
            let c = x.concord_lights();
            ns |= c.north_south == Light::Go;
            ew |= c.east_west == Light::Go;
        });
        assert!(ns && ew, "Concord never cycled both roads");
    }

    #[test]
    fn baseline_puts_both_roads_on_green_when_the_ns_controller_is_cut() {
        let mut x = Intersection::new(1);
        run(&mut x, 150, |_| {});
        x.toggle_cable(1);
        let mut conflict = false;
        run(&mut x, 200, |x| conflict |= x.baseline_lights().conflict());
        assert!(conflict);
    }

    #[test]
    fn concord_flashes_red_while_a_light_controller_is_cut_and_recovers_after() {
        let mut x = Intersection::new(1);
        run(&mut x, 150, |_| {});
        x.toggle_cable(1);
        let mut flashed = false;
        run(&mut x, 200, |x| {
            let l = x.concord_lights();
            assert!(!l.conflict());
            flashed |= l.east_west == Light::Flash;
        });
        assert!(flashed, "the majority never agreed on flashing red");
        x.toggle_cable(1);
        let mut green = false;
        run(&mut x, 200, |x| {
            green |= x.concord_lights().east_west == Light::Go
        });
        assert!(green, "normal cycling never resumed");
    }

    #[test]
    fn concord_keeps_cycling_when_the_backup_is_off() {
        let mut x = Intersection::new(2);
        run(&mut x, 150, |_| {});
        x.toggle_power(3);
        let (mut ns, mut ew) = (false, false);
        run(&mut x, 200, |x| {
            let l = x.concord_lights();
            assert!(
                l.north_south != Light::Flash && l.east_west != Light::Flash,
                "tick {}: {l:?}",
                x.tick()
            );
            ns |= l.north_south == Light::Go;
            ew |= l.east_west == Light::Go;
        });
        assert!(ns && ew);
    }

    /// The property the demo rests on: whatever is cut or turned off, Concord's committed plan
    /// never puts both roads on green.
    #[test]
    fn concord_never_shows_both_roads_green_under_random_faults() {
        let mut baseline_conflicts = 0;
        for seed in 0..40 {
            let mut x = Intersection::new(seed);
            let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0x51_6E_A1);
            for _ in 0..1_500 {
                if rng.next_u32() % 12 == 0 {
                    let id = u64::from(rng.next_u32() % 3) + 1;
                    if rng.next_u32() % 2 == 0 {
                        x.toggle_cable(id)
                    } else {
                        x.toggle_power(id)
                    }
                }
                x.step();
                let l = x.concord_lights();
                assert!(!l.conflict(), "seed {seed} tick {}: {l:?}", x.tick());
                baseline_conflicts += usize::from(x.baseline_lights().conflict());
            }
        }
        assert!(
            baseline_conflicts > 0,
            "the baseline never conflicted, so this test proves nothing"
        );
    }
}
