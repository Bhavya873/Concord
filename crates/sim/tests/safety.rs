use concord_raft::Role;
use concord_sim::{chaos_run, chaos_run_with, AckPolicy, Cluster, PutError, TicketState};

fn elected(seed: u64) -> Cluster {
    let mut c = Cluster::new(seed);
    assert!(
        c.run_until(200, |c| c.converged_leader()),
        "no leader for seed {seed}"
    );
    c
}

trait Ext {
    fn converged_leader(&self) -> bool;
}

impl Ext for Cluster {
    fn converged_leader(&self) -> bool {
        self.serving_leader().is_some()
    }
}

#[test]
fn at_most_one_leader_per_term_across_many_seeds() {
    for seed in 0..40 {
        let mut c = elected(seed);
        c.run(400);
        assert!(
            c.violations().is_empty(),
            "seed {seed}: {:?}",
            c.violations()
        );
    }
}

#[test]
fn a_new_leader_commits_a_noop_before_serving_writes() {
    let c = elected(1);
    let leader = c.serving_leader().unwrap();
    let raft = c.members[&leader].raft.as_ref().unwrap();
    let first = raft.entry(raft.last_log_index()).unwrap();
    assert_eq!(first.kind, concord_raft::EntryKind::Noop);
    assert_eq!(first.term, raft.term());
}

#[test]
fn acked_write_survives_a_leader_kill() {
    for seed in 0..20 {
        let mut c = elected(seed);
        let leader = c.serving_leader().unwrap();
        let ticket = c.put(b"color", b"blue").unwrap();
        assert!(c.run_until(100, |c| c.ticket_state(&ticket) == TicketState::Acked));

        c.kill(leader);
        assert!(
            c.run_until(300, |c| c.serving_leader().is_some_and(|l| l != leader)),
            "seed {seed}: no new leader"
        );
        let new_leader = c.serving_leader().unwrap();
        let t2 = c.put(b"shape", b"circle").unwrap();
        assert!(c.run_until(100, |c| c.ticket_state(&t2) == TicketState::Acked));

        c.restart(leader);
        assert!(
            c.run_until(400, |c| c.converged()),
            "seed {seed}: no converge"
        );
        c.run(20);

        assert!(c.violations().is_empty(), "{:?}", c.violations());
        assert!(c.check_acked_in_committed_prefix().is_empty());
        assert!(
            c.check_acked_in_stores().is_empty(),
            "{:?}",
            c.check_acked_in_stores()
        );
        for id in [1, 2, 3] {
            assert_eq!(c.get(id, b"color").as_deref(), Some(b"blue".as_slice()));
            assert_eq!(c.get(id, b"shape").as_deref(), Some(b"circle".as_slice()));
        }
        assert_ne!(new_leader, leader);
    }
}

#[test]
fn acked_write_survives_a_partition_heal_and_the_old_leaders_entry_is_discarded() {
    for seed in 0..20 {
        let mut c = elected(seed);
        let old = c.serving_leader().unwrap();
        let before = c.put(b"stable", b"1").unwrap();
        assert!(c.run_until(100, |c| c.ticket_state(&before) == TicketState::Acked));

        c.isolate(old);
        // The cut-off leader still believes it leads and takes a write it can never commit.
        let doomed = c.put_on(old, b"stale", b"never-acked").unwrap();
        assert!(
            c.run_until(300, |c| c.serving_leader().is_some_and(|l| l != old)),
            "seed {seed}: majority side elected nobody"
        );
        let t = c.put(b"fresh", b"2").unwrap();
        assert!(c.run_until(100, |c| c.ticket_state(&t) == TicketState::Acked));

        c.heal();
        assert!(
            c.run_until(400, |c| c.converged()),
            "seed {seed}: no converge"
        );
        c.run(20);

        assert_ne!(c.ticket_state(&doomed), TicketState::Acked);
        assert!(c.violations().is_empty(), "{:?}", c.violations());
        assert!(c.check_acked_in_committed_prefix().is_empty());
        assert!(
            c.check_acked_in_stores().is_empty(),
            "{:?}",
            c.check_acked_in_stores()
        );
        for id in [1, 2, 3] {
            assert_eq!(
                c.get(id, b"stale"),
                None,
                "node {id} kept the discarded write"
            );
            assert_eq!(c.get(id, b"fresh").as_deref(), Some(b"2".as_slice()));
            assert_eq!(c.get(id, b"stable").as_deref(), Some(b"1".as_slice()));
        }
        assert_eq!(
            c.members[&old].raft.as_ref().unwrap().role(),
            Role::Follower
        );
    }
}

#[test]
fn kill_drops_unsynced_state_and_keeps_the_synced_wal() {
    let mut c = elected(7);
    let leader = c.serving_leader().unwrap();
    let synced = c.put(b"kept", b"yes").unwrap();
    assert!(c.run_until(100, |c| c.ticket_state(&synced) == TicketState::Acked));
    let wal_len = c.members[&leader].disk.entries.len();

    // Proposed but never driven through a `Ready`: it exists only in RAM.
    c.put_on(leader, b"lost", b"unsynced").unwrap();
    c.kill(leader);
    assert_eq!(c.members[&leader].disk.entries.len(), wal_len);
    c.restart(leader);

    let raft = c.members[&leader].raft.as_ref().unwrap();
    assert_eq!(raft.last_log_index() as usize, wal_len);
    assert!(c.run_until(400, |c| c.converged()));
    c.run(20);
    for id in [1, 2, 3] {
        assert_eq!(c.get(id, b"kept").as_deref(), Some(b"yes".as_slice()));
        assert_eq!(c.get(id, b"lost"), None);
    }
    assert!(c.violations().is_empty());
}

#[test]
fn writes_need_a_majority() {
    let mut c = elected(3);
    let leader = c.serving_leader().unwrap();
    for id in [1, 2, 3] {
        if id != leader {
            c.kill(id);
        }
    }
    let t = c.put_on(leader, b"x", b"1").unwrap();
    c.run(100);
    assert_ne!(
        c.ticket_state(&t),
        TicketState::Acked,
        "committed without a quorum"
    );
    assert_eq!(c.put(b"y", b"2").unwrap_err(), PutError::NoLeader);
}

#[test]
fn seeded_chaos_keeps_every_invariant() {
    let mut total_acked = 0;
    for seed in 0..60 {
        let rep = chaos_run(seed, 1_500);
        assert!(
            rep.violations.is_empty(),
            "seed {seed}: {:?}",
            rep.violations
        );
        total_acked += rep.puts_acked;
    }
    assert!(total_acked > 0, "chaos never acknowledged a write");
}

#[test]
fn chaos_is_reproducible() {
    assert_eq!(chaos_run(11, 800), chaos_run(11, 800));
}

// ---- baseline comparison: same Raft, the client is told "saved" earlier ----------------

fn elected_with(seed: u64, policy: AckPolicy) -> Cluster {
    let mut c = Cluster::with_policy(seed, policy);
    assert!(
        c.run_until(200, |c| c.serving_leader().is_some()),
        "no leader for seed {seed}"
    );
    c
}

/// Write, wait until the leader has it on its own disk, crash the leader before its messages
/// leave, then let the survivors elect a new leader and commit.
fn write_then_crash(policy: AckPolicy, seed: u64) -> (Cluster, concord_sim::Ticket) {
    let mut c = elected_with(seed, policy);
    let leader = c.serving_leader().unwrap();
    let t = c.put(b"order-1043", b"a desk lamp").unwrap();
    c.step();
    c.crash_now(leader);
    assert!(
        c.run_until(400, |c| c.serving_leader().is_some_and(|l| l != leader)),
        "seed {seed}: no new leader"
    );
    c.run(40);
    (c, t)
}

/// Cut the leader off, write to it, let the majority side commit, then heal.
fn write_to_cut_off_leader(policy: AckPolicy, seed: u64) -> (Cluster, concord_sim::Ticket) {
    let mut c = elected_with(seed, policy);
    let old = c.serving_leader().unwrap();
    c.isolate(old);
    let t = c.put_on(old, b"order-1043", b"a desk lamp").unwrap();
    assert!(
        c.run_until(400, |c| c.serving_leader().is_some_and(|l| l != old)),
        "seed {seed}: majority side elected nobody"
    );
    // Work carries on on the majority side and fills the slot the cut-off leader used.
    let fresh = c.put(b"order-1044", b"a keyboard").unwrap();
    assert!(c.run_until(200, |c| c.ticket_state(&fresh) == TicketState::Acked));
    c.run(40);
    c.heal();
    assert!(
        c.run_until(400, |c| c.converged()),
        "seed {seed}: no converge"
    );
    (c, t)
}

#[test]
fn baseline_loses_a_confirmed_write_when_the_leader_crashes() {
    for seed in 0..20 {
        let (c, t) = write_then_crash(AckPolicy::LeaderOnly, seed);
        assert_eq!(
            c.ticket_state(&t),
            TicketState::ConfirmedLost,
            "seed {seed}"
        );
        assert_eq!(c.confirmed_lost(), 1, "seed {seed}");
        assert!(c.violations().is_empty(), "{:?}", c.violations());
    }
}

#[test]
fn baseline_loses_a_confirmed_write_on_the_cut_off_leader() {
    for seed in 0..20 {
        let (c, t) = write_to_cut_off_leader(AckPolicy::LeaderOnly, seed);
        assert_eq!(
            c.ticket_state(&t),
            TicketState::ConfirmedLost,
            "seed {seed}"
        );
        for id in [1, 2, 3] {
            assert_eq!(c.get(id, b"order-1043"), None, "seed {seed}: node {id}");
        }
        assert!(c.violations().is_empty(), "{:?}", c.violations());
    }
}

#[test]
fn concord_never_confirms_what_it_later_loses_on_the_same_scripts() {
    for seed in 0..20 {
        let (c, t) = write_then_crash(AckPolicy::Majority, seed);
        assert_eq!(
            c.ticket_state(&t),
            TicketState::Lost,
            "seed {seed}: told the client"
        );
        assert_eq!(c.confirmed_lost(), 0);
        assert!(c.violations().is_empty(), "{:?}", c.violations());

        let (c, t) = write_to_cut_off_leader(AckPolicy::Majority, seed);
        assert_eq!(
            c.ticket_state(&t),
            TicketState::Lost,
            "seed {seed}: told the client"
        );
        assert_eq!(c.confirmed_lost(), 0);
        assert!(c.violations().is_empty(), "{:?}", c.violations());
    }
}

#[test]
fn chaos_baseline_loses_confirmed_writes_and_concord_does_not() {
    let (mut base, mut concord) = (0, 0);
    for seed in 0..30 {
        let b = chaos_run_with(seed, 1_500, AckPolicy::LeaderOnly);
        assert!(b.violations.is_empty(), "seed {seed}: {:?}", b.violations);
        base += b.confirmed_lost;
        concord += chaos_run(seed, 1_500).confirmed_lost;
    }
    assert_eq!(concord, 0);
    assert!(
        base > 0,
        "baseline never lost a confirmed write in 30 seeds"
    );
}
