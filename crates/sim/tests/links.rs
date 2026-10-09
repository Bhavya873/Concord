//! Single-link cuts: one fiber between two data centers goes down, the others stay up.

use concord_sim::{Cluster, TicketState};

fn elected(seed: u64) -> Cluster {
    let mut c = Cluster::new(seed);
    assert!(
        c.run_until(200, |c| c.serving_leader().is_some()),
        "no leader for seed {seed}"
    );
    c
}

#[test]
fn a_cut_link_blocks_only_that_pair() {
    let mut c = elected(3);
    c.cut_link(1, 2);
    assert!(!c.reachable(1, 2));
    assert!(!c.reachable(2, 1), "cuts are symmetric");
    assert!(c.reachable(1, 3));
    assert!(c.reachable(2, 3));
    c.restore_link(2, 1);
    assert!(c.reachable(1, 2));
}

#[test]
fn heal_clears_cut_links() {
    let mut c = elected(4);
    c.cut_link(1, 3);
    c.cut_link(2, 3);
    c.heal();
    assert!(c.reachable(1, 3) && c.reachable(2, 3));
}

#[test]
fn writes_still_commit_with_one_link_cut() {
    for seed in 0..20 {
        let mut c = elected(seed);
        let leader = c.serving_leader().unwrap();
        let other = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
        c.cut_link(leader, other);
        // The leader still reaches a majority (itself plus the third node), but the node on the
        // far side of the cut may start elections. Writes must still commit through whoever leads.
        let committed = c.run_until(600, |c| c.serving_leader().is_some());
        assert!(
            committed,
            "seed {seed}: no serving leader with one link cut"
        );
        let t = c.put(b"order-1042", b"2x headphones").unwrap();
        assert!(
            c.run_until(600, |c| c.ticket_state(&t) == TicketState::Acked
                || c.ticket_state(&t) == TicketState::Lost),
            "seed {seed}: write neither committed nor lost"
        );
        c.heal();
        assert!(
            c.run_until(600, |c| c.converged()),
            "seed {seed}: no converge"
        );
        c.run(30);
        assert!(
            c.violations().is_empty(),
            "seed {seed}: {:?}",
            c.violations()
        );
        assert!(c.check_acked_in_committed_prefix().is_empty());
        assert!(c.check_acked_in_stores().is_empty());
    }
}

#[test]
fn cutting_both_links_of_a_node_is_like_isolating_it() {
    let mut c = elected(5);
    let leader = c.serving_leader().unwrap();
    let others: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();
    c.cut_link(leader, others[0]);
    c.cut_link(leader, others[1]);
    assert!(
        c.run_until(400, |c| c.serving_leader().is_some_and(|l| l != leader)),
        "the far side should elect a new leader"
    );
    // The cut-off old leader can't confirm anything.
    let stranded = c.put_on(leader, b"order-1", b"x");
    c.run(100);
    if let Ok(t) = stranded {
        assert_ne!(c.ticket_state(&t), TicketState::Acked);
    }
    assert!(c.violations().is_empty());
}
