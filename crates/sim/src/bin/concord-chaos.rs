//! `cargo run -p concord-sim --release --bin concord-chaos -- [seeds] [ticks]`
//! Runs seeded kill/partition/write schedules and exits non-zero on any violation. The same
//! schedules are also run with the leader-only baseline ("async replication") for comparison.

use concord_sim::{chaos_run, chaos_run_with, AckPolicy};

fn main() {
    let mut args = std::env::args().skip(1);
    let seeds: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(100);
    let ticks: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(1_500);
    let (mut acked, mut lost, mut kills, mut parts, mut failing) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut base_lost, mut base_runs) = (0u64, 0u64);
    let start = std::time::Instant::now();
    for seed in 0..seeds {
        let r = chaos_run(seed, ticks);
        acked += u64::from(r.puts_acked);
        lost += u64::from(r.puts_lost);
        kills += u64::from(r.kills);
        parts += u64::from(r.partitions);
        if !r.violations.is_empty() {
            failing += 1;
            println!("seed {seed} FAILED: {}", r.violations.join("; "));
        }
        let b = chaos_run_with(seed, ticks, AckPolicy::LeaderOnly);
        base_lost += u64::from(b.confirmed_lost);
        if b.confirmed_lost > 0 {
            base_runs += 1;
        }
    }
    println!(
        "{seeds} seeds x {ticks} ticks (sim-virtual): {kills} kills, {parts} partitions, {acked} acked writes, {lost} writes lost before ack, {failing} failing seeds, {} ms wall",
        start.elapsed().as_millis()
    );
    println!(
        "same schedules, async replication (leader-only confirm): {base_lost} confirmed writes lost, in {base_runs} of {seeds} schedules"
    );
    if failing > 0 {
        std::process::exit(1);
    }
}
