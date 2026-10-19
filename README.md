# Concord 🚦

Concord is a small replicated database I built in Rust to learn how systems like etcd keep data safe. It saves every write on three servers and uses the Raft consensus algorithm, so a write it confirms is still there after one server crashes or gets cut off from the network.

The demo puts Concord inside a traffic intersection. Three controllers share one light schedule, and you can cut their cables or turn them off to see what happens with Raft and without it.

Concord is a portfolio project, not a production database or a real traffic controller.

## The problem 🚥

Picture an intersection run by three controllers, so that one failure can't stop it. C1 drives the North–South lights, C2 drives the East–West lights and C3 is a backup. They have to agree on whose turn it is.

If they can't agree, redundancy makes things worse. Cut C1's cable and it can't tell whether the others are gone, so it keeps North–South green on its own while C2 keeps cycling East–West. Both roads get green and cars collide.

With Concord, a green only counts once 2 of the 3 controllers have it. A controller that's cut off can't reach 2 of 3, so it can't start a green on its own. When one of the light controllers is lost, the other two still make a majority, and they agree to switch the whole intersection to flashing red, an all-way stop, until it's back.

## How it works 🧭

```text
Leader plans "North–South green from tick 120 to 138"
        |
        v
Writes it to its log, syncs to disk, sends it to the other two
        |
        +-- only 1 of 3 has it --> no light shows it yet
        |
        +-- 2 of 3 have it -----> committed --> the lights may show it
```

Raft has three main parts:

1. **Leader election.** The controllers vote for one leader. Winning takes 2 of 3 votes. Every election has a term number, so an old leader that comes back is ignored.
2. **Log replication.** The leader sends every entry to the others in order. A follower only accepts an entry if its log matches the leader's up to that point, so the copies can't drift apart.
3. **Commit rule.** An entry counts once a majority has it. Any two majorities out of 3 share at least one controller, so the next leader always has every committed entry.

The rule I had to be most careful with: a node syncs its log to disk *before* it votes or replies. If it replied first and then crashed, it could forget something it already agreed to.

The intersection adds one rule on top. Each green has a start and end on a shared clock, and the leader only plans a new green after every green already in its log has ended. The next leader's log holds every committed green, so two committed greens can never overlap.

## Why it helps ⚡

I ran the same random crash and network-split schedules against Concord and against async replication, which confirms a write as soon as the leader has it (1,000 schedules, 1,500 ticks each):

| Approach | Confirmed writes lost | Schedules with a loss |
| --- | ---: | ---: |
| Async replication | 3,339 | 848 of 1,000 |
| Concord | 0 | 0 of 1,000 |

Concord confirmed 183,268 writes across those runs and lost none of them. These numbers come from simulated time on my laptop (Apple M1 Pro), not real servers.

For the intersection, a test runs 40 random schedules of cable cuts and power-offs (1,500 ticks each) and checks that Concord never shows both roads green. The version without Raft does.

Concord isn't free, though. When it can't be sure, it flashes red, so drivers wait longer. If 2 of the 3 controllers are down, it can't agree on anything new and stays on flashing red. I haven't measured latency, throughput or failover time.

## The demo 🖥️

Two intersections sit side by side with the same hardware and the same traffic:

- **Baseline**: three controllers without Raft. Each one trusts only what it hears.
- **Concord**: three controllers with Raft. A green needs 2 of 3 to agree.

Tap a controller to **cut its cable** or **turn it off**. The same failure hits both intersections. Things to try:

- Cut C1's cable. Baseline puts both roads on green and cars crash. Concord flashes red and nobody crashes.
- Turn off C2, the leader. Concord's other two controllers elect a new leader and fail safe.
- Turn off C3, the backup. Concord keeps cycling normally.

Each side counts **collisions**, **cars through** and **average wait**. Keyboard: `1`/`2`/`3` cut or reconnect a controller, `Shift` + number turns it off or on, `Space` pauses.

The lights and controllers come from the simulator on the server, and every visitor gets their own private copy. Concord's controllers are a real Raft cluster running this repo's code. The browser only draws the cars and counts what happens. Drivers treat a dark or flashing signal like a stop sign and yield to cross traffic, so a crash means two roads really had green at once.

Real intersections also have a hardware conflict monitor that forces flashing red if two roads ever get green. The demo leaves it out to show what the controllers' agreement does on its own. The simulator's tick stands in for the GPS-synced clock real controllers share.

## Built with 🛠️

- **Rust** for Raft, the write-ahead log, the key-value store, the simulator and the intersection
- **tonic (gRPC)** for running a real three-node cluster
- Plain HTML, CSS and JavaScript (canvas) for the demo page, served by a small Rust HTTP server with no framework

## Run it

```bash
cargo run -p concord-sim --bin concord-demo
```

Open `http://127.0.0.1:8766` in a browser.

### Run it with Docker

```bash
docker build -t concord-demo .
docker run --name concord -p 8766:8766 concord-demo
```

Open `http://localhost:8766` in a browser.

Turn it off and remove everything (container + image):

```bash
docker rm -f concord && docker rmi concord-demo
```

To put it on Railway, create a service from this repo. Railway builds the `Dockerfile` and sets `PORT` for you. `railway.json` sets `/healthz` as the health check.

### Run a real cluster

```bash
./scripts/run-cluster.sh
target/release/concord-server put --addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 city lisbon
target/release/concord-server get --addr 127.0.0.1:7002 city
```

This starts three real nodes on your machine, each with its own log file in `./data`. Kill one of them, keep writing through the other two, then start it again with the same `--data-dir`. It reloads its log from disk and catches up. Building the server needs `protoc` installed.

## Tests and benchmarks

```bash
cargo test --workspace

# The 1,000-schedule comparison from the table above
cargo run -p concord-sim --release --bin concord-chaos -- 1000 1500
```

The simulator checks these on every tick:

- at most one leader per term
- no two servers ever disagree about a committed write
- a server's commit point never moves backwards

After each run it also checks that every confirmed write is still there.

I also broke the Raft code on purpose to make sure the tests would catch it. Committing with only one vote, or voting twice in the same term, makes the tests fail. Removing the current-term commit rule is *not* caught by the random schedules, so that rule has its own unit tests in `crates/raft/src/commit.rs`.

## Project layout

| Folder | What's in it |
| --- | --- |
| `crates/raft` | Raft itself. No clock, network or disk inside it, so every run can be replayed exactly. |
| `crates/storage` | The write-ahead log. It syncs to disk before returning, and on startup it cuts off a half-written last record. |
| `crates/kv` | The key-value store. Put and Get only. |
| `crates/sim` | The simulator, the chaos runs, the intersection and the web demo. |
| `crates/server` | gRPC server and client for a real cluster. |

## Limits

- Always three servers. You can't add or remove one.
- Crash testing covers the simulator's crash model (synced data survives, unsynced data is lost). It doesn't test real power loss or disks that lie about syncing.
- Reads come from one server's copy, so they can be a little stale.
- There's no PreVote, so a single flaky cable can cause repeated elections.
- In the intersection, the leader learns which controllers are reachable from the simulated network. A real controller would notice that its messages go unanswered.
- Not built yet: snapshots, log compaction, deletes, client sessions (a retried Put can be applied twice), TLS and auth. The log file grows forever.
