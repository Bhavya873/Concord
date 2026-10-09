# Concord 🗄️

Concord is a small replicated database I built in Rust to learn how systems like etcd keep data safe. It saves every write on three servers and uses the Raft consensus algorithm, so a write it confirms is still there after one server crashes or gets cut off from the network.

The demo compares two ways of telling a customer "your order is confirmed":

- **Async replication** — confirm as soon as the main server has the order, then copy it to the others afterwards.
- **Concord** — confirm only after 2 of the 3 servers have saved it.

Concord is a portfolio project, not a production database.

## The problem 🛒

Say an online store keeps its orders in three data centers. A customer checks out, the main data center saves the order and says "confirmed". If that data center loses power before it copies the order anywhere else, the order is gone. The customer has a confirmation for an order the store no longer has.

Concord waits until a second data center has the order on disk before it says "confirmed". Losing any one data center can't erase an order that was confirmed.

## How it works 🧭

```text
Customer places an order
        |
        v
Main server writes it to its log and syncs to disk
        |
        v
Sends it to the other two servers
        |
        +-- only 1 of 3 has it --> keep waiting (async replication already said "confirmed")
        |
        +-- 2 of 3 have it -----> committed --> tell the customer "confirmed"
```

Raft has three main parts:

1. **Leader election.** The servers vote for one main server (the leader). Winning takes 2 of 3 votes. Every election has a term number, so an old leader that comes back is ignored.
2. **Log replication.** The leader sends every write to the others in order. A follower only accepts an entry if its log matches the leader's up to that point, so the copies can't drift apart.
3. **Commit rule.** A write counts as saved once a majority has it. Any two majorities out of 3 share at least one server, so the next leader always has every committed write.

The rule I had to be most careful with: a server syncs its log to disk *before* it votes or replies. If it replied first and then crashed, it could forget something it already agreed to.

## Why it helps ⚡

I ran the same random crash and network-split schedules against both approaches in the simulator (1,000 schedules, 1,500 ticks each):

| Approach | Confirmed writes lost | Schedules with a loss |
| --- | ---: | ---: |
| Async replication | 3,339 | 848 of 1,000 |
| Concord | 0 | 0 of 1,000 |

Concord confirmed 183,268 writes across those runs and lost none of them. These numbers come from simulated time on my laptop (Apple M1 Pro), not real servers.

Concord isn't free, though. Every write waits for a second server, which adds a network round trip. If 2 of the 3 servers are down, Concord stops taking writes instead of risking them. Async replication is still a good choice for things like caches or metrics, where losing the last few writes is okay. I haven't measured latency, throughput or failover time yet.

## The demo 🖥️

The demo is a 3D model of three data centers (Ashburn, Frankfurt and Singapore). Both approaches run on the same data centers and get hit by the same failures:

- **Power cut**: the main data center saves an order and loses power before it copies it.
- **Cable cut**: the main data center gets cut off, takes an order on its own, and the other two carry on without it.

You press **Next** to go through each step. The **What happened** log at the bottom shows both stores side by side, so you can see Async replication confirm an order that Concord is still waiting on. The counter at the top shows how many confirmed orders each one lost. There's also a **Try it yourself** panel where you can place orders, turn data centers off and cut cables.

The page runs Concord's real Raft code inside a simulator on the server, and every visitor gets their own private copy. A "power cut" is a simulated crash: whatever a server synced to disk survives, and everything else is lost.

## Built with 🛠️

- **Rust** for Raft, the write-ahead log, the key-value store and the simulator
- **tonic (gRPC)** for running a real three-node cluster
- **three.js** for the 3D data centers
- Plain HTML, CSS and JavaScript for the demo page, served by a small Rust HTTP server with no framework
- **Phosphor Icons** for the icons in the log

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

To put it on Railway, create a service from this repo. Railway builds the `Dockerfile` and sets `PORT` for you. Use `/healthz` as the health check path.

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
| `crates/sim` | The simulator, the chaos runs and the web demo. |
| `crates/server` | gRPC server and client for a real cluster. |

## Limits

- Always three servers. You can't add or remove one.
- Crash testing covers the simulator's crash model (synced data survives, unsynced data is lost). It doesn't test real power loss or disks that lie about syncing.
- Reads come from one server's copy, so they can be a little stale.
- There's no PreVote, so a single flaky cable can cause repeated elections.
- Not built yet: snapshots, log compaction, deletes, client sessions (a retried Put can be applied twice), TLS and auth. The log file grows forever.
