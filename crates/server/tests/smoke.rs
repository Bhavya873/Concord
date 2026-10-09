//! Three real nodes on localhost: gRPC between them, one WAL file each.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use concord_server::{start, KvCluster, NodeHandle, NodeSpec};
use tokio::net::TcpListener;

fn spec(id: u64, peers: &BTreeMap<u64, SocketAddr>, dir: &Path) -> NodeSpec {
    NodeSpec {
        id,
        peers: peers.clone(),
        data_dir: dir.join(format!("node{id}")),
        tick: Duration::from_millis(20),
        rng_seed: 1000 + id,
    }
}

async fn eventually<F, Fut>(what: &str, limit: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < limit {
        if f().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_survive_a_leader_kill_and_recover_from_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut listeners = Vec::new();
    let mut peers = BTreeMap::new();
    for id in 1..=3u64 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        peers.insert(id, l.local_addr().unwrap());
        listeners.push((id, l));
    }
    let mut nodes: BTreeMap<u64, NodeHandle> = BTreeMap::new();
    for (id, l) in listeners {
        nodes.insert(id, start(spec(id, &peers, dir.path()), l).await.unwrap());
    }
    let addrs: Vec<SocketAddr> = peers.values().copied().collect();
    let mut client = KvCluster::new(addrs.clone());

    let first = client
        .put(b"city", b"lisbon", Duration::from_secs(10))
        .await
        .expect("first write");
    let leader = first.leader_id;

    // Followers apply the write too.
    for addr in peers.values() {
        let c = &client;
        eventually(
            "value on every node",
            Duration::from_secs(5),
            || async move {
                c.get_from(*addr, b"city").await.ok().flatten().as_deref()
                    == Some(b"lisbon".as_slice())
            },
        )
        .await;
    }

    // Kill the leader. The acknowledged write must survive on the other two.
    nodes[&leader].kill();
    let second = client
        .put(b"river", b"tagus", Duration::from_secs(15))
        .await
        .expect("write after leader kill");
    assert_ne!(second.leader_id, leader, "dead node cannot lead");
    for (id, addr) in &peers {
        if *id == leader {
            continue;
        }
        let c = &client;
        eventually(
            "survivors hold both writes",
            Duration::from_secs(5),
            || async move {
                let a = c.get_from(*addr, b"city").await.ok().flatten();
                let b = c.get_from(*addr, b"river").await.ok().flatten();
                a.as_deref() == Some(b"lisbon".as_slice())
                    && b.as_deref() == Some(b"tagus".as_slice())
            },
        )
        .await;
    }

    // Restart the killed node on its old address. It rebuilds from its WAL and catches up.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let l = TcpListener::bind(peers[&leader]).await.unwrap();
    let back = start(spec(leader, &peers, dir.path()), l).await.unwrap();
    nodes.insert(leader, back);
    let c = &client;
    let addr = peers[&leader];
    eventually(
        "restarted node catches up",
        Duration::from_secs(10),
        || async move {
            let a = c.get_from(addr, b"city").await.ok().flatten();
            let b = c.get_from(addr, b"river").await.ok().flatten();
            a.as_deref() == Some(b"lisbon".as_slice()) && b.as_deref() == Some(b"tagus".as_slice())
        },
    )
    .await;

    for node in nodes.values() {
        node.kill();
    }
}
