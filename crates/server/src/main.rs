//! `concord-server serve | put | get`. Minimal argument parsing: no CLI framework.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use concord_server::{start, KvCluster, NodeSpec};
use tokio::net::TcpListener;

const USAGE: &str = "usage:
  concord-server serve --id N --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data-dir DIR [--tick-ms 50]
  concord-server put   --addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 KEY VALUE
  concord-server get   --addr 127.0.0.1:7001 KEY      (reads that node's applied store; not linearizable)";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match run(args).await {
        Ok(()) => 0,
        Err(msg) => {
            eprintln!("{msg}");
            1
        }
    };
    std::process::exit(code);
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn positional(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut skip = false;
    for a in args.iter().skip(1) {
        if skip {
            skip = false;
        } else if a.starts_with("--") {
            skip = true;
        } else {
            out.push(a.clone());
        }
    }
    out
}

fn addrs(list: &str) -> Result<Vec<SocketAddr>, String> {
    list.split(',')
        .map(|s| s.parse().map_err(|_| format!("bad address {s}")))
        .collect()
}

async fn run(args: Vec<String>) -> Result<(), String> {
    match args.first().map(String::as_str) {
        Some("serve") => {
            let id: u64 = flag(&args, "--id")
                .and_then(|v| v.parse().ok())
                .ok_or(USAGE)?;
            let peers_arg = flag(&args, "--peers").ok_or(USAGE)?;
            let data_dir = PathBuf::from(flag(&args, "--data-dir").ok_or(USAGE)?);
            let tick_ms: u64 = flag(&args, "--tick-ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(50);
            let mut peers = BTreeMap::new();
            for part in peers_arg.split(',') {
                let (pid, addr) = part.split_once('=').ok_or(USAGE)?;
                peers.insert(
                    pid.parse::<u64>().map_err(|_| USAGE)?,
                    addr.parse::<SocketAddr>()
                        .map_err(|_| format!("bad address {addr}"))?,
                );
            }
            let me = *peers.get(&id).ok_or("--id must appear in --peers")?;
            let listener = TcpListener::bind(me)
                .await
                .map_err(|e| format!("bind {me}: {e}"))?;
            let spec = NodeSpec {
                id,
                peers,
                data_dir,
                tick: Duration::from_millis(tick_ms),
                rng_seed: id
                    .wrapping_mul(0x9E37_79B9)
                    .wrapping_add(std::process::id() as u64),
            };
            let node = start(spec, listener).await.map_err(|e| e.to_string())?;
            println!("node {id} listening on {}", node.addr);
            tokio::signal::ctrl_c().await.map_err(|e| e.to_string())?;
            node.kill();
            Ok(())
        }
        Some("put") => {
            let list = addrs(&flag(&args, "--addrs").ok_or(USAGE)?)?;
            let pos = positional(&args);
            let (key, value) = (pos.first().ok_or(USAGE)?, pos.get(1).ok_or(USAGE)?);
            let mut client = KvCluster::new(list);
            let resp = client
                .put(key.as_bytes(), value.as_bytes(), Duration::from_secs(10))
                .await
                .map_err(|e| e.to_string())?;
            println!(
                "committed on leader {} at index {} (term {})",
                resp.leader_id, resp.index, resp.term
            );
            Ok(())
        }
        Some("get") => {
            let addr: SocketAddr = flag(&args, "--addr")
                .ok_or(USAGE)?
                .parse()
                .map_err(|_| "bad --addr".to_string())?;
            let pos = positional(&args);
            let key = pos.first().ok_or(USAGE)?;
            let client = KvCluster::new(vec![addr]);
            match client
                .get_from(addr, key.as_bytes())
                .await
                .map_err(|e| e.to_string())?
            {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => println!("(not found)"),
            }
            Ok(())
        }
        _ => Err(USAGE.to_string()),
    }
}
