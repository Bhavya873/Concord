//! Browser demo over the deterministic simulator. Std only: no web framework.
//!
//! Two clusters run side by side on the same seed and receive the same faults:
//! Concord tells the client "confirmed" once a write is committed on a majority; the baseline
//! tells it as soon as the leader has the write on its own disk. Raft runs identically in both.
//!
//! Every visitor gets a private pair of clusters, keyed by a `sid` cookie (see
//! `concord_sim::demo_session`). Sessions advance lazily on each request.
//!
//! `cargo run -p concord-sim --bin concord-demo` then open http://127.0.0.1:8766

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use concord_sim::demo_session::{Session, Sessions, IDLE_TTL, MAX_SESSIONS};
use concord_sim::{Cluster, PutError, TicketState};
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};

const PAGE: &str = include_str!("../../web/index.html");

const SCENE: &str = include_str!("../../web/scene.js");

/// The page with the scene script's URL versioned by a hash of its contents, so a browser that
/// cached an older `scene.js` fetches the new one after a deploy.
fn page() -> &'static str {
    static PAGE_VERSIONED: OnceLock<String> = OnceLock::new();
    PAGE_VERSIONED.get_or_init(|| {
        // FNV-1a: stable across builds and platforms, unlike `DefaultHasher`.
        let hash = SCENE.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
        PAGE.replace("__SCENE_VERSION__", &format!("{hash:016x}"))
    })
}

/// Static files the page loads, embedded so the binary is self-contained. Anything not listed
/// here is a 404; there is no file-system access.
const ASSETS: &[(&str, &str, &str)] = &[
    ("/scene.js", "text/javascript; charset=utf-8", SCENE),
    (
        "/vendor/three.module.min.js",
        "text/javascript; charset=utf-8",
        include_str!("../../web/vendor/three.module.min.js"),
    ),
    (
        "/vendor/OrbitControls.js",
        "text/javascript; charset=utf-8",
        include_str!("../../web/vendor/OrbitControls.js"),
    ),
    (
        "/vendor/icons.svg",
        "image/svg+xml",
        include_str!("../../web/vendor/icons.svg"),
    ),
    (
        "/vendor/LICENSE-phosphor.txt",
        "text/plain; charset=utf-8",
        include_str!("../../web/vendor/LICENSE-phosphor.txt"),
    ),
    (
        "/vendor/LICENSE-three.txt",
        "text/plain; charset=utf-8",
        include_str!("../../web/vendor/LICENSE-three.txt"),
    ),
];

/// Requests are small GETs and POSTs with query strings; anything bigger is refused.
const MAX_REQUEST_BYTES: usize = 4096;
/// Connections handled at once. Beyond this the server answers 503 straight away.
const MAX_CONNECTIONS: usize = 64;

struct App {
    sessions: Mutex<Sessions>,
    ids: Mutex<ChaCha8Rng>,
    connections: AtomicUsize,
}

impl App {
    fn new_id(&self) -> String {
        let mut rng = match self.ids.lock() {
            Ok(rng) => rng,
            Err(poisoned) => poisoned.into_inner(),
        };
        format!("{:016x}{:016x}", rng.next_u64(), rng.next_u64())
    }
}

/// What a handler sends back. `sid` is set when the visitor needs a (new) session cookie.
struct Reply {
    status: &'static str,
    ctype: &'static str,
    body: String,
    sid: Option<String>,
    /// Static assets may be cached for an hour; everything else is `no-store`.
    cacheable: bool,
}

impl Reply {
    fn new(status: &'static str, ctype: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            ctype,
            body: body.into(),
            sid: None,
            cacheable: false,
        }
    }

    fn text(status: &'static str, body: &str) -> Self {
        Self::new(status, "text/plain; charset=utf-8", body)
    }
}

fn main() {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8766);
    let host = if std::env::var("PORT").is_ok() {
        "0.0.0.0"
    } else {
        "127.0.0.1"
    };
    let listener = match TcpListener::bind((host, port)) {
        Ok(l) => l,
        Err(err) => {
            eprintln!("cannot bind {host}:{port}: {err}");
            std::process::exit(1);
        }
    };
    // Session ids only keep visitors apart; they guard nothing of value. Seeding from the clock
    // and process id is enough to make them unpredictable to a casual visitor.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let app = Arc::new(App {
        sessions: Mutex::new(Sessions::new(MAX_SESSIONS, IDLE_TTL)),
        ids: Mutex::new(ChaCha8Rng::seed_from_u64(
            nanos ^ u64::from(std::process::id()).rotate_left(32),
        )),
        connections: AtomicUsize::new(0),
    });

    println!("Concord demo (simulator) on http://{host}:{port}");
    for stream in listener.incoming().flatten() {
        let app = Arc::clone(&app);
        if app.connections.fetch_add(1, Ordering::SeqCst) >= MAX_CONNECTIONS {
            app.connections.fetch_sub(1, Ordering::SeqCst);
            let mut stream = stream;
            let _ = respond(&mut stream, Reply::text("503 Service Unavailable", "busy"));
            continue;
        }
        thread::spawn(move || {
            let mut stream = stream;
            let reply = match read_request(&mut stream) {
                Ok(Some(req)) => route(&app, &req),
                Ok(None) => Reply::text("413 Payload Too Large", "request too large"),
                Err(_) => Reply::text("400 Bad Request", "bad request"),
            };
            let _ = respond(&mut stream, reply);
            app.connections.fetch_sub(1, Ordering::SeqCst);
        });
    }
}

struct Request {
    method: String,
    path: String,
    query: String,
    sid: Option<String>,
}

/// Read up to the end of the headers. `Ok(None)` when the request is too large.
fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Request>> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_REQUEST_BYTES {
            return Ok(None);
        }
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or("").split_whitespace();
    let method = first.next().unwrap_or("").to_string();
    let target = first.next().unwrap_or("/");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let sid = lines
        .take_while(|l| !l.is_empty())
        .filter_map(|l| l.split_once(':'))
        .filter(|(name, _)| name.trim().eq_ignore_ascii_case("cookie"))
        .flat_map(|(_, value)| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == "sid")
        .map(|(_, value)| value.to_string())
        .filter(|v| v.len() == 32 && v.bytes().all(|b| b.is_ascii_hexdigit()));
    Ok(Some(Request {
        method,
        path: path.to_string(),
        query: query.to_string(),
        sid,
    }))
}

fn respond(stream: &mut TcpStream, reply: Reply) -> std::io::Result<()> {
    let cookie = reply
        .sid
        .map(|sid| {
            format!(
                "Set-Cookie: sid={sid}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}\r\n",
                IDLE_TTL.as_secs()
            )
        })
        .unwrap_or_default();
    let cache = if reply.cacheable {
        "public, max-age=3600"
    } else {
        "no-store"
    };
    let head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: {cache}\r\n\
         X-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n{cookie}Connection: close\r\n\r\n",
        reply.status,
        reply.ctype,
        reply.body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(reply.body.as_bytes())
}

fn route(app: &App, req: &Request) -> Reply {
    let q = req.query.as_str();
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/healthz") => Reply::text("200 OK", "ok"),
        ("GET", "/") => {
            // Hand out the session cookie with the page so the first API call already has it.
            let mut r = Reply::new("200 OK", "text/html; charset=utf-8", page());
            r.sid = with_session(app, req, |_| String::new()).sid;
            r
        }
        ("GET", "/api/state") => with_session(app, req, |s| s.to_json()),
        ("POST", "/api/put") => {
            let (key, value) = (param(q, "key"), param(q, "value"));
            each(app, req, |c| {
                c.put(key.as_bytes(), value.as_bytes())
                    .map(|_| ())
                    .map_err(put_error)
            })
        }
        ("POST", "/api/kill") => on_id(app, req, Cluster::kill),
        ("POST", "/api/restart") => on_id(app, req, Cluster::restart),
        ("POST", "/api/cut-link") => {
            let (a, b) = (id_param(q, "a"), id_param(q, "b"));
            each(app, req, |c| {
                c.cut_link(a, b);
                Ok(())
            })
        }
        ("POST", "/api/restore-link") => {
            let (a, b) = (id_param(q, "a"), id_param(q, "b"));
            each(app, req, |c| {
                c.restore_link(a, b);
                Ok(())
            })
        }
        ("POST", "/api/heal") => each(app, req, |c| {
            c.heal();
            Ok(())
        }),
        ("POST", "/api/restart-all") => each(app, req, |c| {
            for id in [1, 2, 3] {
                c.restart(id);
            }
            Ok(())
        }),
        ("POST", "/api/scenario/queue-then-crash") => with_session(app, req, |s| {
            let (key, value) = (param(q, "key"), param(q, "value"));
            reply(&queue_then_crash(s, &key, &value), s)
        }),
        ("POST", "/api/scenario/cut-primary-and-queue") => {
            let (key, value) = (param(q, "key"), param(q, "value"));
            each(app, req, |c| {
                let leader = c
                    .serving_leader()
                    .ok_or_else(|| put_error(PutError::NoLeader))?;
                c.isolate(leader);
                c.put_on(leader, key.as_bytes(), value.as_bytes())
                    .map(|_| ())
                    .map_err(put_error)
            })
        }
        ("POST", "/api/reset") => {
            let now = Instant::now();
            with_session(app, req, |s| {
                *s = Session::new(1, now);
                reply("", s)
            })
        }
        ("GET", path) if path.starts_with("/scene.js") || path.starts_with("/vendor/") => {
            match ASSETS.iter().find(|(p, _, _)| *p == path) {
                Some((_, ctype, body)) => {
                    let mut r = Reply::new("200 OK", ctype, *body);
                    r.cacheable = true;
                    r
                }
                None => Reply::text("404 Not Found", "not found"),
            }
        }
        ("GET", _) | ("POST", _) => Reply::text("404 Not Found", "not found"),
        _ => Reply::text("405 Method Not Allowed", "method not allowed"),
    }
}

/// Run `f` on the visitor's session after catching it up to now.
fn with_session(app: &App, req: &Request, f: impl FnOnce(&mut Session) -> String) -> Reply {
    let now = Instant::now();
    let mut sessions = match app.sessions.lock() {
        Ok(s) => s,
        Err(poisoned) => poisoned.into_inner(),
    };
    let (sid, created, session) = sessions.touch(req.sid.as_deref(), || app.new_id(), now);
    session.catch_up(now);
    let mut r = Reply::new("200 OK", "application/json", f(session));
    if created {
        r.sid = Some(sid);
    }
    r
}

/// Apply `f` to both clusters. They see the same faults, so the first error stands for both.
fn each(app: &App, req: &Request, mut f: impl FnMut(&mut Cluster) -> Result<(), String>) -> Reply {
    with_session(app, req, |s| {
        let concord = f(&mut s.concord);
        let baseline = f(&mut s.baseline);
        reply(&concord.and(baseline).err().unwrap_or_default(), s)
    })
}

fn on_id(app: &App, req: &Request, f: impl Fn(&mut Cluster, u64)) -> Reply {
    let id = id_param(&req.query, "id");
    each(app, req, |c| {
        f(c, id);
        Ok(())
    })
}

/// Queue a write on both clusters, run until the baseline leader has it on its own disk (the
/// moment the baseline tells the client "confirmed"), then crash both leaders before their
/// messages leave. Both clusters step together, so they stay in lockstep.
fn queue_then_crash(s: &mut Session, key: &str, value: &str) -> String {
    let base = s.baseline.put(key.as_bytes(), value.as_bytes());
    let conc = s.concord.put(key.as_bytes(), value.as_bytes());
    let (base, conc) = match (base, conc) {
        (Ok(b), Ok(c)) => (b, c),
        (Err(e), _) | (_, Err(e)) => return put_error(e),
    };
    for _ in 0..10 {
        if s.baseline.ticket_state(&base) == TicketState::Acked {
            break;
        }
        s.step();
    }
    s.baseline.crash_now(base.node);
    s.concord.crash_now(conc.node);
    String::new()
}

fn put_error(err: PutError) -> String {
    match err {
        PutError::NoLeader => {
            "No main data center right now. The others are picking one; try again in a moment."
                .to_string()
        }
        PutError::NodeDown => "That data center is turned off.".to_string(),
        PutError::NotLeader { .. } => "That data center isn't the main one.".to_string(),
    }
}

fn reply(error: &str, s: &Session) -> String {
    let error = error.replace('\\', "\\\\").replace('"', "'");
    format!("{{\"error\":\"{error}\",\"state\":{}}}", s.to_json())
}

fn id_param(query: &str, name: &str) -> u64 {
    param(query, name).parse().unwrap_or(0)
}

fn param(query: &str, name: &str) -> String {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == name {
                return decode(v);
            }
        }
    }
    String::new()
}

fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).chars().take(32).collect()
}
