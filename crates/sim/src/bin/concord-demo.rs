//! Browser demo: two traffic intersections, one with Raft and one without. Std only.
//!
//! Every visitor gets a private intersection, keyed by a `sid` cookie (see
//! `concord_sim::demo_session`). Sessions advance lazily on each request, so an idle tab costs
//! nothing. The page draws the cars; the lights and controllers come from the simulator.
//!
//! `cargo run -p concord-sim --bin concord-demo` then open http://127.0.0.1:8766. With `PORT`
//! set (as on Railway) it listens on `0.0.0.0:$PORT`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use concord_sim::demo_session::{Session, Sessions, IDLE_TTL, MAX_SESSIONS, TICK_MS};
use concord_sim::intersection::Intersection;
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};

/// The page, embedded so the binary is self-contained. It loads nothing but Google Fonts.
const PAGE: &str = include_str!("../../web/intersection.html");

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
}

impl Reply {
    fn new(status: &'static str, ctype: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            ctype,
            body: body.into(),
            sid: None,
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
    let head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\n\
         X-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n{cookie}Connection: close\r\n\r\n",
        reply.status,
        reply.ctype,
        reply.body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(reply.body.as_bytes())
}

fn route(app: &App, req: &Request) -> Reply {
    let id = || id_param(&req.query, "id");
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/healthz") => Reply::text("200 OK", "ok"),
        // The session cookie comes with the page, so the first API call already has it.
        ("GET", "/") => {
            let mut r = Reply::new("200 OK", "text/html; charset=utf-8", PAGE);
            r.sid = with_session(app, req, |_| String::new()).sid;
            r
        }
        ("GET", "/api/x/state") => with_session(app, req, state),
        ("POST", "/api/x/cable") => with_session(app, req, |s| {
            s.intersection.toggle_cable(id());
            state(s)
        }),
        ("POST", "/api/x/power") => with_session(app, req, |s| {
            s.intersection.toggle_power(id());
            state(s)
        }),
        ("POST", "/api/x/reset") => with_session(app, req, |s| {
            s.intersection = Intersection::new(1);
            state(s)
        }),
        ("GET", _) | ("POST", _) => Reply::text("404 Not Found", "not found"),
        _ => Reply::text("405 Method Not Allowed", "method not allowed"),
    }
}

fn state(s: &mut Session) -> String {
    s.intersection.to_json(TICK_MS)
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
