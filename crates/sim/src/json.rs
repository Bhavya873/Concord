//! Hand-written JSON for the demo page. No serde: the shape is small and fixed, and it only
//! carries what the page and the 3D scene read.

use std::fmt::Write;

use concord_raft::{Entry, EntryKind, Message};

use crate::{show, Cluster, Member, TicketState};

/// How many of a node's most recent log entries are sent. The page uses them to tell which data
/// centers have saved an order.
const LOG_WINDOW: u64 = 14;

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn kind_name(msg: &Message) -> &'static str {
    match msg {
        Message::RequestVote(_) => "RequestVote",
        Message::RequestVoteResp(_) => "VoteReply",
        Message::AppendEntries(a) if a.entries.is_empty() => "Heartbeat",
        Message::AppendEntries(_) => "AppendEntries",
        Message::AppendEntriesResp(_) => "AppendReply",
    }
}

/// Whether `m` has `e` on its disk (same index and term).
fn on_disk(m: &Member, e: &Entry) -> bool {
    m.disk
        .entries
        .get((e.index - 1) as usize)
        .is_some_and(|d| d.term == e.term)
}

fn node(out: &mut String, c: &Cluster, m: &Member) {
    let (role, entries): (_, Vec<Entry>) = match m.raft.as_ref() {
        Some(r) => {
            let last = r.last_log_index();
            let from = last.saturating_sub(LOG_WINDOW - 1).max(1);
            (
                r.role().as_str(),
                (from..=last).filter_map(|i| r.entry(i)).collect(),
            )
        }
        None => {
            let last = m.disk.entries.len() as u64;
            let from = last.saturating_sub(LOG_WINDOW - 1).max(1);
            (
                "down",
                (from..=last)
                    .filter_map(|i| m.disk.entries.get((i - 1) as usize).cloned())
                    .collect(),
            )
        }
    };
    // Orders (command entries) this node holds on disk, and ones that exist only in memory.
    let disk_puts = m
        .disk
        .entries
        .iter()
        .filter(|e| e.kind == EntryKind::Command)
        .count();
    let mem_puts = m.raft.as_ref().map_or(0, |r| {
        (1..=r.last_log_index())
            .filter_map(|i| r.entry(i))
            .filter(|e| e.kind == EntryKind::Command && !on_disk(m, e))
            .count()
    });
    let _ = write!(
        out,
        "{{\"id\":{},\"alive\":{},\"role\":{},\"isolated\":{},\"disk_puts\":{},\"mem_puts\":{},\"log\":[",
        m.id,
        m.alive(),
        esc(role),
        c.is_isolated(m.id),
        disk_puts,
        mem_puts
    );
    for (i, e) in entries.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let synced = !m.alive() || on_disk(m, e);
        let _ = write!(
            out,
            "{{\"i\":{},\"t\":{},\"s\":{}}}",
            e.index, e.term, synced
        );
    }
    out.push_str("]}");
}

pub fn cluster(c: &Cluster) -> String {
    let mut out = String::new();
    let _ = write!(
        out,
        "{{\"leader\":{},\"confirmed_lost\":{},\"tickets\":[",
        c.serving_leader()
            .map_or("null".to_string(), |id| id.to_string()),
        c.confirmed_lost()
    );
    for (i, (t, state)) in c.tickets.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let state = match state {
            TicketState::Pending => "pending",
            TicketState::Acked => "confirmed",
            TicketState::Lost => "not_confirmed",
            TicketState::ConfirmedLost => "confirmed_lost",
        };
        let _ = write!(
            out,
            "{{\"key\":{},\"node\":{},\"index\":{},\"term\":{},\"state\":{}}}",
            esc(&show(&t.key)),
            t.node,
            t.index,
            t.term,
            esc(state)
        );
    }
    out.push_str("],\"nodes\":[");
    for (n, m) in c.members.values().enumerate() {
        if n > 0 {
            out.push(',');
        }
        node(&mut out, c, m);
    }
    out.push_str("],\"cuts\":[");
    for (i, (a, b)) in c.cut_links.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "[{a},{b}]");
    }
    out.push_str("],\"flight\":[");
    for (i, (env, sent)) in c.in_flight().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"from\":{},\"to\":{},\"kind\":{},\"age\":{}}}",
            env.from,
            env.to,
            esc(kind_name(&env.msg)),
            c.tick.saturating_sub(sent)
        );
    }
    out.push_str("]}");
    out
}
