//! In-memory Raft log. Index 0 is a sentinel so `entries[i].index == i`.

use crate::{Entry, EntryKind, LogIndex, RaftError, Term};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RaftLog {
    entries: Vec<Entry>,
}

impl RaftLog {
    pub(crate) fn new() -> Self {
        Self {
            entries: vec![sentinel()],
        }
    }

    pub(crate) fn from_entries(stored: Vec<Entry>) -> Result<Self, RaftError> {
        let mut log = Self::new();
        for entry in stored {
            log.append(entry)?;
        }
        Ok(log)
    }

    pub(crate) fn last_index(&self) -> LogIndex {
        u64::try_from(self.entries.len())
            .unwrap_or(u64::MAX)
            .saturating_sub(1)
    }

    pub(crate) fn last_term(&self) -> Term {
        self.entries.last().map(|entry| entry.term).unwrap_or(0)
    }

    pub(crate) fn term(&self, index: LogIndex) -> Option<Term> {
        self.entries
            .get(usize_index(index)?)
            .map(|entry| entry.term)
    }

    pub(crate) fn get(&self, index: LogIndex) -> Option<Entry> {
        if index == 0 {
            return None;
        }
        self.entries.get(usize_index(index)?).cloned()
    }

    /// Entries with `index >= from`, capped at `max` entries. Index 0 is the sentinel and is never returned.
    pub(crate) fn slice_from(&self, from: LogIndex, max: usize) -> Vec<Entry> {
        if from == 0 {
            return Vec::new();
        }
        let Some(start) = usize_index(from) else {
            return Vec::new();
        };
        if start >= self.entries.len() || max == 0 {
            return Vec::new();
        }
        let end = start.saturating_add(max).min(self.entries.len());
        self.entries.get(start..end).unwrap_or(&[]).to_vec()
    }

    pub(crate) fn entries_after(&self, index: LogIndex) -> Vec<Entry> {
        let start = usize_index(index.saturating_add(1)).unwrap_or(self.entries.len());
        if start >= self.entries.len() {
            return Vec::new();
        }
        self.entries.get(start..).unwrap_or(&[]).to_vec()
    }

    pub(crate) fn append(&mut self, entry: Entry) -> Result<(), RaftError> {
        let expected = self.last_index().saturating_add(1);
        if entry.index != expected {
            return Err(RaftError::LogGap {
                expected,
                got: entry.index,
            });
        }
        if entry.index > 0 && entry.term == 0 {
            return Err(RaftError::BadConfig("log entry term must be non-zero"));
        }
        self.entries.push(entry);
        Ok(())
    }

    /// Delete entries with `index >= from`.
    pub(crate) fn truncate_from(&mut self, from: LogIndex) {
        let Some(at) = usize_index(from) else {
            return;
        };
        if at < self.entries.len() {
            self.entries.truncate(at);
        }
    }

    /// Candidate log is at least as up to date as ours (Raft §5.4.1).
    pub(crate) fn is_up_to_date(&self, last_term: Term, last_index: LogIndex) -> bool {
        let my_term = self.last_term();
        last_term > my_term || (last_term == my_term && last_index >= self.last_index())
    }

    pub(crate) fn first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .find(|entry| entry.index > 0 && entry.term == term)
            .map(|entry| entry.index)
    }

    pub(crate) fn last_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.index > 0 && entry.term == term)
            .map(|entry| entry.index)
    }
}

fn sentinel() -> Entry {
    Entry {
        index: 0,
        term: 0,
        kind: EntryKind::Noop,
        data: Vec::new(),
    }
}

fn usize_index(index: LogIndex) -> Option<usize> {
    usize::try_from(index).ok()
}

#[cfg(test)]
mod tests {
    use super::RaftLog;
    use crate::{Entry, EntryKind};

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            kind: EntryKind::Command,
            data: vec![u8::try_from(index).unwrap_or(0)],
        }
    }

    #[test]
    fn up_to_date_compares_term_then_index() {
        let mut log = RaftLog::new();
        log.append(entry(1, 1)).unwrap();
        log.append(entry(2, 1)).unwrap();
        assert!(log.is_up_to_date(1, 2));
        assert!(log.is_up_to_date(1, 3));
        assert!(log.is_up_to_date(2, 1));
        assert!(!log.is_up_to_date(1, 1));
        assert!(!log.is_up_to_date(0, 5));
    }

    #[test]
    fn truncate_drops_the_suffix() {
        let mut log = RaftLog::new();
        log.append(entry(1, 1)).unwrap();
        log.append(entry(2, 1)).unwrap();
        log.truncate_from(2);
        assert_eq!(log.last_index(), 1);
        assert!(log.get(2).is_none());
    }
}
