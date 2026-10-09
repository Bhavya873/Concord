//! Append-only record log with CRC32 framing.
//!
//! Frame: `len: u32 LE | crc32(payload): u32 LE | payload`.
//! Payload tags: `1` hard state, `2` entry, `3` truncate-from.
//!
//! Recovery replays every frame. A frame that runs past end of file, or a final frame whose
//! CRC does not match, is a torn tail: it is cut off and the file is synced. A bad CRC on a
//! frame that is followed by more data is corruption and is returned as an error.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use concord_raft::{Entry, EntryKind, HardState, LogIndex};
use thiserror::Error;

const TAG_HARD: u8 = 1;
const TAG_ENTRY: u8 = 2;
const TAG_TRUNCATE: u8 = 3;
const HEADER: usize = 8;
/// Upper bound on one payload. Guards recovery against a garbage length field.
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("wal io: {0}")]
    Io(#[from] io::Error),
    #[error("wal corrupt at byte {offset}: {reason}")]
    Corrupt { offset: u64, reason: &'static str },
    #[error("entry is too large for one wal record")]
    TooLarge,
}

/// Durable state rebuilt from the file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recovered {
    pub hard_state: HardState,
    pub entries: Vec<Entry>,
    /// Bytes dropped from a torn tail. Zero on a clean file.
    pub torn_bytes: u64,
}

pub struct Wal {
    file: File,
    path: PathBuf,
}

impl Wal {
    /// Open or create `path` and replay it.
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Recovered), WalError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let (recovered, good_len) = replay(&bytes)?;
        let file_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if good_len < file_len {
            file.set_len(good_len)?;
            file.sync_data()?;
        }
        file.seek(SeekFrom::Start(good_len))?;
        Ok((Self { file, path }, recovered))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write the pieces of one `Ready` and `sync_data` before returning.
    /// Callers must not send a vote or an AppendEntries reply until this returns `Ok`.
    pub fn persist(
        &mut self,
        hard_state: Option<HardState>,
        truncate_from: Option<LogIndex>,
        entries: &[Entry],
    ) -> Result<(), WalError> {
        if hard_state.is_none() && truncate_from.is_none() && entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        if let Some(from) = truncate_from {
            frame(&mut buf, &encode_truncate(from))?;
        }
        for entry in entries {
            frame(&mut buf, &encode_entry(entry)?)?;
        }
        if let Some(hs) = hard_state {
            frame(&mut buf, &encode_hard(&hs))?;
        }
        self.file.write_all(&buf)?;
        self.file.sync_data()?;
        Ok(())
    }
}

fn frame(out: &mut Vec<u8>, payload: &[u8]) -> Result<(), WalError> {
    if payload.len() > MAX_PAYLOAD {
        return Err(WalError::TooLarge);
    }
    let len = u32::try_from(payload.len()).map_err(|_| WalError::TooLarge)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

fn encode_hard(hs: &HardState) -> Vec<u8> {
    let mut out = Vec::with_capacity(25);
    out.push(TAG_HARD);
    out.extend_from_slice(&hs.term.to_le_bytes());
    out.extend_from_slice(&hs.voted_for.unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&hs.commit.to_le_bytes());
    out
}

fn encode_entry(entry: &Entry) -> Result<Vec<u8>, WalError> {
    let data_len = u32::try_from(entry.data.len()).map_err(|_| WalError::TooLarge)?;
    let mut out = Vec::with_capacity(25 + entry.data.len());
    out.push(TAG_ENTRY);
    out.extend_from_slice(&entry.index.to_le_bytes());
    out.extend_from_slice(&entry.term.to_le_bytes());
    out.extend_from_slice(&entry.kind.as_u32().to_le_bytes());
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(&entry.data);
    Ok(out)
}

fn encode_truncate(from: LogIndex) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(TAG_TRUNCATE);
    out.extend_from_slice(&from.to_le_bytes());
    out
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).and_then(|b| b.first().copied())
    }

    fn u32(&mut self) -> Option<u32> {
        let b: [u8; 4] = self.take(4)?.try_into().ok()?;
        Some(u32::from_le_bytes(b))
    }

    fn u64(&mut self) -> Option<u64> {
        let b: [u8; 8] = self.take(8)?.try_into().ok()?;
        Some(u64::from_le_bytes(b))
    }
}

/// Returns the recovered state and the byte length of the valid prefix.
fn replay(bytes: &[u8]) -> Result<(Recovered, u64), WalError> {
    let mut rec = Recovered::default();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let offset = u64::try_from(pos).unwrap_or(u64::MAX);
        let Some(header) = bytes.get(pos..pos + HEADER) else {
            break;
        };
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if len > MAX_PAYLOAD {
            return Err(WalError::Corrupt {
                offset,
                reason: "frame length out of range",
            });
        }
        let start = pos + HEADER;
        let Some(payload) = bytes.get(start..start + len) else {
            break;
        };
        let end = start + len;
        if crc32fast::hash(payload) != crc {
            if end == bytes.len() {
                break;
            }
            return Err(WalError::Corrupt {
                offset,
                reason: "crc mismatch before end of file",
            });
        }
        apply_record(&mut rec, payload, offset)?;
        pos = end;
    }
    let good = u64::try_from(pos).unwrap_or(u64::MAX);
    let total = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    rec.torn_bytes = total - good;
    Ok((rec, good))
}

fn apply_record(rec: &mut Recovered, payload: &[u8], offset: u64) -> Result<(), WalError> {
    let bad = |reason| WalError::Corrupt { offset, reason };
    let mut c = Cursor {
        buf: payload,
        pos: 0,
    };
    match c.u8().ok_or_else(|| bad("empty record"))? {
        TAG_HARD => {
            let term = c.u64().ok_or_else(|| bad("short hard state"))?;
            let voted = c.u64().ok_or_else(|| bad("short hard state"))?;
            let commit = c.u64().ok_or_else(|| bad("short hard state"))?;
            rec.hard_state = HardState {
                term,
                voted_for: if voted == 0 { None } else { Some(voted) },
                commit,
            };
        }
        TAG_ENTRY => {
            let index = c.u64().ok_or_else(|| bad("short entry"))?;
            let term = c.u64().ok_or_else(|| bad("short entry"))?;
            let kind = c.u32().ok_or_else(|| bad("short entry"))?;
            let n = c.u32().ok_or_else(|| bad("short entry"))? as usize;
            let data = c.take(n).ok_or_else(|| bad("short entry data"))?.to_vec();
            let kind = EntryKind::from_u32(kind).ok_or_else(|| bad("unknown entry kind"))?;
            let expected = u64::try_from(rec.entries.len())
                .unwrap_or(u64::MAX)
                .saturating_add(1);
            if index != expected {
                return Err(bad("entry index is not contiguous"));
            }
            rec.entries.push(Entry {
                index,
                term,
                kind,
                data,
            });
        }
        TAG_TRUNCATE => {
            let from = c.u64().ok_or_else(|| bad("short truncate"))?;
            if from == 0 {
                return Err(bad("truncate from index 0"));
            }
            let keep = usize::try_from(from - 1).unwrap_or(usize::MAX);
            rec.entries.truncate(keep);
        }
        _ => return Err(bad("unknown record tag")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn entry(index: u64, term: u64, data: &[u8]) -> Entry {
        Entry {
            index,
            term,
            kind: EntryKind::Command,
            data: data.to_vec(),
        }
    }

    fn hs(term: u64, voted: Option<u64>, commit: u64) -> HardState {
        HardState {
            term,
            voted_for: voted,
            commit,
        }
    }

    #[test]
    fn empty_file_recovers_to_default() {
        let dir = tempfile::tempdir().unwrap();
        let (_, rec) = Wal::open(dir.path().join("wal")).unwrap();
        assert_eq!(rec, Recovered::default());
    }

    #[test]
    fn persisted_state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (mut wal, _) = Wal::open(&path).unwrap();
            wal.persist(
                Some(hs(2, Some(3), 1)),
                None,
                &[entry(1, 1, b"a"), entry(2, 2, b"b")],
            )
            .unwrap();
        }
        let (_, rec) = Wal::open(&path).unwrap();
        assert_eq!(rec.hard_state, hs(2, Some(3), 1));
        assert_eq!(rec.entries, vec![entry(1, 1, b"a"), entry(2, 2, b"b")]);
        assert_eq!(rec.torn_bytes, 0);
    }

    #[test]
    fn truncate_then_append_replays_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (mut wal, _) = Wal::open(&path).unwrap();
            wal.persist(
                None,
                None,
                &[entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")],
            )
            .unwrap();
            wal.persist(None, Some(2), &[entry(2, 2, b"B")]).unwrap();
        }
        let (_, rec) = Wal::open(&path).unwrap();
        assert_eq!(rec.entries, vec![entry(1, 1, b"a"), entry(2, 2, b"B")]);
    }

    #[test]
    fn torn_tail_is_cut_and_earlier_records_survive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (mut wal, _) = Wal::open(&path).unwrap();
            wal.persist(None, None, &[entry(1, 1, b"keep")]).unwrap();
            wal.persist(None, None, &[entry(2, 1, b"torn-away")])
                .unwrap();
        }
        let len = fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 5).unwrap();
        drop(f);
        let (mut wal, rec) = Wal::open(&path).unwrap();
        assert_eq!(rec.entries, vec![entry(1, 1, b"keep")]);
        assert!(rec.torn_bytes > 0);
        // The file is usable again after the cut.
        wal.persist(None, None, &[entry(2, 3, b"fresh")]).unwrap();
        drop(wal);
        let (_, rec) = Wal::open(&path).unwrap();
        assert_eq!(
            rec.entries,
            vec![entry(1, 1, b"keep"), entry(2, 3, b"fresh")]
        );
    }

    #[test]
    fn bad_crc_on_last_frame_is_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (mut wal, _) = Wal::open(&path).unwrap();
            wal.persist(None, None, &[entry(1, 1, b"ok")]).unwrap();
            wal.persist(None, None, &[entry(2, 1, b"bad")]).unwrap();
        }
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let (_, rec) = Wal::open(&path).unwrap();
        assert_eq!(rec.entries, vec![entry(1, 1, b"ok")]);
    }

    #[test]
    fn bad_crc_before_the_end_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (mut wal, _) = Wal::open(&path).unwrap();
            wal.persist(None, None, &[entry(1, 1, b"first")]).unwrap();
            wal.persist(None, None, &[entry(2, 1, b"second")]).unwrap();
        }
        let mut bytes = fs::read(&path).unwrap();
        bytes[HEADER + 3] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(Wal::open(&path), Err(WalError::Corrupt { .. })));
    }

    #[test]
    fn non_contiguous_entries_are_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (mut wal, _) = Wal::open(&path).unwrap();
            wal.persist(None, None, &[entry(1, 1, b"a"), entry(3, 1, b"gap")])
                .unwrap();
        }
        assert!(matches!(Wal::open(&path), Err(WalError::Corrupt { .. })));
    }
}
