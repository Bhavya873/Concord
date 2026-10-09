//! In-memory key-value state machine. Put and Get only.
//!
//! Commands are a small length-prefixed encoding so this crate stays free of prost.
//! Applying the same Put again is idempotent, which makes log replay safe.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use thiserror::Error;

const PUT_TAG: u8 = 1;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KvError {
    #[error("command is truncated")]
    Truncated,
    #[error("unknown command tag {0}")]
    UnknownTag(u8),
    #[error("command has trailing bytes")]
    Trailing,
    #[error("key or value is longer than u32::MAX")]
    TooLarge,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
}

#[derive(Clone, Debug, Default)]
pub struct KvStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(Vec::as_slice)
    }

    pub fn apply(&mut self, command: &Command) {
        match command {
            Command::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
            }
        }
    }

    pub fn apply_data(&mut self, bytes: &[u8]) -> Result<(), KvError> {
        let command = decode(bytes)?;
        self.apply(&command);
        Ok(())
    }
}

pub fn encode_put(key: &[u8], value: &[u8]) -> Result<Vec<u8>, KvError> {
    let key_len = u32::try_from(key.len()).map_err(|_| KvError::TooLarge)?;
    let value_len = u32::try_from(value.len()).map_err(|_| KvError::TooLarge)?;
    let mut out = Vec::with_capacity(1 + 8 + key.len() + value.len());
    out.push(PUT_TAG);
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&value_len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(out)
}

pub fn decode(bytes: &[u8]) -> Result<Command, KvError> {
    let (tag, rest) = bytes.split_first().ok_or(KvError::Truncated)?;
    if *tag != PUT_TAG {
        return Err(KvError::UnknownTag(*tag));
    }
    let (key, rest) = read_bytes(rest)?;
    let (value, rest) = read_bytes(rest)?;
    if !rest.is_empty() {
        return Err(KvError::Trailing);
    }
    Ok(Command::Put { key, value })
}

fn read_bytes(input: &[u8]) -> Result<(Vec<u8>, &[u8]), KvError> {
    if input.len() < 4 {
        return Err(KvError::Truncated);
    }
    let (len_bytes, rest) = input.split_at(4);
    let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
    let len = usize::try_from(len).map_err(|_| KvError::Truncated)?;
    if rest.len() < len {
        return Err(KvError::Truncated);
    }
    let (body, rest) = rest.split_at(len);
    Ok((body.to_vec(), rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_round_trips() {
        let mut store = KvStore::new();
        assert_eq!(store.get(b"missing"), None);
        let encoded = encode_put(b"k", b"v").unwrap();
        store.apply_data(&encoded).unwrap();
        assert_eq!(store.get(b"k"), Some(b"v".as_slice()));
        store.apply(&Command::Put {
            key: b"k".to_vec(),
            value: b"v2".to_vec(),
        });
        assert_eq!(store.get(b"k"), Some(b"v2".as_slice()));
    }

    #[test]
    fn decode_rejects_truncated_and_trailing_bytes() {
        assert_eq!(decode(&[]), Err(KvError::Truncated));
        let mut encoded = encode_put(b"a", b"b").unwrap();
        encoded.pop();
        assert_eq!(decode(&encoded), Err(KvError::Truncated));
        let mut extra = encode_put(b"a", b"b").unwrap();
        extra.push(9);
        assert_eq!(decode(&extra), Err(KvError::Trailing));
    }
}
