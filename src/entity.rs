use crate::error::{conver, RaftError, RaftResult};
use futures_util::io::AsyncReadExt;
use std::collections::HashMap;
use std::sync::RwLock;

pub trait Decode {
    type Item;
    fn decode(buf: Vec<u8>) -> RaftResult<Self::Item>;
}

pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub mod entry_type {
    pub const HEARTBEAT: u8 = 0;
    pub const VOTE: u8 = 1;
    pub const COMMIT: u8 = 2;
    pub const APPLY: u8 = 2;
}

pub enum InternalEntry {
    Heartbeat {
        term: u64,
        leader: u64,
    },
    Vote {
        term: u64,
        leader: u64,
        committed: u64,
    },
}

pub enum Entry {
    Commit {
        term: u64,
        index: u64,
        commond: Vec<u8>,
    },
    Apply {
        term: u64,
        index: u64,
    },
}

async fn read_u64<S: AsyncReadExt + Unpin>(mut stream: S) -> RaftResult<u64> {
    let mut output = [0u8; 8];
    conver(stream.read(&mut output[..]).await)?;
    Ok(u64::from_be_bytes(output))
}

fn read_u64_slice(s: &[u8], start: usize) -> u64 {
    let ptr = s[start..start + 8].as_ptr() as *const [u8; 8];
    u64::from_be_bytes(unsafe { *ptr })
}

impl Decode for InternalEntry {
    type Item = Self;
    fn decode(buf: Vec<u8>) -> RaftResult<Self::Item> {
        let entry = match buf[0] {
            entry_type::HEARTBEAT => InternalEntry::Heartbeat {
                term: read_u64_slice(&buf, 1),
                leader: read_u64_slice(&buf, 9),
            },
            entry_type::VOTE => InternalEntry::Vote {
                term: read_u64_slice(&buf, 1),
                leader: read_u64_slice(&buf, 9),
                committed: read_u64_slice(&buf, 17),
            },
            _ => return Err(RaftError::TypeErr),
        };
        Ok(entry)
    }
}

impl Encode for InternalEntry {
    fn encode(&self) -> Vec<u8> {
        let mut vec;
        match self {
            InternalEntry::Heartbeat { term, leader } => {
                vec = Vec::with_capacity(17);
                vec.push(entry_type::HEARTBEAT);
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*leader));
            }
            InternalEntry::Vote {
                term,
                leader,
                committed,
            } => {
                vec = Vec::with_capacity(25);
                vec.push(entry_type::VOTE);
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*leader));
                vec.extend_from_slice(&u64::to_be_bytes(*committed));
            }
        }
        vec
    }
}

impl InternalEntry {
    pub async fn decode_stream<S: AsyncReadExt + Unpin>(mut stream: S) -> RaftResult<(u64, Self)> {
        let raft_id = read_u64(&mut stream).await?;
        let mut buf = Vec::default();
        conver(stream.read_to_end(&mut buf).await)?;

        Ok((raft_id, Self::decode(buf)?))
    }
}

impl Decode for Entry {
    type Item = Self;
    fn decode(buf: Vec<u8>) -> RaftResult<Self::Item> {
        let entry = match buf[0] {
            entry_type::COMMIT => Entry::Commit {
                term: read_u64_slice(&buf, 1),
                index: read_u64_slice(&buf, 9),
                commond: buf,
            },
            entry_type::APPLY => Entry::Apply {
                term: read_u64_slice(&buf, 1),
                index: read_u64_slice(&buf, 9),
            },
            _ => return Err(RaftError::TypeErr),
        };
        Ok(entry)
    }
}

impl Encode for Entry {
    fn encode(&self) -> Vec<u8> {
        let mut vec: Vec<u8>;
        match &self {
            Entry::Commit {
                term,
                index,
                commond,
            } => {
                vec = Vec::with_capacity(17 + commond.len());
                vec.push(entry_type::COMMIT);
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*index));
                vec.extend_from_slice(commond);
            }
            Entry::Apply { term, index } => {
                vec = Vec::with_capacity(17);
                vec.push(entry_type::APPLY);
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*index));
            }
        }
        vec
    }
}

impl Entry {
    pub fn info(&self) -> (u64, u64, u64) {
        match &self {
            Entry::Commit {
                term,
                index,
                commond,
                ..
            } => (*term, *index, 17 + commond.len() as u64),
            Entry::Apply { term, index } => (*term, *index, 17),
        }
    }

    pub async fn decode_stream<S: AsyncReadExt + Unpin>(mut stream: S) -> RaftResult<(u64, Self)> {
        let raft_id = read_u64(&mut stream).await?;
        let mut buf = Vec::default();
        conver(stream.read_to_end(&mut buf).await)?;
        Ok((raft_id, Self::decode(buf)?))
    }
}

pub struct Config {
    pub node_id: u64,
    pub heartbeat_port: u16,
    pub replicate_port: u16,
    pub log_path: String,
    // how size of MB for file
    pub log_size_m: u64,
}

pub enum RaftState {
    //leader id
    Follower,
    Leader,
    Candidate { num_votes: u32 },
}
