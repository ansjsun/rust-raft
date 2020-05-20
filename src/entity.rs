use crate::error::{conver, RaftError, RaftResult};
use futures_util::io::AsyncReadExt;
use std::collections::HashMap;
use std::sync::RwLock;

pub enum HeartbeatEntry {
    Heartbeat {
        term: u64,
        leader: u64,
    },
    Vote {
        term: u64,
        leader: u64,
        apply_index: u64,
    },
}

pub enum Entry {
    Log {
        index: u64,
        term: u64,
        commond: Vec<u8>,
    },
}

pub mod entry_type {
    pub const HEARTBEAT: u8 = 0;
    pub const VOTE: u8 = 1;
    pub const LOG: u8 = 2;
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

impl HeartbeatEntry {
    pub async fn decode_stream<S: AsyncReadExt + Unpin>(mut stream: S) -> RaftResult<(u64, Self)> {
        let raft_id = read_u64(&mut stream).await?;
        let mut buf = Vec::default();
        conver(stream.read_to_end(&mut buf).await)?;

        Ok((raft_id, Self::decode(buf)?))
    }

    pub fn decode(buf: Vec<u8>) -> RaftResult<Self> {
        let entry = match buf[0] {
            entry_type::HEARTBEAT => HeartbeatEntry::Heartbeat {
                term: read_u64_slice(&buf, 1),
                leader: read_u64_slice(&buf, 9),
            },
            entry_type::VOTE => HeartbeatEntry::Vote {
                term: read_u64_slice(&buf, 1),
                leader: read_u64_slice(&buf, 9),
                apply_index: read_u64_slice(&buf, 17),
            },
            _ => return Err(RaftError::TypeErr),
        };
        Ok(entry)
    }
}

impl Entry {
    pub fn len(&self) -> u64 {
        match &self {
            Entry::Log { commond, .. } => 24 + commond.len() as u64,
        }
    }

    pub fn ecode(&self) -> Vec<u8> {
        let len = self.len() as usize;
        let mut vec = Vec::with_capacity(4 + len);
        vec.extend_from_slice(&u32::to_be_bytes(len as u32));
        match &self {
            Entry::Log {
                index,
                term,
                commond,
            } => {
                vec.push(entry_type::LOG);
                vec.extend_from_slice(&u64::to_be_bytes(*index));
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(commond);
            }
        }
        vec
    }
    pub async fn decode_stream<S: AsyncReadExt + Unpin>(mut stream: S) -> RaftResult<(u64, Self)> {
        let raft_id = read_u64(&mut stream).await?;
        let mut buf = Vec::default();
        conver(stream.read_to_end(&mut buf).await)?;
        Ok((raft_id, Self::decode(buf)?))
    }

    pub fn decode(buf: Vec<u8>) -> RaftResult<Self> {
        let entry = match buf[0] {
            entry_type::LOG => Entry::Log {
                term: read_u64_slice(&buf, 1),
                index: read_u64_slice(&buf, 9),
                commond: buf,
            },
            _ => return Err(RaftError::TypeErr),
        };
        Ok(entry)
    }
}

pub struct Config {
    pub node_id: u64,
    pub heartbeat_port: u16,
    pub replicate_port: u16,
    pub log_path: String,
}

pub enum RaftState {
    //leader id
    Follower,
    Leader,
    Candidate { num_votes: u32 },
}
