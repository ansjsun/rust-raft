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
        leader: u64,
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
        Ok((raft_id, entry))
    }
}

impl Entry {
    pub async fn decode_stream<S: AsyncReadExt + Unpin>(mut stream: S) -> RaftResult<(u64, Self)> {
        let raft_id = read_u64(&mut stream).await?;
        let mut buf = Vec::default();
        conver(stream.read_to_end(&mut buf).await)?;

        let entry = match buf[0] {
            entry_type::LOG => Entry::Log {
                term: read_u64_slice(&buf, 1),
                leader: read_u64_slice(&buf, 9),
                index: read_u64_slice(&buf, 17),
                commond: buf,
            },
            _ => return Err(RaftError::TypeErr),
        };

        Ok((raft_id, entry))
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
