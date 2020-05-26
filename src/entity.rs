use crate::error::{conver, RaftError, RaftResult};
use futures_util::io::AsyncReadExt;

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
    pub const APPLY: u8 = 3;
    pub const TO_LEADER: u8 = 4;
}

#[derive(Debug)]
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
    Heartbeat {
        term: u64,
        leader: u64,
        committed: u64,
        applied: u64,
    },
    Vote {
        leader: u64,
        term: u64,
        committed: u64,
    },
    ToLeader {
        leader: u64,
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

impl Decode for Entry {
    type Item = Self;
    #[warn(unreachable_patterns)]
    fn decode(buf: Vec<u8>) -> RaftResult<Self::Item> {
        let entry = match buf[0] {
            entry_type::HEARTBEAT => Entry::Heartbeat {
                term: read_u64_slice(&buf, 1),
                leader: read_u64_slice(&buf, 9),
                committed: read_u64_slice(&buf, 17),
                applied: read_u64_slice(&buf, 25),
            },
            entry_type::COMMIT => Entry::Commit {
                term: read_u64_slice(&buf, 1),
                index: read_u64_slice(&buf, 9),
                commond: buf,
            },
            entry_type::APPLY => Entry::Apply {
                term: read_u64_slice(&buf, 1),
                index: read_u64_slice(&buf, 9),
            },
            entry_type::VOTE => Entry::Vote {
                leader: read_u64_slice(&buf, 1),
                term: read_u64_slice(&buf, 9),
                committed: read_u64_slice(&buf, 17),
            },
            entry_type::TO_LEADER => Entry::ToLeader {
                leader: read_u64_slice(&buf, 1),
                term: read_u64_slice(&buf, 9),
                index: read_u64_slice(&buf, 17),
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
            Entry::Heartbeat {
                term,
                leader,
                committed,
                applied,
            } => {
                vec = Vec::with_capacity(17);
                vec.push(entry_type::HEARTBEAT);
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*leader));
                vec.extend_from_slice(&u64::to_be_bytes(*committed));
                vec.extend_from_slice(&u64::to_be_bytes(*applied));
            }
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
            Entry::Vote {
                term,
                leader,
                committed,
            } => {
                vec = Vec::with_capacity(25);
                vec.push(entry_type::VOTE);
                vec.extend_from_slice(&u64::to_be_bytes(*leader));
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*committed));
            }
            Entry::ToLeader {
                term,
                leader,
                index,
            } => {
                vec = Vec::with_capacity(25);
                vec.push(entry_type::VOTE);
                vec.extend_from_slice(&u64::to_be_bytes(*leader));
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
            Entry::Heartbeat {
                term, committed, ..
            } => (*term, *committed, 33),
            Entry::Vote {
                term, committed, ..
            } => (*term, *committed, 33),
            Entry::ToLeader { term, index, .. } => (*term, *index, 33),
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
    // how size of num for memory
    pub log_max_num: usize,
    // how size of num for memory
    pub log_min_num: usize,
    // how size of num for memory
    pub log_file_size_mb: u64,
    //Three  without a heartbeat , follower to begin consecutive elections
    pub heartbeate_ms: u64,
}

#[derive(Clone)]
pub enum RaftState {
    //leader id
    Follower,
    Leader,
    Candidate,
}
