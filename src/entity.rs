use crate::error::{convert, RaftError, RaftResult};
use async_std::net::TcpStream;
use async_std::prelude::*;

pub trait Decode {
    type Item;
    fn decode(buf: &[u8]) -> RaftResult<Self::Item>;
}

pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub mod entry_type {
    pub const HEARTBEAT: u8 = 0;
    pub const COMMIT: u8 = 1;
    pub const VOTE: u8 = 2;
    pub const LEADER_CHANGE: u8 = 3;
    pub const MEMBER_CHANGE: u8 = 4;
}

pub mod action_type {
    pub const ADD: u8 = 5;
    pub const REMOVE: u8 = 6;
}

#[derive(Debug, Clone)]
pub enum Entry {
    Commit {
        pre_term: u64,
        term: u64,
        index: u64,
        commond: Vec<u8>,
    },
    LeaderChange {
        pre_term: u64,
        term: u64,
        index: u64,
        leader: u64,
    },
    MemberChange {
        pre_term: u64,
        term: u64,
        index: u64,
        node_id: u64,
        action: u8,
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
}

async fn read_u32(stream: &mut TcpStream) -> RaftResult<u32> {
    let mut output = [0u8; 4];
    convert(stream.read_exact(&mut output[..]).await)?;
    Ok(u32::from_be_bytes(output))
}

async fn read_u64(stream: &mut TcpStream) -> RaftResult<u64> {
    let mut output = [0u8; 8];
    convert(stream.read_exact(&mut output[..]).await)?;
    Ok(u64::from_be_bytes(output))
}

fn read_u64_slice(s: &[u8], start: usize) -> u64 {
    let ptr = s[start..start + 8].as_ptr() as *const [u8; 8];
    u64::from_be_bytes(unsafe { *ptr })
}

impl Decode for Entry {
    type Item = Self;
    #[warn(unreachable_patterns)]
    fn decode(buf: &[u8]) -> RaftResult<Self::Item> {
        if buf.len() == 0 {
            return Err(RaftError::IncompleteErr(0));
        }

        let entry = match buf[0] {
            entry_type::HEARTBEAT => {
                if buf.len() != 33 {
                    return Err(RaftError::IncompleteErr(buf[0] as u64));
                }
                Entry::Heartbeat {
                    term: read_u64_slice(&buf, 1),
                    leader: read_u64_slice(&buf, 9),
                    committed: read_u64_slice(&buf, 17),
                    applied: read_u64_slice(&buf, 25),
                }
            }
            entry_type::COMMIT => {
                if buf.len() < 25 {
                    return Err(RaftError::IncompleteErr(buf[0] as u64));
                }
                Entry::Commit {
                    pre_term: read_u64_slice(&buf, 1),
                    term: read_u64_slice(&buf, 9),
                    index: read_u64_slice(&buf, 17),
                    commond: buf[25..].to_vec(),
                }
            }
            entry_type::VOTE => {
                if buf.len() != 25 {
                    return Err(RaftError::IncompleteErr(buf[0] as u64));
                }
                Entry::Vote {
                    leader: read_u64_slice(&buf, 1),
                    term: read_u64_slice(&buf, 9),
                    committed: read_u64_slice(&buf, 17),
                }
            }
            entry_type::LEADER_CHANGE => {
                if buf.len() != 33 {
                    return Err(RaftError::IncompleteErr(buf[0] as u64));
                }
                Entry::LeaderChange {
                    pre_term: read_u64_slice(&buf, 1),
                    term: read_u64_slice(&buf, 9),
                    index: read_u64_slice(&buf, 17),
                    leader: read_u64_slice(&buf, 25),
                }
            }
            entry_type::MEMBER_CHANGE => {
                if buf.len() != 34 {
                    return Err(RaftError::IncompleteErr(buf[0] as u64));
                }
                Entry::MemberChange {
                    pre_term: read_u64_slice(&buf, 1),
                    term: read_u64_slice(&buf, 9),
                    index: read_u64_slice(&buf, 17),
                    node_id: read_u64_slice(&buf, 25),
                    action: buf[33],
                }
            }
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
                vec = Vec::with_capacity(33);
                vec.push(entry_type::HEARTBEAT);
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*leader));
                vec.extend_from_slice(&u64::to_be_bytes(*committed));
                vec.extend_from_slice(&u64::to_be_bytes(*applied));
            }
            Entry::Commit {
                pre_term,
                term,
                index,
                commond,
            } => {
                vec = Vec::with_capacity(25 + commond.len());
                vec.push(entry_type::COMMIT);
                vec.extend_from_slice(&u64::to_be_bytes(*pre_term));
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*index));
                vec.extend_from_slice(commond);
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
            Entry::LeaderChange {
                pre_term,
                term,
                index,
                leader,
            } => {
                vec = Vec::with_capacity(33);
                vec.push(entry_type::LEADER_CHANGE);
                vec.extend_from_slice(&u64::to_be_bytes(*pre_term));
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*index));
                vec.extend_from_slice(&u64::to_be_bytes(*leader));
            }
            Entry::MemberChange {
                pre_term,
                term,
                index,
                node_id,
                action,
            } => {
                vec = Vec::with_capacity(34);
                vec.push(entry_type::MEMBER_CHANGE);
                vec.extend_from_slice(&u64::to_be_bytes(*pre_term));
                vec.extend_from_slice(&u64::to_be_bytes(*term));
                vec.extend_from_slice(&u64::to_be_bytes(*index));
                vec.extend_from_slice(&u64::to_be_bytes(*node_id));
                vec.push(*action);
            }
        }
        vec
    }
}

impl Entry {
    pub fn info(&self) -> (u64, u64) {
        match &self {
            Entry::Commit { term, index, .. } => (*term, *index),
            Entry::Heartbeat {
                term, committed, ..
            } => (*term, *committed),
            Entry::Vote {
                term, committed, ..
            } => (*term, *committed),
            Entry::LeaderChange { term, index, .. } => (*term, *index),
            Entry::MemberChange { term, index, .. } => (*term, *index),
        }
    }

    // it action need to store raft log
    //it return (pre_term , term, index)
    pub fn commit_info(&self) -> (u64, u64, u64) {
        match &self {
            Entry::Commit {
                pre_term,
                term,
                index,
                ..
            } => (*pre_term, *term, *index),
            Entry::LeaderChange {
                pre_term,
                term,
                index,
                ..
            } => (*pre_term, *term, *index),
            Entry::MemberChange {
                pre_term,
                term,
                index,
                ..
            } => (*pre_term, *term, *index),
            _ => panic!(format!("not support commit_info by this type:{:?}", self)),
        }
    }

    pub async fn decode_stream(stream: &mut TcpStream) -> RaftResult<(u64, Self)> {
        let raft_id = read_u64(stream).await?;
        let len = read_u32(stream).await?;
        let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
        buf.resize_with(len as usize, Default::default);
        convert(stream.read_exact(&mut buf).await)?;
        Ok((raft_id, Self::decode(&buf)?))
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
