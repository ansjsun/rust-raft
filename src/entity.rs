use crate::error::{conver, RaftError, RaftResult};
use futures_util::io::AsyncReadExt;
use std::collections::HashMap;
use std::sync::RwLock;

pub enum Message {
    Heartbeat { leader: u64, term: u64 },
    Vote { apply_index: u64, term: u64 },
    MessageAppend { index: u64, commond: Vec<u8> },
}

pub mod entry_type {
    pub const HEARTBEAT: u8 = 0;
    pub const VOTE: u8 = 1;
}

//0 heartbeat
//1 vote
//2 message
// raft_id, entry_type , message
pub struct Entry {
    pub id: u64,
    pub msg: Message,
}

impl Entry {
    pub async fn decode<S: AsyncReadExt + Unpin>(mut stream: S) -> RaftResult<Self> {
        let mut output = [0u8; 8];
        let bytes = conver(stream.read(&mut output[..]).await)?;
        panic!();
    }
}

pub struct Config {
    pub node_id: u64,
    pub heartbeat_port: u16,
    pub replicate_port: u16,
}

pub enum RaftState {
    //leader id
    Follower,
    Leader,
    Candidate { num_votes: u32 },
}

pub trait Resolver {
    fn resolver(&self, node_id: &u64) -> RaftResult<&str>;
}

pub struct DefResolver {
    map: RwLock<HashMap<u64, &'static str>>,
}

impl Resolver for DefResolver {
    fn resolver(&self, node_id: &u64) -> RaftResult<&str> {
        match self.map.read().unwrap().get(node_id) {
            Some(v) => Ok(v),
            None => Err(RaftError::NotfoundAddr(*node_id)),
        }
    }
}

impl DefResolver {
    pub fn new() -> Self {
        return DefResolver {
            map: RwLock::new(HashMap::new()),
        };
    }

    pub fn add_node(&self, node_id: u64, addr: String) {
        let mut map = self.map.write().unwrap();
        if let Some(v) = map.remove(&node_id) {
            std::mem::forget(v);
        }
        map.insert(node_id, crate::string_to_static_str(addr));
    }

    pub fn remove_node(&self, node_id: u64) {
        let mut map = self.map.write().unwrap();
        if let Some(v) = map.remove(&node_id) {
            std::mem::forget(v);
        }
    }
}
