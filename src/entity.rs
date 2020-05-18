use crate::error::{RaftError, RaftResult};
use std::collections::HashMap;
use std::sync::RwLock;

// raft_id, entry_type , message
pub type Entry = (u64, u8, Vec<u8>);

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
