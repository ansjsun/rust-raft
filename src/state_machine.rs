use crate::{entity::*, error::*};
use std::collections::HashMap;
use std::sync::RwLock;

pub enum CommondType {
    Data,
    AddNode,
    RemoveNode,
}

pub trait StateMachine {
    fn apply(&self, command: &[u8], index: u64) -> RaftResult<()>;
    fn apply_member_change(&self, t: CommondType, index: u64) -> RaftResult<()>;
    fn apply_leader_change(&self, leader: u64, index: u64) -> RaftResult<()>;
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
