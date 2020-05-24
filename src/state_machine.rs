use crate::error::*;
use std::collections::HashMap;
use std::sync::Arc;

pub enum CommondType {
    Data,
    AddNode,
    RemoveNode,
}

pub type RSL = Arc<Box<dyn Resolver + Sync + Send + 'static>>;
pub type SM = Arc<Box<dyn StateMachine + Sync + Send + 'static>>;

pub trait StateMachine {
    fn apply(&self, term: &u64, index: &u64, command: &[u8]) -> RaftResult<()>;
    fn apply_member_change(&self, t: CommondType, index: u64) -> RaftResult<()>;
    fn apply_leader_change(&self, leader: u64, index: u64) -> RaftResult<()>;
}

pub trait Resolver {
    fn heartbeat_addr(&self, node_id: &u64) -> RaftResult<&str>;
    fn log_addr(&self, node_id: &u64) -> RaftResult<&str>;
}

pub struct DefResolver {
    log_addrs: HashMap<u64, &'static str>,
    internal_addrs: HashMap<u64, &'static str>,
}

impl Resolver for DefResolver {
    fn heartbeat_addr(&self, node_id: &u64) -> RaftResult<&str> {
        match self.internal_addrs.get(node_id) {
            Some(v) => Ok(v),
            None => Err(RaftError::NotfoundAddr(*node_id)),
        }
    }

    fn log_addr(&self, node_id: &u64) -> RaftResult<&str> {
        match self.log_addrs.get(node_id) {
            Some(v) => Ok(v),
            None => Err(RaftError::NotfoundAddr(*node_id)),
        }
    }
}

impl DefResolver {
    pub fn new() -> Self {
        return DefResolver {
            log_addrs: HashMap::new(),
            internal_addrs: HashMap::new(),
        };
    }

    pub fn add_node(&mut self, node_id: u64, host: String, log_port: u16, addr_port: u16) {
        if let Some(v) = self.log_addrs.remove(&node_id) {
            std::mem::forget(v);
        }

        if let Some(v) = self.internal_addrs.remove(&node_id) {
            std::mem::forget(v);
        }

        self.log_addrs.insert(
            node_id,
            crate::string_to_static_str(format!("{}:{}", host, log_port)),
        );

        self.internal_addrs.insert(
            node_id,
            crate::string_to_static_str(format!("{}:{}", host, log_port)),
        );
    }

    pub fn remove_node(&mut self, node_id: u64) {
        if let Some(v) = self.log_addrs.remove(&node_id) {
            std::mem::forget(v);
        }

        if let Some(v) = self.internal_addrs.remove(&node_id) {
            std::mem::forget(v);
        }
    }
}
