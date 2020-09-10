use crate::error::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub type RSL = Arc<Box<dyn Resolver + Sync + Send + 'static>>;
pub type SM = Arc<Box<dyn StateMachine + Sync + Send + 'static>>;

#[async_trait]
pub trait StateMachine {
    /// this is a call execute method for leader, and no change log,
    /// example search method only run leader, now you can call it in replica , it forward to leader and return result .
    fn execute(&self, command: &[u8]) -> RaftResult<Vec<u8>>;
    fn apply_log(&self, term: u64, index: u64, command: &[u8]) -> RaftResult<()>;
    async fn apply_member_change(
        &self,
        term: u64,
        index: u64,
        node_id: u64,
        action: u8,
        exists: bool,
    ) -> RaftResult<()>;

    async fn apply_leader_change(&self, term: u64, index: u64, leader: u64) -> RaftResult<()>;
}

pub trait Resolver {
    fn heartbeat_addr(&self, node_id: &u64) -> RaftResult<String>;
    fn log_addr(&self, node_id: &u64) -> RaftResult<String>;
}

pub struct DefResolver {
    log_addrs: HashMap<u64, String>,
    internal_addrs: HashMap<u64, String>,
}

impl Resolver for DefResolver {
    fn heartbeat_addr(&self, node_id: &u64) -> RaftResult<String> {
        match self.internal_addrs.get(node_id) {
            Some(v) => Ok(v.to_string()),
            None => Err(RaftError::NotfoundAddr(*node_id)),
        }
    }

    fn log_addr(&self, node_id: &u64) -> RaftResult<String> {
        match self.log_addrs.get(node_id) {
            Some(v) => Ok(v.to_string()),
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

    pub fn add_node(&mut self, node_id: u64, host: String, heartbeat_port: u16, log_port: u16) {
        if let Some(v) = self.log_addrs.remove(&node_id) {
            std::mem::forget(v);
        }

        if let Some(v) = self.internal_addrs.remove(&node_id) {
            std::mem::forget(v);
        }

        self.log_addrs
            .insert(node_id, format!("{}:{}", host, log_port));

        self.internal_addrs
            .insert(node_id, format!("{}:{}", host, heartbeat_port));
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
