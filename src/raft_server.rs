use crate::{
    entity::*,
    error::*,
    raft::Raft,
    state_machine::{DefResolver, Resolver},
};
use log::{error, info};
use smol::{Async, Task};
use std::borrow::Cow;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

pub struct RaftServer {
    pub config: Arc<Config>,
    pub rafts: RwLock<HashMap<u64, Arc<Raft>>>,
    pub resolver: Arc<Resolver + Sync + Send>,
}

impl RaftServer {
    pub fn new(conf: Arc<Config>) -> Self {
        RaftServer {
            config: conf,
            rafts: RwLock::new(HashMap::new()),
            resolver: Arc::new(DefResolver::new()),
        }
    }

    pub fn stop(&self) -> RaftResult<()> {
        panic!()
    }

    pub fn create_raft(&self, id: u64) -> RaftResult<Arc<Raft>> {
        panic!()
    }

    pub fn remove_raft(&self, id: u64) -> RaftResult<()> {
        match self.rafts.write().unwrap().remove(&id) {
            Some(_) => Ok(()),
            None => Err(RaftError::RaftNotFound(id)),
        }
    }

    pub fn get_raft(&self, id: u64) -> RaftResult<Arc<Raft>> {
        match self.rafts.write().unwrap().remove(&id) {
            Some(r) => Ok(r),
            None => Err(RaftError::RaftNotFound(id)),
        }
    }
}
