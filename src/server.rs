use crate::{entity::*, error::*, raft::Raft, state_machine::*};

use log::{error, info};
use smol::{Async, Task};
use std::collections::{HashMap, HashSet};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

pub struct Server {
    conf: Arc<Config>,
    raft_server: Arc<RaftServer>,
}

impl Server {
    pub fn new<R, S>(conf: Config, resolver: R, sm: S) -> Self
    where
        R: Resolver + Sync + Send + 'static,
        S: StateMachine + Sync + Send + 'static,
    {
        let conf = Arc::new(conf);
        Server {
            conf: conf.clone(),
            raft_server: Arc::new(RaftServer::new(
                Arc::new(Box::new(resolver)),
                Arc::new(Box::new(sm)),
            )),
        }
    }

    pub fn start(self: Arc<Server>) -> Arc<Server> {
        let s = self.clone();
        smol::Task::blocking(async move {
            s._start_heartbeat(s.conf.heartbeat_port).await;
        })
        .detach();

        let s = self.clone();
        smol::Task::blocking(async move {
            s._start_replicate(s.conf.replicate_port).await;
        })
        .detach();

        self
    }

    pub fn stop(&self) -> RaftResult<()> {
        panic!()
    }

    // If the leader is 0, the default discovery leader
    // when you fist create raft you can specify a leader to quickly form a raft group
    // replicas not have node_id , if have , it will remove the node id in replicas
    pub fn create_raft(&self, id: u64, leader: u64, replicas: &Vec<u64>) -> RaftResult<Arc<Raft>> {
        let mut set = HashSet::new();
        set.insert(self.conf.node_id);
        let rep = replicas
            .iter()
            .map(|x| *x)
            .filter(|x| {
                if set.contains(x) {
                    false
                } else {
                    set.insert(*x);
                    true
                }
            })
            .collect();

        let raft = Arc::new(Raft::new(
            id,
            self.conf.clone(),
            rep,
            self.raft_server.resolver.clone(),
            self.raft_server.sm.clone(),
        )?);

        raft.start();

        self.raft_server
            .rafts
            .write()
            .unwrap()
            .insert(id, raft.clone());

        if self.conf.node_id == leader {
            let _ = Raft::try_to_leader(raft.clone());
        }

        Ok(raft)
    }

    pub fn remove_raft(&self, id: u64) -> RaftResult<()> {
        match self.raft_server.rafts.write().unwrap().remove(&id) {
            Some(_) => Ok(()),
            None => Err(RaftError::RaftNotFound(id)),
        }
    }

    pub fn get_raft(&self, id: u64) -> RaftResult<Arc<Raft>> {
        match self.raft_server.rafts.write().unwrap().remove(&id) {
            Some(r) => Ok(r),
            None => Err(RaftError::RaftNotFound(id)),
        }
    }

    pub async fn _start_replicate(&self, port: u16) {
        let rs = self.raft_server.clone();
        let listener = match Async::<TcpListener>::bind(format!("0.0.0.0:{}", port)) {
            Ok(l) => l,
            Err(e) => panic!(RaftError::NetError(e.to_string())),
        };
        info!("start transport on server 0.0.0.0:{}", port);
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let rs = rs.clone();
                    Task::spawn(log(rs, stream)).unwrap().detach();
                }
                Err(e) => error!("listener has err:{}", e.to_string()),
            }
        }
    }

    pub async fn _start_heartbeat(&self, port: u16) {
        let rs = self.raft_server.clone();
        let listener = match Async::<TcpListener>::bind(format!("0.0.0.0:{}", port)) {
            Ok(l) => l,
            Err(e) => panic!(RaftError::NetError(e.to_string())),
        };
        info!("start transport on server 0.0.0.0:{}", port);
        loop {
            let rs = rs.clone();
            match listener.accept().await {
                Ok((stream, _)) => {
                    Task::spawn(heartbeat(rs, stream)).unwrap().detach();
                }
                Err(e) => error!("listener has err:{}", e.to_string()),
            }
        }
    }
}

struct RaftServer {
    rafts: RwLock<HashMap<u64, Arc<Raft>>>,
    resolver: RSL,
    sm: SM,
}

impl RaftServer {
    fn new(resolver: RSL, sm: SM) -> Self {
        RaftServer {
            rafts: RwLock::new(HashMap::new()),
            resolver: resolver,
            sm: sm,
        }
    }
}

async fn heartbeat(rs: Arc<RaftServer>, mut stream: Async<TcpStream>) -> RaftResult<()> {
    let (raft_id, entry) = Entry::decode_stream(&mut stream).await?;
    let raft = match rs.rafts.read().unwrap().get(&raft_id) {
        Some(v) => v.clone(),
        None => return Err(RaftError::RaftNotFound(raft_id)),
    };

    match entry {
        Entry::Heartbeat {
            term,
            leader,
            committed,
            applied,
        } => raft.heartbeat(term, leader, committed, applied),
        _ => {
            error!("err heartbeat type {:?}", entry);
            Err(RaftError::TypeErr)
        }
    }
}

async fn log(rs: Arc<RaftServer>, mut stream: Async<TcpStream>) -> RaftResult<()> {
    let (raft_id, entry) = Entry::decode_stream(&mut stream).await?;

    println!("{}, {:?}", raft_id, entry);

    let raft = match rs.rafts.read().unwrap().get(&raft_id) {
        Some(v) => v.clone(),
        None => return Err(RaftError::RaftNotFound(raft_id)),
    };

    match &entry {
        Entry::Commit { .. } => raft.store.commit(entry),
        Entry::Apply { term, index } => raft.update_apply(*term, *index),
        Entry::Vote {
            leader,
            term,
            committed,
        } => raft.vote(*leader, *term, *committed),
        Entry::ToLeader {
            leader,
            term,
            index,
        } => raft.leader(*leader, *term, *index),
        _ => {
            error!("err heartbeat type {:?}", entry);
            Err(RaftError::TypeErr)
        }
    }
}
