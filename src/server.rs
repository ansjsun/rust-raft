use crate::{entity::*, error::*, raft::Raft, state_machine::*};

use crate::storage::RaftLog;
use log::{error, info};
use smol::{Async, Task};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::net::{TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
    Arc, Mutex, RwLock,
};

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
                conf.clone(),
                Arc::new(Box::new(resolver)),
                Arc::new(Box::new(sm)),
            )),
        }
    }

    pub fn start(&self) -> RaftResult<()> {
        let mut threads = Vec::new();
        threads.push(self._start_heartbeat(self.conf.heartbeat_port));
        threads.push(self._start_replicate(self.conf.replicate_port));
        for t in threads {
            t.join().unwrap()?;
        }
        return Ok(());
    }

    pub fn _start_replicate(&self, port: u16) -> std::thread::JoinHandle<RaftResult<()>> {
        let rs = self.raft_server.clone();
        std::thread::spawn(move || {
            smol::run(async {
                let listener = match Async::<TcpListener>::bind(format!("0.0.0.0:{}", port)) {
                    Ok(l) => l,
                    Err(e) => return Err(RaftError::NetError(e.to_string())),
                };
                info!("start transport on server 0.0.0.0:{}", port);
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let rs = rs.clone();
                            Task::spawn(apply_log(rs, stream)).unwrap().detach();
                        }
                        Err(e) => error!("listener has err:{}", e.to_string()),
                    }
                }
            })
        })
    }

    pub fn _start_heartbeat(&self, port: u16) -> std::thread::JoinHandle<RaftResult<()>> {
        let rs = self.raft_server.clone();
        std::thread::spawn(move || {
            smol::run(async {
                let listener = match Async::<TcpListener>::bind(format!("0.0.0.0:{}", port)) {
                    Ok(l) => l,
                    Err(e) => return Err(RaftError::NetError(e.to_string())),
                };
                info!("start transport on server 0.0.0.0:{}", port);
                loop {
                    let rs = rs.clone();
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            Task::spawn(apply_heartbeat(rs, stream)).unwrap().detach();
                        }
                        Err(e) => error!("listener has err:{}", e.to_string()),
                    }
                }
            })
        })
    }
}

pub async fn apply_heartbeat(rs: Arc<RaftServer>, mut stream: Async<TcpStream>) -> RaftResult<()> {
    let (raft_id, entry) = InternalEntry::decode_stream(&mut stream).await?;
    let raft = match rs.rafts.read().unwrap().get(&raft_id) {
        Some(v) => v.clone(),
        None => return Err(RaftError::RaftNotFound(raft_id)),
    };

    match entry {
        InternalEntry::Heartbeat {
            term,
            leader,
            committed,
            applied,
        } => raft.heartbeat(term, leader, committed, applied),
        InternalEntry::Vote {
            leader,
            term,
            committed,
        } => raft.vote(leader, committed, term),
    }
}

pub struct RaftServer {
    pub conf: Arc<Config>,
    pub rafts: RwLock<HashMap<u64, Arc<Raft>>>,
    pub resolver: RSL,
    sm: SM,
}

impl RaftServer {
    fn new(conf: Arc<Config>, resolver: RSL, sm: SM) -> Self {
        RaftServer {
            conf: conf,
            rafts: RwLock::new(HashMap::new()),
            resolver: resolver,
            sm: sm,
        }
    }

    fn stop(&self) -> RaftResult<()> {
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
            self.resolver.clone(),
            self.sm.clone(),
        )?);

        Raft::start(raft.clone());

        Ok(raft)
    }

    fn remove_raft(&self, id: u64) -> RaftResult<()> {
        match self.rafts.write().unwrap().remove(&id) {
            Some(_) => Ok(()),
            None => Err(RaftError::RaftNotFound(id)),
        }
    }

    fn get_raft(&self, id: u64) -> RaftResult<Arc<Raft>> {
        match self.rafts.write().unwrap().remove(&id) {
            Some(r) => Ok(r),
            None => Err(RaftError::RaftNotFound(id)),
        }
    }
}

pub async fn apply_log(rs: Arc<RaftServer>, mut stream: Async<TcpStream>) -> RaftResult<()> {
    let (raft_id, entry) = Entry::decode_stream(&mut stream).await?;
    let raft = match rs.rafts.read().unwrap().get(&raft_id) {
        Some(v) => v.clone(),
        None => return Err(RaftError::RaftNotFound(raft_id)),
    };

    if let Entry::Apply { term, index } = &entry {
        raft.check_apply(*term, *index)?;
        raft.apply.store(*index, SeqCst);
        return Ok(());
    };

    raft.store.commit(entry)
}
