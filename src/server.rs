use crate::raft_server::RaftServer;
use crate::{entity::*, error::*, raft::Raft};

use log::{error, info};
use smol::{Async, Task};
use std::borrow::Cow;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::{Arc, RwLock};

pub struct TransportServer {
    config: Arc<Config>,
    raft_server: Arc<RaftServer>,
}

impl TransportServer {
    pub fn new(conf: Config) -> Self {
        let conf = Arc::new(conf);
        TransportServer {
            config: conf.clone(),
            raft_server: Arc::new(RaftServer::new(conf.clone())),
        }
    }

    pub fn start(&self) -> RaftResult<()> {
        let mut threads = Vec::new();
        threads.push(self._start_heartbeat(self.config.heartbeat_port));
        // threads.push(self._start_replicate(self.config.replicate_port));
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
    let (raft_id, entry) = HeartbeatEntry::decode_stream(&mut stream).await?;
    let raft = match rs.rafts.read().unwrap().get(&raft_id) {
        Some(v) => v.clone(),
        None => return Err(RaftError::RaftNotFound(raft_id)),
    };

    match entry {
        HeartbeatEntry::Heartbeat { leader, term } => raft.heartbeat(leader, term),
        HeartbeatEntry::Vote {
            leader,
            term,
            apply_index,
        } => raft.vote(leader, apply_index, term),
        _ => return Err(RaftError::TypeErr),
    }
}

pub async fn apply_log(rs: Arc<RaftServer>, mut stream: Async<TcpStream>) -> RaftResult<()> {
    let (raft_id, entry) = HeartbeatEntry::decode_stream(&mut stream).await?;
    let raft = match rs.rafts.read().unwrap().get(&raft_id) {
        Some(v) => v.clone(),
        None => return Err(RaftError::RaftNotFound(raft_id)),
    };

    match entry {
        HeartbeatEntry::Heartbeat { leader, term } => raft.heartbeat(leader, term),
        HeartbeatEntry::Vote {
            leader,
            term,
            apply_index,
        } => raft.vote(leader, apply_index, term),
        _ => return Err(RaftError::TypeErr),
    }
}
