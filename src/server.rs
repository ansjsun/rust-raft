use crate::{entity::*, error::*, raft::Raft, state_machine::*};
use async_std::{
    net::{TcpListener, TcpStream},
    prelude::*,
    sync::RwLock,
    task,
};
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::sync::{atomic::Ordering::SeqCst, Arc};

pub struct Server {
    conf: Arc<Config>,
    raft_server: Arc<RaftServer>,
}

impl Server {
    pub fn new<R>(conf: Config, resolver: R) -> Self
    where
        R: Resolver + Sync + Send + 'static,
    {
        let conf = Arc::new(conf);

        Server {
            conf: conf.clone(),
            raft_server: Arc::new(RaftServer::new(Arc::new(Box::new(resolver)))),
        }
    }

    pub fn start(self: Arc<Server>) -> Arc<Server> {
        let s = self.clone();
        task::spawn(async move {
            s._start_heartbeat(s.conf.heartbeat_port).await;
        });
        let s = self.clone();
        task::spawn(async move {
            s._start_log(s.conf.replicate_port).await;
        });
        self
    }

    pub fn stop(&self) -> RaftResult<()> {
        panic!()
    }

    // If the leader is 0, the default discovery leader
    // when you fist create raft you can specify a leader to quickly form a raft group
    // replicas not have node_id , if have , it will remove the node id in replicas
    pub async fn create_raft<S>(
        &self,
        id: u64,
        start_index: u64,
        leader: u64,
        replicas: &Vec<u64>,
        s: S,
    ) -> RaftResult<Arc<Raft>>
    where
        S: StateMachine + Sync + Send + 'static,
    {
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

        let raft = Raft::new(
            id,
            start_index,
            self.conf.clone(),
            rep,
            self.raft_server.resolver.clone(),
            Arc::new(Box::new(s)),
        )
        .await?;

        raft.start();

        self.raft_server
            .rafts
            .write()
            .await
            .insert(id, raft.clone());

        if self.conf.node_id == leader {
            let _ = raft.try_to_leader();
        }

        Ok(raft)
    }

    pub async fn remove_raft(&self, id: u64) -> RaftResult<()> {
        match self.raft_server.rafts.write().await.remove(&id) {
            Some(_) => Ok(()),
            None => Err(RaftError::RaftNotFound(id)),
        }
    }

    pub async fn get_raft(&self, id: u64) -> RaftResult<Arc<Raft>> {
        match self.raft_server.rafts.write().await.remove(&id) {
            Some(r) => Ok(r),
            None => Err(RaftError::RaftNotFound(id)),
        }
    }

    pub async fn _start_log(&self, port: u16) {
        let rs = self.raft_server.clone();
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
            Ok(l) => l,
            Err(e) => panic!(RaftError::NetError(e.to_string())),
        };
        info!("start transport on server 0.0.0.0:{}", port);
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    task::spawn(log(rs.clone(), stream));
                }
                Err(e) => error!("listener has err:{}", e.to_string()),
            }
        }
    }

    pub async fn _start_heartbeat(&self, port: u16) {
        let rs = self.raft_server.clone();
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
            Ok(l) => l,
            Err(e) => panic!(RaftError::NetError(e.to_string())),
        };
        info!("start transport on server 0.0.0.0:{}", port);
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    task::spawn(heartbeat(rs.clone(), stream));
                }
                Err(e) => error!("listener has err:{}", e.to_string()),
            }
        }
    }
}

struct RaftServer {
    rafts: RwLock<HashMap<u64, Arc<Raft>>>,
    resolver: RSL,
}

impl RaftServer {
    fn new(resolver: RSL) -> Self {
        RaftServer {
            rafts: RwLock::new(HashMap::new()),
            resolver: resolver,
        }
    }

    async fn log(&self, raft_id: u64, entry: Entry) -> RaftResult<()> {
        let raft = match self.rafts.read().await.get(&raft_id) {
            Some(v) => v.clone(),
            None => return Err(RaftError::RaftNotFound(raft_id)),
        };

        match &entry {
            Entry::Commit { index, .. }
            | Entry::LeaderChange { index, .. }
            | Entry::MemberChange { index, .. } => {
                let applied_index = *index - 1;
                raft.store.commit(entry).await?;
                raft.applied.store(applied_index, SeqCst);
                raft.notify().await;
                Ok(())
            }
            Entry::Vote {
                leader,
                term,
                committed,
            } => raft.vote(*leader, *term, *committed).await,
            _ => {
                error!("err log type {:?}", entry);
                Err(RaftError::TypeErr)
            }
        }
    }

    async fn heartbeat(&self, raft_id: u64, entry: Entry) -> RaftResult<()> {
        let raft = match self.rafts.read().await.get(&raft_id) {
            Some(v) => v.clone(),
            None => return Err(RaftError::RaftNotFound(raft_id)),
        };

        match entry {
            Entry::Heartbeat {
                term,
                leader,
                committed,
                applied,
            } => raft.heartbeat(term, leader, committed, applied).await,
            _ => {
                error!("err heartbeat type {:?}", entry);
                Err(RaftError::TypeErr)
            }
        }
    }
}

async fn heartbeat(rs: Arc<RaftServer>, mut stream: TcpStream) {
    loop {
        if let Err(e) = match match Entry::decode_stream(&mut stream).await {
            Ok((raft_id, entry)) => rs.heartbeat(raft_id, entry).await,
            Err(e) => Err(e),
        } {
            Ok(()) => stream.write(SUCCESS).await,
            Err(e) => {
                let result = e.encode();
                if let Err(e) = stream.write(&u32::to_be_bytes(result.len() as u32)).await {
                    Err(e)
                } else {
                    stream.write(&result).await
                }
            }
        } {
            error!("send heartbeat result to client has err:{}", e);
        };
    }
}

async fn log(rs: Arc<RaftServer>, mut stream: TcpStream) {
    loop {
        if let Err(e) = match match Entry::decode_stream(&mut stream).await {
            Ok((raft_id, entry)) => rs.log(raft_id, entry).await,
            Err(e) => Err(e),
        } {
            Ok(()) => stream.write(SUCCESS).await,
            Err(e) => {
                let result = e.encode();
                if let Err(e) = stream.write(&u32::to_be_bytes(result.len() as u32)).await {
                    Err(e)
                } else {
                    stream.write(&result).await
                }
            }
        } {
            error!("send log result to client has err:{}", e);
        };
    }
}
