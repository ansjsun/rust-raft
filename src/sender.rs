use crate::entity::entry_type;
use crate::error::*;
use crate::raft::Raft;
use async_std::{net::TcpStream, prelude::*, sync::channel, sync::RwLock, task};
use async_trait::async_trait;
use log::{error, info, warn};
use std::sync::Arc;

type Pool = deadpool::managed::Pool<Connection, RaftError>;

type RecycleResult = deadpool::managed::RecycleResult<RaftError>;

struct Connection {
    stream: TcpStream,
    len: [u8; 4],
    buf: Vec<u8>,
}

impl Connection {
    async fn write_body(&mut self, raft_id: &[u8; 8], body: &Vec<u8>) -> std::io::Result<()> {
        self.stream.write(raft_id).await?;
        self.stream
            .write(&u32::to_be_bytes(body.len() as u32))
            .await?;
        self.stream.write(body).await?;
        Ok(())
    }

    async fn read_result(&mut self) -> std::io::Result<RaftError> {
        self.stream.read_exact(&mut self.len).await?;
        let len = u32::from_be_bytes(self.len) as usize;

        if len > 256 {
            let mut buf: Vec<u8> = Vec::with_capacity(len);
            buf.resize_with(len as usize, Default::default);
            self.stream.read_exact(&mut buf).await?;
            Ok(RaftError::decode(&buf))
        } else {
            self.buf.resize_with(len as usize, Default::default);
            self.stream.read_exact(&mut self.buf).await?;
            Ok(RaftError::decode(&self.buf))
        }
    }

    async fn close(&mut self) -> RaftResult<()> {
        Ok(())
    }
}

struct Manager {
    typ: u8,
    node_id: u64,
    raft: Arc<Raft>,
}

impl Manager {
    pub fn new(typ: u8, node_id: u64, raft: Arc<Raft>) -> Manager {
        Manager {
            typ: typ,
            node_id: node_id,
            raft: raft,
        }
    }
}

#[async_trait]
impl deadpool::managed::Manager<Connection, RaftError> for Manager {
    async fn create(&self) -> Result<Connection, RaftError> {
        let addr = if self.typ == entry_type::HEARTBEAT {
            self.raft.resolver.heartbeat_addr(&self.node_id)?
        } else {
            self.raft.resolver.log_addr(&self.node_id)?
        };

        info!("create conn by by addr:{}", addr);

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RaftError::NetError(e.to_string()))?;
        Ok(Connection {
            len: [0; 4],
            buf: Vec::with_capacity(256),
            stream: stream,
        })
    }

    async fn recycle(&self, conn: &mut Connection) -> RecycleResult {
        match conn.close().await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum PeerStatus {
    Synchronizing,
    Appending,
    Stoped,
}

pub struct Peer {
    node_id: u64,
    raft_id: [u8; 8],
    raft: Arc<Raft>,
    heart_pool: Pool,
    log_pool: Pool,
    status: RwLock<PeerStatus>,
}

impl Peer {
    async fn pool_log(&self) -> RaftResult<deadpool::managed::Object<Connection, RaftError>> {
        let mut times = 10;
        loop {
            match self.log_pool.get().await {
                Ok(c) => break Ok(c),
                Err(e) => {
                    warn!("conn to:{} has err:{}", self.node_id, e);
                    times -= 1;
                    if times <= 0 {
                        return Err(RaftError::Error(e.to_string()));
                    }
                    task::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
    async fn pool_heartbeat(&self) -> RaftResult<deadpool::managed::Object<Connection, RaftError>> {
        let mut times = 10;
        loop {
            match self.heart_pool.get().await {
                Ok(c) => break Ok(c),
                Err(e) => {
                    warn!("conn has err:{}", e);
                    times -= 1;
                    if times <= 0 {
                        return Err(RaftError::Error(e.to_string()));
                    }
                    task::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn send(&self, body: Arc<Vec<u8>>) -> RaftResult<RaftError> {
        let mut conn = match body[0] {
            entry_type::HEARTBEAT | entry_type::VOTE => self.pool_heartbeat().await?,
            entry_type::COMMIT => match *self.status.read().await {
                PeerStatus::Synchronizing => self.pool_log().await?,
                _ => return Err(RaftError::NotReady),
            },
            _ => self.pool_log().await?,
        };

        // write req
        conn.write_body(&self.raft_id, &*body)
            .await
            .map_err(|e| RaftError::NetError(e.to_string()))?;

        //get resp
        let e = conn
            .read_result()
            .await
            .map_err(|e| RaftError::NetError(e.to_string()))?;

        Ok(e)
    }

    pub async fn appending(self: &Arc<Peer>, index: u64) {
        warn!(
            "to start appending from index:{} for node_id:{}",
            index, self.node_id
        );

        {
            let mut status = self.status.write().await;
            if *status != PeerStatus::Synchronizing {
                warn!("start appending fail , peer status is :{:?}", *status);
                return;
            }
            *status = PeerStatus::Appending;
        }
        let mut conn = self.log_pool.get().await.unwrap();
        let conn = &mut conn;

        let iter = match self.raft.store.iter(index + 1).await {
            Err(e) => {
                error!("appending create iter has err:{}", e);
                None
            }
            Ok(iter) => Some(iter),
        };
        if let Some(mut iter) = iter {
            loop {
                match iter.next(&self.raft.store).await {
                    Err(e) => {
                        error!("appending next has err:{}", e);
                        break;
                    }
                    Ok(v) => match v {
                        Some(body) => {
                            if let Err(e) = conn.write_body(&self.raft_id, &body).await {
                                error!("appending write has err:{}", e);
                                break;
                            };

                            match conn.read_result().await {
                                Err(e) => {
                                    error!("appending read has err:{}", e);
                                    break;
                                }
                                Ok(re) => {
                                    if re != RaftError::Success {
                                        error!("send data has err:{}", re);
                                        break;
                                    }
                                }
                            }
                        }
                        None => break,
                    },
                }
            }
        };
        *self.status.write().await = PeerStatus::Synchronizing;
        warn!(
            "end appending from index:{} for node_id:{}",
            self.raft.store.last_index().await,
            self.node_id
        );
    }
}

pub struct Sender {
    peers: RwLock<Vec<Arc<Peer>>>,
}

impl Sender {
    pub fn new() -> Self {
        Self {
            peers: RwLock::new(Vec::new()),
        }
    }

    pub async fn forward(&self, body: Vec<u8>, leader: u64) -> RaftResult<Vec<u8>> {
        let mut peer: Option<Arc<Peer>> = None;
        for p in &*self.peers.read().await {
            if p.node_id == leader {
                peer = Some(p.clone());
            }
        }

        if peer.is_none() {
            return Err(RaftError::NotfoundAddr(leader));
        }

        let e = peer.unwrap().send(Arc::new(body)).await?;

        match &e {
            RaftError::Success => Ok(Vec::new()),
            RaftError::SuccessRaw(_) => match e {
                RaftError::SuccessRaw(d) => Ok(d),
                _ => panic!("impossibility"),
            },
            _ => Err(e),
        }
    }

    pub async fn send(&self, body: Vec<u8>) -> RaftResult<()> {
        let peers = self.peers.read().await;
        if peers.len() == 0 {
            return Ok(());
        }

        let body = Arc::new(body);

        let (tx, rx) = channel(peers.len());
        for p in &*peers {
            let peer = p.clone();
            let body = body.clone();
            let tx = tx.clone();
            task::spawn(async move {
                match peer.send(body).await {
                    Ok(e) => {
                        if &e != &RaftError::Success {
                            error!("recive from:{} err :{}", peer.node_id, e);
                        };

                        match &e {
                            RaftError::IndexLess(i, t) => {
                                if i > t {
                                    tx.send(e).await;
                                    return;
                                } else {
                                    if *peer.status.read().await == PeerStatus::Synchronizing {
                                        let index = *i;
                                        let peer = peer.clone();
                                        task::spawn(async move {
                                            peer.appending(index).await;
                                        });
                                    }
                                }
                            }
                            _ => {}
                        };
                        tx.send(e).await;
                    }
                    Err(e) => {
                        if RaftError::NotReady != e {
                            error!("send log to node:{} has err:{:?}", peer.node_id, e);
                        }
                        tx.send(e).await;
                    }
                };
            });
        }

        let half = peers.len() / 2;

        let mut ok = 1;
        let mut err = 0;

        drop(tx);

        while let Ok(e) = rx.recv().await {
            if e == RaftError::Success {
                ok += 1;
                if ok > half {
                    return Ok(());
                }
            } else {
                err += 1;
                if err > half {
                    return Err(RaftError::NotEnoughRecipient(
                        half as u16,
                        (peers.len() - err) as u16,
                    ));
                }
            }
        }

        return Err(RaftError::NotEnoughRecipient(
            half as u16,
            (err + ok) as u16,
        ));
    }

    pub async fn add_peer(&self, node_id: u64, raft: Arc<Raft>) {
        let heart_pool = Pool::new(
            Manager::new(entry_type::HEARTBEAT, node_id, raft.clone()),
            1,
        );
        let log_pool = Pool::new(Manager::new(entry_type::COMMIT, node_id, raft.clone()), 5);

        let peer = Arc::new(Peer {
            node_id,
            raft_id: u64::to_be_bytes(raft.id),
            raft: raft.clone(),
            heart_pool,
            log_pool,
            status: RwLock::new(PeerStatus::Synchronizing),
        });

        self.peers.write().await.push(peer);
    }

    pub async fn remove_peer(&self, node_id: u64) {
        let peers = &mut *self.peers.write().await;
        for p in &*peers {
            if p.node_id == node_id {
                *p.status.write().await = PeerStatus::Stoped;
            }
        }
        peers.retain(|p| if node_id == p.node_id { false } else { true });
    }
}
