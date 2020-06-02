use crate::entity::{entry_type, *};
use crate::error::*;
use crate::raft::Raft;
use futures::future::Either;
use futures::prelude::*;
use log::{debug, error, log_enabled, warn, Level::Debug};
use smol::Async;
use smol::Timer;
use std::net::TcpStream;
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::time::Duration;

type Pool = deadpool::managed::Pool<Connection, RaftError>;

struct Connection {
    stream: Async<TcpStream>,
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
impl deadpool::managed::Manager<Computer, Error> for Manager {
    type Connection = Connection;
    type Error = RaftError;

    fn connect(&self) -> Result<Connection, RaftError> {
        let addr = if self.typ == entry_type::HEARTBEAT {
            self.raft.resolver.heartbeat_addr(&self.node_id)?
        } else {
            self.raft.resolver.log_addr(&self.node_id)?
        };

        smol::run(async move {
            println!("to create ...by addr{}", addr);
            Ok(Connection {
                len: [0; 4],
                buf: Vec::with_capacity(256),
                stream: Async::<TcpStream>::connect(addr)
                    .await
                    .map_err(|e| RaftError::NetError(e.to_string()))?,
            })
        })
    }

    fn is_valid(&self, _conn: &mut Connection) -> Result<(), RaftError> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Connection) -> bool {
        true
    }
}

#[derive(Debug, PartialEq)]
pub enum PeerStatus {
    Synchronizing,
    Appending,
}

pub struct Peer {
    node_id: u64,
    raft_id: [u8; 8],
    raft: Arc<Raft>,
    heart_pool: r2d2::Pool<Manager>,
    log_pool: r2d2::Pool<Manager>,
    status: RwLock<PeerStatus>,
}

impl Peer {
    async fn send(&self, body: Arc<Vec<u8>>) -> RaftResult<RaftError> {
        let mut conn = if body[0] == entry_type::HEARTBEAT {
            self.heart_pool.get().unwrap()
        } else if body[0] == entry_type::COMMIT {
            match *self.status.read().unwrap() {
                PeerStatus::Synchronizing => self.log_pool.get().unwrap(),
                _ => return Err(RaftError::NotReady),
            }
        } else {
            let mut err_times = 0;
            loop {
                match self.log_pool.get() {
                    Ok(conn) => break conn,
                    Err(e) => {
                        warn!("take connection has err:{}", e);
                        if err_times > 10 {
                            return Err(RaftError::NetError(e.to_string()));
                        }
                    }
                }
                err_times += 1;
            }
        };
        //write req
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

    async fn appending(&self, index: u64) {
        warn!(
            "to start appending from index:{} for node_id:{}",
            index, self.node_id
        );

        {
            let mut status = self.status.write().unwrap();
            if *status != PeerStatus::Synchronizing {
                warn!("start appending fail , peer status is :{:?}", *status);
                return;
            }
            *status = PeerStatus::Appending;
        }

        let mut conn = self.log_pool.get().unwrap();
        let raft_id = &self.raft_id;
        let conn = &mut conn;
        if let Err(e) = self.raft.store.iter(index + 1, |body| -> RaftResult<bool> {
            smol::block_on(async {
                conn.write_body(&self.raft_id, &body)
                    .await
                    .map_err(|e| RaftError::NetError(e.to_string()))?;
                let re = conn
                    .read_result()
                    .await
                    .map_err(|e| RaftError::NetError(e.to_string()))?;

                if re != RaftError::Success {
                    error!("send data has err:{}", re);
                    Err(re)
                } else {
                    Ok(true)
                }
            })
        }) {
            error!("appending has err:{}", e);
        };

        *self.status.write().unwrap() = PeerStatus::Appending;
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

    pub async fn send_heartbeat(&self, body: Vec<u8>) {
        let body = Arc::new(body);

        let peers = self.peers.read().unwrap();
        for p in &*peers {
            let body = body.clone();
            let peer = p.clone();
            smol::Task::spawn(async move {
                if let Err(e) = peer.send(body).await {
                    error!("send heartbeat has err:{}", e);
                }
            })
            .detach();
        }
    }

    pub async fn send_log(&self, body: Vec<u8>) -> RaftResult<()> {
        let body = Arc::new(body);
        let peers = self.peers.read().unwrap();
        let (mut tx, mut rx) = channel();

        for p in &*peers {
            let peer = p.clone();
            let body = body.clone();
            let mut tx = tx.clone();
            smol::Task::blocking(async move {
                if let Err(e) = match peer.send(body).await {
                    Ok(e) => tx.send(e),
                    Err(e) => {
                        error!("send log has err:{:?}", e);
                        tx.send(e)
                    }
                } {
                    debug!("send to channel has err:{:?}", e);
                };
            })
            .detach();
        }

        let half = peers.len() / 2 + peers.len() % 2;

        let mut ok = 0;
        let mut err = 0;
        while let Ok(e) = rx.recv_timeout(Duration::from_millis(5000)) {
            println!("recive ........{:?}", e);
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
        return Err(RaftError::Timeout(5000)); //TODO full time
    }

    pub fn add_peer(&self, node_id: u64, raft: Arc<Raft>) {
        let heart_pool = r2d2::Pool::builder()
            .connection_timeout(Duration::from_millis(500))
            .max_size(10)
            // .min_idle(Some(0))
            .build(Manager::new(entry_type::HEARTBEAT, node_id, raft.clone()))
            .unwrap();
        let log_pool = r2d2::Pool::builder()
            .connection_timeout(Duration::from_millis(500))
            .max_size(10)
            // .min_idle(Some(0))
            .build(Manager::new(entry_type::COMMIT, node_id, raft.clone()))
            .unwrap();

        let peer = Arc::new(Peer {
            node_id: node_id,
            raft_id: u64::to_be_bytes(raft.id),
            raft: raft.clone(),
            heart_pool: heart_pool,
            log_pool: log_pool,
            status: RwLock::new(PeerStatus::Synchronizing),
        });

        self.peers.write().unwrap().push(peer);
    }
}
