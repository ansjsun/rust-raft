use crate::entity::{entry_type, *};
use crate::error::*;
use crate::raft::Raft;
use futures::future::Either;
use futures::prelude::*;
use log::error;
pub use log::{debug, log_enabled, Level::Debug};
use smol::Async;
use smol::Timer;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc::channel;

struct Connection {
    raft_id: [u8; 8],
    stream: Async<TcpStream>,
    len: [u8; 4],
    buf: Vec<u8>,
}

impl Connection {
    async fn send(&mut self, body: Arc<Vec<u8>>) -> std::io::Result<RaftError> {
        self.stream.write(&self.raft_id).await?;
        self.stream
            .write(&u32::to_be_bytes(body.len() as u32))
            .await?;
        self.stream.write(&body).await?;

        self.stream.read_exact(&mut self.len).await?;
        let len = u32::from_be_bytes(self.len);

        if len > 256 {
            let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
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

impl r2d2::ManageConnection for Manager {
    type Connection = Connection;
    type Error = RaftError;

    fn connect(&self) -> Result<Connection, RaftError> {
        let addr = if self.typ == entry_type::HEARTBEAT {
            self.raft.resolver.heartbeat_addr(&self.node_id)?
        } else {
            self.raft.resolver.log_addr(&self.node_id)?
        };

        let raft_id = u64::to_be_bytes(self.node_id);

        smol::run(async move {
            Ok(Connection {
                raft_id: raft_id,
                len: [0; 4],
                buf: Vec::with_capacity(256),
                stream: timeout(500, Async::<TcpStream>::connect(addr)).await?,
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

pub struct Peer {
    heart_pool: r2d2::Pool<Manager>,
    log_pool: r2d2::Pool<Manager>,
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
            let pool = p.heart_pool.clone();
            let body = body.clone();
            smol::Task::spawn(async move {
                let mut conn = pool.get().unwrap();
                if let Err(e) = conn.send(body).await {
                    error!("send heartbeat has err:{}", e);
                }
            })
            .detach();
        }
    }

    pub async fn send_log(&self, body: Vec<u8>) -> RaftResult<()> {
        let body = Arc::new(body);
        let peers = self.peers.read().unwrap();
        let (tx, mut rx) = channel(peers.len());
        for p in &*peers {
            let pool = p.log_pool.clone();
            let body = body.clone();
            let mut tx = tx.clone();
            smol::Task::spawn(async move {
                let mut conn = pool.get().unwrap();
                let _ = match conn.send(body).await {
                    Ok(e) => tx.try_send(e),
                    Err(e) => tx.try_send(RaftError::NetError(e.to_string())),
                };
            })
            .detach();
        }

        let half = peers.len() / 2 + peers.len() % 2;

        let mut ok = 0;
        let mut err = 0;
        while let Some(e) = rx.recv().await {
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
        return Err(RaftError::Timeout(0)); //TODO full time
    }

    pub fn add_peer(&self, node_id: u64, raft: Arc<Raft>) {
        let heart_pool = r2d2::Pool::builder()
            .max_size(1)
            .min_idle(Some(0))
            .build(Manager::new(entry_type::HEARTBEAT, node_id, raft.clone()))
            .unwrap();
        let log_pool = r2d2::Pool::builder()
            .max_size(5)
            .min_idle(Some(0))
            .build(Manager::new(entry_type::COMMIT, node_id, raft.clone()))
            .unwrap();

        let peer = Arc::new(Peer {
            heart_pool: heart_pool,
            log_pool: log_pool,
        });

        self.peers.write().unwrap().push(peer);
    }

    //     async fn peer_job(
    //         raft: Arc<Raft>,
    //         peer: Arc<Peer>,
    //         mut tx: ChannelSender<(u8, u64, RaftError)>,
    //         buf: Arc<RwLock<(u8, u64, Arc<Vec<u8>>)>>,
    //     ) -> RaftResult<()> {
    //         let addr = raft.resolver.log_addr(&peer.node_id)?;
    //         let mut stream = timeout(500, Async::<TcpStream>::connect(addr)).await?;
    //         let last_info = || -> (u8, u64, Arc<Vec<u8>>) {
    //             let v = buf.read().unwrap();
    //             (v.0, v.1, v.2.clone())
    //         };

    //         let is_current = |t: u8, i: u64| {
    //             let v = buf.read().unwrap();
    //             v.0 == t && v.1 == i
    //         };

    //         let mut resp = Vec::default();
    //         let mut pre_value = (255, 0);
    //         loop {
    //             let (t, index, body) = last_info();
    //             if (t, index) == pre_value {
    //                 peer.notify.notified().await;
    //                 continue;
    //             }

    //             pre_value = (t, index);

    //             conver(stream.write(&u64::to_be_bytes(raft.id)).await)?;
    //             conver(stream.write(&u32::to_be_bytes(body.len() as u32)).await)?;
    //             conver(stream.write(&body).await)?;
    //             resp.clear();
    //             conver(stream.read_to_end(&mut resp).await)?;

    //             let re = RaftError::decode(&resp);

    //             match &re {
    //                 RaftError::Success => {
    //                     if is_current(t, index) {
    //                         conver(tx.send((t, index, re)).await)?;
    //                     }
    //                 }
    //                 RaftError::IndexLess(i) => {
    //                     error!("need index:{} less than push:{}", i, index);
    //                     if is_current(t, index) {
    //                         conver(tx.send((t, index, RaftError::IndexLess(*i))).await)?;
    //                     }

    //                     let raft_id = raft.id;
    //                     raft.store
    //                         .iter(&mut stream, i + 1, |stm, mut body| -> RaftResult<bool> {
    //                             smol::block_on(async move {
    //                                 conver(stm.write(&u64::to_be_bytes(raft_id)).await)?;
    //                                 conver(stm.write(&u32::to_be_bytes(body.len() as u32)).await)?;
    //                                 conver(stm.write(&body).await)?;
    //                                 body.clear();
    //                                 conver(stm.read_to_end(&mut body).await)?;
    //                                 let re = RaftError::decode(&body);
    //                                 if re != RaftError::Success {
    //                                     error!("send data has err:{}", re);
    //                                     Err(re)
    //                                 } else {
    //                                     Ok(true)
    //                                 }
    //                             })
    //                         })?;
    //                 }
    //                 RaftError::TermLess => {
    //                     error!("send commit term less");
    //                     if is_current(t, index) {
    //                         conver(tx.send((t, index, RaftError::TermLess)).await)?;
    //                     }
    //                     break;
    //                 }
    //                 _ => {
    //                     error!(" result  has err:{}", re);
    //                     conver(tx.send((t, index, re)).await)?;
    //                 }
    //             }
    //         }

    //         Ok(())
    //     }
}

async fn timeout<T: std::fmt::Debug>(
    millis: u64,
    f: impl Future<Output = futures::io::Result<T>>,
) -> RaftResult<T> {
    futures::pin_mut!(f);

    let dur = Duration::from_millis(millis);
    match future::select(f, Timer::after(dur)).await {
        Either::Left((out, _)) => match out {
            Ok(t) => Ok(t),
            Err(e) => Err(RaftError::NetError(e.to_string())),
        },
        Either::Right(_) => Err(RaftError::Timeout(millis)),
    }
}

pub fn send(raft: &Arc<Raft>, entry: &Entry) -> RaftResult<()> {
    let len = raft.replicas.len();
    if len == 0 {
        return Ok(());
    }

    let data = Arc::new(entry.encode());
    let len = raft.replicas.len();

    let (tx, mut rx) = channel(len);
    smol::run(async {
        for i in 0..len {
            let entry = data.clone();
            let raft = raft.clone();
            let node_id = raft.replicas[i];
            let mut tx = tx.clone();
            smol::Task::spawn(async move {
                let result = execute(node_id, raft, entry).await;
                let _ = tx.try_send(result);
            })
            .detach();
        }
        drop(tx);

        let mut need = len / 2;

        while let Some(res) = rx.recv().await {
            match res {
                Err(e) => error!("submit log has err:[{:?}] entry:[{:?}]", e, entry),
                Ok(_) => {
                    need -= 1;
                    if need == 0 {
                        return Ok(());
                    }
                }
            }
        }

        Err(RaftError::NotEnoughRecipient(
            len as u16 / 2,
            (len / 2 - need) as u16,
        ))
    })
}

async fn execute(node_id: u64, raft: Arc<Raft>, body: Arc<Vec<u8>>) -> RaftResult<()> {
    let addr = if body[0] == entry_type::HEARTBEAT {
        raft.resolver.heartbeat_addr(&node_id)?
    } else {
        raft.resolver.log_addr(&node_id)?
    };

    let mut stream = conver(Async::<TcpStream>::connect(addr).await)?;

    conver(stream.write(&u64::to_be_bytes(raft.id)).await)?;
    conver(stream.write(&u32::to_be_bytes(body.len() as u32)).await)?;
    conver(stream.write(&body).await)?;

    let mut resp = Vec::default();
    if let Err(e) = stream.read_to_end(&mut resp).await {
        return Err(RaftError::NetError(e.to_string()));
    };

    if log_enabled!(Debug) {
        let e = Entry::decode((*body).clone()).unwrap();
        match &e {
            Entry::Heartbeat { .. } => {}
            _ => {
                let e = RaftError::decode(&resp);
                if RaftError::Success != e {
                    debug!(
                        "node_id:{} send:{:?} got :{:?}",
                        node_id,
                        Entry::decode((*body).clone()),
                        e,
                    );
                }
            }
        }
    }

    let re = RaftError::decode(&resp);

    if let RaftError::Success = &re {
        return Ok(());
    }

    return Err(re);
}

#[test]
fn test_time_out() {
    // let v: RaftResult<()> = smol::block_on(async {
    //     timeout(500, async {
    //         std::thread::sleep(Duration::from_millis(200));
    //         Ok(())
    //     })
    //     .await
    // });
    // assert!(v.is_ok());

    std::thread::spawn(|| {
        smol::run(async {
            println!("1231231................................");
            Timer::after(Duration::from_secs(1)).await;
            println!("1231231");
        })
    });

    std::thread::sleep(Duration::from_secs(12))

    // assert!(v.is_err());
}
