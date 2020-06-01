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
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender as ChannelSender};
use tokio::sync::Notify;

pub struct Peer {
    node_id: u64,
    log_notify: Notify,
    heartbeat_notify: Arc<Notify>,
}

pub struct Sender {
    //receiver<type, index, info>
    rx: Mutex<Receiver<(u8, u64, RaftError)>>,
    tx: ChannelSender<(u8, u64, RaftError)>,
    peers: Vec<Arc<Peer>>,
    last_buf: Arc<RwLock<(u8, u64, Arc<Vec<u8>>)>>,
}

impl Sender {
    pub fn new() -> Self {
        let (tx, rx) = channel(256);
        Self {
            peers: Vec::new(),
            rx: Mutex::new(rx),
            tx: tx,
            last_buf: Arc::new(RwLock::new((0, 0, Arc::new(Vec::default())))),
        }
    }

    pub fn send_heartbeat(&self, body: Vec<u8>) -> RaftResult<()> {
        for p in &self.peers {
            p.heartbeat_notify.notify()
        }
        Ok(())
    }

    pub fn send_log(&self, index: u64, body: Vec<u8>) -> RaftResult<()> {
        let typ = body[0];
        {
            let mut v = self.last_buf.write().unwrap();
            v.0 = typ;
            v.1 = index;
            v.2 = Arc::new(body);
        }

        for p in &self.peers {
            p.log_notify.notify();
        }

        let half = self.peers.len() / 2 + self.peers.len() % 2;

        smol::block_on(async {
            let mut recive = self.rx.lock().unwrap();
            let mut ok = 0;
            let mut err = 0;
            while let Some((t, i, e)) = recive.recv().await {
                if t != typ || i != index {
                    continue;
                }

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
                            (self.peers.len() - err) as u16,
                        ));
                    }
                }
            }
            return Err(RaftError::Timeout(0)); //TODO full time
        })
    }

    pub fn run_peer(&mut self, node_id: u64, raft: Arc<Raft>) {
        let notify = Arc::new(Notify::new());

        let peer = Arc::new(Peer {
            node_id: node_id,
            log_notify: notify.clone(),
        });

        let peer_job = peer.clone();

        let tx = self.tx.clone();
        let last_buf = self.last_buf.clone();
        std::thread::spawn(|| {
            smol::run(async move {
                while !raft.is_stoped() {
                    if !raft.is_leader() {
                        peer_job.log_notify.notified().await;
                    }

                    let job_tx = tx.clone();
                    let buf = last_buf.clone();

                    if let Err(e) =
                        Self::peer_job(raft.clone(), peer_job.clone(), job_tx, buf).await
                    {
                        error!("peer job has err :{}", e);
                    }
                }
            })
        });
        self.peers.push(peer);
    }

    async fn peer_job(
        raft: Arc<Raft>,
        peer: Arc<Peer>,
        mut tx: ChannelSender<(u8, u64, RaftError)>,
        buf: Arc<RwLock<(u8, u64, Arc<Vec<u8>>)>>,
    ) -> RaftResult<()> {
        let addr = raft.resolver.log_addr(&peer.node_id)?;
        let mut stream = timeout(500, Async::<TcpStream>::connect(addr)).await?;
        let last_info = || -> (u8, u64, Arc<Vec<u8>>) {
            let v = buf.read().unwrap();
            (v.0, v.1, v.2.clone())
        };

        let is_current = |t: u8, i: u64| {
            let v = buf.read().unwrap();
            v.0 == t && v.1 == i
        };

        let mut resp = Vec::default();
        let mut pre_value = (255, 0);
        loop {
            let (t, index, body) = last_info();
            if (t, index) == pre_value {
                peer.notify.notified().await;
                continue;
            }

            pre_value = (t, index);

            conver(stream.write(&u64::to_be_bytes(raft.id)).await)?;
            conver(stream.write(&u32::to_be_bytes(body.len() as u32)).await)?;
            conver(stream.write(&body).await)?;
            resp.clear();
            conver(stream.read_to_end(&mut resp).await)?;

            let re = RaftError::decode(&resp);

            match &re {
                RaftError::Success => {
                    if is_current(t, index) {
                        conver(tx.send((t, index, re)).await)?;
                    }
                }
                RaftError::IndexLess(i) => {
                    error!("need index:{} less than push:{}", i, index);
                    if is_current(t, index) {
                        conver(tx.send((t, index, RaftError::IndexLess(*i))).await)?;
                    }

                    let raft_id = raft.id;
                    raft.store
                        .iter(&mut stream, i + 1, |stm, mut body| -> RaftResult<bool> {
                            smol::block_on(async move {
                                conver(stm.write(&u64::to_be_bytes(raft_id)).await)?;
                                conver(stm.write(&u32::to_be_bytes(body.len() as u32)).await)?;
                                conver(stm.write(&body).await)?;
                                body.clear();
                                conver(stm.read_to_end(&mut body).await)?;
                                let re = RaftError::decode(&body);
                                if re != RaftError::Success {
                                    error!("send data has err:{}", re);
                                    Err(re)
                                } else {
                                    Ok(true)
                                }
                            })
                        })?;
                }
                RaftError::TermLess => {
                    error!("send commit term less");
                    if is_current(t, index) {
                        conver(tx.send((t, index, RaftError::TermLess)).await)?;
                    }
                    break;
                }
                _ => {
                    error!(" result  has err:{}", re);
                    conver(tx.send((t, index, re)).await)?;
                }
            }
        }

        Ok(())
    }
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
