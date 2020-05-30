use crate::entity::Entry;
use crate::entity::*;
use crate::error::*;
use crate::raft::Raft;
use futures::prelude::*;
pub use log::Level::Debug;
use log::{debug, error, log_enabled};
use smol::Async;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{channel, Receiver, Sender as ChannelSender};
use tokio::sync::watch;
use tokio::sync::Notify;

pub struct Sender {
    peers: RwLock<HashMap<u64, Arc<Notify>>>,
}

impl Sender {
    pub fn new(raft: &Arc<Raft>, mut tx: ChannelSender<RaftError>) -> Self {
        let peers = RwLock::new(HashMap::new());
        for node_id in &raft.replicas {
            let notify = Self::run_peer(*node_id, raft.clone(), tx.clone());
            peers.write().unwrap().insert(*node_id, notify);
        }

        Self { peers: peers }
    }

    fn run_peer(node_id: u64, raft: Arc<Raft>, mut tx: ChannelSender<RaftError>) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());

        let self_notify = notify.clone();
        smol::Task::blocking(async move {
            while !raft.is_stoped() {
                if !raft.is_leader() {
                    self_notify.notified().await;
                }

                if let Err(e) =
                    Self::peer_job(node_id, raft.clone(), self_notify.clone(), tx.clone()).await
                {
                    //do some thing .....
                    error!("peer job has err :{}", e);
                }
            }
        })
        .detach();

        notify
    }

    async fn peer_job(
        node_id: u64,
        raft: Arc<Raft>,
        self_notify: Arc<Notify>,
        mut tx: ChannelSender<RaftError>,
    ) -> RaftResult<()> {
        let mut stream =
            conver(Async::<TcpStream>::connect(raft.resolver.log_addr(&node_id)?).await)?;
        let mut resp = Vec::default();
        let mut index = raft.store.last_index();
        let mut pre_index = 0;
        loop {
            let mut need_num = raft.replicas.len() / 2;

            if raft.store.last_index() == pre_index {
                self_notify.notified().await;
            }

            pre_index = index;
            need_num = raft.replicas.len() / 2;
            match raft.store.get_entry(index) {
                Ok(body) => {
                    conver(stream.write(&u64::to_be_bytes(raft.id)).await)?;
                    conver(stream.write(&u32::to_be_bytes(body.len() as u32)).await)?;
                    conver(stream.write(&body).await)?;
                    conver(stream.read_to_end(&mut resp).await)?;

                    let re = RaftError::decode(&resp);
                    match &re {
                        RaftError::Success => {
                            if index == raft.store.last_index() {
                                conver(tx.send(re).await)?;
                            }
                        }
                        RaftError::IndexLess(i) => {
                            error!("need index:{} less than push:{}", i, index);
                            conver(tx.send(RaftError::IndexLess(*i)).await)?;
                            index = i + 1;
                        }
                        _ => {
                            error!(" result  has err:{}", re);
                            conver(tx.send(re).await)?;
                        }
                    }
                }
                Err(_e) => panic!("impl me need use snapshot????"),
            }
        }

        Ok(())
    }
}

impl Sender {
    async fn write_entry(
        stream: &mut Async<TcpStream>,
        raft_id: u64,
        body: Vec<u8>,
    ) -> std::io::Result<()> {
        stream.write(&u64::to_be_bytes(raft_id)).await?;
        stream.write(&u32::to_be_bytes(body.len() as u32)).await?;
        stream.write(&body).await?;
        Ok(())
    }
}

pub fn send(raft: Arc<Raft>, entry: &Entry) -> RaftResult<()> {
    let len = raft.replicas.len();
    if len == 0 {
        return Ok(());
    }

    let data = Arc::new(entry.encode());
    let len = raft.replicas.len();

    let (tx, mut rx) = mpsc::channel(len);
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
