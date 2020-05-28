use crate::entity::Entry;
use crate::entity::*;
use crate::error::*;
use crate::raft::Raft;
use futures::prelude::*;
use log::error;
use smol::Async;
use std::net::TcpStream;
use std::sync::Arc;
use tokio::sync::mpsc;

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
                let _ = tx.try_send(execute(node_id, raft, entry).await);
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
    let re = RaftError::decode(resp);
    if let RaftError::Success = &re {
        return Ok(());
    }

    return Err(re);
}
