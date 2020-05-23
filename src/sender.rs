use crate::entity::*;
use crate::error::*;
use crate::raft::Raft;
use crate::state_machine::Resolver;
use futures::io;
use futures::prelude::*;
use smol::Async;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};

pub struct Sender {}

impl Sender {
    pub async fn send_log(node_id: u64, raft: Arc<Raft>, body: Arc<Vec<u8>>) -> RaftResult<()> {
        let mut stream =
            conver(Async::<TcpStream>::connect(raft.resolver.log_addr(&node_id)?).await)?;
        if let Err(e) = stream.write_all(&body).await {
            return Err(RaftError::NetError(e.to_string()));
        };

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
}
