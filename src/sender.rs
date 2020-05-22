use crate::entity::*;
use crate::error::*;
use crate::state_machine::Resolver;
use futures::io;
use futures::prelude::*;
use smol::Async;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};

pub struct Sender {
    resolver: Arc<Resolver + Sync + Send>,
}

impl Sender {
    pub async fn send_log(node_id: &u64, raft_id: &u64, entry: &Vec<u8>) -> RaftResult<()> {
        panic!()
        // let mut stream =
        //     conver(Async::<TcpStream>::connect(self.resolver.log_addr(node_id)?).await)?;
        // if let Err(e) = stream.write_all(body).await {
        //     return Err(RaftError::NetError(e.to_string()));
        // };

        // if let Err(e) = stream.read_to_end(resp).await {
        //     return Err(RaftError::NetError(e.to_string()));
        // };
        // return Ok(());
    }
}
