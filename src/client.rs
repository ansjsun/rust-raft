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
    pub async fn send(&self, id: &u64, body: &[u8]) -> RaftResult<()> {
        let mut stream = conver(Async::<TcpStream>::connect(self.resolver.resolver(id)?).await)?;
        let result = Vec::default();
        let mut stdout = smol::writer(result);
        stream.write(body);
        // let mut stdout = smol::writer(body);
        panic!();
    }
}
