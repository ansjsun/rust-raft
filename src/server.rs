use crate::{entity::*, error::*, raft::Raft};
use smol::{Async, Task};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

pub struct Server {
    config: ServerConfig,
    rafts: RwLock<HashMap<u64, Raft>>,
}

impl Server {
    pub fn new(conf: ServerConfig) -> Self {
        Server {
            config: conf,
            rafts: RwLock::new(HashMap::new()),
        }
    }

    pub fn start(&self) -> RaftResult<()> {
        smol::run(async {
            let listener = match Async::<TcpListener>::bind("127.0.0.1:7000") {
                Ok(l) => l,
                Err(e) => return Err(RaftError::NetError(e.to_string())),
            };
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        println!("Accepted client: ");
                    }
                    Err(e) => return Err(RaftError::NetError(e.to_string())),
                }
            }
        })
    }
    pub fn stop(&self) -> RaftResult<()> {
        panic!()
    }

    pub fn create_raft(&self, config: RaftConfig) -> RaftResult<Arc<Raft>> {
        panic!()
    }

    pub fn remove_raft(&self, id: u64) -> RaftResult<()> {
        panic!()
    }

    pub fn get_raft(&self, id: u64) -> RaftResult<Arc<Raft>> {
        panic!()
    }
}

#[test]
fn test_new() {
    let _ = Server::new(Config {});
}
