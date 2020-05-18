use crate::{entity::*, error::*, raft::Raft};
use log::{error, info};
use smol::{Async, Task};
use std::borrow::Cow;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

pub(crate) struct Server {
    config: Arc<Config>,
    rafts: RwLock<HashMap<u64, Raft>>,
    resolver: Box<dyn Resolver>,
}

impl Server {
    pub fn new(conf: Config) -> Self {
        Server {
            config: Arc::new(conf),
            rafts: RwLock::new(HashMap::new()),
            resolver: Box::new(DefResolver::new()),
        }
    }

    pub fn start(&self) -> RaftResult<()> {
        let mut threads = Vec::new();
        threads.push(_start(self.config.replicate_port));
        for t in threads {
            t.join().unwrap()?;
        }
        return Ok(());
    }

    pub fn stop(&self) -> RaftResult<()> {
        panic!()
    }

    pub fn create_raft(&self, id: u64) -> RaftResult<Arc<Raft>> {
        panic!()
    }

    pub fn remove_raft(&self, id: u64) -> RaftResult<()> {
        panic!()
    }

    pub fn get_raft(&self, id: u64) -> RaftResult<Arc<Raft>> {
        panic!()
    }
}

pub fn _start(port: u16) -> std::thread::JoinHandle<RaftResult<()>> {
    std::thread::spawn(move || {
        smol::run(async {
            let listener = match Async::<TcpListener>::bind(format!("0.0.0.0:{}", port)) {
                Ok(l) => l,
                Err(e) => return Err(RaftError::NetError(e.to_string())),
            };
            info!("start transport on server 0.0.0.0:{}", port);
            loop {
                match listener.accept().await {
                    Ok((_, _)) => {
                        println!("Accepted client: ");
                    }
                    Err(e) => error!("listener has err:{}", e.to_string()),
                }
            }
            info!("stop transport on server 0.0.0.0:{}", port);
        })
    })
}

#[test]
fn test_new() {
    let server = Server::new(Config {
        node_id: 1,
        heartbeat_port: 5110,
        replicate_port: 5111,
    });

    if let Err(e) = server.start() {
        println!("{:?}", e.to_string());
        panic!(e);
    };
}
