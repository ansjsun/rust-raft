mod config;
use config::*;
use log::info;
use raft4rs::server::Server;
use std::sync::Arc;

#[async_std::main]
async fn main() {
    init_log();

    let server1 = Arc::new(Server::new(make_config(1), make_resolver())).start();
    let server2 = Arc::new(Server::new(make_config(2), make_resolver())).start();
    let server3 = Arc::new(Server::new(make_config(3), make_resolver())).start();

    let replicas = &vec![1, 2, 3];

    let raft1 = server1
        .create_raft(1, 0, 1, replicas, MySM { id: 1 })
        .await
        .unwrap();
    let raft2 = server2
        .create_raft(1, 0, 1, replicas, MySM { id: 2 })
        .await
        .unwrap();
    let raft3 = server3
        .create_raft(1, 0, 1, replicas, MySM { id: 3 })
        .await
        .unwrap();

    println!("raft1 index:{:?}", raft1.info().await);
    println!("raft2 index:{:?}", raft2.info().await);
    println!("raft3 index:{:?}", raft3.info().await);

    info!("begin validate log file");
    raft1.store.validate_log_file(1).await.unwrap();
    raft2.store.validate_log_file(1).await.unwrap();
    raft3.store.validate_log_file(1).await.unwrap();
}
