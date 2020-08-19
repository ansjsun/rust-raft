use log::{debug, info};
use raft4rs::server::Server;
use std::sync::Arc;
mod config;
use config::*;

#[async_std::main]
async fn main() {
    init_log();
    debug!("start............");

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
    while !raft1.is_leader().await {
        if let Err(e) = raft1.try_to_leader().await {
            info!("raft1 try to leader has err:{:?}", e);
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
        info!("wait raft1 to leader times");
    }

    for i in 0..100u32 {
        let v = i % 3;
        let raft = match v {
            0 => raft1.clone(),
            1 => raft2.clone(),
            2 => raft3.clone(),
            _ => panic!("no "),
        };
        if let Err(e) = raft
            .submit(
                unsafe { format!("commit: {}", i + 1).as_mut_vec().clone() },
                true,
            )
            .await
        {
            println!("{:?}", e);
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    println!("raft1:{:?}", raft1.info().await);
    println!("raft2:{:?}", raft2.info().await);
    println!("raft3:{:?}", raft3.info().await);

    std::thread::sleep(std::time::Duration::from_secs(10000));
}
