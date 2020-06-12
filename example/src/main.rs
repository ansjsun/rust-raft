use async_std::task;
use async_trait::async_trait;
use log::{debug, info};
use raft4rs::{entity::Config, error::*, server::Server, state_machine::*};
use std::sync::Arc;

#[async_std::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::default().write_style_or("auto", "always"));

    debug!("start............");

    let server1 = Arc::new(Server::new(make_config(1), make_resolver())).start();
    let server2 = Arc::new(Server::new(make_config(2), make_resolver())).start();
    let server3 = Arc::new(Server::new(make_config(3), make_resolver())).start();

    let replicas = &vec![1, 2, 3];

    let raft1 = server1
        .create_raft(1, 0, 1, replicas, SM { id: 1 })
        .await
        .unwrap();
    let raft2 = server2
        .create_raft(1, 0, 1, replicas, SM { id: 2 })
        .await
        .unwrap();
    let raft3 = server3
        .create_raft(1, 0, 1, replicas, SM { id: 3 })
        .await
        .unwrap();

    while !raft1.is_leader().await {
        if let Err(e) = raft1.try_to_leader().await {
            println!("raft1 try to leader has err:{:?}", e);
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
        info!("wait raft1 to leader times");
    }
    for i in 0..10000000u32 {
        if let Err(e) = raft1
            .submit(unsafe { format!("commit: {}", i + 1).as_mut_vec().clone() })
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

struct SM {
    id: usize,
}

#[async_trait]
impl StateMachine for SM {
    fn apply_log(&self, term: u64, index: u64, command: &[u8]) -> RaftResult<()> {
        if index % 10000 == 0 {
            println!(
                "apply {} term:{} index:{} command:{:?}",
                self.id,
                term,
                index,
                String::from_utf8_lossy(command)
            );
        }
        Ok(())
    }
    fn apply_member_change(
        &self,
        term: u64,
        index: u64,
        node_id: u64,
        action: u8,
        exists: bool,
    ) -> RaftResult<()> {
        println!(
            "apply_member_change {} type:{:?} index:{}",
            self.id, action, index
        );
        Ok(())
    }
    async fn apply_leader_change(&self, term: u64, index: u64, leader: u64) -> RaftResult<()> {
        println!(
            "apply_leader_change {} leader:{} term:{} index:{}",
            self.id, leader, term, index
        );
        Ok(())
    }
}

fn make_resolver() -> DefResolver {
    let mut def = DefResolver::new();
    def.add_node(1, String::from("127.0.0.1"), 10000, 10001);
    def.add_node(2, String::from("127.0.0.1"), 20000, 20001);
    def.add_node(3, String::from("127.0.0.1"), 30000, 30001);
    def
}

fn make_config(id: u16) -> Config {
    Config {
        node_id: id as u64,
        heartbeat_port: id * 10000,
        replicate_port: id * 10000 + 1,
        heartbeate_ms: 300,
        log_path: format!("data/raft{}", id),
        log_max_num: 20000,
        log_min_num: 10000,
        log_file_size_mb: 10,
    }
}
