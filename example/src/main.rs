use log::{debug, info};
use rust4rs::{entity::Config, error::*, server::Server, state_machine::*};
use std::sync::Arc;

fn main() {
    env_logger::init_from_env(
        env_logger::Env::default()
            .filter_or("debug", "trace")
            .write_style_or("auto", "always"),
    );

    debug!("start............");
    smol::run(async {
        let server1 = Arc::new(Server::new(make_config(1), make_resolver(), SM { id: 1 })).start();
        let server2 = Arc::new(Server::new(make_config(2), make_resolver(), SM { id: 2 })).start();
        let server3 = Arc::new(Server::new(make_config(3), make_resolver(), SM { id: 3 })).start();

        let replicas = &vec![1, 2, 3];

        let raft1 = server1.create_raft(1, 0, &replicas).unwrap();
        let _raft2 = server2.create_raft(1, 0, &replicas).unwrap();
        let _raft3 = server3.create_raft(1, 0, &replicas).unwrap();
        let mut times = 0;
        println!("1231231");
        while !raft1.is_leader() {
            raft1.try_to_leader().await.unwrap();
            std::thread::sleep(std::time::Duration::from_secs(1));
            times += 1;
            info!("wait raft1 to leader times:{}", times);
        }

        for i in 0..1000000 {
            raft1
                .submit(unsafe { format!("commit: {}", i + 1).as_mut_vec().clone() })
                .await
                .unwrap();
        }
    });

    std::thread::sleep(std::time::Duration::from_secs(10000));
}

struct SM {
    id: usize,
}

impl StateMachine for SM {
    fn apply(&self, term: &u64, index: &u64, command: &[u8]) -> RaftResult<()> {
        println!(
            "apply {} term:{} index:{} command:{:?}",
            self.id,
            term,
            index,
            String::from_utf8_lossy(command)
        );
        Ok(())
    }
    fn apply_member_change(&self, t: CommondType, index: u64) {
        println!(
            "apply_member_change {} type:{:?} index:{}",
            self.id, t, index
        );
    }
    fn apply_leader_change(&self, leader: u64, term: u64, index: u64) {
        println!(
            "apply_member_change {} leader:{} term:{} index:{}",
            self.id, leader, term, index
        );
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
