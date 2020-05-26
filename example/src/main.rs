use rust4rs::{entity::Config, error::*, server::Server, state_machine::*};
use std::sync::Arc;

fn main() {
    env_logger::init();

    let server1 = Arc::new(Server::new(make_config(1), make_resolver(), SM { id: 1 }));
    let server2 = Arc::new(Server::new(make_config(2), make_resolver(), SM { id: 2 }));
    let server3 = Arc::new(Server::new(make_config(3), make_resolver(), SM { id: 3 }));

    let server = server1.clone();
    server.start();

    let server = server2.clone();
    server.start();

    let server = server3.clone();
    server.start();

    let replicas = &vec![1, 2, 3];

    let _raft1 = server1.create_raft(1, 0, &replicas).unwrap();
    let _raft2 = server1.create_raft(1, 0, &replicas).unwrap();
    let _raft3 = server1.create_raft(1, 0, &replicas).unwrap();

    std::thread::sleep(std::time::Duration::from_secs(10000));
}

struct SM {
    id: usize,
}

impl StateMachine for SM {
    fn apply(&self, term: &u64, index: &u64, command: &[u8]) -> RaftResult<()> {
        println!(
            "apply {} term:{} index:{} command:{:?}",
            self.id, term, index, command
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
        heartbeat_port: id * 1000,
        replicate_port: id * 1000 + 1,
        heartbeate_ms: 300,
        log_path: format!("data/raft{}", id),
        log_max_num: 20000,
        log_min_num: 10000,
        log_file_size_mb: 35,
    }
}
