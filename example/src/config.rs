use async_trait::async_trait;
use raft4rs::{entity::Config, error::*, state_machine::*};

pub struct MySM {
    pub id: usize,
}

#[async_trait]
impl StateMachine for MySM {
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
        _term: u64,
        index: u64,
        _node_id: u64,
        action: u8,
        _exists: bool,
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

pub fn make_resolver() -> DefResolver {
    let mut def = DefResolver::new();
    def.add_node(1, String::from("127.0.0.1"), 10000, 10001);
    def.add_node(2, String::from("127.0.0.1"), 20000, 20001);
    def.add_node(3, String::from("127.0.0.1"), 30000, 30001);
    def
}

pub fn make_config(id: u16) -> Config {
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

pub fn init_log() {
    use env_logger::{fmt, Builder, Env};
    use std::io::Write;

    fn init_logger() {
        let env = Env::default()
            .filter_or("MY_LOG_LEVEL", "debug")
            .write_style_or("MY_LOG_STYLE", "always");

        Builder::from_env(env)
            .format(|buf, record| {
                let mut style = buf.style();
                style.set_bg(fmt::Color::Yellow).set_bold(true);

                let timestamp = buf.timestamp();

                writeln!(
                    buf,
                    "[{}] [{}:{}] {}: {}",
                    timestamp,
                    record.file_static().unwrap(),
                    record.line().unwrap(),
                    style.value(record.level()),
                    record.args()
                )
            })
            .init();
    }

    init_logger();
}
