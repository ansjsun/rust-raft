mod entity;
mod error;
mod raft;
mod sender;
mod server;
mod state_machine;
mod storage;
#[macro_use]
extern crate thiserror;

pub use server::Server;

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

pub fn current_millis() -> u64 {
    std::time::SystemTime::now().elapsed().unwrap().as_millis() as u64
}
