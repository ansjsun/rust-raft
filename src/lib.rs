mod client;
mod entity;
mod error;
mod raft;
mod raft_server;
mod server;
mod state_machine;
mod storage;
#[macro_use]
extern crate thiserror;

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
