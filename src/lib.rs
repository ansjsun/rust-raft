pub mod entity;
pub mod error;
pub mod raft;
pub mod server;

#[macro_use]
extern crate thiserror;

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
