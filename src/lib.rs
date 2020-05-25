pub mod entity;
pub mod error;
pub mod raft;
pub mod sender;
pub mod server;
pub mod state_machine;
pub mod storage;
#[macro_use]
extern crate thiserror;

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

pub fn current_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    let ms = since_the_epoch.as_secs() as u64 * 1000u64
        + (since_the_epoch.subsec_nanos() as f64 / 1_000_000.0) as u64;
    ms
}

#[test]
fn test_current_millis() {
    log::info!("{}", current_millis());
}
