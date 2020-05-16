use std::collections::HashMap;
use std::time::SystemTime;

pub struct ServerConfig {
    pub node_id: u64,
}
pub struct RaftConfig {
    // NodeID is the identity of the local node. NodeID cannot be 0.
    // This parameter is required.
    pub node_id: u64,
}

pub struct ReplicaStatus {
    commit: u64, // commmit位置
    next: u64,
    state: String,
    snapshoting: bool,
    paused: bool,
    active: bool,
    last_active: SystemTime,
}

pub struct Status {
    id: u64,
    node_id: u64,
    leader: u64,
    term: u64,
    index: u64,
    commit: u64,
    applied: u64,
    vote: u64,
    pend_queue: usize,
    recv_queue: usize,
    app_queue: usize,
    stopped: bool,
    restoringSnapshot: bool,
    state: String,
    replicas: HashMap<u64, ReplicaStatus>,
}
