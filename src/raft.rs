use crate::{entity::*, error::*};
use std::sync::Arc;

pub trait StateMachine {
    fn apply(&self, command: &[u8], index: u64) -> RaftResult<()>;
    fn apply_member_change(&self, t: CommondType, index: u64) -> RaftResult<()>;
    fn apply_leader_change(&self, leader: u64, index: u64) -> RaftResult<()>;
}

pub struct Raft {
    id: u64,
    conf: Arc<Config>,
    state: RaftState,
    term: u64,
    leader: u64,
    sm: Box<dyn StateMachine>,
}

pub enum CommondType {
    Data,
    AddNode,
    RemoveNode,
}

impl Raft {
    pub fn submit<C: AsRef<[u8]>>(&self, ct: CommondType, cmd: C) {
        panic!()
    }

    pub fn change_memeber(&self, ct: CommondType, id: u64) {
        panic!()
    }

    pub fn status(&self) {}

    pub fn is_leader(&self) -> bool {
        return self.leader == self.id;
    }

    pub fn try_to_leader(&self, sync: bool) -> RaftResult<bool> {
        panic!()
    }

    pub fn recive_message<C: AsRef<[u8]>>(&self, commd: C) -> RaftResult<bool> {
        panic!()
    }
}
