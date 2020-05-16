use crate::{entity::*, error::*};

pub struct Raft {
    raft_config: RaftConfig,
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

    pub fn leader_term(&self) -> (u64, u64) {
        panic!()
    }

    pub fn is_leader(&self) -> bool {
        let (leader, _) = self.leader_term();
        return leader == self.raft_config.node_id;
    }

    pub fn try_to_leader(&self, sync: bool) -> RaftResult<bool> {
        panic!()
    }

    pub fn recive_message<C: AsRef<[u8]>>(&self, commd: C) -> RaftResult<bool> {
        panic!()
    }
}
