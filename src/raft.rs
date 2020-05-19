use crate::{entity::*, error::*};
use raft_log::RaftLog;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
    Arc, Mutex,
};

pub trait StateMachine {
    fn apply(&self, command: &[u8], index: u64) -> RaftResult<()>;
    fn apply_member_change(&self, t: CommondType, index: u64) -> RaftResult<()>;
    fn apply_leader_change(&self, leader: u64, index: u64) -> RaftResult<()>;
}

pub struct Raft {
    id: u64,
    conf: Arc<Config>,
    state: RaftState,
    term: AtomicU64,
    voted: Mutex<u64>,
    leader: AtomicU64,
    election_elapsed: AtomicUsize,
    sm: Arc<StateMachine + Sync + Send>,
    last_heart: AtomicU64,
    raft_log: RaftLog,
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

    // use this function make sure raft is follower
    pub fn heartbeat(&self, leader: u64, term: u64) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        } else if self_term < term {
            self.term.store(term, SeqCst);
        }
        self.leader.store(leader, SeqCst);
        self.election_elapsed.store(0, SeqCst);
        return Ok(());
    }

    pub fn vote(&self, apply_index: u64, term: u64) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        }

        let mut vote_term = self.voted.lock().unwrap();

        if *vote_term >= term {
            return Err(RaftError::VoteNotAllow);
        }

        match &self.state {
            RaftState::Candidate { num_votes: _ } => Err(RaftError::VoteNotAllow),
            _ => {
                *vote_term = term;
                Ok(())
            }
        }
    }

    pub fn is_follower(&self) -> bool {
        match &self.state {
            RaftState::Follower => true,
            _ => false,
        }
    }

    pub fn is_leader(&self) -> bool {
        return self.leader.load(SeqCst) == self.id;
    }

    pub fn try_to_leader(&self, sync: bool) -> RaftResult<bool> {
        panic!()
    }

    pub fn recive_message<C: AsRef<[u8]>>(&self, commd: C) -> RaftResult<bool> {
        panic!()
    }
}
