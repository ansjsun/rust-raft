use crate::state_machine::CommondType;
use crate::state_machine::{Resolver, StateMachine};
use crate::storage::RaftLog;
use crate::{entity::*, error::*, sender::Sender};
use log::error;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
    Arc, Mutex,
};
use tokio::sync::mpsc;

pub struct Raft {
    id: u64,
    conf: Arc<Config>,
    state: RaftState,
    term: AtomicU64,
    voted: Mutex<u64>,
    leader: AtomicU64,
    pub apply: AtomicU64,
    election_elapsed: AtomicUsize,
    last_heart: AtomicU64,
    pub store: RaftLog,
    pub replicas: Vec<u64>,
    pub resolver: Arc<dyn Resolver + Sync + Send + 'static>,
    sm: Arc<dyn StateMachine + Sync + Send>,
}

impl Raft {
    //this function only call by leader
    pub fn submit(self: Arc<Raft>, cmd: Vec<u8>) -> RaftResult<()> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.leader.load(SeqCst)));
        }

        let (term, index) = self.store.info();

        let entry = Entry::Commit {
            term: term,
            index: index + 1,
            commond: cmd,
        };

        let data = Arc::new(entry.encode());
        self.store.commit(entry)?;

        Sender::send_log(self.clone(), &data)?;

        if let Some(Entry::Commit {
            term,
            index,
            commond,
        }) = self.store.log_mem.read().unwrap().get(index + 1)
        {
            return self.sm.apply(term, index, commond);
        };

        panic!("impossible")
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

    pub fn vote(&self, _leader: u64, committed: u64, term: u64) -> RaftResult<()> {
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
