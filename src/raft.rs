use crate::state_machine::CommondType;
use crate::state_machine::{Resolver, StateMachine, RSL, SM};
use crate::storage::RaftLog;
use crate::{entity::*, error::*, sender::Sender};
use log::error;
use std::marker::Sized;
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
    last_heart: AtomicU64,
    pub store: RaftLog,
    pub replicas: Vec<u64>,
    pub resolver: RSL,
    sm: SM,
}

impl Raft {
    pub fn new(
        id: u64,
        conf: Arc<Config>,
        replicas: Vec<u64>,
        resolver: RSL,
        sm: SM,
    ) -> RaftResult<Self> {
        Ok(Raft {
            id: id,
            conf: conf.clone(),
            state: RaftState::Follower,
            term: AtomicU64::new(0),
            voted: Mutex::new(0),
            leader: AtomicU64::new(0),
            apply: AtomicU64::new(0),
            last_heart: AtomicU64::new(crate::current_millis()),
            store: RaftLog::new(id, conf)?,
            replicas: replicas,
            resolver: resolver,
            sm: sm,
        })
    }

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

    pub fn check_apply(&self, term: u64, index: u64) -> RaftResult<()> {
        if self.store.log_mem.read().unwrap().offset >= index {
            return Ok(());
        }

        if let Some(Entry::Commit { term: t, .. }) = self.store.log_mem.read().unwrap().get(index) {
            if *t == term {
                return Ok(());
            } else if *t < term {
                return Err(RaftError::TermLess);
            } else {
                return Err(RaftError::TermGreater);
            }
        };

        return Err(RaftError::IndexLess(
            self.store.log_mem.read().unwrap().last_index(),
        ));
    }
}
