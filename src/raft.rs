use crate::state_machine::CommondType;
use crate::storage::RaftLog;
use crate::{entity::*, error::*, sender::Sender};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
    Arc, Mutex,
};

pub struct Raft {
    id: u64,
    conf: Arc<Config>,
    state: RaftState,
    term: AtomicU64,
    voted: Mutex<u64>,
    leader: AtomicU64,
    election_elapsed: AtomicUsize,
    last_heart: AtomicU64,
    raft_log: RaftLog,
    store: RaftLog,
    replicas: Vec<u64>,
}

impl Raft {
    //this function only call by leader
    pub fn submit(&self, cmd: Vec<u8>) -> RaftResult<()> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.leader.load(SeqCst)));
        }

        let (term, index) = self.store.info();

        let index = index + 1;

        self.store.save(Entry::Log {
            term: term,
            index: index,
            commond: cmd,
        })?;

        //if here unwrap fail , may be program have a bug
        let entry = &self.store.get_log_binary(index);
        let raft_id = &self.id;
        smol::run(async {
            for node_id in &self.replicas {
                smol::Task::spawn(async { Sender::send_log(node_id, raft_id, entry).await });
            }
        });

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
