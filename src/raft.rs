use crate::state_machine::CommondType;
use crate::state_machine::Resolver;
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
    election_elapsed: AtomicUsize,
    last_heart: AtomicU64,
    raft_log: RaftLog,
    store: RaftLog,
    replicas: Vec<u64>,
    pub resolver: Arc<dyn Resolver + Sync + Send + 'static>,
}

impl Raft {
    //this function only call by leader
    pub fn submit(self: Arc<Raft>, cmd: Vec<u8>) -> RaftResult<()> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.leader.load(SeqCst)));
        }

        let (term, index) = self.store.info();

        let entry = Entry::Log {
            term: term,
            index: index + 1,
            commond: cmd,
        };

        //if here unwrap fail , may be program have a bug
        let data = Arc::new(entry.encode());
        self.store.save(entry)?;

        let raft = self.clone();

        let len = raft.replicas.len();

        let (mut tx, mut rx) = mpsc::channel(len);
        smol::run(async {
            for i in 0..len {
                let entry = data.clone();
                let raft = self.clone();
                let node_id = self.replicas[i];
                let mut tx = tx.clone();
                smol::Task::spawn(async move {
                    let _ = tx.try_send(Sender::send_log(node_id, raft, entry).await);
                })
                .detach();
            }

            let mut need = len / 2;

            while let Some(res) = rx.recv().await {
                match res {
                    Err(e) => error!("submit log has err:[{:?}]", e),
                    Ok(v) => {
                        need -= 1;
                        if need == 0 {
                            return Ok(());
                        }
                    }
                }
            }

            Err(RaftError::NotEnoughRecipient(
                len as u16 / 2,
                (len / 2 - need) as u16,
            ))
        })
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
