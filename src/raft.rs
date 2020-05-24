use crate::state_machine::{RSL, SM};
use crate::storage::RaftLog;
use crate::*;
use crate::{entity::*, error::*, sender};
use log::{error, info};
use rand::Rng;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    Arc, Mutex, RwLock,
};
use std::thread::sleep;
use std::time::Duration;

pub struct Raft {
    id: u64,
    conf: Arc<Config>,
    state: RwLock<RaftState>,
    stopd: AtomicBool,
    term: AtomicU64,
    voted: Mutex<Vote>,
    leader: AtomicU64,
    pub apply: AtomicU64,
    last_heart: AtomicU64,
    pub store: RaftLog,
    pub replicas: Vec<u64>,
    pub resolver: RSL,
    sm: SM,
}

#[derive(Default)]
struct Vote {
    leader: u64,
    term: u64,
    end_time: u64,
}

impl Vote {
    fn update(&mut self, leader: u64, term: u64, end_time: u64) -> bool {
        if end_time > current_millis() && (self.leader != leader || self.term != term) {
            return false;
        }
        self.leader = leader;
        self.term = term;
        self.end_time = end_time;
        return true;
    }
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
            state: RwLock::new(RaftState::Follower),
            stopd: AtomicBool::new(false),
            term: AtomicU64::new(0),
            voted: Mutex::new(Vote::default()),
            leader: AtomicU64::new(0),
            apply: AtomicU64::new(0),
            last_heart: AtomicU64::new(current_millis()),
            store: RaftLog::new(id, conf)?,
            replicas: replicas,
            resolver: resolver,
            sm: sm,
        })
    }

    //this function only call by leader
    pub fn submit(self: &Arc<Raft>, cmd: Vec<u8>) -> RaftResult<()> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.leader.load(SeqCst)));
        }

        let (term, index, _) = self.store.info();

        let entry = Entry::Commit {
            term: term,
            index: index + 1,
            commond: cmd,
        };

        self.store.commit(entry)?;

        if let Some(e) = self.store.log_mem.read().unwrap().get(index + 1) {
            sender::send(self.clone(), e)?;
            if let Entry::Commit {
                term,
                index,
                commond,
            } = e
            {
                self.sm.apply(term, index, commond)?;
            };

            let raft = self.clone();
            smol::Task::spawn(async move {
                if let Err(e) = sender::send(
                    raft,
                    &Entry::Apply {
                        term: term,
                        index: index,
                    },
                ) {
                    error!("send apply has err:{}", e);
                }
            })
            .detach();

            return Ok(());
        };

        panic!("impossible")
    }

    // use this function make sure raft is follower
    pub fn heartbeat(
        &self,
        term: u64,
        leader: u64,
        _committed: u64,
        _applied: u64,
    ) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        } else if self_term < term {
            self.term.store(term, SeqCst);
        }
        self.leader.store(leader, SeqCst);
        return Ok(());
    }

    pub fn vote(&self, leader: u64, term: u64, committed: u64) -> RaftResult<()> {
        if self.term.load(SeqCst) > term {
            return Err(RaftError::TermLess);
        }

        if self.store.last_index() > committed {
            return Err(RaftError::IndexLess(committed));
        }

        let mut vote = self.voted.lock().unwrap();

        if vote.update(leader, term, current_millis() + self.conf.heartbeate_ms) {
            Ok(())
        } else {
            Err(RaftError::VoteNotAllow)
        }
    }

    pub fn is_follower(&self) -> bool {
        match *self.state.read().unwrap() {
            RaftState::Follower => true,
            _ => false,
        }
    }

    pub fn is_leader(&self) -> bool {
        match *self.state.read().unwrap() {
            RaftState::Leader => true,
            _ => false,
        }
    }

    pub fn is_candidate(&self) -> bool {
        match *self.state.read().unwrap() {
            RaftState::Candidate => true,
            _ => false,
        }
    }

    pub fn try_to_leader(&self) -> RaftResult<bool> {
        panic!()
    }

    pub fn update_apply(&self, term: u64, index: u64) -> RaftResult<()> {
        if self.store.log_mem.read().unwrap().offset >= index {
            return Ok(());
        }

        if let Some(Entry::Commit { term: t, .. }) = self.store.log_mem.read().unwrap().get(index) {
            if *t == term {
                self.apply.store(index, SeqCst);
                return Ok(());
            } else if *t < term {
                return Err(RaftError::TermLess);
            } else {
                return Err(RaftError::TermGreater);
            }
        };

        return Err(RaftError::IndexLess(self.store.last_index()));
    }
}

impl Raft {
    pub fn start(raft: Arc<Raft>) {
        let mut candidate = 0;
        while !raft.stopd.load(SeqCst) {
            if raft.is_leader() {
                let (_, committed, applied) = raft.store.info();

                let ie = Entry::Heartbeat {
                    term: raft.term.load(SeqCst),
                    leader: raft.id,
                    committed: committed,
                    applied: applied,
                };
                if let Err(e) = sender::send(raft.clone(), &ie) {
                    error!("send heartbeat has err:{:?}", e);
                }
            } else if raft.is_follower() {
                if current_millis() - raft.last_heart.load(SeqCst) > raft.conf.heartbeate_ms * 3 {
                    info!("{} to long time recive heartbeat , try to leader", raft.id);
                    if raft.to_voter() {
                        raft.last_heart.store(current_millis(), SeqCst);
                        if let Err(e) = sender::send(
                            raft.clone(),
                            &Entry::Vote {
                                term: raft.term.load(SeqCst),
                                leader: raft.id,
                                committed: raft.store.last_index(),
                            },
                        ) {
                            error!("send heartbeat has err:{:?}", e);
                            raft.to_follower()
                        } else {
                            raft.to_leader();
                            //put empty log
                            if let Err(e) = sender::send(
                                raft.clone(),
                                &Entry::Commit {
                                    term: raft.term.load(SeqCst),
                                    index: raft.store.last_index(),
                                    commond: Vec::default(),
                                },
                            ) {
                                error!("send first log has err :{}", e);
                            }
                        }
                    }
                }
            }

            sleep(Duration::from_millis(raft.conf.heartbeate_ms));
        }
    }

    fn to_leader(&self) {
        let mut state = self.state.write().unwrap();
        if let RaftState::Leader = *state {
            return;
        };
        *state = RaftState::Leader;
        self.term.fetch_add(1, SeqCst);
        self.last_heart.store(current_millis(), SeqCst);
    }

    fn to_follower(&self) {
        let mut state = self.state.write().unwrap();
        if let RaftState::Follower = *state {
            return;
        };
        *state = RaftState::Follower;
        self.last_heart.store(current_millis(), SeqCst);
    }

    fn to_voter(&self) -> bool {
        //rand sleep to elect
        sleep(Duration::from_millis(
            rand::thread_rng().gen_range(150, 300),
        ));
        if !self.is_follower()
            || current_millis() - self.last_heart.load(SeqCst) < self.conf.heartbeate_ms * 3
        {
            return false;
        }

        if self.voted.lock().unwrap().update(
            self.id,
            self.term.load(SeqCst),
            current_millis() + self.conf.heartbeate_ms,
        ) {
            let mut state = self.state.write().unwrap();
            *state = RaftState::Candidate;
            true
        } else {
            // found myself voting in this term
            false
        }
    }
}
