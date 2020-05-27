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
use tokio::sync::Notify;

pub struct Raft {
    pub id: u64,
    node_id: u64,
    conf: Arc<Config>,
    state: RwLock<RaftState>,
    stopd: AtomicBool,
    term: AtomicU64,
    voted: Mutex<Vote>,
    leader: AtomicU64,
    pub applied: AtomicU64,
    applied_notify: Notify,
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
        if self.end_time > current_millis()
            && self.leader != 0
            && (self.leader != leader || self.term != term)
        {
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
            node_id: conf.node_id,
            conf: conf.clone(),
            state: RwLock::new(RaftState::Follower),
            stopd: AtomicBool::new(false),
            term: AtomicU64::new(0),
            voted: Mutex::new(Vote::default()),
            leader: AtomicU64::new(0),
            applied: AtomicU64::new(0),
            applied_notify: Notify::new(),
            last_heart: AtomicU64::new(current_millis()),
            store: RaftLog::new(id, conf)?,
            replicas: replicas,
            resolver: resolver,
            sm: sm,
        })
    }

    // use this function make sure raft is follower
    pub fn heartbeat(
        &self,
        term: u64,
        leader: u64,
        _committed: u64,
        applied: u64,
    ) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        } else if self_term < term {
            self.term.store(term, SeqCst);
        }

        if self.store.last_applied() < applied {
            self.applied_notify.notify();
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

    pub fn leader(&self, leader: u64, term: u64, index: u64) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        } else if self_term < term {
            self.term.store(term, SeqCst);
        }

        if self.store.last_applied() < index {
            self.applied_notify.notify();
        }

        self.leader.store(leader, SeqCst);

        let _ = self.sm.apply_leader_change(leader, term, index);
        return Ok(());
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

    pub fn get_state(&self) -> RaftState {
        (*self.state.read().unwrap()).clone()
    }

    pub fn update_apply(&self, term: u64, index: u64) -> RaftResult<()> {
        if self.store.log_mem.read().unwrap().offset >= index {
            return Ok(());
        }

        if let Some(Entry::Commit { term: t, .. }) = self.store.log_mem.read().unwrap().get(index) {
            if *t == term {
                self.applied.store(index, SeqCst);
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
            self.applied.fetch_add(1, SeqCst);
            self.applied_notify.notify();
            return Ok(());
        };

        panic!("impossible")
    }

    pub fn start(self: &Arc<Raft>) {
        //this thread for apply log when new applied recived
        let raft = self.clone();
        smol::Task::blocking(async move {
            while !raft.stopd.load(SeqCst) {
                let index = match raft.store.apply(raft.applied.load(SeqCst)) {
                    Err(e) => {
                        error!("store apply has err:{}", e);
                        0
                    }
                    Ok(index) => index,
                };

                if index > 0 {
                    if let Entry::Commit {
                        term,
                        index,
                        commond,
                    } = raft.store.log_mem.read().unwrap().get(index).unwrap()
                    {
                        let _ = raft.sm.apply(term, index, commond);
                    };
                }
                if raft.store.last_applied() >= raft.applied.load(SeqCst) {
                    raft.applied_notify.notified().await;
                }
            }
        })
        .detach();

        let raft = self.clone();
        smol::Task::blocking(async move {
            while !raft.stopd.load(SeqCst) {
                if raft.is_leader() {
                    let (_, committed, applied) = raft.store.info();
                    let ie = Entry::Heartbeat {
                        term: raft.term.load(SeqCst),
                        leader: raft.conf.node_id,
                        committed: committed,
                        applied: applied,
                    };
                    if let Err(e) = sender::send(raft.clone(), &ie) {
                        error!("send heartbeat has err:{:?}", e);
                    }
                } else if raft.is_follower() {
                    if current_millis() - raft.last_heart.load(SeqCst) > raft.conf.heartbeate_ms * 2
                    {
                        info!(
                            "{} to long time recive heartbeat , try to leader",
                            raft.conf.node_id
                        );
                        //rand sleep to elect
                        sleep(Duration::from_millis(
                            rand::thread_rng().gen_range(150, 300),
                        ));
                        if !raft.is_follower()
                            || current_millis() - raft.last_heart.load(SeqCst)
                                < raft.conf.heartbeate_ms * 2
                        {
                            break;
                        }
                        if let Err(e) = Raft::to_voter(raft.clone()) {
                            error!("send vote has err:{:?}", e);
                            raft.to_follower();
                        } else {
                            if let Err(e) = Raft::to_leader(raft.clone()) {
                                error!("raft:{} to leader has err:{}", raft.conf.node_id, e);
                                raft.to_follower();
                            }
                        }
                    }
                }
                sleep(Duration::from_millis(raft.conf.heartbeate_ms));
            }
        })
        .detach();
    }

    pub fn try_to_leader(raft: Arc<Raft>) -> RaftResult<()> {
        if let Err(e) = Raft::to_voter(raft.clone()) {
            error!("send vote has err:{:?}", e);
            raft.to_follower();
        } else {
            if let Err(e) = Raft::to_leader(raft.clone()) {
                error!("raft:{} to leader has err:{}", raft.conf.node_id, e);
                raft.to_follower();
                return Err(e);
            }
        }
        return Ok(());
    }

    //put empty log
    fn to_leader(raft: Arc<Raft>) -> RaftResult<()> {
        info!("raft:{} to leader ", raft.node_id);
        let mut state = raft.state.write().unwrap();
        if let RaftState::Leader = *state {
            return Ok(());
        };
        *state = RaftState::Leader;
        raft.term.fetch_add(1, SeqCst);
        raft.last_heart.store(current_millis(), SeqCst);
        sender::send(
            raft.clone(),
            &Entry::ToLeader {
                leader: raft.conf.node_id,
                term: raft.term.load(SeqCst),
                index: raft.store.last_index(),
            },
        )
    }

    fn to_follower(&self) {
        info!("raft:{} to follower ", self.node_id);
        let mut state = self.state.write().unwrap();
        if let RaftState::Follower = *state {
            return;
        };
        *state = RaftState::Follower;
        self.last_heart.store(current_millis(), SeqCst);
    }

    fn to_voter(raft: Arc<Raft>) -> RaftResult<()> {
        info!("raft:{} to voter ", raft.conf.node_id);

        if raft.voted.lock().unwrap().update(
            raft.conf.node_id,
            raft.term.load(SeqCst),
            current_millis() + raft.conf.heartbeate_ms,
        ) {
            let mut state = raft.state.write().unwrap();
            *state = RaftState::Candidate;
        } else {
            // found myself voting in this term
            return Err(RaftError::VoteNotAllow);
        }

        raft.last_heart.store(current_millis(), SeqCst);
        sender::send(
            raft.clone(),
            &Entry::Vote {
                term: raft.term.load(SeqCst),
                leader: raft.conf.node_id,
                committed: raft.store.last_index(),
            },
        )
    }
}
