use crate::state_machine::{RSL, SM};
use crate::storage::RaftLog;
use crate::*;
use crate::{entity::*, error::*, sender::*};
pub use log::Level::Debug;
use log::{debug, error, info, log_enabled};
use rand::Rng;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    Arc, Mutex, RwLock,
};
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::Notify;

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

pub struct RaftInfo {
    pub id: u64,
    pub node_id: u64,
    pub leader: u64,
    pub last_heart: u64,
    pub term: u64,
    pub committed: u64,
    pub applied: u64,
    pub state: RaftState,
}

//term stands max term for heartbeat.
pub struct Raft {
    pub id: u64,
    node_id: u64,
    conf: Arc<Config>,
    state: RwLock<RaftState>,
    stopd: AtomicBool,
    //this term update by heartbeat
    term: AtomicU64,
    voted: Mutex<Vote>,
    leader: AtomicU64,
    //update by heartbeat or commit
    pub applied: AtomicU64,
    sender: RwLock<Sender>,
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

impl Raft {
    pub fn new(
        id: u64,
        conf: Arc<Config>,
        replicas: Vec<u64>,
        resolver: RSL,
        sm: SM,
    ) -> RaftResult<Arc<Self>> {
        let store = RaftLog::new(id, conf.clone())?;
        let (term, _, applied) = store.info();

        let raft = Arc::new(Raft {
            id: id,
            node_id: conf.node_id,
            conf: conf.clone(),
            state: RwLock::new(RaftState::Follower),
            stopd: AtomicBool::new(false),
            term: AtomicU64::new(term),
            voted: Mutex::new(Vote::default()),
            leader: AtomicU64::new(0),
            applied: AtomicU64::new(applied),
            sender: RwLock::new(sender::Sender::new()),
            applied_notify: Notify::new(),
            last_heart: AtomicU64::new(current_millis()),
            store: store,
            replicas: replicas,
            resolver: resolver,
            sm: sm,
        });

        for node_id in &raft.replicas {
            raft.sender
                .write()
                .unwrap()
                .run_peer(*node_id, raft.clone());
        }
        Ok(raft)
    }

    //this function only call by leader
    pub fn submit(self: &Arc<Raft>, cmd: Vec<u8>) -> RaftResult<()> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader(self.leader.load(SeqCst)));
        }
        let term = self.term.load(SeqCst);
        let pre_term = self.store.last_term();
        let index = self.store.commit(pre_term, term, 0, cmd)?;

        self.sender.read().unwrap().send_log(
            index,
            self.store.log_mem.read().unwrap().get(index).encode(),
        )?;
        self.applied.store(index, SeqCst);
        self.applied_notify.notify();
        self.store.apply(&self.sm, index)
    }

    // use this function make sure raft is follower
    pub fn heartbeat(
        &self,
        term: u64,
        leader: u64,
        committed: u64,
        applied: u64,
    ) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        } else if self_term < term {
            self.term.store(term, SeqCst);
        }
        self.last_heart.store(current_millis(), SeqCst);

        if term == self.store.last_term()
            && committed == self.store.last_index()
            && self.store.last_applied() < applied
        {
            self.applied.store(applied, SeqCst);
            self.applied_notify.notify();
        }

        if self.leader.load(SeqCst) != leader {
            self.leader.store(leader, SeqCst);
        }

        return Ok(());
    }

    pub fn vote(&self, leader: u64, term: u64, committed: u64) -> RaftResult<()> {
        if self.term.load(SeqCst) > term {
            return Err(RaftError::TermLess);
        }

        if self.store.last_index() > committed {
            return Err(RaftError::IndexLess(committed));
        }
        self.last_heart.store(current_millis(), SeqCst);

        let mut vote = self.voted.lock().unwrap();

        if vote.update(leader, term, current_millis() + self.conf.heartbeate_ms) {
            Ok(())
        } else {
            Err(RaftError::VoteNotAllow)
        }
    }

    pub fn leader_change(&self, leader: u64, term: u64, index: u64) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        } else if self_term < term {
            self.term.store(term, SeqCst);
        }
        self.last_heart.store(current_millis(), SeqCst);

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

    pub fn is_stoped(&self) -> bool {
        self.stopd.load(SeqCst)
    }

    pub fn update_apply(&self, term: u64, index: u64) -> RaftResult<()> {
        if self.store.last_applied() >= index {
            return Ok(());
        }

        if self.store.last_index() < index {
            return Err(RaftError::IndexLess(self.store.last_index()));
        }

        if let Entry::Commit { term: t, .. } = self.store.log_mem.read().unwrap().get(index) {
            if *t == term {
                self.applied.store(index, SeqCst);
                self.applied_notify.notify();
                return Ok(());
            } else if *t < term {
                return Err(RaftError::TermLess);
            } else {
                return Err(RaftError::TermGreater);
            }
        };
        return Err(RaftError::IndexLess(self.store.last_index()));
    }

    pub fn info(&self) -> RaftInfo {
        let state = self.state.read().unwrap();
        RaftInfo {
            id: self.id,
            node_id: self.node_id,
            leader: self.leader.load(SeqCst),
            last_heart: self.last_heart.load(SeqCst),
            term: self.term.load(SeqCst),
            committed: self.store.last_index(),
            applied: self.store.last_applied(),
            state: state.clone(),
        }
    }
}

//these method for  replication
impl Raft {
    pub fn try_to_leader(self: &Arc<Raft>) -> RaftResult<bool> {
        if let Err(e) = Raft::to_voter(self.term.load(SeqCst), self) {
            error!("send vote has err:{:?}", e);
            self.to_follower();
            return Ok(false);
        } else {
            if let Err(e) = Raft::to_leader(self) {
                error!("raft:{} to leader has err:{}", self.conf.node_id, e);
                self.to_follower();
                return Err(e);
            }
            return Ok(false);
        }
    }

    //put empty log
    fn to_leader(raft: &Arc<Raft>) -> RaftResult<()> {
        info!("raft_node:{} to leader ", raft.node_id);
        if let Entry::LeaderChange {
            leader,
            term,
            index,
        } = {
            let mut state = raft.state.write().unwrap();
            if let RaftState::Leader = *state {
                return Ok(());
            };

            *state = RaftState::Leader;
            raft.term.fetch_add(1, SeqCst);
            raft.last_heart.store(current_millis(), SeqCst);
            let index = raft.store.last_index();
            let lc = Entry::LeaderChange {
                leader: raft.conf.node_id,
                term: raft.term.load(SeqCst),
                index: index,
            };

            send(raft, &lc)?;
            lc
        } {
            raft.sm.apply_leader_change(leader, term, index);
        }

        Ok(())
    }

    fn to_follower(&self) {
        info!("raft:{} to follower ", self.node_id);
        let mut state = self.state.write().unwrap();
        if let RaftState::Follower = *state {
            return;
        };
        *state = RaftState::Follower;
        self.last_heart.store(current_millis(), SeqCst);
        self.applied_notify.notify();
    }

    fn to_voter(term: u64, raft: &Arc<Raft>) -> RaftResult<()> {
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

        let index = raft.store.last_index();
        send(
            raft,
            &Entry::Vote {
                term: term,
                leader: raft.conf.node_id,
                committed: index,
            },
        )
    }
}

//these method for peer job, if leader will sleep when to member start
impl Raft {
    pub fn start(self: &Arc<Raft>) {
        //this thread for apply log when new applied recived
        let raft = self.clone();
        smol::Task::blocking(async move {
            while !raft.stopd.load(SeqCst) {
                let mut need_index = raft.applied.load(SeqCst);
                if need_index <= raft.store.last_applied() {
                    raft.applied_notify.notified().await;
                    continue;
                }

                if raft.is_leader() {
                    raft.applied_notify.notified().await;
                } else {
                    if need_index > raft.store.last_index() {
                        if log_enabled!(Debug) {
                            debug!(
                                "need_index:{} leass than raft last_index:{} applied:{}",
                                need_index,
                                raft.store.last_index(),
                                raft.store.last_applied()
                            );
                        }
                        need_index = raft.store.last_index();
                    }
                    if let Err(e) = raft.store.apply(&raft.sm, need_index) {
                        error!("store apply has err:{}", e);
                    }
                    if raft.store.last_applied() >= need_index {
                        raft.applied_notify.notified().await;
                    }
                }
            }
        })
        .detach();

        let raft = self.clone();
        //this job for heartbeat . leader to send , follwer to check heartbeat time
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
                    if let Err(e) = raft.sender.read().unwrap().send_heartbeat(ie.encode()) {
                        error!("send heartbeat has err:{:?}", e);
                    }
                } else if raft.is_follower() {
                    if current_millis() - raft.last_heart.load(SeqCst) > raft.conf.heartbeate_ms * 3
                    {
                        info!(
                            "{} to long time recive heartbeat , try to leader",
                            raft.conf.node_id
                        );
                        //rand sleep to elect
                        sleep(Duration::from_millis(
                            rand::thread_rng().gen_range(150, 300),
                        ));

                        let term = raft.term.load(SeqCst);

                        if !raft.is_follower()
                            || current_millis() - raft.last_heart.load(SeqCst)
                                < raft.conf.heartbeate_ms * 3
                        {
                            break;
                        }

                        raft.leader.store(0, SeqCst);

                        if let Err(e) = Raft::to_voter(term, &raft) {
                            error!("send vote has err:{:?}", e);
                            raft.to_follower();
                        } else {
                            if let Err(e) = Raft::to_leader(&raft) {
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
}
