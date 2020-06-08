use crate::state_machine::{RSL, SM};
use crate::storage::RaftLog;
use crate::*;
use crate::{entity::*, error::*, sender::*};
use async_std::sync::{channel, Mutex, Receiver, RwLock, Sender as Tx};
pub use log::Level::Debug;
use log::{debug, error, info, log_enabled};
use rand::Rng;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    Arc,
};
use std::thread::sleep;
use std::time::Duration;

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
    sender: Sender,
    applied_tx_rx: (Tx<usize>, Receiver<usize>),
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
    pub async fn new(
        id: u64,
        conf: Arc<Config>,
        replicas: Vec<u64>,
        resolver: RSL,
        sm: SM,
    ) -> RaftResult<Arc<Self>> {
        let store = RaftLog::new(id, conf.clone())?;
        let (term, _, applied) = store.info().await;

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
            sender: sender::Sender::new(),
            applied_tx_rx: channel(1),
            last_heart: AtomicU64::new(current_millis()),
            store: store,
            replicas: replicas,
            resolver: resolver,
            sm: sm,
        });

        for node_id in &raft.replicas {
            raft.sender.add_peer(*node_id, raft.clone()).await;
        }
        Ok(raft)
    }

    fn notify(self: &Raft) {
        let _ = self.applied_tx_rx.0.try_send(0);
    }

    //this function only call by leader
    pub async fn submit(self: &Arc<Raft>, cmd: Vec<u8>) -> RaftResult<()> {
        if !self.is_leader().await {
            return Err(RaftError::NotLeader(self.leader.load(SeqCst)));
        }
        let term = self.term.load(SeqCst);
        let pre_term = self.store.last_term().await;
        let index = self.store.commit(pre_term, term, 0, cmd).await?;

        self.sender
            .send_log(self.store.log_mem.read().await.get(index).encode())
            .await?;
        self.applied.store(index, SeqCst);
        self.notify();
        self.store.apply(&self.sm, index).await
    }

    // use this function make sure raft is follower
    pub async fn heartbeat(
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

        if term == self.store.last_term().await
            && committed == self.store.last_index().await
            && self.store.last_applied().await < applied
        {
            self.applied.store(applied, SeqCst);
            self.notify();
        }

        if self.leader.load(SeqCst) != leader {
            self.leader.store(leader, SeqCst);
        }

        return Ok(());
    }

    pub async fn vote(&self, leader: u64, term: u64, committed: u64) -> RaftResult<()> {
        if self.term.load(SeqCst) > term {
            return Err(RaftError::TermLess);
        }

        if self.store.last_index().await > committed {
            return Err(RaftError::IndexLess(committed));
        }
        self.last_heart.store(current_millis(), SeqCst);

        let mut vote = self.voted.lock().await;

        if vote.update(leader, term, current_millis() + self.conf.heartbeate_ms) {
            Ok(())
        } else {
            Err(RaftError::VoteNotAllow)
        }
    }

    pub async fn leader_change(&self, leader: u64, term: u64, index: u64) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        } else if self_term < term {
            self.term.store(term, SeqCst);
        }
        self.last_heart.store(current_millis(), SeqCst);

        if self.store.last_applied().await < index {
            self.notify();
        }

        self.leader.store(leader, SeqCst);

        let _ = self.sm.apply_leader_change(leader, term, index);

        return Ok(());
    }

    pub async fn is_follower(&self) -> bool {
        match *self.state.read().await {
            RaftState::Follower => true,
            _ => false,
        }
    }

    pub async fn is_leader(&self) -> bool {
        match *self.state.read().await {
            RaftState::Leader => true,
            _ => false,
        }
    }

    pub async fn is_candidate(&self) -> bool {
        match *self.state.read().await {
            RaftState::Candidate => true,
            _ => false,
        }
    }

    pub async fn get_state(&self) -> RaftState {
        (*self.state.read().await).clone()
    }

    pub fn is_stoped(&self) -> bool {
        self.stopd.load(SeqCst)
    }

    pub async fn update_apply(&self, term: u64, index: u64) -> RaftResult<()> {
        if self.store.last_applied().await >= index {
            return Ok(());
        }

        if self.store.last_index().await < index {
            return Err(RaftError::IndexLess(self.store.last_index().await));
        }

        if let Entry::Commit { term: t, .. } = self.store.log_mem.read().await.get(index) {
            if *t == term {
                self.applied.store(index, SeqCst);
                self.notify();
                return Ok(());
            } else if *t < term {
                return Err(RaftError::TermLess);
            } else {
                return Err(RaftError::TermGreater);
            }
        };
        return Err(RaftError::IndexLess(self.store.last_index().await));
    }

    pub async fn info(&self) -> RaftInfo {
        let state = self.state.read().await;
        RaftInfo {
            id: self.id,
            node_id: self.node_id,
            leader: self.leader.load(SeqCst),
            last_heart: self.last_heart.load(SeqCst),
            term: self.term.load(SeqCst),
            committed: self.store.last_index().await,
            applied: self.store.last_applied().await,
            state: state.clone(),
        }
    }
}

//these method for  replication
impl Raft {
    pub async fn try_to_leader(self: &Arc<Raft>) -> RaftResult<bool> {
        if let Err(e) = self.to_voter(self.term.load(SeqCst)).await {
            error!("send vote has err:{:?}", e);
            self.to_follower().await;
            return Ok(false);
        } else {
            if let Err(e) = self.to_leader().await {
                error!("raft:{} to leader has err:{}", self.conf.node_id, e);
                self.to_follower().await;
                return Err(e);
            }
            return Ok(false);
        }
    }

    //put empty log
    async fn to_leader(self: &Arc<Raft>) -> RaftResult<()> {
        info!("raft_node:{} to leader ", self.node_id);
        if let Entry::LeaderChange {
            leader,
            term,
            index,
        } = {
            let mut state = self.state.write().await;
            if let RaftState::Leader = *state {
                return Ok(());
            };

            *state = RaftState::Leader;
            self.term.fetch_add(1, SeqCst);
            self.last_heart.store(current_millis(), SeqCst);
            let index = self.store.last_index().await;
            let lc = Entry::LeaderChange {
                leader: self.conf.node_id,
                term: self.term.load(SeqCst),
                index: index,
            };

            let body = lc.encode();
            self.sender.send_log(body).await?;
            lc
        } {
            self.sm.apply_leader_change(leader, term, index).await;
        }

        Ok(())
    }

    pub async fn to_follower(&self) {
        info!("raft:{} to follower ", self.node_id);
        let mut state = self.state.write().await;
        if let RaftState::Follower = *state {
            return;
        };
        *state = RaftState::Follower;
        self.last_heart.store(current_millis(), SeqCst);
        self.notify();
    }

    async fn to_voter(self: &Arc<Raft>, term: u64) -> RaftResult<()> {
        info!("raft:{} to voter ", self.conf.node_id);

        if self.voted.lock().await.update(
            self.conf.node_id,
            self.term.load(SeqCst),
            current_millis() + self.conf.heartbeate_ms,
        ) {
            let mut state = self.state.write().await;
            *state = RaftState::Candidate;
        } else {
            // found myself voting in this term
            return Err(RaftError::VoteNotAllow);
        }

        self.last_heart.store(current_millis(), SeqCst);

        let index = self.store.last_index().await;
        self.sender
            .send_log(
                Entry::Vote {
                    term: term,
                    leader: self.conf.node_id,
                    committed: index,
                }
                .encode(),
            )
            .await
    }
}

//these method for peer job, if leader will sleep when to member start
impl Raft {
    pub fn start(self: &Arc<Raft>) {
        //this thread for apply log when new applied recived
        let raft = self.clone();
        async_std::task::spawn(async move {
            while !raft.stopd.load(SeqCst) {
                let mut need_apply = raft.applied.load(SeqCst);
                if need_apply <= raft.store.last_applied().await {
                    raft.applied_tx_rx.1.recv().await.unwrap();
                    continue;
                }

                if raft.is_leader().await {
                    raft.applied_tx_rx.1.recv().await.unwrap();
                } else {
                    if need_apply > raft.store.last_index().await {
                        if log_enabled!(Debug) {
                            debug!(
                                "need_apply:{} leass than raft last_index:{} applied:{}",
                                need_apply,
                                raft.store.last_index().await,
                                raft.store.last_applied().await
                            );
                        }
                        need_apply = raft.store.last_index().await;
                    }
                    if let Err(e) = raft.store.apply(&raft.sm, need_apply).await {
                        error!("store apply has err:{}", e);
                    }
                    if raft.store.last_applied().await >= need_apply {
                        raft.applied_tx_rx.1.recv().await.unwrap();
                    }
                }
            }
        });

        let raft = self.clone();
        //this job for heartbeat . leader to send , follwer to check heartbeat time
        async_std::task::spawn(async move {
            while !raft.stopd.load(SeqCst) {
                if raft.is_leader().await {
                    let (_, committed, applied) = raft.store.info().await;
                    let ie = Entry::Heartbeat {
                        term: raft.term.load(SeqCst),
                        leader: raft.conf.node_id,
                        committed: committed,
                        applied: applied,
                    };

                    raft.sender.send_heartbeat(ie.encode()).await;
                } else if raft.is_follower().await {
                    if current_millis() - raft.last_heart.load(SeqCst) > raft.conf.heartbeate_ms * 3
                    {
                        info!(
                            "{} too long time recived heartbeat , try to leader",
                            raft.conf.node_id
                        );
                        //rand sleep to elect
                        sleep(Duration::from_millis(
                            rand::thread_rng().gen_range(150, 300),
                        ));

                        let term = raft.term.load(SeqCst);

                        if !raft.is_follower().await
                            || current_millis() - raft.last_heart.load(SeqCst)
                                < raft.conf.heartbeate_ms * 3
                        {
                            break;
                        }

                        raft.leader.store(0, SeqCst);

                        if let Err(e) = raft.to_voter(term).await {
                            error!("send vote has err:{:?}", e);
                            raft.to_follower().await;
                        } else {
                            if let Err(e) = raft.to_leader().await {
                                error!("raft:{} to leader has err:{}", raft.conf.node_id, e);
                                raft.to_follower().await;
                            }
                        }
                    }
                }
                sleep(Duration::from_millis(raft.conf.heartbeate_ms));
            }
        });
    }
}
