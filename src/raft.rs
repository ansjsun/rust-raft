use crate::state_machine::{RSL, SM};
use crate::storage::RaftLog;
use crate::*;
use crate::{entity::*, error::*, sender::*};
use async_std::{
    sync::{channel, Mutex, Receiver, RwLock, Sender as Tx},
    task,
};
pub use log::Level::Debug;
use log::{debug, error, info, log_enabled};
use rand::Rng;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    Arc,
};
use std::time::Duration;

impl Vote {
    fn update(&mut self, leader: u64, term: u64, end_time: u64) -> bool {
        if self.end_time > current_millis() {
            println!(
                "tttttttttttt{}---{}------------{}",
                self.end_time,
                current_millis(),
                self.end_time - current_millis()
            );
            return false;
        }
        if self.leader != leader && self.term > term {
            println!(
                "{}--------------{}--------------{}-----------------{}",
                self.leader, leader, self.term, term
            );
            return false;
        }
        self.leader = leader;
        self.term = term;
        self.end_time = end_time;
        return true;
    }
}
#[derive(Debug)]
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
    //when create raft need set this value for member_change has this self in old log .
    start_index: u64,
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
    notify: (Tx<usize>, Receiver<usize>),
    last_heart: AtomicU64,
    pub store: RaftLog,
    pub replicas: RwLock<Vec<u64>>,
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
    /// id is raft id: it unique in diff raft group
    /// start_index: to add member when raft group exists . set it for playback log not remove self
    /// it first to load it by disk , if not exists use by your set, if you unknow how to set , set it 0
    /// conf is raft config
    /// replicas all members in raft group .
    /// resolver to find addr by node_id
    /// callback for app
    pub async fn new(
        id: u64,
        start_index: u64,
        conf: Arc<Config>,
        replicas: Vec<u64>,
        resolver: RSL,
        sm: SM,
    ) -> RaftResult<Arc<Self>> {
        let store = RaftLog::new(id, conf.clone())?;
        let (term, _, applied) = store.info().await;

        let raft = Arc::new(Raft {
            id,
            node_id: conf.node_id,
            start_index: store.load_start_index_or_def(start_index),
            conf: conf.clone(),
            state: RwLock::new(RaftState::Follower),
            stopd: AtomicBool::new(false),
            term: AtomicU64::new(term),
            voted: Mutex::new(Vote::default()),
            leader: AtomicU64::new(0),
            applied: AtomicU64::new(applied),
            sender: sender::Sender::new(),
            notify: channel(100),
            last_heart: AtomicU64::new(current_millis()),
            store,
            replicas: RwLock::new(replicas),
            resolver,
            sm,
        });

        for node_id in &*raft.replicas.read().await {
            raft.sender.add_peer(*node_id, raft.clone()).await;
        }
        Ok(raft)
    }

    pub async fn notify(self: &Raft) {
        self.notify.0.send(0).await;
    }

    async fn notified(self: &Raft) {
        self.notify.1.recv().await.unwrap();
    }

    //this method callbak execute method , if forward_leader is true , it will forward
    pub async fn execute(
        self: &Arc<Raft>,
        cmd: Vec<u8>,
        forward_leader: bool,
    ) -> RaftResult<Vec<u8>> {
        if forward_leader {
            let leader = self.leader.load(SeqCst);
            if self.node_id != leader {
                return self
                    .sender
                    .forward(Entry::ForwardExecute { commond: cmd }.encode(), leader)
                    .await;
            }
        }
        self.sm.execute(&cmd)
    }

    //call this .to submit log , cmd is data ,
    // forward is leader, if true , you can submit data by replica , if false you must call it by leader
    pub async fn submit(self: &Arc<Raft>, cmd: Vec<u8>, forward_leader: bool) -> RaftResult<()> {
        if forward_leader {
            let leader = self.leader.load(SeqCst);
            if self.node_id != leader {
                self.sender
                    .forward(Entry::ForwardSubmit { commond: cmd }.encode(), leader)
                    .await?;
                return Ok(());
            }
        }

        self.commit(Entry::Commit {
            pre_term: self.store.last_term().await,
            term: self.term.load(SeqCst),
            index: 0,
            commond: cmd,
        })
        .await
    }

    //this function only call by leader
    //action in entry::action_type::{ADD, REMOVE}
    pub async fn add_member(self: &Arc<Raft>, node_id: u64) -> RaftResult<()> {
        self.commit(Entry::MemberChange {
            pre_term: self.store.last_term().await,
            term: self.term.load(SeqCst),
            index: 0,
            node_id,
            action: action_type::ADD,
        })
        .await
    }

    //this function only call by leader
    //action in entry::action_type::{ADD, REMOVE}
    pub async fn remove_member(self: &Arc<Raft>, node_id: u64) -> RaftResult<()> {
        self.commit(Entry::MemberChange {
            pre_term: self.store.last_term().await,
            term: self.term.load(SeqCst),
            index: 0,
            node_id,
            action: action_type::REMOVE,
        })
        .await
    }

    //this function only call by leader
    async fn commit(self: &Arc<Raft>, entry: Entry) -> RaftResult<()> {
        if !self.is_leader().await {
            return Err(RaftError::NotLeader(self.leader.load(SeqCst)));
        }

        let index = self.store.commit(entry).await?;

        let e = {
            if let Err(e) = self
                .sender
                .send(self.store.log_mem.read().await.get_uncheck(index).encode())
                .await
            {
                Some(e)
            } else {
                None
            }
        };

        if let Some(e) = e {
            self.store.rollback().await;
            return Err(e);
        }
        self.applied.store(index, SeqCst);
        self.store.save_to_log(index, self).await
    }

    //use this function make sure , index in memory.
    //leader to call this mehtod
    pub async fn apply(self: &Arc<Raft>, entry: &Entry) -> RaftResult<()> {
        match entry {
            Entry::Commit {
                term,
                index,
                commond,
                ..
            } => self.sm.apply_log(*term, *index, commond),
            Entry::LeaderChange {
                term,
                index,
                leader,
                ..
            } => {
                let leader = *leader;
                let term = *term;
                if self.is_leader().await && leader != self.node_id {
                    self.to_follower().await;
                }
                self.leader.store(leader, SeqCst);
                self.term.store(term, SeqCst);
                self.sm.apply_leader_change(term, *index, leader)
            }
            Entry::MemberChange {
                term,
                index,
                node_id,
                action,
                ..
            } => {
                //if change member equal self node id and
                // index before node raft create, skip this log
                let node_id = *node_id;
                if self.start_index > *index && node_id == self.node_id {
                    return Ok(());
                }

                let action = *action;
                let exists = {
                    let replicas = &mut *self.replicas.write().await;
                    let exists = replicas.contains(&node_id);
                    if action == action_type::ADD && !exists {
                        replicas.push(node_id);
                        self.sender.add_peer(node_id, self.clone()).await;
                    }

                    if action == action_type::REMOVE && exists {
                        replicas.retain(|&id| id != node_id);
                        self.sender.remove_peer(node_id).await;
                    }
                    exists
                };

                self.sm
                    .apply_member_change(*term, *index, node_id, action, exists)
            }
            _ => panic!("not support!!!!!!"),
        }
    }

    pub fn get_resolver(&self) -> &RSL {
        &self.resolver
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
        } else if self.is_leader().await {
            self.to_follower().await;
        }
        self.last_heart.store(current_millis(), SeqCst);

        if term == self.store.last_term().await
            && committed == self.store.last_index().await
            && self.store.last_applied().await < applied
        {
            self.applied.store(applied, SeqCst);
            self.notify().await;
        }

        if self.term.load(SeqCst) != term {
            self.leader.store(leader, SeqCst);
            self.term.store(term, SeqCst);
        }

        return Ok(());
    }

    pub async fn vote(&self, leader: u64, term: u64, committed: u64) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            println!("term:{} ---------------------------   {}", self_term, term);
            return Err(RaftError::TermLess);
        }

        if self_term == term && self.store.last_index().await > committed {
            return Err(RaftError::IndexLess(
                self.store.last_index().await,
                committed,
            ));
        }
        self.last_heart.store(current_millis(), SeqCst);

        if self
            .voted
            .lock()
            .await
            .update(leader, term, current_millis() + self.conf.heartbeate_ms)
        {
            Ok(())
        } else {
            Err(RaftError::VoteNotAllow)
        }
    }

    pub async fn leader_change(&self, term: u64, index: u64, leader: u64) -> RaftResult<()> {
        let self_term = self.term.load(SeqCst);
        if self_term > term {
            return Err(RaftError::TermLess);
        } else if self_term < term {
            self.term.store(term, SeqCst);
        }
        self.last_heart.store(current_millis(), SeqCst);

        if self.store.last_applied().await < index {
            self.notify().await;
        }

        self.leader.store(leader, SeqCst);

        let _ = self.sm.apply_leader_change(term, index, leader);

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

    pub fn leader(&self) -> u64 {
        return self.leader.load(SeqCst);
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
            return Err(RaftError::IndexLess(self.store.last_index().await, index));
        }

        if let Entry::Commit { term: t, .. } = self.store.log_mem.read().await.get_uncheck(index) {
            if *t == term {
                self.applied.store(index, SeqCst);
                self.notify().await;
                return Ok(());
            } else if *t < term {
                return Err(RaftError::TermLess);
            } else {
                return Err(RaftError::TermGreater);
            }
        };
        return Err(RaftError::IndexLess(self.store.last_index().await, index));
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

            return Ok(true);
        }
    }

    //put empty log
    async fn to_leader(self: &Arc<Raft>) -> RaftResult<()> {
        info!("raft_node:{} to leader ", self.node_id);
        {
            let mut state = self.state.write().await;
            if let RaftState::Leader = *state {
                return Ok(());
            };
            *state = RaftState::Leader;
        }
        self.term.fetch_add(1, SeqCst);
        self.last_heart.store(current_millis(), SeqCst);
        self.commit(Entry::LeaderChange {
            pre_term: self.store.last_term().await,
            term: self.term.load(SeqCst),
            index: 0,
            leader: self.node_id,
        })
        .await
    }

    pub async fn to_follower(&self) {
        info!("raft:{} to follower ", self.node_id);
        let mut state = self.state.write().await;
        if let RaftState::Follower = *state {
            return;
        };
        *state = RaftState::Follower;
        self.last_heart.store(current_millis(), SeqCst);
        self.notify().await;
    }

    async fn to_voter(self: &Arc<Raft>, term: u64) -> RaftResult<()> {
        info!("raft:{} to voter ", self.conf.node_id);

        if self.voted.lock().await.update(
            self.conf.node_id,
            term,
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
            .send(
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
        task::spawn(async move {
            while !raft.stopd.load(SeqCst) {
                if raft.is_leader().await {
                    raft.notified().await;
                } else {
                    let mut need_apply = raft.applied.load(SeqCst);
                    if need_apply <= raft.store.last_applied().await {
                        raft.notified().await;
                        continue;
                    }

                    if need_apply > raft.store.last_index().await {
                        if log_enabled!(Debug) {
                            debug!(
                                "need_apply:{} less than raft last_index:{} applied:{}",
                                need_apply,
                                raft.store.last_index().await,
                                raft.store.last_applied().await
                            );
                        }
                        need_apply = raft.store.last_index().await;
                    }
                    if let Err(e) = raft.store.save_to_log(need_apply, &raft).await {
                        error!("store save_to_log has err:{}", e);
                    }
                }
            }
        });

        let raft = self.clone();
        //this job for heartbeat . leader to send , follwer to check heartbeat time
        task::spawn(async move {
            let mut leader_err_time = 0;
            while !raft.stopd.load(SeqCst) {
                if raft.is_leader().await {
                    let (_, committed, applied) = raft.store.info().await;
                    let ie = Entry::Heartbeat {
                        term: raft.term.load(SeqCst),
                        leader: raft.conf.node_id,
                        committed,
                        applied,
                    };
                    if let Err(e) = raft.sender.send(ie.encode()).await {
                        println!("================================={}", leader_err_time);
                        leader_err_time += 1;
                        if leader_err_time > 5 {
                            leader_err_time = 0;
                            raft.to_follower().await;
                        }
                        error!("send heartbeat has err:[{:?}]", e);
                    }
                } else if raft.is_follower().await {
                    if current_millis() - raft.last_heart.load(SeqCst) > raft.conf.heartbeate_ms * 3
                    {
                        info!(
                            "{} too long time receive heartbeat , try to leader",
                            raft.conf.node_id
                        );
                        leader_err_time = 0;
                        //rand sleep to elect
                        let random = rand::thread_rng()
                            .gen_range(raft.conf.heartbeate_ms, raft.conf.heartbeate_ms * 3);
                        task::sleep(Duration::from_millis(random)).await;

                        let term = raft.term.load(SeqCst) + 1;

                        if !raft.is_follower().await
                            || current_millis() - raft.last_heart.load(SeqCst)
                                < raft.conf.heartbeate_ms * 3
                        {
                            continue;
                        }

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
                std::thread::sleep(Duration::from_millis(raft.conf.heartbeate_ms));
            }
        });
    }
}
