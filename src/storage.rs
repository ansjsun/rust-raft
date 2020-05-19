use crate::entity::Message;
use std::collections::VecDeque;
pub struct RaftLog {
    logs: VecDeque<Message>,
    begin_index: usize,
    commit_index: usize,
    last_applied: usize,
    sm: Arc<StateMachine + Sync + Send>,
}

impl RaftLog {
    fn new(capacity: usize, sm: Arc<StateMachine + Sync + Send> ) -> Self {
        RaftLog {
            logs: VecDeque::with_capacity(capacity),
            sm:sm,
            Default..
        }
    }


    
}
