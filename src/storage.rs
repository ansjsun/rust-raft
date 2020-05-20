use crate::entity::*;
use crate::error::*;
use crate::state_machine::StateMachine;
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

static offset_file_name: &str = ".offset";

pub struct RaftLog {
    id: u64,
    conf: Arc<Config>,
    logs: VecDeque<Entry>,
    offset: usize,
    committed: usize,
    applied: usize,
    file_id: u64,
    sm: Arc<dyn StateMachine + Sync + Send>,
    log_writer: io::BufWriter<fs::File>,
}

impl RaftLog {
    pub fn new(
        id: u64,
        conf: Arc<Config>,
        capacity: usize,
        sm: Arc<dyn StateMachine + Sync + Send>,
    ) -> RaftResult<Self> {
        let dir = Path::new(&conf.log_path).join(format!("{}", id));

        if !dir.exists() {
            conver(fs::create_dir_all(&dir))?;
        }

        let offset_path = dir.clone().join(offset_file_name);

        let mut file_id = 0;
        let mut offset = 0;

        if offset_path.exists() {
            let bs = conver(fs::read(offset_path))?;
            let mut f: [u8; 8] = Default::default();
            f.copy_from_slice(&bs[..8]);
            file_id = u64::from_be_bytes(f);
            f.copy_from_slice(&bs[8..]);
            offset = u64::from_be_bytes(f);
        }

        let file_path = dir.clone().join(format!("raft_{}.id", file_id));
        let mut input = conver(fs::File::open(file_path))?;
        if offset > 0 {
            conver(input.seek(io::SeekFrom::Start(offset)))?;
        }

        Ok(RaftLog {
            id: id,
            conf: conf,
            logs: VecDeque::with_capacity(capacity),
            offset: 0,
            committed: 0,
            applied: 0,
            sm: sm,
            file_id: file_id,
            log_writer: io::BufWriter::new(input),
        })
    }

    pub fn save(&self, e: Entry) {
        self.logs.push_back(e);
    }
}
