use crate::entity::*;
use crate::error::*;
use crate::state_machine::StateMachine;
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

static file_start: &str = "raft_";
static file_end: &str = ".log";

pub struct RaftLog {
    id: u64,
    conf: Arc<Config>,
    log_mem: RwLock<LogMem>,
    log_file: RwLock<LogFile>,
    sm: Arc<dyn StateMachine + Sync + Send>,
}

impl RaftLog {
    pub fn new(
        id: u64,
        conf: Arc<Config>,
        sm: Arc<dyn StateMachine + Sync + Send>,
    ) -> RaftResult<Self> {
        let dir = Path::new(&conf.log_path).join(format!("{}", id));

        if !dir.exists() {
            conver(fs::create_dir_all(&dir))?;
        }

        Ok(RaftLog {
            id: id,
            conf: conf,
            log_mem: RwLock::new(LogMem::new(20000)),
            log_file: RwLock::new(LogFile::new(dir)?),
            sm: sm,
        })
    }

    pub fn save(&self, e: Entry) {
        let mut mem = self.log_mem.write().unwrap();
        mem.len += e.len();
        mem.queue.push_back(e);
    }

    pub fn move_to_file(&self) -> RaftResult<()> {
        match self.log_mem.read().unwrap().queue.front() {
            Some(e) => {
                if e.index() > self.applied {
                    return Ok(());
                }
            }
            None => return Ok(()),
        }

        let e = { self.log_mem.write().unwrap().queue.pop_front() };
        if let Some(e) = e {
            let bs = e.ecode();
            if let Err(err) = self.log_file.write().unwrap().writer.write(&bs) {
                //if write has err rollback
                self.log_mem.write().unwrap().queue.push_front(e);
                return Err(RaftError::IOError(err.to_string()));
            }
        }

        Ok(())
    }
}

struct LogMem {
    committed: u64,
    applied: u64,
    queue: VecDeque<Entry>,
    mem_len: u64,
}

impl LogMem {
    fn new(capacity: usize) -> LogMem {
        return LogMem {
            queue: VecDeque::with_capacity(capacity),
            mem_len: 0,
        };
    }
}

struct LogFile {
    file_id: u64,
    writer: io::BufWriter<fs::File>,
}

impl LogFile {
    //give a dir to found file index and max index id for Log file
    //file id start from 1
    fn new(dir: PathBuf) -> RaftResult<LogFile> {
        let file_id = conver(fs::read_dir(&dir))?
            .map(|r| r.unwrap().path())
            .filter(|p| !p.is_dir())
            .filter(|p| {
                let name = p.to_str().unwrap();
                name.starts_with(file_start) && name.ends_with(file_end)
            })
            .map(|p| {
                let name = p.to_str().unwrap();
                name[5..name.len() - 4].parse::<u64>().unwrap_or(0)
            })
            .max()
            .unwrap_or(0);

        let file_path = dir.clone().join(format!("raft_{}.id", file_id));
        let mut file = conver(fs::File::open(file_path))?;

        if file_id == 0 {
            //mean's new file
            return Ok(Self::_new(1, 0, 0, file));
        }

        let mut offset: u64 = 0;
        let mut len = file.metadata().unwrap().len();
        let mut pre_offset: u64 = 0;

        loop {
            if len == 0 {
                break;
            }
            let dl = read_u64(&mut file)?;
            if len == offset + dl {
                let mut buf = Vec::with_capacity(dl as usize);
                conver(file.read(&mut buf))?;
                let index = match Entry::decode(buf)? {
                    Entry::Log { index, .. } => index,
                };

                return Ok(Self::_new(file_id, index, index, file));
            } else if len < offset + dl {
                len = offset;
                conver(file.seek(io::SeekFrom::Start(pre_offset)))?;
            } else {
                pre_offset = offset;
                offset += dl;
            }
        }

        return Ok(Self::_new(1, 0, 0, file));
    }

    fn _new(file_id: u64, offset: u64, index: u64, file: fs::File) -> LogFile {
        return LogFile {
            offset: offset,
            committed: index,
            applied: index,
            file_id: file_id,
            writer: io::BufWriter::new(file),
        };
    }
}

fn read_u64(file: &mut fs::File) -> RaftResult<u64> {
    let mut output = [0u8; 8];
    conver(file.read(&mut output[..]))?;
    Ok(u64::from_be_bytes(output))
}
