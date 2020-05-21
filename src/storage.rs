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
const MEM_CAPACITY: usize = 20000;

pub struct RaftLog {
    id: u64,
    conf: Arc<Config>,
    log_mem: RwLock<LogMem>,
    log_file: RwLock<LogFile>,
    sm: Arc<dyn StateMachine + Sync + Send>,
}

impl RaftLog {
    //give a dir to found file index and max index id for Log file
    //file id start from 1
    pub fn new(
        id: u64,
        conf: Arc<Config>,
        sm: Arc<dyn StateMachine + Sync + Send>,
    ) -> RaftResult<Self> {
        let dir = Path::new(&conf.log_path).join(format!("{}", id));

        if !dir.exists() {
            conver(fs::create_dir_all(&dir))?;
        }

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

        let mut log_file: LogFile;
        let mut log_mem: LogMem;

        if file_id == 0 {
            //mean's new file
            log_file = LogFile::new(dir.clone(), 1, 0)?;
            log_mem = LogMem::new(MEM_CAPACITY, 0);
        } else {
            let file_path = dir.join(format!("raft_{}.id", file_id));
            let mut file = conver(fs::File::open(file_path))?;

            let mut offset: u64 = 0;
            let mut len = file.metadata().unwrap().len();
            let mut pre_offset: u64 = 0;
            let mut index = 0;
            loop {
                if len == 0 {
                    log_file = LogFile::new(dir.clone(), file_id, len)?;
                    log_mem = LogMem::new(MEM_CAPACITY, file_id - 1);
                    break;
                }
                let dl = read_u64(&mut file)?;
                if len == offset + dl {
                    let mut buf = Vec::with_capacity(dl as usize);
                    conver(file.read(&mut buf))?;
                    let index = match Entry::decode(buf)? {
                        Entry::Log { index, .. } => index,
                    };
                    log_mem = LogMem::new(MEM_CAPACITY, index);
                    log_file = LogFile::new(dir.clone(), file_id, len)?;
                    break;
                } else if len < offset + dl {
                    len = offset;
                    conver(file.seek(io::SeekFrom::Start(pre_offset)))?;
                } else {
                    pre_offset = offset;
                    offset += dl;
                }
            }
        }

        Ok(RaftLog {
            id: id,
            conf: conf,
            log_mem: RwLock::new(log_mem),
            log_file: RwLock::new(log_file),
            sm: sm,
        })
    }

    pub fn save(&self, e: Entry) -> RaftResult<()> {
        let mut mem = self.log_mem.write().unwrap();
        let (term, index, len) = e.info();
        if mem.term > term {
            return Err(RaftError::TermLess);
        }

        if mem.committed + 1 < index {
            return Err(RaftError::IndexLess(mem.committed));
        } else if mem.committed + 1 > index {
            //Indicates that log conflicts need to be rolled back
            let at = index - mem.offset - 1;
            let _ = mem.queue.split_off(at as usize);
        }

        mem.committed = index;
        mem.term = index;
        mem.queue.push_back(e);

        Ok(())
    }

    pub fn move_to_file(&self) -> RaftResult<()> {
        {
            let mem = self.log_mem.read().unwrap();
            let first = mem.queue.front();
            let e = match first {
                Some(e) => e,
                None => return Ok(()),
            };

            let (_, index, len) = e.info();
            if index > mem.applied {
                return Ok(());
            }

            let bs = e.ecode();

            let mut file = self.log_file.write().unwrap();

            if let Err(err) = file.writer.write(&bs) {
                return Err(RaftError::IOError(err.to_string()));
            }
            file.file_len += len;
        }

        {
            self.log_mem.write().unwrap().queue.pop_front();
        }

        Ok(())
    }
}

struct LogMem {
    offset: u64,
    term: u64,
    committed: u64,
    applied: u64,
    queue: VecDeque<Entry>,
    mem_len: u64,
}

impl LogMem {
    fn new(capacity: usize, index: u64) -> LogMem {
        return LogMem {
            queue: VecDeque::with_capacity(capacity),
            offset: index,
            term: 0,
            committed: index,
            applied: index,
            mem_len: 0,
        };
    }
}

struct LogFile {
    file_id: u64,
    file_len: u64,
    writer: io::BufWriter<fs::File>,
}

impl LogFile {
    fn new(dir: PathBuf, file_id: u64, offset: u64) -> RaftResult<LogFile> {
        let file_path = dir.join(format!("raft_{}.id", file_id));
        let mut file = conver(fs::File::open(file_path))?;
        if offset > 0 {
            conver(file.seek(io::SeekFrom::Start(offset)))?;
        }
        Ok(LogFile {
            file_id: file_id,
            file_len: offset,
            writer: io::BufWriter::new(file),
        })
    }
}

fn read_u64(file: &mut fs::File) -> RaftResult<u64> {
    let mut output = [0u8; 8];
    conver(file.read(&mut output[..]))?;
    Ok(u64::from_be_bytes(output))
}
