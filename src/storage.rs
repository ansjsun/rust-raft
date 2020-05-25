use crate::entity::*;
use crate::error::*;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

static FILE_START: &str = "raft_";
static FILE_END: &str = ".log";
const MEM_CAPACITY: usize = 20000;

pub struct RaftLog {
    id: u64,
    conf: Arc<Config>,
    pub log_mem: RwLock<LogMem>,
    log_file: RwLock<LogFile>,
}

impl RaftLog {
    //give a dir to found file index and max index id for Log file
    //file id start from 1
    pub fn new(id: u64, conf: Arc<Config>) -> RaftResult<Self> {
        let dir = Path::new(&conf.log_path).join(format!("{}", id));
        if !dir.exists() {
            conver(fs::create_dir_all(&dir))?;
        }

        let file_id = conver(fs::read_dir(&dir))?
            .map(|r| r.unwrap().path())
            .filter(|p| !p.is_dir())
            .filter(|p| {
                let name = p.to_str().unwrap();
                name.starts_with(FILE_START) && name.ends_with(FILE_END)
            })
            .map(|p| {
                let name = p.to_str().unwrap();
                name[5..name.len() - 4].parse::<u64>().unwrap_or(0)
            })
            .max()
            .unwrap_or(0);

        let log_file: LogFile;
        let log_mem: LogMem;

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
                        Entry::Commit { index, .. } => index,
                        _ => panic!("log type has err"),
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
        })
    }

    pub fn info(&self) -> (u64, u64, u64) {
        let mem = self.log_mem.read().unwrap();
        (mem.term, mem.committed, mem.applied)
    }

    pub fn last_index(&self) -> u64 {
        self.log_mem.read().unwrap().committed
    }

    pub fn last_applied(&self) -> u64 {
        self.log_mem.read().unwrap().applied
    }

    pub fn commit(&self, e: Entry) -> RaftResult<()> {
        let mut mem = self.log_mem.write().unwrap();
        let (term, index, _) = e.info();
        if mem.term > term {
            return Err(RaftError::TermLess);
        }

        if mem.committed + 1 < index {
            return Err(RaftError::IndexLess(mem.committed));
        } else if mem.committed + 1 > index {
            //Indicates that log conflicts need to be rolled back
            let new_len = (index - 1 - mem.offset) as usize;
            unsafe { mem.logs.set_len(new_len) };
        }

        mem.committed = index;
        mem.term = index;
        mem.logs.push(e);

        Ok(())
    }

    //if this function has err ,Means that raft may not work anymore
    // If an IO error, such as insufficient disk space, the data will be unclean. Or an unexpected error occurred
    pub fn apply(&self, target_applied: u64) -> RaftResult<u64> {
        let (bs, index) = {
            let mem = self.log_mem.read().unwrap();
            if mem.applied >= target_applied {
                return Ok(0);
            }
            match mem.get(mem.applied + 1) {
                Some(e) => {
                    let (_, index, _) = e.info();
                    (e.encode(), index)
                }
                None => return Ok(0),
            }
        };

        let mut file = self.log_file.write().unwrap();

        if let Err(err) = file.writer.write(&u32::to_be_bytes(bs.len() as u32)) {
            return Err(RaftError::IOError(err.to_string()));
        }

        if let Err(err) = file.writer.write(&bs) {
            return Err(RaftError::IOError(err.to_string()));
        }

        self.log_mem.write().unwrap().applied = index;

        Ok(index)
    }
}

pub struct LogMem {
    pub offset: u64,
    term: u64,
    committed: u64,
    applied: u64,
    logs: Vec<Entry>,
    mem_len: u64,
}

impl LogMem {
    fn new(capacity: usize, index: u64) -> LogMem {
        return LogMem {
            logs: Vec::with_capacity(capacity),
            offset: index,
            term: 0,
            committed: index,
            applied: index,
            mem_len: 0,
        };
    }

    pub fn get(&self, index: u64) -> Option<&Entry> {
        return self.logs.get((index - self.offset - 1) as usize);
    }
}

struct LogFile {
    file_id: u64,
    file_len: u64,
    writer: io::BufWriter<fs::File>,
}

impl LogFile {
    fn new(dir: PathBuf, file_id: u64, offset: u64) -> RaftResult<LogFile> {
        let file_path = dir.join(format!("{}{}{}", FILE_START, file_id, FILE_END));

        let mut file = conver(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .write(true)
                .open(file_path),
        )?;
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
