use crate::entity::*;
use crate::error::*;
use crate::state_machine::SM;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

static FILE_START: &str = "raft_";
static FILE_END: &str = ".log";

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

        let mut file_ids: Vec<u64> = conver(fs::read_dir(&dir))?
            .map(|r| r.unwrap().path())
            .filter(|p| !p.is_dir())
            .filter(|p| {
                let name = p.to_str().unwrap();
                println!("{}", name);
                name.starts_with(FILE_START) && name.ends_with(FILE_END)
            })
            .map(|p| {
                let name = p.to_str().unwrap();
                name[5..name.len() - 4].parse::<u64>().unwrap_or(0)
            })
            .collect();

        file_ids.sort_by(|a, b| a.cmp(b));

        let log_file: LogFile;
        let log_mem: LogMem;

        if file_ids.len() == 0 {
            //mean's new file
            log_file = LogFile::new(dir.clone(), 0, vec![1])?;
            log_mem = LogMem::new(conf.log_max_num, 0, 0);
        } else {
            let last_index = file_ids.len() - 1;

            if let Some((offset, entry)) =
                LogFile::read_last_entry(dir.clone(), file_ids[last_index], false)?
            {
                println!("{},..............{:?}", offset, entry);
                panic!("123");
                let (term, index, _) = entry.info();
                log_file = LogFile::new(dir.clone(), offset, file_ids)?;
                log_mem = LogMem::new(conf.log_max_num, term, index);
            } else {
                match LogFile::read_last_entry(dir.clone(), file_ids[last_index - 2], true)? {
                    Some((_, entry)) => {
                        let (term, index, _) = entry.info();
                        log_file = LogFile::new(dir.clone(), 0, file_ids)?;
                        log_mem = LogMem::new(conf.log_max_num, term, index);
                    }
                    None => return Err(RaftError::LogFileInvalid(file_ids[last_index - 2])),
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

    //this method to store entry to mem  by vec .
    //if vec length gather conf max log num  , it will truncation to min log num , but less than apllied index
    pub fn commit(&self, term: u64, mut index: u64, cmd: Vec<u8>) -> RaftResult<u64> {
        let mut mem = self.log_mem.write().unwrap();

        if mem.term > term {
            return Err(RaftError::TermLess);
        }

        if index == 0 {
            index = mem.committed + 1;
        } else {
            if mem.committed + 1 < index {
                return Err(RaftError::IndexLess(mem.committed));
            } else if mem.committed + 1 > index {
                //Indicates that log conflicts need to be rolled back
                let new_len = (index - 1 - mem.offset) as usize;
                unsafe { mem.logs.set_len(new_len) };
            }
        }

        mem.committed = index;
        mem.term = term;
        mem.logs.push(Entry::Commit {
            term: term,
            index: index,
            commond: cmd,
        });

        if mem.logs.len() >= self.conf.log_max_num {
            let keep_num = usize::max(self.conf.log_min_num, (index - mem.applied + 1) as usize);
            if mem.logs.len() - keep_num > 10 {
                let off = mem.logs.len() - keep_num;
                mem.logs = mem.logs.split_off(off as usize);
                mem.offset = mem.offset + off as u64;
            }
        }

        Ok(index)
    }

    //if this function has err ,Means that raft may not work anymore
    // If an IO error, such as insufficient disk space, the data will be unclean. Or an unexpected error occurred
    pub fn apply(&self, sm: &SM, target_applied: u64) -> RaftResult<()> {
        let index = {
            let mem = self.log_mem.read().unwrap();
            if mem.applied >= target_applied {
                return Ok(());
            }
            let entry = mem.get(mem.applied + 1);

            let bs = entry.encode();
            let (_, index, _) = entry.info();

            let mut file = self.log_file.write().unwrap();

            if let Err(err) = file.writer.write(&u32::to_be_bytes(bs.len() as u32)) {
                return Err(RaftError::IOError(err.to_string()));
            }

            if let Err(err) = file.writer.write(&bs) {
                return Err(RaftError::IOError(err.to_string()));
            }

            file.offset = file.offset + bs.len() as u64;

            if file.offset >= self.conf.log_file_size_mb * 1024 * 1024 {
                file.log_rolling(
                    Path::new(&self.conf.log_path).join(format!("{}", self.id)),
                    index + 1,
                )?;
            }

            if let Entry::Commit {
                term,
                index,
                commond,
            } = entry
            {
                sm.apply(term, index, commond)?;
            };

            index
        };

        self.log_mem.write().unwrap().applied = index;

        Ok(())
    }
}
// term , commited stands last entry info.
// applied stands last store file log.
pub struct LogMem {
    pub offset: u64,
    term: u64,
    committed: u64,
    applied: u64,
    logs: Vec<Entry>,
}

impl LogMem {
    fn new(capacity: usize, term: u64, index: u64) -> LogMem {
        return LogMem {
            logs: Vec::with_capacity(capacity),
            offset: index,
            term: term,
            committed: index,
            applied: index,
        };
    }

    // if use this function make sure, the log index is exists, if not it will panic.
    pub fn get(&self, index: u64) -> &Entry {
        return self.logs.get((index - self.offset - 1) as usize).unwrap();
    }
}

struct LogFile {
    offset: u64,
    filed_ids: Vec<u64>,
    writer: io::BufWriter<fs::File>,
}

impl LogFile {
    //new logfile by ids, the ids is file list
    fn new(dir: PathBuf, offset: u64, ids: Vec<u64>) -> RaftResult<LogFile> {
        let file_path = dir.join(format!("{}{}{}", FILE_START, ids[ids.len() - 1], FILE_END));

        let mut writer = io::BufWriter::new(conver(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .write(true)
                .open(file_path),
        )?);

        if offset > 0 {
            conver(writer.seek(io::SeekFrom::Start(offset)))?;
        }

        Ok(LogFile {
            offset: offset,
            filed_ids: ids,
            writer: writer,
        })
    }

    // read last entry by file , it return value Option<entry start offeset , entry>
    // if read dir is empty , it allways return zero entry
    // if field_id is not exists , it return LogFileNotFound err value is field_id
    //validate means the file complete, the last log file may be not complete,  but the penultimate must complete
    fn read_last_entry(
        dir: PathBuf,
        field_id: u64,
        validate: bool,
    ) -> RaftResult<Option<(u64, Entry)>> {
        let file_path = dir.join(format!("{}{}{}", FILE_START, field_id, FILE_END));

        if !file_path.exists() {
            if field_id == 0 {
                return Ok(Some((
                    0,
                    Entry::Commit {
                        term: 0,
                        index: 0,
                        commond: Vec::default(),
                    },
                )));
            } else {
                return Err(RaftError::LogFileNotFound(field_id));
            }
        }

        let mut file = conver(fs::OpenOptions::new().read(true).open(file_path))?;

        let mut offset: u64 = 0;
        let mut len = file.metadata().unwrap().len();
        let mut pre_offset: u64 = 0;
        loop {
            if len == 0 {
                return Ok(None);
            }
            let dl = read_u64(&mut file)?;
            if len == offset + dl {
                let mut buf = Vec::with_capacity(dl as usize);
                conver(file.read(&mut buf))?;
                return Ok(Some((offset, Entry::decode(buf)?)));
            } else if len < offset + dl {
                if validate {
                    return Err(RaftError::LogFileInvalid(field_id));
                }
                len = offset;
                conver(file.seek(io::SeekFrom::Start(pre_offset)))?;
            } else {
                pre_offset = offset;
                offset += dl;
            }
        }
    }

    fn log_rolling(&mut self, dir: PathBuf, new_id: u64) -> RaftResult<()> {
        conver(self.writer.flush())?;

        let file = conver(
            fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(dir.join(format!("{}{}{}", FILE_START, new_id, FILE_END))),
        )?;
        self.writer = io::BufWriter::new(file);
        self.filed_ids.push(new_id);
        self.offset = 0;

        Ok(())
    }
}

fn read_u64(file: &mut fs::File) -> RaftResult<u64> {
    let mut output = [0u8; 8];
    conver(file.read(&mut output[..]))?;
    Ok(u64::from_be_bytes(output))
}
