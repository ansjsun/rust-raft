use crate::entity::*;
use crate::error::*;
use crate::state_machine::SM;
use log::warn;
use smol::Async;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

static FILE_START: &str = "raft_";
static FILE_END: &str = ".log";
static BUF_SIZE: usize = 1024 * 1024;

pub struct RaftLog {
    _id: u64,
    conf: Arc<Config>,
    pub log_mem: RwLock<LogMem>,
    log_file: RwLock<LogFile>,
    lock_truncation: Mutex<usize>,
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
            .filter_map(|r| {
                let path = r.unwrap().path();
                if path.is_file() {
                    Some(path.file_name().unwrap().to_str().unwrap().to_string())
                } else {
                    None
                }
            })
            .filter(|name| name.starts_with(FILE_START) && name.ends_with(FILE_END))
            .map(|name| name[5..name.len() - 4].parse::<u64>().unwrap())
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

            let (offset, entry) =
                LogFile::read_last_entry(dir.clone(), file_ids[last_index], false)?;
            let (term, index, _) = entry.info();
            if index > 0 {
                log_file = LogFile::new(dir.clone(), offset, file_ids)?;
                log_mem = LogMem::new(conf.log_max_num, term, index);
            } else if file_ids[last_index] == 1 {
                log_file = LogFile::new(dir.clone(), 0, file_ids)?;
                log_mem = LogMem::new(conf.log_max_num, term, index);
            } else {
                warn!("first log file is invalidate so use the 2th log file");

                let (_, entry) =
                    LogFile::read_last_entry(dir.clone(), file_ids[last_index - 2], true)?;
                let (term, index, _) = entry.info();
                log_file = LogFile::new(dir.clone(), 0, file_ids)?;
                log_mem = LogMem::new(conf.log_max_num, term, index);
            }
        }

        Ok(RaftLog {
            _id: id,
            conf: conf,
            log_mem: RwLock::new(log_mem),
            log_file: RwLock::new(log_file),
            lock_truncation: Mutex::new(0),
        })
    }

    pub fn iter(
        &self,
        mut index: u64,
        mut execute: impl FnMut(Vec<u8>) -> RaftResult<bool>,
    ) -> RaftResult<()> {
        let _v = self.lock_truncation.lock().unwrap();

        if self.log_mem.read().unwrap().offset >= index {
            let ids = self.log_file.read().unwrap().file_ids.clone();
            let dir = self.log_file.read().unwrap().dir.clone();

            let mut offset = 0;
            for id in &ids {
                let id = *id;
                if index > id {
                    continue;
                }
                let (o, e) = LogFile::read_last_entry(dir.clone(), id, true)?;
                let (_, i, _) = e.info();
                if i == 0 {
                    return Err(RaftError::LogFileInvalid(id));
                }
                offset = o - 4;
            }

            for id in ids {
                if index > id {
                    continue;
                }
                let file = conver(
                    fs::OpenOptions::new()
                        .read(true)
                        .open(dir.join(format!("{}{}{}", FILE_START, id, FILE_END))),
                )?;

                let file_len = file.metadata().unwrap().len();

                let mut file = io::BufReader::new(file);
                if offset > 0 {
                    conver(file.seek(io::SeekFrom::Start(offset)))?;
                }
                loop {
                    let dl = read_u32(&mut file)? as u64;
                    offset += 4;
                    let mut buf = vec![0; dl as usize];
                    conver(file.read_exact(&mut buf))?;
                    execute(buf)?;
                    offset += dl;
                    if offset == file_len {
                        break;
                    }
                }
                offset = 0;
            }
        }

        while index < self.last_applied() {
            let v = self.log_mem.read().unwrap().get(index).encode();
            if !execute(v)? {
                return Ok(());
            }
            index += 1;
        }
        Ok(())
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

    pub fn last_term(&self) -> u64 {
        self.log_mem.read().unwrap().term
    }

    //this method to store entry to mem  by vec .
    //if vec length gather conf max log num  , it will truncation to min log num , but less than apllied index
    pub fn commit(
        &self,
        pre_term: u64,
        term: u64,
        mut index: u64,
        cmd: Vec<u8>,
    ) -> RaftResult<u64> {
        let mut mem = self.log_mem.write().unwrap();

        if mem.term > term {
            return Err(RaftError::TermLess);
        }

        if index == 0 {
            index = mem.committed + 1;
        } else if index > mem.committed + 1 {
            return Err(RaftError::IndexLess(mem.committed));
        } else if index < mem.committed + 1 {
            if index == mem.committed && term == mem.term {
                return Ok(index);
            }
            //Indicates that log conflicts need to be rolled back
            let new_len = (index - mem.offset) as usize;
            unsafe { mem.logs.set_len(new_len) };
        }

        if mem.term != pre_term {
            //if not same pre term , means last entry is invalided . rolled bak
            let new_len = mem.committed as usize - 1;
            unsafe { mem.logs.set_len(new_len) };
            let (term, index, _) = mem.get(mem.committed - 1).info();
            mem.committed = index;
            mem.term = term;
            return Err(RaftError::IndexLess(index));
        }

        mem.logs.push(Entry::Commit {
            pre_term: pre_term,
            term: term,
            index: index,
            commond: cmd,
        });

        mem.committed = index;
        mem.term = term;

        if mem.logs.len() >= self.conf.log_max_num {
            let keep_num = usize::max(self.conf.log_min_num, (index - mem.applied) as usize);
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
    //return result<has_next>
    pub fn apply(&self, sm: &SM, target_applied: u64) -> RaftResult<()> {
        let (index, apply_result) = {
            let mem = self.log_mem.read().unwrap();
            if mem.applied >= target_applied {
                return Ok(());
            }

            if mem.committed < target_applied {
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

            conver(file.writer.flush())?;

            file.offset = file.offset + bs.len() as u64;

            if file.offset >= self.conf.log_file_size_mb * 1024 * 1024 {
                file.log_rolling(index + 1)?;
            }

            (
                index,
                match entry {
                    Entry::Commit {
                        term,
                        index,
                        commond,
                        ..
                    } => sm.apply(term, index, commond),
                    _ => panic!("not support!!!!!!"),
                },
            )
        };

        self.log_mem.write().unwrap().applied = index;

        apply_result
    }
}
// term , committed stands last entry info.
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
    dir: PathBuf,
    file_ids: Vec<u64>,
    writer: io::BufWriter<fs::File>,
}

impl LogFile {
    //new logfile by ids, the ids is file list
    fn new(dir: PathBuf, offset: u64, ids: Vec<u64>) -> RaftResult<LogFile> {
        let file_path = dir.join(format!("{}{}{}", FILE_START, ids[ids.len() - 1], FILE_END));
        let mut writer = io::BufWriter::with_capacity(
            BUF_SIZE,
            conver(
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .read(true)
                    .write(true)
                    .open(file_path),
            )?,
        );

        if offset > 0 {
            conver(writer.seek(io::SeekFrom::Start(offset)))?;
        }

        Ok(LogFile {
            offset: offset,
            dir: dir,
            file_ids: ids,
            writer: writer,
        })
    }

    // read last entry by file , it return value Option<entry start offeset , entry>
    // if read dir is empty , it allways return zero entry
    // if file_id is not exists , it return LogFileNotFound err value is file_id
    //validate means the file complete, the last log file may be not complete,  but the penultimate must complete
    fn read_last_entry(dir: PathBuf, file_id: u64, validate: bool) -> RaftResult<(u64, Entry)> {
        let file_path = dir.join(format!("{}{}{}", FILE_START, file_id, FILE_END));
        if !file_path.exists() {
            if file_id == 0 {
                return Ok((
                    0,
                    Entry::Commit {
                        pre_term: 0,
                        term: 0,
                        index: 0,
                        commond: Vec::default(),
                    },
                ));
            } else {
                return Err(RaftError::LogFileNotFound(file_id));
            }
        }
        let file = conver(fs::OpenOptions::new().read(true).open(&file_path))?;

        let mut offset: u64 = 0;
        let mut len = file.metadata().unwrap().len();
        let mut pre_offset: u64 = 0;

        let mut file = io::BufReader::new(file);

        loop {
            if len == 0 {
                return Ok((
                    0,
                    Entry::Commit {
                        pre_term: 0,
                        term: 0,
                        index: 0,
                        commond: Vec::default(),
                    },
                ));
            }

            if len - offset <= 4 {
                len = offset - 4;
                offset = pre_offset;
                conver(file.seek(io::SeekFrom::Start(offset)))?;
                // if file has  dirty data, change to valid length
                conver(
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&file_path)
                        .unwrap()
                        .set_len(len),
                )?;
                continue;
            }

            let dl = read_u32(&mut file)? as u64;
            offset += 4;

            if len == offset + dl {
                let mut buf = vec![0; dl as usize];
                file.read_exact(&mut buf).unwrap();
                return Ok((offset, Entry::decode(&buf)?));
            } else if len < offset + dl {
                if validate {
                    return Err(RaftError::LogFileInvalid(file_id));
                }
                len = offset - 4;
                offset = pre_offset;
                conver(file.seek(io::SeekFrom::Start(offset)))?;
                // if file has  dirty data, change to valid length
                conver(
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&file_path)
                        .unwrap()
                        .set_len(len),
                )?;
            } else {
                conver(file.seek(io::SeekFrom::Current(dl as i64)))?;
                pre_offset = offset - 4;
                offset += dl;
            }
        }
    }

    fn log_rolling(&mut self, new_id: u64) -> RaftResult<()> {
        conver(self.writer.flush())?;

        let file = conver(
            fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(
                    self.dir
                        .join(format!("{}{}{}", FILE_START, new_id, FILE_END)),
                ),
        )?;
        self.writer = io::BufWriter::with_capacity(BUF_SIZE, file);
        self.file_ids.push(new_id);
        self.offset = 0;

        Ok(())
    }
}

fn read_u32(file: &mut io::BufReader<fs::File>) -> RaftResult<u32> {
    let mut output = [0u8; 4];
    conver(file.read_exact(&mut output[..]))?;
    Ok(u32::from_be_bytes(output))
}

pub fn validate_log_file(path: std::path::PathBuf, check: bool) -> RaftResult<u64> {
    let file = conver(fs::OpenOptions::new().read(true).open(&path))?;
    let mut offset: usize = 0;
    let len = file.metadata().unwrap().len();

    let mut file = io::BufReader::new(file);
    let name = path.file_name().unwrap().to_str().unwrap().to_string();
    let mut pre_index = name[5..name.len() - 4].parse::<u64>().unwrap() - 1;

    loop {
        if len as usize == offset {
            return Ok(pre_index);
        }

        let dl = read_u32(&mut file)? as u64;
        offset += 4;
        if dl < 4 {
            return Err(RaftError::Error(format!("lenth is err:{}", dl)));
        }
        let mut buf = vec![0; dl as usize];
        conver(file.read_exact(&mut buf))?;
        offset += buf.len();

        let e = Entry::decode(&buf)?;
        pre_index += 1;
        if check {
            let (_, index, _) = e.info();
            if check && pre_index != index {
                return Err(RaftError::Error(format!(
                    "id is not continuous pre:{} now:{}",
                    pre_index, index
                )));
            }
        } else {
            println!("{:?}", e);
        }
    }
}

#[test]
fn test_validate_log_file() {
    let raft_id = 1;
    let file_id = 1;
    let path = Path::new("example/data/raft1")
        .join(format!("{}", raft_id))
        .join(format!("{}{}{}", FILE_START, file_id, FILE_END));
    println!("dir....{:?}", path);
    println!(
        "result:.................{}",
        validate_log_file(path, false).unwrap()
    );
}
