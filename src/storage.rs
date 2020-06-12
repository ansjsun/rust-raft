use crate::entity::*;
use crate::error::*;
use crate::raft::Raft;
use async_std::sync::RwLock;
use log::{error, warn};
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

static FILE_START: &str = "raft_";
static FILE_END: &str = ".log";
static BUF_SIZE: usize = 1024 * 1024;

pub struct RaftLog {
    id: u64,
    conf: Arc<Config>,
    pub log_mem: RwLock<LogMem>,
    log_file: RwLock<LogFile>,
}

pub struct RaftLogIter {
    ids: Option<Vec<u64>>,
    dir: Option<PathBuf>,
    file_index: usize,
    file: Option<fs::File>,
    file_offset: u64,
    current_index: u64,
}

impl RaftLogIter {
    fn file_len(&self) -> u64 {
        self.file.as_ref().unwrap().metadata().unwrap().len()
    }

    pub async fn next(&mut self, raft_log: &RaftLog) -> RaftResult<Option<Vec<u8>>> {
        if self.file.is_some() {
            self.iter_file(raft_log).await
        } else {
            self.iter_mem(raft_log).await
        }
    }

    async fn iter_mem(&mut self, raft_log: &RaftLog) -> RaftResult<Option<Vec<u8>>> {
        if self.current_index <= raft_log.log_mem.read().await.offset {
            return Err(RaftError::OutMemIndex(self.current_index));
        }

        if self.current_index > raft_log.log_mem.read().await.applied {
            return Ok(None);
        }

        let v = raft_log
            .log_mem
            .read()
            .await
            .get(self.current_index)?
            .encode();
        return Ok(Some(v));
    }

    async fn iter_file(&mut self, raft_log: &RaftLog) -> RaftResult<Option<Vec<u8>>> {
        if self.file_len() - self.file_offset <= 4 {
            if self.current_index > raft_log.log_mem.read().await.offset {
                self.file = None;
                return self.iter_mem(raft_log).await;
            }
            let index = self.file_index;
            return Err(RaftError::LogFileInvalid(self.ids()[index]));
        }
        let dl = read_u32(self.file.as_mut().unwrap())? as u64;
        self.file_offset += 4;

        if self.file_offset + dl > self.file_len() {
            if self.current_index > raft_log.log_mem.read().await.offset {
                println!("...................................");
                self.file = None;
                return self.iter_mem(raft_log).await;
            }
            let index = self.file_index;
            return Err(RaftError::LogFileInvalid(self.ids()[index]));
        }
        let mut buf = vec![0; dl as usize];
        convert(self.file().read_exact(&mut buf))?;
        self.current_index += 1;
        self.file_offset += dl;

        if self.file_offset == self.file_len() {
            self.file_index += 1;
            if self.file_index >= self.ids().len() {
                println!("===============================================..");
                self.file = None;
            } else {
                let file = convert(fs::OpenOptions::new().read(true).open(
                    self.dir.as_ref().unwrap().clone().join(format!(
                        "{}{}{}",
                        FILE_START,
                        self.ids.as_ref().unwrap()[self.file_index],
                        FILE_END
                    )),
                ))?;
                self.file = Some(file);
                self.file_offset = 0;
            }
        }

        return Ok(Some(buf));
    }

    fn file(&mut self) -> &mut fs::File {
        self.file.as_mut().unwrap()
    }

    fn ids(&mut self) -> &Vec<u64> {
        self.ids.as_ref().unwrap()
    }
}

impl RaftLog {
    //give a dir to found file index and max index id for Log file
    //file id start from 1
    pub fn new(id: u64, conf: Arc<Config>) -> RaftResult<Self> {
        let dir = Path::new(&conf.log_path).join(format!("{}", id));
        if !dir.exists() {
            convert(fs::create_dir_all(&dir))?;
        }

        let file_ids = RaftLog::file_ids(&dir)?;

        println!("fieldids           {:?}", file_ids);

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
            let (term, index) = entry.info();
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
                let (term, index) = entry.info();
                log_file = LogFile::new(dir.clone(), 0, file_ids)?;
                log_mem = LogMem::new(conf.log_max_num, term, index);
            }
        }

        Ok(RaftLog {
            id: id,
            conf: conf,
            log_mem: RwLock::new(log_mem),
            log_file: RwLock::new(log_file),
        })
    }

    /// search all filed start id , and sorted
    fn file_ids(dir: &PathBuf) -> RaftResult<Vec<u64>> {
        let mut file_ids = vec![];
        fs::read_dir(&dir)
            .unwrap()
            .for_each(|v| println!("{:?}", v));
        for file in convert(fs::read_dir(&dir))? {
            let meta = file.as_ref().unwrap().metadata().unwrap();
            if !meta.is_file() {
                continue;
            }

            if let Some(name) = file.as_ref().unwrap().file_name().to_str() {
                if name.starts_with(FILE_START) && name.ends_with(FILE_END) {
                    file_ids.push(name[5..name.len() - 4].parse::<u64>().unwrap());
                }
            };
        }

        file_ids.sort_by(|a, b| a.cmp(b));
        Ok(file_ids)
    }

    pub async fn validate_log_file(&self, mut start_index: u64) -> RaftResult<()> {
        if start_index == 0 {
            start_index = 1;
        }
        let mut iter = self.iter(start_index).await?;
        let mut pre_index = start_index - 1;
        while let Some(buf) = iter.next(&self).await? {
            let e = Entry::decode(&buf)?;
            pre_index += 1;
            let (_, index) = e.info();
            if pre_index != index {
                return Err(RaftError::Error(format!(
                    "id is not continuous pre:{} now:{}",
                    pre_index, index
                )));
            }
        }
        Ok(())
    }

    //it to instance a intertor , use it first lock_truncation, my be will panic if not lock lock_truncation;
    pub async fn iter(&self, index: u64) -> RaftResult<RaftLogIter> {
        if self.log_mem.read().await.offset >= index {
            let ids = self.log_file.read().await.file_ids.clone();
            let dir = self.log_file.read().await.dir.clone();
            let start_index = match ids.binary_search(&index) {
                Ok(i) => i,
                Err(i) => {
                    if i == 0 {
                        return Err(RaftError::LogFileNotFound(index));
                    } else {
                        i - 1
                    }
                }
            };

            let id = ids[start_index];
            let mut file = convert(
                fs::OpenOptions::new()
                    .read(true)
                    .open(dir.join(format!("{}{}{}", FILE_START, id, FILE_END))),
            )?;
            let file_len = file.metadata().unwrap().len();
            let mut offset = 0;
            if id == index {
                return Ok(RaftLogIter {
                    ids: Some(ids),
                    dir: Some(dir),
                    file_index: start_index,
                    file: Some(file),
                    file_offset: offset,
                    current_index: index,
                });
            }

            for _i in 0..index - id {
                let dl = read_u32(&mut file)? as u64;
                offset += 4;
                if offset + dl > file_len {
                    return Err(RaftError::LogFileInvalid(id));
                }
                convert(file.seek(io::SeekFrom::Current(dl as i64)))?;
                offset += dl;
                if offset + 4 > file_len {
                    return Err(RaftError::LogFileInvalid(id));
                }
            }

            return Ok(RaftLogIter {
                ids: Some(ids),
                dir: Some(dir),
                file_index: start_index,
                file: Some(file),
                file_offset: offset,
                current_index: index,
            });
        }

        Ok(RaftLogIter {
            ids: None,
            dir: None,
            file_index: 0,
            file: None,
            file_offset: 0,
            current_index: index,
        })
    }

    pub async fn info(&self) -> (u64, u64, u64) {
        let mem = self.log_mem.read().await;
        (mem.term, mem.committed, mem.applied)
    }

    pub async fn last_index(&self) -> u64 {
        self.log_mem.read().await.committed
    }

    pub async fn last_applied(&self) -> u64 {
        self.log_mem.read().await.applied
    }

    pub async fn last_term(&self) -> u64 {
        self.log_mem.read().await.term
    }

    pub fn load_start_index_or_def(&self, start_index: u64) -> u64 {
        let path = Path::new(&self.conf.log_path)
            .join(format!("{}", self.id))
            .join("start_index");
        if let Ok(v) = fs::read(&path) {
            if v.len() == 8 {
                let mut dst = [0u8; 8];
                dst.clone_from_slice(&v);
                return u64::from_be_bytes(dst);
            }
        }
        fs::write(&path, u64::to_be_bytes(start_index)).expect("write start index has err");
        start_index
    }

    // pre_term: u64,
    // term: u64,
    // mut index: u64,
    // cmd: Vec<u8>,
    //this method to store entry to mem  by vec .
    //if vec length gather conf max log num  , it will truncation to min log num , but less than apllied index
    pub async fn commit(&self, mut entry: Entry) -> RaftResult<u64> {
        let mut mem = self.log_mem.write().await;

        let (pre_term, term, mut new_index) = entry.commit_info();

        if mem.term > term {
            return Err(RaftError::TermLess);
        }

        if new_index == 0 {
            new_index = mem.committed + 1;
            match entry {
                Entry::Commit { ref mut index, .. }
                | Entry::MemberChange { ref mut index, .. }
                | Entry::LeaderChange { ref mut index, .. } => {
                    *index = new_index;
                }
                _ => panic!("not support this type:{:?}", entry),
            }
        } else if new_index > mem.committed + 1 {
            return Err(RaftError::IndexLess(mem.committed, new_index));
        } else if new_index < mem.committed + 1 {
            if new_index == mem.committed && term == mem.term {
                return Ok(new_index);
            }
            //Indicates that log conflicts need to be rolled back
            let new_len = (new_index - mem.offset) as usize;
            unsafe { mem.logs.set_len(new_len) };
        }

        if mem.term != pre_term {
            //if not same pre term , means last entry is invalided . rolled bak
            let new_len = mem.committed as usize - 1;
            unsafe { mem.logs.set_len(new_len) };
            let (term, index) = mem.get_uncheck(mem.committed - 1).info();
            mem.committed = index;
            mem.term = term;
            return Err(RaftError::IndexLess(index, new_index));
        }

        mem.logs.push(entry);

        mem.committed = new_index;
        mem.term = term;

        if mem.logs.len() >= self.conf.log_max_num {
            let keep_num = usize::max(self.conf.log_min_num, (new_index - mem.applied) as usize);
            if mem.logs.len() - keep_num > 10 {
                let off = mem.logs.len() - keep_num;
                mem.logs = mem.logs.split_off(off as usize);
                mem.offset = mem.offset + off as u64;
            }
        }

        Ok(new_index)
    }

    pub async fn rollback(&self) {
        let mut mem = self.log_mem.write().await;
        mem.committed = mem.pre_committed;
        mem.term = mem.pre_term;
        let last = mem.logs.len() - 1;
        mem.logs.remove(last);
    }

    //if this function has err ,Means that raft may not work anymore
    // If an IO error, such as insufficient disk space, the data will be unclean. Or an unexpected error occurred
    pub async fn save_to_log(&self, target_applied: u64, raft: &Arc<Raft>) -> RaftResult<()> {
        let index = {
            let mem = self.log_mem.read().await;
            if mem.applied >= target_applied || mem.committed < target_applied {
                return Ok(());
            }
            let entry = mem.get_uncheck(mem.applied + 1);
            let bs = entry.encode();
            let (_, index) = entry.info();
            let mut file = self.log_file.write().await;
            if let Err(err) = file.writer.write(&u32::to_be_bytes(bs.len() as u32)) {
                return Err(RaftError::IOError(err.to_string()));
            }
            if let Err(err) = file.writer.write(&bs) {
                return Err(RaftError::IOError(err.to_string()));
            }
            convert(file.writer.flush())?;
            file.offset = file.offset + bs.len() as u64;
            if file.offset >= self.conf.log_file_size_mb * 1024 * 1024 {
                file.log_rolling(index + 1)?;
            }

            if let Err(e) = raft.apply(entry).await {
                error!("apply index:{} has err:{}", index, e);
            }

            index
        };
        self.log_mem.write().await.applied = index;
        Ok(())
    }
}
// term , committed stands last entry info.
// applied stands last store file log.
pub struct LogMem {
    pub offset: u64,
    term: u64,
    pre_term: u64,
    committed: u64,
    pre_committed: u64,
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
            pre_term: term,
            pre_committed: index,
            applied: index,
        };
    }

    // if use this function make sure, the log index is exists, if not it will panic.
    pub fn get_uncheck(&self, index: u64) -> &Entry {
        return self.logs.get((index - self.offset - 1) as usize).unwrap();
    }

    pub fn get(&self, index: u64) -> RaftResult<&Entry> {
        match self.logs.get((index - self.offset - 1) as usize) {
            Some(e) => Ok(e),
            None => Err(RaftError::OutMemIndex(index)),
        }
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
            convert(
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .read(true)
                    .write(true)
                    .open(file_path),
            )?,
        );

        if offset > 0 {
            convert(writer.seek(io::SeekFrom::Start(offset)))?;
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
        let mut file = convert(fs::OpenOptions::new().read(true).open(&file_path))?;

        let mut offset: u64 = 0;
        let mut len = file.metadata().unwrap().len();
        let mut pre_offset: u64 = 0;

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
                convert(file.seek(io::SeekFrom::Start(offset)))?;
                // if file has  dirty data, change to valid length
                convert(
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
                convert(file.seek(io::SeekFrom::Start(offset)))?;
                // if file has  dirty data, change to valid length
                convert(
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&file_path)
                        .unwrap()
                        .set_len(len),
                )?;
            } else {
                convert(file.seek(io::SeekFrom::Current(dl as i64)))?;
                pre_offset = offset - 4;
                offset += dl;
            }
        }
    }

    fn log_rolling(&mut self, new_id: u64) -> RaftResult<()> {
        convert(self.writer.flush())?;

        let file = convert(
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

fn read_u32(file: &mut fs::File) -> RaftResult<u32> {
    let mut output = [0u8; 4];
    convert(file.read_exact(&mut output[..]))?;
    Ok(u32::from_be_bytes(output))
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
