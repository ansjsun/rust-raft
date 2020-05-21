#[derive(Debug, Error, PartialEq)]
pub enum RaftError {
    #[error("{0}")]
    Error(String),
    #[error("net has error: {0}")]
    NetError(String),
    #[error("io has error: {0}")]
    IOError(String),
    #[error("not found address by node id:{0}")]
    NotfoundAddr(u64),
    #[error("term less than target machine")]
    TermLess,
    #[error("index:{0} less than target machine ")]
    IndexLess(u64),
    #[error("vote not allow")]
    VoteNotAllow,
    #[error("type has err")]
    TypeErr,
    #[error("raft not found by id:{0}")]
    RaftNotFound(u64),
}

pub type RaftResult<T> = std::result::Result<T, RaftError>;

pub fn conver<T, E: ToString>(result: std::result::Result<T, E>) -> RaftResult<T> {
    match result {
        Ok(t) => Ok(t),
        Err(e) => Err(RaftError::Error(e.to_string())),
    }
}
