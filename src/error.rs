#[derive(Debug, Error, PartialEq)]
pub enum RaftError {
    #[error("success")]
    Success,
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
    #[error("term greater than target machine")]
    TermGreater,
    #[error("index:{0} less than target machine ")]
    IndexLess(u64),
    #[error("vote not allow")]
    VoteNotAllow,
    #[error("type has err")]
    TypeErr,
    #[error("raft not found by id:{0}")]
    RaftNotFound(u64),
    #[error("raft not leader leader is:{0}")]
    NotLeader(u64),
    #[error("not enough recipiet expect:{0} found:{1}")]
    NotEnoughRecipient(u16, u16),
}

pub type RaftResult<T> = std::result::Result<T, RaftError>;

pub fn conver<T, E: ToString>(result: std::result::Result<T, E>) -> RaftResult<T> {
    match result {
        Ok(t) => Ok(t),
        Err(e) => Err(RaftError::Error(e.to_string())),
    }
}

impl RaftError {
    pub fn code(&self) -> Vec<u8> {
        panic!()
    }

    pub fn decode(data: Vec<u8>) -> Self {
        panic!()
    }
}
