#[derive(Debug, Error, PartialEq)]
pub enum RaftError {
    #[error("{0}")]
    Error(String),
    #[error("net has error: {0}")]
    NetError(String),
    #[error("not found address by node id:{0}")]
    NotfoundAddr(u64),
}

pub type RaftResult<T> = std::result::Result<T, RaftError>;
