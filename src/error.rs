pub enum RaftError {
    NetError(String),
}

pub type RaftResult<T> = std::result::Result<T, RaftError>;
