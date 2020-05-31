pub static SUCCESS: &[u8] = &[0];

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
    #[error("raft log file not found by id:{0}")]
    LogFileNotFound(u64),
    #[error("raft log file invalid by id:{0}")]
    LogFileInvalid(u64),
    #[error("time out by ms:{0}")]
    Timeout(u64),
    #[error("not enough recipiet expect:{0} found:{1}")]
    NotEnoughRecipient(u16, u16),
}

impl RaftError {
    fn id(&self) -> u8 {
        match self {
            RaftError::Success => 0,
            RaftError::Error(_) => 1,
            RaftError::NetError(_) => 2,
            RaftError::IOError(_) => 3,
            RaftError::NotfoundAddr(_) => 4,
            RaftError::TermLess => 5,
            RaftError::TermGreater => 6,
            RaftError::IndexLess(_) => 7,
            RaftError::VoteNotAllow => 8,
            RaftError::TypeErr => 9,
            RaftError::RaftNotFound(_) => 10,
            RaftError::NotLeader(_) => 11,
            RaftError::LogFileNotFound(_) => 12,
            RaftError::LogFileInvalid(_) => 13,
            RaftError::Timeout(_) => 14,
            RaftError::NotEnoughRecipient(_, _) => 15,
        }
    }
}

pub type RaftResult<T> = std::result::Result<T, RaftError>;

pub fn conver<T, E: ToString>(result: std::result::Result<T, E>) -> RaftResult<T> {
    match result {
        Ok(t) => Ok(t),
        Err(e) => Err(RaftError::Error(e.to_string())),
    }
}

impl RaftError {
    pub fn encode(&self) -> Vec<u8> {
        let mut result: Vec<u8>;
        match self {
            RaftError::Success
            | RaftError::TermLess
            | RaftError::TermGreater
            | RaftError::VoteNotAllow
            | RaftError::TypeErr => {
                result = Vec::with_capacity(1);
                result.push(self.id());
            }
            RaftError::Error(msg) | RaftError::NetError(msg) | RaftError::IOError(msg) => {
                result = Vec::with_capacity(1 + msg.len());
                result.push(self.id());
                result.extend_from_slice(msg.as_str().as_bytes());
            }
            RaftError::NotfoundAddr(num)
            | RaftError::IndexLess(num)
            | RaftError::RaftNotFound(num)
            | RaftError::NotLeader(num)
            | RaftError::LogFileNotFound(num)
            | RaftError::LogFileInvalid(num)
            | RaftError::Timeout(num) => {
                result = Vec::with_capacity(9);
                result.push(self.id());
                result.extend_from_slice(&u64::to_be_bytes(*num));
            }
            RaftError::NotEnoughRecipient(expect, got) => {
                result = Vec::with_capacity(5);
                result.push(12);
                result.extend_from_slice(&u16::to_be_bytes(*expect));
                result.extend_from_slice(&u16::to_be_bytes(*got));
            }
        }

        result
    }

    pub fn decode(data: &Vec<u8>) -> Self {
        match data[0] {
            0 => RaftError::Success,
            1 => RaftError::Error(read_string(data, 1)),
            2 => RaftError::NetError(read_string(data, 1)),
            3 => RaftError::IOError(read_string(data, 1)),
            4 => RaftError::NotfoundAddr(read_u64_slice(data, 1)),
            5 => RaftError::TermLess,
            6 => RaftError::TermGreater,
            7 => RaftError::IndexLess(read_u64_slice(data, 1)),
            8 => RaftError::VoteNotAllow,
            9 => RaftError::TypeErr,
            10 => RaftError::RaftNotFound(read_u64_slice(data, 1)),
            11 => RaftError::NotLeader(read_u64_slice(data, 1)),
            12 => RaftError::LogFileNotFound(read_u64_slice(data, 1)),
            13 => RaftError::LogFileInvalid(read_u64_slice(data, 1)),
            14 => RaftError::Timeout(read_u64_slice(data, 1)),
            15 => RaftError::NotEnoughRecipient(read_u16_slice(data, 1), read_u16_slice(data, 3)),
            _ => panic!("not found"),
        }
    }
}

fn read_string(data: &[u8], start: usize) -> String {
    String::from_utf8_lossy(&data[start..]).to_string()
}

fn read_u64_slice(s: &[u8], start: usize) -> u64 {
    let ptr = s[start..start + 8].as_ptr() as *const [u8; 8];
    u64::from_be_bytes(unsafe { *ptr })
}

fn read_u16_slice(s: &[u8], start: usize) -> u16 {
    let ptr = s[start..start + 2].as_ptr() as *const [u8; 2];
    u16::from_be_bytes(unsafe { *ptr })
}
