use serde_json;
use sled;
use std::fmt;
use std::io;
use std::process::exit;
use std::result;

/// Custom Result type for kvs.
pub type Result<T> = result::Result<T, KvsError>;

#[derive(Debug)]
pub enum KvsError {
    InvalidKeySize,
    InvalidValueSize,
    KeyNotFound,
    ParseEngineError,
    CmdNotSupport,
    IOError(io::Error),
    DeserError(serde_json::error::Error),
    SledError(sled::Error),
}

impl KvsError {
    pub fn exit(&self, err: i32) -> ! {
        println!("{}", self);
        exit(err);
    }
}

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            KvsError::InvalidKeySize => write!(f, "The key cannot be larger than 256B."),
            KvsError::InvalidValueSize => write!(f, "The value cannot be larger than 4KB."),
            KvsError::KeyNotFound => write!(f, "Key not found"),
            KvsError::IOError(inner) => write!(f, "{}", inner),
            KvsError::DeserError(inner) => write!(f, "{}", inner),
            KvsError::ParseEngineError => write!(f, "Can not parse engine name."),
            KvsError::CmdNotSupport => write!(f, "Command not support."),
            KvsError::SledError(inner) => write!(f, "{}", inner),
        }
    }
}

impl From<io::Error> for KvsError {
    fn from(error: io::Error) -> Self {
        KvsError::IOError(error)
    }
}

impl From<serde_json::error::Error> for KvsError {
    fn from(error: serde_json::error::Error) -> Self {
        KvsError::DeserError(error)
    }
}

impl From<KvsError> for String {
    fn from(error: KvsError) -> Self {
        error.to_string()
    }
}

impl From<sled::Error> for KvsError {
    fn from(error: sled::Error) -> Self {
        KvsError::SledError(error)
    }
}

impl std::error::Error for KvsError {}
