use std::fmt;
use std::result;

pub type Result<T> = result::Result<T, KvsError>;

#[derive(Debug)]
pub enum KvsError {
    InvalidKeySize,
    InvalidValueSize,
    KeyNotFound
}

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            KvsError::InvalidKeySize => {
                write!(f, "The key cannot be larger than 256B.")
            },
            KvsError::InvalidValueSize => {
                write!(f, "The value cannot be larger than 4KB.")
            },
            KvsError::KeyNotFound => {
                write!(f, "The key not found in database.")
            }
        }
    }
}
