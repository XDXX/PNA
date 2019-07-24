use std::fmt;
use std::result;

pub type Result = result::Result<(), SizeError>;

#[derive(Debug)]
pub enum SizeError {
    InvalidKeySize,
    InvalidValueSize
}

impl fmt::Display for SizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            SizeError::InvalidKeySize => {
                write!(f, "The key cannot be larger than 256B")
            },
            SizeError::InvalidValueSize => {
                write!(f, "The value cannot be larger than 4KB")
            }
        }
    }
}
