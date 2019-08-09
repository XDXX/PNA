//! A Simple Key-Value DataBase in memory.
//#[deny(missing_docs)]
mod engines;
mod error;

pub use engines::{KvStore, KvsEngine};
pub use error::{KvsError, Result};
