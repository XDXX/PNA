//! A Simple Key-Value DataBase in memory.
#[deny(missing_docs)]
mod engines;
mod error;
pub mod thread_pool;

pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use thread_pool::{NaiveThreadPool, ThreadPool, SharedQueueThreadPool};
