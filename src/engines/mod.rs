pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
use crate::Result;

mod kvs;
mod sled;

/// An interface for representing the backend engine of kvs.
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the value of a string key to a string.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Get the string value of a string key. If the key does not exist, return `None`.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Remove a given string key.
    fn remove(&self, key: String) -> Result<()>;

    /// Returns an iterator of all the keys in the DataBase.
    fn scan(&self) -> Vec<String>;

    /// Store index file of DataBase to disk.
    fn save_index_log(&self) -> Result<()> {
        Ok(())
    }
}
