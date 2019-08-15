pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
use crate::Result;

mod kvs;
mod sled;

/// An interface for repersenting the backend engine of kvs.
pub trait KvsEngine {
    /// Set the value of a string key to a string.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the stirng value of a string key. If the key does not exist, return `None`.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given string key.
    fn remove(&mut self, key: String) -> Result<()>;

    /// Returns an iterator of all the keys in the DataBase.
    fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = String> + 'a>;
}
