use super::KvsEngine;
use crate::error::{KvsError, Result};
use std::path::Path;

use sled::Db;

/// Warpper of the [sled](https://docs.rs/sled/0.24.1/sled/) backed engine.
pub struct SledKvsEngine {
    database: Db,
}

impl SledKvsEngine {
    /// Open a SledKvsEngine from the directory contains the existing.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Db::start_default(path)?;
        Ok(SledKvsEngine { database: db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.database.set(key, value.as_bytes())?;
        self.database.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let v = self.database.get(key)?;
        Ok(v.and_then(|s| Some(String::from_utf8(s.to_vec()).unwrap())))
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.database.del(key)?.ok_or(KvsError::KeyNotFound)?;
        self.database.flush()?;
        Ok(())
    }

    fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = String> + 'a> {
        let iter = self
            .database
            .iter()
            .keys()
            .map(|s| String::from_utf8(s.unwrap()).unwrap());
        Box::new(iter)
    }
}
