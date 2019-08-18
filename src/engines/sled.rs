use super::KvsEngine;
use crate::error::{KvsError, Result};
use std::path::Path;
use std::sync::{Arc, Mutex};

use sled::Db;

/// Wrapper of the [sled](https://docs.rs/sled/0.24.1/sled/) backed engine.
#[derive(Clone)]
pub struct SledKvsEngine {
    database: Arc<Mutex<Db>>,
}

impl SledKvsEngine {
    /// Open a SledKvsEngine from the directory contains the existing.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Arc::new(Mutex::new(Db::start_default(path)?));
        Ok(SledKvsEngine { database: db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        let database = self.database.lock().unwrap();
        database.set(key, value.as_bytes())?;
        database.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let v = self.database.lock().unwrap().get(key)?;
        Ok(v.and_then(|s| Some(String::from_utf8(s.to_vec()).unwrap())))
    }

    fn remove(&self, key: String) -> Result<()> {
        let database = self.database.lock().unwrap();
        database.del(key)?.ok_or(KvsError::KeyNotFound)?;
        database.flush()?;
        Ok(())
    }

    fn scan(&self) -> Vec<String> {
        let database = self.database.lock().unwrap();
        database
            .iter()
            .keys()
            .map(|s| String::from_utf8(s.unwrap()).unwrap())
            .collect()
    }
}
