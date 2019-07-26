//! A Simple Key-Value DataBase in memory.

#![deny(missing_docs)]
use std::collections::HashMap;
use error::{KvsError, Result};

mod error;


/// The struct of Key-Value DataBase implemented with
/// [HashMap](https://doc.rust-lang.org/std/collections/hash_map/struct.HashMap.html).
///
/// The key can be up to 256B and the value can be up to 4KB.
#[derive(Default)]
pub struct KvStore {
    table: HashMap<String, String>,
}

impl KvStore {
    /// Creates an empty DataBase.
    ///
    /// # Examples
    /// ```
    /// use kvs::KvStore;
    /// let db = KvStore::new();
    /// ```
    pub fn new() -> KvStore {
        KvStore {
            table: HashMap::new(),
        }
    }

    /// Insert the `key`(up to 256B) with `value`(up to 4KB) to the DataBase. 
    ///
    /// If the `key` already exists, update the associated value to `value` while keep the key
    /// unchanged.
    ///
    /// # Errors
    /// If the size of key or value exceeds the limitation, then an error is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut db = KvStore::new();
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap(); // insert the record successfully.
    ///
    /// let big_key: Vec<u8> = vec![0; 257];
    /// let big_key = String::from_utf8(big_key).unwrap(); // A key in size of 257B
    ///
    /// db.set(big_key, "value".to_owned()).expect_err("expect err there"); // set returns an error
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        check_length(&key, "key", 256)?;
        check_length(&value, "value", 1 << 12)?;

        self.table.insert(key, value);
        Ok(())
    }

    /// Returns the value associated with the key.
    ///
    /// # Errors
    /// Return an error if the value is not read successfully.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut db = KvStore::new();
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// assert_eq!(db.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
    /// assert_eq!(db.get("key2".to_owned()).unwrap(), None);
    /// ```
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.table.get(&key).cloned())
    }

    /// Removes the key and associated value from the DataBase.
    ///
    /// # Errors
    /// Return an error if the key does not exist or is not removed successfully.
    /// 
    /// # Examples
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut db = KvStore::new();
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// db.remove("key1".to_owned()).expect("Expect Ok(()) here"); // Removes "key1" from the DataBase
    ///
    /// db.remove("key2".to_owned()).expect_err("Expect KeyNotFound Err."); // "key2" doesn't in DataBase.
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.table.remove(&key) {
            Some(_) => Ok(()),
            None => Err(KvsError::KeyNotFound)
        }
    }

    /// Returns an iterator of all the keys in the DataBase. If the DataBase is empty, returns an
    /// empty iterator. The order of the keys is arbitrary.
    /// # Examples
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut db = KvStore::new();
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// db.set("key2".to_owned(), "value2".to_owned()).unwrap();
    ///
    /// for k in db.scan() {
    ///     println!("key: {}, value: {}", k, *k); // print all the key-value pairs in the DataBase
    /// }
    /// ```
    pub fn scan(&self) -> impl Iterator<Item = &String> {
        self.table.keys()
    }
}

fn check_length(s: &str, s_type: &str, max_len_in_bytes: usize) -> Result<()> {
    if s.len() <= max_len_in_bytes {
        Ok(())
    } else {
        match s_type {
            "key" => Err(KvsError::InvalidKeySize),
            "value" => Err(KvsError::InvalidValueSize),
            _ => panic!("Unsupport type!")
        }
    }
}
