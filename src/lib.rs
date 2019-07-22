//! A Simple Key-Value DataBase in memory.

//#![deny(missing_docs)]
use std::collections::HashMap;

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
    pub fn set(&mut self, key: String, value: String) -> Result<(), String> {
        check_length(&key, "key", 256)?;
        check_length(&value, "value", 1 << 12)?;

        self.table.insert(key, value);
        Ok(())
    }

    /// Returns the value associated with the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut db = KvStore::new();
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// assert_eq!(db.get("key1".to_owned()), Some("value1".to_owned()));
    /// assert_eq!(db.get("key2".to_owned()), None);
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.table.get(&key).cloned()
    }

    /// Removes the key and associated value from the DataBase. If the key does't exists,
    /// nothing will happen.
    /// 
    /// # Examples
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut db = KvStore::new();
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// db.remove("key1".to_owned()); // Removes "key1" from the DataBase
    ///
    /// db.remove("key2".to_owned()); // "key2" doesn't in DataBase, so nothing will happen.
    /// ```
    pub fn remove(&mut self, key: String) {
        self.table.remove(&key);
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

fn check_length(s: &str, s_type: &str, max_len_in_bytes: usize) -> Result<(), String> {
    if s.len() <= max_len_in_bytes {
        Ok(())
    } else {
        Err(format!(
            "The {} must be less than {} bytes.",
            s_type, max_len_in_bytes
        ))
    }
}
