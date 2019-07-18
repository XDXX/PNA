use std::collections::HashMap;

pub struct KvStore {
    table: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            table: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<(), String> {
        check_length(&key, "key", 256)?;
        check_length(&value, "value", 1 << 12)?;

        self.table.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.table.get(&key).cloned()
    }

    pub fn remove(&mut self, key: String) {
        self.table.remove(&key);
    }
}

fn check_length(s: &String, s_type: &str, maxLen_in_bytes: usize) -> Result<(), String> {
    if s.len() <= maxLen_in_bytes {
        return Ok(());
    } else {
        return Err(format!(
            "The {} must be less than {} bytes.",
            s_type, maxLen_in_bytes
        ));
    }
}
