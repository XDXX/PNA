//! A Simple Key-Value DataBase in memory.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::ops::Deref;

use super::KvsEngine;
use crate::error::{KvsError, Result};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

const REDUNDANCE_THRESHOLD: u64 = 1 << 20; // threshold that tigger log compacting, default 1MB.

/// The struct of Key-Value DataBase implemented with
/// [HashMap](https://doc.rust-lang.org/std/collections/hash_map/struct.HashMap.html).
///
/// The key can be up to 256B and the value can be up to 4KB.
#[derive(Clone)]
pub struct KvStore {
    index: Arc<Mutex<HashMap<String, CommandPos>>>,
    logreader: Arc<Mutex<LogReader>>,
    logwriter: Arc<Mutex<LogWriter>>,
    index_path: Arc<PathBuf>,
    log_path: Arc<PathBuf>,
    redundance_bytes: Arc<Mutex<u64>>,
}

impl KvStore {
    /// Open a KvStore DataBase from the directory contains logfile and index file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        let log_file = Arc::new(path.as_ref().to_path_buf().join("log"));
        let index_file = Arc::new(path.as_ref().to_path_buf().join("index"));

        let log_handle = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(log_file.deref())?;

        let logreader = Arc::new(Mutex::new(LogReader::new(log_handle.try_clone()?)));
        let logwriter = Arc::new(Mutex::new(LogWriter::new(log_handle.try_clone()?)));
        let index_arc: Arc<Mutex<HashMap<String, CommandPos>>>;

        if index_file.exists() {
            let index_handle = OpenOptions::new().read(true).open(index_file.deref())?;
            index_arc = Arc::new(Mutex::new(serde_json::from_reader(index_handle)?));
        } else {
            index_arc = Arc::new(Mutex::new(HashMap::new()));
            let mut index = index_arc.lock().unwrap();
            let mut log_stream =
                Deserializer::from_reader(&mut logreader.lock().unwrap().reader)
                    .into_iter::<Command>();

            let mut curr_head_pos: u64 = 0;
            while let Some(cmd) = log_stream.next() {
                if let Ok(cmd) = cmd {
                    let cmd_pos = CommandPos {
                        pos: curr_head_pos,
                        len: log_stream.byte_offset() as u64 - curr_head_pos,
                    };
                    curr_head_pos += cmd_pos.len;

                    match cmd {
                        Command::Set { key, .. } => index.insert(key, cmd_pos),
                        Command::Rm { key } => index.remove(&key),
                    };
                }
            }
        }

        Ok(KvStore {
            index: index_arc,
            logreader,
            logwriter,
            index_path: index_file,
            log_path: log_file,
            redundance_bytes: Arc::new(Mutex::new(0)),
        })
    }

    fn log_compact(&mut self) -> Result<()> {
        self.logwriter.lock().unwrap().flush()?;

        let tmp_log = format!("{}.tmp", self.log_path.display());
        let log_handle = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(&tmp_log)?;

        let new_logwriter_arc = Arc::new(Mutex::new(LogWriter::new(log_handle.try_clone()?)));
        let mut new_logwriter = new_logwriter_arc.lock().unwrap();
        let new_logreader_arc = Arc::new(Mutex::new(LogReader::new(log_handle.try_clone()?)));

        let mut cmd_head_pos: u64 = 0;
        for (_, cmd_pos) in self.index.lock().unwrap().iter_mut() {
            let cmd_bytes = self.logreader.lock().unwrap().read_raw_in_pos(cmd_pos.pos, cmd_pos.len)?;
            cmd_pos.pos = cmd_head_pos;
            cmd_head_pos += cmd_pos.len;

            new_logwriter.writer.write_all(&cmd_bytes)?;
        }

        self.logwriter = new_logwriter_arc;
        self.logreader = new_logreader_arc;

        std::fs::remove_file(self.log_path.deref())?;
        std::fs::rename(&tmp_log, self.log_path.deref()).unwrap();

        Ok(())
    }
}

impl Drop for KvStore {
    /// Store index file of DataBase when the KvStore instance go out of scope.
    fn drop(&mut self) {
        let index_writer = BufWriter::new(File::create(self.index_path.deref()).unwrap());
        serde_json::to_writer(index_writer, self.index.lock().unwrap().deref()).unwrap();
    }
}

impl KvsEngine for KvStore {
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
    /// use kvs::KvsEngine;
    /// use tempfile::TempDir;
    /// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// println!("{:?}", temp_dir);
    /// let mut db = KvStore::open(&temp_dir).unwrap();
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap(); // insert the record successfully.
    ///
    /// let big_key: Vec<u8> = vec![0; 257];
    /// let big_key = String::from_utf8(big_key).unwrap(); // A key in size of 257B
    ///
    /// db.set(big_key, "value".to_owned()).expect_err("expect err there"); // set returns an error
    /// ```
    fn set(&self, key: String, value: String) -> Result<()> {
        check_length(&key, "key", 256)?;
        check_length(&value, "value", 1 << 12)?;

        let cmd = Command::Set { key, value };
        let mut logwriter = self.logwriter.lock().unwrap();
        let cmd_head_pos = logwriter.write(&cmd)?;

        let cmd_pos = CommandPos {
            pos: cmd_head_pos,
            len: logwriter.writer.seek(SeekFrom::End(0))? - cmd_head_pos,
        };

        
        let mut redundance_bytes = self.redundance_bytes.lock().unwrap();
        if let Command::Set { key, .. } = cmd {
            if let Some(old_pos) = self.index.lock().unwrap().insert(key, cmd_pos) {
                *redundance_bytes += old_pos.len;
            }
        }

        if *redundance_bytes >= REDUNDANCE_THRESHOLD {
            self.log_compact()?;
            *redundance_bytes = 0;
        }
        Ok(())
    }

    /// Returns the value associated with the key.
    ///
    /// # Errors
    /// Return an error if the value in log file is not read successfully.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// use kvs::KvsEngine;
    /// use tempfile::TempDir;
    ///
    /// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// let mut db = KvStore::open(&temp_dir).unwrap();
    ///
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// assert_eq!(db.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
    /// assert_eq!(db.get("key2".to_owned()).unwrap(), None);
    /// ```
    fn get(&self, key: String) -> Result<Option<String>> {
        self.logwriter.lock().unwrap().flush()?;

        if let Some(cmd_pos) = self.index.lock().unwrap().get(&key) {
            let cmd = self.logreader.lock().unwrap().read_in_pos(cmd_pos.pos, cmd_pos.len)?;
            match cmd {
                Command::Set { value, .. } => Ok(Some(value)),
                _ => Err(KvsError::KeyNotFound),
            }
        } else {
            Ok(None)
        }
    }

    /// Removes the key and associated value from the DataBase.
    ///
    /// # Errors
    /// Return an error if the key does not exist or is not removed successfully.
    ///
    /// # Examples
    /// ```
    /// use kvs::KvStore;
    /// use kvs::KvsEngine;
    /// use tempfile::TempDir;
    ///
    /// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// let mut db = KvStore::open(&temp_dir).unwrap();
    ///
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// db.remove("key1".to_owned()).expect("Expect Ok(()) here"); // Removes "key1" from the DataBase
    ///
    /// db.remove("key2".to_owned()).expect_err("Expect KeyNotFound Err."); // "key2" doesn't in DataBase.
    /// ```
    fn remove(&self, key: String) -> Result<()> {
        if let Some(old_cmd_pos) = self.index.lock().unwrap().remove(&key) {
            let cmd = Command::Rm { key };
            let cmd_head_pos = self.logwriter.lock().unwrap().write(&cmd)?;

            let cmd_pos = CommandPos {
                pos: cmd_head_pos,
                len: self.logwriter.lock().unwrap().writer.seek(SeekFrom::End(0))? - cmd_head_pos,
            };

            let mut redundance_bytes = self.redundance_bytes.lock().unwrap();
            *redundance_bytes += old_cmd_pos.len + cmd_pos.len;
            if *redundance_bytes >= REDUNDANCE_THRESHOLD {
                self.log_compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Returns an iterator of all the keys in the DataBase. If the DataBase is empty, returns an
    /// empty iterator. The order of the keys is arbitrary.
    /// # Examples
    /// ```
    /// use kvs::KvStore;
    /// use kvs::KvsEngine;
    /// use tempfile::TempDir;
    ///
    /// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    /// let mut db = KvStore::open(&temp_dir).unwrap();
    ///
    /// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    /// db.set("key2".to_owned(), "value2".to_owned()).unwrap();
    ///
    /// for k in db.scan() {
    ///     println!("key: {}", k); // print all the keys in the DataBase
    /// }
    /// ```
    fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = String> + 'a> {
        Box::new(self.index.lock().unwrap().keys().cloned())
    }
}

#[derive(Deserialize, Serialize)]
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}

#[derive(Deserialize, Serialize)]
struct CommandPos {
    pos: u64,
    len: u64,
}

struct LogWriter {
    writer: BufWriter<File>,
}

impl LogWriter {
    fn new(f: File) -> LogWriter {
        LogWriter {
            writer: BufWriter::new(f),
        }
    }

    fn write(&mut self, cmd: &Command) -> Result<u64> {
        let cmd_head_pos = self.writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.writer, cmd)?;
        Ok(cmd_head_pos)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

struct LogReader {
    reader: BufReader<File>,
}

impl LogReader {
    fn new(f: File) -> LogReader {
        LogReader {
            reader: BufReader::new(f),
        }
    }

    fn read_in_pos(&mut self, pos: u64, len: u64) -> Result<Command> {
        self.reader.seek(SeekFrom::Start(pos))?;
        let adaptor = self.reader.by_ref().take(len);

        let cmd = serde_json::from_reader(adaptor)?;
        Ok(cmd)
    }

    fn read_raw_in_pos(&mut self, pos: u64, len: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len as usize];
        self.reader.seek(SeekFrom::Start(pos))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

fn check_length(s: &str, s_type: &str, max_len_in_bytes: usize) -> Result<()> {
    if s.len() <= max_len_in_bytes {
        Ok(())
    } else {
        match s_type {
            "key" => Err(KvsError::InvalidKeySize),
            "value" => Err(KvsError::InvalidValueSize),
            _ => panic!("Unsupport type!"),
        }
    }
}
