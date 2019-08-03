//! A Simple Key-Value DataBase in memory.

#![deny(missing_docs)]
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};

use error::KvsError;
pub use error::Result;

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

mod error;

const REDUNDANCE_THRESHOLD: u64 = 1 << 20; // threshold that tigger log compacting.

/// The struct of Key-Value DataBase implemented with
/// [HashMap](https://doc.rust-lang.org/std/collections/hash_map/struct.HashMap.html).
///
/// The key can be up to 256B and the value can be up to 4KB.
pub struct KvStore {
    index: HashMap<String, CommandPos>,
    logreader: LogReader,
    logwriter: LogWriter,
    index_path: PathBuf,
    log_path: PathBuf,
    redundance_bytes: u64,
}

impl KvStore {
    /// Insert the `key`(up to 256B) with `value`(up to 4KB) to the DataBase.
    ///
    /// If the `key` already exists, update the associated value to `value` while keep the key
    /// unchanged.
    ///
    /// # Errors
    /// If the size of key or value exceeds the limitation, then an error is returned.
    ///
    ///// # Examples
    /////
    ///// ```
    ///// use kvs::KvStore;
    ///// let mut db = KvStore::new();
    ///// db.set("key1".to_owned(), "value1".to_owned()).unwrap(); // insert the record successfully.
    /////
    ///// let big_key: Vec<u8> = vec![0; 257];
    ///// let big_key = String::from_utf8(big_key).unwrap(); // A key in size of 257B
    /////
    ///// db.set(big_key, "value".to_owned()).expect_err("expect err there"); // set returns an error
    ///// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        check_length(&key, "key", 256)?;
        check_length(&value, "value", 1 << 12)?;

        let cmd = Command::Set { key, value };
        let cmd_head_pos = self.logwriter.write(&cmd)?;

        let cmd_pos = CommandPos {
            pos: cmd_head_pos,
            len: self.logwriter.writer.seek(SeekFrom::End(0))? - cmd_head_pos,
        };

        if let Command::Set { key, .. } = cmd {
            if let Some(old_pos) = self.index.insert(key, cmd_pos) {
                self.redundance_bytes += old_pos.len;
            }
        }

        if self.redundance_bytes >= REDUNDANCE_THRESHOLD {
            self.log_compact()?;
            self.redundance_bytes = 0;
        }
        Ok(())
    }

    /// Returns the value associated with the key.
    ///
    /// # Errors
    /// Return an error if the value is not read successfully.
    ///
    ///// # Examples
    /////
    ///// ```
    ///// use kvs::KvStore;
    /////
    ///// let mut db = KvStore::new();
    ///// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    ///// assert_eq!(db.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
    ///// assert_eq!(db.get("key2".to_owned()).unwrap(), None);
    ///// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.logwriter.flush()?;

        if let Some(cmd_pos) = self.index.get(&key) {
            let cmd = self.logreader.read_in_pos(cmd_pos.pos, cmd_pos.len)?;
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
    ///// # Examples
    ///// ```
    ///// use kvs::KvStore;
    /////
    ///// let mut db = KvStore::new();
    ///// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    ///// db.remove("key1".to_owned()).expect("Expect Ok(()) here"); // Removes "key1" from the DataBase
    /////
    ///// db.remove("key2".to_owned()).expect_err("Expect KeyNotFound Err."); // "key2" doesn't in DataBase.
    ///// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(cmd_pos) = self.index.remove(&key) {
            //let cmd = Command::Rm { key };
            //self.logwriter.write(&cmd)?;
            self.redundance_bytes += cmd_pos.len;
            if self.redundance_bytes >= REDUNDANCE_THRESHOLD {
                self.log_compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    ///// Returns an iterator of all the keys in the DataBase. If the DataBase is empty, returns an
    ///// empty iterator. The order of the keys is arbitrary.
    ///// # Examples
    ///// ```
    ///// use kvs::KvStore;
    /////
    ///// let mut db = KvStore::new();
    ///// db.set("key1".to_owned(), "value1".to_owned()).unwrap();
    ///// db.set("key2".to_owned(), "value2".to_owned()).unwrap();
    /////
    ///// for k in db.scan() {
    /////     println!("key: {}, value: {}", k, *k); // print all the key-value pairs in the DataBase
    ///// }
    ///// ```
    //pub fn scan(&self) -> impl Iterator<Item = &String> {
    //    self.table.keys()
    //}

    /// Open a KvStore DataBase from a file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        let log_file = path.as_ref().to_path_buf().join("log");
        let index_file = path.as_ref().to_path_buf().join("index");

        let log_handle = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(&log_file)?;

        let mut logreader = LogReader::new(log_handle.try_clone()?);
        let logwriter = LogWriter::new(log_handle.try_clone()?);
        let mut index: HashMap<String, CommandPos>;

        if index_file.exists() {
            let index_handle = OpenOptions::new().read(true).open(&index_file)?;
            index = serde_json::from_reader(index_handle)?
        } else {
            index = HashMap::new();
            let mut log_stream =
                Deserializer::from_reader(&mut logreader.reader).into_iter::<Command>();

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
            index,
            logreader,
            logwriter,
            index_path: index_file,
            log_path: log_file,
            redundance_bytes: 0,
        })
    }

    fn log_compact(&mut self) -> Result<()> {
        self.logwriter.flush()?;

        let log_handle = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(format!("{}.tmp", self.log_path.display()))?;

        let mut new_logwriter = LogWriter::new(log_handle.try_clone()?);
        let new_logreader = LogReader::new(log_handle.try_clone()?);

        let mut cmd_head_pos: u64 = 0;
        for (_, cmd_pos) in self.index.iter_mut() {
            let cmd_bytes = self.logreader.read_raw_in_pos(cmd_pos.pos, cmd_pos.len)?;
            cmd_pos.pos = cmd_head_pos;
            cmd_head_pos += cmd_pos.len;

            new_logwriter.writer.write_all(&cmd_bytes)?;
        }

        self.logwriter = new_logwriter;
        self.logreader = new_logreader;

        std::fs::remove_file(&self.log_path)?;
        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        let index_writer = BufWriter::new(File::create(&self.index_path).unwrap());
        serde_json::to_writer(index_writer, &self.index).unwrap();

        let tmp_log = format!("{}.tmp", self.log_path.display());
        let tmp_log_path = Path::new(&tmp_log);
        if tmp_log_path.exists() {
            std::fs::rename(&tmp_log, &self.log_path).unwrap();
        }
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
