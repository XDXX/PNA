use std::env::current_dir;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::ErrorKind::WouldBlock;
use std::net::SocketAddr;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::sync::Mutex;

use crossbeam_channel::{bounded, select, Receiver};
use ctrlc;
use slog::{error, info, o, Drain};
use slog_json;
use structopt::StructOpt;

use kvs::{KvStore, KvsEngine, KvsError, NaiveThreadPool, SledKvsEngine, ThreadPool};

enum BackEngines {
    Kvs,
    Sled,
    Auto,
}

impl FromStr for BackEngines {
    type Err = KvsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let engine_name = s.to_lowercase();
        match engine_name.as_ref() {
            "kvs" => Ok(BackEngines::Kvs),
            "sled" => Ok(BackEngines::Sled),
            "auto" => Ok(BackEngines::Auto),
            _ => Err(KvsError::ParseEngineError),
        }
    }
}

impl std::fmt::Debug for BackEngines {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackEngines::Kvs => write!(f, "kvs"),
            BackEngines::Sled => write!(f, "sled"),
            BackEngines::Auto => write!(f, "automatically select from kvs or sled"),
        }
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server", about = "A simple Key-Value Server")]
struct Kvs {
    /// An IP address with format IP:PORT, which kvs server will bind in.
    #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
    ip: SocketAddr,

    /// The built-in engine used as backend, either "kvs" or "sled". Automatically select
    /// from "kvs" or "sled" by default.
    #[structopt(long = "engine", default_value = "auto")]
    engine: BackEngines,
}

fn main() -> kvs::Result<()> {
    let drain = Mutex::new(slog_json::Json::default(std::io::stderr())).map(slog::Fuse);
    let log = slog::Logger::root(drain, o!());
    info!(log, "kvs-server start up"; "version" => env!("CARGO_PKG_VERSION"));

    let opt = Kvs::from_args();
    let ctrl_c_events = ctrl_channel().unwrap();

    let engine_type = get_engine(current_dir()?, opt.engine, &log);
    info!(log, "kvs-server configuration";
          "socket address" => opt.ip,
          "engine used" => format!("{:?}", engine_type)
    );

    match engine_type {
        BackEngines::Kvs => {
            let engine = KvStore::open(current_dir()?).exit_if_err(&log, 1);
            run_server(&opt.ip, ctrl_c_events, engine)
        }
        BackEngines::Sled => {
            let engine = SledKvsEngine::open(current_dir()?).exit_if_err(&log, 1);
            run_server(&opt.ip, ctrl_c_events, engine)
        }
        BackEngines::Auto => exit(1),
    }
}

fn run_server<E: KvsEngine>(
    ip: &SocketAddr,
    ctrl_c_events: Receiver<()>,
    engine: E,
) -> kvs::Result<()> {
    let listener = TcpListener::bind(ip)?;
    listener
        .set_nonblocking(true)
        .expect("Cannot set non-blocking");

    let pool = NaiveThreadPool::new(1000)?;

    loop {
        select! {
            recv(ctrl_c_events) -> _ => {
                engine.save_index_log()?;
                exit(0);
            }
            default => {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let engine = engine.clone();
                        pool.spawn(move || {
                            let response = match get_response(&stream, engine) {
                                Ok(response) => response,
                                Err(e) => format!("Error\r\n{}\r\n", e),
                            };
                            stream.write_all(response.as_bytes()).unwrap();
                        })
                    }
                    Err(ref e) if e.kind() == WouldBlock => continue,
                    Err(e) => {
                        return Err(e.into())
                    }
                }
            }
        }
    }
}

fn get_response<E: KvsEngine>(stream: &TcpStream, engine: E) -> kvs::Result<String> {
    let mut buf_reader = BufReader::new(stream);
    let cmd = read_line_from_stream(&mut buf_reader)?;

    match cmd.as_ref() {
        "SET" => {
            let key = read_line_from_stream(&mut buf_reader)?;
            let value = read_line_from_stream(&mut buf_reader)?;
            engine.set(key, value)?;
            Ok("Success\r\n".to_string())
        }
        "GET" => {
            let key = read_line_from_stream(&mut buf_reader)?;
            let value = engine.get(key)?;
            match value {
                Some(v) => Ok(format!("Success\r\n{}\r\n{}\r\n", v.len(), v)),
                None => Ok("Success\r\n-1\r\n".to_string()),
            }
        }
        "RM" => {
            let key = read_line_from_stream(&mut buf_reader)?;
            engine.remove(key)?;
            Ok("Success\r\n".to_string())
        }
        "SCAN" => {
            let keys = engine.scan().join("\r\n");
            Ok(format!("Success\r\n{}\r\n", keys))
        }
        _ => Err(KvsError::CmdNotSupport),
    }
}

fn read_line_from_stream(reader: &mut BufReader<&TcpStream>) -> kvs::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    line.truncate(line.len() - 2);
    Ok(line)
}

trait LogAndExit {
    type RESULT;
    fn exit_if_err(self, logger: &slog::Logger, exit_code: i32) -> Self::RESULT;
}

impl<T, E: std::error::Error> LogAndExit for Result<T, E> {
    type RESULT = T;
    fn exit_if_err(self, logger: &slog::Logger, exit_code: i32) -> Self::RESULT {
        match self {
            Result::Err(e) => {
                error!(logger, "An error occurred.";
                       "Error" => e.to_string());
                exit(exit_code)
            }
            Result::Ok(t) => t,
        }
    }
}

fn get_engine(dir: PathBuf, engine: BackEngines, log: &slog::Logger) -> BackEngines {
    let persisted_engine = dir.join("db.type");
    if persisted_engine.exists() {
        let engine_type = std::fs::read_to_string(&persisted_engine).unwrap();
        if format!("{:?}", engine).contains(&engine_type) {
            BackEngines::from_str(&engine_type).unwrap()
        } else {
            error!(log, "Engines are not compatible.";
                   "engine previously used" => engine_type);
            exit(1);
        }
    } else {
        let engine = match engine {
            BackEngines::Auto => BackEngines::Kvs,
            _ => engine,
        };
        let mut engine_file = File::create(persisted_engine).unwrap();
        engine_file
            .write_all(format!("{:?}", engine).as_bytes())
            .unwrap();
        engine
    }
}

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(10);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}
