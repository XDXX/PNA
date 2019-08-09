use std::net::SocketAddr;
use std::str::FromStr;
use std::net::{TcpListener, TcpStream};
use std::io::prelude::*;
use std::io::BufReader;
use std::env::current_dir;
use std::process::exit;
use std::path::PathBuf;
use std::sync::Mutex;
use std::fs::File;

use slog::{info, o, error, Drain};
use slog_json;
use structopt::StructOpt;

use kvs::{KvsEngine, KvsError, KvStore};

enum BackEngines {
    Kvs,
    Sled,
    Auto
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

    /// The built-in engine used as beckend, either "kvs" or "sled". Automatically select
    /// from "kvs" or "sled" by default.
    #[structopt(long = "engine", default_value = "auto")]
    engine: BackEngines,
}

fn main() -> kvs::Result<()> {
    let drain = Mutex::new(slog_json::Json::default(std::io::stderr())).map(slog::Fuse);
    let log = slog::Logger::root(drain, o!());

    info!(log, "kvs-server start up"; "version" => env!("CARGO_PKG_VERSION"));

    let opt = Kvs::from_args();

    let engine_type = get_engin(current_dir()?, opt.engine, &log);
    let mut engine = match engine_type {
        BackEngines::Kvs => KvStore::open(current_dir()?).exit_if_err(&log, 1),
        BackEngines::Sled => KvStore::open(current_dir()?).exit_if_err(&log, 1),
        BackEngines::Auto => exit(1)
    };

    info!(log, "kvs-server configuration";
          "socket address" => opt.ip,
          "engine used" => format!("{:?}", engine_type)
    );

    let listener = TcpListener::bind(&opt.ip)?;
    for stream in listener.incoming() {
        let mut stream = stream?;
        let response = match get_response(&stream, &mut engine) {
            Ok(response) => {
                response
            }
            Err(e) => {
                format!("Error\r\n{}\r\n", e)
            }
        };

        stream.write_all(response.as_bytes())?;
    }
    Ok(())
}

fn get_response<T: KvsEngine>(stream: &TcpStream, engine: &mut T) -> kvs::Result<String> {
    let mut buf_reader = BufReader::new(stream);
    let cmd = read_line_from_stream(&mut buf_reader)?;

    match cmd.as_ref() {
        "SET" => {
            let key = read_line_from_stream(&mut buf_reader)?;
            let value = read_line_from_stream(&mut buf_reader)?;
            engine.set(key, value)?;
            Ok("Success\r\n".to_string())
        },
        "GET" => {
            let key = read_line_from_stream(&mut buf_reader)?;
            let value = engine.get(key)?;
            match value {
                Some(v) => Ok(format!("Success\r\n{}\r\n{}\r\n", v.len(), v)),
                None => Ok("Success\r\n-1\r\n".to_string())
            }
        },
        "RM" => {
            let key = read_line_from_stream(&mut buf_reader)?;
            engine.remove(key)?;
            Ok("Success\r\n".to_string())
        },
        "SCAN" => {
            let keys = engine.scan().map(|x| x.as_str()).collect::<Vec<&str>>().join("\r\n");
            Ok(format!("Success\r\n{}\r\n", keys))
        },
        _ => {
            Err(KvsError::CmdNotSupport)
        }
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
            },
            Result::Ok(t) => t
        }
    }
}

fn get_engin(dir: PathBuf, engine: BackEngines, log: &slog::Logger) -> BackEngines {
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
            _ => engine
        };
        let mut engine_fs = File::create(persisted_engine).unwrap();
        engine_fs.write(format!("{:?}", engine).as_bytes()).unwrap();
        engine
    }
}
