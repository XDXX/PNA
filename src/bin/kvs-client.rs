use std::io::prelude::*;
use std::net::TcpStream;
use std::net::SocketAddr;
use std::io::BufReader;
use std::process::exit;

use structopt::StructOpt;

use kvs::Result as KvsResult;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "kvs-client",
    about = "A simple Key-Value database client",
    raw(setting = "structopt::clap::AppSettings::VersionlessSubcommands")
)]
struct Kvs {
    #[structopt(subcommand)]
    option: Opt,

    /// An IP address with the format IP:PORT
    #[structopt(
        long = "addr", default_value = "127.0.0.1:4000",
        raw(set = "structopt::clap::ArgSettings::Global")
    )]
    ip: SocketAddr,
}

#[derive(StructOpt, Debug)]
enum Opt {
    ///Insert the <key> with <value> into dataset.
    ///If the <key> already exists, update the associated value to <value>.
    #[structopt(
        name = "set",
        raw(setting = "structopt::clap::AppSettings::DisableHelpFlags")
    )]
    Set { key: String, value: String },

    ///Get the associated value of <key>. If <key> does't exist, return None.
    #[structopt(
        name = "get",
        raw(setting = "structopt::clap::AppSettings::DisableHelpFlags")
    )]
    Get { key: String },

    ///Remove and return the associated value of <key>. If <key> does't exist, return None.
    #[structopt(
        name = "rm",
        raw(setting = "structopt::clap::AppSettings::DisableHelpFlags")
    )]
    Remove { key: String },

    ///Scan all keys in the dataset.
    #[structopt(
        name = "scan",
        raw(setting = "structopt::clap::AppSettings::DisableHelpFlags")
    )]
    Scan,
}

enum Command {
    Set{key: String, value: String},
    Get{key: String},
    Rm{key: String},
    Scan
}

fn main() {
    let opt = Kvs::from_args();

    match opt.option {
        Opt::Set { key, value } => {
            let cmd = Command::Set{key, value};

            let reader = request_to_server(&opt.ip, cmd).unwrap_or_else(|e| e.exit(1));
            match parse_response_to_string(reader, "SET") {
                Ok(_) => (),
                Err(err) => {
                    eprintln!("{}", err);
                    exit(1);
                }
            }
        }
        Opt::Get { key } =>  {
            let cmd = Command::Get{key};

            let reader = request_to_server(&opt.ip, cmd).unwrap_or_else(|e| e.exit(1));
            match parse_response_to_string(reader, "GET") {
                Ok(response) => println!("{}", response),
                Err(err) => {
                    eprintln!("{}", err);
                    exit(1);
                }
            }
        },
        Opt::Remove { key } => {
            let cmd = Command::Rm{key};

            let reader = request_to_server(&opt.ip, cmd).unwrap_or_else(|e| e.exit(1));
            match parse_response_to_string(reader, "RM") {
                Ok(_) => (),
                Err(err) => {
                    eprintln!("{}", err);
                    exit(1);
                }
            }
        }
        Opt::Scan => {
            let cmd = Command::Scan;

            let reader = request_to_server(&opt.ip, cmd).unwrap_or_else(|e| e.exit(1));
            match parse_response_to_string(reader, "SCAN") {
                Ok(response) => println!("{}", response),
                Err(err) => {
                    eprintln!("{}", err);
                    exit(1);
                }
            }
        }
    };
}

fn request_to_server(addr: &SocketAddr, cmd: Command) -> KvsResult<BufReader<TcpStream>> {
    let mut stream = TcpStream::connect(addr)?;
    let request = match cmd {
        Command::Set{key, value} => format!("SET\r\n{}\r\n{}\r\n", key, value),
        Command::Get{key} => format!("GET\r\n{}\r\n", key),
        Command::Rm{key} => format!("RM\r\n{}\r\n", key),
        Command::Scan => format!("SCAN\r\n")
    };

    stream.write_all(request.as_bytes())?;
    Ok(BufReader::new(stream))
}

fn parse_response_to_string(mut reader: BufReader<TcpStream>, response_type: &str) -> Result<String, String> {
    let is_success = read_line_from_stream(&mut reader)?;

    match is_success.as_ref() {
        "Success" => {
            if response_type == "GET" {
                let value_len = read_line_from_stream(&mut reader)?;
                if value_len == "-1" {
                    Ok("Key not found".to_string())
                } else {
                    Ok(read_line_from_stream(&mut reader)?)
                }
            } else if response_type == "SCAN" {
                Ok(read_line_from_stream(&mut reader)?) 
            } else {
                Ok(String::new())
            }
        },
        "Error" => Err(read_line_from_stream(&mut reader)?),
        _ => Err("Some unknown errors have occurred.".to_string())
    }
}

fn read_line_from_stream(reader: &mut BufReader<TcpStream>) -> KvsResult<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    line.truncate(line.len() - 2);
    Ok(line)
}
