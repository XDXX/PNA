use std::process::exit;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    about = "A simple Key-Value Server",
    raw(setting = "structopt::clap::AppSettings::VersionlessSubcommands")
)]
struct Kvs {
    #[structopt(subcommand)]
    option: Opt,
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
    Get{ key: String },

    ///Remove and return the associated value of <key>. If <key> does't exist, return None.
    #[structopt(
        name = "remove",
        raw(setting = "structopt::clap::AppSettings::DisableHelpFlags")
    )]
    Remove{ key: String },

    ///Scan all keys in the dataset.
    #[structopt(
        name = "scan",
        raw(setting = "structopt::clap::AppSettings::DisableHelpFlags")
    )]
    Scan

}

fn main() {
    let opt = Kvs::from_args();

    match opt.option {
        Opt::Set{..} => {
            eprintln!("unimplemented");
            exit(1);
        },
        Opt::Get{..} => {
            eprintln!("unimplemented");
            exit(1);
        },
        Opt::Remove{..} => {
            eprintln!("unimplemented");
            exit(1);
        },
        Opt::Scan => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
