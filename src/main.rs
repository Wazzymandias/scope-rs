use clap::Parser;

use crate::cli::Command;

mod proto {
    include!("pb.rs");
}
mod cli;

fn main() {

    let cmd = Command::parse();
    if let Err(err) = cmd.execute() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}

