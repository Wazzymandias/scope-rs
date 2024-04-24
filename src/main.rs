#![feature(unix_sigpipe)]

use clap::Parser;
use crate::cli::Command;

mod proto {
    include!("pb.rs");
}
mod cli;
mod hub_diff;
mod sync_id;

#[unix_sigpipe = "sig_dfl"]
fn main() {
    let cmd = Command::parse();
    if let Err(err) = cmd.execute() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
