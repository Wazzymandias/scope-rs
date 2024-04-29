#![feature(unix_sigpipe)]
#![feature(duration_constructors)]

extern crate core;

use clap::Parser;
use slog::{Drain, Logger, o, PushFnValue, Record};
use slog_async::Async;
use slog_json::Json;
use slog_scope;

use crate::cmd::cmd::Command;

mod proto {
    include!("pb.rs");
}

mod hub_diff;
mod cmd;
mod farcaster;

fn create_logger() -> Logger {
    let drain =
    Json::new(std::io::stdout())
        .add_key_value(o!(
            "msg" => PushFnValue(move |record : &Record, ser| {
                ser.emit(record.msg())
            }),
        )).build().fuse();
    let drain = Async::new(drain).build().fuse();

    Logger::root(drain, o!())
}

#[unix_sigpipe = "sig_dfl"]
#[tokio::main(flavor="multi_thread", worker_threads = 32)]
async fn main() {
    // Create the global logger
    let logger = create_logger();
    let _guard = slog_scope::set_global_logger(logger);

    let cmd = Command::parse();
    if let Err(err) = cmd.execute().await {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
