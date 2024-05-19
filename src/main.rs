#![feature(duration_constructors)]
#![feature(new_uninit)]

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

mod cmd;
mod db;
mod farcaster;
mod hub_diff;
mod deque;
mod lru;

fn create_logger() -> Logger {
    let drain = Json::new(std::io::stdout())
        .add_key_value(o!(
            "msg" => PushFnValue(move |record : &Record, ser| {
                ser.emit(record.msg())
            }),
        ))
        .build()
        .fuse();
    let drain = Async::new(drain).chan_size(8192).build().fuse();

    Logger::root(drain, o!())
}

fn main() {
    // Create the global logger
    let logger = create_logger();
    let _guard = slog_scope::set_global_logger(logger);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .max_blocking_threads(1024)  // Increase the blocking thread pool size
        .build()
        .unwrap();

    runtime.block_on(async {
        let cmd = Command::parse();
        if let Err(err) = cmd.execute().await {
            eprintln!("Error: {err:?}");
            std::process::exit(1);
        }
    });
}
