#![feature(duration_constructors)]
#![feature(new_uninit)]

extern crate core;

use clap::Parser;
use slog::{Drain, Logger, o, PushFnValue, Record};
use slog_async::Async;
use slog_json::Json;
use console_subscriber;

use crate::cmd::cmd::Command;

mod proto {
    include!("pb.rs");
}

mod cmd;
mod db;
mod farcaster;
mod hub_diff;
mod deque;
mod signals;
mod waitgroup;

fn create_logger() -> Logger {
    let drain = Json::new(std::io::stdout())
        .add_key_value(o!(
            "msg" => PushFnValue(move |record : &Record, ser| {
                ser.emit(record.msg())
            }),
        ))
        .build()
        .filter_level(slog::Level::Info)
        .fuse();
    let drain = Async::new(drain).chan_size(8192).build().fuse();

    Logger::root(drain, o!())
}

fn main() {
    console_subscriber::init();
    // Create the global logger
    let logger = create_logger();
    let _guard = slog_scope::set_global_logger(logger);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .max_blocking_threads(2048)  // Increase the blocking thread pool size
        .max_io_events_per_tick(2048)  // Increase the number of IO events per tick
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
