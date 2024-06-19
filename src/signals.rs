use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Notify;


pub async fn handle_signals(notify: Arc<Notify>) {
    let mut term_signal = signal(SignalKind::terminate()).expect("failed to set up SIGTERM handler");
    let mut int_signal = signal(SignalKind::interrupt()).expect("failed to set up SIGINT handler");
    let mut hup_signal = signal(SignalKind::hangup()).expect("failed to set up SIGHUP handler");

    tokio::select! {
        _ = term_signal.recv() => {
            println!("Received SIGTERM, shutting down...");
            notify.notify_waiters();
        },
        _ = int_signal.recv() => {
            println!("Received SIGINT, shutting down...");
            notify.notify_waiters();
        },
        _ = hup_signal.recv() => {
            println!("Received SIGHUP, shutting down...");
            notify.notify_waiters();
        }
    }

}
