use tokio::signal::unix::{signal, SignalKind};


pub async fn handle_signals() {
    let mut term_signal = signal(SignalKind::terminate()).expect("failed to set up SIGTERM handler");
    let mut int_signal = signal(SignalKind::interrupt()).expect("failed to set up SIGINT handler");
    let mut hup_signal = signal(SignalKind::hangup()).expect("failed to set up SIGHUP handler");

    tokio::select! {
        _ = term_signal.recv() => {
            println!("Received SIGTERM, shutting down...");
        },
        _ = int_signal.recv() => {
            println!("Received SIGINT, shutting down...");
        },
        _ = hup_signal.recv() => {
            println!("Received SIGHUP, shutting down...");
        }
    }

}
