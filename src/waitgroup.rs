use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

pub struct WaitGroup {
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl WaitGroup {
    pub fn new() -> Self {
        WaitGroup {
            counter: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn add(&self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }

    pub fn done(&self) {
        if self.counter.fetch_sub(1, Ordering::SeqCst) > 0 {
            self.notify.notify_last();
        }
    }

    pub async fn wait(&self) {
        while self.counter.load(Ordering::SeqCst) != 0 {
            self.notify.notified().await;
        }
    }
}


