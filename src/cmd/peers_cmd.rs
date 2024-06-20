use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use clap::Args;
use eyre::eyre;
use tokio::sync::{Notify, RwLock};

use crate::cmd::cmd::BaseRpcConfig;
use crate::proto::Empty;
use crate::proto::hub_service_client::HubServiceClient;

#[derive(Args, Debug)]
pub struct PeersCommand {
    #[clap(flatten)]
    base: BaseRpcConfig,

    #[arg(long, default_value = "false")]
    validate_rpc: bool,
}


struct WaitGroup {
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl WaitGroup {
    fn new() -> Self {
        WaitGroup {
            counter: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    fn add(&self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }

    fn done(&self) {
        if self.counter.fetch_sub(1, Ordering::SeqCst) > 0 {
            self.notify.notify_last();
        }
    }

    async fn wait(&self) {
        while self.counter.load(Ordering::SeqCst) != 0 {
            self.notify.notified().await;
        }
    }
}


impl PeersCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let tonic_endpoint = self.base.load_endpoint()?;
        let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();
        let request = tonic::Request::new(Empty {});
        let response = client.get_current_peers(request).await.unwrap();

        let valid = Arc::new(RwLock::new(vec![]));
        let response = if self.validate_rpc {
            let wg = Arc::new(WaitGroup::new());
            for peer in response.into_inner().contacts {
                wg.add();
                let w = Arc::clone(&wg);
                let valid = Arc::clone(&valid);
                tokio::task::spawn(async move {
                    let conf = BaseRpcConfig::from_contact_info(&peer).await;
                    if let Ok((_, endp)) = conf {
                        if let Ok(client) = HubServiceClient::connect(endp).await {
                            valid.write().await.push(peer);
                            drop(client);
                        }
                    }
                    w.done();
                });
            }
            wg.wait().await;
            valid.read().await.to_vec()
        } else {
            response.into_inner().contacts
        };

        let str_response = serde_json::to_string_pretty(response.as_slice());
        if str_response.is_err() {
            return Err(eyre!("{:?}", str_response.err()));
        }
        println!("{}", str_response?);
        Ok(())
    }
}
