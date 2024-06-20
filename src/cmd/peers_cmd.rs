use std::sync::Arc;

use clap::Args;
use eyre::eyre;
use tokio::sync::RwLock;

use crate::cmd::cmd::BaseRpcConfig;
use crate::proto::Empty;
use crate::proto::hub_service_client::HubServiceClient;
use crate::waitgroup::WaitGroup;

#[derive(Args, Debug)]
pub struct PeersCommand {
    #[clap(flatten)]
    base: BaseRpcConfig,

    #[arg(long, default_value = "false")]
    validate_rpc: bool,
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
