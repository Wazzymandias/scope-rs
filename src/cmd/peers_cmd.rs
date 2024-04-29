use crate::cmd::cmd::{load_endpoint, BaseConfig};
use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::Empty;
use clap::Args;
use eyre::eyre;

#[derive(Args, Debug)]
pub struct PeersCommand {
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    endpoint: String,
}

impl PeersCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let tonic_endpoint = load_endpoint(&self.base, &self.endpoint)?;
        let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();
        let request = tonic::Request::new(Empty {});
        let response = client.get_current_peers(request).await.unwrap();

        let str_response = serde_json::to_string_pretty(&response.into_inner());
        if str_response.is_err() {
            return Err(eyre!("{:?}", str_response.err()));
        }
        println!("{}", str_response.unwrap());
        Ok(())
    }
}
