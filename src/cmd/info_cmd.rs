use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::HubInfoRequest;
use clap::Args;
use eyre::eyre;

#[derive(Args, Debug)]
pub struct InfoCommand {
    #[clap(flatten)]
    base: crate::cmd::cmd::BaseRpcConfig,
}

impl InfoCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let base = self.base.clone();

        let tonic_endpoint =
            crate::cmd::cmd::load_endpoint(&base).or_else(|e| Err(eyre!("{:?}", e)))?;
        let mut client = HubServiceClient::connect(tonic_endpoint).await?;
        let request = tonic::Request::new(HubInfoRequest { db_stats: true });
        let response = client.get_info(request).await?;

        let str_response = serde_json::to_string_pretty(&response.into_inner());
        if let Err(e) = str_response {
            return Err(eyre!("{:?}", e));
        }

        Ok(println!("{}", str_response.unwrap()))
    }
}
