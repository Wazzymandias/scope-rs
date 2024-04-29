use clap::Args;
use eyre::eyre;
use crate::cmd::cmd::{BaseConfig, load_endpoint};
use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::SyncIds;

#[derive(Args, Debug)]
pub struct MessagesCommand {
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    endpoint: String,

    #[arg(long)]
    sync_id: Option<String>
}

impl MessagesCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let tonic_endpoint = load_endpoint(&self.base, &self.endpoint)?;
        let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();
        let prefix = crate::cmd::cmd::parse_prefix(&self.sync_id)?;
        let response = client
            .get_all_messages_by_sync_ids(SyncIds { sync_ids: vec![prefix] }).await.unwrap();

        let str_response = serde_json::to_string_pretty(&response.into_inner());
        if str_response.is_err() {
            return Err(eyre!("{:?}", str_response.err()));
        }
        println!("{}", str_response.unwrap());
        Ok(())
    }
}
