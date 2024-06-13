use crate::proto::TrieNodePrefix;
use clap::Args;
use eyre::eyre;

#[derive(Debug, Args)]
pub(crate) struct SyncIdsCommand {
    #[clap(flatten)]
    base: crate::cmd::cmd::BaseRpcConfig,

    #[arg(long)]
    prefix: String,
}

impl SyncIdsCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let prefix = crate::cmd::cmd::parse_prefix(&Some(self.prefix.clone()))?;
        let endpoint = crate::cmd::cmd::load_endpoint(&self.base)?;
        let mut client =
            crate::proto::hub_service_client::HubServiceClient::connect(endpoint).await?;
        let response = client
            .get_all_sync_ids_by_prefix(TrieNodePrefix { prefix })
            .await?;
        let str_response = serde_json::to_string_pretty(&response.into_inner());
        match str_response {
            Ok(resp) => {
                println!("{}", resp);
                Ok(())
            }
            Err(e) => Err(eyre!("{:?}", e)),
        }
    }
}
