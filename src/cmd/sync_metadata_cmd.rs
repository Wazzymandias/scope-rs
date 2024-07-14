use clap::Args;
use eyre::eyre;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use crate::cmd::cmd::{BaseRpcConfig, parse_prefix};
use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::TrieNodePrefix;

#[derive(Args, Debug)]
pub struct SyncMetadataCommand {
    #[clap(flatten)]
    base: BaseRpcConfig,

    #[arg(long)]
    sync_id: Option<String>,

    /// Sets the prefix as a comma-separated string of bytes, e.g., --prefix 48,48,51
    #[arg(long)]
    prefix: Option<String>,

    #[arg(long)]
    prefixes: Option<String>,
}

impl SyncMetadataCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let tonic_endpoint = self.base.load_endpoint()?;
        let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();

        if self.prefixes.is_some() {
            let file = File::open(self.prefixes.as_ref().unwrap()).await?;
            let reader = tokio::io::BufReader::new(file);
            let mut prefixes = Vec::new();
            let mut lines = reader.lines();
            while let Some(line) = lines.next_line().await? {
                prefixes.push(line);
            }
            // let prefixes = self.prefixes.as_ref().unwrap().split(|c: &String| c == "\n").filter(|s| !s.is_empty()).collect::<Vec<_>>();
            let mut total = 0;
            for prefix in prefixes {
                println!("Processing prefix: {}", prefix);
                let input = prefix
                    // .join(",")
                    .trim()
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .split(|c: char| c.is_whitespace() || c == ',').filter(|s| !s.is_empty()).map(|s| s.trim().parse::<u8>().map_err(|e| eyre!("{:?}", e))).collect::<Result<Vec<u8>, eyre::Error>>()?;
                let response = client.get_sync_metadata_by_prefix(tonic::Request::new(TrieNodePrefix { prefix: input })).await?.into_inner();
                total += response.num_messages;
            }
            println!("Total messages: {}", total);

            Ok(())
        } else {
            let prefix = parse_prefix(&self.prefix)?;

            let response = client
                .get_sync_metadata_by_prefix(tonic::Request::new(TrieNodePrefix { prefix }))
                .await
                .unwrap();

            let str_response = serde_json::to_string_pretty(&response.into_inner());
            if str_response.is_err() {
                return Err(eyre!("{:?}", str_response.err()));
            }

            Ok(println!("{}", str_response.unwrap()))
        }

        // println!(
        //     "default: {}",
        //     serde_json::to_string_pretty(&TrieNodeMetadataResponse::default()).unwrap()
        // );
    }
}

