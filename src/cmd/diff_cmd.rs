use crate::cmd::cmd::BaseConfig;
use clap::Args;
use eyre::eyre;

use crate::hub_diff::HubStateDiffer;
#[derive(Args, Debug)]
pub struct DiffCommand {
    #[arg(long)]
    source_endpoint: String,

    #[arg(long, default_value = "2283")]
    source_port: u16,

    #[arg(long, default_value = "false")]
    source_https: bool,

    #[arg(long, default_value = "true")]
    source_http: bool,

    #[arg(long)]
    target_endpoint: String,

    #[arg(long, default_value = "2283")]
    target_port: u16,

    #[arg(long, default_value = "false")]
    target_https: bool,

    #[arg(long, default_value = "true")]
    target_http: bool,

    #[arg(long)]
    event_type: Option<String>,

    #[arg(long)]
    since_hours: Option<u64>,

    #[arg(long)]
    exhaustive: Option<bool>,
}

impl DiffCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let source_config: BaseConfig = BaseConfig {
            http: self.source_http,
            https: self.source_https,
            port: self.source_port,
        };
        let target_config: BaseConfig = BaseConfig {
            http: self.target_http,
            https: self.target_https,
            port: self.target_port,
        };
        let source_endpoint =
            crate::cmd::cmd::load_endpoint(&source_config, &self.source_endpoint)?;
        let target_endpoint =
            crate::cmd::cmd::load_endpoint(&target_config, &self.target_endpoint)?;

        let state_differ = HubStateDiffer::new(source_endpoint, target_endpoint);
        let sync_id_diff_report = state_differ?
            .diff_sync_ids()
            .await
            .or_else(|e| Err(eyre!("{:?}", e)))?;
        Ok(println!("{:?}", sync_id_diff_report))
    }
}
