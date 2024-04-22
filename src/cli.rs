use clap::{Args, CommandFactory, Parser};
use tokio::runtime::Runtime;

use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::HubInfoRequest;

#[derive(Parser, Debug)]
pub struct Command {
    #[command(subcommand)]
    pub(crate) subcommand: Option<SubCommands>,
}

#[derive(Parser, Debug)]
pub enum SubCommands {
    Info(InfoCommand),
    Diff(DiffCommand),
}

#[derive(Args, Debug)]
pub struct InfoCommand {
    #[arg(long)]
    endpoint : String,

    #[arg(long)]
    port : u16,
}

#[derive(Args, Debug)]
pub struct DiffCommand {
    #[arg(long)]
    source_endpoint : String,

    #[arg(long)]
    target_endpoint : String,
}

impl Command {
    pub fn execute(self) -> eyre::Result<()> {
        match &self.subcommand {
            Some(subcommand) => match subcommand {
                SubCommands::Info(info) => info.execute()?,
                SubCommands::Diff(diff) => diff.execute()?,
            },
            None => {
                Command::command().print_help()?;
            }
        }
        Ok(())
    }
}

impl InfoCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let endpoint: String = format!("http://{}:{}", self.endpoint, self.port);
            let mut client = HubServiceClient::connect(endpoint).await.unwrap();
            let request = tonic::Request::new(HubInfoRequest { db_stats: true });
            let response = client.get_info(request).await.unwrap();

            let str_response = serde_json::to_string_pretty(&response.into_inner());
            if str_response.is_err() {
                println!("Error: {:?}", str_response.err());
                return;
            }
            println!("{}", str_response.unwrap());
        });
        Ok(())
    }
}

impl DiffCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        println!("source_endpoint: {:?}", self.source_endpoint);
        println!("target_endpoint: {:?}", self.target_endpoint);
        Ok(())
    }
}
