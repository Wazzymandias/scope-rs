use std::fs::File;
use std::io::{BufWriter, Write};
use std::str::FromStr;

use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Args, CommandFactory, Parser};
use eyre::eyre;
use rustls_native_certs::load_native_certs;
use tokio::runtime::Runtime;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

use crate::cmd::diff_cmd::DiffCommand;
use crate::cmd::info_cmd::InfoCommand;
use crate::cmd::messages_cmd::MessagesCommand;
use crate::cmd::parse_cmd::ParseCommand;
use crate::cmd::peers_cmd::PeersCommand;
use crate::proto::{Message, SyncIds, TrieNodePrefix};
use crate::proto::hub_service_client::HubServiceClient;

#[derive(Debug, Args, Clone)]
pub(crate) struct BaseConfig {
    #[arg(long)]
    #[arg(default_value = "true")]
    pub(crate) http: bool,

    #[arg(long)]
    #[arg(default_value = "false")]
    pub(crate) https: bool,

    #[arg(long, default_value = "2283")]
    pub(crate) port: u16,
}

#[derive(Parser, Debug)]
pub struct Command {
    #[clap(flatten)]
    base: BaseConfig,

    #[command(subcommand)]
    pub(crate) subcommand: Option<SubCommands>,
}

#[derive(Parser, Debug)]
pub enum SubCommands {
    Info(InfoCommand),
    Diff(DiffCommand),
    Peers(PeersCommand),
    SyncMetadata(SyncMetadataCommand),
    SyncSnapshot(SyncSnapshotCommand),
    Messages(MessagesCommand),
    Parse(ParseCommand),
}



#[derive(Args, Debug)]
pub struct SyncMetadataCommand {
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    endpoint: String,

    #[arg(long)]
    sync_id: Option<String>,

    /// Sets the prefix as a comma-separated string of bytes, e.g., --prefix 48,48,51
    #[arg(long)]
    prefix: Option<String>, // Use Option if prefix is optional
}

#[derive(Args, Debug)]
pub struct SyncSnapshotCommand {
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    endpoint: String,

    /// Sets the prefix as a comma-separated string of bytes, e.g., --prefix 48,48,51
    #[arg(long)]
    prefix: Option<String>, // Use Option if prefix is optional
}

// convert comma separated string into Vec<u8>
pub(crate) fn parse_prefix(input: &Option<String>) -> Result<Vec<u8>, std::num::ParseIntError> {
    match input {
        None => {
            return Ok(vec![]);
        }
        Some(input) => {
            if input.is_empty() {
                return Ok(vec![]);
            } else {
                input.split(',').map(|s| s.trim().parse()).collect()
            }
        }
    }
}

pub(crate) fn load_endpoint(base_config: &BaseConfig, endpoint: &String) -> eyre::Result<Endpoint> {
    let protocol = if base_config.https {
        "https"
    } else if base_config.http {
        "http"
    } else {
        return Err(eyre!("Invalid protocol"));
    };
    let endpoint: String = format!("{}://{}:{}", protocol, endpoint, base_config.port);
    if base_config.https {
        let native_certs = load_native_certs().expect("could not load native certificates");
        let mut combined_pem = Vec::new();
        for cert in native_certs {
            writeln!(&mut combined_pem, "-----BEGIN CERTIFICATE-----").unwrap();
            writeln!(&mut combined_pem, "{}", STANDARD.encode(&cert.to_vec())).unwrap();
            writeln!(&mut combined_pem, "-----END CERTIFICATE-----").unwrap();
        }

        let cert: Certificate = Certificate::from_pem(combined_pem);
        let tls_config = ClientTlsConfig::new().ca_certificate(cert);
        Ok(Endpoint::from_str(endpoint.as_str())
            .unwrap()
            .tls_config(tls_config)
            .unwrap())
    } else {
        Ok(Endpoint::from_str(endpoint.as_str()).unwrap())
    }
}

impl Command {
    pub async fn execute(self) -> eyre::Result<()> {
        match &self.subcommand {
            Some(subcommand) => match subcommand {
                SubCommands::Info(info) => info.execute().await?,
                SubCommands::Diff(diff) => diff.execute().await?,
                SubCommands::Messages(messages) => messages.execute().await?,
                SubCommands::Parse(parse) => parse.execute()?,
                SubCommands::Peers(peers) => peers.execute().await?,
                SubCommands::SyncMetadata(sync_metadata) => sync_metadata.execute().await?,
                SubCommands::SyncSnapshot(sync_snapshot) => sync_snapshot.execute()?,
            },
            _ => {
                Command::command().print_help()?;
            }
        }
        Ok(())
    }
}

impl SyncMetadataCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let tonic_endpoint = load_endpoint(&self.base, &self.endpoint)?;
        let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();
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
        // println!(
        //     "default: {}",
        //     serde_json::to_string_pretty(&TrieNodeMetadataResponse::default()).unwrap()
        // );
    }
}

impl SyncSnapshotCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let tonic_endpoint = load_endpoint(&self.base, &self.endpoint)?;
            let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();
            let prefix = parse_prefix(&self.prefix)?;

            let response = client
                .get_sync_snapshot_by_prefix(tonic::Request::new(TrieNodePrefix { prefix }))
                .await
                .unwrap();

            let str_response = serde_json::to_string_pretty(&response.into_inner());
            if str_response.is_err() {
                return Err(eyre!("{:?}", str_response.err()));
            }
            println!("{}", str_response.unwrap());
            Ok(())
        });

        result
    }
}

pub(crate) fn save_to_file(messages: &(Vec<Message>, Vec<Message>), first_path: &str, second_path: &str) -> eyre::Result<()> {
    let file1 = File::create(first_path)?;
    let (a, b) = messages;
    let writer1 = BufWriter::new(file1);
    serde_json::to_writer(writer1, a)?;

    let file2 = File::create(second_path)?;
    let writer2 = BufWriter::new(file2);
    serde_json::to_writer(writer2, b)?;
    Ok(())
}
