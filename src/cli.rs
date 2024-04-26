use std::fs::File;
use std::io::{BufWriter, Write};
use std::str::FromStr;

use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Args, CommandFactory, Parser};
use eyre::eyre;
use rustls_native_certs::load_native_certs;
use tokio::runtime::Runtime;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

use crate::farcaster::farcaster_to_unix;
use crate::hub_diff::HubStateDiffer;
use crate::proto::{Empty, HubInfoRequest, Message, SyncIds, TrieNodePrefix};
use crate::proto::hub_service_client::HubServiceClient;

#[derive(Debug, Args, Clone)]
struct BaseConfig {
    #[arg(long)]
    #[arg(default_value = "true")]
    http: bool,

    #[arg(long)]
    #[arg(default_value = "false")]
    https: bool,

    #[arg(long, default_value = "2283")]
    port: u16,
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
}

#[derive(Args, Debug)]
pub struct InfoCommand {
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    endpoint: String,
}

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

#[derive(Args, Debug)]
pub struct MessagesCommand {
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    endpoint: String,

    #[arg(long)]
    sync_id: Option<String>
}

#[derive(Args, Debug)]
pub struct PeersCommand {
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    endpoint: String,
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
fn parse_prefix(input: &Option<String>) -> Result<Vec<u8>, std::num::ParseIntError> {
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

fn load_endpoint(base_config: &BaseConfig, endpoint: &String) -> eyre::Result<Endpoint> {
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
                SubCommands::Peers(peers) => peers.execute().await?,
                SubCommands::Messages(messages) => messages.execute().await?,
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

impl InfoCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let endpoint = self.endpoint.clone();
        let base = self.base.clone();

        let tonic_endpoint = load_endpoint(&base, &endpoint).or_else(|e| Err(eyre!("{:?}", e)))?;
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
        let source_endpoint = load_endpoint(&source_config, &self.source_endpoint)?;
        let target_endpoint = load_endpoint(&target_config, &self.target_endpoint)?;

        let state_differ = HubStateDiffer::new(source_endpoint, target_endpoint);
        let (source, target) = state_differ.diff_exhaustive().await.or_else(|e| Err(eyre!("{:?}", e)))?;
        let output = serde_json::to_string_pretty(&(&source, &target)).or_else(|e| Err(eyre!("{:?}", e)))?;
        let mut hist = histogram::Histogram::new(7, 64)?;
        for message in target.iter() {
            if !source.contains(message) {
                slog_scope::info!("message not found in source: {:?}", message);
                hist.increment(message.clone().data.unwrap().timestamp.clone() as u64)?;
            }
        }
        println!("{:?}", histogram::SparseHistogram::from(&hist));
        save_to_file(&(source, target), "messages1.json", "messages2.json").or_else(|e| Err(eyre!("{:?}", e)))?;
        Ok(println!("{}", output))
    }
}

impl MessagesCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let tonic_endpoint = load_endpoint(&self.base, &self.endpoint)?;
        let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();
        let prefix = parse_prefix(&self.sync_id)?;
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

fn save_to_file(messages: &(Vec<Message>, Vec<Message>), first_path: &str, second_path: &str) -> eyre::Result<()> {
    let file1 = File::create(first_path)?;
    let (a, b) = messages;
    let writer1 = BufWriter::new(file1);
    serde_json::to_writer(writer1, a)?;

    let file2 = File::create(second_path)?;
    let writer2 = BufWriter::new(file2);
    serde_json::to_writer(writer2, b)?;
    Ok(())
}
