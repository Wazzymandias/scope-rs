use std::fs::File;
use std::io::{BufWriter, Write};
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Args, CommandFactory, Parser};
use eyre::eyre;
use rustls_native_certs::load_native_certs;
use tokio::runtime::Runtime;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use crate::cmd::cmd::SubCommands::Parse;

use crate::cmd::diff_cmd::DiffCommand;
use crate::cmd::info_cmd::InfoCommand;
use crate::cmd::messages_cmd::MessagesCommand;
use crate::cmd::parse_cmd::ParseCommand;
use crate::cmd::peers_cmd::PeersCommand;
use crate::cmd::sync_ids_cmd::SyncIdsCommand;
use crate::cmd::watch_cmd::WatchCommand;
use crate::proto::{ContactInfoContentBody, Message, TrieNodePrefix};
use crate::proto::hub_service_client::HubServiceClient;

#[derive(Debug, Args, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BaseRpcConfig {
    #[arg(long)]
    #[arg(default_value = "true")]
    pub(crate) http: bool,

    #[arg(long)]
    #[arg(default_value = "false")]
    pub(crate) https: bool,

    #[arg(long, default_value = "2283")]
    pub(crate) port: u16,

    #[arg(long)]
    pub(crate) endpoint: String,
}

impl BaseRpcConfig {
    pub fn load_endpoint(&self) -> eyre::Result<Endpoint> {
        let protocol = if self.https {
            "https"
        } else if self.http {
            "http"
        } else {
            return Err(eyre!("Invalid protocol"));
        };
        let endpoint: String = format!(
            "{}://{}:{}",
            protocol, self.endpoint, self.port
        );

        if self.https {
            Ok(Endpoint::from_str(endpoint.as_str())
                .unwrap()
                .keep_alive_while_idle(false)
                .tcp_keepalive(None)
                .timeout(Duration::from_secs(10))
                .http2_adaptive_window(false)
                .connect_timeout(Duration::from_secs(10))
                .tcp_nodelay(true)
                .tls_config(get_tls_config().clone())
                .unwrap())
        } else {
            Ok(Endpoint::from_str(endpoint.as_str()).unwrap()
                .keep_alive_while_idle(false)
                .tcp_keepalive(None)
                .http2_adaptive_window(false)
                .timeout(Duration::from_secs(10))
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_secs(10)))
        }
    }
    pub async fn from_contact_info(contact_info: &ContactInfoContentBody) -> eyre::Result<Self> {
        match contact_info.rpc_address.as_ref() {
            None => Err(eyre!("No rpc address found")),
            Some(rpc_info) => {
                let https_conf = BaseRpcConfig {
                    http: false,
                    https: true,
                    port: rpc_info.port as u16,
                    endpoint: rpc_info.address.clone(),
                };
                let http_conf = BaseRpcConfig {
                    http: true,
                    https: false,
                    port: rpc_info.port as u16,
                    endpoint: rpc_info.address.clone(),
                };

                let http_result = http_conf.load_endpoint();
                let http_error = match http_result {
                    Ok(endpoint) => {
                        endpoint
                            .connect()
                            .await
                            .map_err(|err| eyre!("Failed to connect to http endpoint: {:?}", err))
                    }
                    Err(e) => Err(e),
                };

                match http_error {
                    Ok(_) => Ok(http_conf),
                    Err(http_err) => {
                        let https_result = https_conf.load_endpoint();
                        match https_result {
                            Ok(endpoint) => {
                                endpoint
                                    .connect()
                                    .await
                                    .map(|_| https_conf)
                                    .map_err(|err| eyre!("Failed to connect to https endpoint: {:?}, http endpoint: {:?}", err, http_err))
                            },
                            Err(https_err) => Err(eyre!("Failed to connect to http endpoint: {:?}, https endpoint: {:?}", http_err, https_err)),
                        }
                    }
                }
            }
        }
    }
}

#[derive(Parser, Debug)]
pub struct Command {
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
    SyncIds(SyncIdsCommand),
    Messages(MessagesCommand),
    Parse(ParseCommand),
    Watch(WatchCommand),
}

#[derive(Args, Debug)]
pub struct SyncMetadataCommand {
    #[clap(flatten)]
    base: BaseRpcConfig,

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
    base: BaseRpcConfig,

    #[arg(long)]
    endpoint: String,

    /// Sets the prefix as a comma-separated string of bytes, e.g., --prefix 48,48,51
    #[arg(long)]
    prefix: Option<String>,
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

static TLS_CONFIG: OnceLock<ClientTlsConfig> = OnceLock::new();

fn initialize_tls_config() -> ClientTlsConfig {
    let native_certs = load_native_certs().expect("could not load native certificates");
    let mut combined_pem = Vec::new();
    for cert in native_certs {
        writeln!(&mut combined_pem, "-----BEGIN CERTIFICATE-----").unwrap();
        writeln!(&mut combined_pem, "{}", STANDARD.encode(&cert.to_vec())).unwrap();
        writeln!(&mut combined_pem, "-----END CERTIFICATE-----").unwrap();
    }

    let cert: Certificate = Certificate::from_pem(combined_pem);
    ClientTlsConfig::new().ca_certificate(cert)
}

fn get_tls_config() -> &'static ClientTlsConfig {
    TLS_CONFIG.get_or_init(initialize_tls_config)
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
                SubCommands::SyncIds(sync_ids) => sync_ids.execute().await?,
                SubCommands::Watch(watch) => watch.execute().await?,
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
        let tonic_endpoint = self.base.load_endpoint()?;
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
            let tonic_endpoint = self.base.load_endpoint()?;
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

pub(crate) fn save_to_file(
    messages: &(Vec<Message>, Vec<Message>),
    first_path: &str,
    second_path: &str,
) -> eyre::Result<()> {
    let file1 = File::create(first_path)?;
    let (a, b) = messages;
    let writer1 = BufWriter::new(file1);
    serde_json::to_writer(writer1, a)?;

    let file2 = File::create(second_path)?;
    let writer2 = BufWriter::new(file2);
    serde_json::to_writer(writer2, b)?;
    Ok(())
}
