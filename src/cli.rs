use base64::{engine::general_purpose::STANDARD, Engine};
use std::io::Write;
use std::str::FromStr;

use crate::proto;
use clap::{Args, CommandFactory, Parser};
use eyre::eyre;
use prost::{bytes, Message};
use rustls_native_certs::load_native_certs;
use tokio::runtime::Runtime;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use crate::hub_diff::HubStateDiffer;

use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::{Empty, HubInfoRequest, TrieNodePrefix};
use crate::sync_id::{SyncId, UnpackedSyncId};

#[derive(Debug, Args)]
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
}

#[derive(Args, Debug)]
pub struct PeersCommand {
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    endpoint: String,
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
    pub fn execute(self) -> eyre::Result<()> {
        match &self.subcommand {
            Some(subcommand) => match subcommand {
                SubCommands::Info(info) => info.execute()?,
                SubCommands::Diff(diff) => diff.execute()?,
                SubCommands::Peers(peers) => peers.execute()?,
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
        let result = rt.block_on(async {
            let tonic_endpoint = load_endpoint(&self.base, &self.endpoint)?;
            let mut client = HubServiceClient::connect(tonic_endpoint).await.unwrap();
            let request = tonic::Request::new(HubInfoRequest { db_stats: true });
            let response = client.get_info(request).await.unwrap();

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

impl DiffCommand {
    pub fn execute(&self) -> eyre::Result<()> {
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
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let source_client = HubServiceClient::connect(source_endpoint).await.unwrap();
            let target_client = HubServiceClient::connect(target_endpoint).await.unwrap();
            let state_differ = HubStateDiffer::new(source_client, target_client);
            let result = state_differ._diff().await?;
            println!("{:?}", result);
            Ok(())
        });
        result
    }
}

impl PeersCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
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
        });

        result
    }
}
