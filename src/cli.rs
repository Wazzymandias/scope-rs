use base64::{Engine, engine::general_purpose::STANDARD};
use std::io::Write;
use std::str::FromStr;

use clap::{Args, CommandFactory, Parser};
use eyre::eyre;
use rustls_native_certs::load_native_certs;
use tokio::runtime::Runtime;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::{Empty, HubInfoRequest};

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
    #[clap(flatten)]
    base: BaseConfig,

    #[arg(long)]
    source_endpoint: String,

    #[arg(long)]
    target_endpoint: String,
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
            writeln!(
                &mut combined_pem,
                "{}",
                STANDARD.encode(&cert.to_vec())
            )
            .unwrap();
            writeln!(&mut combined_pem, "-----END CERTIFICATE-----").unwrap();
        }

        let cert: Certificate = Certificate::from_pem(combined_pem);
        let tls_config = ClientTlsConfig::new().ca_certificate(cert);
        Ok(Endpoint::from_str(endpoint.as_str())
            .unwrap()
            .tls_config(tls_config)
            .unwrap()
        )
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
        println!("source_endpoint: {:?}", self.source_endpoint);
        println!("target_endpoint: {:?}", self.target_endpoint);
        Ok(())
    }
}

impl PeersCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        let rt = Runtime::new().unwrap();
        let result = rt.block_on(async {
            let protocol = if self.base.https {
                "https"

            } else if self.base.http {
                "http"
            } else {
                return Err(eyre!("Invalid protocol"));
            };


            let endpoint: String = format!("{}://{}:{}", protocol, self.endpoint, self.base.port);
            let tonic_endpoint = if self.base.https {
                let native_certs = load_native_certs().expect("could not load native certificates");
                let mut combined_pem = Vec::new();
                for cert in native_certs {
                    writeln!(&mut combined_pem, "-----BEGIN CERTIFICATE-----")?;
                    writeln!(
                        &mut combined_pem,
                        "{}",
                        base64::engine::general_purpose::STANDARD.encode(&cert.to_vec())
                    )?;
                    writeln!(&mut combined_pem, "-----END CERTIFICATE-----")?;
                }

                let cert = Certificate::from_pem(combined_pem);
                let tls_config = ClientTlsConfig::new().ca_certificate(cert);
                Endpoint::from_str(endpoint.as_str())
                    .unwrap()
                    .tls_config(tls_config)
                    .unwrap()
            } else {
                Endpoint::from_str(endpoint.as_str()).unwrap()
            };
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
