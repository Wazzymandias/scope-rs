use clap::Args;
use eyre::{eyre, Report};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use prometheus::{histogram_opts, opts, Encoder, Registry, TextEncoder};
use slog_scope::info;
use std::collections::HashMap;
use std::string::ToString;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Duration;
use tokio::signal;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tonic::transport::Endpoint;
use tonic::Request;
use warp::http::Response;
use warp::{get, path, Filter};

use crate::cmd::cmd::BaseRpcConfig;
use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::{ContactInfoContentBody, HubInfoRequest, HubInfoResponse};

fn default_mainnet_bootstrap_peers() -> [BaseRpcConfig; 4] {
    [
        BaseRpcConfig {
            http: false,
            https: true,
            port: 2283,
            endpoint: "hoyt.farcaster.xyz".to_string(),
        },
        BaseRpcConfig {
            http: false,
            https: true,
            port: 2283,
            endpoint: "lamia.farcaster.xyz".to_string(),
        },
        BaseRpcConfig {
            http: false,
            https: true,
            port: 2283,
            endpoint: "nemes.farcaster.xyz".to_string(),
        },
        BaseRpcConfig {
            http: false,
            https: true,
            port: 2283,
            endpoint: "bootstrap.neynar.com".to_string(),
        },
    ]
}

#[derive(Args, Debug)]
pub struct WatchCommand {
    /// Duration in seconds to watch for. If 0, watch indefinitely.
    #[arg(long)]
    #[arg(default_value = "0")]
    #[arg(short = 't')]
    duration: Option<usize>,
}

enum HubStatus {
    Available,
    Unavailable,
    Intermittent, // Intermittent connectivity issues
    
}

type HubUniquePeers = HashMap<BaseRpcConfig, HubInfoResponse>;
#[derive(Debug)]
struct Metrics {
    peers_per_hub: prometheus::Histogram,
    total_hub_count: prometheus::Gauge,
    total_messages_histogram: prometheus::Histogram,
    unavailable_hub_count: prometheus::Gauge,
}

const CONCURRENCY_LIMIT: usize = 1024;
static SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(CONCURRENCY_LIMIT));

impl WatchCommand {
    async fn initialize_metrics() -> eyre::Result<(&'static Registry, Metrics)> {
        let total_messages_histogram = prometheus::Histogram::with_opts(histogram_opts!(
            "total_messages_histogram",
            "Total message count histogram across hubs"
        ))?;

        let unavailable_hub_count = prometheus::Gauge::with_opts(opts!(
            "unavailable_hub_count",
            "Total number of hubs that are currently unavailable"
        ))?;

        let total_hub_count = prometheus::Gauge::with_opts(opts!(
            "total_hub_count",
            "Total number of hubs that are currently available"
        ))?;

        let registry = prometheus::default_registry();

        registry.register(Box::new(total_messages_histogram.clone()))?;
        registry.register(Box::new(unavailable_hub_count.clone()))?;
        registry.register(Box::new(total_hub_count.clone()))?;

        Ok((
            registry,
            Metrics {
                total_messages_histogram,
                unavailable_hub_count,
                total_hub_count,
            },
        ))
    }

    async fn initialize_peers() -> eyre::Result<HubUniquePeers> {
        let bootstrap_peer_configs = default_mainnet_bootstrap_peers();

        let mut peer_handles = FuturesUnordered::new();
        let unique_peers = Arc::new(tokio::sync::RwLock::new(HubUniquePeers::new()));

        for hub_conf in bootstrap_peer_configs.iter() {
            // 1. Query bootstrap node for hub info
            let endpoint = hub_conf.load_endpoint()?;
            let mut client =
                match HubServiceClient::connect(endpoint.connect_timeout(Duration::from_secs(2)))
                    .await
                {
                    Ok(client) => client,
                    Err(err) => {
                        info!(
                            "Failed to connect to hub {:?}: {:?}",
                            hub_conf.endpoint.clone(),
                            err
                        );
                        continue;
                    }
                };

            let hub_info_response = match client.get_info(HubInfoRequest { db_stats: false }).await
            {
                Ok(response) => response,
                Err(err) => {
                    info!(
                        "Failed to query hub {:?}: {:?}",
                        hub_conf.endpoint.clone(),
                        err
                    );
                    continue;
                }
            };
            let hub_info = hub_info_response.into_inner();

            // 2. For each bootstrap node, query for their current peers
            let hub_peers_response = client
                .get_current_peers(Request::new(crate::proto::Empty {}))
                .await?;
            let hub_peers = hub_peers_response.into_inner();
            info!("Received peers from hub: {:?}", hub_peers.contacts.len());

            // 3. For each peer, add to unique_peers
            for peer in hub_peers.contacts.iter() {
                let peer = peer.clone();
                let uniq = Arc::clone(&unique_peers);
                peer_handles.push(tokio::task::spawn(async move {
                    let _permit = SEMAPHORE.acquire().await.unwrap();
                    // 3.1 Convert ContactInfoContentBody to BaseRpcConfig
                    let peer_conf = BaseRpcConfig::from_contact_info(&peer).await;
                    match peer_conf {
                        Err(err) => {
                            info!(
                                "Failed to query peer {:?}: {:?}",
                                peer.rpc_address.as_ref(),
                                err.to_string()
                            );
                            Err(err)
                        }
                        Ok(peer_conf) => {
                            if uniq.read().await.contains_key(&peer_conf) {
                                return Ok(());
                            }
                            // 3.2 Query the peer for their hub information, which also confirms
                            // their connectivity / reachability
                            let peer_endpoint = peer_conf.load_endpoint()?;
                            let mut peer_client = HubServiceClient::connect(
                                peer_endpoint
                                    .connect_timeout(Duration::from_secs(1))
                                    .timeout(Duration::from_secs(1)),
                            )
                            .await?;
                            let peer_hub_info_response = peer_client
                                .get_info(HubInfoRequest { db_stats: false })
                                .await?;
                            let peer_hub_info = peer_hub_info_response.into_inner();
                            uniq.write().await.insert(peer_conf.clone(), peer_hub_info);
                            Ok(())
                        }
                    }
                }));
            }
            info!(
                "Finished querying peers for hub: {:?}",
                hub_conf.endpoint.to_string()
            );

            // 4. Add hub to unique_peers
            unique_peers
                .write()
                .await
                .insert(hub_conf.clone(), hub_info);
        }

        info!("Waiting for peer queries to complete",);
        let results = futures::future::join_all(peer_handles).await;
        for result in results {
            match result {
                Ok(result) => {}
                Err(err) => {
                    info!("Failed to query peer: {:?}", err.to_string());
                }
            }
        }

        if unique_peers.read().await.is_empty() {
            return Err(eyre!("No peers found"));
        }

        return match Arc::try_unwrap(unique_peers) {
            Ok(rwlock) => Ok(tokio::sync::RwLock::into_inner(rwlock)),
            Err(_) => Err(eyre!("Failed to unwrap unique peers")),
        };
    }

    pub async fn execute(&self) -> eyre::Result<()> {
        let (registry, metrics) = WatchCommand::initialize_metrics().await?;

        let metrics_route = path!("metrics").and(get()).map(move || {
            let metric_families = registry.gather();
            let mut buffer = Vec::new();
            let encoder = TextEncoder::new();
            encoder
                .encode(&metric_families, &mut buffer)
                .expect("Failed to encode metrics");
            Response::builder()
                .header("Content-Type", encoder.format_type())
                .body(buffer)
        });

        let unique_peers: HubUniquePeers = WatchCommand::initialize_peers().await?;
        info!(
            "Finished initializing peers for hub: {:?}",
            unique_peers.len()
        );

        if unique_peers.is_empty() {
            return Err(eyre!("No peers found"));
        }

        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    for (peer, hub_info) in unique_peers.iter() {
                        println!("Peer: {:?}", peer);
                        println!("Hub Info: {:?}", hub_info);
                    }
                }
                _ = signal::ctrl_c() => {
                    println!("Received Ctrl-C, exiting");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::hub_service_client::HubServiceClient;
    use crate::proto::HubInfoRequest;

    use super::*;

    #[tokio::test]
    async fn test_default_mainnet_bootstrap_peers() {
        assert_eq!(default_mainnet_bootstrap_peers().len(), 4);

        for peer in default_mainnet_bootstrap_peers().iter() {
            let endpoint_result = peer.load_endpoint();
            assert!(endpoint_result.is_ok());

            let endpoint = endpoint_result.unwrap();

            let client_result = HubServiceClient::connect(endpoint).await;
            assert!(client_result.is_ok());

            let mut client = client_result.unwrap();

            let result = client.get_info(HubInfoRequest { db_stats: false }).await;
            assert!(result.is_ok());

            let response = result.unwrap();

            let hub_info = response.into_inner();
            assert!(!hub_info.peer_id.is_empty());
        }
    }
}
