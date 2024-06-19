use clap::Args;
use eyre::{eyre, Report};
use futures::stream::FuturesUnordered;
use prometheus::{histogram_opts, opts, Encoder, Registry, TextEncoder, linear_buckets, exponential_buckets};
use slog_scope::info;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::process::exit;
use std::string::ToString;
use std::sync::atomic::{AtomicBool};
use std::sync::{Arc, LazyLock};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;
use duckdb::ffi::int_fast16_t;
use prometheus::core::{Atomic, AtomicU64};
use slog::Drain;
use tokio::sync::{Notify, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{sleep};
use tonic::transport::{Channel};
use tonic::Request;
use warp::http::Response;
use warp::{get, path, Filter};

use crate::cmd::cmd::BaseRpcConfig;
use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::{ContactInfoContentBody, HubInfoRequest, HubInfoResponse};
use crate::signals::handle_signals;

const MINIMUM_POLL_INTERVAL_MS: u32 = 3000;

type UnavailableHubSet = HashSet<ContactInfoContentBody>;

fn default_mainnet_bootstrap_peers() -> [BaseRpcConfig; 3] {
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
        // BaseRpcConfig {
        //     http: false,
        //     https: true,
        //     port: 2283,
        //     endpoint: "nemes.farcaster.xyz".to_string(),
        // },
        BaseRpcConfig {
            http: true,
            https: false,
            port: 2283,
            endpoint: "bootstrap.neynar.com".to_string(),
        },
    ]
}

impl Eq for ContactInfoContentBody{

}

impl Hash for ContactInfoContentBody{
    fn hash<H: Hasher>(&self, state: &mut H) {
        for addr in [&self.gossip_address, &self.rpc_address].iter() {
            addr.as_ref().map(|info| {
                info.address.hash(state);
                info.family.hash(state);
                info.port.hash(state);
                info.dns_name.hash(state);
            }).unwrap_or_else(|| 0.hash(state));
        }

        for hash in &self.excluded_hashes {
            hash.hash(state);
        }

        self.count.hash(state);
        self.hub_version.hash(state);
        self.network.hash(state);
        self.app_version.hash(state);
        self.timestamp.hash(state);
    }
}

#[derive(Args, Debug)]
pub struct WatchCommand {
    /// Duration in seconds to watch for. If 0, watch indefinitely.
    #[arg(long)]
    #[arg(default_value = "0")]
    #[arg(short = 't')]
    duration: Option<usize>,
}

struct WatchServer {
    notify: Arc<Notify>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    done: Arc<AtomicBool>,
    metrics_registry: &'static Registry,

    poll_interval_duration: Duration,

    peer_endpoints: PeerChannelMap,
    unavailable_peers: Arc<RwLock<UnavailableHubSet>>,
    unique_peers: Arc<RwLock<HubUniquePeers>>,
}

type PeerChannelMap = Arc<RwLock<HashMap<String,Channel>>>;

trait HubClientLoader {
    async fn hub_client_from_contact_info(&self, contact_info: &ContactInfoContentBody) -> eyre::Result<(BaseRpcConfig, HubInfoResponse, HubServiceClient<Channel>)>;
    async fn load_hub_client(&self, peer_id: String, conf: &BaseRpcConfig) -> eyre::Result<HubServiceClient<Channel>>;
}

impl HubClientLoader for PeerChannelMap {
    async fn hub_client_from_contact_info(&self, contact_info: &ContactInfoContentBody) -> eyre::Result<(BaseRpcConfig, HubInfoResponse, HubServiceClient<Channel>)> {
        let conf = BaseRpcConfig::from_contact_info(contact_info).await?;
        let endpoint = conf.load_endpoint()?;

        let mut client = HubServiceClient::connect(endpoint.clone()).await.map_err(|err| {
            info!("failed to connect to hub {:?} {:?}: {:?} {:#}", endpoint.uri().clone(), conf.clone(), err.source(), err);
            Report::new(err)
        })?;

        // HubServiceClient::get_info(&mut client, HubInfoRequest{db_stats: true}).await.map_err(|err| {
        //     info!("failed on info request: {:?} {:#}", err.source(), err);
        //     Report::new(err)
        // })?.into_inner();
        // let rpc_address = contact_info.rpc_address.clone().ok_or(eyre!("No rpc address found"))?;
        // let mut endpoint = format!("http://{}:{}", rpc_address.address, rpc_address.port);
        // let mut client: HubServiceClient<Channel>;

        // let result = HubServiceClient::connect(endpoint.clone()).await;
        // if result.is_err() {
        //     endpoint = format!("https://{}:{}", rpc_address.address, rpc_address.port);
        //     client = HubServiceClient::connect(endpoint.clone()).await?;
        // } else {
        //     client = result?;
        // }

        let peer_info = client.get_info(HubInfoRequest{db_stats: true}).await.map_err(|err| {
            info!("failed on info request: {:?} {:#}", err.source(), err);
            Report::new(err)
        })?.into_inner();

        // info!("Was able to get peer info",);
        let peer_id = peer_info.peer_id.clone();
        let mut map = self.write().await;
        if let Some(existing_channel) = map.get(&peer_id) {
            drop(endpoint);
            drop(client);
            client = HubServiceClient::new(existing_channel.clone());
        } else {
            // let channel = Channel::from_shared(endpoint.clone())?.connect().await?;
            // map.insert(peer_info.peer_id.clone(), channel);
            map.insert(peer_info.peer_id.clone(), endpoint.clone().connect().await?);
        }
        // map.remove(&peer_id);
        // map.insert(peer_id.clone(), endpoint.clone().connect_lazy());
        drop(map);

        Ok((conf, peer_info, client))
    }

    async fn load_hub_client(&self, peer_id: String, conf: &BaseRpcConfig) -> eyre::Result<HubServiceClient<Channel>> {
        let mut map = self.write().await;
        match map.get(&peer_id) {
            Some(channel) => {
                Ok(HubServiceClient::new(channel.clone()))
            }
            None => {
                info!("Channel not found for peer {}", peer_id,);
                match conf.load_endpoint() {
                    Ok(endpoint) => {
                        let channel = endpoint.connect().await?;
                        map.insert(peer_id.clone(), channel.clone());
                        Ok(HubServiceClient::new(channel))
                    },
                    Err(err) => {
                        Err(err)
                    }
                }
            }
        }
        // map.remove(&peer_id);
        // drop(map);
        // Ok(HubServiceClient::connect(conf.load_endpoint()?).await?)
    }
}

impl WatchServer {
    async fn new(notify: Arc<Notify>, poll_interval_ms: u32) -> eyre::Result<Self> {
        if poll_interval_ms < MINIMUM_POLL_INTERVAL_MS {
            return Err(eyre!(format!(
                "Poll interval must be at least {} ms",
                MINIMUM_POLL_INTERVAL_MS
            )));
        }
        let (registry, metrics) = WatchServer::initialize_metrics().await?;

        Ok(WatchServer {
            done: Arc::new(AtomicBool::new(false)),
            semaphore: Arc::new(Semaphore::new(1)),
            notify,
            metrics: Arc::new(metrics),
            metrics_registry: registry,
            poll_interval_duration: Duration::from_millis(poll_interval_ms as u64),

            peer_endpoints: Arc::new(RwLock::new(HashMap::new())),
            unavailable_peers: Arc::new(RwLock::new(UnavailableHubSet::new())),
            unique_peers: Arc::new(RwLock::new(HubUniquePeers::new())),
        })
    }

    async fn run(&self) -> eyre::Result<()> {
        // start metrics server
        let metrics_registry = self.metrics_registry.clone();
        let _metrics_handle = tokio::task::spawn(async {
            let metrics_route = path!("metrics").and(get()).map(move || {
                let metric_families = metrics_registry.gather();
                let mut buffer = Vec::new();
                let encoder = TextEncoder::new();
                encoder
                    .encode(&metric_families, &mut buffer)
                    .expect("Failed to encode metrics");
                Response::builder()
                    .header("Content-Type", encoder.format_type())
                    .body(buffer)
            });

            let routes = metrics_route.with(warp::log("telescope::watch"));
            warp::serve(routes).run(([0, 0, 0, 0], 9090)).await;
        });

        let notify = Arc::clone(&self.notify);
        let done = Arc::clone(&self.done);
        tokio::task::spawn(async move {
            notify.notified().await;
            done.store(true, SeqCst);
        });

        self.initialize_bootstrap_peers().await?;

        let mut poll_interval = tokio::time::interval(self.poll_interval_duration);
        loop {
            tokio::select! {
                _ = self.notify.notified() => {
                    self.done.store(true, SeqCst);
                    break;
                },
                _ = poll_interval.tick() => {
                    if self.done.load(SeqCst) {
                        break;
                    }

                    if let Ok(_permit) = self.semaphore.try_acquire() {
                        // Scope to automatically release the permit after use
                        {
                            info!("Starting new scan of peers in the network", );
                            self.watch_peers().await?;
                        }
                    } else {
                        info!("Skipping tick as previous task is still running",);
                    }
                }
            }
        }

        Ok(())
    }

    async fn initialize_metrics() -> eyre::Result<(&'static Registry, Metrics)> {
        let peers_per_hub = prometheus::Histogram::with_opts(histogram_opts!(
            "peers_per_hub",
            "Number of peers per hub",
            exponential_buckets(2.0, 2.0, 16)?,
        ))?;

        let total_fid_events_histogram = prometheus::Histogram::with_opts(histogram_opts!(
            "total_fid_events_histogram",
            "Total FID event count histogram across hubs",
            linear_buckets(600_000.0, 50_000.0, 10)?,
        ))?;

        let total_fname_events_histogram = prometheus::Histogram::with_opts(histogram_opts!(
            "total_fname_events_histogram",
            "Total Fname event count histogram across hubs",
            linear_buckets(510_000.0, 5_000.0, 10)?,
        ))?;

        let total_hub_count = prometheus::Gauge::with_opts(opts!(
            "total_hub_count",
            "Total number of hubs that are currently available"
        ))?;

        let total_messages_histogram = prometheus::Histogram::with_opts(histogram_opts!(
            "total_messages_histogram",
            "Total message count histogram across hubs",
            linear_buckets(420_000_000.0, 5_000_000.0, 10)?,
        ))?;

        let unavailable_hub_count = prometheus::Gauge::with_opts(opts!(
            "unavailable_hub_count",
            "Total number of hubs that are currently unavailable"
        ))?;

        let registry = prometheus::default_registry();

        registry.register(Box::new(peers_per_hub.clone()))?;
        registry.register(Box::new(total_fid_events_histogram.clone()))?;
        registry.register(Box::new(total_fname_events_histogram.clone()))?;
        registry.register(Box::new(total_hub_count.clone()))?;
        registry.register(Box::new(total_messages_histogram.clone()))?;
        registry.register(Box::new(unavailable_hub_count.clone()))?;

        Ok((
            registry,
            Metrics {
                peers_per_hub,
                total_fid_events_histogram,
                total_fname_events_histogram,
                total_hub_count,
                total_messages_histogram,
                unavailable_hub_count,
            },
        ))
    }

    async fn initialize_bootstrap_peers(&self) -> eyre::Result<()> {
        let bootstrap_peer_configs = default_mainnet_bootstrap_peers();
        for hub_conf in bootstrap_peer_configs.iter() {
            let endpoint = hub_conf.load_endpoint()?;
            let mut client = HubServiceClient::connect(endpoint).await?;

            let hub_info_response = client
                .get_info(HubInfoRequest { db_stats: false })
                .await
                .map_err(|err| {
                    info!("Failed to query hub for peers {}: {:#}", hub_conf.endpoint.clone(), err);
                    Report::new(err)
                })?;
            let hub_info = hub_info_response.into_inner();

            self.unique_peers.write().await.insert(
                hub_conf.clone(),
                HubInfo {
                    status: HubStatus::Available,
                    hub_info,
                },
            );
        }

        Ok(())
    }

    async fn traverse_peers(
        metrics: Arc<Metrics>,
        unique_peers: Arc<RwLock<HubUniquePeers>>,
        unavailable_peers: Arc<RwLock<UnavailableHubSet>>,
        peer_endpoints: Arc<RwLock<HashMap<String, Channel>>>,
    ) -> eyre::Result<()> {
        let current_peer_set = Arc::new(RwLock::new(HubUniquePeers::new()));

        let uniq: Vec<(BaseRpcConfig, HubInfo)> = unique_peers.read().await.iter().map(|(k, v)| {
            (k.clone(), v.clone())
        }).collect();
        let mut set: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));

        let peer_handles: FuturesUnordered<JoinHandle<eyre::Result<_>>> = Default::default();
        for (peer_conf, peer_info) in uniq {
            // info!("Processing peer {}", peer_info.hub_info.peer_id);

            let mut client = peer_endpoints.load_hub_client(peer_info.hub_info.peer_id, &peer_conf).await.map_err(|err| {
                info!("Yeah it's failing on the new loop {}: {:#}", peer_conf.endpoint.clone(), err);
                err
            })?;
            let hub_peers = client
                .get_current_peers(Request::new(Default::default()))
                .await
                .map_err(|err| {
                    info!("Failed to query hub for peers {}: {:#}", peer_conf.endpoint.clone(), err);
                    Report::new(err)
                })?.into_inner();

            metrics.peers_per_hub.observe(hub_peers.contacts.len() as f64);

            let mut count = 0;
            for peer in hub_peers.contacts {
                count += 1;
                let current_peer_set = Arc::clone(&current_peer_set);
                let peer = peer.clone();
                let peer_addr = peer.gossip_address.clone().unwrap().address;
                let peer_endpoints = Arc::clone(&peer_endpoints);
                set.write().await.insert(peer_addr.clone());

                if count > 10 {
                    break;
                }

                peer_handles.push(tokio::task::spawn(async move {
                    {
                        let permit = SEMAPHORE.acquire().await;
                        if permit.is_err() {
                            info!("Semaphore limit reached for unique peers, skipping",);
                            return Ok(());
                        }

                        let (conf, info, client) = peer_endpoints.hub_client_from_contact_info(&peer).await?;

                        {
                            let mut lock = current_peer_set.write().await;
                            if lock.contains_key(&conf) {
                                return Ok(());
                            }
                            lock.insert(conf.clone(), HubInfo {
                                status: HubStatus::Available,
                                hub_info: info,
                            });
                        }

                        Ok(())
                    }
                }))
            }
        }

        let results = futures::future::join_all(peer_handles).await;
        for res in results {
            if let Err(err) = res {
                info!("Join error found on traversing peers: {:#}", err);
            } else if let Ok(Err(err)) = res {
                info!("Error found traversing peers: {:#}", err);
            }
        }

        info!("got past results",);
        {
            let mut uniqp = unique_peers.write().await;
            current_peer_set.write().await.drain().for_each(|(k, v)| {
                uniqp.insert(k, v);
            });
        }

        let total_hub_count = set.read().await.len();
        let total_unavailable_hub_count = total_hub_count - unique_peers.read().await.len();
        metrics.total_hub_count.set(total_hub_count as f64);
        metrics.unavailable_hub_count.set(total_unavailable_hub_count as f64);

        Ok(())
    }

    async fn watch_peers(&self) -> eyre::Result<()> {
        let unique_peers = Arc::clone(&self.unique_peers);
        let uniq: Vec<(BaseRpcConfig, HubInfo)> = unique_peers.read().await.iter().map(|(k, v)| {
            (k.clone(), v.clone())
        }).collect();


        let peer_handles: FuturesUnordered<JoinHandle<eyre::Result<_>>> = Default::default();
        for (peer_conf, peer_info) in uniq {
            let metrics = Arc::clone(&self.metrics);
            let peer_conf = peer_conf.clone();
            let peer_endpoints = Arc::clone(&self.peer_endpoints);
            let uniqp = Arc::clone(&unique_peers);

            peer_handles.push(tokio::task::spawn(async move {
                {
                    let permit = SEMAPHORE.acquire().await;
                    if permit.is_err() {
                        info!("Semaphore limit reached for unique peers, skipping",);
                        return Ok(());
                    }

                    let mut client = peer_endpoints.load_hub_client(peer_info.hub_info.peer_id, &peer_conf).await?;

                    match client.get_info(HubInfoRequest {db_stats: true}).await {
                        Ok(response) => {
                            if let Some(db_stats) = response.into_inner().db_stats {
                                metrics.total_fid_events_histogram.observe(db_stats.num_fid_events as f64);
                                metrics.total_fname_events_histogram.observe(db_stats.num_fname_events as f64);
                                metrics.total_messages_histogram.observe(db_stats.num_messages as f64);
                            }
                        }
                        Err(err) => {
                            info!("Failed to query hub {:#}: {:#}", peer_conf.endpoint.clone(), err);
                        }
                    }

                    Ok(())
                }
            }))
        }

        let met = Arc::clone(&self.metrics);
        let uniqp = Arc::clone(&self.unique_peers);
        let unavp = Arc::clone(&self.unavailable_peers);
        let endps = Arc::clone(&self.peer_endpoints);
        peer_handles.push(tokio::task::spawn(async move {
            WatchServer::traverse_peers(met, uniqp, unavp, endps).await
        }));

        info!("Waiting for peer watch to finish",);
        let result = futures::future::join_all(peer_handles).await;
        for res in result {
            if let Err(e) = res {
                info!("Join error found on watch peers: {:?}", e);
            } else if let Ok(Err(e)) = res {
                info!("Error found for peer handler: {:?}", e);
            }
        }

        Ok(())
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
enum HubStatus {
    Unknown,
    Available,
    Unavailable,
    Intermittent, // Intermittent connectivity issues
}

type HubUniquePeers = HashMap<BaseRpcConfig, HubInfo>;

#[derive(Debug, Clone)]
struct HubInfo {
    status: HubStatus,
    hub_info: HubInfoResponse,
}

#[derive(Debug)]
struct Metrics {
    peers_per_hub: prometheus::Histogram,
    total_fid_events_histogram: prometheus::Histogram,
    total_fname_events_histogram: prometheus::Histogram,
    total_hub_count: prometheus::Gauge,
    total_messages_histogram: prometheus::Histogram,
    unavailable_hub_count: prometheus::Gauge,
}

const CONCURRENCY_LIMIT: usize = 256;
static SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(CONCURRENCY_LIMIT));

impl WatchCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let notify = Arc::new(Notify::new());
        let watch_server = WatchServer::new(Arc::clone(&notify), MINIMUM_POLL_INTERVAL_MS).await?;

        let notif = Arc::clone(&notify);
        tokio::task::spawn(async move {
            handle_signals(notif).await;
            sleep(Duration::from_secs(1)).await;
            exit(0);
        });

        info!("Starting watch server",);
        watch_server.run().await?;

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
