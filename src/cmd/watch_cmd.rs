use clap::Args;
use eyre::{eyre, Report};
use futures::stream::FuturesUnordered;
use prometheus::{histogram_opts, opts, Encoder, Registry, TextEncoder, linear_buckets, exponential_buckets};
use slog_scope::info;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::process::exit;
use std::string::ToString;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, LazyLock};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;
use futures::{FutureExt, TryFutureExt};
use prometheus::core::Atomic;
use slog::Drain;
use tokio::sync::{Notify, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{sleep};
use warp::http::Response;
use warp::{get, path, Filter};

use crate::cmd::cmd::BaseRpcConfig;
use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::{ContactInfoContentBody, Empty, HubInfoRequest, HubInfoResponse};
use crate::signals::handle_signals;

const MINIMUM_POLL_INTERVAL_MS: u32 = 5000;
const CONCURRENCY_LIMIT: usize = 512;
static SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(CONCURRENCY_LIMIT));

#[derive(Clone, Debug)]
struct HubContact {
    inner: ContactInfoContentBody,
}

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

impl PartialEq for HubContact{
    fn eq(&self, other: &Self) -> bool {
        // self.inner.gossip_address == other.inner.gossip_address &&
        //     self.inner.rpc_address == other.inner.rpc_address &&
        //     self.inner.hub_version == other.inner.hub_version &&
        //     self.inner.network == other.inner.network &&
        //     self.inner.app_version == other.inner.app_version
        self.inner.rpc_address == other.inner.rpc_address
    }
}

impl Eq for HubContact{

}

impl Hash for HubContact {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // for addr in [self.inner.gossip_address.as_ref(), self.inner.rpc_address.as_ref()].iter() {
        //     addr.as_ref().map(|&info| {
        //         info.address.hash(state);
        //         info.family.hash(state);
        //         info.port.hash(state);
        //         info.dns_name.hash(state);
        //     }).unwrap_or_else(|| 0.hash(state));
        // }
        //
        // self.inner.hub_version.hash(state);
        // self.inner.network.hash(state);
        // self.inner.app_version.hash(state);
        self.inner.rpc_address.as_ref().unwrap().address.hash(state);
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

    unique_peers: Arc<RwLock<HubUniquePeers>>,
    unreachable_peers: Arc<RwLock<HashMap<BaseRpcConfig, String>>>,
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

            unique_peers: Arc::new(RwLock::new(HubUniquePeers::new())),
            unreachable_peers: Arc::new(RwLock::new(HashMap::new())),
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
                            WatchServer::traverse_peers(Arc::clone(&self.metrics), Arc::clone(&self.unique_peers), Arc::clone(&self.unreachable_peers)).await?;
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

        let total_messages_histogram = prometheus::Histogram::with_opts(histogram_opts!(
            "total_messages_histogram",
            "Total message count histogram across hubs",
            linear_buckets(420_000_000.0, 5_000_000.0, 10)?,
        ))?;

        let total_hub_count = prometheus::Gauge::with_opts(opts!(
            "total_hub_count",
            "Total number of hubs that are currently available"
        ))?;

        let total_unique_endpoints = prometheus::Gauge::with_opts(opts!(
            "total_unique_endpoints",
            "Total number of unique endpoints found"
        ))?;

        let available_hub_count = prometheus::Gauge::with_opts(opts!(
            "total_hub_count_available",
            "Total number of hubs that are currently available"
        ))?;

        let unavailable_hub_count = prometheus::Gauge::with_opts(opts!(
            "total_hub_count_unavailable",
            "Total number of hubs that are currently unavailable"
        ))?;

        let peer_address_change_count = prometheus::Counter::with_opts(opts!(
            "peer_address_change_count",
            "Total number of times a peer's address has changed"
        ))?;

        let registry = prometheus::default_registry();

        registry.register(Box::new(peers_per_hub.clone()))?;
        registry.register(Box::new(total_fid_events_histogram.clone()))?;
        registry.register(Box::new(total_fname_events_histogram.clone()))?;
        registry.register(Box::new(total_hub_count.clone()))?;
        registry.register(Box::new(total_messages_histogram.clone()))?;
        registry.register(Box::new(total_unique_endpoints.clone()))?;
        registry.register(Box::new(available_hub_count.clone()))?;
        registry.register(Box::new(unavailable_hub_count.clone()))?;
        registry.register(Box::new(peer_address_change_count.clone()))?;

        Ok((
            registry,
            Metrics {
                peers_per_hub,
                total_fid_events_histogram,
                total_fname_events_histogram,
                total_hub_count,
                total_messages_histogram,
                total_unique_endpoints,
                available_hub_count,
                unavailable_hub_count,
                peer_address_change_count,
            },
        ))
    }

    async fn initialize_bootstrap_peers(&self) -> eyre::Result<()> {
        let bootstrap_peer_configs = default_mainnet_bootstrap_peers();
        for hub_conf in bootstrap_peer_configs.iter() {
            let endpoint = hub_conf.load_endpoint()?;
            let mut client = HubServiceClient::connect(endpoint).await?;

            if let Ok(response) = client
                .get_info(HubInfoRequest { db_stats: false })
                .await
                .map_err(|err| {
                    info!("Failed to query hub for peers {}: {:#}", hub_conf.endpoint.clone(), err);
                    Report::new(err)
                }) {
                let hub_info = response.into_inner();
                self.unique_peers.write().await.insert(
                    hub_info.peer_id.clone(),
                    HubInfo {
                        status: HubStatus::Available,
                        hub_info,
                        rpc_config: hub_conf.clone(),
                    },
                );
            }
        }

        if self.unique_peers.read().await.is_empty() {
            return Err(eyre!("Failed to initialize any bootstrap peers"));
        }
        Ok(())
    }

    async fn find_unique_contacts(
        metrics: Arc<Metrics>,
        unique_peers: Arc<RwLock<HubUniquePeers>>,
        unreachable: Arc<RwLock<HashMap<BaseRpcConfig, String>>>
    ) -> eyre::Result<HashSet<HubContact>> {
        let peer_handles: FuturesUnordered<JoinHandle<eyre::Result<_>>> = Default::default();

        info!("Attempting to read unique peers",);
        // let mut uniq: Vec<(BaseRpcConfig, HubInfo)> = vec![];
        // {
        //     let read_guard = unique_peers.read().await;
        //     if read_guard.is_empty() {
        //         return Err(eyre!("No unique peers found"));
        //     }
        //
        //     if read_guard.len() <= 25 {
        //         uniq.extend(read_guard.iter().map(|(conf, info)| (conf.clone(), info.clone())));
        //     } else {
        //         let keys = read_guard.keys().cloned().collect::<Vec<BaseRpcConfig>>();
        //         let mut rng = rand::thread_rng();
        //         let sample = rand::seq::index::sample(&mut rng, keys.len(), 100);
        //         sample.iter().for_each(|idx| {
        //             let key = &keys[idx];
        //             if let Some(info) = read_guard.get(key) {
        //                 uniq.push((key.clone(), info.clone()));
        //             }
        //         });
        //     }
        // }
        let uniq: HashMap<String, HubInfo> = unique_peers.read()
            .await
            .iter()
            .map(|(peer_id, info)| (peer_id.clone(), info.clone()))
            .collect();
        info!("Read unique peers [{}]", uniq.len());

        let semaphore = Arc::new(Semaphore::new(256));
        for (peer_id, peer_info) in uniq.iter() {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let metrics = Arc::clone(&metrics);
            let peer_id = peer_id.clone();
            let peer_info = peer_info.clone();
            let unreachable = Arc::clone(&unreachable);

            peer_handles.push(tokio::task::spawn(async move {
                let timeout = Duration::from_secs(5);
                let result = tokio::select! {
                    result = async {
                        let channel = peer_info.rpc_config.load_endpoint()?.connect().await?;
                        HubServiceClient::new(channel).get_current_peers(Empty {})
                            .await
                            .map(|response| response.into_inner())
                            .map_err(|err| Report::new(err))
                    } => {
                        if result.is_err() {
                            unreachable.write().await.insert(peer_info.rpc_config.clone(), peer_id);
                        }
                        result
                    }
                    _ = tokio::time::sleep(timeout) => {
                        Err(eyre!("Timeout waiting for response from peer"))
                    }
                };

                drop(permit);
                result
            }))
        }

        let results = futures::future::join_all(peer_handles).await;
        let known_unreachable = unreachable.read().await.len();
        info!("Finished scanning peers [unreachable: {}]", known_unreachable);

        let mut uniq_contacts = HashSet::new();
        let mut uniq_rpc: HashSet<String> = HashSet::new();
        for res in results {
            match res {
                Ok(Ok(contacts)) => {
                    metrics.peers_per_hub.observe(contacts.contacts.len() as f64);
                    uniq_rpc.extend(contacts.contacts.iter().map(|contact| format!("{}:{}", contact.clone().rpc_address.unwrap().address.clone(), contact.clone().rpc_address.unwrap().port)));
                    let input = contacts.contacts.into_iter()
                        .map(|contact| HubContact{ inner: contact })
                        .filter(|contact| !uniq_contacts.contains(contact)).collect::<HashSet<HubContact>>();
                    uniq_contacts.extend(input);
                },
                Ok(Err(err)) => {
                    info!("Error found on finding unique contacts: {:#}", err);
                },
                Err(err) => {
                    info!("Join error found on finding unique contacts: {:#}", err);
                }
            }
        }

        Ok(uniq_contacts)
    }

    // Traverse peers first iterates through existing known hubs and queries their current peers
    // It returns that set of unique hubs and queries each of their info endpoints to see if they
    // are reachable
    async fn traverse_peers(
        metrics: Arc<Metrics>,
        unique_peers: Arc<RwLock<HubUniquePeers>>,
        unreachable: Arc<RwLock<HashMap<BaseRpcConfig, String>>>
    ) -> eyre::Result<()> {
        let unique_contacts = WatchServer::find_unique_contacts(Arc::clone(&metrics), Arc::clone(&unique_peers), Arc::clone(&unreachable)).await?;
        let total = unique_contacts.len();

        // let uniq = unique_contacts.iter().map(|entry| entry.clone()).choose_multiple(&mut rand::thread_rng(), 512);
        let uniq = unique_contacts;
        info!("Unique contacts found: [{}]", total);

        let peer_handles: FuturesUnordered<JoinHandle<eyre::Result<_>>> = Default::default();
        let semaphore = Arc::new(Semaphore::new(256));
        for contact in uniq {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let metrics = Arc::clone(&metrics);

            peer_handles.push(tokio::task::spawn(async move {
                let timeout = Duration::from_secs(5);
                let result = tokio::select! {
                    result = async {
                        let (conf, endp) = BaseRpcConfig::from_contact_info(&contact.inner).await?;
                        let response = HubServiceClient::connect(endp)
                            .await?
                            .get_info(HubInfoRequest {db_stats: true})
                            .await?
                            .into_inner();

                        if let Some(db_stats) = &response.db_stats {
                            metrics.total_fid_events_histogram.observe(db_stats.num_fid_events as f64);
                            metrics.total_fname_events_histogram.observe(db_stats.num_fname_events as f64);
                            metrics.total_messages_histogram.observe(db_stats.num_messages as f64);
                        }

                        Ok((response.peer_id.clone(), HubInfo {
                            status: HubStatus::Available,
                            hub_info: response,
                            rpc_config: conf,
                        }))
                    } => {
                        result
                    }
                    _ = tokio::time::sleep(timeout) => {
                        Err(eyre!("Timeout waiting for response from peer"))
                    }
                };
                drop(permit);
                result
            }))
        }

        let results = futures::future::join_all(peer_handles).await;
        let available = AtomicU32::new(0);
        let unavailable = AtomicU32::new(0);
        let mut errors = Vec::new();

        let mut output = vec![];
        for res in results {
            match res {
                Ok(Ok((peer_id, info))) => {
                    output.push((peer_id, info));
                    available.fetch_add(1, SeqCst);
                }
                Ok(Err(err)) => {
                    errors.push(err);
                    unavailable.fetch_add(1, SeqCst);
                },
                Err(err) => {
                    errors.push(Report::new(err));
                    // unavailable.fetch_add(1, SeqCst);
                }
            }
        }

        let error_str = errors.iter().map(|err| err.to_string()).collect::<Vec<String>>().join("\n");
        info!("Finished scanning peers [errors: {:#}]", error_str);

        {
            unique_peers.write().await.extend(output);
            let actual = unique_peers.read().await.len();

            let available = available.load(SeqCst);
            let unavailable = unavailable.load(SeqCst);
            let unknown = (total as u32) - available - unavailable;
            info!("Updated {} contacts: [total: {}] [available: {}] [unavailable: {}] [unknown: {}]",
                actual, total, available, unavailable, unknown);
            metrics.total_hub_count.set(total as f64);
            metrics.available_hub_count.set(available as f64);
            metrics.unavailable_hub_count.set((unavailable + unknown) as f64);
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

type HubUniquePeers = HashMap<String, HubInfo>;

#[derive(Debug, Clone)]
struct HubInfo {
    status: HubStatus,
    hub_info: HubInfoResponse,
    rpc_config: BaseRpcConfig,
}

#[derive(Debug)]
struct Metrics {
    peers_per_hub: prometheus::Histogram,
    total_fid_events_histogram: prometheus::Histogram,
    total_fname_events_histogram: prometheus::Histogram,
    total_hub_count: prometheus::Gauge,
    total_unique_endpoints: prometheus::Gauge,
    total_messages_histogram: prometheus::Histogram,
    unavailable_hub_count: prometheus::Gauge,
    available_hub_count: prometheus::Gauge,
    peer_address_change_count: prometheus::Counter,
}

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
