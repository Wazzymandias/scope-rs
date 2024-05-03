use chrono::Utc;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::hash::Hash;
use std::io::BufWriter;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::farcaster;
use crate::farcaster::sync_id::{RootPrefix, SyncId, UnpackedSyncId, TIMESTAMP_LENGTH};
use crate::farcaster::time::{farcaster_time_range, farcaster_time_to_str, farcaster_to_unix, FARCASTER_EPOCH, str_bytes_to_unix_time};
use eyre::{eyre};
use histogram::Histogram;
use sled::{IVec, Tree};
use slog_scope::info;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};

use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::{Message, SyncIds, TrieNodeMetadataResponse, TrieNodePrefix};
use crate::queue::Queue;

const PREFIX_SET_KEY: &[u8] = b"prefix_set";

fn extract_timestamp(message: &Message) -> eyre::Result<SystemTime> {
    let timestamp = match &message.data {
        Some(data) => {
            // Directly use the timestamp from `data`
            FARCASTER_EPOCH + data.timestamp as u64
        }
        None => {
            // Extract and parse the timestamp from `data_bytes`
            message
                .data_bytes
                .as_ref()
                .and_then(|bytes| std::str::from_utf8(&bytes[0..10]).ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(|t| FARCASTER_EPOCH + t)
                .ok_or_else(|| eyre!("Failed to extract timestamp"))?
        }
    };
    Ok(UNIX_EPOCH + Duration::from_secs(timestamp))
}

#[derive(Debug)]
enum DbOperation {
    Insert { tree: Arc<Tree>, key: Vec<u8>, value: Vec<u8> },
    // Other operations like Delete, Query, etc., can be defined similarly.
}

fn spawn_tree_thread(mut channel: Receiver<DbOperation>) -> JoinHandle<()> {
    return tokio::task::spawn_blocking(move || {
        while let Some(operation) = channel.blocking_recv() {
            match operation {
                DbOperation::Insert { tree, key, value } => {
                    match tree.insert(key, value) {
                        Ok(_) => {

                        },
                        Err(e) => info!("Failed to insert key, error: {:?}", e),
                    }
                }
            }
        }
    });
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct Item {
    prefix: Vec<u8>,
    hash: blake3::Hash
}

#[derive(Debug)]
pub struct HubStateDiffer {
    endpoint_a: Endpoint,
    tree_a: Arc<Tree>,
    endpoint_b: Endpoint,
    tree_b: Arc<Tree>,
    set_a: Arc<tokio::sync::RwLock<HashSet<blake3::Hash>>>,
    set_b: Arc<tokio::sync::RwLock<HashSet<blake3::Hash>>>,
}

fn save_to_file(sync_ids: &SyncIds, path: &str) -> eyre::Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, sync_ids)?;
    Ok(())
}

impl HubStateDiffer {
    pub(crate) fn new(endpoint_a: Endpoint, endpoint_b: Endpoint) -> Self {
        let db = sled::open(".db").unwrap();
        let a_tree_prefix = endpoint_a.uri().host().unwrap().to_owned();
        let b_tree_prefix = endpoint_b.uri().host().unwrap().to_owned();
        let a_tree = db.open_tree(a_tree_prefix).unwrap();
        let b_tree = db.open_tree(b_tree_prefix).unwrap();

        let mut set_a: HashSet<blake3::Hash> = HashSet::new();
        let mut set_b: HashSet<blake3::Hash> = HashSet::new();
        a_tree.get(PREFIX_SET_KEY).unwrap().map(|v| {
            let mut i = 0;
            while i < v.len() {
                let b: [u8; 32] = v[i..i+32].try_into().unwrap();
                set_a.insert(blake3::Hash::from_bytes(b));
                i += 32;
            }
        });
        b_tree.get(PREFIX_SET_KEY).unwrap().map(|v| {
            let mut i = 0;
            while i < v.len() {
                let b: [u8; 32] = v[i..i+32].try_into().unwrap();
                set_b.insert(blake3::Hash::from_bytes(b));
                i += 32;
            }
        });

        info!("loaded database [sets: {:?} {:?}] [trees: {:?} {:?}]",
            set_a.len(), set_b.len(), a_tree.len(), b_tree.len());

        Self {
            endpoint_a,
            tree_a: Arc::new(a_tree),
            endpoint_b,
            tree_b: Arc::new(b_tree),
            set_a: Arc::new(tokio::sync::RwLock::new(set_a)),
            set_b: Arc::new(tokio::sync::RwLock::new(set_b)),
        }
    }

    async fn sync_worker(
        endpoint: Endpoint,
        queue: Arc<tokio::sync::RwLock<Queue<Item>>>,
        tree: Arc<Tree>,
        set: Arc<tokio::sync::RwLock<HashSet<blake3::Hash>>>,
        sender: Sender<DbOperation>,
        counter: Arc<AtomicUsize>,
    ) -> eyre::Result<Vec<SyncIds>> {
        const BATCH_SIZE: usize = 3;
        let mut result: Vec<SyncIds> = vec![];
        let mut client = HubServiceClient::connect(endpoint).await?;
        while !(counter.load(SeqCst) == 0) {
            let items = queue.write().await.drain_batch(BATCH_SIZE);
            if items.len() == 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            for item in items {
                if item.prefix.is_empty() {
                    continue;
                }

                if set.read().await.contains(&item.hash) {
                    continue;
                }

                let metadata = client
                    .get_sync_metadata_by_prefix(TrieNodePrefix {
                        prefix: item.prefix.clone(),
                    })
                    .await?
                    .into_inner();

                if metadata.children.is_empty() {
                    let sync_ids = client
                        .get_all_sync_ids_by_prefix(TrieNodePrefix {
                            prefix: metadata.prefix,
                        })
                        .await?
                        .into_inner();
                    result.push(sync_ids);
                }

                for child in metadata.children.iter() {
                    let child_h = blake3::hash(child.prefix.as_slice());
                    if child.num_messages > 0 && !set.read().await.contains(&child_h) {
                        counter.fetch_add(1, SeqCst);
                        queue.write().await.push_back(Item{
                            prefix: child.prefix.clone(),
                            hash: child_h
                        });
                    }
                }

                if metadata.num_messages <= 1 {
                    info!("inserting item into db: {:?}", item.prefix);
                    sender.send(DbOperation::Insert {
                        tree: tree.clone(),
                        key: item.prefix,
                        value: metadata.hash.as_bytes().to_vec(),
                    }).await?;
                }
                set.write().await.insert(item.hash);
                counter.fetch_sub(1, SeqCst);
            }
        }
        Ok(result)
    }

    async fn sync_and_persist(
        endpoint: Endpoint,
        start_time: u32,
        end_time: u32,
        tree: Arc<Tree>,
        set: Arc<tokio::sync::RwLock<HashSet<blake3::Hash>>>,
        sender: Sender<DbOperation>,
    ) -> eyre::Result<SyncIds> {
        const WORKER_POOL_SIZE: usize = 8;
        const TIME_WINDOW_SECONDS: u32 = 600;
        let queue = Arc::new(tokio::sync::RwLock::new(Queue::new()));
        let mut current_time = start_time;
        let mut sync_ids: Vec<SyncIds> = vec![];
        let mut counter = Arc::new(AtomicUsize::new(0));

        while current_time >= end_time {
            let start_time = Utc::now();
            let mut q = queue.write().await;
            let batch_size = TIME_WINDOW_SECONDS.min(current_time - end_time);
            let start = current_time - batch_size;
            let end = current_time;
            info!("processing batch of messages"; "ts" => current_time, "start" => start, "end" => end);
            for t in (start..end).rev() {
                let prefix = farcaster_time_to_str(t - FARCASTER_EPOCH as u32).as_bytes().to_vec();
                let hash = blake3::hash(prefix.as_slice());
                counter.fetch_add(1, SeqCst);
                q.enqueue(Item{
                    prefix,
                    hash,
                });
            }
            drop(q);

            let workers = (0..WORKER_POOL_SIZE).map(|i| {
                info!("spawning worker {:?}", i);
                let endpoint = endpoint.clone()
                    .buffer_size(65536)
                    .concurrency_limit(512);
                let queue = Arc::clone(&queue);
                let tree = tree.clone();
                let set = set.clone();
                let sender = sender.clone();
                let counter = counter.clone();

                tokio::task::spawn(async move {
                    HubStateDiffer::sync_worker(endpoint, queue, tree, set, sender, counter).await
                })
            }).collect::<Vec<JoinHandle<_>>>();

            let results = futures::future::join_all(workers).await;
            for result in results {
                match result {
                    Ok(Ok(sync_ids_result)) => {
                        // `sync_ids` is of type `HashSet<Vec<u8>>` here, coming from the spawned task.
                        slog_scope::info!("Task completed successfully: {:?}", sync_ids_result.len());
                        sync_ids.extend(sync_ids_result);
                    } // The task completed successfully.
                    Ok(Err(e)) => {
                        // `e` is of type `eyre::Report` here, coming from the spawned task.
                        slog_scope::error!("Task join error: {:?}", e);
                    } // The task returned an error.
                    Err(e) => {
                        // `e` is of type `ErrReport` here, coming from the spawned task.
                        slog_scope::error!("Task error: {:?}", e);
                    }
                }
            }

            let set_size = set.read().await.len();
            info!("inserting prefix set into db [size: {:?}]", set_size);
            let t = tree.clone();
            let mut hash_set_bytes = Vec::with_capacity(set.read().await.len() * blake3::OUT_LEN);
            for hash in set.read().await.iter() {
                hash_set_bytes.extend_from_slice(hash.as_bytes());
            }

            sender.send(DbOperation::Insert {
                tree: t,
                key: PREFIX_SET_KEY.to_vec(),
                value: hash_set_bytes,
            }).await?;

            current_time = current_time - batch_size - 1;
            info!("successfully processed batch of messages [duration: {:?}] [sync_ids: {:?}]",
                Utc::now() - start_time, sync_ids.len());
        }

        return Ok(SyncIds{
            sync_ids: sync_ids.into_iter().flat_map(|s| s.sync_ids).collect(),
        });
    }

    // diff exhaustive will do a full scan of two hub tries
    // to return all the messages that are missing
    // since it is expensive, we should take the result and persist it to a file as well
    pub async fn diff_exhaustive(self) -> eyre::Result<(Vec<Message>, Vec<Message>)> {
        let now = Utc::now();
        let (start, end) = (now.timestamp() as u32, (now - Duration::from_days(1)).timestamp() as u32);

        let (sender, receiver) = tokio::sync::mpsc::channel::<DbOperation>(8192);
        let db_handle = spawn_tree_thread(receiver);
        let source_handle = tokio::spawn(HubStateDiffer::sync_and_persist(
            self.endpoint_a.clone(),
            start,
            end,
            self.tree_a.clone(),
            self.set_a.clone(),
            sender.clone(),
        ));

        let target_handle = tokio::spawn(HubStateDiffer::sync_and_persist(
            self.endpoint_b.clone(),
            start,
            end,
            self.tree_b.clone(),
            self.set_b.clone(),
            sender.clone(),
        ));

        let result = futures::future::join(source_handle, target_handle).await;
        drop(sender);

        let (source_sync_ids, target_sync_ids) = result;

        let source: SyncIds;
        match source_sync_ids? {
            Ok(s) => source = s,
            Err(e) => return Err(eyre!("error fetching source sync ids: {:?}", e)),
        }

        let target: SyncIds;
        match target_sync_ids? {
            Ok(s) => target = s,
            Err(e) => return Err(eyre!("error fetching target sync ids: {:?}", e)),
        }

        save_to_file(&source, "source_sync_ids.json")?;
        save_to_file(&target, "target_sync_ids.json")?;
        // get all messages by sync_ids
        let s_len = source.sync_ids.len();
        let t_len = target.sync_ids.len();
        slog_scope::info!("source sync ids: {:?}", s_len);
        slog_scope::info!("target sync ids: {:?}", t_len);
        let mut hist = Histogram::new(7, 64)?;
        let mut missing = HashMap::new();
        for sync_id in (&target).sync_ids.iter() {
            if !source.sync_ids.contains(sync_id) {
                let ts = sync_id[0..TIMESTAMP_LENGTH].to_vec();
                let ts_str = String::from_utf8(ts).unwrap();
                let timestamp = ts_str.parse::<u32>().unwrap();
                missing.insert(timestamp, sync_id);
                hist.increment(farcaster_to_unix(timestamp as u64))?;
            }
        }

        if missing.len() == 0 {
            info!("no missing sync ids between source and target endpoints",);
        } else {
            let mut v: Vec<_> = missing.into_iter().collect();
            v.sort_by_key(|&(key, _)| key);
            slog_scope::info!("missing: {:?}", v);
            let sparse = histogram::SparseHistogram::from(&hist);
            let pct = hist.percentiles(&[50.0, 75.0, 90.0, 99.0, 99.9, 99.99])?;
            slog_scope::info!("histogram: {:?}", sparse);
            slog_scope::info!("percentiles: {:?}", pct);
            let median = sparse.percentile(50.0).unwrap();
            slog_scope::info!("median: {:?} {:?}", median.start(), median.end());
        }

        let source_messages =
            HubStateDiffer::messages_by_sync_ids(self.endpoint_a.clone(), source).await?;
        let target_messages =
            HubStateDiffer::messages_by_sync_ids(self.endpoint_b.clone(), target).await?;
        info!("source messages: {:?}", source_messages.len());
        info!("target messages: {:?}", target_messages.len());

        Ok((source_messages, target_messages))
    }

    async fn messages_by_sync_ids(
        endpoint: Endpoint,
        sync_ids: SyncIds,
    ) -> eyre::Result<Vec<Message>> {
        let mut client = HubServiceClient::connect(endpoint).await?;
        let ids = client
            .get_all_messages_by_sync_ids(sync_ids)
            .await?
            .into_inner();
        Ok(ids.messages)
    }

    async fn sync_ids(
        input_prefix: Vec<u8>,
        client: &mut HubServiceClient<Channel>,
    ) -> eyre::Result<SyncIds> {
        let mut source_sync_ids: SyncIds = SyncIds { sync_ids: vec![] };

        async fn fetch_sync_ids(
            client: &mut HubServiceClient<Channel>,
            out: &mut SyncIds,
            prefix: Vec<u8>,
        ) -> eyre::Result<()> {
            // Fetch sync ids
            let result = client
                .get_all_sync_ids_by_prefix(tonic::Request::new(TrieNodePrefix {
                    prefix: prefix.clone(),
                }))
                .await;
            match result {
                Ok(sync_ids) => {
                    out.sync_ids.extend(sync_ids.into_inner().sync_ids);
                }
                Err(e) => {
                    return Err(eyre!(
                        "error fetching sync ids for prefix {:?}: {:?}",
                        prefix,
                        e
                    ))
                }
            }
            Ok(())
        }
        let mut queue: VecDeque<Vec<u8>> = VecDeque::new();
        for i in 0..input_prefix.len() {
            queue.push_back(vec![input_prefix[i]]);
        }

        let mut visited: HashSet<Vec<u8>> = HashSet::new();
        while let Some(prefix) = queue.pop_front() {
            if visited.contains(&prefix) {
                continue;
            }
            visited.insert(prefix.clone());

            if prefix.len() + 1 >= 4 {
                continue;
            }
            for i in 0..input_prefix.len() {
                let mut new_prefix = prefix.clone();
                new_prefix.push(input_prefix[i]);
                fetch_sync_ids(client, &mut source_sync_ids, new_prefix.clone()).await?;
                queue.push_back(new_prefix);
            }
        }

        Ok(source_sync_ids)
    }
    pub async fn _diff(self) -> eyre::Result<HashMap<u64, UnpackedSyncId>> {
        let source_client = &mut HubServiceClient::connect(self.endpoint_a).await?;
        let target_client = &mut HubServiceClient::connect(self.endpoint_b).await?;

        let source_snap = source_client
            .get_sync_snapshot_by_prefix(tonic::Request::new(Default::default()))
            .await?
            .into_inner();
        let target_snap = target_client
            .get_sync_snapshot_by_prefix(tonic::Request::new(Default::default()))
            .await?
            .into_inner();

        let source_sync_ids: SyncIds =
            HubStateDiffer::sync_ids(source_snap.prefix, source_client).await?;
        let target_sync_ids: SyncIds =
            HubStateDiffer::sync_ids(target_snap.prefix, target_client).await?;

        let source_set: HashSet<Vec<u8>> = source_sync_ids.sync_ids.into_iter().collect();
        let mut message_vec: HashMap<u64, UnpackedSyncId> = HashMap::new();
        let mut fname_vec: HashMap<u64, UnpackedSyncId> = HashMap::new();
        let mut on_chain_event_vec: HashMap<u64, UnpackedSyncId> = HashMap::new();

        target_sync_ids.sync_ids.iter().for_each(|sync_id| {
            if !source_set.contains(sync_id) {
                let id = SyncId(sync_id.clone());
                let timestamp_bytes = id.0[0..TIMESTAMP_LENGTH].to_vec();
                let ts_str = String::from_utf8(timestamp_bytes).unwrap();
                let timestamp = ts_str.parse::<u64>().unwrap();
                let result: UnpackedSyncId = SyncId::unpack(&id.0);
                match result {
                    UnpackedSyncId::Message {
                        fid,
                        primary_key,
                        hash,
                    } => {
                        message_vec.insert(
                            timestamp,
                            UnpackedSyncId::Message {
                                fid,
                                primary_key,
                                hash,
                            },
                        );
                    }
                    UnpackedSyncId::FName { fid, name, padded } => {
                        fname_vec.insert(timestamp, UnpackedSyncId::FName { fid, name, padded });
                    }
                    UnpackedSyncId::OnChainEvent {
                        fid,
                        event_type,
                        block_number,
                        log_index,
                    } => {
                        on_chain_event_vec.insert(
                            timestamp,
                            UnpackedSyncId::OnChainEvent {
                                fid,
                                event_type,
                                block_number,
                                log_index,
                            },
                        );
                    }
                    _ => {
                        println!("unknown sync id found: {:?}", sync_id);
                        // Do nothing
                    }
                }
            }
        });

        Ok(
            message_vec
                .iter()
                .chain(fname_vec.iter())
                .chain(on_chain_event_vec.iter())
                .map(|(k, v)| (*k, v.clone())) // Create owned versions of both keys and values
                .collect(), // Collect into a HashMap<u64, UnpackedSyncId>
        )
    }
}
