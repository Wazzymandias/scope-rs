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
use eyre::{eyre, OptionExt, Report};
use futures::AsyncWriteExt;
use histogram::Histogram;
use sled::Tree;
use slog_scope::info;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, watch, Notify};
use tokio::time;
use tonic::transport::{Channel, Endpoint};

use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::{Message, SyncIds, TrieNodeMetadataResponse, TrieNodePrefix};

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
pub struct HubStateDiffer {
    endpoint_a: Endpoint,
    tree_a: Arc<Tree>,
    endpoint_b: Endpoint,
    tree_b: Arc<Tree>,
}

struct HubState {}

fn save_to_file(sync_ids: &SyncIds, path: &str) -> eyre::Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, sync_ids)?;
    Ok(())
}

struct UniqueQueue<T> {
    queue: VecDeque<T>,
    set: HashSet<T>,
}

impl<T: Eq + Hash + Clone> UniqueQueue<T> {
    fn new() -> Self {
        UniqueQueue {
            queue: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    fn from(arr: Vec<T>) -> Self {
        let mut set = HashSet::new();
        let queue = arr
            .into_iter()
            .filter(|item| set.insert(item.clone()))
            .collect();
        UniqueQueue { queue, set }
    }

    fn drain(&mut self) -> Vec<T> {
        self.queue.drain(..).collect()
    }

    fn drain_batch(&mut self, batch_size: usize) -> Vec<T> {
        let drain_until = batch_size.min(self.queue.len());
        self.queue.drain(..drain_until).collect()
    }

    fn pop_front(&mut self) -> Option<T> {
        if let Some(item) = self.queue.pop_front() {
            self.set.remove(&item);
            return Some(item);
        }
        None
    }

    fn push_back(&mut self, item: T) {
        if self.set.insert(item.clone()) {
            self.queue.push_back(item);
        }
    }

    // Add an item to the queue if it's not already present
    fn enqueue(&mut self, item: T) {
        if self.set.insert(item.clone()) {
            self.queue.push_back(item);
        }
    }

    // Remove and return the first item from the queue, if any
    fn dequeue(&mut self) -> Option<T> {
        if let Some(item) = self.queue.pop_front() {
            self.set.remove(&item);
            return Some(item);
        }
        None
    }

    // Peek at the first item in the queue without removing it, if any
    fn peek(&self) -> Option<&T> {
        self.queue.front()
    }

    // Check if the queue is empty
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl HubStateDiffer {
    pub(crate) fn new(endpoint_a: Endpoint, endpoint_b: Endpoint) -> Self {
        let db = sled::open(".db").unwrap();
        let a_tree_prefix = endpoint_a.uri().host().unwrap().to_owned();
        let b_tree_prefix = endpoint_b.uri().host().unwrap().to_owned();
        let a_tree = db.open_tree(a_tree_prefix).unwrap();
        let b_tree = db.open_tree(b_tree_prefix).unwrap();

        Self {
            endpoint_a,
            tree_a: Arc::new(a_tree),
            endpoint_b,
            tree_b: Arc::new(b_tree),
        }
    }

    async fn sync_worker(
        endpoint: Endpoint,
        queue: Arc<tokio::sync::RwLock<UniqueQueue<Vec<u8>>>>,
        tree: Arc<Tree>,
    ) -> eyre::Result<Vec<Vec<u8>>> {
        const BATCH_SIZE: usize = 10;
        let mut result: Vec<Vec<u8>> = vec![];
        let mut client = HubServiceClient::connect(endpoint).await?;
        while !queue.read().await.is_empty() {
            let prefixes = queue.write().await.drain_batch(BATCH_SIZE);
            for prefix in prefixes {
                if prefix.is_empty() {
                    continue;
                }

                let t = tree.clone();
                let h = blake3::hash(&prefix.as_slice());
                let href = h.clone();
                let has_key = tokio::task::spawn_blocking(move || {
                    t.contains_key(href.as_bytes()).unwrap()
                }).await?;
                if has_key {
                    continue
                }

                let sync_ids = client
                    .get_all_sync_ids_by_prefix(TrieNodePrefix {
                        prefix: prefix.clone(),
                    })
                    .await?
                    .into_inner();
                let metadata = client
                    .get_sync_metadata_by_prefix(TrieNodePrefix {
                        prefix: prefix.clone(),
                    })
                    .await?
                    .into_inner();
                result.extend(sync_ids.sync_ids);

                let t = tree.clone();
                let href = h.clone();
                let mut q = queue.clone();
                tokio::task::spawn_blocking(move || -> eyre::Result<()> {
                    for child in metadata.children.iter() {
                        let child_h = blake3::hash(child.prefix.as_slice());
                        if child.num_messages > 0 && !t.contains_key(child_h.as_bytes())? {
                            info!("enqueuing child prefix: {:?}", child.prefix);
                            q.blocking_write().push_back(child.prefix.clone());
                        }
                    }
                    drop(q);

                    t.insert(href.as_bytes(), vec![])?;
                    t.flush()?;
                    Ok(())
                }).await??;
            }
        }
        Ok(result)
    }

    async fn sync_and_persist(
        endpoint: Endpoint,
        start_time: u32,
        end_time: u32,
        tree: Arc<Tree>,
    ) -> eyre::Result<SyncIds> {
        const WORKER_POOL_SIZE: usize = 4;
        const TIME_WINDOW_SECONDS: u32 = 600;
        let queue = Arc::new(tokio::sync::RwLock::new(UniqueQueue::new()));
        let mut current_time = start_time;
        let mut sync_ids: Vec<Vec<u8>> = vec![];
        let e = Arc::new(&endpoint);

        while current_time >= end_time {
            let mut q = queue.write().await;
            for t in (current_time - TIME_WINDOW_SECONDS..current_time - 1).rev() {
                let prefix = farcaster_time_to_str(t - FARCASTER_EPOCH as u32).as_bytes().to_vec();
                q.enqueue(prefix);
            }
            drop(q);

            let workers = (0..WORKER_POOL_SIZE).map(|i| {
                slog_scope::info!("spawning worker {:?}", i);
                let endpoint = endpoint.clone()
                    .buffer_size(32768)
                    .timeout(Duration::from_secs(3))
                    .concurrency_limit(256);
                let queue = Arc::clone(&queue);
                HubStateDiffer::sync_worker(endpoint, queue, tree.clone())
            });

            let results = futures::future::join_all(workers).await;
            (&results).into_iter().for_each(|res| {
                match res {
                    Ok(sync_ids_result) => {
                        // `sync_ids` is of type `HashSet<Vec<u8>>` here, coming from the spawned task.
                        slog_scope::info!("Task completed successfully: {:?}", sync_ids_result.len());
                        sync_ids.extend(sync_ids_result.clone());
                    } // The task completed successfully.
                    Err(e) => {
                        // `e` is of type `ErrReport` here, coming from the spawned task.
                        slog_scope::error!("Task error: {:?}", e);
                    }
                }
            });

            let t = tree.clone();
            tokio::task::spawn_blocking(move || {
                let mut batch = sled::Batch::default();
                results.into_iter().for_each(|res| {
                    match res {
                        Ok(sync_ids_result) => {
                            sync_ids_result.into_iter().for_each(|sync_id| {
                                batch.insert(sync_id.to_vec(), vec![]);
                            });
                        }
                        Err(e) => {
                            slog_scope::error!("Batch error: {:?}", e);
                        }
                    }
                });
                t.apply_batch(batch).expect("TODO: panic message");
                t.flush().unwrap();
            }).await?;

            current_time = current_time - TIME_WINDOW_SECONDS;
            info!("successfully processed batch of messages"; "ts" => current_time);
        }

        return Ok(SyncIds{
            sync_ids,
        });
    }

    // diff exhaustive will do a full scan of two hub tries
    // to return all the messages that are missing
    // since it is expensive, we should take the result and persist it to a file as well
    pub async fn diff_exhaustive(self) -> eyre::Result<(Vec<Message>, Vec<Message>)> {
        let now = Utc::now();
        let (start, end) = (now.timestamp() as u32, (now - Duration::from_days(1)).timestamp() as u32);
        let source_handle = tokio::spawn(HubStateDiffer::sync_and_persist(
            self.endpoint_a.clone(),
            start,
            end,
            self.tree_a.clone(),
        ));
        let target_handle = tokio::spawn(HubStateDiffer::sync_and_persist(
            self.endpoint_b.clone(),
            start,
            end,
            self.tree_b.clone(),
        ));

        let result = futures::future::join(source_handle, target_handle).await;
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
        let mut v: Vec<_> = missing.into_iter().collect();
        v.sort_by_key(|&(key, _)| key);
        slog_scope::info!("missing: {:?}", v);
        let sparse = histogram::SparseHistogram::from(&hist);
        let pct = hist.percentiles(&[50.0, 75.0, 90.0, 99.0, 99.9, 99.99])?;
        slog_scope::info!("histogram: {:?}", sparse);
        slog_scope::info!("percentiles: {:?}", pct);
        let median = sparse.percentile(50.0).unwrap();
        slog_scope::info!("median: {:?} {:?}", median.start(), median.end());
        let source_messages =
            HubStateDiffer::messages_by_sync_ids(self.endpoint_a.clone(), source).await?;
        let target_messages =
            HubStateDiffer::messages_by_sync_ids(self.endpoint_b.clone(), target).await?;
        slog_scope::info!("source messages: {:?}", source_messages.len());
        slog_scope::info!("target messages: {:?}", target_messages.len());

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
