use chrono::{DateTime, Utc};
use std::collections::{VecDeque};
use std::fs::File;
use std::hash::Hash;
use std::io::BufWriter;
use std::sync::Arc;

use crate::farcaster;
use crate::farcaster::sync_id::{
    RootPrefix, SyncId, SyncIdType, TIMESTAMP_LENGTH,
};
use crate::farcaster::time::{
    farcaster_time_to_str, farcaster_to_unix,
    FARCASTER_EPOCH,
};
use duckdb::{Connection, ToSql, Transaction};
use eyre::eyre;
use histo::Histogram;
use farcaster::CachedRepository;
use sled::Tree;
use slog_scope::{debug, info};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};
use crate::farcaster::DEFAULT_CACHE_DB_DIR;

use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::{SyncIds, TrieNodePrefix};

const PREFIX_SET_KEY: &[u8] = b"prefix_set";
const DUCKDB_PATH: &str = ".duckdb";

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct DBSyncID {
    // timestamp_bytes: [u8; 10],
    // sync_id_type: u8,
    // data_bytes: Vec<u8>,
    sync_id_type: u8,
    timestamp_bytes: Vec<u8>,
    data_bytes: Vec<u8>,
}

#[derive(Debug)]
enum DbOperation {
    BatchSyncIds {
        source: String,
        sync_ids_vec: Vec<SyncIds>,
        cache_tree: Tree,
        cache_entries: Vec<(Vec<u8>, Vec<u8>)>
    },
}

fn spawn_tree_thread(
    mut conn: Connection,
    mut channel: Receiver<DbOperation>,
) -> std::thread::JoinHandle<eyre::Result<()>> {
    std::thread::spawn(move || {
        while let Some(operation) = channel.blocking_recv() {
            match operation {
                DbOperation::BatchSyncIds {
                    source,
                    sync_ids_vec,
                    cache_tree,
                    cache_entries,
                } => {
                    let tx_result = conn.transaction();
                    let tx: Transaction;
                    match tx_result {
                        Ok(t) => {
                            tx = t;
                        }
                        Err(e) => {
                            info!("Failed to start transaction, error: {:?}", e);
                            continue;
                        }
                    }

                    let valid_sync_ids = sync_ids_vec
                        .iter()
                        .flat_map(|sync_ids| sync_ids.sync_ids.iter())
                        .filter(|sync_id| sync_id.len() > 10)
                        .collect::<Vec<_>>();
                    if valid_sync_ids.is_empty() {
                        info!("No valid sync ids found for source: {:?}", source);
                        continue;
                    }
                    // Prepare the query with the appropriate number of placeholders
                    let placeholders: String = valid_sync_ids
                        .iter()
                        .map(|_| "(?, ?, ?)")
                        .collect::<Vec<_>>()
                        .join(", ");

                    let values: Vec<DBSyncID> = valid_sync_ids
                        .iter()
                        .map(|sync_id| {
                            let (timestamp_bytes, rest) = sync_id.split_at(10);
                            DBSyncID {
                                timestamp_bytes: timestamp_bytes.to_vec(),
                                sync_id_type: rest[0],
                                data_bytes: rest[1..].to_vec(),
                           }
                        })
                        .collect();

                    let to_sql_params: Vec<&dyn ToSql> = values
                        .iter()
                        .flat_map(|sync_id| {
                            vec![
                                &sync_id.timestamp_bytes as &dyn ToSql,
                                &sync_id.sync_id_type,
                                &sync_id.data_bytes,
                            ]
                        })
                        .collect();

                    let insert_sync_ids_query = format!(
                        "INSERT INTO sync_ids (timestamp_prefix, sync_id_type, data_bytes) \
                            VALUES {} \
                            ON CONFLICT (timestamp_prefix,sync_id_type,data_bytes) DO NOTHING \
                            RETURNING *",
                        placeholders
                    );
                    debug!(
                        "executing insert sync ids query: {:?}",
                        insert_sync_ids_query
                    );

                    // 1. Insert the sync ids
                    let mut statement = tx.prepare(&insert_sync_ids_query).unwrap();
                    let rows = statement.query(
                        to_sql_params.to_vec()
                            .as_slice(),
                    );
                    if rows.is_err() {
                        info!("Failed to insert sync_ids, error: {:?}", rows.err());
                        tx.rollback().unwrap();
                        continue;
                    }

                    // 2. Get the valid sequence IDs by filtering rows that were actually inserted
                    // We want to do a comparison which looks like:
                    // WHERE (timestamp_prefix, sync_id_type, data_bytes) IN ((?, ?, ?), (?, ?, ?), ...)

                    let conditions = format!(
                        "(timestamp_prefix, sync_id_type, data_bytes) IN ({})",
                        values
                            .iter()
                            .map(|_| "(?, ?, ?)")
                            .collect::<Vec<_>>()
                            .join(", ")
                    );

                    let sources_query = format!("SELECT id FROM sync_ids WHERE {}", conditions);
                    debug!(
                        "executing sources query: {:?} {:?} {:?}",
                        sources_query,
                        to_sql_params.len(),
                        values.len()
                    );
                    let mut sources_statement = tx.prepare(&sources_query).unwrap();
                    // the valid query params does a search by checking IN functions for each of the three columns
                    // which is different from the id sql params which does a insert by the tuple for each row
                    // as a result we loop through the values three times to get the right types for the query

                    let rows = sources_statement
                        .query_map(to_sql_params.as_slice(), |row| row.get(0))
                        .unwrap();
                    let ids: Vec<u64> = rows.map(|r| r.unwrap()).collect();

                    // Prepare and execute the query for inserting into source_sync_ids
                    let placeholders_source: String =
                        ids.iter().map(|_| "(?, ?)").collect::<Vec<_>>().join(", ");
                    let source_params: Vec<&(dyn ToSql + Sync)> = ids
                        .iter()
                        .flat_map(|id| vec![&source as &(dyn ToSql + Sync), id])
                        .collect();

                    let query_source_sync_ids = format!(
                        "INSERT INTO source_sync_ids (source, sync_id) VALUES {}",
                        placeholders_source
                    );

                    let source_sync_ids_result = tx.execute(
                        &query_source_sync_ids,
                        source_params
                            .iter()
                            .map(|&p| p as &dyn ToSql)
                            .collect::<Vec<_>>()
                            .as_slice(),
                    );
                    match source_sync_ids_result {
                        Ok(size) => {
                            debug!(
                                "Inserted source_sync_ids: {:?} [channel: {:?}]",
                                size,
                                channel.len()
                            );
                        }
                        Err(e) => {
                            info!("Failed to insert source_sync_ids, error: {:?}", e);
                            tx.rollback().unwrap();
                            continue;
                        },
                    }

                    let result = tx.commit();
                    match result {
                        Ok(_) => {
                            debug!("Committed transaction {:?}", source);
                        }
                        Err(e) => {
                            info!("Failed to commit transaction, error: {:?}", e);
                        }
                    }

                    let mut cache_batch = sled::Batch::default();
                    for (key, value) in cache_entries {
                        cache_batch.insert(key, value);
                    }
                    cache_tree.apply_batch(cache_batch).unwrap();
                }
            }
        }
        conn.close().unwrap();
        Ok(())
    })
}

#[derive(Debug)]
pub struct HubStateDiffer {
    endpoint_a: Endpoint,
    endpoint_b: Endpoint,
    sync_id_types: Vec<SyncIdType>,
    repo: CachedRepository,
}

#[derive(Debug)]
pub struct SyncIdDiffReport {
    pub only_in_a: SyncIds,

    pub only_in_b: SyncIds,
}

trait ParseSyncId {
    fn root_prefix_from(input: &Vec<u8>) -> RootPrefix {
        RootPrefix::from_u8(input[TIMESTAMP_LENGTH])
    }
    fn root_prefix_type(&self) -> RootPrefix;
    fn timestamp(&self) -> eyre::Result<u64>;
    fn timestamp_from(input: &Vec<u8>) -> eyre::Result<u64> {
        let ts_bytes = &input[..TIMESTAMP_LENGTH];
        let ts_str = String::from_utf8(ts_bytes.to_vec())?;
        let ts = ts_str.parse::<u32>()?;
        Ok(farcaster_to_unix(ts as u64))
    }
}

impl ParseSyncId for SyncId {
    fn root_prefix_type(&self) -> RootPrefix {
        Self::root_prefix_from(&self.0)
    }

    fn timestamp(&self) -> eyre::Result<u64> {
        Self::timestamp_from(&self.0)
    }
}

impl SyncIdDiffReport {
    pub fn histogram_by_root_prefix(ids: &SyncIds) -> eyre::Result<Histogram> {
        let mut histogram = Histogram::with_buckets(32);
        let id_types = ids.sync_ids.iter().map(SyncId::root_prefix_from);
        id_types.for_each(|id_type| {
            histogram.add(id_type as u64)
        });

        Ok(histogram)
    }

    pub fn histogram_by_timestamp(ids: &SyncIds) -> eyre::Result<Histogram> {
        let mut histogram = Histogram::with_buckets(64);
        let timestamps = ids.sync_ids.iter().map(SyncId::timestamp_from);
        timestamps.for_each(|timestamp| {
            histogram.add(timestamp.unwrap())
        });
        Ok(histogram)
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct Item {
    prefix: Vec<u8>,
    hash: blake3::Hash,
}

fn save_to_file(sync_ids: &SyncIds, path: &str) -> eyre::Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, sync_ids)?;
    Ok(())
}

struct PendingItem {
    entry: Option<Item>,
    pending: Arc<tokio::sync::RwLock<VecDeque<Item>>>,
}

impl PendingItem {
    fn new(pending: Arc<tokio::sync::RwLock<VecDeque<Item>>>) -> Self {
        Self {
            entry: None,
            pending,
        }
    }

    fn set(&mut self, item: Option<Item>) {
        self.entry = item;
    }

    fn clear(&mut self) {
        self.entry = None;
    }
}

impl Drop for PendingItem {
    fn drop(&mut self) {
        if let Some(item) = self.entry.take() {
            let pending = Arc::clone(&self.pending);
            tokio::spawn(async move {
                pending.write().await.push_back(item);
            });
        }
    }
}

impl HubStateDiffer {
    pub fn sync_id_types(mut self, types: impl IntoIterator<Item = SyncIdType>) -> Self {
        self.sync_id_types.extend(types);
        self
    }

    pub(crate) fn new(endpoint_a: Endpoint, endpoint_b: Endpoint) -> eyre::Result<Self> {
        let repo = CachedRepository::new(DEFAULT_CACHE_DB_DIR.to_string(), DUCKDB_PATH.to_string())?;
        Ok(Self {
            endpoint_a,
            endpoint_b,
            sync_id_types: vec![],
            repo,
        })
    }

    async fn sync_worker(
        source: String,
        mut client: HubServiceClient<Channel>,
        queue: Arc<tokio::sync::RwLock<VecDeque<Item>>>,
        pending: Arc<tokio::sync::RwLock<VecDeque<Item>>>,
        tree: Tree,
        sender: Sender<DbOperation>,
    ) -> eyre::Result<usize> {
        let mut num_processed: usize = 0;
        let mut result_sync_ids: Vec<SyncIds> = vec![];
        let mut pending_item = PendingItem::new(pending);
        let mut cache_entries = vec![];

        while !queue.read().await.is_empty() {
            pending_item.set(queue.write().await.pop_back());
            let item: &Item = match &pending_item.entry {
                None => { break; }
                Some(i) => i
            };
            let exists = CachedRepository::exists_in_cache_tree(&tree, item.hash.as_bytes())?;
            if exists {
                pending_item.clear();
                continue;
            }

            let metadata = client
                .get_sync_metadata_by_prefix(TrieNodePrefix {
                    prefix: item.prefix.clone(),
                })
                .await?
                .into_inner();

            // if metadata.children.is_empty() {
            //     result_sync_ids.push(
            //         client
            //             .get_all_sync_ids_by_prefix(TrieNodePrefix {
            //                 prefix: metadata.prefix,
            //             })
            //             .await?
            //             .into_inner(),
            //     );
            // }
            //
            // let children = metadata.children.iter().map(|child| {
            //     let prefix = child.prefix.clone();
            //     let hash = blake3::hash(prefix.as_slice());
            //     Item { prefix, hash }
            // });
            // queue.write().await.extend(children);
            if metadata.children.len() <= 1024 {
                result_sync_ids.push(
                    client
                        .get_all_sync_ids_by_prefix(TrieNodePrefix {
                            prefix: metadata.prefix,
                        })
                        .await?
                        .into_inner(),
                );
            } else {
                let children = metadata.children.iter().map(|child| {
                    let prefix = child.prefix.clone();
                    let hash = blake3::hash(prefix.as_slice());
                    Item { prefix, hash }
                });
                queue.write().await.extend(children);
            }

            cache_entries.push((item.hash.as_bytes().to_vec(), vec![]));
            pending_item.clear();
        }

        num_processed = result_sync_ids.len();
        if num_processed > 0 {
            sender
                .send(DbOperation::BatchSyncIds {
                    source: source.clone(),
                    sync_ids_vec: result_sync_ids,
                    cache_tree: tree.clone(),
                    cache_entries,
                })
                .await?;
        }
        Ok(num_processed)
    }

    async fn sync_and_persist(
        endpoint: Endpoint,
        start_time: u32,
        end_time: u32,
        tree: Tree,
        sender: Sender<DbOperation>,
    ) -> eyre::Result<usize> {
        const WORKER_POOL_SIZE: usize = 512;
        const TIME_WINDOW_SECONDS: u32 = 600;
        let queue = Arc::new(tokio::sync::RwLock::new(VecDeque::new()));
        let pending = Arc::new(tokio::sync::RwLock::new(VecDeque::new()));
        let mut current_time = end_time;

        let source = endpoint.clone().uri().to_string();
        let client = HubServiceClient::connect(endpoint).await?;
        let mut num_processed: usize = 0;

        loop {
            if sender.is_closed() {
                info!("sender closed, cannot continue, exiting now", );
                break;
            }

            let duration_start = Utc::now();
            if current_time >= start_time {
                let batch_size = TIME_WINDOW_SECONDS.min(current_time - start_time);
                let start = current_time - batch_size;
                let end = current_time;

                let mut q = queue.write().await;
                for t in (start..end + 1).rev() {
                    let prefix = farcaster_time_to_str(t - FARCASTER_EPOCH as u32)
                        .as_bytes()
                        .to_vec();
                    let hash = blake3::hash(prefix.as_slice());
                    q.push_back(Item { prefix, hash });
                }
                drop(q);
                current_time = current_time - batch_size - 1;
            }

            info!("spawning {:?} workers", WORKER_POOL_SIZE);
            let workers = (0..WORKER_POOL_SIZE)
                .map(|_| {
                    let source = source.clone();
                    let client = client.clone();
                    let queue = Arc::clone(&queue);
                    let pending = pending.clone();
                    let tree = tree.clone();
                    let sender = sender.clone();

                    tokio::task::spawn(async move {
                        HubStateDiffer::sync_worker(source, client, queue, pending, tree, sender)
                            .await
                    })
                })
                .collect::<Vec<JoinHandle<_>>>();

            let results = futures::future::join_all(workers).await;
            let mut batch_count: usize = 0;
            for result in results {
                match result {
                    Ok(Ok(count)) => {
                        // `sync_ids` is of type `HashSet<Vec<u8>>` here, coming from the spawned task.
                        num_processed += count;
                        batch_count += count;
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

            info!(
                "successfully processed batch of messages [duration: {:?}] [batch_processed: {:?}] [num_processed: {:?}]",
                (Utc::now() - duration_start).num_milliseconds(), batch_count, num_processed,
            );

            let num_pending = pending.read().await.len();
            queue
                .write()
                .await
                .extend(pending.write().await.drain(..num_pending));

            if current_time < start_time && queue.read().await.is_empty() {
                break;
            }
        }

        Ok(num_processed)
    }

    // diff exhaustive will do a full scan of two hub tries
    // to return all the sync IDs that are missing
    // since it is expensive, we should take the result and persist it to a file as well
    pub async fn diff_sync_ids(self, start_date: DateTime<Utc>, end_date: DateTime<Utc>) -> eyre::Result<SyncIdDiffReport> {
        const DB_OPS_CHANNEL_SIZE: usize = 131072;

        let (start, end) = (
            start_date.timestamp() as u32,
            end_date.timestamp() as u32,
        );

        let (sender, receiver) = tokio::sync::mpsc::channel::<DbOperation>(DB_OPS_CHANNEL_SIZE);

        let source_tree_name = self.endpoint_a.uri().host().ok_or(eyre!("missing host"))?;
        let source_tree = self.repo.open_cache_tree(source_tree_name).await?;

        let target_tree_name = self.endpoint_b.uri().host().ok_or(eyre!("missing host"))?;
        let target_tree = self.repo.open_cache_tree(target_tree_name).await?;

        let db_handle = spawn_tree_thread(self.repo.conn()?, receiver);

        let source_handle = tokio::spawn(HubStateDiffer::sync_and_persist(
            self.endpoint_a.clone(),
            start,
            end,
            source_tree,
            sender.clone(),
        ));

        let target_handle = tokio::spawn(HubStateDiffer::sync_and_persist(
            self.endpoint_b.clone(),
            start,
            end,
            target_tree,
            sender.clone(),
        ));

        let result = futures::future::join(source_handle, target_handle).await;
        drop(sender);
        let _ = db_handle
            .join()
            .map_err(|e| eyre!("error joining db thread: {:?}", e))?;

        let only_in_a = self.repo.sync_id_set_difference(
            self.endpoint_a.clone().uri().to_string(),
            self.endpoint_b.clone().uri().to_string(),
        )?;
        let only_in_b = self.repo.sync_id_set_difference(
            self.endpoint_b.clone().uri().to_string(),
            self.endpoint_a.clone().uri().to_string(),
        )?;

        // Should just use the repository to get the disjoint set
        // of sync ids because storing them in memory is way too expensive
        // and duckdb is fast enough to return it in seconds
        Ok(SyncIdDiffReport {
            only_in_a,
            only_in_b,
        })
    }
}
