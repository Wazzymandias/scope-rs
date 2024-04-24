use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eyre::eyre;
use tonic::transport::Channel;

use crate::proto::{Message, SyncIds, TrieNodeMetadataResponse, TrieNodePrefix};
use crate::proto::hub_service_client::HubServiceClient;
use crate::sync_id::{SyncId, TIMESTAMP_LENGTH, UnpackedSyncId};

const FARCASTER_EPOCH: u64 = 1609459200; // Seconds from UNIX_EPOCH to Jan 1, 2021

fn extract_timestamp(message: &Message) -> eyre::Result<SystemTime> {
    let timestamp = match &message.data {
        Some(data) => {
            // Directly use the timestamp from `data`
            FARCASTER_EPOCH + data.timestamp as u64
        }
        None => {
            // Extract and parse the timestamp from `data_bytes`
            message.data_bytes.as_ref()
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
    client_a: HubServiceClient<Channel>,
    client_b: HubServiceClient<Channel>,
}

#[derive(Debug)]
struct NodeMetaData {
    prefix: Vec<u8>,
    num_messages: u64,
    children: HashMap<char, NodeMetaData>
}

impl NodeMetaData {
    fn from_proto(proto: Rc<TrieNodeMetadataResponse>) -> Self {
        let mut children = HashMap::new();
        for i in 0..proto.children.len() {
            let child = proto.children.get(i);
            if let Some(c) = child {
                // Char is the last char of prefix
                let char = c.prefix[c.prefix.len() - 1] as char;
                children.insert(char, NodeMetaData{
                    prefix: c.prefix.clone(),
                    num_messages: c.num_messages,
                    children: HashMap::new()
                });
            }
        }
        NodeMetaData {
            prefix: proto.prefix.clone(),
            num_messages: proto.num_messages,
            children
        }
    }

    fn to_proto(&self) -> TrieNodeMetadataResponse {
        let mut children = Vec::new();
        for (_, child) in self.children.iter() {
            children.push(TrieNodeMetadataResponse {
                prefix: child.prefix.clone(),
                num_messages: child.num_messages,
                hash: "".to_string(),
                children: Vec::new()
            });
        }
        TrieNodeMetadataResponse {
            prefix: self.prefix.clone(),
            num_messages: self.num_messages,
            hash: "".to_string(),
            children
        }
    }
}

impl HubStateDiffer {
    pub(crate) fn new(client_a: HubServiceClient<Channel>, client_b: HubServiceClient<Channel>) -> Self {
        Self { client_a, client_b }
    }

    async fn sync_ids(input_prefix: Vec<u8>, client: &mut HubServiceClient<Channel>) -> eyre::Result<SyncIds> {
        let mut source_sync_ids: SyncIds = SyncIds{
            sync_ids: vec![]
        };

        async fn fetch_sync_ids(client: &mut HubServiceClient<Channel>, out: &mut SyncIds, prefix: Vec<u8>) -> eyre::Result<()> {
            // Fetch sync ids
            let result = client.get_all_sync_ids_by_prefix(tonic::Request::new(TrieNodePrefix {
                prefix: prefix.clone(),
            })).await;
            match result {
                Ok(sync_ids) => {
                    out.sync_ids.extend(sync_ids.into_inner().sync_ids);
                },
                Err(e) => {
                    return Err(eyre!("error fetching sync ids for prefix {:?}: {:?}", prefix, e))
                }
            }
            Ok(())
        }
        let mut queue: VecDeque<Vec<u8>> = VecDeque::new();
        for i in 0..input_prefix.len() {
            queue.push_back(vec![input_prefix[i]]);
        }

        let mut visited: HashSet<Vec<u8>> = HashSet::new();
        while let(Some(prefix)) = queue.pop_front() {
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
    pub async fn _diff(mut self) -> eyre::Result<HashMap<u64, UnpackedSyncId>> {
        let source_client = &mut self.client_a;
        let target_client = &mut self.client_b;

        let source_snap = source_client
            .get_sync_snapshot_by_prefix(tonic::Request::new(Default::default()))
            .await?
            .into_inner();
        let target_snap = target_client
            .get_sync_snapshot_by_prefix(tonic::Request::new(Default::default()))
            .await?
            .into_inner();

        let source_sync_ids: SyncIds = HubStateDiffer::sync_ids(source_snap.prefix, source_client).await?;
        let target_sync_ids: SyncIds = HubStateDiffer::sync_ids(target_snap.prefix, target_client).await?;

        let source_set: HashSet<Vec<u8>> = source_sync_ids.sync_ids.into_iter().collect();
        let mut message_vec: HashMap<u64, UnpackedSyncId> = HashMap::new();
        let mut fname_vec: HashMap<u64,UnpackedSyncId> = HashMap::new();
        let mut on_chain_event_vec: HashMap<u64, UnpackedSyncId> = HashMap::new();

        target_sync_ids.sync_ids.iter().for_each(|sync_id| {
            if !source_set.contains(sync_id) {
                let id = SyncId(sync_id.clone());
                let timestamp_bytes = id.0[0..TIMESTAMP_LENGTH].to_vec();
                let ts_str = String::from_utf8(timestamp_bytes).unwrap();
                let timestamp = ts_str.parse::<u64>().unwrap();
                let result: UnpackedSyncId = SyncId::unpack(&id.0);
                match result {
                    UnpackedSyncId::Message { fid, primary_key, hash } => {
                        message_vec.insert(timestamp, UnpackedSyncId::Message { fid, primary_key, hash });
                    },
                    UnpackedSyncId::FName { fid, name, padded } => {
                        fname_vec.insert(timestamp, UnpackedSyncId::FName { fid, name, padded });
                    },
                    UnpackedSyncId::OnChainEvent { fid, event_type, block_number, log_index } => {
                        on_chain_event_vec.insert(timestamp, UnpackedSyncId::OnChainEvent { fid, event_type, block_number, log_index });
                    }
                    _ => {
                        println!("unknown sync id found: {:?}", sync_id);
                        // Do nothing
                    }
                }
            }
        });

        Ok(
            message_vec.iter()
                .chain(fname_vec.iter())
                .chain(on_chain_event_vec.iter())
                .map(|(k, v)| (*k, v.clone())) // Create owned versions of both keys and values
                .collect() // Collect into a HashMap<u64, UnpackedSyncId>
        )
    }
}

fn farcaster_to_unix(timestamp: u64) -> u64 {
    FARCASTER_EPOCH + timestamp
}
