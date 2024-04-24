use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use crate::proto::hub_service_client::HubServiceClient;
use tonic::transport::Channel;
use crate::proto::{Message, TrieNodeMetadataResponse, TrieNodePrefix};

#[derive(Debug)]
pub struct HubStateDiffer {
    client_a: HubServiceClient<Channel>,
    client_b: HubServiceClient<Channel>,
    queue: VecDeque<(Rc<Option<TrieNodeMetadataResponse>>, Rc<TrieNodeMetadataResponse>)>
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

// 1. get the latest state and info from the peer
// 1.5 here we would use the latest sync snapshot state from both endpoints
// to check if they match in terms of their root hashes, number of messages, etc
// but for our purposes we want to drill down and see where they are different so we
// will skip for now

// 2. After we do the "top level" check on snapshot, we now get the metadata
// used to enqueue the root node
// 3. We now got the metadata, and enqueue using the root node prefix to drill into it
// and get the metadata for the root node prefix
// NOTE: this is a bit redundant because the metadata from previous step
// already returns root node, but the Typescript code does it for consistency
// and so the root node is treated as the other nodes to reduce code complexity
// 4. check if our node is empty or if the target hub (which for now we assume is at
// later state i.e. has more messages) has <= 1 num messages, because if it's zero,
// there might have been a delete operation or something and we need to reach their
// same level of consistent state
// 5. Now we fetch the missing sync ids - note that in the actual implementation
// we would just get all the IDs for both hubs since we are drilling into all their
// differences which would be from the root node
// We could then pretty print their trees in the CLI to show where they differ

// 5.1 convert the missing sync id byte vectors into SyncId type
// and then fetch the messages for each sync id


impl HubStateDiffer {
    pub(crate) fn new(client_a: HubServiceClient<Channel>, client_b: HubServiceClient<Channel>) -> Self {
        Self { client_a, client_b, queue: Default::default() }
    }
    async fn process_queue(&mut self) -> eyre::Result<(Vec<Message>)> {
        let mut differences = vec![];
        while let Some((source_node_metadata, target_node_metadata)) = self.queue.pop_front() {
            println!("Processing queue: \nsource: {:?}, \ntarget: {:?}\n", source_node_metadata, target_node_metadata);
            // 3. We now got the metadata, and enqueue using the root node prefix to drill into it
            // and get the metadata for the root node prefix
            // NOTE: this is a bit redundant because the metadata from previous step
            // already returns root node, but the Typescript code does it for consistency
            // and so the root node is treated as the other nodes to reduce code complexity

            if source_node_metadata.is_none()
                || source_node_metadata.as_ref().as_ref().map_or(false, |s| s.num_messages == 0)
                || target_node_metadata.num_messages <= 1 {
                let target_missing_sync_ids = self.client_b
                    .get_all_sync_ids_by_prefix(tonic::Request::new(TrieNodePrefix {
                        prefix: target_node_metadata.prefix.clone(),
                    }))
                    .await?
                    .into_inner();
                // now get the messages
                let target_messages = self.client_b
                    .get_all_messages_by_sync_ids(tonic::Request::new(target_missing_sync_ids.clone()))
                    .await?
                    .into_inner();

                let source_messages = self.client_a
                    .get_all_messages_by_sync_ids(tonic::Request::new(target_missing_sync_ids.clone()))
                    .await?
                    .into_inner();

                for (i, target_message) in target_messages.messages.iter().enumerate() {
                    let source_message = source_messages.messages.get(i);
                    if source_message.is_none() {
                        println!("Missing message: {:?} {:?}", target_message.clone().data.unwrap().r#type, target_message);
                        differences.push(target_message.clone());
                    }
                }
            } else {
                println!("skiping for some reason : {:?} {:?}\n", source_node_metadata, target_node_metadata)
            }

            // Recurse into the children to to schedule the next set of work. Note that we didn't after  the
            // `getMessagesFromOtherNode` above. This is because even after we get messages from the other node,
            // there still might be left over messages at this node (if, for eg., there are > 10k messages, and the
            // other node only sent 1k messages).
            for (target_child_char, target_child) in NodeMetaData::from_proto(target_node_metadata).children.iter() {
                let source = NodeMetaData::from_proto(
                    Rc::new(source_node_metadata
                        .as_ref()
                        .as_ref()
                        .map_or(Default::default(), |s| s.clone())));
                let source_node = source.children.get(target_child_char);
                println!("\nEnqueueing: {:?} {:?}\n", source_node, target_child);
                self.enqueue(
                    source_node.map_or(Rc::new(None), |c| Rc::new(Some(c.to_proto())))
                   ,
                   Rc::new(target_child.to_proto())
                )
            }
        }
        Ok(differences)
    }
    fn enqueue(&mut self, source_node_metadata: Rc<Option<TrieNodeMetadataResponse>>, target_node_metadata: Rc<TrieNodeMetadataResponse>) {
        self.queue.push_back((source_node_metadata, target_node_metadata));
    }

    pub async fn _diff(mut self) -> eyre::Result<Vec<Message>> {
        let source_client = &mut self.client_a;
        let target_client = &mut self.client_b;

        let source_peer_state_result = source_client
            .get_sync_snapshot_by_prefix(tonic::Request::new(Default::default()))
            .await
            .unwrap();

        let target_peer_state_result = target_client
            .get_sync_snapshot_by_prefix(tonic::Request::new(Default::default()))
            .await
            .unwrap();

        let source_peer_state = source_peer_state_result.into_inner();
        let target_peer_state = target_peer_state_result.into_inner();
        if source_peer_state.root_hash.eq(&target_peer_state.root_hash) {
            println!("Root hashes match, no differences found");
            return Ok(vec![]);
        }

        let target_metadata = target_client
            .get_sync_metadata_by_prefix(tonic::Request::new(Default::default()))
            .await
            .unwrap()
            .into_inner();

        let source_node = source_client
            .get_sync_metadata_by_prefix(tonic::Request::new(TrieNodePrefix {
                prefix: target_metadata.clone().prefix,
            }))
            .await?;
        let target_node = target_client
            .get_sync_metadata_by_prefix(tonic::Request::new(TrieNodePrefix {
                prefix: target_metadata.clone().prefix,
            }))
            .await?;

        let source_metadata = source_node.into_inner();
        // 1.5 here we would use the latest sync snapshot state from both endpoints
        // to check if they match in terms of their root hashes, number of messages, etc
        // but for our purposes we want to drill down and see where they are different so we
        // will skip for now

        // 2. After we do the "top level" check on snapshot, we now get the metadata
        // used to enqueue the root node
        {
            self.enqueue(Rc::new(Some(source_metadata)), Rc::new(target_metadata));
        }

        let mut differences: Vec<Message> = vec![];
        {
            differences.extend(self.process_queue().await?);
        }

        Ok(differences)
    }
    async fn diff(self) -> eyre::Result<()> {
        // TODO: for now we don't focus on source to simplify the logic
        // let source_metadata = source_client
        //     .get_sync_metadata_by_prefix(tonic::Request::new(Default::default()))
        //     .await
        //     .unwrap()
        //     .into_inner();

            // 5. Now we fetch the missing sync ids - note that in the actual implementation
            // we would just get all the IDs for both hubs since we are drilling into all their
            // differences which would be from the root node
            // We could then pretty print their trees in the CLI to show where they differ

            // 5.1 convert the missing sync id byte vectors into SyncId type
            // and then fetch the messages for each sync id
            // TODO: the SyncIds type is protobuf but the SyncId itself is not which
            //   I guess is because of the bit packing needed to make it work
            //   so we would need to convert the byte vectors into SyncId type

            Ok(())
        }
    }
