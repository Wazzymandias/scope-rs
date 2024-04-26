use crate::proto::OnChainEventType;
use std::convert::TryInto;

// Given constants and types for the simulation
pub const TIMESTAMP_LENGTH: usize = 10;
const FID_BYTES: usize = 4; // Assuming 4 bytes for fid
const HASH_LENGTH: usize = 20; // Assuming fixed length for hash, for simplicity

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RootPrefix {
    /* Used for multiple purposes, starts with a 4-byte fid */
    User = 1,
    /* Used to index casts by parent */
    CastsByParent = 2,
    /* Used to index casts by mention */
    CastsByMention = 3,
    /* Used to index links by target */
    LinksByTarget = 4,
    /* Used to index reactions by target  */
    ReactionsByTarget = 5,
    /* Deprecated */
    // IdRegistryEvent = 6,
    // NameRegistryEvent = 7,
    // IdRegistryEventByCustodyAddress = 8,
    /* Used to store the state of the hub */
    HubState = 9,
    /* Revoke signer jobs */
    JobRevokeMessageBySigner = 10,
    /* Sync Merkle Trie Node */
    SyncMerkleTrieNode = 11,
    /* Deprecated */
    // JobUpdateNameExpiry = 12,
    // NameRegistryEventsByExpiry = 13,
    /* To check if the Hub was cleanly shutdown */
    HubCleanShutdown = 14,
    /* Event log */
    HubEvents = 15,
    /* The network ID that the rocksDB was created with */
    Network = 16,
    /* Used to store fname server name proofs */
    FNameUserNameProof = 17,
    /* Used to store gossip network metrics */
    // Deprecated, DO NOT USE
    // GossipMetrics = 18,

    /* Used to index user submited username proofs */
    UserNameProofByName = 19,

    // Deprecated
    // RentRegistryEvent = 20,
    // RentRegistryEventsByExpiry = 21,
    // StorageAdminRegistryEvent = 22,

    /* Used to store on chain events */
    OnChainEvent = 23,

    /** DB Schema version */
    DBSchemaVersion = 24,

    /* Used to index verifications by address */
    VerificationByAddress = 25,

    /* Store the connected peers */
    ConnectedPeers = 26,

    /* Used to index fname username proofs by fid */
    FNameUserNameProofByFid = 27,
}

impl RootPrefix {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => RootPrefix::User,
            2 => RootPrefix::CastsByParent,
            3 => RootPrefix::CastsByMention,
            4 => RootPrefix::LinksByTarget,
            5 => RootPrefix::ReactionsByTarget,
            9 => RootPrefix::HubState,
            10 => RootPrefix::JobRevokeMessageBySigner,
            11 => RootPrefix::SyncMerkleTrieNode,
            14 => RootPrefix::HubCleanShutdown,
            15 => RootPrefix::HubEvents,
            16 => RootPrefix::Network,
            17 => RootPrefix::FNameUserNameProof,
            19 => RootPrefix::UserNameProofByName,
            23 => RootPrefix::OnChainEvent,
            24 => RootPrefix::DBSchemaVersion,
            25 => RootPrefix::VerificationByAddress,
            26 => RootPrefix::ConnectedPeers,
            27 => RootPrefix::FNameUserNameProofByFid,
            _ => RootPrefix::User,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            RootPrefix::User => 1,
            RootPrefix::CastsByParent => 2,
            RootPrefix::CastsByMention => 3,
            RootPrefix::LinksByTarget => 4,
            RootPrefix::ReactionsByTarget => 5,
            RootPrefix::HubState => 9,
            RootPrefix::JobRevokeMessageBySigner => 10,
            RootPrefix::SyncMerkleTrieNode => 11,
            RootPrefix::HubCleanShutdown => 14,
            RootPrefix::HubEvents => 15,
            RootPrefix::Network => 16,
            RootPrefix::FNameUserNameProof => 17,
            RootPrefix::UserNameProofByName => 19,
            RootPrefix::OnChainEvent => 23,
            RootPrefix::DBSchemaVersion => 24,
            RootPrefix::VerificationByAddress => 25,
            RootPrefix::ConnectedPeers => 26,
            RootPrefix::FNameUserNameProofByFid => 27,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SyncIdType {
    Unknown = 0,
    Message = 1,
    FName = 2,
    OnChainEvent = 3,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum UnpackedSyncId {
    Unknown {
        fid: u32,
    },
    Message {
        fid: u32,
        primary_key: Vec<u8>,
        hash: Vec<u8>,
    },
    FName {
        fid: u32,
        name: Vec<u8>,
        padded: bool,
    },
    OnChainEvent {
        fid: u32,
        event_type: OnChainEventType,
        block_number: u32,
        log_index: u32,
    },
}

impl UnpackedSyncId {
    // Assuming you have a mechanism to determine the type and parse accordingly
    // This function is a placeholder to be fleshed out with actual logic
    fn from_bytes(bytes: Vec<u8>) -> Self {
        use SyncIdType::*;

        // Basic check to determine type from bytes, for example:
        let sync_id_type = match bytes[TIMESTAMP_LENGTH] {
            1 => Message,
            2 => FName,
            3 => OnChainEvent,
            _ => Unknown,
        };

        match sync_id_type {
            Unknown => UnpackedSyncId::Unknown { fid: 0 },
            Message => {
                // Note: Real implementation should extract and convert actual values from `bytes`
                let fid = 0; // Placeholder: Extract fid from bytes
                let primary_key = vec![]; // Placeholder: Extract primary_key from bytes
                let hash = vec![]; // Placeholder: Extract hash from bytes
                UnpackedSyncId::Message {
                    fid,
                    primary_key,
                    hash,
                }
            }
            FName => {
                let fid = 0; // Placeholder: Extract fid from bytes
                let name = vec![]; // Placeholder: Extract name from bytes
                let padded = true; // Placeholder: Determine if name is padded
                UnpackedSyncId::FName { fid, name, padded }
            }
            OnChainEvent => {
                let fid = 0; // Placeholder: Extract fid from bytes
                let event_type = OnChainEventType::EventTypeIdRegister; // Placeholder: Determine event type
                let block_number = 0; // Placeholder: Extract block_number from bytes
                let log_index = 0; // Placeholder: Extract log_index from bytes
                UnpackedSyncId::OnChainEvent {
                    fid,
                    event_type,
                    block_number,
                    log_index,
                }
            }
        }
    }
}

pub struct SyncId(pub Vec<u8>);

impl SyncId {
    pub(crate) fn unpack(sync_id: &[u8]) -> UnpackedSyncId {
        assert!(sync_id.len() > TIMESTAMP_LENGTH, "Invalid syncId length");
        let root_prefix = sync_id[TIMESTAMP_LENGTH];

        match RootPrefix::from_u8(root_prefix) {
            RootPrefix::User => {
                // Equivalent to SyncIdType::Message case in TypeScript
                let fid = u32::from_be_bytes(
                    sync_id[TIMESTAMP_LENGTH + 1..TIMESTAMP_LENGTH + 1 + FID_BYTES]
                        .try_into()
                        .unwrap(),
                );
                let hash_start = TIMESTAMP_LENGTH + 1 + FID_BYTES + 1; // Skipping timestamp, fid and set postfix
                let hash = sync_id[hash_start..hash_start + HASH_LENGTH].to_vec();
                let primary_key = sync_id[TIMESTAMP_LENGTH..].to_vec();
                UnpackedSyncId::Message {
                    fid,
                    primary_key,
                    hash,
                }
            }
            RootPrefix::FNameUserNameProof => {
                // Equivalent to SyncIdType::FName case in TypeScript
                let fid = u32::from_be_bytes(
                    sync_id[TIMESTAMP_LENGTH + 1..TIMESTAMP_LENGTH + 1 + FID_BYTES]
                        .try_into()
                        .unwrap(),
                );
                let name_bytes = &sync_id[TIMESTAMP_LENGTH + 1 + FID_BYTES..];
                let first_zero_index = name_bytes
                    .iter()
                    .position(|&x| x == 0)
                    .unwrap_or(name_bytes.len());
                let name = name_bytes[..first_zero_index].to_vec();
                let padded = first_zero_index != name_bytes.len();
                UnpackedSyncId::FName { fid, name, padded }
            }
            RootPrefix::OnChainEvent => {
                // Equivalent to SyncIdType::OnChainEvent case in TypeScript
                let event_type_byte = sync_id[TIMESTAMP_LENGTH + 1 + 1];
                let event_type = match event_type_byte {
                    1 => OnChainEventType::EventTypeSigner,
                    2 => OnChainEventType::EventTypeSignerMigrated,
                    3 => OnChainEventType::EventTypeIdRegister,
                    4 => OnChainEventType::EventTypeStorageRent,
                    _ => OnChainEventType::EventTypeNone,
                };

                let fid = u32::from_be_bytes(
                    sync_id[TIMESTAMP_LENGTH + 1 + 1 + 1..TIMESTAMP_LENGTH + 1 + 1 + 1 + FID_BYTES]
                        .try_into()
                        .unwrap(),
                );
                let block_number_start = TIMESTAMP_LENGTH + 1 + 1 + 1 + FID_BYTES;
                let block_number = u32::from_be_bytes(
                    sync_id[block_number_start..block_number_start + 4]
                        .try_into()
                        .unwrap(),
                );
                let log_index_start = block_number_start + 4; // Adjusted based on TypeScript logic
                let log_index = u32::from_be_bytes(
                    sync_id[log_index_start..log_index_start + 4]
                        .try_into()
                        .unwrap(),
                );

                UnpackedSyncId::OnChainEvent {
                    fid,
                    event_type,
                    block_number,
                    log_index,
                }
            }
            _ => UnpackedSyncId::Unknown { fid: 0 },
        }
    }
}
