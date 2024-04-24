use std::convert::TryInto;
use crate::proto::OnChainEventType;

// Given constants and types for the simulation
const TIMESTAMP_LENGTH: usize = 10;
const FID_BYTES: usize = 4; // Assuming 4 bytes for fid
const HASH_LENGTH: usize = 20; // Assuming fixed length for hash, for simplicity

#[derive(Debug, Clone, PartialEq, Eq)]
enum SyncIdType {
    Unknown = 0,
    Message = 1,
    FName = 2,
    OnChainEvent = 3,
}

#[derive(Debug)]
pub(crate) enum UnpackedSyncId {
    Unknown { fid: u32 },
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
                // Emulated parsing logic based on bytes layout
                // Note: Real implementation should extract and convert actual values from `bytes`
                let fid = 0; // Placeholder: Extract fid from bytes
                let primary_key = vec![]; // Placeholder: Extract primary_key from bytes
                let hash = vec![]; // Placeholder: Extract hash from bytes
                UnpackedSyncId::Message { fid, primary_key, hash }
            },
            FName => {
                let fid = 0; // Placeholder: Extract fid from bytes
                let name = vec![]; // Placeholder: Extract name from bytes
                let padded = true; // Placeholder: Determine if name is padded
                UnpackedSyncId::FName { fid, name, padded }
            },
            OnChainEvent => {
                let fid = 0; // Placeholder: Extract fid from bytes
                let event_type = OnChainEventType::EventTypeIdRegister; // Placeholder: Determine event type
                let block_number = 0; // Placeholder: Extract block_number from bytes
                let log_index = 0; // Placeholder: Extract log_index from bytes
                UnpackedSyncId::OnChainEvent { fid, event_type, block_number, log_index }
            },
        }
    }
}


pub struct SyncId(pub Vec<u8>);

impl SyncId {
    pub(crate) fn unpack(&self) -> UnpackedSyncId {
        let bytes = &self.0;
        let sync_id_type = bytes[TIMESTAMP_LENGTH];

        match sync_id_type {
            _ if sync_id_type == SyncIdType::Message as u8 => {
                // Assuming TIMESTAMP_LENGTH + 1 for the root prefix, then reading fid
                let fid = u32::from_be_bytes(bytes[(TIMESTAMP_LENGTH + 1)..(TIMESTAMP_LENGTH + 1 + FID_BYTES)].try_into().unwrap());
                // Skipping the primary key and hash logic for brevity
                UnpackedSyncId::Message { fid, primary_key: vec![], hash: vec![] }
            },
            // Implement other cases as per need
            _ => UnpackedSyncId::Unknown { fid: 0 },
        }
    }
}
