use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use eyre::eyre;
use serde::de::Error;
use std::fs::File;
use std::io::BufReader;

use crate::farcaster::sync_id::{SyncId, TIMESTAMP_LENGTH};
use crate::farcaster::time::{farcaster_to_unix, FARCASTER_EPOCH};
use crate::proto::SyncIds;

#[derive(Debug, Parser)]
pub struct ParseCommand {
    #[clap(flatten)]
    base: crate::cmd::cmd::BaseConfig,

    #[clap(subcommand)]
    subcommand: SubCommands,
}

impl ParseCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        match &self.subcommand {
            SubCommands::SyncId(sync_id) => sync_id.execute()?,
        }

        Ok(())
    }
}

#[derive(Debug, Subcommand)]
enum SubCommands {
    #[clap(name = "sync-id")]
    SyncId(SyncIdCommand),
}

#[derive(Args, Debug)]
struct SyncIdCommand {
    sync_id: String,
}

fn timestamp_from_sync_id(input: &Vec<u8>) -> eyre::Result<DateTime<Utc>> {
    let s = String::from_utf8(input[0..TIMESTAMP_LENGTH].to_vec())?;
    let first = &s[0..TIMESTAMP_LENGTH];
    let ts_value = first.parse::<u32>()?;
    let c = chrono::DateTime::from_timestamp((FARCASTER_EPOCH + ts_value as u64) as i64, 0);
    match c {
        Some(date) => Ok(date),
        _ => Err(eyre!("failed to parse timestamp")),
    }
}

impl SyncIdCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        let parts: Vec<&str> = self.sync_id.split(',').collect();
        if parts.len().le(&(TIMESTAMP_LENGTH - 1)) {
            return Err(eyre!("invalid sync id: {:?}", parts));
        }

        let bytes: Vec<u8> = parts
            .iter()
            .map(|part| {
                let result = part.parse::<u8>();
                if result.is_err() {
                    println!("error parsing part: {}", part);
                }
                result.unwrap()
            })
            .collect();
        println!("bytes: {:?}", bytes);
        let s = String::from_utf8(bytes[0..TIMESTAMP_LENGTH].to_vec())?;
        let first = &s[0..TIMESTAMP_LENGTH];
        let ts_value = first.parse::<u32>()?;
        println!("{}", ts_value);
        let c = chrono::DateTime::from_timestamp((FARCASTER_EPOCH + ts_value as u64) as i64, 0);
        let d = c.unwrap();
        println!("{:?}", farcaster_to_unix(ts_value as u64));
        // let x = c.unwrap();
        // let y = x.to_utc().to_string();
        println!("{}", d.to_string());

        // let source_file = File::open("json/source_sync_ids.json")?;
        let source_file = File::open("json/target_sync_ids.json")?;
        let source_reader = BufReader::new(source_file);
        let source_sync_ids: Result<SyncIds, serde_json::Error> =
            serde_json::from_reader(source_reader);
        source_sync_ids
            .and_then(|sync_ids| {
                sync_ids.sync_ids.iter().for_each(|sync_id| {
                    let ts = timestamp_from_sync_id(sync_id);
                    match ts {
                        Ok(t) => {
                            println!("{}", t)
                        }
                        _ => {}
                    }
                });
                Ok(())
            })
            .expect("TODO: panic message");
        let target_file = File::open("json/target_sync_ids.json")?;

        Ok(())
    }
}
