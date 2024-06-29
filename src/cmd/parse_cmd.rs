use chrono::{DateTime, NaiveDate, Utc};
use clap::{Args, Parser, Subcommand};
use eyre::{eyre, Report};

use crate::farcaster::sync_id::{FID_BYTES, TIMESTAMP_LENGTH};
use crate::farcaster::time::{farcaster_to_unix, FARCASTER_EPOCH};

#[derive(Debug, Parser)]
pub struct ParseCommand {
    #[clap(subcommand)]
    subcommand: SubCommands,
}

impl ParseCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        match &self.subcommand {
            SubCommands::SyncId(sync_id) => sync_id.execute()?,
            SubCommands::Timestamp(timestamp) => timestamp.execute()?,
        }

        Ok(())
    }
}

#[derive(Debug, Subcommand)]
enum SubCommands {
    #[clap(name = "sync-id")]
    SyncId(SyncIdCommand),
    #[clap(name = "ts")]
    Timestamp(TimestampCommand),
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
                let result = part.trim().parse::<u8>();
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
        println!("{}", d);
        let fid = u32::from_be_bytes(
            bytes[TIMESTAMP_LENGTH + 1..TIMESTAMP_LENGTH + 1 + FID_BYTES]
                .try_into()
                .unwrap(),
        );
        println!("fid: {}", fid);

        // let source_file = File::open("json/source_sync_ids.json")?;
        // let source_file = File::open("json/target_sync_ids.json")?;
        // let source_reader = BufReader::new(source_file);
        // let source_sync_ids: Result<SyncIds, serde_json::Error> =
        //     serde_json::from_reader(source_reader);
        // source_sync_ids
        //     .and_then(|sync_ids| {
        //         sync_ids.sync_ids.iter().for_each(|sync_id| {
        //             let ts = timestamp_from_sync_id(sync_id);
        //             match ts {
        //                 Ok(t) => {
        //                     println!("{}", t)
        //                 }
        //                 _ => {}
        //             }
        //         });
        //         Ok(())
        //     })
        //     .expect("TODO: panic message");
        // let target_file = File::open("json/target_sync_ids.json")?;

        Ok(())
    }
}

// SyncId:
// Example: [48,49,48,54,50,49,49,50,56,48,1,0,7,243]
//   1. The first 10 bytes are the timestamp, which is the delta since Farcaster Epoch
//   2. The next byte is the type of the event
//   public type(): SyncIdType {
//     switch (this._bytes[TIMESTAMP_LENGTH]) {
//       case RootPrefix.User:
//         return SyncIdType.Message;
//       case RootPrefix.FNameUserNameProof:
//         return SyncIdType.FName;
//       case RootPrefix.OnChainEvent:
//         return SyncIdType.OnChainEvent;
//       default:
//         return SyncIdType.Unknown;
//     }
//   }
//   3. In this case, since the type is 0x01, it is of type User
//   4. The next 4 bytes are the user FID

#[derive(Args, Debug)]
struct TimestampCommand {
    #[arg(long = "byte-vector", short = 'b')]
    byte_vector: Option<String>,

    #[arg(long = "from-date")]
    from_date: Option<String>,

    #[arg(long = "from-delta")]
    from_delta: Option<u32>,

    #[arg(long = "from-range")]
    from_range: Option<String>
}

fn parse_timestamp(input: &str) -> eyre::Result<Vec<u8>> {
    let cleaned = input.trim().trim_start_matches("[").trim_end_matches("]");

    let numbers = cleaned
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().parse::<u8>().map_err(|e| Report::new(e)))
        .collect();

    numbers
}

impl TimestampCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        if self.byte_vector.is_some() {
            let vec = parse_timestamp(self.byte_vector.as_ref().unwrap().as_str())?;
            let s = String::from_utf8(vec[0..TIMESTAMP_LENGTH.min(vec.len())].to_vec())?;
            let first = &s[0..TIMESTAMP_LENGTH.min(vec.len())];
            let ts_value = first.parse::<u32>()?;
            println!("{}", ts_value);
            let c = chrono::DateTime::from_timestamp((FARCASTER_EPOCH + ts_value as u64) as i64, 0);
            let d = c.unwrap();
            println!("{:?}", farcaster_to_unix(ts_value as u64));
            println!("{}", d);
        } else if self.from_date.is_some() {
            let date_time = NaiveDate::parse_from_str(self.from_date.as_ref().unwrap().as_str(), "%Y-%m-%d")?;
            let unix = date_time.and_hms_opt(0, 0, 0).ok_or(eyre!("Invalid date"))?.and_utc().timestamp();
            println!("{}", unix);
            let diff = unix - FARCASTER_EPOCH as i64;
            println!("{}", diff);
            let diff_str = diff.to_string();
            let padding = "0".repeat(TIMESTAMP_LENGTH - diff_str.len());
            let formatted = format!("{}{}", padding, diff_str);
            println!("{:?}", formatted.as_bytes());
        } else if self.from_delta.is_some() {
            let delta = self.from_delta.unwrap();
            let unix = FARCASTER_EPOCH as u32 + delta;
            let formatted = format!("{}", chrono::DateTime::from_timestamp(unix as i64, 0).ok_or(eyre!("Invalid timestamp"))?);
            println!("{:?}", formatted.to_string());
        } else if self.from_range.is_some() {
            let parts: Vec<&str> = self.from_range.as_ref().unwrap().trim().split(',').collect();
            if parts.len() != 2 {
                return Err(eyre!("invalid range"));
            }
            let start = NaiveDate::parse_from_str(parts[0], "%Y-%m-%d")?;
            let end = NaiveDate::parse_from_str(parts[1], "%Y-%m-%d")?;
            let start_unix = start.and_hms_opt(0, 0, 0).ok_or(eyre!("Invalid date"))?.and_utc().timestamp();
            let end_unix = end.and_hms_opt(0, 0, 0).ok_or(eyre!("Invalid date"))?.and_utc().timestamp();
            let start_fc = start_unix - FARCASTER_EPOCH as i64;
            let end_fc = end_unix - FARCASTER_EPOCH as i64;
            for i in start_fc..end_fc {
                let diff_str = i.to_string();
                let padding = "0".repeat(TIMESTAMP_LENGTH - diff_str.len());
                let formatted = format!("{}{}", padding, diff_str);
                println!("{:?}", formatted.as_bytes());
            }

        }
        let now = chrono::Utc::now();
        let hour_ago = now - chrono::Duration::days(1);
        let unix = hour_ago.timestamp();
        let diff = unix - FARCASTER_EPOCH as i64;
        let diff_str = diff.to_string();
        let padding = "0".repeat(TIMESTAMP_LENGTH - diff_str.len());
        let formatted = format!("{}{}", padding, diff_str);
        println!("{:?}", formatted.as_bytes());

        // let timestamp = self.timestamp.parse::<u32>()?;
        // let unix_time = farcaster_to_unix(timestamp as u64);
        // println!("unix time: {}", unix_time);
        Ok(())
    }
}
