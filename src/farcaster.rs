use chrono::Utc;

pub const FARCASTER_EPOCH: u64 = 1609459200; // Seconds from UNIX_EPOCH to Jan 1, 2021
pub fn farcaster_to_unix(timestamp: u64) -> u64 {
    FARCASTER_EPOCH + timestamp
}

pub fn farcaster_time_range(start: chrono::DateTime<Utc>, end: chrono::DateTime<Utc>) -> impl Iterator<Item = u32> {
    let start = start.timestamp() as u32 - (FARCASTER_EPOCH) as u32;
    let end = end.timestamp() as u32 - (FARCASTER_EPOCH) as u32;
    (start..end).step_by(1)
}

pub fn farcaster_time_to_str(timestamp: u32) -> String {
    format!("{:010}", timestamp).to_string()
}

pub fn str_bytes_to_unix_time(timestamp: &str) -> u64 {
    let timestamp = timestamp.parse::<u64>().unwrap();
    farcaster_to_unix(timestamp)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use super::*;

    #[test]
    fn test_farcaster_to_unix() {
        assert_eq!(farcaster_to_unix(0), FARCASTER_EPOCH);
        assert_eq!(farcaster_to_unix(1), FARCASTER_EPOCH + 1);
        assert_eq!(farcaster_to_unix(2), FARCASTER_EPOCH + 2);
    }

    #[test]
    fn test_farcaster_time_range() {
        let start = Utc.with_ymd_and_hms(2021, 1, 1,0, 0, 0);
        let end = Utc.with_ymd_and_hms(2021, 1, 1,0, 0, 2);
        let range: Vec<u32> = farcaster_time_range(start.unwrap(), end.unwrap()).collect();
        assert_eq!(range, vec![0, 1]);
    }

    #[test]
    fn test_farcaster_time_to_str() {
        assert_eq!(farcaster_time_to_str(0), "0000000000");
        assert_eq!(farcaster_time_to_str(1), "0000000001");
        assert_eq!(farcaster_time_to_str(2), "0000000002");
    }

    #[test]
    fn test_str_bytes_to_unix_time() {
        assert_eq!(str_bytes_to_unix_time("0"), FARCASTER_EPOCH);
        assert_eq!(str_bytes_to_unix_time("1"), FARCASTER_EPOCH + 1);
        assert_eq!(str_bytes_to_unix_time("2"), FARCASTER_EPOCH + 2);
    }
}
