use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use clap::Args;
use eyre::eyre;
use slog_scope::info;

use crate::cmd::cmd::BaseRpcConfig;
use crate::hub_diff::{HubStateDiffer, SyncIdDiffReport};

#[derive(Args, Debug)]
struct TimeArgs {
    #[arg(long)]
    from_day: Option<String>,

    #[arg(long)]
    to_day: Option<String>,
}

impl TimeArgs {
    pub fn parse_start_and_end_time(&self) -> eyre::Result<(DateTime<Utc>, DateTime<Utc>)> {
        let end_date = match &self.to_day {
            Some(day) => {
                let date_time = NaiveDate::parse_from_str(&day, "%Y-%m-%d")?;
                Utc.from_utc_datetime(&date_time.and_hms_opt(0, 0, 0).ok_or(eyre!("Invalid date"))?)
            },
            None => {
                Utc::now()
            },
        };
        let start_date = match &self.from_day {
            Some(day) => {
                let date_time = NaiveDate::parse_from_str(&day, "%Y-%m-%d")?;
                Utc.from_utc_datetime(&date_time.and_hms_opt(0, 0, 0).ok_or(eyre!("Invalid date"))?)
            },
            None => {
                end_date - chrono::Duration::days(1)
            },
        };

        Ok((start_date, end_date))
    }

}

#[derive(Debug, Args, Clone)]
pub struct SourceConfig {
    #[arg(long)]
    #[arg(default_value = "true")]
    pub(crate) source_http: bool,

    #[arg(long)]
    #[arg(default_value = "false")]
    pub(crate) source_https: bool,

    #[arg(long, default_value = "2283")]
    pub(crate) source_port: u16,

    #[arg(long)]
    pub(crate) source_endpoint: String,
}

#[derive(Debug, Args, Clone)]
pub struct TargetConfig {
    #[arg(long)]
    #[arg(default_value = "true")]
    pub(crate) target_http: bool,

    #[arg(long)]
    #[arg(default_value = "false")]
    pub(crate) target_https: bool,

    #[arg(long, default_value = "2283")]
    pub(crate) target_port: u16,

    #[arg(long)]
    pub(crate) target_endpoint: String,
}


#[derive(Args, Debug)]
pub struct DiffCommand {
    #[clap(flatten)]
    source: SourceConfig,

    #[clap(flatten)]
    target: TargetConfig,

    #[arg(long)]
    event_type: Option<String>,

    #[clap(flatten)]
    time_args: TimeArgs,
}

impl DiffCommand {
    pub async fn execute(&self) -> eyre::Result<()> {
        let source_endpoint =
            crate::cmd::cmd::load_endpoint(&BaseRpcConfig {
                http: self.source.source_http,
                https: self.source.source_https,
                port: self.source.source_port,
                endpoint: self.source.source_endpoint.clone(),
            })?;
        let target_endpoint =
            crate::cmd::cmd::load_endpoint(&BaseRpcConfig {
                http: self.target.target_http,
                https: self.target.target_https,
                port: self.target.target_port,
                endpoint: self.target.target_endpoint.clone(),
            })?;

        let (start_time, end_time) = self.time_args.parse_start_and_end_time()?;
        info!("Performing diff between {:?} and {:?}", start_time, end_time);

        let state_differ = HubStateDiffer::new(source_endpoint, target_endpoint);
        let sync_id_diff_report = state_differ?
            .diff_sync_ids(start_time, end_time)
            .await
            .or_else(|e| Err(eyre!("{:?}", e)))?;

        println!("-----------------------Only in Source-----------------------------");
        println!("{}", SyncIdDiffReport::histogram_by_root_prefix(&sync_id_diff_report.only_in_a)?);
        println!("{}", SyncIdDiffReport::histogram_by_timestamp(&sync_id_diff_report.only_in_b)?);

        println!("-----------------------Only in Target-----------------------------");
        println!("{}", SyncIdDiffReport::histogram_by_root_prefix(&sync_id_diff_report.only_in_b)?);
        println!("{}", SyncIdDiffReport::histogram_by_timestamp(&sync_id_diff_report.only_in_b)?);

        Ok(())
    }
}
