use clap::Args;
use slog_scope::info;

#[derive(Debug, Args)]
struct DbCmd {
    #[arg(long)]
    path: String,

}

impl DbCmd {
    fn execute(&self) {
        info!("Executing db command with path: {}", self.path);
    }
}
