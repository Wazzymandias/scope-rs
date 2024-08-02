use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use clap::Args;
use rocksdb::{MultiThreaded, Options, TransactionDB, TransactionDBOptions};

pub const TRIE_DBPATH_PREFIX: &str = "trieDb";

#[derive(Args, Debug)]
pub struct InspectCommand {
    #[arg(long)]
    db_path: String,
}

impl InspectCommand {
    pub fn execute(&self) -> eyre::Result<()> {
        let db_options = Options::default();
        let txdb_options = TransactionDBOptions::default();
        let db: TransactionDB<MultiThreaded> = TransactionDB::open(&db_options, &txdb_options, &self.db_path)?;

        let mut read_opts = rocksdb::ReadOptions::default();

        let user_root_prefix= vec![1u8, 0u8, 0u8, 0u8, 0u8, 85u8];
        read_opts.set_iterate_lower_bound(user_root_prefix);
        let upper_bound = vec![1u8, 0xff, 0xff, 0xff, 0xff, 0xff];
        read_opts.set_iterate_upper_bound(upper_bound);
        read_opts.fill_cache(false);
        read_opts.set_auto_readahead_size(true);
        read_opts.set_async_io(true);

        let mut db_iter = db.raw_iterator_opt(read_opts);
        db_iter.seek_to_first();

        let total_key_size_bytes = AtomicU64::new(0);
        let total_value_size_bytes = AtomicU64::new(0);
        while db_iter.valid() {
            db_iter.item().and_then(|(key, value)| {
                if key.len() > 4 {
                    let postfix = key[1 + 4];
                    if postfix > 85 {
                        total_key_size_bytes.fetch_add(key.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        total_value_size_bytes.fetch_add(value.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                Some(())
            });
            db_iter.next();
        }

        println!("[key_bytes: {}], [value_bytes: {}]",
            total_key_size_bytes.load(std::sync::atomic::Ordering::SeqCst),
            total_value_size_bytes.load(std::sync::atomic::Ordering::SeqCst),
        );
        Ok(())
    }
}

fn iterate_trie_db(db_path_prefix: String) -> eyre::Result<()> {
    let path = Path::join(Path::new(&db_path_prefix), Path::new(TRIE_DBPATH_PREFIX))
        .into_os_string()
        .into_string()
        .map_err(|e| eyre::eyre!("Error converting path to string: {:?}", e))?;
    let _db = Arc::new(TransactionDB::<MultiThreaded>::open(&Options::default(), &TransactionDBOptions::default(), &path)?);

    Ok(())
}
