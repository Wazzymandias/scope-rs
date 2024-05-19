use sled::Tree;
use slog_scope::error;

#[derive(Debug)]
pub struct CachedRepository {
    persistent_cache_db: sled::Db,
    db_conn: duckdb::Connection,
}

impl CachedRepository {
    pub fn new(cache_path: String, db_path: String) -> eyre::Result<Self> {
        let persistent_cache_db = sled::open(cache_path)?;
        let db_conn = duckdb::Connection::open_with_flags(
            db_path,
            duckdb::Config::default().enable_object_cache(true).unwrap(),
        )?;

        db_conn.execute_batch(
            "\
BEGIN;
SET preserve_insertion_order = false;

CREATE SEQUENCE IF NOT EXISTS seq_sync_id START 1;

CREATE TABLE IF NOT EXISTS sync_ids (
    id UBIGINT PRIMARY KEY DEFAULT nextval('seq_sync_id'),
    timestamp_prefix BLOB,
    sync_id_type UTINYINT,
    data_bytes BLOB,
    UNIQUE (timestamp_prefix, sync_id_type, data_bytes)
);

CREATE TABLE IF NOT EXISTS source_sync_ids (
    source VARCHAR(255),
    sync_id UBIGINT,
    FOREIGN KEY(sync_id) REFERENCES sync_ids(id)
);

COMMIT;
                ",
        )?;


        Ok(CachedRepository {
            persistent_cache_db,
            db_conn,
        })
    }

    pub async fn cache_db(&self) -> &sled::Db {
        &self.persistent_cache_db
    }

    pub fn conn(&mut self) -> &mut duckdb::Connection {
        &mut self.db_conn
    }

    pub fn stop(self) -> eyre::Result<()> {
        self.persistent_cache_db.flush()?;
        self.db_conn.close().or_else(|e| {
            error!("Error closing duckdb connection: {:?}", e);
            Err(eyre::eyre!("Error closing duckdb connection"))
        })?;
        Ok(())
    }

    pub fn exists_in_cache_tree<K: AsRef<[u8]>>(tree: &Tree, key: K) -> eyre::Result<bool> {
        let contains = tree.contains_key(key)?;
        Ok(contains)
    }

    pub async fn open_cache_tree(&self, tree_name: &str) -> eyre::Result<Tree> {
        let tree = self.persistent_cache_db.open_tree(tree_name)?;
        Ok(tree)
    }
}
