use duckdb::params;
use sled::Tree;
use slog_scope::error;
use crate::proto::SyncIds;

pub const DEFAULT_CACHE_DB_DIR: &str = ".db";

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

    pub fn conn(&self) -> eyre::Result<duckdb::Connection> {
        self.db_conn.try_clone().map_err(|e| eyre::eyre!("Error cloning duckdb connection: {:?}", e))
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

    pub fn insert_sync_ids_with_source(self, source: String, sync_ids: SyncIds) -> eyre::Result<()> {
        let mut insert_sync_id = self.db_conn.prepare("INSERT INTO sync_ids (timestamp_prefix, sync_id_type, data_bytes) VALUES (?, ?, ?)")?;
        let mut insert_source_sync_id = self.db_conn.prepare("INSERT INTO source_sync_ids (source, sync_id) VALUES (?, ?)")?;

        for sync_id in sync_ids.sync_ids {
            let timestamp_prefix = sync_id[0..8].to_vec();
            let sync_id_type = sync_id[8];
            let data_bytes = sync_id[9..].to_vec();

            let result = insert_sync_id.execute(params![timestamp_prefix, sync_id_type, data_bytes])?;
            let sync_id_id = result + 1;

            insert_source_sync_id.execute(params![source, sync_id_id])?;
        }
        Ok(())
    }
    pub fn sync_id_set_difference(&self, a_source: String, b_source: String) -> eyre::Result<SyncIds> {
        let difference = "SELECT a.* FROM sync_ids a
            INNER JOIN (
                SELECT sync_id FROM source_sync_ids WHERE source = ?
                EXCEPT
                SELECT sync_id FROM source_sync_ids WHERE source = ?
            ) sub
            ON a.id = sub.sync_id
            ";

        let mut result = self.db_conn.prepare(&difference)?;
        let mut rows = result.query(params![a_source, b_source]).map_err(|e| eyre::eyre!("Error querying difference: {:?}", e))?;
        let mut sync_ids: SyncIds = SyncIds{
            sync_ids: Vec::new(),
        };
        while let Some(row) = rows.next()? {
            let timestamp_bytes: Vec<u8> = row.get(1)?;
            let sync_id_type: u8 = row.get(2)?;
            let data_bytes: Vec<u8> = row.get(3)?;

            let mut result = Vec::new();
            result.extend_from_slice(&timestamp_bytes);
            result.push(sync_id_type);
            result.extend_from_slice(&data_bytes);

            sync_ids.sync_ids.push(result);
        }
        Ok(sync_ids)
    }
}
