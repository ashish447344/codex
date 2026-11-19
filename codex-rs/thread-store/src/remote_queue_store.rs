//! MySQL/MariaDB-backed queue persistence for remote app-server runtimes.

use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_state::QueuedUserSubmissionRecord;
use sqlx::MySql;
use sqlx::MySqlPool;
use sqlx::QueryBuilder;
use sqlx::Row;
use sqlx::mysql::MySqlPoolOptions;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::MAX_QUEUE_ITEMS;
use crate::QueueStore;
use crate::ThreadStoreError;
use crate::ThreadStoreFuture;

/// Resolved MySQL settings shared by remote runtime services.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteQueueStoreConfig {
    pub mysql_url: String,
    pub mysql_database: String,
}

/// MySQL/MariaDB persistence for ordered, thread-scoped queued submissions.
#[derive(Clone)]
pub struct RemoteQueueStore {
    config: RemoteQueueStoreConfig,
    pool: Arc<OnceCell<MySqlPool>>,
}

impl RemoteQueueStore {
    pub fn new(config: RemoteQueueStoreConfig) -> Self {
        Self {
            config,
            pool: Arc::new(OnceCell::new()),
        }
    }

    async fn pool(&self) -> Result<&MySqlPool, ThreadStoreError> {
        self.pool
            .get_or_try_init(|| async {
                let pool = MySqlPoolOptions::new()
                    .max_connections(10)
                    .connect(&self.config.mysql_url)
                    .await
                    .map_err(internal)?;
                let database = quoted_database(&self.config.mysql_database)?;
                sqlx::query(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
                    .execute(&pool)
                    .await
                    .map_err(internal)?;
                for statement in queue_schema(&database) {
                    sqlx::query(statement)
                        .execute(&pool)
                        .await
                        .map_err(internal)?;
                }
                Ok(pool)
            })
            .await
    }

    fn table(&self, table: &str) -> Result<String, ThreadStoreError> {
        Ok(format!(
            "{}.{}",
            quoted_database(&self.config.mysql_database)?,
            table
        ))
    }

    async fn lock_thread(
        &self,
        transaction: &mut sqlx::Transaction<'_, MySql>,
        thread_id: ThreadId,
    ) -> Result<(), ThreadStoreError> {
        // This per-thread row serializes writers even before the thread has queue items.
        // The lock is held until this transaction commits or rolls back.
        sqlx::query(&format!(
            "INSERT INTO {} (thread_id) VALUES (?)
             ON DUPLICATE KEY UPDATE thread_id = VALUES(thread_id)",
            self.table("queued_queue_locks")?
        ))
        .bind(thread_id.to_string())
        .execute(transaction.as_mut())
        .await
        .map_err(internal)?;
        Ok(())
    }

    async fn record_change(
        &self,
        transaction: &mut sqlx::Transaction<'_, MySql>,
        thread_id: ThreadId,
    ) -> Result<(), ThreadStoreError> {
        let revision = sqlx::query(&format!(
            "INSERT INTO {} (thread_id) VALUES (?)",
            self.table("queued_queue_revisions")?
        ))
        .bind(thread_id.to_string())
        .execute(transaction.as_mut())
        .await
        .map_err(internal)?
        .last_insert_id();
        sqlx::query(&format!(
            "INSERT INTO {} (thread_id, revision) VALUES (?, ?)
             ON DUPLICATE KEY UPDATE revision = VALUES(revision)",
            self.table("queued_thread_revisions")?
        ))
        .bind(thread_id.to_string())
        .bind(i64::try_from(revision).map_err(internal)?)
        .execute(transaction.as_mut())
        .await
        .map_err(internal)?;
        Ok(())
    }
}

impl QueueStore for RemoteQueueStore {
    fn change_version(&self) -> ThreadStoreFuture<'_, i64> {
        Box::pin(async move {
            sqlx::query_scalar(&format!(
                "SELECT COALESCE(MAX(revision), 0) FROM {}",
                self.table("queued_queue_revisions")?
            ))
            .fetch_one(self.pool().await?)
            .await
            .map_err(internal)
        })
    }

    fn changes_since<'a>(
        &'a self,
        revision: i64,
        thread_ids: &'a [ThreadId],
    ) -> ThreadStoreFuture<'a, Vec<(ThreadId, i64)>> {
        Box::pin(async move {
            if thread_ids.is_empty() {
                return Ok(Vec::new());
            }
            let mut query = QueryBuilder::<MySql>::new(format!(
                "SELECT thread_id, revision FROM {} WHERE revision > ",
                self.table("queued_thread_revisions")?
            ));
            query.push_bind(revision).push(" AND thread_id IN (");
            let mut separated = query.separated(", ");
            for thread_id in thread_ids {
                separated.push_bind(thread_id.to_string());
            }
            separated.push_unseparated(") ORDER BY revision");
            let rows = query
                .build_query_as::<(String, i64)>()
                .fetch_all(self.pool().await?)
                .await
                .map_err(internal)?;
            rows.into_iter()
                .map(|(thread_id, revision)| {
                    ThreadId::try_from(thread_id)
                        .map(|thread_id| (thread_id, revision))
                        .map_err(internal)
                })
                .collect()
        })
    }

    fn enqueue(
        &self,
        thread_id: ThreadId,
        payload: String,
    ) -> ThreadStoreFuture<'_, QueuedUserSubmissionRecord> {
        Box::pin(async move {
            let pool = self.pool().await?;
            let mut transaction = pool.begin().await.map_err(internal)?;
            self.lock_thread(&mut transaction, thread_id).await?;
            let count: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {} WHERE thread_id = ?",
                self.table("queued_items")?
            ))
            .bind(thread_id.to_string())
            .fetch_one(transaction.as_mut())
            .await
            .map_err(internal)?;
            if count >= i64::try_from(MAX_QUEUE_ITEMS).map_err(internal)? {
                transaction.rollback().await.map_err(internal)?;
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!(
                        "queue cannot contain more than {MAX_QUEUE_ITEMS} submissions"
                    ),
                });
            }
            let order: i64 = sqlx::query_scalar(&format!(
                "SELECT COALESCE(MAX(queue_order), -1) + 1 FROM {} WHERE thread_id = ?",
                self.table("queued_items")?
            ))
            .bind(thread_id.to_string())
            .fetch_one(transaction.as_mut())
            .await
            .map_err(internal)?;
            let item = QueuedUserSubmissionRecord {
                id: Uuid::now_v7().to_string(),
                thread_id,
                payload,
            };
            let now_ms = chrono::Utc::now().timestamp_millis();
            sqlx::query(&format!(
                "INSERT INTO {}
                 (id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms)
                 VALUES (?, ?, ?, ?, ?, ?)",
                self.table("queued_items")?
            ))
            .bind(&item.id)
            .bind(thread_id.to_string())
            .bind(&item.payload)
            .bind(order)
            .bind(now_ms)
            .bind(now_ms)
            .execute(transaction.as_mut())
            .await
            .map_err(internal)?;
            self.record_change(&mut transaction, thread_id).await?;
            transaction.commit().await.map_err(internal)?;
            Ok(item)
        })
    }

    fn list_page(
        &self,
        thread_id: ThreadId,
        offset: usize,
        limit: usize,
    ) -> ThreadStoreFuture<'_, Vec<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            let rows = sqlx::query(&format!(
                "SELECT id, thread_id, payload_json FROM {}
                 WHERE thread_id = ? ORDER BY queue_order LIMIT ? OFFSET ?",
                self.table("queued_items")?
            ))
            .bind(thread_id.to_string())
            .bind(i64::try_from(limit).map_err(internal)?)
            .bind(i64::try_from(offset).map_err(internal)?)
            .fetch_all(self.pool().await?)
            .await
            .map_err(internal)?;
            rows.iter().map(queue_item_from_row).collect()
        })
    }

    fn update(
        &self,
        thread_id: ThreadId,
        item_id: String,
        payload: String,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            let pool = self.pool().await?;
            let mut transaction = pool.begin().await.map_err(internal)?;
            self.lock_thread(&mut transaction, thread_id).await?;
            let exists: Option<String> = sqlx::query_scalar(&format!(
                "SELECT id FROM {} WHERE thread_id = ? AND id = ?",
                self.table("queued_items")?
            ))
            .bind(thread_id.to_string())
            .bind(&item_id)
            .fetch_optional(transaction.as_mut())
            .await
            .map_err(internal)?;
            if exists.is_none() {
                transaction.commit().await.map_err(internal)?;
                return Ok(None);
            }
            sqlx::query(&format!(
                "UPDATE {} SET payload_json = ?, updated_at_ms = ?
                 WHERE thread_id = ? AND id = ?",
                self.table("queued_items")?
            ))
            .bind(&payload)
            .bind(chrono::Utc::now().timestamp_millis())
            .bind(thread_id.to_string())
            .bind(&item_id)
            .execute(transaction.as_mut())
            .await
            .map_err(internal)?;
            self.record_change(&mut transaction, thread_id).await?;
            transaction.commit().await.map_err(internal)?;
            Ok(Some(QueuedUserSubmissionRecord {
                id: item_id,
                thread_id,
                payload,
            }))
        })
    }

    fn delete(&self, thread_id: ThreadId, item_id: String) -> ThreadStoreFuture<'_, bool> {
        Box::pin(async move {
            let pool = self.pool().await?;
            let mut transaction = pool.begin().await.map_err(internal)?;
            self.lock_thread(&mut transaction, thread_id).await?;
            let deleted = sqlx::query(&format!(
                "DELETE FROM {} WHERE thread_id = ? AND id = ?",
                self.table("queued_items")?
            ))
            .bind(thread_id.to_string())
            .bind(item_id)
            .execute(transaction.as_mut())
            .await
            .map_err(internal)?
            .rows_affected()
                > 0;
            if deleted {
                self.record_change(&mut transaction, thread_id).await?;
            }
            transaction.commit().await.map_err(internal)?;
            Ok(deleted)
        })
    }

    fn reorder(&self, thread_id: ThreadId, item_ids: Vec<String>) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let pool = self.pool().await?;
            let mut transaction = pool.begin().await.map_err(internal)?;
            self.lock_thread(&mut transaction, thread_id).await?;
            let existing: Vec<(String, i64)> = sqlx::query_as(&format!(
                "SELECT id, queue_order FROM {}
                 WHERE thread_id = ? ORDER BY queue_order",
                self.table("queued_items")?
            ))
            .bind(thread_id.to_string())
            .fetch_all(transaction.as_mut())
            .await
            .map_err(internal)?;
            let mut expected_ids = existing
                .iter()
                .map(|(item_id, _)| item_id.clone())
                .collect::<Vec<_>>();
            let mut requested_ids = item_ids.clone();
            expected_ids.sort();
            requested_ids.sort();
            if expected_ids != requested_ids {
                transaction.rollback().await.map_err(internal)?;
                return Err(ThreadStoreError::InvalidRequest {
                    message: "queue reorder must include every queued submission exactly once"
                        .to_string(),
                });
            }
            let maximum_order = existing.last().map_or(-1, |(_, order)| *order);
            let now_ms = chrono::Utc::now().timestamp_millis();
            for (index, item_id) in item_ids.iter().enumerate() {
                sqlx::query(&format!(
                    "UPDATE {} SET queue_order = ?, updated_at_ms = ?
                     WHERE thread_id = ? AND id = ?",
                    self.table("queued_items")?
                ))
                .bind(maximum_order + i64::try_from(index).map_err(internal)? + 1)
                .bind(now_ms)
                .bind(thread_id.to_string())
                .bind(item_id)
                .execute(transaction.as_mut())
                .await
                .map_err(internal)?;
            }
            if !existing.is_empty() {
                self.record_change(&mut transaction, thread_id).await?;
            }
            transaction.commit().await.map_err(internal)?;
            Ok(())
        })
    }
}

fn quoted_database(database: &str) -> Result<String, ThreadStoreError> {
    if database.is_empty()
        || !database
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: "MySQL database must contain only letters, digits, and underscores"
                .to_string(),
        });
    }
    Ok(format!("`{database}`"))
}

fn queue_schema(database: &str) -> [String; 4] {
    [
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.queued_items (
                id VARCHAR(64) NOT NULL,
                thread_id VARCHAR(64) NOT NULL,
                payload_json LONGTEXT NOT NULL,
                queue_order BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (thread_id, id),
                UNIQUE KEY queued_items_thread_order_idx (thread_id, queue_order)
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.queued_queue_locks (
                thread_id VARCHAR(64) NOT NULL PRIMARY KEY
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.queued_queue_revisions (
                revision BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                thread_id VARCHAR(64) NOT NULL,
                KEY queued_queue_revisions_thread_idx (thread_id)
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.queued_thread_revisions (
                thread_id VARCHAR(64) NOT NULL PRIMARY KEY,
                revision BIGINT NOT NULL
            )"
        ),
    ]
}

fn queue_item_from_row(
    row: &sqlx::mysql::MySqlRow,
) -> Result<QueuedUserSubmissionRecord, ThreadStoreError> {
    Ok(QueuedUserSubmissionRecord {
        id: row.try_get("id").map_err(internal)?,
        thread_id: ThreadId::try_from(row.try_get::<String, _>("thread_id").map_err(internal)?)
            .map_err(internal)?,
        payload: row.try_get("payload_json").map_err(internal)?,
    })
}

fn internal(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("queue storage failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_schema_is_idempotent_and_database_qualified() {
        let schema = queue_schema("`remote`");

        assert_eq!(4, schema.len());
        assert!(
            schema
                .iter()
                .all(|statement| { statement.starts_with("CREATE TABLE IF NOT EXISTS `remote`.") })
        );
        assert!(
            schema
                .iter()
                .any(|statement| statement.contains("queued_thread_revisions"))
        );
    }

    #[test]
    fn database_identifiers_are_quoted_and_validated() {
        assert_eq!("`remote_queue`", quoted_database("remote_queue").unwrap());
        assert!(quoted_database("remote-queue").is_err());
    }
}
