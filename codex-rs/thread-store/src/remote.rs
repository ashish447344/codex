//! Shared MongoDB/MySQL/Redis thread persistence.
//!
//! MongoDB is the canonical append-only rollout log. MySQL/MariaDB stores the
//! queryable thread record and the next rollout ordinal, which is also the
//! commit point for history appends. Redis grants a renewable,
//! owner-token-checked writer lease so separate app-server processes cannot
//! append to the same thread concurrently.

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::GitInfo;
use codex_protocol::protocol::SessionContextWindow;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::into_persisted_rollout_items;
use futures::TryStreamExt;
use mongodb::Client;
use mongodb::IndexModel;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::options::IndexOptions;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde::Serialize;
use sqlx::AssertSqlSafe;
use sqlx::MySqlPool;
use sqlx::Row;
use sqlx::mysql::MySqlPoolOptions;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tracing::warn;
use uuid::Uuid;

use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::CreateThreadParams;
use crate::DeleteThreadParams;
use crate::ListThreadsParams;
use crate::LoadThreadHistoryParams;
use crate::PersistContext;
use crate::ReadThreadByRolloutPathParams;
use crate::ReadThreadParams;
use crate::RemoteQueueStoreConfig;
use crate::ResumeThreadParams;
use crate::StoredModelContext;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::ThreadMetadataPatch;
use crate::ThreadPage;
use crate::ThreadRelationFilter;
use crate::ThreadSortKey;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreFuture;
use crate::ThreadStoreResult;
use crate::UpdateThreadMetadataParams;

const HISTORY_COLLECTION: &str = "thread_rollout_items";
const THREADS_TABLE: &str = "remote_threads";
const MYSQL_MAX_CONNECTIONS: u32 = 10;
const LEASE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

static LEASE_RELEASE_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
"#,
    )
});
static LEASE_RENEW_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
"#,
    )
});

/// Remote-store connection settings. URL values are resolved by the caller
/// from the app-server environment, rather than being persisted in config.
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteThreadStoreConfig {
    pub mongo_url: String,
    pub mongo_database: String,
    pub mysql_url: String,
    pub mysql_database: String,
    pub redis_url: String,
    pub redis_namespace: String,
    pub redis_acquire_timeout: Duration,
    pub redis_lease_ttl: Duration,
}

// Connection URLs usually embed credentials, so they are never printed.
impl std::fmt::Debug for RemoteThreadStoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteThreadStoreConfig")
            .field("mongo_url", &"<redacted>")
            .field("mongo_database", &self.mongo_database)
            .field("mysql_url", &"<redacted>")
            .field("mysql_database", &self.mysql_database)
            .field("redis_url", &"<redacted>")
            .field("redis_namespace", &self.redis_namespace)
            .field("redis_acquire_timeout", &self.redis_acquire_timeout)
            .field("redis_lease_ttl", &self.redis_lease_ttl)
            .finish()
    }
}

impl RemoteThreadStoreConfig {
    /// Resolves the configured environment-variable references. Missing or
    /// blank values are rejected before the first persistence operation.
    #[allow(clippy::too_many_arguments)]
    pub fn from_environment(
        mongo_url_env: &str,
        mongo_database: String,
        mysql_url_env: &str,
        mysql_database: String,
        redis_url_env: &str,
        redis_namespace: String,
        acquire_timeout_ms: u64,
        lease_ttl_ms: u64,
    ) -> ThreadStoreResult<Self> {
        if acquire_timeout_ms == 0 || lease_ttl_ms == 0 {
            return Err(ThreadStoreError::InvalidRequest {
                message: "remote Redis lease timeouts must be greater than zero".to_string(),
            });
        }
        Ok(Self {
            mongo_url: required_environment(mongo_url_env)?,
            mongo_database: valid_identifier("MongoDB database", mongo_database)?,
            mysql_url: required_environment(mysql_url_env)?,
            mysql_database: valid_identifier("MySQL database", mysql_database)?,
            redis_url: required_environment(redis_url_env)?,
            redis_namespace: valid_redis_namespace(redis_namespace)?,
            redis_acquire_timeout: Duration::from_millis(acquire_timeout_ms),
            redis_lease_ttl: Duration::from_millis(lease_ttl_ms),
        })
    }

    /// Returns the resolved MySQL settings for remote runtime services that
    /// share the thread store's queue database.
    pub fn queue_store_config(&self) -> RemoteQueueStoreConfig {
        RemoteQueueStoreConfig {
            mysql_url: self.mysql_url.clone(),
            mysql_database: self.mysql_database.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RemoteThreadStore {
    config: RemoteThreadStoreConfig,
    clients: Arc<OnceCell<RemoteClients>>,
    live_leases: Arc<Mutex<HashMap<ThreadId, LiveLease>>>,
}

/// A writer lease held by this process.
#[derive(Clone)]
struct LiveLease {
    token: String,
    history_mode: ThreadHistoryMode,
}

struct RemoteClients {
    mongo: mongodb::Database,
    mysql: MySqlPool,
    redis: ConnectionManager,
    threads_table: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct HistoryDocument {
    item_json: String,
}

struct ThreadRow {
    thread: StoredThread,
    next_ordinal: i64,
}

impl RemoteThreadStore {
    pub fn new(config: RemoteThreadStoreConfig) -> Self {
        Self {
            config,
            clients: Arc::new(OnceCell::new()),
            live_leases: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Connects to every backing service and provisions the schema once.
    /// A failed attempt is retried by the next operation.
    async fn clients(&self) -> ThreadStoreResult<&RemoteClients> {
        self.clients
            .get_or_try_init(|| async {
                let mongo_client = Client::with_uri_str(&self.config.mongo_url)
                    .await
                    .map_err(|error| internal_context("connect to MongoDB", error))?;
                let mongo = mongo_client.database(&self.config.mongo_database);
                mongo
                    .run_command(doc! { "ping": 1 })
                    .await
                    .map_err(|error| internal_context("ping MongoDB", error))?;
                mongo
                    .collection::<Document>(HISTORY_COLLECTION)
                    .create_index(
                        IndexModel::builder()
                            .keys(doc! { "thread_id": 1, "ordinal": 1 })
                            .options(IndexOptions::builder().unique(true).build())
                            .build(),
                    )
                    .await
                    .map_err(|error| internal_context("create MongoDB history index", error))?;

                let mysql = connect_mysql_pool(&self.config.mysql_url)
                    .await
                    .map_err(|error| internal_context("connect to MySQL", error))?;
                let database = quoted_identifier(&self.config.mysql_database)?;
                sqlx::query(AssertSqlSafe(format!(
                    "CREATE DATABASE IF NOT EXISTS {database}"
                )))
                .execute(&mysql)
                .await
                .map_err(|error| internal_context("create MySQL database", error))?;
                let threads_table = format!("{database}.`{THREADS_TABLE}`");
                sqlx::query(AssertSqlSafe(format!(
                    "CREATE TABLE IF NOT EXISTS {threads_table} (
                        thread_id VARCHAR(64) NOT NULL PRIMARY KEY,
                        thread_json LONGTEXT NOT NULL,
                        archived_at DATETIME(3) NULL,
                        next_ordinal BIGINT NOT NULL DEFAULT 0
                    )"
                )))
                .execute(&mysql)
                .await
                .map_err(|error| internal_context("create MySQL thread table", error))?;

                let redis_client = redis::Client::open(self.config.redis_url.as_str())
                    .map_err(|error| internal_context("parse Redis URL", error))?;
                let mut redis = redis_client
                    .get_connection_manager()
                    .await
                    .map_err(|error| internal_context("connect to Redis", error))?;
                let _: String = redis::cmd("PING")
                    .query_async(&mut redis)
                    .await
                    .map_err(|error| internal_context("ping Redis", error))?;

                Ok(RemoteClients {
                    mongo,
                    mysql,
                    redis,
                    threads_table,
                })
            })
            .await
    }

    fn lease_key(&self, thread_id: ThreadId) -> String {
        format!("{}:thread-writer:{thread_id}", self.config.redis_namespace)
    }

    fn lease_ttl_ms(&self) -> u64 {
        u64::try_from(self.config.redis_lease_ttl.as_millis()).unwrap_or(u64::MAX)
    }

    /// Acquires the cross-process writer lease, waiting up to the configured
    /// timeout while another process holds it.
    async fn acquire_lease(
        &self,
        thread_id: ThreadId,
        history_mode: ThreadHistoryMode,
    ) -> ThreadStoreResult<()> {
        let token = Uuid::now_v7().to_string();
        {
            let mut leases = self.live_leases.lock().await;
            if leases.contains_key(&thread_id) {
                return Err(ThreadStoreError::Conflict {
                    message: format!("thread {thread_id} is already open in this process"),
                });
            }
            leases.insert(
                thread_id,
                LiveLease {
                    token: token.clone(),
                    history_mode,
                },
            );
        }

        let result = async {
            let clients = self.clients().await?;
            let mut redis = clients.redis.clone();
            let key = self.lease_key(thread_id);
            let deadline = tokio::time::Instant::now() + self.config.redis_acquire_timeout;
            loop {
                let acquired: Option<String> = redis::cmd("SET")
                    .arg(&key)
                    .arg(&token)
                    .arg("NX")
                    .arg("PX")
                    .arg(self.lease_ttl_ms())
                    .query_async(&mut redis)
                    .await
                    .map_err(|error| internal_context("acquire Redis writer lease", error))?;
                if acquired.is_some() {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(ThreadStoreError::Conflict {
                        message: format!(
                            "timed out waiting for remote writer lease for {thread_id}; \
                             another app-server process has this thread open"
                        ),
                    });
                }
                tokio::time::sleep(LEASE_RETRY_INTERVAL).await;
            }
        }
        .await;

        if result.is_err() {
            let mut leases = self.live_leases.lock().await;
            if leases
                .get(&thread_id)
                .is_some_and(|lease| lease.token == token)
            {
                leases.remove(&thread_id);
            }
        } else {
            self.spawn_lease_renewer(thread_id, token);
        }
        result
    }

    /// Keeps a held lease alive while the thread stays open, including idle periods
    /// with no persistence calls.
    fn spawn_lease_renewer(&self, thread_id: ThreadId, token: String) {
        let store = self.clone();
        let interval = (store.config.redis_lease_ttl / 3).max(LEASE_RETRY_INTERVAL);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let still_owner = store
                    .live_leases
                    .lock()
                    .await
                    .get(&thread_id)
                    .is_some_and(|lease| lease.token == token);
                if !still_owner {
                    return;
                }
                if let Err(error) = store.renew_lease(thread_id).await {
                    warn!("stopped renewing remote writer lease for {thread_id}: {error}");
                    return;
                }
            }
        });
    }

    /// Extends the lease and proves this process still owns it. A lost lease is
    /// dropped locally so later writes fail instead of racing a new owner.
    async fn renew_lease(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let token = self
            .live_leases
            .lock()
            .await
            .get(&thread_id)
            .map(|lease| lease.token.clone())
            .ok_or_else(|| ThreadStoreError::Conflict {
                message: format!("thread {thread_id} has no remote writer lease in this process"),
            })?;
        let clients = self.clients().await?;
        let mut redis = clients.redis.clone();
        let renewed: i64 = LEASE_RENEW_SCRIPT
            .key(self.lease_key(thread_id))
            .arg(&token)
            .arg(self.lease_ttl_ms())
            .invoke_async(&mut redis)
            .await
            .map_err(|error| internal_context("renew Redis writer lease", error))?;
        if renewed == 1 {
            return Ok(());
        }
        let mut leases = self.live_leases.lock().await;
        if leases
            .get(&thread_id)
            .is_some_and(|lease| lease.token == token)
        {
            leases.remove(&thread_id);
        }
        Err(ThreadStoreError::Conflict {
            message: format!("remote writer lease for {thread_id} was lost"),
        })
    }

    async fn release_lease(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let Some(lease) = self.live_leases.lock().await.remove(&thread_id) else {
            return Ok(());
        };
        let clients = self.clients().await?;
        let mut redis = clients.redis.clone();
        let _: i64 = LEASE_RELEASE_SCRIPT
            .key(self.lease_key(thread_id))
            .arg(lease.token)
            .invoke_async(&mut redis)
            .await
            .map_err(|error| internal_context("release Redis writer lease", error))?;
        Ok(())
    }

    /// Ensures this process may mutate `thread_id`. Returns true when a
    /// temporary lease was acquired and must be released by the caller.
    async fn ensure_writer(
        &self,
        thread_id: ThreadId,
        history_mode: ThreadHistoryMode,
    ) -> ThreadStoreResult<bool> {
        let held = self.live_leases.lock().await.contains_key(&thread_id);
        if held {
            self.renew_lease(thread_id).await?;
            Ok(false)
        } else {
            self.acquire_lease(thread_id, history_mode).await?;
            Ok(true)
        }
    }

    async fn release_temporary_writer(&self, thread_id: ThreadId, acquired: bool) {
        if acquired && let Err(error) = self.release_lease(thread_id).await {
            warn!("failed to release remote writer lease for {thread_id}: {error}");
        }
    }

    async fn history_mode(&self, thread_id: ThreadId) -> ThreadStoreResult<ThreadHistoryMode> {
        if let Some(lease) = self.live_leases.lock().await.get(&thread_id) {
            return Ok(lease.history_mode);
        }
        Ok(self.thread_row(thread_id).await?.thread.history_mode)
    }

    /// Appends items under the writer lease. MongoDB documents at or past
    /// `next_ordinal` are uncommitted leftovers from a failed append, so they
    /// are replaced; the compare-and-set on `next_ordinal` commits the batch.
    async fn write_history(
        &self,
        thread_id: ThreadId,
        items: Vec<RolloutItem>,
    ) -> ThreadStoreResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.renew_lease(thread_id).await?;
        let clients = self.clients().await?;
        let next_ordinal = self.thread_row(thread_id).await?.next_ordinal;
        let thread_key = thread_id.to_string();

        let history = clients.mongo.collection::<Document>(HISTORY_COLLECTION);
        history
            .delete_many(doc! {
                "thread_id": &thread_key,
                "ordinal": { "$gte": next_ordinal },
            })
            .await
            .map_err(|error| internal_context("remove uncommitted MongoDB history", error))?;
        let documents = items
            .iter()
            .enumerate()
            .map(|(offset, item)| {
                Ok(doc! {
                    "thread_id": &thread_key,
                    "ordinal": next_ordinal + i64::try_from(offset).map_err(internal)?,
                    "item_json": serde_json::to_string(item).map_err(internal)?,
                })
            })
            .collect::<ThreadStoreResult<Vec<_>>>()?;
        let appended = i64::try_from(documents.len()).map_err(internal)?;
        history
            .insert_many(documents)
            .await
            .map_err(|error| internal_context("append MongoDB history", error))?;

        let committed = sqlx::query(AssertSqlSafe(format!(
            "UPDATE {} SET next_ordinal = ? WHERE thread_id = ? AND next_ordinal = ?",
            clients.threads_table
        )))
        .bind(next_ordinal + appended)
        .bind(&thread_key)
        .bind(next_ordinal)
        .execute(&clients.mysql)
        .await
        .map_err(|error| internal_context("commit history ordinal", error))?
        .rows_affected();
        if committed != 1 {
            return Err(ThreadStoreError::Conflict {
                message: format!("remote history for {thread_id} was advanced by another writer"),
            });
        }
        Ok(())
    }

    /// Reads committed history in append order.
    async fn history(&self, thread_id: ThreadId) -> ThreadStoreResult<Vec<RolloutItem>> {
        let next_ordinal = self.thread_row(thread_id).await?.next_ordinal;
        self.history_before(thread_id, next_ordinal).await
    }

    async fn history_before(
        &self,
        thread_id: ThreadId,
        next_ordinal: i64,
    ) -> ThreadStoreResult<Vec<RolloutItem>> {
        let clients = self.clients().await?;
        let mut cursor = clients
            .mongo
            .collection::<HistoryDocument>(HISTORY_COLLECTION)
            .find(doc! {
                "thread_id": thread_id.to_string(),
                "ordinal": { "$lt": next_ordinal },
            })
            .sort(doc! { "ordinal": 1 })
            .await
            .map_err(|error| internal_context("read MongoDB history", error))?;
        let mut items = Vec::new();
        while let Some(document) = cursor
            .try_next()
            .await
            .map_err(|error| internal_context("read MongoDB history", error))?
        {
            items.push(serde_json::from_str(&document.item_json).map_err(internal)?);
        }
        Ok(items)
    }

    async fn thread_row(&self, thread_id: ThreadId) -> ThreadStoreResult<ThreadRow> {
        let clients = self.clients().await?;
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT thread_json, next_ordinal FROM {} WHERE thread_id = ?",
            clients.threads_table
        )))
        .bind(thread_id.to_string())
        .fetch_optional(&clients.mysql)
        .await
        .map_err(|error| internal_context("read MySQL thread", error))?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        let encoded: String = row.try_get("thread_json").map_err(internal)?;
        Ok(ThreadRow {
            thread: serde_json::from_str(&encoded).map_err(internal)?,
            next_ordinal: row.try_get("next_ordinal").map_err(internal)?,
        })
    }

    /// Applies `change` to the thread summary inside a row-locked MySQL
    /// transaction so concurrent metadata writers cannot lose updates.
    async fn mutate_thread(
        &self,
        thread_id: ThreadId,
        include_archived: bool,
        change: impl FnOnce(&mut StoredThread),
    ) -> ThreadStoreResult<StoredThread> {
        let clients = self.clients().await?;
        let mut transaction = clients.mysql.begin().await.map_err(internal)?;
        let encoded: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT thread_json FROM {} WHERE thread_id = ? FOR UPDATE",
            clients.threads_table
        )))
        .bind(thread_id.to_string())
        .fetch_optional(transaction.as_mut())
        .await
        .map_err(|error| internal_context("lock MySQL thread", error))?;
        let Some(encoded) = encoded else {
            transaction.rollback().await.map_err(internal)?;
            return Err(ThreadStoreError::ThreadNotFound { thread_id });
        };
        let mut thread: StoredThread = serde_json::from_str(&encoded).map_err(internal)?;
        if !include_archived && thread.archived_at.is_some() {
            transaction.rollback().await.map_err(internal)?;
            return Err(ThreadStoreError::ThreadNotFound { thread_id });
        }
        change(&mut thread);
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE {} SET thread_json = ?, archived_at = ? WHERE thread_id = ?",
            clients.threads_table
        )))
        .bind(serde_json::to_string(&thread).map_err(internal)?)
        .bind(thread.archived_at.map(|value| value.naive_utc()))
        .bind(thread_id.to_string())
        .execute(transaction.as_mut())
        .await
        .map_err(|error| internal_context("update MySQL thread", error))?;
        transaction.commit().await.map_err(internal)?;
        Ok(thread)
    }

    async fn all_threads(&self, archived: bool) -> ThreadStoreResult<Vec<StoredThread>> {
        let clients = self.clients().await?;
        let rows = sqlx::query(AssertSqlSafe(format!(
            "SELECT thread_json FROM {} WHERE archived_at IS {}NULL",
            clients.threads_table,
            if archived { "NOT " } else { "" }
        )))
        .fetch_all(&clients.mysql)
        .await
        .map_err(|error| internal_context("list MySQL threads", error))?;
        rows.into_iter()
            .map(|row| {
                let encoded: String = row.try_get("thread_json").map_err(internal)?;
                serde_json::from_str(&encoded).map_err(internal)
            })
            .collect()
    }

    async fn create_thread_impl(&self, params: CreateThreadParams) -> ThreadStoreResult<()> {
        if params.history_mode == ThreadHistoryMode::Paginated {
            return Err(ThreadStoreError::InvalidRequest {
                message: "the remote thread store supports only legacy thread history".to_string(),
            });
        }
        let thread_id = params.thread_id;
        self.acquire_lease(thread_id, params.history_mode).await?;
        let result = async {
            let clients = self.clients().await?;
            let stored = stored_thread_from_create(&params);
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {} (thread_id, thread_json, archived_at, next_ordinal)
                 VALUES (?, ?, NULL, 0)",
                clients.threads_table
            )))
            .bind(thread_id.to_string())
            .bind(serde_json::to_string(&stored).map_err(internal)?)
            .execute(&clients.mysql)
            .await
            .map_err(|error| internal_context("insert MySQL thread", error))?;
            self.write_history(thread_id, vec![session_meta_item(&params)])
                .await
        }
        .await;
        if result.is_err() {
            self.release_temporary_writer(thread_id, /*acquired*/ true)
                .await;
        }
        result
    }

    async fn resume_thread_impl(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreResult<Arc<Vec<RolloutItem>>> {
        let thread_id = params.thread_id;
        if let Some(history) = params.history.as_deref()
            && !history.is_empty()
            && !has_canonical_session_header(history, thread_id)
        {
            return Err(ThreadStoreError::Unsupported {
                operation: "resume_thread with caller-supplied history",
            });
        }
        let thread = self.thread_row(thread_id).await?.thread;
        if !params.include_archived && thread.archived_at.is_some() {
            return Err(ThreadStoreError::ThreadNotFound { thread_id });
        }
        self.acquire_lease(thread_id, thread.history_mode).await?;
        // Read after acquiring the lease so writes committed by a previous owner are included.
        let result = self.history(thread_id).await.map(Arc::new);
        if result.is_err() {
            self.release_temporary_writer(thread_id, /*acquired*/ true)
                .await;
        }
        result
    }

    async fn list_threads_impl(&self, params: ListThreadsParams) -> ThreadStoreResult<ThreadPage> {
        let mut items = self.all_threads(params.archived).await?;
        if !params.allowed_sources.is_empty() {
            items.retain(|thread| params.allowed_sources.contains(&thread.source));
        }
        if let Some(providers) = &params.model_providers
            && !providers.is_empty()
        {
            items.retain(|thread| providers.contains(&thread.model_provider));
        }
        if let Some(cwds) = &params.cwd_filters {
            items.retain(|thread| cwds.contains(&thread.cwd));
        }
        if let Some(section) = &params.section {
            items.retain(|thread| {
                thread.section.as_ref().map(|section| section.id.as_str()) == section.as_deref()
            });
        }
        if let Some(project_id) = &params.project_id {
            items.retain(|thread| thread.project_id.as_deref() == project_id.as_deref());
        }
        if let Some(search) = &params.search_term {
            let search = search.to_lowercase();
            items.retain(|thread| {
                thread.preview.to_lowercase().contains(&search)
                    || thread
                        .name
                        .as_deref()
                        .is_some_and(|name| name.to_lowercase().contains(&search))
            });
        }
        match params.relation_filter {
            Some(ThreadRelationFilter::DirectChildrenOf(parent_thread_id)) => {
                items.retain(|thread| thread.parent_thread_id == Some(parent_thread_id));
            }
            Some(ThreadRelationFilter::DescendantsOf(ancestor_thread_id)) => {
                let mut subtree = HashSet::from([ancestor_thread_id]);
                loop {
                    let mut discovered = false;
                    for thread in &items {
                        if thread
                            .parent_thread_id
                            .is_some_and(|parent_thread_id| subtree.contains(&parent_thread_id))
                        {
                            discovered |= subtree.insert(thread.thread_id);
                        }
                    }
                    if !discovered {
                        break;
                    }
                }
                items.retain(|thread| {
                    thread.thread_id != ancestor_thread_id && subtree.contains(&thread.thread_id)
                });
            }
            Some(ThreadRelationFilter::AttachedThreadsOf(_)) => {
                return Err(ThreadStoreError::Unsupported {
                    operation: "list attached threads",
                });
            }
            None => {}
        }

        match params.sort_key {
            ThreadSortKey::CreatedAt => {
                items.sort_by_key(|thread| (thread.created_at, thread.thread_id.to_string()));
            }
            ThreadSortKey::UpdatedAt => {
                items.sort_by_key(|thread| (thread.updated_at, thread.thread_id.to_string()));
            }
            ThreadSortKey::RecencyAt => {
                items.sort_by_key(|thread| (thread.recency_at, thread.thread_id.to_string()));
            }
            ThreadSortKey::SectionPosition => {
                items.sort_by_key(|thread| {
                    (
                        thread.section_position.unwrap_or(i64::MAX),
                        thread.thread_id.to_string(),
                    )
                });
            }
        }
        if params.sort_direction == crate::SortDirection::Desc {
            items.reverse();
        }

        // Cursors are opaque offsets into the filtered, sorted listing.
        let offset = match params.cursor.as_deref() {
            Some(cursor) => {
                cursor
                    .parse::<usize>()
                    .map_err(|_| ThreadStoreError::InvalidRequest {
                        message: format!("invalid remote thread list cursor: {cursor}"),
                    })?
            }
            None => 0,
        };
        let page_size = params.page_size.max(1);
        let total = items.len();
        let items = items
            .into_iter()
            .skip(offset)
            .take(page_size)
            .collect::<Vec<_>>();
        let consumed = offset.saturating_add(items.len());
        Ok(ThreadPage {
            items,
            next_cursor: (consumed < total).then(|| consumed.to_string()),
        })
    }

    async fn delete_thread_impl(&self, params: DeleteThreadParams) -> ThreadStoreResult<()> {
        let thread_id = params.thread_id;
        let history_mode = self.history_mode(thread_id).await?;
        let acquired = self.ensure_writer(thread_id, history_mode).await?;
        let result = async {
            let clients = self.clients().await?;
            let deleted = sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {} WHERE thread_id = ?",
                clients.threads_table
            )))
            .bind(thread_id.to_string())
            .execute(&clients.mysql)
            .await
            .map_err(|error| internal_context("delete MySQL thread", error))?;
            if deleted.rows_affected() == 0 {
                return Err(ThreadStoreError::ThreadNotFound { thread_id });
            }
            clients
                .mongo
                .collection::<Document>(HISTORY_COLLECTION)
                .delete_many(doc! { "thread_id": thread_id.to_string() })
                .await
                .map_err(|error| internal_context("delete MongoDB history", error))?;
            Ok(())
        }
        .await;
        // Release a temporary lease, and never keep a live lease for a deleted thread.
        self.release_temporary_writer(thread_id, acquired || result.is_ok())
            .await;
        result
    }

    async fn set_archived(
        &self,
        thread_id: ThreadId,
        archived: bool,
    ) -> ThreadStoreResult<StoredThread> {
        let history_mode = self.history_mode(thread_id).await?;
        let acquired = self.ensure_writer(thread_id, history_mode).await?;
        let result = self
            .mutate_thread(thread_id, /*include_archived*/ true, |thread| {
                thread.archived_at = archived.then(Utc::now);
            })
            .await;
        self.release_temporary_writer(thread_id, acquired).await;
        result
    }
}

impl ThreadStore for RemoteThreadStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.create_thread_impl(params))
    }

    fn resume_thread(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreFuture<'_, Arc<Vec<RolloutItem>>> {
        Box::pin(self.resume_thread_impl(params))
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let history_mode = self.history_mode(params.thread_id).await?;
            let items = into_persisted_rollout_items(params.items, history_mode);
            self.write_history(params.thread_id, items).await
        })
    }

    // Appends are durable when `append_items` returns, so persistence
    // checkpoints only need to prove that this process still owns the thread.
    fn persist_thread(
        &self,
        thread_id: ThreadId,
        _context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { self.renew_lease(thread_id).await })
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { self.renew_lease(thread_id).await })
    }

    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { self.release_lease(thread_id).await })
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { self.release_lease(thread_id).await })
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(async move {
            let row = self.thread_row(params.thread_id).await?;
            if !params.include_archived && row.thread.archived_at.is_some() {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }
            Ok(StoredThreadHistory {
                thread_id: params.thread_id,
                items: self
                    .history_before(params.thread_id, row.next_ordinal)
                    .await?,
                revision: None,
            })
        })
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(async move {
            let row = self.thread_row(params.thread_id).await?;
            if !params.include_archived && row.thread.archived_at.is_some() {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }
            Ok(StoredModelContext {
                thread_id: params.thread_id,
                items: self
                    .history_before(params.thread_id, row.next_ordinal)
                    .await?,
                revision: None,
            })
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            let ThreadRow {
                mut thread,
                next_ordinal,
            } = self.thread_row(params.thread_id).await?;
            if !params.include_archived && thread.archived_at.is_some() {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }
            if params.include_history {
                thread.history = Some(StoredThreadHistory {
                    thread_id: params.thread_id,
                    items: self.history_before(params.thread_id, next_ordinal).await?,
                    revision: None,
                });
            }
            Ok(thread)
        })
    }

    fn read_thread_by_rollout_path(
        &self,
        _params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "read_thread_by_rollout_path",
            })
        })
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(self.list_threads_impl(params))
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        Box::pin(async move {
            let thread = self
                .mutate_thread(params.thread_id, params.include_archived, |thread| {
                    apply_metadata_patch(thread, params.patch);
                })
                .await?;
            Ok(Some(thread))
        })
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.set_archived(params.thread_id, /*archived*/ true)
                .await
                .map(|_| ())
        })
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(self.set_archived(params.thread_id, /*archived*/ false))
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.delete_thread_impl(params))
    }
}

/// Opens a MySQL/MariaDB pool. The workspace's SQLite-pool lint does not apply
/// to this driver.
#[allow(clippy::disallowed_methods)]
pub(crate) async fn connect_mysql_pool(url: &str) -> Result<MySqlPool, sqlx::Error> {
    MySqlPoolOptions::new()
        .max_connections(MYSQL_MAX_CONNECTIONS)
        .connect(url)
        .await
}

/// Validates a MySQL identifier and returns it backtick-quoted for dynamic DDL.
pub(crate) fn quoted_identifier(value: &str) -> ThreadStoreResult<String> {
    if is_simple_identifier(value) {
        Ok(format!("`{value}`"))
    } else {
        Err(ThreadStoreError::InvalidRequest {
            message: "MySQL database must contain only letters, digits, and underscores"
                .to_string(),
        })
    }
}

fn is_simple_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn has_canonical_session_header(history: &[RolloutItem], thread_id: ThreadId) -> bool {
    matches!(
        history.first(),
        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == thread_id
    )
}

fn required_environment(name: &str) -> ThreadStoreResult<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("remote thread store requires non-empty environment variable {name}"),
        })
}

fn valid_identifier(label: &str, value: String) -> ThreadStoreResult<String> {
    if is_simple_identifier(&value) {
        Ok(value)
    } else {
        Err(ThreadStoreError::InvalidRequest {
            message: format!("{label} must contain only letters, digits, and underscores"),
        })
    }
}

fn valid_redis_namespace(value: String) -> ThreadStoreResult<String> {
    if value.trim().is_empty() || value.contains(char::is_whitespace) {
        Err(ThreadStoreError::InvalidRequest {
            message: "Redis namespace must be non-empty and contain no whitespace".to_string(),
        })
    } else {
        Ok(value)
    }
}

fn internal(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: error.to_string(),
    }
}

fn internal_context(action: &str, error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("remote thread store failed to {action}: {error}"),
    }
}

fn session_meta_item(params: &CreateThreadParams) -> RolloutItem {
    RolloutItem::SessionMeta(Box::new(SessionMetaLine {
        meta: SessionMeta {
            attached_thread: params.attached_thread,
            session_id: params.session_id,
            id: params.thread_id,
            forked_from_id: params.forked_from_id,
            parent_thread_id: params.parent_thread_id,
            cwd: params.metadata.cwd.clone().unwrap_or_default(),
            runtime_workspace_roots: params
                .runtime_workspace_roots
                .as_ref()
                .map(|roots| roots.iter().map(|root| root.to_path_buf()).collect()),
            agent_nickname: params.source.get_nickname(),
            agent_role: params.source.get_agent_role(),
            agent_path: params.source.get_agent_path().map(Into::into),
            originator: params.originator.clone(),
            creator_user_id: params.creator_user_id.clone(),
            creator_account_id: params.creator_account_id.clone(),
            source: params.source.clone(),
            thread_source: params.thread_source.clone(),
            model_provider: Some(params.metadata.model_provider.clone()),
            base_instructions: Some(params.base_instructions.clone()),
            dynamic_tools: (!params.dynamic_tools.is_empty()).then(|| params.dynamic_tools.clone()),
            selected_capability_roots: params.selected_capability_roots.clone(),
            memory_mode: matches!(
                params.metadata.memory_mode,
                codex_protocol::protocol::ThreadMemoryMode::Disabled
            )
            .then_some("disabled".to_string()),
            history_mode: params.history_mode,
            history_base: params.history_base,
            subagent_history_start_ordinal: params.subagent_history_start_ordinal,
            multi_agent_version: params.multi_agent_version,
            context_window: Some(SessionContextWindow::new(params.initial_window_id.clone())),
            ..SessionMeta::default()
        },
        git: None,
    }))
}

fn stored_thread_from_create(params: &CreateThreadParams) -> StoredThread {
    let now = Utc::now();
    StoredThread {
        creator_user_id: params.creator_user_id.clone(),
        creator_account_id: params.creator_account_id.clone(),
        originator: Some(params.originator.clone()),
        thread_id: params.thread_id,
        extra_config: params.extra_config.clone(),
        rollout_path: None,
        forked_from_id: params.forked_from_id,
        parent_thread_id: params.parent_thread_id,
        preview: String::new(),
        name: None,
        model_provider: params.metadata.model_provider.clone(),
        model: None,
        reasoning_effort: None,
        created_at: now,
        updated_at: now,
        recency_at: now,
        archived_at: None,
        section: None,
        section_position: None,
        section_entered_at: None,
        project_id: None,
        daybreak_enabled: None,
        cwd: params.metadata.cwd.clone().unwrap_or_default(),
        cli_version: env!("CARGO_PKG_VERSION").to_string(),
        source: params.source.clone(),
        history_mode: params.history_mode,
        thread_source: params.thread_source.clone(),
        agent_nickname: params.source.get_nickname(),
        agent_role: params.source.get_agent_role(),
        agent_path: params.source.get_agent_path().map(Into::into),
        git_info: None,
        approval_mode: AskForApproval::Never,
        permission_profile: PermissionProfile::read_only(),
        token_usage: None,
        first_user_message: None,
        history: None,
    }
}

fn apply_metadata_patch(thread: &mut StoredThread, patch: ThreadMetadataPatch) {
    if let Some(name) = patch.name {
        thread.name = name;
    }
    if let Some(preview) = patch.preview {
        thread.preview = preview;
    }
    if let Some(model_provider) = patch.model_provider {
        thread.model_provider = model_provider;
    }
    if let Some(model) = patch.model {
        thread.model = Some(model);
    }
    if let Some(reasoning_effort) = patch.reasoning_effort {
        thread.reasoning_effort = reasoning_effort;
    }
    if let Some(created_at) = patch.created_at {
        thread.created_at = created_at;
    }
    if let Some(updated_at) = patch.updated_at {
        thread.updated_at = updated_at;
    } else {
        thread.updated_at = Utc::now();
    }
    if let Some(recency_at) = patch.advance_recency_at {
        thread.recency_at = thread.recency_at.max(recency_at);
    }
    if let Some(source) = patch.source {
        thread.source = source;
    }
    if thread.creator_user_id.is_none() {
        thread.creator_user_id = patch.creator_user_id;
    }
    if thread.creator_account_id.is_none() {
        thread.creator_account_id = patch.creator_account_id;
    }
    if thread.originator.is_none() {
        thread.originator = patch.originator;
    }
    if let Some(thread_source) = patch.thread_source {
        thread.thread_source = thread_source;
    }
    if let Some(nickname) = patch.agent_nickname {
        thread.agent_nickname = nickname;
    }
    if let Some(role) = patch.agent_role {
        thread.agent_role = role;
    }
    if let Some(path) = patch.agent_path {
        thread.agent_path = path;
    }
    if let Some(cwd) = patch.cwd {
        thread.cwd = cwd;
    }
    if let Some(cli_version) = patch.cli_version {
        thread.cli_version = cli_version;
    }
    if let Some(approval_mode) = patch.approval_mode {
        thread.approval_mode = approval_mode;
    }
    if let Some(permission_profile) = patch.permission_profile {
        thread.permission_profile = permission_profile;
    }
    if let Some(git_info) = patch.git_info {
        let mut current = thread.git_info.take().unwrap_or(GitInfo {
            commit_hash: None,
            branch: None,
            repository_url: None,
        });
        if let Some(sha) = git_info.sha {
            current.commit_hash = sha.as_deref().map(codex_git_utils::GitSha::new);
        }
        if let Some(branch) = git_info.branch {
            current.branch = branch;
        }
        if let Some(origin_url) = git_info.origin_url {
            current.repository_url = origin_url;
        }
        thread.git_info = (current.commit_hash.is_some()
            || current.branch.is_some()
            || current.repository_url.is_some())
        .then_some(current);
    }
    if let Some(token_usage) = patch.token_usage {
        thread.token_usage = Some(token_usage);
    }
    if let Some(first_user_message) = patch.first_user_message {
        thread.first_user_message = Some(first_user_message);
    }
    if let Some(project_id) = patch.project_id {
        thread.project_id = project_id;
    }
    if let Some(daybreak_enabled) = patch.daybreak_enabled {
        thread.daybreak_enabled = Some(daybreak_enabled);
    }
}

#[cfg(test)]
#[path = "remote_tests.rs"]
mod tests;
