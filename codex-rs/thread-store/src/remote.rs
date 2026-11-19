//! Shared MongoDB/MySQL/Redis thread persistence.
//!
//! MongoDB is the canonical append-only rollout log. MySQL/MariaDB stores the
//! queryable thread record and the next rollout ordinal. Redis grants a
//! renewable, owner-token-checked writer lease so separate app-server
//! processes cannot append to the same thread concurrently.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::SessionContextWindow;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_rollout::RolloutItem;
use codex_rollout::into_persisted_rollout_items;
use futures::TryStreamExt;
use mongodb::Client;
use mongodb::IndexModel;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::options::IndexOptions;
use serde::Deserialize;
use serde::Serialize;
use sqlx::MySqlPool;
use sqlx::Row;
use sqlx::mysql::MySqlPoolOptions;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
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
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreFuture;
use crate::ThreadStoreResult;
use crate::UpdateThreadMetadataParams;

const HISTORY_COLLECTION: &str = "thread_rollout_items";
const LEASE_RELEASE_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
"#;
const LEASE_RENEW_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
"#;

/// Remote-store connection settings. URL values are resolved by the caller
/// from the app-server environment, rather than being persisted in config.
#[derive(Clone, Debug, PartialEq, Eq)]
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

impl RemoteThreadStoreConfig {
    /// Resolves the configured environment-variable references. Missing or
    /// blank values are rejected before the first persistence operation.
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
    live_leases: Arc<Mutex<HashMap<ThreadId, String>>>,
}

struct RemoteClients {
    mongo: mongodb::Database,
    mysql: MySqlPool,
    redis: redis::Client,
    table: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct HistoryDocument {
    item_json: String,
}

impl RemoteThreadStore {
    pub fn new(config: RemoteThreadStoreConfig) -> Self {
        Self {
            config,
            clients: Arc::new(OnceCell::new()),
            live_leases: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn clients(&self) -> ThreadStoreResult<&RemoteClients> {
        self.clients
            .get_or_try_init(|| async {
                let mongo_client = Client::with_uri_str(&self.config.mongo_url)
                    .await
                    .map_err(internal)?;
                let mongo = mongo_client.database(&self.config.mongo_database);
                mongo
                    .run_command(doc! { "ping": 1 })
                    .await
                    .map_err(internal)?;

                let mysql = MySqlPoolOptions::new()
                    .max_connections(10)
                    .connect(&self.config.mysql_url)
                    .await
                    .map_err(internal)?;
                let table = format!("`{}`.`remote_threads`", self.config.mysql_database);
                sqlx::query(&format!(
                    "CREATE DATABASE IF NOT EXISTS `{}`",
                    self.config.mysql_database
                ))
                .execute(&mysql)
                .await
                .map_err(internal)?;
                sqlx::query(&format!(
                    "CREATE TABLE IF NOT EXISTS {table} (
                        thread_id VARCHAR(64) PRIMARY KEY,
                        thread_json LONGTEXT NOT NULL,
                        archived_at DATETIME(3) NULL,
                        next_ordinal BIGINT NOT NULL DEFAULT 0
                    )"
                ))
                .execute(&mysql)
                .await
                .map_err(internal)?;

                let history = mongo.collection::<Document>(HISTORY_COLLECTION);
                history
                    .create_index(
                        IndexModel::builder()
                            .keys(doc! { "thread_id": 1, "ordinal": 1 })
                            .options(IndexOptions::builder().unique(true).build())
                            .build(),
                    )
                    .await
                    .map_err(internal)?;

                let redis =
                    redis::Client::open(self.config.redis_url.as_str()).map_err(internal)?;
                let mut connection = redis
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(internal)?;
                let _: String = redis::cmd("PING")
                    .query_async(&mut connection)
                    .await
                    .map_err(internal)?;

                Ok(RemoteClients {
                    mongo,
                    mysql,
                    redis,
                    table,
                })
            })
            .await
    }

    fn lease_key(&self, thread_id: ThreadId) -> String {
        format!("{}:thread-writer:{thread_id}", self.config.redis_namespace)
    }

    async fn acquire_lease(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let token = Uuid::now_v7().to_string();
        {
            let mut leases = self.live_leases.lock().await;
            if leases.contains_key(&thread_id) {
                return Err(ThreadStoreError::Conflict {
                    message: format!("thread {thread_id} is already open in this process"),
                });
            }
            leases.insert(thread_id, token.clone());
        }

        let result = async {
            let clients = self.clients().await?;
            let key = self.lease_key(thread_id);
            let deadline = tokio::time::Instant::now() + self.config.redis_acquire_timeout;
            loop {
                let mut connection = clients
                    .redis
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(internal)?;
                let acquired: Option<String> = redis::cmd("SET")
                    .arg(&key)
                    .arg(&token)
                    .arg("NX")
                    .arg("PX")
                    .arg(self.config.redis_lease_ttl.as_millis() as u64)
                    .query_async(&mut connection)
                    .await
                    .map_err(internal)?;
                if acquired.is_some() {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(ThreadStoreError::Conflict {
                        message: format!(
                            "timed out waiting for remote writer lease for {thread_id}"
                        ),
                    });
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        .await;

        if result.is_err() {
            self.live_leases.lock().await.remove(&thread_id);
        } else {
            self.spawn_lease_renewer(thread_id, token);
        }
        result
    }

    fn spawn_lease_renewer(&self, thread_id: ThreadId, token: String) {
        let store = self.clone();
        let interval = (store.config.redis_lease_ttl / 3).max(Duration::from_millis(100));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let still_owner = store
                    .live_leases
                    .lock()
                    .await
                    .get(&thread_id)
                    .is_some_and(|current| current == &token);
                if !still_owner {
                    return;
                }
                if store.renew_lease(thread_id).await.is_err() {
                    store.live_leases.lock().await.remove(&thread_id);
                    return;
                }
            }
        });
    }

    async fn renew_lease(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let token = self
            .live_leases
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .ok_or_else(|| ThreadStoreError::Conflict {
                message: format!("thread {thread_id} has no remote writer lease"),
            })?;
        let clients = self.clients().await?;
        let mut connection = clients
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(internal)?;
        let renewed: i64 = redis::Script::new(LEASE_RENEW_SCRIPT)
            .key(self.lease_key(thread_id))
            .arg(token)
            .arg(self.config.redis_lease_ttl.as_millis() as u64)
            .invoke_async(&mut connection)
            .await
            .map_err(internal)?;
        if renewed == 1 {
            Ok(())
        } else {
            Err(ThreadStoreError::Conflict {
                message: format!("remote writer lease for {thread_id} was lost"),
            })
        }
    }

    async fn release_lease(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let Some(token) = self.live_leases.lock().await.remove(&thread_id) else {
            return Ok(());
        };
        let clients = self.clients().await?;
        let mut connection = clients
            .redis
            .get_multiplexed_async_connection()
            .await
            .map_err(internal)?;
        let _: i64 = redis::Script::new(LEASE_RELEASE_SCRIPT)
            .key(self.lease_key(thread_id))
            .arg(token)
            .invoke_async(&mut connection)
            .await
            .map_err(internal)?;
        Ok(())
    }

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
        let next_ordinal: i64 = sqlx::query_scalar(&format!(
            "SELECT next_ordinal FROM {} WHERE thread_id = ?",
            clients.table
        ))
        .bind(thread_id.to_string())
        .fetch_optional(&clients.mysql)
        .await
        .map_err(internal)?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;

        let history = clients.mongo.collection::<Document>(HISTORY_COLLECTION);
        for (offset, item) in items.iter().enumerate() {
            let item_json = serde_json::to_string(item).map_err(internal)?;
            history
                .insert_one(doc! {
                    "thread_id": thread_id.to_string(),
                    "ordinal": next_ordinal + offset as i64,
                    "item_json": item_json,
                })
                .await
                .map_err(internal)?;
        }
        sqlx::query(&format!(
            "UPDATE {} SET next_ordinal = ? WHERE thread_id = ?",
            clients.table
        ))
        .bind(next_ordinal + items.len() as i64)
        .bind(thread_id.to_string())
        .execute(&clients.mysql)
        .await
        .map_err(internal)?;
        Ok(())
    }

    async fn history(&self, thread_id: ThreadId) -> ThreadStoreResult<Vec<RolloutItem>> {
        let clients = self.clients().await?;
        let collection = clients
            .mongo
            .collection::<HistoryDocument>(HISTORY_COLLECTION);
        let mut cursor = collection
            .find(doc! { "thread_id": thread_id.to_string() })
            .sort(doc! { "ordinal": 1 })
            .await
            .map_err(internal)?;
        let mut items = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(internal)? {
            items.push(serde_json::from_str(&document.item_json).map_err(internal)?);
        }
        Ok(items)
    }

    async fn thread(&self, thread_id: ThreadId) -> ThreadStoreResult<StoredThread> {
        let clients = self.clients().await?;
        let encoded: Option<String> = sqlx::query_scalar(&format!(
            "SELECT thread_json FROM {} WHERE thread_id = ?",
            clients.table
        ))
        .bind(thread_id.to_string())
        .fetch_optional(&clients.mysql)
        .await
        .map_err(internal)?;
        encoded
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })
            .and_then(|value| serde_json::from_str(&value).map_err(internal))
    }

    async fn save_thread(&self, thread: &StoredThread) -> ThreadStoreResult<()> {
        let clients = self.clients().await?;
        let archived_at = thread.archived_at.map(|value| value.naive_utc());
        sqlx::query(&format!(
            "UPDATE {} SET thread_json = ?, archived_at = ? WHERE thread_id = ?",
            clients.table
        ))
        .bind(serde_json::to_string(thread).map_err(internal)?)
        .bind(archived_at)
        .bind(thread.thread_id.to_string())
        .execute(&clients.mysql)
        .await
        .map_err(internal)?;
        Ok(())
    }
}

impl ThreadStore for RemoteThreadStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let thread_id = params.thread_id;
            self.acquire_lease(thread_id).await?;
            let result = async {
                let clients = self.clients().await?;
                let stored = stored_thread_from_create(&params);
                sqlx::query(&format!(
                    "INSERT INTO {} (thread_id, thread_json, archived_at, next_ordinal)
                     VALUES (?, ?, NULL, 0)",
                    clients.table
                ))
                .bind(thread_id.to_string())
                .bind(serde_json::to_string(&stored).map_err(internal)?)
                .execute(&clients.mysql)
                .await
                .map_err(internal)?;
                self.write_history(thread_id, vec![session_meta_item(&params)])
                    .await
            }
            .await;
            if result.is_err() {
                let _ = self.release_lease(thread_id).await;
            }
            result
        })
    }

    fn resume_thread(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreFuture<'_, Arc<Vec<RolloutItem>>> {
        Box::pin(async move {
            self.acquire_lease(params.thread_id).await?;
            let result = self.history(params.thread_id).await.map(Arc::new);
            if result.is_err() {
                let _ = self.release_lease(params.thread_id).await;
            }
            result
        })
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let thread = self.thread(params.thread_id).await?;
            let items = into_persisted_rollout_items(params.items, thread.history_mode);
            self.write_history(params.thread_id, items).await
        })
    }

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
            let thread = self.thread(params.thread_id).await?;
            if !params.include_archived && thread.archived_at.is_some() {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }
            Ok(StoredThreadHistory {
                thread_id: params.thread_id,
                items: self.history(params.thread_id).await?,
                revision: None,
            })
        })
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(async move {
            Ok(StoredModelContext {
                thread_id: params.thread_id,
                items: self.history(params.thread_id).await?,
                revision: None,
            })
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            let mut thread = self.thread(params.thread_id).await?;
            if !params.include_archived && thread.archived_at.is_some() {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }
            if params.include_history {
                thread.history = Some(StoredThreadHistory {
                    thread_id: params.thread_id,
                    items: self.history(params.thread_id).await?,
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
        Box::pin(async move {
            let clients = self.clients().await?;
            let rows = sqlx::query(&format!(
                "SELECT thread_json FROM {} WHERE archived_at IS {} NULL",
                clients.table,
                if params.archived { "NOT" } else { "" }
            ))
            .fetch_all(&clients.mysql)
            .await
            .map_err(internal)?;
            let mut items = rows
                .into_iter()
                .map(|row| row.try_get::<String, _>("thread_json").map_err(internal))
                .map(|encoded| {
                    encoded.and_then(|value| serde_json::from_str(&value).map_err(internal))
                })
                .collect::<ThreadStoreResult<Vec<StoredThread>>>()?;
            if !params.allowed_sources.is_empty() {
                items.retain(|thread| params.allowed_sources.contains(&thread.source));
            }
            if let Some(providers) = &params.model_providers {
                items.retain(|thread| {
                    providers.is_empty() || providers.contains(&thread.model_provider)
                });
            }
            if let Some(search) = &params.search_term {
                let search = search.to_ascii_lowercase();
                items.retain(|thread| {
                    thread.preview.to_ascii_lowercase().contains(&search)
                        || thread
                            .name
                            .as_deref()
                            .is_some_and(|name| name.to_ascii_lowercase().contains(&search))
                });
            }
            items.sort_by_key(|thread| thread.updated_at);
            if params.sort_direction == crate::SortDirection::Desc {
                items.reverse();
            }
            items.truncate(params.page_size);
            Ok(ThreadPage {
                items,
                next_cursor: None,
            })
        })
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        Box::pin(async move {
            let mut thread = self.thread(params.thread_id).await?;
            if !params.include_archived && thread.archived_at.is_some() {
                return Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                });
            }
            apply_metadata_patch(&mut thread, params.patch);
            self.save_thread(&thread).await?;
            Ok(Some(thread))
        })
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.acquire_lease(params.thread_id).await?;
            let result = async {
                let mut thread = self.thread(params.thread_id).await?;
                thread.archived_at = Some(Utc::now());
                self.save_thread(&thread).await
            }
            .await;
            let _ = self.release_lease(params.thread_id).await;
            result
        })
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            self.acquire_lease(params.thread_id).await?;
            let result = async {
                let mut thread = self.thread(params.thread_id).await?;
                thread.archived_at = None;
                self.save_thread(&thread).await?;
                Ok(thread)
            }
            .await;
            let _ = self.release_lease(params.thread_id).await;
            result
        })
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.acquire_lease(params.thread_id).await?;
            let result = async {
                let clients = self.clients().await?;
                let deleted = sqlx::query(&format!(
                    "DELETE FROM {} WHERE thread_id = ?",
                    clients.table
                ))
                .bind(params.thread_id.to_string())
                .execute(&clients.mysql)
                .await
                .map_err(internal)?;
                if deleted.rows_affected() == 0 {
                    return Err(ThreadStoreError::ThreadNotFound {
                        thread_id: params.thread_id,
                    });
                }
                clients
                    .mongo
                    .collection::<Document>(HISTORY_COLLECTION)
                    .delete_many(doc! { "thread_id": params.thread_id.to_string() })
                    .await
                    .map_err(internal)?;
                Ok(())
            }
            .await;
            let _ = self.release_lease(params.thread_id).await;
            result
        })
    }
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
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
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
