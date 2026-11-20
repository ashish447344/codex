use std::time::Duration;

use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadMemoryMode;
use pretty_assertions::assert_eq;

use super::*;
use crate::ThreadPersistenceMetadata;

// Integration tests run only when these point at disposable services, e.g. the
// local setup in docs/remote-thread-store.md.
const TEST_MONGO_URL_ENV: &str = "CODEX_REMOTE_TEST_MONGO_URL";
const TEST_MYSQL_URL_ENV: &str = "CODEX_REMOTE_TEST_MYSQL_URL";
const TEST_REDIS_URL_ENV: &str = "CODEX_REMOTE_TEST_REDIS_URL";

#[test]
fn config_resolves_urls_from_named_environment_variables() {
    // PATH is always set, so it stands in for a connection variable.
    let path = std::env::var("PATH").expect("PATH is set");
    let config = RemoteThreadStoreConfig::from_environment(
        "PATH",
        "codex".to_string(),
        "PATH",
        "codex_db".to_string(),
        "PATH",
        "codex:test".to_string(),
        /*acquire_timeout_ms*/ 10,
        /*lease_ttl_ms*/ 20,
    )
    .expect("valid config");

    assert_eq!(config.mongo_url, path);
    assert_eq!(config.mysql_database, "codex_db");
    assert_eq!(config.redis_acquire_timeout, Duration::from_millis(10));
    assert_eq!(config.redis_lease_ttl, Duration::from_millis(20));
    assert_eq!(
        config.queue_store_config(),
        RemoteQueueStoreConfig {
            mysql_url: path,
            mysql_database: "codex_db".to_string(),
        }
    );
}

#[test]
fn config_rejects_missing_variables_bad_names_and_zero_timeouts() {
    let resolve = |mongo_env: &str, mysql_database: &str, namespace: &str, ttl_ms: u64| {
        RemoteThreadStoreConfig::from_environment(
            mongo_env,
            "codex".to_string(),
            "PATH",
            mysql_database.to_string(),
            "PATH",
            namespace.to_string(),
            /*acquire_timeout_ms*/ 10,
            ttl_ms,
        )
    };

    let missing = resolve(
        "CODEX_REMOTE_TEST_VARIABLE_THAT_IS_NOT_SET",
        "codex",
        "ns",
        10,
    )
    .expect_err("missing variable");
    assert!(
        missing
            .to_string()
            .contains("CODEX_REMOTE_TEST_VARIABLE_THAT_IS_NOT_SET"),
        "{missing}"
    );
    assert!(resolve("PATH", "codex; DROP TABLE x", "ns", 10).is_err());
    assert!(resolve("PATH", "codex", "has space", 10).is_err());
    assert!(resolve("PATH", "codex", "ns", 0).is_err());
}

#[test]
fn config_debug_output_redacts_connection_urls() {
    let config = RemoteThreadStoreConfig {
        mongo_url: "mongodb://user:secret@mongo".to_string(),
        mongo_database: "codex".to_string(),
        mysql_url: "mysql://user:secret@mysql".to_string(),
        mysql_database: "codex".to_string(),
        redis_url: "redis://:secret@redis".to_string(),
        redis_namespace: "codex".to_string(),
        redis_acquire_timeout: Duration::from_secs(1),
        redis_lease_ttl: Duration::from_secs(1),
    };

    let debug = format!("{config:?}");
    assert!(!debug.contains("secret"), "{debug}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn threads_round_trip_and_writer_lease_moves_between_processes() {
    let Some(config) = test_config() else {
        return;
    };
    // Two stores model two app-server processes sharing the same services.
    let first = RemoteThreadStore::new(config.clone());
    let second = RemoteThreadStore::new(config);
    let thread_id = ThreadId::new();

    first
        .create_thread(create_params(thread_id))
        .await
        .expect("create thread");
    first
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message("user", "hello"), message("assistant", "hi there")],
        })
        .await
        .expect("append items");
    first
        .update_thread_metadata(UpdateThreadMetadataParams {
            thread_id,
            patch: ThreadMetadataPatch {
                name: Some(Some("remote thread".to_string())),
                preview: Some("hello".to_string()),
                ..Default::default()
            },
            include_archived: false,
        })
        .await
        .expect("update metadata");

    let read = second
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: true,
        })
        .await
        .expect("read thread from another process");
    assert_eq!(read.name.as_deref(), Some("remote thread"));
    assert_eq!(read.model_provider, "test-provider");
    let history = read.history.expect("history").items;
    assert_eq!(history.len(), 3);
    assert!(has_canonical_session_header(&history, thread_id));
    assert_eq!(message_texts(&history), vec!["hello", "hi there"]);

    let listed = second
        .list_threads(list_params(/*archived*/ false))
        .await
        .expect("list threads");
    assert!(
        listed
            .items
            .iter()
            .any(|thread| thread.thread_id == thread_id)
    );

    let contended = second
        .resume_thread(resume_params(thread_id))
        .await
        .expect_err("lease is held by the first process");
    assert!(
        matches!(contended, ThreadStoreError::Conflict { .. }),
        "{contended}"
    );

    first
        .shutdown_thread(thread_id)
        .await
        .expect("release lease");
    let resumed = second
        .resume_thread(resume_params(thread_id))
        .await
        .expect("resume after release");
    assert_eq!(resumed.len(), 3);
    second
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message("user", "second process")],
        })
        .await
        .expect("append from new owner");
    let stale_write = first
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message("user", "stale")],
        })
        .await
        .expect_err("previous owner cannot append");
    assert!(
        matches!(stale_write, ThreadStoreError::Conflict { .. }),
        "{stale_write}"
    );

    let history = first
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load history")
        .items;
    assert_eq!(
        message_texts(&history),
        vec!["hello", "hi there", "second process"]
    );

    second
        .archive_thread(ArchiveThreadParams { thread_id })
        .await
        .expect("archive while open");
    let archived_read = first
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await
        .expect_err("archived threads are hidden");
    assert!(matches!(
        archived_read,
        ThreadStoreError::ThreadNotFound { .. }
    ));
    let archived = first
        .list_threads(list_params(/*archived*/ true))
        .await
        .expect("list archived");
    assert!(
        archived
            .items
            .iter()
            .any(|thread| thread.thread_id == thread_id && thread.archived_at.is_some())
    );
    let unarchived = second
        .unarchive_thread(ArchiveThreadParams { thread_id })
        .await
        .expect("unarchive");
    assert_eq!(unarchived.archived_at, None);

    second
        .delete_thread(DeleteThreadParams { thread_id })
        .await
        .expect("delete");
    let deleted = first
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: true,
            include_history: false,
        })
        .await
        .expect_err("deleted");
    assert!(matches!(deleted, ThreadStoreError::ThreadNotFound { .. }));
    let clients = first.clients().await.expect("clients");
    let remaining = clients
        .mongo
        .collection::<Document>(HISTORY_COLLECTION)
        .count_documents(doc! { "thread_id": thread_id.to_string() })
        .await
        .expect("count history");
    assert_eq!(remaining, 0);
    let mut redis = clients.redis.clone();
    let lease: Option<String> = redis::cmd("GET")
        .arg(first.lease_key(thread_id))
        .query_async(&mut redis)
        .await
        .expect("read lease");
    assert_eq!(lease, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncommitted_history_is_hidden_and_replaced_by_the_next_append() {
    let Some(config) = test_config() else {
        return;
    };
    let store = RemoteThreadStore::new(config);
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id))
        .await
        .expect("create thread");

    // Simulate an append that wrote MongoDB but died before committing the ordinal.
    let clients = store.clients().await.expect("clients");
    clients
        .mongo
        .collection::<Document>(HISTORY_COLLECTION)
        .insert_one(doc! {
            "thread_id": thread_id.to_string(),
            "ordinal": 1_i64,
            "item_json": serde_json::to_string(&message("user", "orphan")).expect("json"),
        })
        .await
        .expect("insert orphan");

    let history = store
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load history")
        .items;
    assert_eq!(history.len(), 1);

    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![message("user", "committed")],
        })
        .await
        .expect("append replaces orphan");
    let history = store
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load history")
        .items;
    assert_eq!(message_texts(&history), vec!["committed"]);

    store
        .delete_thread(DeleteThreadParams { thread_id })
        .await
        .expect("delete");
}

fn test_config() -> Option<RemoteThreadStoreConfig> {
    let url = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let (Some(mongo_url), Some(mysql_url), Some(redis_url)) = (
        url(TEST_MONGO_URL_ENV),
        url(TEST_MYSQL_URL_ENV),
        url(TEST_REDIS_URL_ENV),
    ) else {
        eprintln!(
            "skipping remote thread-store integration test; set {TEST_MONGO_URL_ENV}, \
             {TEST_MYSQL_URL_ENV}, and {TEST_REDIS_URL_ENV}"
        );
        return None;
    };
    // A fresh database and namespace per test keeps runs independent.
    let suffix = Uuid::now_v7().simple().to_string();
    Some(RemoteThreadStoreConfig {
        mongo_url,
        mongo_database: format!("codex_test_{suffix}"),
        mysql_url,
        mysql_database: format!("codex_test_{suffix}"),
        redis_url,
        redis_namespace: format!("codex-test:{suffix}"),
        redis_acquire_timeout: Duration::from_millis(300),
        redis_lease_ttl: Duration::from_secs(5),
    })
}

fn create_params(thread_id: ThreadId) -> CreateThreadParams {
    CreateThreadParams {
        attached_thread: false,
        creator_user_id: None,
        creator_account_id: None,
        session_id: thread_id.into(),
        thread_id,
        extra_config: None,
        forked_from_id: None,
        parent_thread_id: None,
        source: SessionSource::Exec,
        thread_source: None,
        originator: "remote_test".to_string(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode: ThreadHistoryMode::Legacy,
        history_base: None,
        subagent_history_start_ordinal: None,
        initial_window_id: Uuid::now_v7().to_string(),
        runtime_workspace_roots: None,
        metadata: persistence_metadata(),
    }
}

fn resume_params(thread_id: ThreadId) -> ResumeThreadParams {
    ResumeThreadParams {
        thread_id,
        rollout_path: None,
        history: None,
        history_revision: None,
        include_archived: true,
        metadata: persistence_metadata(),
    }
}

fn persistence_metadata() -> ThreadPersistenceMetadata {
    ThreadPersistenceMetadata {
        cwd: None,
        model_provider: "test-provider".to_string(),
        memory_mode: ThreadMemoryMode::Enabled,
    }
}

fn list_params(archived: bool) -> ListThreadsParams {
    ListThreadsParams {
        page_size: 100,
        cursor: None,
        sort_key: ThreadSortKey::CreatedAt,
        sort_direction: crate::SortDirection::Desc,
        allowed_sources: Vec::new(),
        model_providers: None,
        cwd_filters: None,
        section: None,
        project_id: None,
        archived,
        search_term: None,
        relation_filter: None,
        use_state_db_only: false,
    }
}

fn message(role: &str, text: &str) -> RolloutItem {
    let content = if role == "assistant" {
        ContentItem::OutputText {
            annotations: None,
            logprobs: None,
            text: text.to_string(),
        }
    } else {
        ContentItem::InputText {
            text: text.to_string(),
        }
    };
    RolloutItem::ResponseItem(
        ResponseItem::Message {
            status: None,
            encrypted_content: None,
            id: None,
            role: role.to_string(),
            content: vec![content],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    )
}

fn message_texts(items: &[RolloutItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| {
            let RolloutItem::ResponseItem(response) = item else {
                return None;
            };
            let ResponseItem::Message { content, .. } = &response.item else {
                return None;
            };
            match content.first()? {
                ContentItem::InputText { text } | ContentItem::OutputText { text, .. } => {
                    Some(text.clone())
                }
                _ => None,
            }
        })
        .collect()
}
