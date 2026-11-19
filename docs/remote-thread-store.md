# Remote Thread Store

Remote thread storage lets many Codex app-server processes share a thread
without writing rollout JSONL, SQLite state, or file locks below `CODEX_HOME`.
The Rust app-server connects directly to the data services. The Python SDK and
a FastAPI service only pass configuration and child-process environment
variables; they never proxy database operations or send credentials over
app-server JSON-RPC.

## Configuration

Secrets belong in the process environment, not `config.toml`:

```toml
[experimental_thread_store]
type = "remote"

[experimental_thread_store.mongo]
url_env = "CODEX_REMOTE_MONGO_URL"
database = "codex"

[experimental_thread_store.mysql]
url_env = "CODEX_REMOTE_MYSQL_URL"
database = "codex"

[experimental_thread_store.redis]
url_env = "CODEX_REMOTE_REDIS_URL"
namespace = "codex:production"
acquire_timeout_ms = 30000
lease_ttl_ms = 120000
```

The configured MySQL/MariaDB URL must connect to an existing server-level
database (or omit the database path) and grant permission to create and use
the named database. The first remote-store operation verifies MongoDB and Redis
connectivity and creates the listed indexes/tables idempotently. A missing
connection variable, an invalid database/namespace name, or an unavailable
service prevents remote thread operations from starting; Codex never falls
back to local persistence.

For FastAPI, use `RemoteThreadStoreConfig`:

```python
from openai_codex import Codex, CodexConfig, RemoteThreadStoreConfig

store = RemoteThreadStoreConfig(
    mongo_url=settings.mongo_url,
    mongo_database="codex",
    mysql_url=settings.mysql_server_url,
    mysql_database="codex",
    redis_url=settings.redis_url,
    redis_namespace="codex:production",
)
codex = Codex(CodexConfig(remote_thread_store=store))
```

Load `settings.*_url` from the service's secret manager. Do not populate them
from an HTTP request, log them, or store them in rollout items.

## Provisioned resources

The app-server owns the following names. It creates MongoDB indexes and the
MySQL table on first remote-store use. Redis keys are created only while their
leases are held.

### MongoDB

| Collection | Purpose | Required index |
| --- | --- | --- |
| `thread_rollout_items` | Canonical append-only history. Each document maps to exactly one former rollout JSONL line. | Unique `{ thread_id: 1, ordinal: 1 }` |

Each document contains `thread_id`, monotonically increasing `ordinal`, and
`item_json`, the serialized `RolloutItem`. The unique compound index makes an
append retry safe and preserves replay order independent of MongoDB object
IDs.

### MySQL or MariaDB

| Table | Purpose |
| --- | --- |
| `remote_threads` | One row per thread: complete queryable thread summary JSON, archive timestamp, and the next Mongo rollout ordinal. |

`remote_threads.thread_id` is the primary key. The app-server updates the
summary and archive timestamp with the same thread writer lease that protects
rollout appends. The MySQL database name must use only letters, digits, and
underscores, which prevents dynamic-DDL identifier injection.

### Redis

| Key pattern | Purpose | Lifetime |
| --- | --- | --- |
| `<namespace>:thread-writer:<thread-id>` | Exclusive writer lease for a live thread. | Configured `lease_ttl_ms`, renewed on persistence boundaries. |

Leases are acquired with `SET key token NX PX ttl`. Release and renewal use
owner-token-checked Lua scripts so one process cannot delete or extend a lease
that a different process has acquired. The owning app-server releases its
lease on shutdown, discard, archive, unarchive, and deletion.

## Local-storage guarantee and current capability boundary

Selecting `remote` prevents the app-server from initializing the local SQLite
runtime and does not create rollout JSONL or thread writer lock files. The
remote store has no path-addressed rollout API, so callers must use `thread_id`
rather than a local rollout path.

SQLite-backed services that have not yet been ported to the remote relational
backend (for example durable queue, goals, memory jobs, structured logs,
sections, projects, attachments, and paginated-history projections) are
disabled in remote mode. This is intentional: unsupported services must fail
explicitly rather than creating local state and violating the no-local-storage
guarantee. Their eventual migration belongs in separate versioned MySQL
tables and MongoDB collections, with a documented migration version per
service.

## Operations and recovery

- Back up MySQL and MongoDB together. Mongo history is canonical; MySQL holds
  discovery metadata and the append ordinal.
- Use TLS URLs and service identities with access limited to the configured
  MongoDB database, MySQL database, and Redis namespace.
- If a process dies, its Redis lease expires. A later resume obtains a new
  lease and replays MongoDB documents in ordinal order.
- Keep `lease_ttl_ms` above the longest expected persistence operation. Every
  persist/flush/append operation renews ownership before writing.
