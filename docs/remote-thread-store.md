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

The SDK passes the URLs to the app-server as `CODEX_REMOTE_MONGO_URL`,
`CODEX_REMOTE_MYSQL_URL`, and `CODEX_REMOTE_REDIS_URL`, and keeps them out of
the config's `repr()`. The app-server checks at startup that all three are set
and that the database and namespace names are valid.

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
| `queued_items` | Queued user submissions, ordered per thread. |
| `queued_queue_locks` | One row per thread, locked to serialize queue writers. |
| `queued_queue_revisions` | Append-only queue change log. |
| `queued_thread_revisions` | Latest queue revision per thread. |

`remote_threads.thread_id` is the primary key. Summary updates run in a
row-locked transaction, and archive, unarchive and delete also hold the thread
writer lease. `next_ordinal` is the commit point for history: an append
writes the Mongo documents first, then advances `next_ordinal` with a
compare-and-set. Readers only return documents below `next_ordinal`, and the
next append replaces documents left behind by an append that failed half-way.
The MySQL database name must use only letters, digits, and underscores, which
prevents dynamic-DDL identifier injection.

### Redis

| Key pattern | Purpose | Lifetime |
| --- | --- | --- |
| `<namespace>:thread-writer:<thread-id>` | Exclusive writer lease for a live thread. | Configured `lease_ttl_ms`, renewed on persistence boundaries. |

Leases are acquired with `SET key token NX PX ttl`. Release and renewal use
owner-token-checked Lua scripts so one process cannot delete or extend a lease
that a different process has acquired. A background task renews each lease
while its thread stays loaded, including idle periods. The owning app-server
releases its lease on thread shutdown, discard, and deletion; archive and
unarchive of a thread that is not loaded take a short-lived lease. A second
process that resumes a loaded thread waits up to `acquire_timeout_ms` and then
fails with a conflict.

## Local-storage guarantee and current capability boundary

Selecting `remote` prevents the app-server from initializing the local SQLite
runtime and does not create rollout JSONL or thread writer lock files. The
remote store has no path-addressed rollout API, so callers must use `thread_id`
rather than a local rollout path.

The durable submission queue uses the MySQL tables above. SQLite-backed
services that have not yet been ported to the remote relational backend (for
example goals, memory jobs, structured logs, sections, projects, attachments,
and paginated-history projections) are disabled in remote mode, and threads
use legacy (non-paginated) history. This is intentional: unsupported services must fail
explicitly rather than creating local state and violating the no-local-storage
guarantee. Their eventual migration belongs in separate versioned MySQL
tables and MongoDB collections, with a documented migration version per
service.

## Operations and recovery

- Back up MySQL and MongoDB together. Mongo history is canonical; MySQL holds
  discovery metadata and the append ordinal.
- Use TLS URLs and service identities with access limited to the configured
  MongoDB database, MySQL database, and Redis namespace.
- If a process dies without shutting down (for example `kill -9`), its Redis
  lease expires after `lease_ttl_ms`; until then, resuming that thread
  elsewhere fails with a conflict. A later resume obtains a new lease and
  replays MongoDB documents in ordinal order. A graceful shutdown releases
  leases immediately.
- Keep `lease_ttl_ms` above the longest expected persistence operation. Every
  persist/flush/append operation renews ownership before writing.

## Local testing

On macOS, Homebrew runs all three services on localhost:

```sh
brew tap mongodb/brew
brew install mongodb-community mysql redis
brew services start mongodb-community
brew services start mysql
brew services start redis

mysql -uroot -e "
CREATE DATABASE IF NOT EXISTS codex;
CREATE USER IF NOT EXISTS 'codex'@'localhost' IDENTIFIED BY 'codex';
CREATE USER IF NOT EXISTS 'codex'@'%' IDENTIFIED BY 'codex';
GRANT ALL ON codex.* TO 'codex'@'localhost';
GRANT ALL ON codex.* TO 'codex'@'%';"

export CODEX_REMOTE_MONGO_URL="mongodb://127.0.0.1:27017"
export CODEX_REMOTE_MYSQL_URL="mysql://codex:codex@127.0.0.1:3306/codex"
export CODEX_REMOTE_REDIS_URL="redis://127.0.0.1:6379/0"
```

Inspect what the app-server stored:

```sh
mysql -ucodex -pcodex -h127.0.0.1 codex -e "SELECT thread_id, next_ordinal, archived_at FROM remote_threads"
mongosh codex --quiet --eval "db.thread_rollout_items.countDocuments()"
redis-cli --scan --pattern "codex:*"
```

The thread-store integration tests run when these variables point at
disposable services. They create their own databases, so the MySQL user needs
`CREATE` on new databases (for example `root`):

```sh
CODEX_REMOTE_TEST_MONGO_URL="mongodb://127.0.0.1:27017" \
CODEX_REMOTE_TEST_MYSQL_URL="mysql://root:PASSWORD@127.0.0.1:3306" \
CODEX_REMOTE_TEST_REDIS_URL="redis://127.0.0.1:6379/0" \
cargo test -p codex-thread-store remote
```
