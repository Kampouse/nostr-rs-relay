//! Database schema and migrations for libSQL
// Reusing SQLite schema since libSQL is SQLite-compatible
use crate::error::Result;
use crate::repo::libsql::LibsqlConn;
use const_format::formatcp;
use tracing::{debug, error, info};

/// Startup DB Pragmas
pub const STARTUP_SQL: &str = r##"
PRAGMA main.synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA journal_size_limit = 32768;
PRAGMA temp_store = 2; -- use memory, not temp files
PRAGMA main.cache_size = 20000; -- 80MB max cache size per conn
pragma mmap_size = 0; -- disable mmap (default)
"##;

/// Latest database version
pub const DB_VERSION: usize = 18;

/// Schema definition
const INIT_SQL: &str = formatcp!(
    r##"
-- Database settings
PRAGMA encoding = "UTF-8";
PRAGMA journal_mode = WAL;
PRAGMA auto_vacuum = FULL;
PRAGMA main.synchronous=NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA application_id = 1654008667;
PRAGMA user_version = {};

-- Event Table
CREATE TABLE IF NOT EXISTS event (
id INTEGER PRIMARY KEY,
event_hash BLOB NOT NULL, -- 32-byte SHA256 hash
first_seen INTEGER NOT NULL, -- when the event was first seen (not authored!) (seconds since 1970)
created_at INTEGER NOT NULL, -- when the event was authored
expires_at INTEGER, -- when the event expires and may be deleted
author BLOB NOT NULL, -- author pubkey
delegated_by BLOB, -- delegator pubkey (NIP-26)
kind INTEGER NOT NULL, -- event kind
hidden INTEGER, -- relevant for queries
content TEXT NOT NULL -- serialized json of event object
);

-- Event Indexes
CREATE UNIQUE INDEX IF NOT EXISTS event_hash_index ON event(event_hash);
CREATE INDEX IF NOT EXISTS author_index ON event(author);
CREATE INDEX IF NOT EXISTS kind_index ON event(kind);
CREATE INDEX IF NOT EXISTS created_at_index ON event(created_at);
CREATE INDEX IF NOT EXISTS delegated_by_index ON event(delegated_by);
CREATE INDEX IF NOT EXISTS event_composite_index ON event(kind,created_at);
CREATE INDEX IF NOT EXISTS kind_author_index ON event(kind,author);
CREATE INDEX IF NOT EXISTS kind_created_at_index ON event(kind,created_at);
CREATE INDEX IF NOT EXISTS author_created_at_index ON event(author,created_at);
CREATE INDEX IF NOT EXISTS author_kind_index ON event(author,kind);
CREATE INDEX IF NOT EXISTS event_expiration ON event(expires_at);

-- Tag Table
-- Tag values are stored as either a BLOB (if they come in as a
-- hex-string), or TEXT otherwise.
-- This means that searches need to select the appropriate column.
-- We duplicate the kind/created_at to make indexes much more efficient.
CREATE TABLE IF NOT EXISTS tag (
id INTEGER PRIMARY KEY,
event_id INTEGER NOT NULL, -- an event ID that contains a tag.
name TEXT, -- the tag name ("p", "e", whatever)
value TEXT, -- the tag value, if not hex.
value_hex BLOB, -- the tag value, if it can be interpreted as a lowercase hex string.
created_at INTEGER NOT NULL, -- when the event was authored
kind INTEGER NOT NULL, -- event kind
FOREIGN KEY(event_id) REFERENCES event(id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS tag_val_index ON tag(value);
CREATE INDEX IF NOT EXISTS tag_composite_index ON tag(event_id,name,value);
CREATE INDEX IF NOT EXISTS tag_name_eid_index ON tag(name,event_id,value);
CREATE INDEX IF NOT EXISTS tag_covering_index ON tag(name,kind,value,created_at,event_id);

-- NIP-05 User Validation
CREATE TABLE IF NOT EXISTS user_verification (
id INTEGER PRIMARY KEY,
metadata_event INTEGER NOT NULL, -- the metadata event used for this validation.
name TEXT NOT NULL, -- the nip05 field value (user@domain).
verified_at INTEGER, -- timestamp this author/nip05 was most recently verified.
failed_at INTEGER, -- timestamp a verification attempt failed (host down).
failure_count INTEGER DEFAULT 0, -- number of consecutive failures.
FOREIGN KEY(metadata_event) REFERENCES event(id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS user_verification_name_index ON user_verification(name);
CREATE INDEX IF NOT EXISTS user_verification_event_index ON user_verification(metadata_event);

-- Create account table
CREATE TABLE IF NOT EXISTS account (
pubkey TEXT PRIMARY KEY,
is_admitted INTEGER NOT NULL DEFAULT 0,
balance INTEGER NOT NULL DEFAULT 0,
tos_accepted_at INTEGER
);

-- Create account index
CREATE INDEX IF NOT EXISTS user_pubkey_index ON account(pubkey);

-- Invoice table
CREATE TABLE IF NOT EXISTS invoice (
payment_hash TEXT PRIMARY KEY,
pubkey TEXT NOT NULL,
invoice TEXT NOT NULL,
amount INTEGER NOT NULL,
status TEXT CHECK ( status IN ('Paid', 'Unpaid', 'Expired' ) ) NOT NUll DEFAULT 'Unpaid',
description TEXT,
created_at INTEGER NOT NULL,
confirmed_at INTEGER,
CONSTRAINT invoice_pubkey_fkey FOREIGN KEY (pubkey) REFERENCES account (pubkey) ON DELETE CASCADE
);

-- Create invoice index
CREATE INDEX IF NOT EXISTS invoice_pubkey_index ON invoice(pubkey);


"##,
    DB_VERSION
);

/// Determine the current application database schema version.
pub async fn curr_db_version(conn: &mut LibsqlConn) -> Result<usize> {
    use crate::repo::libsql::Value;

    let query = "PRAGMA user_version;";
    let mut rows = conn.query(query, vec![]).await?;

    if let Some(row) = rows.next().await {
        let curr_version_value = row.get(0)?;

        match curr_version_value {
            Value::Integer(i) => Ok(i as usize),
            _ => Ok(0),
        }
    } else {
        Ok(0)
    }
}

/// Initialize database from scratch
async fn mig_init(conn: &mut LibsqlConn) -> usize {
    match conn.execute_batch(INIT_SQL).await {
        Ok(()) => {
            info!(
                "database pragma/schema initialized to v{}, and ready",
                DB_VERSION
            );
        }
        Err(err) => {
            error!("update (init) failed: {}", err);
            panic!("database could not be initialized");
        }
    }
    DB_VERSION
}

/// Upgrade DB to latest version, and execute pragma settings
pub async fn upgrade_db(conn: &mut LibsqlConn) -> Result<usize> {
    // check the version.
    let mut curr_version = curr_db_version(conn).await?;
    info!("DB version = {:?}", curr_version);

    match curr_version.cmp(&DB_VERSION) {
        // Database is new or not current
        std::cmp::Ordering::Less => {
            // initialize from scratch
            if curr_version == 0 {
                curr_version = mig_init(conn).await;
            }
            // for initialized but out-of-date schemas, proceed to
            // upgrade sequentially until we are current.
            // libSQL is new, so we should always be at the latest version
            if curr_version == DB_VERSION {
                info!(
                    "All migration scripts completed successfully.  Welcome to v{}.",
                    DB_VERSION
                );
            }
        }
        // Database is current, all is good
        std::cmp::Ordering::Equal => {
            debug!("Database version was already current (v{DB_VERSION})");
        }
        // Database is newer than what this code understands, abort
        std::cmp::Ordering::Greater => {
            panic!(
                "Database version is newer than supported by this executable (v{curr_version} > v{DB_VERSION})",
            );
        }
    }

    // Setup PRAGMA
    conn.execute_batch(STARTUP_SQL).await?;
    debug!("libSQL PRAGMA startup completed");
    Ok(DB_VERSION)
}
