//! Event persistence and querying using libSQL
use crate::config::Settings;
use crate::db::QueryResult;
use crate::error::{Error, Result};
use crate::event::{single_char_tagname, Event};
use crate::nip05::{Nip05Name, VerificationRecord};
use crate::payment::{InvoiceInfo, InvoiceStatus};
use crate::repo::libsql_migration::{upgrade_db, STARTUP_SQL};
use crate::repo::{now_jitter, NostrRepo};
use crate::server::NostrMetrics;
use crate::subscription::{ReqFilter, Subscription, TagOperand};
use crate::utils::{is_hex, unix_time};
use async_trait::async_trait;
use hex;
use itertools::Itertools;
use libsql::Builder;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{debug, info};

use nostr::key::Keys;

/// libSQL database connection wrapper
pub struct LibsqlConn {
    // We need to use Arc because libsql::Connection is not cloneable but we need
    // to share it across async tasks
    conn: Arc<libsql::Connection>,
}

impl LibsqlConn {
    /// Execute a SQL statement and return the number of rows affected
    pub async fn execute(&self, sql: &str, params: Vec<Value>) -> Result<u64> {
        self.conn
            .execute(sql, params)
            .await
            .map_err(Error::LibsqlError)
    }

    /// Execute multiple SQL statements in a batch
    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        // Split by semicolon and execute each statement
        for statement in sql.split(';') {
            let stmt = statement.trim();
            if !stmt.is_empty() && !stmt.starts_with("--") {
                self.execute(stmt, vec![]).await?;
            }
        }
        Ok(())
    }

    /// Query multiple rows from the database
    pub async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Rows> {
        let rows = self
            .conn
            .query(sql, params)
            .await
            .map_err(Error::LibsqlError)?;

        Ok(Rows { inner: rows })
    }
}

/// libSQL connection wrapper
#[derive(Clone)]
pub struct LibsqlRepo {
    /// Metrics
    metrics: NostrMetrics,
    /// Database URL
    _url: String,
    /// Auth token (optional)
    _auth_token: Option<String>,
    /// Database reference
    db: Arc<libsql::Database>,
    /// Write operations lock
    write_in_progress: Arc<Mutex<u64>>,
}

/// libSQL value type (re-export)
pub use libsql::Value;

/// libSQL row type (wrapper around libsql::Row)
pub struct Row(pub libsql::Row);

impl Row {
    pub fn get(&self, index: usize) -> Result<Value> {
        Ok(self.0.get::<Value>(index as i32)?)
    }

    pub fn get_blob(&self, index: usize) -> Result<Option<Vec<u8>>> {
        match self.0.get::<Value>(index as i32) {
            Ok(Value::Blob(b)) => Ok(Some(b)),
            Ok(Value::Null) => Ok(None),
            Ok(_) => Err(Error::CustomError("Expected blob value".to_string())),
            Err(e) => Err(Error::LibsqlError(e)),
        }
    }

    pub fn get_text(&self, index: usize) -> Result<String> {
        match self.0.get::<Value>(index as i32) {
            Ok(Value::Text(t)) => Ok(t),
            Ok(_) => Err(Error::CustomError("Expected text value".to_string())),
            Err(e) => Err(Error::LibsqlError(e)),
        }
    }

    pub fn get_i64(&self, index: usize) -> Result<i64> {
        match self.0.get::<Value>(index as i32) {
            Ok(Value::Integer(i)) => Ok(i),
            Ok(_) => Err(Error::CustomError("Expected integer value".to_string())),
            Err(e) => Err(Error::LibsqlError(e)),
        }
    }

    pub fn get_u64(&self, index: usize) -> Result<u64> {
        self.get_i64(index).map(|i| i as u64)
    }
}

/// libSQL rows wrapper
pub struct Rows {
    inner: libsql::Rows,
}

impl Rows {
    pub async fn next(&mut self) -> Option<Row> {
        match self.inner.next().await {
            Ok(Some(row)) => Some(Row(row)),
            Ok(None) => None,
            Err(_) => None,
        }
    }
}

impl LibsqlRepo {
    /// Create a new libSQL repository
    pub async fn new(settings: &Settings, metrics: NostrMetrics) -> Self {
        let url = settings.database.connection.clone();
        let auth_token = settings.database.connection_write.clone(); // reuse for auth token

        info!("Connecting to libSQL database at: {}", url);

        let db = if let Some(token) = &auth_token {
            // Remote database with auth
            Builder::new_remote(url.clone(), token.clone())
                .build()
                .await
                .unwrap()
        } else {
            // Remote without auth - for local files, just use the URL as path
            Builder::new_remote(url.clone(), String::new())
                .build()
                .await
                .unwrap()
        };

        // Run startup SQL
        if let Ok(conn) = db.connect() {
            let conn_wrapper = LibsqlConn {
                conn: Arc::new(conn),
            };
            tokio::spawn(async move {
                conn_wrapper.execute_batch(STARTUP_SQL).await.ok();
            });
        }

        LibsqlRepo {
            metrics,
            _url: url,
            _auth_token: auth_token,
            db: Arc::new(db),
            write_in_progress: Arc::new(Mutex::new(0)),
        }
    }

    /// Get a database connection
    async fn get_conn(&self) -> Result<LibsqlConn> {
        let conn = self.db.connect().map_err(Error::LibsqlError)?;
        Ok(LibsqlConn {
            conn: Arc::new(conn),
        })
    }

    /// Persist an event to the database, returning rows added.
    pub async fn persist_event(&self, e: &Event) -> Result<u64> {
        let conn = self.get_conn().await?;

        // get relevant fields from event and convert to blobs.
        let id_blob = hex::decode(&e.id).ok();
        let pubkey_blob: Option<Vec<u8>> = hex::decode(&e.pubkey).ok();
        let delegator_blob: Option<Vec<u8>> =
            e.delegated_by.as_ref().and_then(|d| hex::decode(d).ok());
        let event_str = serde_json::to_string(&e).ok();

        // check for replaceable events that would hide this one
        if e.is_replaceable() {
            let query =
                "SELECT e.id FROM event e INDEXED BY author_index WHERE e.author=? AND e.kind=? AND e.created_at >= ? LIMIT 1;";
            let params = vec![
                Value::Blob(pubkey_blob.clone().unwrap()),
                Value::Integer(e.kind as i64),
                Value::Integer(e.created_at as i64),
            ];
            let mut rows = conn.query(query, params.clone()).await?;
            if rows.next().await.is_some() {
                return Ok(0);
            }
        }

        // check for parameterized replaceable events
        if let Some(d_tag) = e.distinct_param() {
            let query = "SELECT e.id FROM event e LEFT JOIN tag t ON e.id=t.event_id WHERE e.author=? AND e.kind=? AND t.name='d' AND t.value=? AND e.created_at >= ? LIMIT 1;";
            let params = vec![
                Value::Blob(pubkey_blob.clone().unwrap()),
                Value::Integer(e.kind as i64),
                Value::Text(d_tag),
                Value::Integer(e.created_at as i64),
            ];
            let mut rows = conn.query(query, params.clone()).await?;
            if rows.next().await.is_some() {
                return Ok(0);
            }
        }

        // Insert the event
        let query = "INSERT OR IGNORE INTO event (event_hash, created_at, expires_at, kind, author, delegated_by, content, first_seen, hidden) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, strftime('%s','now'), FALSE);";
        let params = vec![
            Value::Blob(id_blob.clone().unwrap()),
            Value::Integer(e.created_at as i64),
            match e.expiration() {
                Some(exp) => Value::Integer(exp as i64),
                None => Value::Null,
            },
            Value::Integer(e.kind as i64),
            Value::Blob(pubkey_blob.clone().unwrap()),
            match delegator_blob {
                Some(ref d) => Value::Blob(d.clone()),
                None => Value::Null,
            },
            Value::Text(event_str.clone().unwrap()),
        ];

        let ins_count = conn.execute(query, params.clone()).await? as u64;
        if ins_count == 0 {
            return Ok(ins_count);
        }

        // Get the event ID
        let mut rows = conn.query("SELECT last_insert_rowid()", vec![]).await?;
        let ev_id = if let Some(row) = rows.next().await {
            row.get_u64(0)?
        } else {
            return Ok(0);
        };

        // Add all tags
        for tag in &e.tags {
            if tag.len() >= 2 {
                let tagname = &tag[0];
                let tagval = &tag[1];
                if single_char_tagname(tagname).is_some() {
                    let query =
                        "INSERT OR IGNORE INTO tag (event_id, name, value, kind, created_at) VALUES (?1, ?2, ?3, ?4, ?5)";
                    let params = vec![
                        Value::Integer(ev_id as i64),
                        Value::Text(tagname.clone()),
                        Value::Text(tagval.clone()),
                        Value::Integer(e.kind as i64),
                        Value::Integer(e.created_at as i64),
                    ];
                    conn.execute(query, params.clone()).await.ok();
                }
            }
        }

        // Handle replaceable events - delete older ones
        if e.is_replaceable() {
            let author = hex::decode(&e.pubkey)?;
            let query = "DELETE FROM event WHERE kind=? and author=? and id NOT IN (SELECT id FROM event INDEXED BY author_kind_index WHERE kind=? AND author=? ORDER BY created_at DESC LIMIT 1)";
            let params = vec![
                Value::Integer(e.kind as i64),
                Value::Blob(author.clone()),
                Value::Integer(e.kind as i64),
                Value::Blob(author),
            ];
            let update_count = conn.execute(query, params.clone()).await?;
            if update_count > 0 {
                info!(
                    "removed {} older replaceable kind {} events for author: {:?}",
                    update_count,
                    e.kind,
                    e.get_author_prefix()
                );
            }
        }

        // Handle parameterized replaceable events
        if let Some(d_tag) = e.distinct_param() {
            let query = "DELETE FROM event WHERE kind=? AND author=? AND id IN (SELECT e.id FROM event e LEFT JOIN tag t ON e.id=t.event_id WHERE e.kind=? AND e.author=? AND t.name='d' AND t.value=? ORDER BY t.created_at DESC LIMIT -1 OFFSET 1);";
            let params = vec![
                Value::Integer(e.kind as i64),
                Value::Blob(pubkey_blob.clone().unwrap()),
                Value::Integer(e.kind as i64),
                Value::Blob(pubkey_blob.clone().unwrap()),
                Value::Text(d_tag),
            ];
            let update_count = conn.execute(query, params.clone()).await?;
            if update_count > 0 {
                info!(
                    "removed {} older parameterized replaceable kind {} events for author: {:?}",
                    update_count,
                    e.kind,
                    e.get_author_prefix()
                );
            }
        }

        // Handle deletion events (kind=5)
        if e.kind == 5 {
            let event_candidates = e.tag_values_by_name("e");
            let mut params: Vec<Value> = vec![Value::Blob(hex::decode(&e.pubkey)?)];
            event_candidates
                .iter()
                .filter(|x| is_hex(x) && x.len() == 64)
                .filter_map(|x| hex::decode(x).ok())
                .for_each(|x| params.push(Value::Blob(x)));

            let query = format!(
                "UPDATE event SET hidden=TRUE WHERE kind!=5 AND author=? AND event_hash IN ({})",
                repeat_vars(params.len() - 1)
            );
            let update_count = conn.execute(&query, params.clone()).await?;
            info!(
                "hid {} deleted events for author {:?}",
                update_count,
                e.get_author_prefix()
            );
        } else {
            // Check if a deletion exists for this event
            let query = "SELECT e.id FROM event e WHERE e.author=? AND e.id IN (SELECT t.event_id FROM tag t WHERE t.name='e' AND t.kind=5 AND t.value=?) LIMIT 1;";
            let params = vec![
                Value::Blob(pubkey_blob.clone().unwrap()),
                Value::Blob(hex::decode(&e.id)?),
            ];
            let mut rows = conn.query(query, params.clone()).await?;
            if rows.next().await.is_some() {
                // Mark as hidden
                let query = "UPDATE event SET hidden=TRUE WHERE id=?";
                conn.execute(query, vec![Value::Integer(ev_id as i64)])
                    .await?;
                info!(
                    "hid event: {:?} due to existing deletion by author: {:?}",
                    e.get_event_id_prefix(),
                    e.get_author_prefix()
                );
                return Ok(0);
            }
        }

        Ok(ins_count)
    }
}

#[async_trait]
impl NostrRepo for LibsqlRepo {
    async fn start(&self) -> Result<()> {
        // libSQL doesn't need WAL checkpointing like SQLite
        // Just start cleanup task
        cleanup_expired_libsql(self.clone(), Duration::from_secs(600)).await
    }

    async fn migrate_up(&self) -> Result<usize> {
        let _write_guard = self.write_in_progress.lock().await;
        let mut conn = self.get_conn().await?;
        upgrade_db(&mut conn).await
    }

    async fn write_event(&self, e: &Event) -> Result<u64> {
        let start = Instant::now();
        let _write_guard = self.write_in_progress.lock().await;
        let event_count = self.persist_event(e).await?;
        self.metrics
            .write_events
            .observe(start.elapsed().as_secs_f64());
        Ok(event_count)
    }

    async fn query_subscription(
        &self,
        sub: Subscription,
        client_id: String,
        query_tx: tokio::sync::mpsc::Sender<QueryResult>,
        mut abandon_query_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()> {
        let start = Instant::now();
        let _slow_cutoff = Duration::from_millis(250);
        let mut row_count: usize = 0;

        // check before getting a DB connection if the client still wants the results
        if abandon_query_rx.try_recv().is_ok() {
            debug!(
                "query cancelled by client (before execution) (cid: {}, sub: {:?})",
                client_id, sub.id
            );
            return Ok(());
        }

        let conn = self.get_conn().await?;

        for (filter_idx, filter) in sub.filters.iter().enumerate() {
            let filter_start = Instant::now();

            // check if this is still active
            if abandon_query_rx.try_recv().is_ok() {
                debug!(
                    "query cancelled by client (cid: {}, sub: {:?})",
                    client_id, sub.id
                );
                return Ok(());
            }

            let (q, p, _idx) = query_from_filter(filter);

            let mut rows = conn.query(&q, p.clone()).await?;

            let mut first_result = true;
            loop {
                if let Some(row) = rows.next().await {
                    let first_event_elapsed = filter_start.elapsed();
                    if first_result {
                        debug!(
                            "first result in {:?} (cid: {}, sub: {:?}, filter: {})",
                            first_event_elapsed, client_id, sub.id, filter_idx
                        );
                        first_result = false;
                    }

                    // check if this is still active; every 100 rows
                    if row_count % 100 == 0 && abandon_query_rx.try_recv().is_ok() {
                        debug!(
                            "query cancelled by client (cid: {}, sub: {:?})",
                            client_id, sub.id
                        );
                        return Ok(());
                    }
                    row_count += 1;

                    let event_json = row.get_text(0)?;
                    query_tx
                        .send(QueryResult {
                            sub_id: sub.get_id(),
                            event: event_json,
                        })
                        .await
                        .ok();
                } else {
                    break;
                }
            }

            self.metrics
                .query_db
                .observe(filter_start.elapsed().as_secs_f64());
        }

        debug!(
            "query completed in {:?} (cid: {}, sub: {:?}, rows: {})",
            start.elapsed(),
            client_id,
            sub.id,
            row_count
        );

        query_tx
            .send(QueryResult {
                sub_id: sub.get_id(),
                event: "EOSE".to_string(),
            })
            .await
            .ok();

        self.metrics
            .query_sub
            .observe(start.elapsed().as_secs_f64());

        Ok(())
    }

    async fn optimize_db(&self) -> Result<()> {
        let conn = self.get_conn().await?;
        let start = Instant::now();
        conn.execute_batch("PRAGMA optimize;").await?;
        info!("optimize ran in {:?}", start.elapsed());
        Ok(())
    }

    async fn create_verification_record(&self, event_id: &str, name: &str) -> Result<()> {
        let e = hex::decode(event_id).ok();
        let n = name.to_owned();
        let conn = self.get_conn().await?;
        let _write_guard = self.write_in_progress.lock().await;

        let query =
            "INSERT INTO user_verification (metadata_event, name, verified_at) VALUES ((SELECT id from event WHERE event_hash=?), ?, strftime('%s','now'));";
        let params = vec![Value::Blob(e.unwrap()), Value::Text(n.clone())];
        conn.execute(query, params.clone()).await?;

        // delete everything else by this name
        let query = "DELETE FROM user_verification WHERE name = ? AND id != (SELECT id FROM user_verification WHERE name = ? ORDER BY id DESC LIMIT 1);";
        let params = vec![Value::Text(n.clone()), Value::Text(n.clone())];
        let count = conn.execute(query, params.clone()).await?;
        if count > 0 {
            info!("removed {} old verification records for ({:?})", count, n);
        }

        info!("saved new verification record for ({:?})", name);
        Ok(())
    }

    async fn update_verification_timestamp(&self, id: u64) -> Result<()> {
        let conn = self.get_conn().await?;
        let _write_guard = self.write_in_progress.lock().await;
        let verif_time = now_jitter(600);

        let query = "UPDATE user_verification SET verified_at=?, failure_count=0 WHERE id=?";
        let params = vec![Value::Integer(verif_time as i64), Value::Integer(id as i64)];
        conn.execute(query, params.clone()).await?;
        Ok(())
    }

    async fn fail_verification(&self, id: u64) -> Result<()> {
        let conn = self.get_conn().await?;
        let _write_guard = self.write_in_progress.lock().await;
        let fail_time = now_jitter(600);

        let query =
            "UPDATE user_verification SET failed_at=?, failure_count=failure_count+1 WHERE id=?";
        let params = vec![Value::Integer(fail_time as i64), Value::Integer(id as i64)];
        conn.execute(query, params.clone()).await?;
        Ok(())
    }

    async fn delete_verification(&self, id: u64) -> Result<()> {
        let conn = self.get_conn().await?;
        let _write_guard = self.write_in_progress.lock().await;

        let query = "DELETE FROM user_verification WHERE id=?;";
        let params = vec![Value::Integer(id as i64)];
        conn.execute(query, params.clone()).await?;
        Ok(())
    }

    async fn get_latest_user_verification(&self, pub_key: &str) -> Result<VerificationRecord> {
        let conn = self.get_conn().await?;
        let pub_key = pub_key.to_owned();

        let query = "SELECT v.id, v.name, e.event_hash, e.created_at, v.verified_at, v.failed_at, v.failure_count FROM user_verification v LEFT JOIN event e ON e.id=v.metadata_event WHERE e.author=? ORDER BY e.created_at DESC, v.verified_at DESC, v.failed_at DESC LIMIT 1;";
        let params = vec![Value::Blob(hex::decode(&pub_key)?)];
        let mut rows = conn.query(query, params.clone()).await?;

        if let Some(row) = rows.next().await {
            let rowid = row.get_u64(0)?;
            let rowname = row.get_text(1)?;
            let eventid = row.get_blob(2)?.unwrap();
            let created_at = row.get_u64(3)?;
            let verified_at = if row.get_blob(4)?.is_some() {
                Some(row.get_u64(4)?)
            } else {
                None
            };
            let failed_at = if row.get_blob(5)?.is_some() {
                Some(row.get_u64(5)?)
            } else {
                None
            };
            let failure_count = row.get_u64(6)?;

            Ok(VerificationRecord {
                rowid,
                name: Nip05Name::try_from(&rowname[..])?,
                address: pub_key,
                event: hex::encode(eventid),
                event_created: created_at,
                last_success: verified_at,
                last_failure: failed_at,
                failure_count,
            })
        } else {
            Err(Error::CustomError(
                "No verification record found".to_string(),
            ))
        }
    }

    async fn get_oldest_user_verification(&self, before: u64) -> Result<VerificationRecord> {
        let conn = self.get_conn().await?;

        let query = "SELECT v.id, v.name, e.event_hash, e.author, e.created_at, v.verified_at, v.failed_at, v.failure_count FROM user_verification v INNER JOIN event e ON e.id=v.metadata_event WHERE (v.verified_at < ? OR v.verified_at IS NULL) AND (v.failed_at < ? OR v.failed_at IS NULL) ORDER BY v.verified_at ASC, v.failed_at ASC LIMIT 1;";
        let params = vec![Value::Integer(before as i64), Value::Integer(before as i64)];
        let mut rows = conn.query(query, params.clone()).await?;

        if let Some(row) = rows.next().await {
            let rowid = row.get_u64(0)?;
            let rowname = row.get_text(1)?;
            let eventid = row.get_blob(2)?.unwrap();
            let pubkey = row.get_blob(3)?.unwrap();
            let created_at = row.get_u64(4)?;
            let verified_at = if row.get_blob(5)?.is_some() {
                Some(row.get_u64(5)?)
            } else {
                None
            };
            let failed_at = if row.get_blob(6)?.is_some() {
                Some(row.get_u64(6)?)
            } else {
                None
            };
            let failure_count = row.get_u64(7)?;

            Ok(VerificationRecord {
                rowid,
                name: Nip05Name::try_from(&rowname[..])?,
                address: hex::encode(pubkey),
                event: hex::encode(eventid),
                event_created: created_at,
                last_success: verified_at,
                last_failure: failed_at,
                failure_count,
            })
        } else {
            Err(Error::CustomError(
                "No verification record found".to_string(),
            ))
        }
    }

    async fn create_account(&self, pub_key: &Keys) -> Result<bool> {
        let pub_key = pub_key.public_key().to_string();
        let conn = self.get_conn().await?;

        let query =
            "INSERT OR IGNORE INTO account (pubkey, is_admitted, balance) VALUES (?1, ?2, ?3);";
        let params = vec![
            Value::Text(pub_key.clone()),
            Value::Integer(0),
            Value::Integer(0),
        ];
        let ins_count = conn.execute(query, params.clone()).await? as u64;

        Ok(ins_count == 1)
    }

    async fn admit_account(&self, pub_key: &Keys, admission_cost: u64) -> Result<()> {
        let pub_key = pub_key.public_key().to_string();
        let conn = self.get_conn().await?;

        let query = "UPDATE account SET is_admitted = TRUE, tos_accepted_at =  strftime('%s','now'), balance = balance - ?1 WHERE pubkey=?2;";
        let params = vec![Value::Integer(admission_cost as i64), Value::Text(pub_key)];
        conn.execute(query, params.clone()).await?;
        Ok(())
    }

    async fn get_account_balance(&self, pub_key: &Keys) -> Result<(bool, u64)> {
        let pub_key = pub_key.public_key().to_string();
        let conn = self.get_conn().await?;

        let query = "SELECT is_admitted, balance FROM account WHERE pubkey = ?1;";
        let params = vec![Value::Text(pub_key)];
        let mut rows = conn.query(query, params.clone()).await?;

        if let Some(row) = rows.next().await {
            let is_admitted = match row.get(0)? {
                Value::Integer(i) => i != 0,
                _ => false,
            };
            let balance = match row.get(1)? {
                Value::Integer(i) => i as u64,
                _ => 0,
            };
            Ok((is_admitted, balance))
        } else {
            Err(Error::CustomError("Account not found".to_string()))
        }
    }

    async fn update_account_balance(
        &self,
        pub_key: &Keys,
        positive: bool,
        new_balance: u64,
    ) -> Result<()> {
        let pub_key = pub_key.public_key().to_string();
        let conn = self.get_conn().await?;

        let query = if positive {
            "UPDATE account SET balance=balance + ?1 WHERE pubkey=?2"
        } else {
            "UPDATE account SET balance=balance - ?1 WHERE pubkey=?2"
        };
        let params = vec![Value::Integer(new_balance as i64), Value::Text(pub_key)];
        conn.execute(query, params.clone()).await?;
        Ok(())
    }

    async fn create_invoice_record(&self, pub_key: &Keys, invoice_info: InvoiceInfo) -> Result<()> {
        let pub_key = pub_key.public_key().to_string();
        let conn = self.get_conn().await?;

        let query = "INSERT INTO invoice (pubkey, payment_hash, amount, status, description, created_at, invoice) VALUES (?1, ?2, ?3, ?4, ?5, strftime('%s','now'), ?6);";
        let params = vec![
            Value::Text(pub_key),
            Value::Text(invoice_info.payment_hash),
            Value::Integer(invoice_info.amount as i64),
            Value::Text(invoice_info.status.to_string()),
            Value::Text(invoice_info.memo),
            Value::Text(invoice_info.bolt11),
        ];
        conn.execute(query, params.clone()).await?;
        Ok(())
    }

    async fn update_invoice(&self, payment_hash: &str, status: InvoiceStatus) -> Result<String> {
        let conn = self.get_conn().await?;

        // Get the pubkey
        let query = "SELECT pubkey, status, amount FROM invoice WHERE payment_hash=?1;";
        let params = vec![Value::Text(payment_hash.to_string())];
        let mut rows = conn.query(query, params.clone()).await?;

        if let Some(row) = rows.next().await {
            let pub_key = row.get_text(0)?;
            let prev_status = row.get_text(1)?;
            let amount = row.get_u64(2)?;

            // Update the status
            let query = if status == InvoiceStatus::Paid {
                "UPDATE invoice SET status=?1, confirmed_at = strftime('%s', 'now') WHERE payment_hash=?2;"
            } else {
                "UPDATE invoice SET status=?1 WHERE payment_hash=?2;"
            };
            let params = vec![
                Value::Text(status.to_string()),
                Value::Text(payment_hash.to_string()),
            ];
            conn.execute(query, params.clone()).await?;

            // If the invoice was unpaid and is now paid, update the account balance
            if prev_status == "Unpaid" && status == InvoiceStatus::Paid {
                let query = "UPDATE account SET balance = balance + ?1 WHERE pubkey = ?2;";
                let params = vec![Value::Integer(amount as i64), Value::Text(pub_key.clone())];
                conn.execute(query, params.clone()).await?;
            }

            Ok(pub_key)
        } else {
            Err(Error::CustomError("Invoice not found".to_string()))
        }
    }

    async fn get_unpaid_invoice(&self, pubkey: &Keys) -> Result<Option<InvoiceInfo>> {
        let conn = self.get_conn().await?;
        let pubkey_str = pubkey.public_key().to_string();

        let query = r#"
SELECT amount, payment_hash, description, invoice
FROM invoice
WHERE pubkey = ?1 AND status = 'Unpaid'
ORDER BY created_at DESC
LIMIT 1;
        "#;
        let params = vec![Value::Text(pubkey_str)];
        let mut rows = conn.query(query, params.clone()).await?;

        match rows.next().await {
            Some(row) => Ok(Some(InvoiceInfo {
                pubkey: pubkey.public_key().to_string(),
                payment_hash: row.get_text(1)?,
                bolt11: row.get_text(3)?,
                amount: row.get_u64(0)?,
                status: InvoiceStatus::Unpaid,
                memo: row.get_text(2)?,
                confirmed_at: None,
            })),
            None => Ok(None),
        }
    }
}

/// Cleanup expired events on a regular basis
async fn cleanup_expired_libsql(repo: LibsqlRepo, frequency: Duration) -> Result<()> {
    tokio::task::spawn(async move {
        loop {
            tokio::time::sleep(frequency).await;
            let start = Instant::now();
            if let Ok(conn) = repo.get_conn().await {
                let query = "DELETE FROM event WHERE expires_at <= ?";
                let params = vec![Value::Integer(unix_time() as i64)];
                match conn.execute(query, params).await {
                    Ok(count) => {
                        if count > 0 {
                            info!("removed {} expired events in: {:?}", count, start.elapsed());
                        }
                    }
                    Err(e) => {
                        info!("there was an error cleaning up expired events: {:?}", e);
                    }
                }
            }
        }
    });
    Ok(())
}

/// Decide if there is an index that should be used explicitly
fn override_index(f: &ReqFilter) -> Option<String> {
    if f.ids.is_some() {
        return Some("event_hash_index".into());
    }
    if let Some(ks) = &f.kinds {
        if f.ids.is_none()
            && ks.len() > 1
            && f.since.is_none()
            && f.until.is_none()
            && f.tags.is_none()
            && f.authors.is_none()
        {
            return Some("kind_created_at_index".into());
        }
    }
    if f.authors.is_some() {
        if f.since.is_none() && f.until.is_none() && f.limit.is_none() {
            if f.kinds.is_none() {
                return Some("author_index".into());
            }
            return Some("author_kind_index".into());
        }
        return Some("author_created_at_index".into());
    }
    None
}

/// Create a dynamic SQL subquery and params from a subscription filter
fn query_from_filter(f: &ReqFilter) -> (String, Vec<Value>, Option<String>) {
    if f.force_no_match {
        let empty_query = "SELECT e.content FROM event e WHERE 1=0".to_owned();
        let empty_params: Vec<Value> = vec![];
        return (empty_query, empty_params, None);
    }

    let idx_name = override_index(f);
    let idx_stmt = idx_name
        .as_ref()
        .map_or_else(|| "".to_owned(), |i| format!("INDEXED BY {i}"));
    let mut query = format!("SELECT e.content FROM event e {idx_stmt}");
    let mut params: Vec<Value> = vec![];
    let mut filter_components: Vec<String> = Vec::new();

    // Authors
    if let Some(authvec) = &f.authors {
        let mut auth_searches: Vec<String> = vec![];
        for auth in authvec {
            auth_searches.push("author=?".to_owned());
            let auth_bin = hex::decode(auth).ok();
            params.push(Value::Blob(auth_bin.unwrap()));
        }
        if !authvec.is_empty() {
            let auth_clause = format!("({})", auth_searches.join(" OR "));
            filter_components.push(auth_clause);
        } else {
            filter_components.push("false".to_owned());
        }
    }

    // Kinds
    if let Some(ks) = &f.kinds {
        let str_kinds: Vec<String> = ks.iter().map(|k| k.to_string()).collect();
        let kind_clause = format!("kind IN ({})", str_kinds.join(", "));
        filter_components.push(kind_clause);
    }

    // IDs
    if let Some(idvec) = &f.ids {
        let mut id_searches: Vec<String> = vec![];
        for id in idvec {
            id_searches.push("event_hash=?".to_owned());
            let id_bin = hex::decode(id).ok();
            params.push(Value::Blob(id_bin.unwrap()));
        }
        if idvec.is_empty() {
            filter_components.push("false".to_owned());
        } else {
            let id_clause = format!("({})", id_searches.join(" OR "));
            filter_components.push(id_clause);
        }
    }

    // Tags
    if let Some(map) = &f.tags {
        for (key, val) in map.iter().sorted_by(|(k1, _), (k2, _)| k1.cmp(k2)) {
            if val.is_empty() {
                continue;
            }

            let kind_clause = if let Some(ks) = &f.kinds {
                let str_kinds: Vec<String> = ks.iter().map(|k| k.to_string()).collect();
                format!("AND kind IN ({})", str_kinds.join(", "))
            } else {
                String::new()
            };

            let since_clause = if f.since.is_some() {
                format!("AND created_at >= {}", f.since.unwrap())
            } else {
                String::new()
            };

            let until_clause = if f.until.is_some() {
                format!("AND created_at <= {}", f.until.unwrap())
            } else {
                String::new()
            };

            match val {
                TagOperand::Or(v_or) => {
                    let mut sorted_values: Vec<String> = v_or.iter().cloned().collect();
                    sorted_values.sort();

                    let str_clause = format!("AND value IN ({})", repeat_vars(v_or.len()));

                    let mut tag_where_parts = vec![format!("name=? {str_clause}")];
                    if !kind_clause.is_empty() {
                        tag_where_parts.push(kind_clause);
                    }
                    if !since_clause.is_empty() {
                        tag_where_parts.push(since_clause);
                    }
                    if !until_clause.is_empty() {
                        tag_where_parts.push(until_clause);
                    }

                    let tag_clause = format!(
                        "e.id IN (SELECT t.event_id FROM tag t WHERE ({}))",
                        tag_where_parts.join(" ")
                    );

                    params.push(Value::Text(key.to_string()));
                    for v in &sorted_values {
                        params.push(Value::Text(v.clone()));
                    }
                    filter_components.push(tag_clause);
                }
                TagOperand::And(v_and) => {
                    let mut sorted_values: Vec<String> = v_and.iter().cloned().collect();
                    sorted_values.sort();

                    for v in &sorted_values {
                        let mut tag_where_parts = vec!["name=? AND value=?".to_string()];
                        if !kind_clause.is_empty() {
                            tag_where_parts.push(kind_clause.clone());
                        }
                        if !since_clause.is_empty() {
                            tag_where_parts.push(since_clause.clone());
                        }
                        if !until_clause.is_empty() {
                            tag_where_parts.push(until_clause.clone());
                        }

                        let tag_clause = format!(
                            "e.id IN (SELECT t.event_id FROM tag t WHERE ({}))",
                            tag_where_parts.join(" ")
                        );

                        params.push(Value::Text(key.to_string()));
                        params.push(Value::Text(v.clone()));
                        filter_components.push(tag_clause);
                    }
                }
            }
        }
    }

    // Timestamps
    if f.since.is_some() {
        filter_components.push(format!("created_at >= {}", f.since.unwrap()));
    }
    if f.until.is_some() {
        filter_components.push(format!("created_at <= {}", f.until.unwrap()));
    }

    // Hidden events
    query.push_str(" WHERE hidden!=TRUE");
    filter_components.push("(expires_at IS NULL OR expires_at > ?)".to_string());
    params.push(Value::Integer(unix_time() as i64));

    if !filter_components.is_empty() {
        query.push_str(" AND ");
        query.push_str(&filter_components.join(" AND "));
    }

    // Limit
    if let Some(lim) = f.limit {
        let _ = write!(query, " ORDER BY e.created_at DESC LIMIT {lim}");
    } else {
        query.push_str(" ORDER BY e.created_at ASC");
    }

    (query, params, idx_name)
}

/// Produce a arbitrary list of '?' parameters.
fn repeat_vars(count: usize) -> String {
    if count == 0 {
        return "".to_owned();
    }
    let mut s = "?, ".repeat(count);
    s.pop();
    s.pop();
    s
}
