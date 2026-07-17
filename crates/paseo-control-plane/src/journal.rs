//! Content-free `SQLite` recovery journal.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension as _, params};

/// Maximum content-free transition rows retained locally.
pub const MAX_TRANSITIONS: usize = 10_000;

/// Metadata transition safe for durable storage.
pub struct JournalEntry<'a> {
    /// Opaque operation identity.
    pub operation_id: &'a str,
    /// Opaque summary identity.
    pub summary_id: &'a str,
    /// Provenance-derived destination identity.
    pub destination_thread_id: &'a str,
    /// SHA-256 of the response, never the body.
    pub response_sha256: &'a str,
    /// State transition name.
    pub state: &'a str,
    /// Injected monotonic timestamp.
    pub timestamp_ms: u64,
    /// Optional receiver acknowledgement identity.
    pub receiver_message_id: Option<&'a str>,
}

/// Latest durable status for one operation, with no response content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationStatus {
    /// Opaque operation identity.
    pub operation_id: String,
    /// Latest transition state.
    pub state: String,
    /// Injected transition timestamp.
    pub timestamp_ms: u64,
    /// Optional receiver acknowledgement identity.
    pub receiver_message_id: Option<String>,
}

/// Append-only metadata journal.
pub struct Journal {
    connection: Connection,
}

impl Journal {
    /// Open or create a journal and initialise its schema.
    ///
    /// # Errors
    ///
    /// Returns a `SQLite` error when opening or initialising fails.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        prepare_file(path)?;
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS transitions (
               sequence INTEGER PRIMARY KEY AUTOINCREMENT,
               operation_id TEXT NOT NULL,
               summary_id TEXT NOT NULL,
               destination_thread_id TEXT NOT NULL,
               response_sha256 TEXT NOT NULL,
               state TEXT NOT NULL,
               timestamp_ms INTEGER NOT NULL,
               receiver_message_id TEXT
             );",
        )?;
        Ok(Self { connection })
    }

    /// Append one content-free transition.
    ///
    /// # Errors
    ///
    /// Returns a `SQLite` error if the append is not durable.
    pub fn append(&self, entry: &JournalEntry<'_>) -> rusqlite::Result<()> {
        let timestamp_ms = i64::try_from(entry.timestamp_ms)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        self.connection.execute(
            "INSERT INTO transitions
             (operation_id, summary_id, destination_thread_id, response_sha256, state,
              timestamp_ms, receiver_message_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                entry.operation_id,
                entry.summary_id,
                entry.destination_thread_id,
                entry.response_sha256,
                entry.state,
                timestamp_ms,
                entry.receiver_message_id,
            ],
        )?;
        self.prune()?;
        Ok(())
    }

    /// Query the latest durable state for one opaque operation ID.
    ///
    /// # Errors
    ///
    /// Returns a `SQLite` error if the query fails.
    pub fn latest_status(&self, operation_id: &str) -> rusqlite::Result<Option<OperationStatus>> {
        self.connection
            .query_row(
                "SELECT operation_id, state, timestamp_ms, receiver_message_id
                 FROM transitions WHERE operation_id = ?
                 ORDER BY sequence DESC LIMIT 1",
                [operation_id],
                |row| {
                    let timestamp_ms = row.get::<_, i64>(2)?;
                    let timestamp_ms = u64::try_from(timestamp_ms).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })?;
                    Ok(OperationStatus {
                        operation_id: row.get(0)?,
                        state: row.get(1)?,
                        timestamp_ms,
                        receiver_message_id: row.get(3)?,
                    })
                },
            )
            .optional()
    }

    /// Convert restart-unsafe states without constructing a fresh send.
    ///
    /// # Errors
    ///
    /// Returns a `SQLite` error if recovery metadata cannot be appended.
    pub fn recover(&self, timestamp_ms: u64) -> rusqlite::Result<()> {
        let timestamp_ms = i64::try_from(timestamp_ms)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        self.connection.execute(
            "INSERT INTO transitions
             (operation_id, summary_id, destination_thread_id, response_sha256, state,
              timestamp_ms, receiver_message_id)
             SELECT operation_id, summary_id, destination_thread_id, response_sha256,
                    CASE WHEN state = 'dispatching' THEN 'outcome_unknown' ELSE 'invalidated' END,
                    ?, NULL
             FROM transitions latest
             WHERE sequence = (SELECT MAX(sequence) FROM transitions candidate
                               WHERE candidate.operation_id = latest.operation_id)
               AND state IN ('dispatching', 'pending')",
            [timestamp_ms],
        )?;
        self.prune()?;
        Ok(())
    }

    fn prune(&self) -> rusqlite::Result<()> {
        self.connection.execute(
            "DELETE FROM transitions
             WHERE sequence NOT IN (
               SELECT sequence FROM transitions ORDER BY sequence DESC LIMIT ?
             )",
            [i64::try_from(MAX_TRANSITIONS).expect("retention bound fits i64")],
        )?;
        Ok(())
    }
}

#[cfg(unix)]
fn prepare_file(path: &Path) -> rusqlite::Result<()> {
    use std::{
        fs::OpenOptions,
        os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _},
    };

    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let mut permissions = file
        .metadata()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
        .permissions();
    permissions.set_mode(0o600);
    file.set_permissions(permissions)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

#[cfg(not(unix))]
fn prepare_file(path: &Path) -> rusqlite::Result<()> {
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map(|_| ())
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}
