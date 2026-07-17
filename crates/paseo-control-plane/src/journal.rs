//! Content-free `SQLite` recovery journal.

use std::path::Path;

use rusqlite::{Connection, params};

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
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
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
        Ok(())
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
        Ok(())
    }
}
