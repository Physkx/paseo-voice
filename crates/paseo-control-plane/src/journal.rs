//! Content-free `SQLite` recovery journal.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension as _, params};
use sha2::{Digest as _, Sha256};

/// Maximum content-free transition rows retained locally.
pub const MAX_TRANSITIONS: usize = 10_000;
/// Maximum latest operation states accepted by the public timeline tool.
pub const MAX_TIMELINE_ENTRIES: usize = 100;

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
    /// Opaque summary identity.
    pub summary_id: String,
    /// Provenance-derived destination identity.
    pub destination_thread_id: String,
    /// Latest transition state.
    pub state: String,
    /// Injected transition timestamp.
    pub timestamp_ms: u64,
    /// Optional receiver acknowledgement identity.
    pub receiver_message_id: Option<String>,
}

/// Opaque continuation for latest-state timeline pagination.
#[derive(Clone, Copy)]
pub struct TimelineCursor {
    filter_fingerprint: [u8; 32],
    snapshot_max_sequence: u64,
    before_sequence: u64,
    snapshot_min_sequence: u64,
}

impl TimelineCursor {
    pub(crate) const fn from_parts(
        filter_fingerprint: [u8; 32],
        snapshot_max_sequence: u64,
        before_sequence: u64,
        snapshot_min_sequence: u64,
    ) -> Self {
        Self {
            filter_fingerprint,
            snapshot_max_sequence,
            before_sequence,
            snapshot_min_sequence,
        }
    }

    pub(crate) const fn filter_fingerprint(self) -> [u8; 32] {
        self.filter_fingerprint
    }

    pub(crate) const fn snapshot_max_sequence(self) -> u64 {
        self.snapshot_max_sequence
    }

    pub(crate) const fn before_sequence(self) -> u64 {
        self.before_sequence
    }

    pub(crate) const fn snapshot_min_sequence(self) -> u64 {
        self.snapshot_min_sequence
    }
}

/// Failure to query a latest-state timeline page.
#[derive(Debug)]
pub enum TimelineError {
    /// The continuation does not belong to this exact query.
    InvalidCursor,
    /// Retention removed rows that belonged to the captured snapshot.
    CursorExpired,
    /// The `SQLite` query failed.
    Storage(rusqlite::Error),
}

impl From<rusqlite::Error> for TimelineError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error)
    }
}

/// Exact filters and bound for a latest-state timeline query.
pub struct TimelineQuery<'a> {
    /// Optional exact latest-state filter.
    pub state: Option<&'a str>,
    /// Optional exact summary identity filter.
    pub summary_id: Option<&'a str>,
    /// Optional exact destination thread identity filter.
    pub destination_thread_id: Option<&'a str>,
    /// Optional opaque continuation returned by the previous page.
    pub cursor: Option<TimelineCursor>,
    /// Maximum number of latest operation states to return.
    pub limit: usize,
}

/// One page of latest content-free operation states.
pub struct TimelinePage {
    /// Latest states in descending transition sequence order.
    pub entries: Vec<OperationStatus>,
    next_cursor: Option<TimelineCursor>,
}

impl TimelinePage {
    /// Return a continuation only when another matching page exists.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<TimelineCursor> {
        self.next_cursor
    }
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
                "SELECT operation_id, summary_id, destination_thread_id, state,
                        timestamp_ms, receiver_message_id
                 FROM transitions WHERE operation_id = ?
                 ORDER BY sequence DESC LIMIT 1",
                [operation_id],
                operation_status,
            )
            .optional()
    }

    /// List the latest content-free state for recent operations.
    ///
    /// # Errors
    ///
    /// Returns a `SQLite` error if the query fails.
    pub fn timeline(
        &self,
        state: Option<&str>,
        limit: usize,
    ) -> rusqlite::Result<Vec<OperationStatus>> {
        let result = self.query_timeline(&TimelineQuery {
            state,
            summary_id: None,
            destination_thread_id: None,
            cursor: None,
            limit,
        });
        match result {
            Ok(page) => Ok(page.entries),
            Err(TimelineError::Storage(error)) => Err(error),
            Err(TimelineError::InvalidCursor) => {
                unreachable!("a first-page timeline query cannot have an invalid cursor")
            }
            Err(TimelineError::CursorExpired) => {
                unreachable!("a first-page timeline query cannot have an expired cursor")
            }
        }
    }

    /// Query latest content-free operation states through exact predicates.
    ///
    /// # Errors
    ///
    /// Returns [`TimelineError::InvalidCursor`] when filters change,
    /// [`TimelineError::CursorExpired`] when retention advances, or
    /// [`TimelineError::Storage`] when the `SQLite` query fails.
    pub fn query_timeline(&self, query: &TimelineQuery<'_>) -> Result<TimelinePage, TimelineError> {
        let filter_fingerprint = timeline_filter_fingerprint(query);
        let cursor = query.cursor;
        if cursor.is_some_and(|cursor| cursor.filter_fingerprint() != filter_fingerprint) {
            return Err(TimelineError::InvalidCursor);
        }
        let transaction = self.connection.unchecked_transaction()?;
        let (minimum, maximum) = transaction.query_row(
            "SELECT MIN(sequence), MAX(sequence) FROM transitions",
            [],
            |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
        )?;
        let (Some(minimum), Some(maximum)) = (minimum, maximum) else {
            if cursor.is_some() {
                return Err(TimelineError::CursorExpired);
            }
            transaction.commit()?;
            return Ok(TimelinePage {
                entries: Vec::new(),
                next_cursor: None,
            });
        };
        let current_min_sequence = timeline_sequence(minimum, 0)?;
        let current_max_sequence = timeline_sequence(maximum, 1)?;
        let (snapshot_min_sequence, snapshot_max_sequence) = if let Some(cursor) = cursor {
            if current_min_sequence > cursor.snapshot_min_sequence()
                || current_max_sequence < cursor.snapshot_max_sequence()
            {
                return Err(TimelineError::CursorExpired);
            }
            (
                cursor.snapshot_min_sequence(),
                cursor.snapshot_max_sequence(),
            )
        } else {
            (current_min_sequence, current_max_sequence)
        };
        let snapshot_max = i64::try_from(snapshot_max_sequence)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let before_sequence = cursor
            .map(TimelineCursor::before_sequence)
            .map(i64::try_from)
            .transpose()
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let mut statement = transaction.prepare(
            "WITH latest AS (
               SELECT MAX(sequence) AS sequence
               FROM transitions
               WHERE sequence <= ?4
               GROUP BY operation_id
             )
             SELECT transition.operation_id, transition.summary_id,
                    transition.destination_thread_id, transition.state,
                    transition.timestamp_ms, transition.receiver_message_id,
                    transition.sequence
             FROM transitions transition
             JOIN latest ON latest.sequence = transition.sequence
             WHERE (?1 IS NULL OR transition.state = ?1)
               AND (?2 IS NULL OR transition.summary_id = ?2)
               AND (?3 IS NULL OR transition.destination_thread_id = ?3)
               AND (?5 IS NULL OR transition.sequence < ?5)
             ORDER BY transition.sequence DESC
             LIMIT ?6",
        )?;
        let limit = query.limit;
        let fetch_limit = i64::try_from(limit.saturating_add(1))
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let mut rows = statement
            .query_map(
                params![
                    query.state,
                    query.summary_id,
                    query.destination_thread_id,
                    snapshot_max,
                    before_sequence,
                    fetch_limit
                ],
                timeline_row,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let next_cursor = if limit > 0 && rows.len() > limit {
            Some(TimelineCursor::from_parts(
                filter_fingerprint,
                snapshot_max_sequence,
                rows[limit - 1].1,
                snapshot_min_sequence,
            ))
        } else {
            None
        };
        rows.truncate(limit);
        let page = TimelinePage {
            entries: rows.into_iter().map(|(status, _)| status).collect(),
            next_cursor,
        };
        drop(statement);
        transaction.commit()?;
        Ok(page)
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

fn operation_status(row: &rusqlite::Row<'_>) -> rusqlite::Result<OperationStatus> {
    let timestamp_ms = row.get::<_, i64>(4)?;
    let timestamp_ms = u64::try_from(timestamp_ms).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    Ok(OperationStatus {
        operation_id: row.get(0)?,
        summary_id: row.get(1)?,
        destination_thread_id: row.get(2)?,
        state: row.get(3)?,
        timestamp_ms,
        receiver_message_id: row.get(5)?,
    })
}

fn timeline_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(OperationStatus, u64)> {
    let sequence = row.get::<_, i64>(6)?;
    let sequence = u64::try_from(sequence).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            6,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    Ok((operation_status(row)?, sequence))
}

fn timeline_sequence(sequence: i64, column: usize) -> rusqlite::Result<u64> {
    u64::try_from(sequence).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn timeline_filter_fingerprint(query: &TimelineQuery<'_>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"paseo-timeline-filter-v1");
    for value in [query.state, query.summary_id, query.destination_thread_id] {
        match value {
            None => hasher.update([0]),
            Some(value) => {
                hasher.update([1]);
                hasher.update(
                    u64::try_from(value.len())
                        .expect("filter length fits u64")
                        .to_be_bytes(),
                );
                hasher.update(value.as_bytes());
            }
        }
    }
    hasher.finalize().into()
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
