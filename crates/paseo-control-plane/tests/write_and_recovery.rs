use std::{collections::HashMap, sync::Mutex, time::Duration};

use paseo_control_plane::{
    journal::{Journal, JournalEntry},
    paseo::{PaseoAdapter, ProcessExecutor, ProcessOutput, WriteResult},
};
use rusqlite::Connection;

type RecordedCall = (Vec<String>, HashMap<String, String>);

struct FakeExecutor {
    output: ProcessOutput,
    calls: Mutex<Vec<RecordedCall>>,
}

impl ProcessExecutor for &FakeExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        self.calls
            .lock()
            .expect("calls lock")
            .push((args.to_vec(), env.clone()));
        self.output.clone()
    }
}

fn output(stdout: &str) -> ProcessOutput {
    ProcessOutput {
        exit_code: Some(0),
        stdout: stdout.to_owned(),
        stderr: String::new(),
        timed_out: false,
        spawn_failed: false,
    }
}

#[test]
fn write_uses_exact_destination_and_body_with_secret_only_in_environment() {
    let executor = FakeExecutor {
        output: output(r#"{"messageId":"receiver-1"}"#),
        calls: Mutex::default(),
    };
    let adapter = PaseoAdapter::new(
        &executor,
        "paseo".to_owned(),
        "secret-password".to_owned(),
        None,
    );
    assert_eq!(
        adapter.send_message("thread-a", "  exact\r\n"),
        WriteResult::Delivered {
            receiver_message_id: "receiver-1".to_owned()
        }
    );
    let calls = executor.calls.lock().expect("calls lock");
    assert_eq!(
        calls[0].0,
        [
            "send",
            "thread-a",
            "--prompt",
            "  exact\r\n",
            "--no-wait",
            "--json"
        ]
    );
    assert!(!calls[0].0.join(" ").contains("secret-password"));
    assert_eq!(calls[0].1["PASEO_PASSWORD"], "secret-password");
}

#[test]
fn unvalidated_success_and_timeout_are_outcome_unknown_and_never_retried() {
    for uncertain in [
        output(r#"{"ok":true}"#),
        ProcessOutput {
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: true,
            spawn_failed: false,
        },
    ] {
        let executor = FakeExecutor {
            output: uncertain,
            calls: Mutex::default(),
        };
        let adapter = PaseoAdapter::new(&executor, "paseo".to_owned(), "secret".to_owned(), None);
        assert_eq!(
            adapter.send_message("thread", "body"),
            WriteResult::OutcomeUnknown
        );
        assert_eq!(executor.calls.lock().expect("calls lock").len(), 1);
    }
}

#[test]
fn recovery_journal_contains_only_metadata_and_never_resends() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("journal.sqlite3");
    let journal = Journal::open(&path).expect("open journal");
    journal
        .append(&JournalEntry {
            operation_id: "operation-a",
            summary_id: "summary-a",
            destination_thread_id: "thread-a",
            response_sha256: "digest-only",
            state: "dispatching",
            timestamp_ms: 10,
            receiver_message_id: None,
        })
        .expect("append dispatching");
    journal.recover(20).expect("recover metadata");
    drop(journal);

    let connection = Connection::open(path).expect("inspect journal");
    let latest: String = connection
        .query_row(
            "SELECT state FROM transitions ORDER BY sequence DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("latest state");
    assert_eq!(latest, "outcome_unknown");
    let columns: String = connection
        .query_row(
            "SELECT group_concat(name, ',') FROM pragma_table_info('transitions')",
            [],
            |row| row.get(0),
        )
        .expect("journal columns");
    for prohibited in [
        "body",
        "response_text",
        "transcript",
        "summary_text",
        "secret",
    ] {
        assert!(!columns.contains(prohibited));
    }
}
