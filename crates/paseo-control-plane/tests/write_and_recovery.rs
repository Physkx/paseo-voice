use std::{collections::HashMap, sync::Mutex, time::Duration};

use paseo_control_plane::{
    journal::{Journal, JournalEntry, MAX_TRANSITIONS},
    paseo::{PaseoAdapter, ProcessExecutor, ProcessOutput, WriteResult},
};
use paseo_safety_core::{
    Applied, Command, DispatchAuthorization, InteractionSequence, ProposalId, ReplyId,
    ResponseBody, SafetyCore, SummaryId, ThreadId,
};
use rusqlite::Connection;

type RecordedCall = (Vec<String>, HashMap<String, String>);

struct FakeExecutor {
    output: ProcessOutput,
    calls: Mutex<Vec<RecordedCall>>,
}

#[test]
fn journal_retention_is_bounded_to_the_documented_transition_count() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("retention.sqlite3");
    let journal = Journal::open(&path).expect("open journal");
    let mut raw = Connection::open(&path).expect("open raw journal connection");
    let transaction = raw.transaction().expect("start transaction");
    for sequence in 0..MAX_TRANSITIONS {
        transaction
            .execute(
                "INSERT INTO transitions
                 (operation_id, summary_id, destination_thread_id, response_sha256, state,
                  timestamp_ms, receiver_message_id) VALUES (?, 'summary', 'thread', 'digest',
                  'delivered', 1, NULL)",
                [format!("old-{sequence}")],
            )
            .expect("insert old transition");
    }
    transaction.commit().expect("commit old transitions");
    drop(raw);

    journal
        .append(&JournalEntry {
            operation_id: "newest",
            summary_id: "summary",
            destination_thread_id: "thread",
            response_sha256: "digest",
            state: "delivered",
            timestamp_ms: 2,
            receiver_message_id: Some("receiver"),
        })
        .expect("append and prune");
    drop(journal);

    let connection = Connection::open(path).expect("inspect retained journal");
    let count: i64 = connection
        .query_row("SELECT count(*) FROM transitions", [], |row| row.get(0))
        .expect("transition count");
    assert_eq!(
        count,
        i64::try_from(MAX_TRANSITIONS).expect("retention limit fits i64")
    );
    let newest: i64 = connection
        .query_row(
            "SELECT count(*) FROM transitions WHERE operation_id = 'newest'",
            [],
            |row| row.get(0),
        )
        .expect("newest count");
    assert_eq!(newest, 1);
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

fn authorization(thread: &str, body: &str) -> DispatchAuthorization {
    let mut core = SafetyCore::new(120_000);
    let summary_id = SummaryId::new("summary").expect("summary ID");
    core.apply(Command::ObserveReply {
        summary_id: summary_id.clone(),
        source_thread_id: ThreadId::new(thread).expect("thread ID"),
        source_reply_id: ReplyId::new("reply").expect("reply ID"),
        observed_at_ms: 0,
    })
    .expect("observe reply");
    core.apply(Command::MarkSummaryReady {
        summary_id: summary_id.clone(),
    })
    .expect("ready summary");
    core.apply(Command::ActivateNext).expect("activate summary");
    let proposal_id = ProposalId::new("proposal").expect("proposal ID");
    core.apply(Command::ProposeResponse {
        proposal_id: proposal_id.clone(),
        summary_id,
        response: ResponseBody::new(body).expect("response body"),
        interaction: InteractionSequence::new(1),
        now_ms: 1,
    })
    .expect("propose response");
    let Applied::DispatchAuthorized(authorization) = core
        .apply(Command::ConfirmResponse {
            proposal_id,
            interaction: InteractionSequence::new(2),
            now_ms: 2,
        })
        .expect("confirm response")
    else {
        panic!("confirmation must return authorization");
    };
    authorization
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
        adapter.send_message(&authorization("thread-a", "  exact\r\n")),
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
            adapter.send_message(&authorization("thread", "body")),
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            std::fs::metadata(&path)
                .expect("journal metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
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
    let recovered = journal
        .latest_status("operation-a")
        .expect("query recovered status")
        .expect("operation status");
    assert_eq!(recovered.state, "outcome_unknown");
    assert_eq!(recovered.timestamp_ms, 20);
    assert!(recovered.receiver_message_id.is_none());
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
