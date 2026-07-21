use std::{collections::HashMap, sync::Mutex, time::Duration};

use paseo_control_plane::{
    journal::{Journal, JournalEntry, MAX_TRANSITIONS, TimelineQuery},
    paseo::{PaseoAdapter, ProcessExecutor, ProcessOutput, WriteResult},
    tools::ToolEngine,
};
use paseo_safety_core::{
    Applied, Command, DispatchAuthorization, InteractionSequence, ProposalId, ReplyId,
    ResponseBody, SafetyCore, SummaryId, ThreadId,
};
use rusqlite::Connection;

type RecordedCall = (Vec<String>, HashMap<String, String>);

const CLI_OUTPUT_SENTINEL: &str = "cli-output-must-not-be-reused";
const SEND_BODY: &str = "  exact send body\r\n";
const RUN_PROMPT: &str = "exact run task";

struct FakeExecutor {
    output: ProcessOutput,
    calls: Mutex<Vec<RecordedCall>>,
}

struct RunExecutor {
    output: ProcessOutput,
    calls: Mutex<Vec<RecordedCall>>,
}

#[derive(Clone, Copy)]
enum ExpectedRunResult {
    Rejected,
    OutcomeUnknown,
    Created(&'static str),
}

#[test]
fn operation_timeline_returns_latest_content_free_states_in_reverse_order() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    for (operation, summary, thread, state, timestamp) in [
        ("operation-a", "summary-a", "thread-a", "dispatching", 1),
        ("operation-a", "summary-a", "thread-a", "delivered", 2),
        ("operation-b", "summary-b", "thread-b", "outcome_unknown", 3),
    ] {
        journal
            .append(&JournalEntry {
                operation_id: operation,
                summary_id: summary,
                destination_thread_id: thread,
                response_sha256: "digest-only",
                state,
                timestamp_ms: timestamp,
                receiver_message_id: None,
            })
            .expect("append timeline state");
    }

    let timeline = journal.timeline(None, 10).expect("timeline");
    assert_eq!(timeline.len(), 2);
    assert_eq!(timeline[0].operation_id, "operation-b");
    assert_eq!(timeline[0].summary_id, "summary-b");
    assert_eq!(timeline[0].destination_thread_id, "thread-b");
    assert_eq!(timeline[0].state, "outcome_unknown");
    assert_eq!(timeline[1].operation_id, "operation-a");
    assert_eq!(timeline[1].state, "delivered");

    let delivered = journal
        .timeline(Some("delivered"), 1)
        .expect("filtered timeline");
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].operation_id, "operation-a");
}

#[test]
fn legacy_journal_timeline_preserves_its_requested_limit() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    for sequence in 0..=100 {
        journal
            .append(&JournalEntry {
                operation_id: &format!("operation-{sequence}"),
                summary_id: "summary",
                destination_thread_id: "thread",
                response_sha256: "digest-only",
                state: "delivered",
                timestamp_ms: sequence,
                receiver_message_id: None,
            })
            .expect("append timeline state");
    }

    let entries = journal.timeline(None, 101).expect("legacy timeline");

    assert_eq!(entries.len(), 101);
    assert_eq!(entries[0].operation_id, "operation-100");
    assert_eq!(entries[100].operation_id, "operation-0");
}

#[test]
fn journal_timeline_cursor_is_opaque_and_reusable() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    for (operation_id, state, timestamp_ms) in [
        ("operation-old", "delivered", 1),
        ("operation-middle", "dispatching", 2),
        ("operation-middle", "delivered", 3),
        ("operation-new", "delivered", 4),
    ] {
        journal
            .append(&JournalEntry {
                operation_id,
                summary_id: "summary",
                destination_thread_id: "thread",
                response_sha256: "digest-only",
                state,
                timestamp_ms,
                receiver_message_id: None,
            })
            .expect("append timeline transition");
    }

    let first = journal
        .query_timeline(&TimelineQuery {
            state: Some("delivered"),
            summary_id: None,
            destination_thread_id: None,
            cursor: None,
            limit: 2,
        })
        .expect("first page");
    let second = journal
        .query_timeline(&TimelineQuery {
            state: Some("delivered"),
            summary_id: None,
            destination_thread_id: None,
            cursor: first.next_cursor(),
            limit: 2,
        })
        .expect("second page");

    assert_eq!(first.entries[0].operation_id, "operation-new");
    assert_eq!(first.entries[1].operation_id, "operation-middle");
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].operation_id, "operation-old");
    assert!(second.next_cursor().is_none());
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

impl ProcessExecutor for &RunExecutor {
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
        match args.first().map(String::as_str) {
            Some("provider") => output(r#"[{"id":"gpt-5.6-sol-max"}]"#),
            Some("run") => self.output.clone(),
            Some("ls") => output("[]"),
            _ => output(""),
        }
    }
}

fn output(stdout: &str) -> ProcessOutput {
    process_output(Some(0), stdout, "", false, false)
}

fn overflowed_output_with_retained_prefix(stdout: &str) -> ProcessOutput {
    process_output(Some(0), stdout, "", true, false)
}

fn process_output(
    exit_code: Option<i32>,
    stdout: &str,
    stderr: &str,
    timed_out: bool,
    spawn_failed: bool,
) -> ProcessOutput {
    ProcessOutput {
        exit_code,
        stdout: stdout.to_owned(),
        stderr: stderr.to_owned(),
        timed_out,
        spawn_failed,
    }
}

#[test]
fn read_only_adapter_rejects_every_uncertain_exit_zero_capture() {
    let sessions = FakeExecutor {
        output: process_output(
            Some(0),
            r#"[{"id":"thread-a","name":"Agent A","status":"idle"}]"#,
            "",
            true,
            false,
        ),
        calls: Mutex::default(),
    };
    let adapter = PaseoAdapter::new(
        &sessions,
        "paseo".to_owned(),
        "secret-password".to_owned(),
        None,
    );
    assert!(adapter.list_sessions(false).is_none());

    let logs = FakeExecutor {
        output: process_output(Some(0), "valid reply prefix", "", true, false),
        calls: Mutex::default(),
    };
    let adapter = PaseoAdapter::new(
        &logs,
        "paseo".to_owned(),
        "secret-password".to_owned(),
        None,
    );
    assert!(adapter.read_log_text("thread-a", 6, true).is_none());

    let permissions = FakeExecutor {
        output: process_output(Some(0), r#"[{"id":"permission-a"}]"#, "", true, false),
        calls: Mutex::default(),
    };
    let adapter = PaseoAdapter::new(
        &permissions,
        "paseo".to_owned(),
        "secret-password".to_owned(),
        None,
    );
    assert!(adapter.list_pending_permissions().is_none());

    let providers = FakeExecutor {
        output: process_output(Some(0), r#"[{"id":"gpt-5.6-sol-max"}]"#, "", true, false),
        calls: Mutex::default(),
    };
    let adapter = PaseoAdapter::new(
        &providers,
        "paseo".to_owned(),
        "secret-password".to_owned(),
        None,
    );
    assert!(!adapter.provider_model_available("opencode/gpt-5.6-sol-max"));
}

fn assert_cli_output_is_not_reused(case: &str, calls: &[RecordedCall]) {
    assert!(
        calls.iter().all(|(args, env)| {
            args.iter()
                .chain(env.values())
                .all(|value| !value.contains(CLI_OUTPUT_SENTINEL))
        }),
        "{case}: CLI output entered a later argument or environment value"
    );
    assert!(
        calls
            .iter()
            .all(|(args, _)| args.first().map(String::as_str) != Some("logs")),
        "{case}: adapter invoked logs after a write attempt"
    );
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
#[allow(clippy::too_many_lines)]
fn send_adapter_classifies_process_outcomes_without_retry_or_content_reuse() {
    let cases = [
        (
            "spawn failure",
            process_output(None, "", "", false, true),
            WriteResult::Rejected,
        ),
        (
            "timeout after spawn",
            process_output(Some(137), CLI_OUTPUT_SENTINEL, "", true, false),
            WriteResult::OutcomeUnknown,
        ),
        (
            "valid receipt prefix plus an overflow byte",
            overflowed_output_with_retained_prefix(
                r#"{"messageId":"cli-output-must-not-be-reused"}"#,
            ),
            WriteResult::OutcomeUnknown,
        ),
        (
            "signal without exit code",
            process_output(None, CLI_OUTPUT_SENTINEL, "", false, false),
            WriteResult::OutcomeUnknown,
        ),
        (
            "plain nonzero exit",
            process_output(Some(1), "", CLI_OUTPUT_SENTINEL, false, false),
            WriteResult::OutcomeUnknown,
        ),
        (
            "structured nonzero stdout",
            process_output(
                Some(1),
                r#"{"error":{"code":"cli-output-must-not-be-reused"}}"#,
                "",
                false,
                false,
            ),
            WriteResult::OutcomeUnknown,
        ),
        (
            "structured nonzero stderr",
            process_output(
                Some(1),
                "",
                r#"{"error":{"code":"cli-output-must-not-be-reused"}}"#,
                false,
                false,
            ),
            WriteResult::OutcomeUnknown,
        ),
        (
            "zero with malformed output",
            output("{cli-output-must-not-be-reused"),
            WriteResult::OutcomeUnknown,
        ),
        (
            "zero with missing receipt",
            output(r#"{"ok":true,"detail":"cli-output-must-not-be-reused"}"#),
            WriteResult::OutcomeUnknown,
        ),
        (
            "zero with unrelated generic identity",
            output(r#"{"id":"cli-output-must-not-be-reused"}"#),
            WriteResult::OutcomeUnknown,
        ),
        (
            "zero with invalid receipt",
            output(r#"{"messageId":" cli-output-must-not-be-reused"}"#),
            WriteResult::OutcomeUnknown,
        ),
        (
            "zero with valid receipt",
            output(r#"{"messageId":"cli-output-must-not-be-reused"}"#),
            WriteResult::Delivered {
                receiver_message_id: CLI_OUTPUT_SENTINEL.to_owned(),
            },
        ),
    ];

    for (name, process_result, expected) in cases {
        let executor = FakeExecutor {
            output: process_result,
            calls: Mutex::default(),
        };
        let adapter = PaseoAdapter::new(
            &executor,
            "paseo".to_owned(),
            "secret-password".to_owned(),
            None,
        );

        assert_eq!(
            adapter.send_message(&authorization("thread-a", SEND_BODY)),
            expected,
            "{name}"
        );

        let calls = executor.calls.lock().expect("calls lock");
        assert_eq!(calls.len(), 1, "{name}: send was retried");
        assert_eq!(
            calls[0].0,
            [
                "send",
                "thread-a",
                "--prompt",
                SEND_BODY,
                "--no-wait",
                "--json"
            ],
            "{name}: send arguments changed"
        );
        assert!(!calls[0].0.join(" ").contains("secret-password"), "{name}");
        assert_eq!(calls[0].1["PASEO_PASSWORD"], "secret-password", "{name}");
        assert!(
            calls[0].1.values().all(|value| !value.contains(SEND_BODY)),
            "{name}: send body entered the child environment"
        );
        assert_cli_output_is_not_reused(name, &calls);
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn run_adapter_classifies_process_outcomes_without_retry_or_content_reuse() {
    let cases = [
        (
            "spawn failure",
            process_output(None, "", "", false, true),
            ExpectedRunResult::Rejected,
        ),
        (
            "timeout after spawn",
            process_output(Some(137), CLI_OUTPUT_SENTINEL, "", true, false),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "valid agent prefix plus an overflow byte",
            overflowed_output_with_retained_prefix(r#"{"agentId":"agent-created"}"#),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "signal without exit code",
            process_output(None, CLI_OUTPUT_SENTINEL, "", false, false),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "plain nonzero exit",
            process_output(Some(1), "", CLI_OUTPUT_SENTINEL, false, false),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "structured nonzero stdout",
            process_output(
                Some(1),
                r#"{"error":{"code":"cli-output-must-not-be-reused"}}"#,
                "",
                false,
                false,
            ),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "structured nonzero stderr",
            process_output(
                Some(1),
                "",
                r#"{"error":{"code":"cli-output-must-not-be-reused"}}"#,
                false,
                false,
            ),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "zero with malformed output",
            output("{cli-output-must-not-be-reused"),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "zero with missing agent identity",
            output(r#"{"id":"cli-output-must-not-be-reused"}"#),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "zero with invalid agent identity",
            output(r#"{"agentId":" cli-output-must-not-be-reused"}"#),
            ExpectedRunResult::OutcomeUnknown,
        ),
        (
            "zero with valid agent identity",
            output(r#"{"agentId":"cli-output-must-not-be-reused"}"#),
            ExpectedRunResult::Created(CLI_OUTPUT_SENTINEL),
        ),
    ];

    for (name, process_result, expected) in cases {
        let executor = RunExecutor {
            output: process_result,
            calls: Mutex::default(),
        };
        let mut tools = ToolEngine::new(
            Some(PaseoAdapter::new(
                &executor,
                "paseo".to_owned(),
                "secret-password".to_owned(),
                None,
            )),
            120_000,
        );
        tools.observe_user_turn();
        let proposed = tools.dispatch(
            "create_session",
            &serde_json::json!({"prompt": RUN_PROMPT}).to_string(),
            1,
        );
        let token = proposed["proposal_token"]
            .as_str()
            .expect("run proposal token");
        tools.observe_user_turn();

        let result = tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token": token}).to_string(),
            2,
        );

        match expected {
            ExpectedRunResult::Rejected => {
                assert_eq!(result["error"], "delivery_rejected", "{name}");
            }
            ExpectedRunResult::OutcomeUnknown => {
                assert_eq!(result["delivery"], "outcome_unknown", "{name}");
            }
            ExpectedRunResult::Created(agent_id) => {
                assert_eq!(result["delivery"], "delivered", "{name}");
                assert_eq!(result["session_id"], agent_id, "{name}");
            }
        }

        let calls = executor.calls.lock().expect("calls lock");
        let run_calls = calls
            .iter()
            .filter(|(args, _)| args.first().map(String::as_str) == Some("run"))
            .collect::<Vec<_>>();
        assert_eq!(run_calls.len(), 1, "{name}: detached run was retried");
        assert_eq!(
            run_calls[0].0,
            [
                "run",
                RUN_PROMPT,
                "--detach",
                "--provider",
                "opencode/gpt-5.6-sol-max",
                "--cwd",
                "~/",
                "--json"
            ],
            "{name}: run arguments changed"
        );
        assert!(
            !run_calls[0].0.join(" ").contains("secret-password"),
            "{name}"
        );
        assert_eq!(
            run_calls[0].1["PASEO_PASSWORD"], "secret-password",
            "{name}"
        );
        assert!(
            run_calls[0]
                .1
                .values()
                .all(|value| !value.contains(RUN_PROMPT)),
            "{name}: run prompt entered the child environment"
        );
        assert_cli_output_is_not_reused(name, &calls);
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
