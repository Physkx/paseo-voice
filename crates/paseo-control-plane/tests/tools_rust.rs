use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use paseo_control_plane::{
    commands::CommandState,
    config::PaseoHostProfile,
    journal::{Journal, JournalEntry, MAX_TRANSITIONS},
    paseo::{PaseoAdapter, ProcessExecutor, ProcessOutput},
    tools::{PaseoHost, ProposalPresentation, ToolEngine, definitions, proposal_frame},
};
use serde_json::Value;

struct FakeExecutor {
    calls: Mutex<Vec<Vec<String>>>,
}

struct SelectiveUncertainExecutor {
    uncertain_command: &'static str,
}

impl ProcessExecutor for &FakeExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        self.calls.lock().expect("calls lock").push(args.to_vec());
        let stdout = match args.first().map(String::as_str) {
            Some("ls") => {
                r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"},{"id":"thread-b","shortId":"b","name":"Agent B","status":"idle"}]"#
            }
            Some("logs") if args.get(1).map(String::as_str) == Some("thread-a") => {
                "  exact reply A\r\n"
            }
            Some("logs") => "reply B",
            Some("send") => r#"{"messageId":"receiver-a"}"#,
            Some("provider") => r#"[{"id":"gpt-5.6-sol-max","model":"GPT 5.6"}]"#,
            Some("run") => r#"{"agentId":"run-a"}"#,
            _ => "",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

impl ProcessExecutor for &SelectiveUncertainExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        let command = args.first().map(String::as_str);
        let stdout = match command {
            Some("ls") => r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#,
            Some("logs") => "valid reply prefix",
            Some("provider") => r#"[{"id":"gpt-5.6-sol-max"}]"#,
            Some("permit") => r#"[{"id":"permission-a"}]"#,
            _ => "",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: command == Some(self.uncertain_command),
            spawn_failed: false,
        }
    }
}

fn engine(executor: &FakeExecutor) -> ToolEngine<&FakeExecutor> {
    ToolEngine::new(
        Some(PaseoAdapter::new(
            executor,
            "paseo".to_owned(),
            "password".to_owned(),
            None,
        )),
        120_000,
    )
}

fn uncertain_engine(
    executor: &SelectiveUncertainExecutor,
) -> ToolEngine<&SelectiveUncertainExecutor> {
    ToolEngine::new(
        Some(PaseoAdapter::new(
            executor,
            "paseo".to_owned(),
            "password".to_owned(),
            None,
        )),
        120_000,
    )
}

#[test]
fn a_reconnecting_engine_keeps_dedup_but_inherits_no_active_context() {
    let executor = FakeExecutor {
        calls: Mutex::new(Vec::new()),
    };
    let mut first = engine(&executor);
    assert_eq!(
        first.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        Value::Bool(true)
    );

    // The broker process hands the content-free queue to a fresh connection.
    let snapshot = first.queue_snapshot();
    let mut second = engine(&executor);
    second.seed_queue(snapshot);

    // No active context is inherited: a response has no target after reconnect.
    assert_eq!(
        second.dispatch("send_message", r#"{"text":"no target after reconnect"}"#, 2)["error"],
        "no_active_reply"
    );

    // The already-read reply stays deduplicated across the reconnect.
    let reread = second.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 3);
    assert_eq!(reread["respondable"], Value::Bool(false));
    assert_eq!(reread["reason"], "reply_already_observed");

    // A different, unread reply still reads freshly and is respondable.
    assert_eq!(
        second.dispatch("read_latest_reply", r#"{"session":"thread-b"}"#, 4)["respondable"],
        Value::Bool(true)
    );
}

fn active_summary_id<E: ProcessExecutor>(tools: &ToolEngine<E>) -> String {
    tools.presentation_snapshot()["bound_context"]["summary_id"]
        .as_str()
        .expect("active summary ID")
        .to_owned()
}

fn update_current_summary<E: ProcessExecutor>(
    tools: &mut ToolEngine<E>,
    summary: &str,
    degraded: bool,
) {
    let summary_id = active_summary_id(tools);
    assert!(tools.update_active_summary_if_current(&summary_id, summary, degraded));
}

fn mutate_timeline_cursor(cursor: &str, mutate: impl FnOnce(&mut [u8])) -> String {
    let mut payload = URL_SAFE_NO_PAD.decode(cursor).expect("decode real cursor");
    mutate(&mut payload);
    URL_SAFE_NO_PAD.encode(payload)
}

#[test]
fn operation_timeline_definition_publishes_string_bounds() {
    let definitions = definitions();
    let timeline = definitions
        .as_array()
        .expect("tool definitions")
        .iter()
        .find(|definition| definition["name"] == "list_operation_timeline")
        .expect("timeline definition");
    let properties = &timeline["parameters"]["properties"];

    assert_eq!(properties["state"]["maxLength"], 15);
    assert_eq!(properties["summary_id"]["minLength"], 1);
    assert_eq!(properties["summary_id"]["maxLength"], 128);
    assert_eq!(properties["destination_thread_id"]["minLength"], 1);
    assert_eq!(properties["destination_thread_id"]["maxLength"], 128);
    assert_eq!(properties["cursor"]["minLength"], 76);
    assert_eq!(properties["cursor"]["maxLength"], 76);
}

#[test]
fn realtime_definitions_do_not_publish_confirmation() {
    let definitions = definitions();

    assert!(
        definitions
            .as_array()
            .expect("tool definitions")
            .iter()
            .all(|definition| definition["name"] != "confirm_action")
    );
}

#[test]
fn replay_summary_definition_accepts_only_an_empty_object() {
    let definitions = definitions();
    let replay = definitions
        .as_array()
        .expect("tool definitions")
        .iter()
        .find(|definition| definition["name"] == "replay_summary")
        .expect("replay summary definition");

    assert_eq!(
        replay["parameters"],
        serde_json::json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    );
}

#[test]
fn uncertain_process_prefixes_cannot_select_read_or_propose() {
    let sessions = SelectiveUncertainExecutor {
        uncertain_command: "ls",
    };
    let mut tools = uncertain_engine(&sessions);
    assert_eq!(
        tools.dispatch("list_sessions", "{}", 1)["error"],
        "paseo_unavailable"
    );
    assert_eq!(
        tools.dispatch("set_current_session", r#"{"session":"thread-a"}"#, 2)["error"],
        "session_not_found_or_ambiguous"
    );
    assert_eq!(tools.presentation_snapshot()["bound_context"], Value::Null);

    let logs = SelectiveUncertainExecutor {
        uncertain_command: "logs",
    };
    let mut tools = uncertain_engine(&logs);
    assert_eq!(
        tools.dispatch("set_current_session", r#"{"session":"thread-a"}"#, 1)["ok"],
        true
    );
    assert_eq!(
        tools.dispatch("read_latest_reply", "{}", 2)["error"],
        "reply_unavailable"
    );
    assert_eq!(tools.presentation_snapshot()["bound_context"], Value::Null);
    assert_eq!(
        tools.dispatch("send_message", r#"{"text":"must not propose"}"#, 3)["error"],
        "no_active_reply"
    );
    assert!(!tools.has_pending_action());

    let providers = SelectiveUncertainExecutor {
        uncertain_command: "provider",
    };
    let mut tools = uncertain_engine(&providers);
    assert_eq!(
        tools.dispatch("create_session", r#"{"prompt":"must not propose"}"#, 1)["error"],
        "provider_unavailable"
    );
    assert!(!tools.has_pending_action());

    let permissions = SelectiveUncertainExecutor {
        uncertain_command: "permit",
    };
    assert_eq!(
        uncertain_engine(&permissions).dispatch("list_pending_permissions", "{}", 1)["error"],
        "paseo_unavailable"
    );
}

#[test]
fn replay_summary_returns_only_the_current_broker_summary_without_process_io() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    update_current_summary(&mut tools, "Current short summary A.", false);
    let summary_id = active_summary_id(&tools);
    let calls_before = executor.calls.lock().expect("calls lock").len();

    let replay = tools.dispatch("replay_summary", "{}", 2);

    assert_eq!(
        replay,
        serde_json::json!({
            "ok": true,
            "summary_id": summary_id,
            "spoken_text": "Current short summary A."
        })
    );
    assert_eq!(
        executor.calls.lock().expect("calls lock").len(),
        calls_before
    );
}

#[test]
fn replay_summary_rejects_every_nonempty_or_nonobject_argument() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    let calls_before = executor.calls.lock().expect("calls lock").len();

    for arguments in [
        "null",
        "[]",
        r#"{"summary":"caller supplied"}"#,
        r#"{"summary_id":"summary-stale"}"#,
        r#"{"destination":"thread-b"}"#,
        r#"{"thread":"thread-b"}"#,
        r#"{"text":"caller supplied"}"#,
    ] {
        assert_eq!(
            tools.dispatch("replay_summary", arguments, 2)["error"],
            "invalid_arguments",
            "arguments: {arguments}"
        );
    }
    assert_eq!(
        executor.calls.lock().expect("calls lock").len(),
        calls_before
    );
}

#[test]
fn replay_summary_without_an_active_context_fails_without_process_io() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);

    assert_eq!(
        tools.dispatch("replay_summary", "{}", 1),
        serde_json::json!({"ok": false, "error": "no_active_summary"})
    );
    assert!(executor.calls.lock().expect("calls lock").is_empty());
    assert!(!tools.has_pending_action());
    assert_eq!(tools.presentation_snapshot()["bound_context"], Value::Null);
}

#[test]
fn repeated_replay_preserves_pending_proposal_evidence_and_delivery_provenance() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("journal.sqlite3")).expect("journal");
    let mut tools = engine(&executor).with_journal(Arc::new(Mutex::new(journal)));
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    update_current_summary(&mut tools, "Replayable summary A.", false);
    let summary_id = active_summary_id(&tools);
    let proposal = tools.dispatch("send_message", r#"{"text":"response for A"}"#, 2);
    let token = proposal["proposal_token"]
        .as_str()
        .expect("proposal token")
        .to_owned();
    let calls_before = executor.calls.lock().expect("calls lock").len();

    let first = tools.dispatch("replay_summary", "{}", 3);
    let second = tools.dispatch("replay_summary", "{}", 4);

    assert_eq!(first, second);
    assert_eq!(first["summary_id"], summary_id);
    assert_eq!(first["spoken_text"], "Replayable summary A.");
    assert_eq!(tools.pending_action_token(), Some(token.as_str()));
    assert_eq!(
        tools.dispatch("list_operation_timeline", "{}", 5)["entries"],
        serde_json::json!([])
    );
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token":token}).to_string(),
            6,
        )["error"],
        "confirmation_rejected"
    );
    assert_eq!(tools.pending_action_token(), Some(token.as_str()));
    assert_eq!(
        executor.calls.lock().expect("calls lock").len(),
        calls_before
    );

    tools.observe_user_turn();
    let delivered = tools.dispatch(
        "confirm_action",
        &serde_json::json!({"token":token}).to_string(),
        7,
    );
    assert_eq!(delivered["delivery"], "delivered");
    assert_eq!(
        tools.dispatch(
            "get_operation_status",
            &serde_json::json!({"operation_id":token}).to_string(),
            8,
        )["summary_id"],
        summary_id
    );
    let calls = executor.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), calls_before + 1);
    assert_eq!(calls.last().expect("send process")[1], "thread-a");
}

#[test]
fn replay_uses_only_the_new_active_context_and_cannot_select_a_stale_summary() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    update_current_summary(&mut tools, "Stale summary A.", false);
    let stale_summary_id = active_summary_id(&tools);
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-b"}"#, 2)["respondable"],
        true
    );
    update_current_summary(&mut tools, "Current summary B.", false);
    let current_summary_id = active_summary_id(&tools);
    let calls_before = executor.calls.lock().expect("calls lock").len();

    let replay = tools.dispatch("replay_summary", "{}", 3);
    let stale_selection = tools.dispatch(
        "replay_summary",
        &serde_json::json!({"summary_id":stale_summary_id}).to_string(),
        4,
    );

    assert_ne!(current_summary_id, stale_summary_id);
    assert_eq!(replay["summary_id"], current_summary_id);
    assert_eq!(replay["spoken_text"], "Current summary B.");
    assert_eq!(stale_selection["error"], "invalid_arguments");
    assert_eq!(active_summary_id(&tools), current_summary_id);
    assert_eq!(
        executor.calls.lock().expect("calls lock").len(),
        calls_before
    );
}

#[test]
fn replay_after_successful_confirmation_has_no_active_summary() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    let proposal = tools.dispatch("send_message", r#"{"text":"response for A"}"#, 2);
    let token = proposal["proposal_token"].as_str().expect("proposal token");
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token":token}).to_string(),
            3,
        )["delivery"],
        "delivered"
    );
    let calls_before = executor.calls.lock().expect("calls lock").len();

    assert_eq!(
        tools.dispatch("replay_summary", "{}", 4),
        serde_json::json!({"ok":false,"error":"no_active_summary"})
    );
    assert_eq!(
        executor.calls.lock().expect("calls lock").len(),
        calls_before
    );
}

#[test]
fn replay_after_host_change_has_no_active_summary() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let host = |id: &str, default: bool| {
        PaseoHost::new(
            PaseoHostProfile {
                id: id.to_owned(),
                label: format!("{id} host"),
                target: None,
                default,
                default_cwd: "~/".to_owned(),
                default_provider: "opencode/gpt-5.6-sol-max".to_owned(),
            },
            Some(PaseoAdapter::new(
                &executor,
                "paseo".to_owned(),
                "password".to_owned(),
                None,
            )),
        )
    };
    let mut tools =
        ToolEngine::from_hosts(vec![host("first", true), host("second", false)], 120_000)
            .expect("valid hosts");
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    assert_eq!(tools.select_host("second")["selected_host_id"], "second");
    let calls_before = executor.calls.lock().expect("calls lock").len();

    assert_eq!(
        tools.dispatch("replay_summary", "{}", 2),
        serde_json::json!({"ok":false,"error":"no_active_summary"})
    );
    assert_eq!(
        executor.calls.lock().expect("calls lock").len(),
        calls_before
    );
}

#[test]
fn proposal_presentation_exposes_only_a_distinct_matching_handle() {
    let proposal = ProposalPresentation::new("proposal-safety-token", "Confirm this response.");
    let frame = proposal_frame(Some(&proposal));
    let handle = frame["handle"].as_str().expect("presentation handle");

    assert_eq!(frame["type"], "proposal");
    assert_eq!(frame["echo"], "Confirm this response.");
    assert_ne!(handle, "proposal-safety-token");
    assert!(!frame.to_string().contains("proposal-safety-token"));
    assert!(proposal.matches(&serde_json::json!({"handle":handle})));
    assert!(!proposal.matches(&serde_json::json!({"handle":"stale"})));
    assert!(!proposal.matches(&serde_json::json!({})));
    assert_eq!(proposal.token(), "proposal-safety-token");
    assert_eq!(
        proposal_frame(None),
        serde_json::json!({"type":"proposal","echo":null})
    );
}

#[test]
fn operation_timeline_rejects_invalid_cursor_wire_values() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    for sequence in 1..=2 {
        journal
            .append(&JournalEntry {
                operation_id: &format!("operation-{sequence}"),
                summary_id: "summary-target",
                destination_thread_id: "thread-target",
                response_sha256: "digest-only",
                state: "delivered",
                timestamp_ms: sequence,
                receiver_message_id: None,
            })
            .expect("append timeline entry");
    }
    let mut tools = engine(&executor).with_journal(Arc::new(Mutex::new(journal)));
    let first = tools.dispatch("list_operation_timeline", r#"{"limit":1}"#, 3);
    let cursor = first["next_cursor"].as_str().expect("real cursor");
    let wrong_version = mutate_timeline_cursor(cursor, |payload| payload[0] = 2);
    let zero_sequence = mutate_timeline_cursor(cursor, |payload| payload[33..41].fill(0));
    let out_of_range_sequence = mutate_timeline_cursor(cursor, |payload| {
        payload[33..41].copy_from_slice(&u64::MAX.to_be_bytes());
    });

    for invalid_cursor in [wrong_version, zero_sequence, out_of_range_sequence] {
        let arguments = serde_json::json!({"cursor": invalid_cursor}).to_string();
        assert_eq!(
            tools.dispatch("list_operation_timeline", &arguments, 4)["error"],
            "invalid_cursor"
        );
    }
    for arguments in [
        serde_json::json!({"cursor": 1}),
        serde_json::json!({"cursor": "x".repeat(77)}),
    ] {
        assert_eq!(
            tools.dispatch("list_operation_timeline", &arguments.to_string(), 4)["error"],
            "invalid_cursor"
        );
    }
}

#[test]
fn operation_timeline_filters_by_exact_summary_id() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    for (operation_id, summary_id, timestamp_ms) in [
        ("operation-a", "summary-target", 1),
        ("operation-b", "summary-other", 2),
    ] {
        journal
            .append(&JournalEntry {
                operation_id,
                summary_id,
                destination_thread_id: "thread-a",
                response_sha256: "digest-only",
                state: "delivered",
                timestamp_ms,
                receiver_message_id: None,
            })
            .expect("append timeline entry");
    }
    let mut tools = engine(&executor).with_journal(Arc::new(Mutex::new(journal)));

    let timeline = tools.dispatch(
        "list_operation_timeline",
        r#"{"summary_id":"summary-target"}"#,
        3,
    );

    assert_eq!(timeline["ok"], true);
    assert_eq!(timeline["entries"].as_array().expect("entries").len(), 1);
    assert_eq!(timeline["entries"][0]["operation_id"], "operation-a");
    assert_eq!(timeline["entries"][0]["summary_id"], "summary-target");
}

#[test]
fn operation_timeline_filters_are_conjunctive() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    for (operation_id, summary_id, thread_id, state, timestamp_ms) in [
        (
            "operation-match",
            "summary-target",
            "thread-target",
            "delivered",
            1,
        ),
        (
            "operation-other-thread",
            "summary-target",
            "thread-other",
            "delivered",
            2,
        ),
        (
            "operation-other-state",
            "summary-target",
            "thread-target",
            "rejected",
            3,
        ),
        (
            "operation-other-summary",
            "summary-other",
            "thread-target",
            "delivered",
            4,
        ),
    ] {
        journal
            .append(&JournalEntry {
                operation_id,
                summary_id,
                destination_thread_id: thread_id,
                response_sha256: "digest-only",
                state,
                timestamp_ms,
                receiver_message_id: None,
            })
            .expect("append timeline entry");
    }
    let mut tools = engine(&executor).with_journal(Arc::new(Mutex::new(journal)));

    let timeline = tools.dispatch(
        "list_operation_timeline",
        r#"{"state":"delivered","summary_id":"summary-target","destination_thread_id":"thread-target"}"#,
        5,
    );

    assert_eq!(timeline["ok"], true);
    assert_eq!(timeline["entries"].as_array().expect("entries").len(), 1);
    assert_eq!(timeline["entries"][0]["operation_id"], "operation-match");
}

#[test]
fn operation_timeline_cursor_pages_each_latest_state_once() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    for (operation_id, state, timestamp_ms) in [
        ("operation-hidden", "delivered", 1),
        ("operation-middle", "delivered", 2),
        ("operation-old", "dispatching", 3),
        ("operation-old", "delivered", 4),
        ("operation-hidden", "rejected", 5),
        ("operation-new", "delivered", 6),
    ] {
        journal
            .append(&JournalEntry {
                operation_id,
                summary_id: "summary-target",
                destination_thread_id: "thread-target",
                response_sha256: "digest-only",
                state,
                timestamp_ms,
                receiver_message_id: None,
            })
            .expect("append timeline transition");
    }
    let mut tools = engine(&executor).with_journal(Arc::new(Mutex::new(journal)));

    let first = tools.dispatch(
        "list_operation_timeline",
        r#"{"state":"delivered","limit":2}"#,
        7,
    );
    let cursor = first["next_cursor"].as_str().expect("another page cursor");
    let second_arguments = serde_json::json!({
        "state": "delivered",
        "limit": 2,
        "cursor": cursor
    })
    .to_string();
    let second = tools.dispatch("list_operation_timeline", &second_arguments, 8);

    let operation_ids = first["entries"]
        .as_array()
        .expect("first entries")
        .iter()
        .chain(second["entries"].as_array().expect("second entries"))
        .map(|entry| entry["operation_id"].as_str().expect("operation ID"))
        .collect::<Vec<_>>();
    assert_eq!(
        operation_ids,
        ["operation-new", "operation-old", "operation-middle"]
    );
    assert!(second.get("next_cursor").is_none());
}

#[test]
fn operation_timeline_cursor_keeps_an_inter_page_append_out_of_the_snapshot() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Arc::new(Mutex::new(
        Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal"),
    ));
    for (operation_id, state, timestamp_ms) in [
        ("operation-old", "delivered", 1),
        ("operation-changing", "dispatching", 2),
        ("operation-middle", "delivered", 3),
        ("operation-new", "delivered", 4),
    ] {
        journal
            .lock()
            .expect("journal lock")
            .append(&JournalEntry {
                operation_id,
                summary_id: "summary-target",
                destination_thread_id: "thread-target",
                response_sha256: "digest-only",
                state,
                timestamp_ms,
                receiver_message_id: None,
            })
            .expect("append timeline transition");
    }
    let mut tools = engine(&executor).with_journal(Arc::clone(&journal));
    let first = tools.dispatch("list_operation_timeline", r#"{"limit":2}"#, 5);
    let cursor = first["next_cursor"].as_str().expect("snapshot cursor");

    journal
        .lock()
        .expect("journal lock")
        .append(&JournalEntry {
            operation_id: "operation-changing",
            summary_id: "summary-target",
            destination_thread_id: "thread-target",
            response_sha256: "digest-only",
            state: "delivered",
            timestamp_ms: 6,
            receiver_message_id: Some("receiver-later"),
        })
        .expect("append later transition");
    let second_arguments = serde_json::json!({"limit": 2, "cursor": cursor}).to_string();
    let second = tools.dispatch("list_operation_timeline", &second_arguments, 7);

    assert_eq!(second["ok"], true);
    assert_eq!(second["entries"][0]["operation_id"], "operation-changing");
    assert_eq!(second["entries"][0]["state"], "dispatching");
    assert_eq!(second["entries"][0]["receiver_message_id"], Value::Null);
    assert_eq!(second["entries"][1]["operation_id"], "operation-old");
    assert!(second.get("next_cursor").is_none());
}

#[test]
fn operation_timeline_cursor_rejects_changed_filters() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    for sequence in 1..=3 {
        journal
            .append(&JournalEntry {
                operation_id: &format!("operation-{sequence}"),
                summary_id: "summary-target",
                destination_thread_id: "thread-target",
                response_sha256: "digest-only",
                state: "delivered",
                timestamp_ms: sequence,
                receiver_message_id: None,
            })
            .expect("append timeline entry");
    }
    let mut tools = engine(&executor).with_journal(Arc::new(Mutex::new(journal)));
    let first = tools.dispatch(
        "list_operation_timeline",
        r#"{"state":"delivered","summary_id":"summary-target","destination_thread_id":"thread-target","limit":1}"#,
        4,
    );
    let cursor = first["next_cursor"].as_str().expect("filter cursor");

    for arguments in [
        serde_json::json!({
            "state": "rejected",
            "summary_id": "summary-target",
            "destination_thread_id": "thread-target",
            "cursor": cursor
        }),
        serde_json::json!({
            "state": "delivered",
            "summary_id": "summary-other",
            "destination_thread_id": "thread-target",
            "cursor": cursor
        }),
        serde_json::json!({
            "state": "delivered",
            "summary_id": "summary-target",
            "destination_thread_id": "thread-other",
            "cursor": cursor
        }),
    ] {
        assert_eq!(
            tools.dispatch("list_operation_timeline", &arguments.to_string(), 5)["error"],
            "invalid_cursor",
            "arguments: {arguments}"
        );
    }
}

#[test]
fn operation_timeline_cursor_expires_when_retention_advances() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("timeline.sqlite3");
    let journal = Arc::new(Mutex::new(Journal::open(&path).expect("journal")));
    for sequence in 1..=3 {
        journal
            .lock()
            .expect("journal lock")
            .append(&JournalEntry {
                operation_id: &format!("operation-{sequence}"),
                summary_id: "summary-target",
                destination_thread_id: "thread-target",
                response_sha256: "digest-only",
                state: "delivered",
                timestamp_ms: sequence,
                receiver_message_id: None,
            })
            .expect("append timeline entry");
    }
    let mut tools = engine(&executor).with_journal(Arc::clone(&journal));
    let first = tools.dispatch("list_operation_timeline", r#"{"limit":1}"#, 4);
    let cursor = first["next_cursor"]
        .as_str()
        .expect("retention cursor")
        .to_owned();

    let mut raw = rusqlite::Connection::open(&path).expect("raw journal connection");
    let transaction = raw.transaction().expect("retention transaction");
    for sequence in 0..MAX_TRANSITIONS {
        transaction
            .execute(
                "INSERT INTO transitions
                 (operation_id, summary_id, destination_thread_id, response_sha256, state,
                  timestamp_ms, receiver_message_id)
                 VALUES (?, 'summary-new', 'thread-new', 'digest-only', 'delivered', 5, NULL)",
                [format!("retention-{sequence}")],
            )
            .expect("insert retention transition");
    }
    transaction.commit().expect("commit retention transitions");
    drop(raw);
    journal
        .lock()
        .expect("journal lock")
        .append(&JournalEntry {
            operation_id: "retention-newest",
            summary_id: "summary-new",
            destination_thread_id: "thread-new",
            response_sha256: "digest-only",
            state: "delivered",
            timestamp_ms: 6,
            receiver_message_id: None,
        })
        .expect("trigger retention prune");

    let arguments = serde_json::json!({"limit": 1, "cursor": cursor}).to_string();
    let continuation = tools.dispatch("list_operation_timeline", &arguments, 7);
    assert_eq!(continuation["error"], "cursor_expired");
    assert!(continuation.get("entries").is_none());
}

#[test]
fn operation_timeline_rejects_invalid_filters_fields_and_cursors() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("timeline.sqlite3")).expect("journal");
    let mut tools = engine(&executor).with_journal(Arc::new(Mutex::new(journal)));

    for (arguments, expected_error) in [
        (r#"{"cursor":"Af__________"}"#, "invalid_cursor"),
        (r#"{"cursor":"not-a-cursor"}"#, "invalid_cursor"),
        (r#"{"summary_id":" summary"}"#, "invalid_summary_id"),
        (
            r#"{"destination_thread_id":"thread\n"}"#,
            "invalid_destination_thread_id",
        ),
        (r#"{"state":"Delivered"}"#, "invalid_arguments"),
        (r#"{"limit":101}"#, "invalid_arguments"),
        (r#"{"search":"content"}"#, "invalid_arguments"),
    ] {
        assert_eq!(
            tools.dispatch("list_operation_timeline", arguments, 1)["error"],
            expected_error,
            "arguments: {arguments}"
        );
    }
}

#[test]
fn response_is_bound_to_the_reply_that_was_read_and_requires_a_later_turn() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("journal.sqlite3")).expect("journal");
    let mut tools = engine(&executor).with_journal(Arc::new(Mutex::new(journal)));
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("set_current_session", r#"{"session":"thread-a"}"#, 1)["ok"],
        true
    );
    assert_eq!(
        tools.dispatch("read_latest_reply", "{}", 2)["respondable"],
        true
    );
    let proposed = tools.dispatch("send_message", r#"{"text":"  exact response\r\n"}"#, 3);
    let token = proposed["proposal_token"]
        .as_str()
        .expect("proposal token")
        .to_owned();

    assert_eq!(
        tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 4)["error"],
        "confirmation_rejected"
    );
    assert!(
        executor
            .calls
            .lock()
            .expect("calls lock")
            .iter()
            .all(|args| args.first().map(String::as_str) != Some("send"))
    );

    tools.observe_user_turn();
    let confirmed = tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 5);
    assert_eq!(confirmed["delivery"], "delivered");
    let status = tools.dispatch(
        "get_operation_status",
        &format!(r#"{{"operation_id":"{token}"}}"#),
        6,
    );
    assert_eq!(status["state"], "delivered");
    assert_eq!(status["receiver_message_id"], "receiver-a");
    assert_eq!(status["destination_thread_id"], "thread-a");
    let timeline = tools.dispatch(
        "list_operation_timeline",
        r#"{"state":"delivered","limit":10}"#,
        7,
    );
    assert_eq!(timeline["entries"][0]["operation_id"], token);
    assert_eq!(timeline["entries"][0]["destination_thread_id"], "thread-a");
    let mut entry_fields = timeline["entries"][0]
        .as_object()
        .expect("timeline entry")
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    entry_fields.sort_unstable();
    assert_eq!(
        entry_fields,
        [
            "destination_thread_id",
            "operation_id",
            "receiver_message_id",
            "state",
            "summary_id",
            "timestamp_ms"
        ]
    );
    assert_eq!(
        tools.dispatch("list_operation_timeline", r#"{"response":"leak"}"#, 8,)["error"],
        "invalid_arguments"
    );
    let calls = executor.calls.lock().expect("calls lock");
    let send = calls
        .iter()
        .find(|args| args.first().map(String::as_str) == Some("send"))
        .expect("one send");
    assert_eq!(
        send,
        &[
            "send",
            "thread-a",
            "--prompt",
            "  exact response\r\n",
            "--no-wait",
            "--json"
        ]
    );
}

#[test]
fn expired_response_confirmation_clears_the_pending_mirror() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    let proposed = tools.dispatch("send_message", r#"{"text":"exact response"}"#, 2);
    let token = proposed["proposal_token"].as_str().expect("proposal token");
    tools.observe_user_turn();

    let expired = tools.dispatch(
        "confirm_action",
        &format!(r#"{{"token":"{token}"}}"#),
        120_002,
    );

    assert_eq!(expired["error"], "proposal_expired");
    assert!(!tools.has_pending_action());
    assert!(
        executor
            .calls
            .lock()
            .expect("calls lock")
            .iter()
            .all(|args| args.first().map(String::as_str) != Some("send"))
    );
}

#[test]
fn rereading_a_reply_preserves_its_summary_and_pending_proposal() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let first = tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1);
    assert_eq!(first["respondable"], true);
    update_current_summary(&mut tools, "Actionable summary for reply A.", false);
    let proposed = tools.dispatch("send_message", r#"{"text":"exact response"}"#, 2);
    let token = proposed["proposal_token"]
        .as_str()
        .expect("proposal token")
        .to_owned();

    let second = tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 3);

    assert_eq!(second["reason"], "reply_already_observed");
    assert!(second.get("spoken_text").is_none());
    assert_eq!(second["spoken_hint"], "That reply was already read.");
    assert!(
        second["spoken_hint"]
            .as_str()
            .expect("bounded status")
            .chars()
            .count()
            <= 128
    );
    assert!(!second.to_string().contains("exact reply A"));
    assert_eq!(
        tools.dashboard_state()["bound_context"]["latest_summary"],
        "Actionable summary for reply A."
    );
    assert!(tools.has_pending_action());

    tools.observe_user_turn();
    let confirmed = tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 4);
    assert_eq!(confirmed["delivery"], "delivered");
    let calls = executor.calls.lock().expect("calls lock");
    let send = calls
        .iter()
        .find(|args| args.first().map(String::as_str) == Some("send"))
        .expect("one send");
    assert_eq!(send[1], "thread-a");
}

#[test]
fn rereading_a_historical_duplicate_invalidates_the_active_reply_and_proposal() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-b"}"#, 2)["respondable"],
        true
    );
    update_current_summary(&mut tools, "Actionable summary for reply B.", false);
    let proposed = tools.dispatch("send_message", r#"{"text":"response for B"}"#, 3);
    let token = proposed["proposal_token"]
        .as_str()
        .expect("proposal token")
        .to_owned();

    let historical = tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 4);

    assert_eq!(historical["reason"], "reply_already_observed");
    assert_eq!(historical["respondable"], false);
    assert_eq!(tools.dashboard_state()["bound_context"], Value::Null);
    assert!(!tools.has_pending_action());
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 5)["error"],
        "not_pending"
    );
    assert!(
        executor
            .calls
            .lock()
            .expect("calls lock")
            .iter()
            .all(|args| args.first().map(String::as_str) != Some("send"))
    );
}

#[test]
fn duplicate_after_failed_journal_before_dispatch_does_not_preserve_stale_state() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let journal = Journal::open(&directory.path().join("journal.sqlite3")).expect("journal");
    let mut tools = ToolEngine::new(
        Some(PaseoAdapter::new(
            &executor,
            "paseo".to_owned(),
            "password".to_owned(),
            None,
        )),
        u64::MAX,
    )
    .with_journal(Arc::new(Mutex::new(journal)));
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 0)["respondable"],
        true
    );
    let proposed = tools.dispatch("send_message", r#"{"text":"exact response"}"#, 0);
    let token = proposed["proposal_token"]
        .as_str()
        .expect("proposal token")
        .to_owned();
    tools.observe_user_turn();
    let unjournalable_now = i64::MAX.unsigned_abs() + 1;
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &format!(r#"{{"token":"{token}"}}"#),
            unjournalable_now,
        )["error"],
        "journal_failed_before_dispatch"
    );

    let duplicate = tools.dispatch(
        "read_latest_reply",
        r#"{"session":"thread-a"}"#,
        unjournalable_now + 1,
    );

    assert_eq!(duplicate["reason"], "reply_already_observed");
    assert_eq!(duplicate["respondable"], false);
    assert_eq!(tools.dashboard_state()["bound_context"], Value::Null);
    assert!(!tools.has_pending_action());
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &format!(r#"{{"token":"{token}"}}"#),
            unjournalable_now + 2,
        )["error"],
        "not_pending"
    );
    assert!(
        executor
            .calls
            .lock()
            .expect("calls lock")
            .iter()
            .all(|args| args.first().map(String::as_str) != Some("send"))
    );
}

#[test]
fn identical_reply_on_another_host_is_actionable_and_routes_to_the_real_session() {
    struct HostRecordingExecutor {
        inner: FakeExecutor,
        send_host: Mutex<Option<String>>,
    }
    impl ProcessExecutor for &HostRecordingExecutor {
        fn execute(
            &self,
            program: &str,
            args: &[String],
            env: &HashMap<String, String>,
            timeout: Duration,
        ) -> ProcessOutput {
            if args.first().map(String::as_str) == Some("send") {
                *self.send_host.lock().expect("send host lock") = env.get("PASEO_HOST").cloned();
            }
            (&self.inner).execute(program, args, env, timeout)
        }
    }
    let executor = HostRecordingExecutor {
        inner: FakeExecutor {
            calls: Mutex::default(),
        },
        send_host: Mutex::default(),
    };
    let host = |id: &str, default: bool| {
        let target = format!("{id}.example:6767");
        PaseoHost::new(
            PaseoHostProfile {
                id: id.to_owned(),
                label: format!("{id} host"),
                target: Some(target.clone()),
                default,
                default_cwd: "~/".to_owned(),
                default_provider: "opencode/gpt-5.6-sol-max".to_owned(),
            },
            Some(PaseoAdapter::new(
                &executor,
                "paseo".to_owned(),
                "password".to_owned(),
                Some(target),
            )),
        )
    };
    let mut tools = ToolEngine::from_hosts(vec![host("alpha", true), host("beta", false)], 120_000)
        .expect("valid hosts");
    tools.observe_user_turn();
    let first = tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1);
    assert_eq!(first["respondable"], true);

    assert_eq!(tools.select_host("beta")["selected_host_id"], "beta");
    let second = tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 2);

    assert_eq!(second["respondable"], true);
    assert_eq!(second["session_id"], "thread-a");
    let proposed = tools.dispatch("send_message", r#"{"text":"exact response"}"#, 3);
    let token = proposed["proposal_token"].as_str().expect("proposal token");
    tools.observe_user_turn();
    let confirmed = tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 4);
    assert_eq!(confirmed["delivery"], "delivered");

    let calls = executor.inner.calls.lock().expect("calls lock");
    let send = calls
        .iter()
        .find(|args| args.first().map(String::as_str) == Some("send"))
        .expect("one send");
    assert_eq!(send[1], "thread-a");
    assert_eq!(
        executor
            .send_host
            .lock()
            .expect("send host lock")
            .as_deref(),
        Some("beta.example:6767")
    );
}

#[test]
fn dashboard_state_exposes_only_ephemeral_routing_cues_from_trusted_state() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let _ = tools.dispatch("set_current_session", r#"{"session":"thread-a"}"#, 1);
    let read = tools.dispatch("read_latest_reply", "{}", 2);
    assert_eq!(read["respondable"], true);
    update_current_summary(&mut tools, "Agent A completed the requested change.", false);

    let dashboard = tools.dashboard_state();
    assert_eq!(dashboard["selected_host_id"], "local");
    assert_eq!(dashboard["bound_context"]["thread_id"], "thread-a");
    assert_eq!(dashboard["bound_context"]["thread_name"], "Agent A");
    assert_eq!(
        dashboard["bound_context"]["latest_summary"],
        "Agent A completed the requested change."
    );
    assert_eq!(dashboard["bound_context"]["summary_degraded"], false);
    assert_eq!(dashboard["queue_count"], 0);
    assert_eq!(dashboard["agents"][0]["thread_id"], "thread-a");
    assert_eq!(dashboard["agents"][0]["provider"], "unknown");
    assert_eq!(dashboard["agents"][0]["state"], "idle");
    assert!(dashboard["agents"][0].get("raw").is_none());
    assert!(dashboard.get("target").is_none());

    let _ = tools.select_host("local");
    assert_eq!(
        tools.dashboard_state()["bound_context"]["thread_id"],
        "thread-a"
    );
}

#[test]
fn completed_summary_updates_only_the_context_that_started_it() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let first = tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1);
    assert_eq!(first["respondable"], true);
    let first_summary_id = tools.dashboard_state()["bound_context"]["summary_id"]
        .as_str()
        .expect("first summary ID")
        .to_owned();

    let second = tools.dispatch("read_latest_reply", r#"{"session":"thread-b"}"#, 2);
    assert_eq!(second["respondable"], true);
    let second_context = tools.dashboard_state()["bound_context"].clone();
    let second_summary_id = second_context["summary_id"]
        .as_str()
        .expect("second summary ID")
        .to_owned();

    assert!(!tools.update_active_summary_if_current(
        &first_summary_id,
        "Stale summary for reply A.",
        false,
    ));
    assert_eq!(tools.dashboard_state()["bound_context"], second_context);
    assert!(tools.update_active_summary_if_current(
        &second_summary_id,
        "Current summary for reply B.",
        false,
    ));
    assert_eq!(
        tools.dashboard_state()["bound_context"]["latest_summary"],
        "Current summary for reply B."
    );
}

#[test]
fn dashboard_state_bounds_untrusted_daemon_and_summariser_labels() {
    struct OversizedMetadataExecutor;
    impl ProcessExecutor for OversizedMetadataExecutor {
        fn execute(
            &self,
            _program: &str,
            args: &[String],
            _env: &HashMap<String, String>,
            _timeout: Duration,
        ) -> ProcessOutput {
            let stdout = match args.first().map(String::as_str) {
                Some("ls") => serde_json::json!([{
                    "id": "thread-a",
                    "shortId": "a",
                    "name": format!("Agent\u{0}{}", "n".repeat(500)),
                    "provider": "p".repeat(500),
                    "status": "s".repeat(500)
                }])
                .to_string(),
                Some("logs") => "reply".to_owned(),
                _ => String::new(),
            };
            ProcessOutput {
                exit_code: Some(0),
                stdout,
                stderr: String::new(),
                timed_out: false,
                spawn_failed: false,
            }
        }
    }
    let mut tools = ToolEngine::new(
        Some(PaseoAdapter::new(
            OversizedMetadataExecutor,
            "paseo".to_owned(),
            "password".to_owned(),
            None,
        )),
        120_000,
    );
    let _ = tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1);
    update_current_summary(&mut tools, &"summary ".repeat(500), false);

    let dashboard = tools.dashboard_state();
    let agent = &dashboard["agents"][0];
    assert!(
        agent["thread_name"]
            .as_str()
            .expect("thread name")
            .chars()
            .count()
            <= 128
    );
    assert!(
        agent["provider"]
            .as_str()
            .expect("provider")
            .chars()
            .count()
            <= 256
    );
    assert!(agent["state"].as_str().expect("state").chars().count() <= 64);
    assert!(
        !agent["thread_name"]
            .as_str()
            .expect("thread name")
            .chars()
            .any(char::is_control)
    );
    assert!(
        dashboard["bound_context"]["latest_summary"]
            .as_str()
            .expect("summary")
            .chars()
            .count()
            <= 600
    );
}

#[test]
fn changing_the_read_context_invalidates_the_previous_proposal() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let _ = tools.dispatch("set_current_session", r#"{"session":"thread-a"}"#, 1);
    let _ = tools.dispatch("read_latest_reply", "{}", 2);
    let old = tools.dispatch("send_message", r#"{"text":"old response"}"#, 3);
    let old_token = old["proposal_token"].as_str().expect("old token");

    let _ = tools.dispatch("set_current_session", r#"{"session":"thread-b"}"#, 4);
    tools.observe_user_turn();
    let result = tools.dispatch(
        "confirm_action",
        &format!(r#"{{"token":"{old_token}"}}"#),
        5,
    );
    assert_eq!(result["error"], "not_pending");
}

#[test]
fn changing_host_exposes_safe_metadata_and_invalidates_the_pending_action() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let host = |id: &str, default: bool| {
        let target = format!("{id}.example:6767");
        PaseoHost::new(
            PaseoHostProfile {
                id: id.to_owned(),
                label: format!("{id} host"),
                target: Some(target.clone()),
                default,
                default_cwd: "~/".to_owned(),
                default_provider: "opencode/gpt-5.6-sol-max".to_owned(),
            },
            Some(PaseoAdapter::new(
                &executor,
                "paseo".to_owned(),
                "password".to_owned(),
                Some(target),
            )),
        )
    };
    let mut tools = ToolEngine::from_hosts(vec![host("wsl", true), host("spark", false)], 120_000)
        .expect("valid hosts");
    tools.observe_user_turn();
    let _ = tools.dispatch("set_current_session", r#"{"session":"thread-a"}"#, 1);
    let _ = tools.dispatch("read_latest_reply", "{}", 2);
    let proposed = tools.dispatch("send_message", r#"{"text":"exact response"}"#, 3);
    let token = proposed["proposal_token"].as_str().expect("proposal token");

    let changed = tools.select_host("spark");
    assert_eq!(changed["selected_host_id"], "spark");
    assert!(changed.get("current_session").is_none());
    assert_eq!(
        tools.dispatch("list_sessions", "{}", 4)["current_session"],
        Value::Null
    );
    assert!(changed["hosts"][0].get("target").is_none());
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 4,)["error"],
        "not_pending"
    );
}

#[test]
fn create_session_uses_trusted_defaults_and_selects_only_a_validated_agent_id() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let profile = PaseoHostProfile {
        id: "wsl".to_owned(),
        label: "Paseo WSL Host".to_owned(),
        target: Some("paseo-wsl.example:6767".to_owned()),
        default: true,
        default_cwd: "~/".to_owned(),
        default_provider: "opencode/gpt-5.6-sol-max".to_owned(),
    };
    let mut tools = ToolEngine::from_hosts(
        vec![PaseoHost::new(
            profile,
            Some(PaseoAdapter::new(
                &executor,
                "paseo".to_owned(),
                "password".to_owned(),
                Some("paseo-wsl.example:6767".to_owned()),
            )),
        )],
        120_000,
    )
    .expect("valid host");
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch(
            "create_session",
            r#"{"prompt":"fix tests","provider":"other/model"}"#,
            9,
        )["error"],
        "invalid_arguments"
    );
    let proposed = tools.dispatch("create_session", r#"{"prompt":"fix tests"}"#, 10);
    let token = proposed["proposal_token"].as_str().expect("run token");
    let echo = proposed["spoken_echo"].as_str().expect("run readback");
    assert!(echo.contains("Paseo WSL Host"));
    assert!(echo.contains("~/"));
    assert!(echo.contains("opencode/gpt-5.6-sol-max"));
    assert_eq!(
        tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 11)["error"],
        "confirmation_rejected"
    );
    tools.observe_user_turn();
    let created = tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 12);
    assert_eq!(created["session_id"], "run-a");
    assert_eq!(
        tools.dispatch("list_sessions", "{}", 13)["current_session"]["id"],
        "run-a"
    );
    let calls = executor.calls.lock().expect("calls lock");
    assert!(
        calls
            .iter()
            .any(|args| { args == &["provider", "models", "opencode", "--json"] })
    );
    let run = calls
        .iter()
        .find(|args| args.first().map(String::as_str) == Some("run"))
        .expect("one run");
    assert_eq!(
        run,
        &[
            "run",
            "fix tests",
            "--detach",
            "--provider",
            "opencode/gpt-5.6-sol-max",
            "--cwd",
            "~/",
            "--json"
        ]
    );
}

#[test]
fn first_model_create_session_call_requires_a_later_task_interaction_without_process_io() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();

    let result = tools.dispatch_model_call(
        "create_session",
        "not JSON and never parsed as a task",
        None,
        1,
    );

    assert_eq!(
        result,
        serde_json::json!({
            "ok": false,
            "error": "session_task_required",
            "spoken_hint": "What should the new session work on?"
        })
    );
    assert!(!tools.has_pending_action());
    assert_eq!(tools.presentation_snapshot()["bound_context"], Value::Null);
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn repeated_model_create_session_calls_in_the_same_interaction_remain_non_proposing() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let first = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"first invented task"}"#,
        None,
        1,
    );

    let repeated = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"second invented task"}"#,
        None,
        2,
    );

    assert_eq!(repeated, first);
    assert!(!tools.has_pending_action());
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn later_model_create_session_call_proposes_only_its_exact_prompt_and_trusted_defaults_once() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let _ = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"discard this model prompt"}"#,
        None,
        1,
    );
    tools.observe_user_turn();

    let proposed = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"use only this later task"}"#,
        None,
        2,
    );

    assert_eq!(proposed["ok"], true);
    assert_eq!(proposed["proposed"], true);
    assert_eq!(proposed["executed"], false);
    assert_eq!(proposed["host_id"], "local");
    assert_eq!(
        proposed["spoken_echo"],
        "Create a new session on Local Paseo in ~/ with opencode/gpt-5.6-sol-max: \"use only this later task\". Confirm?"
    );
    let token = proposed["proposal_token"]
        .as_str()
        .expect("proposal token")
        .to_owned();
    let repeated = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"must not replace the later task"}"#,
        None,
        3,
    );
    assert_eq!(repeated["error"], "session_task_required");
    assert_eq!(tools.pending_action_token(), Some(token.as_str()));

    let calls = executor.calls.lock().expect("calls lock");
    assert_eq!(
        calls
            .iter()
            .filter(|args| args.first().map(String::as_str) == Some("provider"))
            .count(),
        1
    );
    assert!(
        calls
            .iter()
            .all(|args| args.first().map(String::as_str) != Some("run"))
    );
}

#[test]
fn host_change_clears_model_session_task_collection() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let host = |id: &str, default: bool| {
        PaseoHost::new(
            PaseoHostProfile {
                id: id.to_owned(),
                label: format!("{id} host"),
                target: None,
                default,
                default_cwd: "~/".to_owned(),
                default_provider: "opencode/gpt-5.6-sol-max".to_owned(),
            },
            Some(PaseoAdapter::new(
                &executor,
                "paseo".to_owned(),
                "password".to_owned(),
                None,
            )),
        )
    };
    let mut tools =
        ToolEngine::from_hosts(vec![host("first", true), host("second", false)], 120_000)
            .expect("valid hosts");
    tools.observe_user_turn();
    let _ = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"discard before host change"}"#,
        None,
        1,
    );

    assert_eq!(tools.select_host("second")["selected_host_id"], "second");
    executor.calls.lock().expect("calls lock").clear();
    tools.observe_user_turn();
    let result = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"must begin collection on second host"}"#,
        None,
        2,
    );

    assert_eq!(result["error"], "session_task_required");
    assert!(!tools.has_pending_action());
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn cancellation_clears_model_session_task_collection() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let _ = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"discard before cancellation"}"#,
        None,
        1,
    );

    assert_eq!(
        tools.dispatch("cancel_action", "{}", 2),
        serde_json::json!({"ok":true,"cancelled":false})
    );
    tools.observe_user_turn();
    let result = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"must begin collection again"}"#,
        None,
        3,
    );

    assert_eq!(result["error"], "session_task_required");
    assert!(!tools.has_pending_action());
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn failed_later_model_session_proposal_clears_task_collection() {
    let mut tools = ToolEngine::<&FakeExecutor>::new(None, 120_000);
    tools.observe_user_turn();
    let _ = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"discarded first prompt"}"#,
        None,
        1,
    );
    tools.observe_user_turn();

    assert_eq!(
        tools.dispatch_model_call(
            "create_session",
            r#"{"prompt":"later task with unavailable provider"}"#,
            None,
            2,
        )["error"],
        "paseo_unavailable"
    );
    let retry = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"same-interaction retry"}"#,
        None,
        3,
    );
    assert_eq!(retry["error"], "session_task_required");
    assert!(!tools.has_pending_action());
}

#[test]
fn model_session_task_turn_cannot_confirm_and_later_confirmation_executes_exactly_once() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let _ = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"discarded first-interaction prompt"}"#,
        None,
        1,
    );
    tools.observe_user_turn();
    let proposed = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"exact trusted task"}"#,
        None,
        2,
    );
    let token = proposed["proposal_token"]
        .as_str()
        .expect("proposal token")
        .to_owned();

    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token":token}).to_string(),
            3,
        )["error"],
        "confirmation_rejected"
    );
    assert_eq!(
        executor
            .calls
            .lock()
            .expect("calls lock")
            .iter()
            .filter(|args| args.first().map(String::as_str) == Some("run"))
            .count(),
        0
    );

    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token":token}).to_string(),
            4,
        )["session_id"],
        "run-a"
    );
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token":token}).to_string(),
            5,
        )["error"],
        "not_pending"
    );
    let calls = executor.calls.lock().expect("calls lock");
    let runs = calls
        .iter()
        .filter(|args| args.first().map(String::as_str) == Some("run"))
        .collect::<Vec<_>>();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0][1], "exact trusted task");
    assert!(!runs[0].iter().any(|arg| arg.contains("discarded")));
}

#[test]
fn successful_session_confirmation_clears_reopened_task_collection() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let _ = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"discarded intent prompt"}"#,
        None,
        1,
    );
    tools.observe_user_turn();
    let proposed =
        tools.dispatch_model_call("create_session", r#"{"prompt":"confirmed task"}"#, None, 2);
    let token = proposed["proposal_token"]
        .as_str()
        .expect("proposal token")
        .to_owned();
    assert_eq!(
        tools.dispatch_model_call(
            "create_session",
            r#"{"prompt":"same-response retry"}"#,
            None,
            3,
        )["error"],
        "session_task_required"
    );
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token":token}).to_string(),
            4,
        )["session_id"],
        "run-a"
    );
    let calls_before = executor.calls.lock().expect("calls lock").len();

    tools.observe_user_turn();
    let next_intent = tools.dispatch_model_call(
        "create_session",
        r#"{"prompt":"must be discarded after confirmation"}"#,
        None,
        5,
    );

    assert_eq!(next_intent["error"], "session_task_required");
    assert!(!tools.has_pending_action());
    assert_eq!(
        executor.calls.lock().expect("calls lock").len(),
        calls_before
    );
}

#[test]
fn successful_session_creation_invalidates_the_previous_summary_and_response() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"session":"thread-a"}"#, 1)["respondable"],
        true
    );
    let old_proposal = tools.dispatch("send_message", r#"{"text":"response for A"}"#, 2);
    let old_token = old_proposal["proposal_token"]
        .as_str()
        .expect("old response token")
        .to_owned();
    let run = tools.dispatch("create_session", r#"{"prompt":"replace A"}"#, 3);
    let run_token = run["proposal_token"]
        .as_str()
        .expect("run token")
        .to_owned();
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token":run_token}).to_string(),
            4,
        )["session_id"],
        "run-a"
    );

    assert_eq!(tools.presentation_snapshot()["bound_context"], Value::Null);
    assert_eq!(
        tools.dispatch("replay_summary", "{}", 5)["error"],
        "no_active_summary"
    );
    assert_eq!(
        tools.dispatch("send_message", r#"{"text":"late response for A"}"#, 6)["error"],
        "no_active_reply"
    );
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch(
            "confirm_action",
            &serde_json::json!({"token":old_token}).to_string(),
            7,
        )["error"],
        "not_pending"
    );
    assert_eq!(
        executor
            .calls
            .lock()
            .expect("calls lock")
            .iter()
            .filter(|args| args.first().map(String::as_str) == Some("send"))
            .count(),
        0
    );
}

#[test]
fn create_session_does_not_select_or_retry_an_ambiguous_result() {
    struct AmbiguousRunExecutor(Mutex<Vec<Vec<String>>>);
    impl ProcessExecutor for &AmbiguousRunExecutor {
        fn execute(
            &self,
            _program: &str,
            args: &[String],
            _env: &HashMap<String, String>,
            _timeout: Duration,
        ) -> ProcessOutput {
            self.0.lock().expect("calls lock").push(args.to_vec());
            let stdout = match args.first().map(String::as_str) {
                Some("provider") => r#"[{"id":"gpt-5.6-sol-max"}]"#,
                Some("run") => r#"{"id":"unrelated-id"}"#,
                Some("ls") => "[]",
                _ => "",
            };
            ProcessOutput {
                exit_code: Some(0),
                stdout: stdout.to_owned(),
                stderr: String::new(),
                timed_out: false,
                spawn_failed: false,
            }
        }
    }
    let executor = AmbiguousRunExecutor(Mutex::default());
    let mut tools = ToolEngine::new(
        Some(PaseoAdapter::new(
            &executor,
            "paseo".to_owned(),
            "password".to_owned(),
            None,
        )),
        120_000,
    );
    tools.observe_user_turn();
    let proposed = tools.dispatch("create_session", r#"{"prompt":"fix tests"}"#, 1);
    let token = proposed["proposal_token"].as_str().expect("proposal token");
    tools.observe_user_turn();
    let result = tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 2);

    assert_eq!(result["delivery"], "outcome_unknown");
    assert_eq!(result["sessions_refreshed"], true);
    assert_eq!(
        executor
            .0
            .lock()
            .expect("calls lock")
            .iter()
            .filter(|args| args.first().map(String::as_str) == Some("ls"))
            .count(),
        1
    );
    assert_eq!(
        tools.dispatch("list_sessions", "{}", 3)["current_session"],
        Value::Null
    );
    assert_eq!(
        executor
            .0
            .lock()
            .expect("calls lock")
            .iter()
            .filter(|args| args.first().map(String::as_str) == Some("run"))
            .count(),
        1
    );
}

#[test]
fn read_modes_and_archived_listing_preserve_the_read_only_interface() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();

    assert_eq!(
        tools.dispatch("list_sessions", r#"{"include_archived":true}"#, 1)["ok"],
        true
    );
    let read = tools.dispatch(
        "read_latest_reply",
        r#"{"session":"thread-a","mode":"full"}"#,
        2,
    );
    assert_eq!(read["mode"], "full");
    assert_eq!(
        tools.dispatch("read_latest_reply", r#"{"mode":"invalid"}"#, 3)["error"],
        "invalid_arguments"
    );

    let calls = executor.calls.lock().expect("calls lock");
    assert!(
        calls
            .iter()
            .any(|args| { args == &["ls", "-a", "-g", "--json"] })
    );
    assert!(
        calls
            .iter()
            .any(|args| { args == &["logs", "thread-a", "--tail", "2", "--filter", "text",] })
    );
}

#[test]
fn shared_text_commands_treat_destination_like_syntax_as_exact_response_text() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    let mut commands = CommandState::default();
    tools.observe_user_turn();
    let _ = commands.run(&mut tools, "use thread-a", 1);
    let _ = commands.run(&mut tools, "read", 2);

    let proposed = commands.run(&mut tools, "send thread-b = exact response", 3);
    assert!(proposed.speech.contains("thread-b = exact response"));
    tools.observe_user_turn();
    assert_eq!(commands.run(&mut tools, "yes", 4).speech, "Done.");
    let calls = executor.calls.lock().expect("calls lock");
    let send = calls
        .iter()
        .find(|args| args.first().map(String::as_str) == Some("send"))
        .expect("one send");
    assert_eq!(send[1], "thread-a");
    assert_eq!(send[3], "thread-b = exact response");
}

#[test]
fn bare_new_session_command_collects_the_task_before_proposing() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    let mut commands = CommandState::default();
    tools.observe_user_turn();

    let question = commands.run(&mut tools, "new session", 1);
    assert_eq!(question.speech, "What should the new session work on?");
    assert!(question.proposal.is_none());
    assert!(
        executor
            .calls
            .lock()
            .expect("calls lock")
            .iter()
            .all(|args| args.first().map(String::as_str) != Some("run"))
    );

    tools.observe_user_turn();
    let proposed = commands.run(&mut tools, "fix the failing build", 2);
    assert!(proposed.proposal.is_some());
    assert!(proposed.speech.contains("fix the failing build"));
}

#[test]
fn session_task_cancellation_commands_clear_local_and_tool_collection_before_task_dispatch() {
    for cancellation in ["no", "n", "cancel"] {
        let executor = FakeExecutor {
            calls: Mutex::default(),
        };
        let mut tools = engine(&executor);
        let mut commands = CommandState::default();
        tools.observe_user_turn();
        assert_eq!(
            commands.run(&mut tools, "new session", 1).speech,
            "What should the new session work on?"
        );
        assert_eq!(
            tools.dispatch_model_call(
                "create_session",
                r#"{"prompt":"discarded intent prompt"}"#,
                None,
                1,
            )["error"],
            "session_task_required"
        );
        tools.observe_user_turn();

        let cancelled = commands.run(&mut tools, cancellation, 2);

        assert_eq!(cancelled.speech, "Done.", "command: {cancellation}");
        assert!(cancelled.proposal.is_none(), "command: {cancellation}");
        assert!(!tools.has_pending_action(), "command: {cancellation}");
        assert!(
            executor.calls.lock().expect("calls lock").is_empty(),
            "command: {cancellation}"
        );

        tools.observe_user_turn();
        assert_eq!(
            tools.dispatch_model_call(
                "create_session",
                r#"{"prompt":"fresh intent must still be discarded"}"#,
                None,
                3,
            )["error"],
            "session_task_required",
            "command: {cancellation}"
        );
        let _ = tools.dispatch("cancel_action", "{}", 3);
        assert_eq!(
            commands.run(&mut tools, "new session", 4).speech,
            "What should the new session work on?",
            "command: {cancellation}"
        );
        tools.observe_user_turn();
        let proposed = commands.run(&mut tools, "fresh task", 5);
        assert!(proposed.proposal.is_some(), "command: {cancellation}");
        assert!(
            proposed.speech.contains("fresh task"),
            "command: {cancellation}"
        );
    }
}

#[test]
fn trusted_direct_new_prompt_command_still_proposes_immediately() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    let mut commands = CommandState::default();
    tools.observe_user_turn();

    let proposed = commands.run(&mut tools, "new trusted direct task", 1);

    assert!(proposed.proposal.is_some());
    assert!(proposed.speech.contains("\"trusted direct task\""));
    assert!(!proposed.speech.contains("\"new trusted direct task\""));
    assert_eq!(
        executor
            .calls
            .lock()
            .expect("calls lock")
            .iter()
            .filter(|args| args.first().map(String::as_str) == Some("provider"))
            .count(),
        1
    );
}

#[test]
fn host_change_clears_an_unfinished_new_session_task() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    let mut commands = CommandState::default();
    tools.observe_user_turn();

    let question = commands.run(&mut tools, "new session", 1);
    assert_eq!(question.speech, "What should the new session work on?");
    commands.clear_pending();

    tools.observe_user_turn();
    let next = commands.run(&mut tools, "fix the failing build", 2);
    assert!(next.speech.starts_with("Unrecognised command."));
    assert!(next.proposal.is_none());
}
