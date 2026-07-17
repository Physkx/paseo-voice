use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use paseo_control_plane::{
    commands::CommandState,
    journal::Journal,
    paseo::{PaseoAdapter, ProcessExecutor, ProcessOutput},
    tools::ToolEngine,
};

struct FakeExecutor {
    calls: Mutex<Vec<Vec<String>>>,
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
            Some("run") => r#"{"id":"run-a"}"#,
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
fn starting_a_run_is_also_single_use_and_requires_a_later_turn() {
    let executor = FakeExecutor {
        calls: Mutex::default(),
    };
    let mut tools = engine(&executor);
    tools.observe_user_turn();
    let proposed = tools.dispatch(
        "start_run",
        r#"{"prompt":"fix tests","provider":"codex","title":"Test fix"}"#,
        10,
    );
    let token = proposed["proposal_token"].as_str().expect("run token");
    let echo = proposed["spoken_echo"].as_str().expect("run readback");
    assert!(echo.contains("provider codex"));
    assert!(echo.contains("titled Test fix"));
    assert_eq!(
        tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 11)["error"],
        "confirmation_rejected"
    );
    tools.observe_user_turn();
    assert_eq!(
        tools.dispatch("confirm_action", &format!(r#"{{"token":"{token}"}}"#), 12)["delivery"],
        "delivered"
    );
    let calls = executor.calls.lock().expect("calls lock");
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
            "codex",
            "--title",
            "Test fix",
            "--json"
        ]
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
