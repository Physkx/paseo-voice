//! Deterministic text commands shared by mock mode and the terminal.

use serde_json::{Value, json};

use crate::{paseo::ProcessExecutor, tools::ToolEngine};

const HELP: &str = "Commands: ls, use SESSION, read [SESSION], full [SESSION], perms, send TEXT, new PROMPT, yes, no.";

/// Speakable command output and optional proposal readback.
pub struct CommandOutput {
    /// Text to display or speak.
    pub speech: String,
    /// Exact proposal readback when a proposal was created.
    pub proposal: Option<String>,
    refresh_dashboard: bool,
}

/// Per-connection token state for deterministic commands.
#[derive(Default)]
pub struct CommandState {
    last_token: Option<String>,
    awaiting_session_task: bool,
}

impl CommandState {
    /// Drop any browser-visible proposal token after a host context change.
    pub fn clear_pending(&mut self) {
        self.last_token = None;
        self.awaiting_session_task = false;
    }

    /// Interpret one text turn through the same tool engine as Realtime.
    pub fn run<E: ProcessExecutor>(
        &mut self,
        tools: &mut ToolEngine<E>,
        input: &str,
        now_ms: u64,
    ) -> CommandOutput {
        let line = input.trim();
        let lower = line.to_lowercase();
        if matches!(lower.as_str(), "no" | "n" | "cancel") {
            let cancelling_session_task = self.awaiting_session_task;
            self.last_token = None;
            self.awaiting_session_task = false;
            let result = tools.dispatch("cancel_action", "{}", now_ms);
            let mut output = self.finish(&result);
            if cancelling_session_task {
                output.refresh_dashboard = false;
            }
            return output;
        }
        if self.awaiting_session_task {
            if line.is_empty() {
                return output("What should the new session work on?");
            }
            self.awaiting_session_task = false;
            let result = tools.dispatch(
                "create_session",
                &json!({"prompt":line}).to_string(),
                now_ms,
            );
            return self.finish(&result);
        }
        if lower == "new session" {
            self.awaiting_session_task = true;
            let result = tools.dispatch("create_session", "{}", now_ms);
            if result.get("error").and_then(Value::as_str) == Some("session_task_required") {
                return self.finish(&result);
            }
            return output("What should the new session work on?");
        }
        let result = if matches!(lower.as_str(), "ls" | "list" | "sessions") {
            tools.dispatch("list_sessions", "{}", now_ms)
        } else if lower.starts_with("use ") {
            tools.dispatch(
                "set_current_session",
                &json!({"session":line[4..].trim()}).to_string(),
                now_ms,
            )
        } else if matches!(lower.as_str(), "read" | "full")
            || lower.starts_with("read ")
            || lower.starts_with("full ")
        {
            let full = lower == "full" || lower.starts_with("full ");
            let session = line.get(4..).unwrap_or_default().trim();
            let args = match (session.is_empty(), full) {
                (true, false) => json!({}),
                (false, false) => json!({"session":session}),
                (true, true) => json!({"mode":"full"}),
                (false, true) => json!({"session":session,"mode":"full"}),
            };
            tools.dispatch("read_latest_reply", &args.to_string(), now_ms)
        } else if matches!(lower.as_str(), "perms" | "permissions") {
            tools.dispatch("list_pending_permissions", "{}", now_ms)
        } else if lower.starts_with("send ") {
            let text = line[5..].trim();
            tools.dispatch("send_message", &json!({"text":text}).to_string(), now_ms)
        } else if lower.starts_with("new ") || lower.starts_with("run ") {
            tools.dispatch(
                "create_session",
                &json!({"prompt":line[4..].trim()}).to_string(),
                now_ms,
            )
        } else if matches!(lower.as_str(), "yes" | "y" | "confirm") {
            let Some(token) = self.last_token.take() else {
                return output("Nothing to confirm.");
            };
            tools.dispatch(
                "confirm_action",
                &json!({"token":token}).to_string(),
                now_ms,
            )
        } else if line.is_empty() || matches!(lower.as_str(), "help" | "?") {
            return output(HELP);
        } else {
            return output(&format!("Unrecognised command. {HELP}"));
        };
        self.finish(&result)
    }

    fn finish(&mut self, result: &Value) -> CommandOutput {
        if let Some(token) = result.get("proposal_token").and_then(Value::as_str) {
            self.last_token = Some(token.to_owned());
        }
        let session_task_required =
            result.get("error").and_then(Value::as_str) == Some("session_task_required");
        if session_task_required {
            self.awaiting_session_task = true;
        }
        let proposal = result
            .get("spoken_echo")
            .and_then(Value::as_str)
            .map(str::to_owned);
        CommandOutput {
            speech: speakable_result(result),
            proposal,
            refresh_dashboard: !session_task_required,
        }
    }
}

impl CommandOutput {
    /// Report whether this result needs a fresh Paseo dashboard probe.
    #[must_use]
    pub(crate) const fn refresh_dashboard(&self) -> bool {
        self.refresh_dashboard
    }
}

fn speakable_result(result: &Value) -> String {
    if let Some(hint) = result.get("spoken_hint").and_then(Value::as_str) {
        return hint.to_owned();
    }
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        for key in ["spoken_text", "spoken_echo"] {
            if let Some(text) = result.get(key).and_then(Value::as_str) {
                return text.to_owned();
            }
        }
        if result.get("cancelled").and_then(Value::as_bool) == Some(true) {
            return "Cancelled.".to_owned();
        }
        "Done.".to_owned()
    } else {
        format!(
            "Error: {}.",
            result
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        )
    }
}

fn output(speech: &str) -> CommandOutput {
    CommandOutput {
        speech: speech.to_owned(),
        proposal: None,
        refresh_dashboard: true,
    }
}
