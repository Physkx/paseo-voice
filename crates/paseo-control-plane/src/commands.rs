//! Deterministic text commands shared by mock mode and the terminal.

use serde_json::{Value, json};

use crate::{paseo::ProcessExecutor, tools::ToolEngine};

const HELP: &str = "Commands: ls, use SESSION, read [SESSION], full [SESSION], perms, send TEXT, run PROMPT, yes, no.";

/// Speakable command output and optional proposal readback.
pub struct CommandOutput {
    /// Text to display or speak.
    pub speech: String,
    /// Exact proposal readback when a proposal was created.
    pub proposal: Option<String>,
}

/// Per-connection token state for deterministic commands.
#[derive(Default)]
pub struct CommandState {
    last_token: Option<String>,
}

impl CommandState {
    /// Interpret one text turn through the same tool engine as Realtime.
    pub fn run<E: ProcessExecutor>(
        &mut self,
        tools: &mut ToolEngine<E>,
        input: &str,
        now_ms: u64,
    ) -> CommandOutput {
        let line = input.trim();
        let lower = line.to_lowercase();
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
        } else if lower.starts_with("run ") {
            tools.dispatch(
                "start_run",
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
        } else if matches!(lower.as_str(), "no" | "n" | "cancel") {
            self.last_token = None;
            tools.dispatch("cancel_action", "{}", now_ms)
        } else if line.is_empty() || matches!(lower.as_str(), "help" | "?") {
            return output(HELP);
        } else {
            return output(&format!("Unrecognised command. {HELP}"));
        };
        if let Some(token) = result.get("proposal_token").and_then(Value::as_str) {
            self.last_token = Some(token.to_owned());
        }
        let proposal = result
            .get("spoken_echo")
            .and_then(Value::as_str)
            .map(str::to_owned);
        CommandOutput {
            speech: speakable_result(&result),
            proposal,
        }
    }
}

fn speakable_result(result: &Value) -> String {
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        for key in ["spoken_text", "spoken_echo", "spoken_hint"] {
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
    }
}
