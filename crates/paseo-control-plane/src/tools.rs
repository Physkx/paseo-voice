//! Provenance-bound voice tool dispatcher.

use std::{
    fmt::Write as _,
    sync::{Arc, Mutex},
};

use paseo_safety_core::{
    Applied, Command, DeliveryOutcome, InteractionSequence, ProposalId, ReplyId, ResponseBody,
    SafetyCore, SummaryId, ThreadId,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    journal::{Journal, JournalEntry},
    paseo::{PaseoAdapter, ProcessExecutor, RunAuthorization, WriteResult},
};

/// Realtime tool definitions. Mutating tools cannot accept a destination.
#[must_use]
pub fn definitions() -> Value {
    json!([
        {
            "type": "function", "name": "list_sessions",
            "description": "List Paseo coding-agent sessions.",
            "parameters": {
                "type": "object",
                "properties": {"include_archived": {"type": "boolean"}},
                "additionalProperties": false
            }
        },
        {
            "type": "function", "name": "set_current_session",
            "description": "Select a session for the next read.",
            "parameters": {
                "type": "object",
                "properties": {
                    "session": {"type": "string"},
                    "mode": {"type": "string", "enum": ["summary", "full"]}
                },
                "required": ["session"], "additionalProperties": false
            }
        },
        {
            "type": "function", "name": "read_latest_reply",
            "description": "Read and bind the latest reply from a session for a possible response.",
            "parameters": {
                "type": "object",
                "properties": {"session": {"type": "string"}},
                "additionalProperties": false
            }
        },
        {
            "type": "function", "name": "list_pending_permissions",
            "description": "Narrate pending permissions. Approval is never available.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": false}
        },
        {
            "type": "function", "name": "get_operation_status",
            "description": "Read the latest durable delivery state by opaque proposal token.",
            "parameters": {
                "type": "object",
                "properties": {"operation_id": {"type": "string"}},
                "required": ["operation_id"], "additionalProperties": false
            }
        },
        {
            "type": "function", "name": "send_message",
            "description": "Propose a response to the reply most recently read. This never sends.",
            "parameters": {
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"], "additionalProperties": false
            }
        },
        {
            "type": "function", "name": "start_run",
            "description": "Propose a new detached coding-agent run. This never starts it.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "provider": {"type": "string"},
                    "cwd": {"type": "string"},
                    "title": {"type": "string"}
                },
                "required": ["prompt"], "additionalProperties": false
            }
        },
        {
            "type": "function", "name": "confirm_action",
            "description": "Confirm the pending response after a later explicit user turn.",
            "parameters": {
                "type": "object",
                "properties": {"token": {"type": "string"}},
                "required": ["token"], "additionalProperties": false
            }
        },
        {
            "type": "function", "name": "cancel_action",
            "description": "Cancel the pending response.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": false}
        }
    ])
}

#[derive(Clone, Debug)]
struct Session {
    id: String,
    short_id: String,
    name: String,
    raw: Value,
}

#[derive(Clone, Debug)]
struct ActiveContext {
    summary_id: SummaryId,
    title: String,
}

#[derive(Clone, Debug)]
struct PendingRun {
    proposal_id: ProposalId,
    prompt: String,
    provider: Option<String>,
    cwd: Option<String>,
    title: Option<String>,
    created_interaction: InteractionSequence,
    expires_at_ms: u64,
}

/// Per-browser owner of selection, provenance, and confirmation state.
pub struct ToolEngine<E> {
    paseo: Option<PaseoAdapter<E>>,
    safety: SafetyCore,
    proposal_ttl_ms: u64,
    interaction: InteractionSequence,
    selected: Option<(String, String)>,
    active: Option<ActiveContext>,
    pending: Option<ProposalId>,
    pending_run: Option<PendingRun>,
    journal: Option<Arc<Mutex<Journal>>>,
    log_tail_entries: usize,
}

impl<E: ProcessExecutor> ToolEngine<E> {
    /// Construct a dispatcher. None keeps tools safely unavailable.
    #[must_use]
    pub fn new(paseo: Option<PaseoAdapter<E>>, proposal_ttl_ms: u64) -> Self {
        Self {
            paseo,
            safety: SafetyCore::new(proposal_ttl_ms),
            proposal_ttl_ms,
            interaction: InteractionSequence::new(0),
            selected: None,
            active: None,
            pending: None,
            pending_run: None,
            journal: None,
            log_tail_entries: 40,
        }
    }

    /// Attach the shared content-free transition journal.
    #[must_use]
    pub fn with_journal(mut self, journal: Arc<Mutex<Journal>>) -> Self {
        self.journal = Some(journal);
        self
    }

    /// Override the bounded number of Paseo log entries read per request.
    #[must_use]
    pub fn with_log_tail_entries(mut self, entries: usize) -> Self {
        self.log_tail_entries = entries.clamp(1, 200);
        self
    }

    /// Record one trusted browser-observed user turn.
    pub fn observe_user_turn(&mut self) {
        self.interaction = InteractionSequence::new(self.interaction.value().saturating_add(1));
    }

    /// Dispatch one strict tool call. Errors are returned as bounded JSON.
    #[must_use]
    pub fn dispatch(&mut self, name: &str, arguments: &str, now_ms: u64) -> Value {
        let parsed = serde_json::from_str::<Value>(arguments);
        let Ok(args) = parsed else {
            return error("invalid_arguments");
        };
        match name {
            "list_sessions" => self.list_sessions(&args),
            "set_current_session" => self.set_current_session(&args),
            "read_latest_reply" => self.read_latest_reply(&args, now_ms),
            "list_pending_permissions" => self.list_pending_permissions(),
            "get_operation_status" => self.operation_status(&args),
            "send_message" => self.propose_response(&args, now_ms),
            "start_run" => self.propose_run(&args, now_ms),
            "confirm_action" => self.confirm_response(&args, now_ms),
            "cancel_action" => self.cancel_response(),
            _ => error("unknown_tool"),
        }
    }

    fn list_sessions(&self, args: &Value) -> Value {
        let Some(object) = args.as_object() else {
            return error("invalid_arguments");
        };
        if object.keys().any(|key| key != "include_archived") {
            return error("invalid_arguments");
        }
        let include_archived = match object.get("include_archived") {
            None => false,
            Some(Value::Bool(value)) => *value,
            Some(_) => return error("invalid_arguments"),
        };
        let Some(sessions) = self.sessions(include_archived) else {
            return error("paseo_unavailable");
        };
        let spoken_hint = if sessions.is_empty() {
            "No sessions.".to_owned()
        } else {
            let names = sessions
                .iter()
                .take(8)
                .map(|session| session.name.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            format!("{} sessions. {names}", sessions.len())
        };
        json!({
            "ok": true,
            "sessions": sessions.into_iter().map(|session| session.raw).collect::<Vec<_>>(),
            "current_session": self.selected.as_ref().map(|(id, name)| json!({"id": id, "name": name})),
            "spoken_hint": spoken_hint
        })
    }

    fn set_current_session(&mut self, args: &Value) -> Value {
        let Some(reference) = exact_string(args, "session") else {
            return error("invalid_arguments");
        };
        let Some(session) = self.resolve(reference) else {
            return error("session_not_found_or_ambiguous");
        };
        if self
            .selected
            .as_ref()
            .is_some_and(|(selected_id, _)| selected_id != &session.id)
        {
            self.invalidate_active_response();
        }
        self.selected = Some((session.id.clone(), session.name.clone()));
        json!({"ok": true, "session_id": session.id, "title": session.name})
    }

    fn list_pending_permissions(&self) -> Value {
        let Some(items) = self
            .paseo
            .as_ref()
            .and_then(PaseoAdapter::list_pending_permissions)
        else {
            return error("paseo_unavailable");
        };
        let count = items.len();
        let spoken_hint = if items.is_empty() {
            "Nothing is waiting for permission."
        } else {
            "Permission requests are waiting. Approve them in Paseo."
        };
        json!({
            "ok": true,
            "count": count,
            "items": items,
            "spoken_hint": spoken_hint
        })
    }

    fn operation_status(&self, args: &Value) -> Value {
        let Some(operation_id) = exact_string(args, "operation_id") else {
            return error("invalid_arguments");
        };
        if paseo_safety_core::Identifier::new(operation_id).is_err() {
            return error("invalid_operation_id");
        }
        let Some(journal) = &self.journal else {
            return error("journal_unavailable");
        };
        let Ok(journal) = journal.lock() else {
            return error("journal_unavailable");
        };
        let Ok(status) = journal.latest_status(operation_id) else {
            return error("journal_unavailable");
        };
        let Some(status) = status else {
            return error("operation_not_found");
        };
        json!({
            "ok": true,
            "operation_id": status.operation_id,
            "state": status.state,
            "timestamp_ms": status.timestamp_ms,
            "receiver_message_id": status.receiver_message_id
        })
    }

    fn read_latest_reply(&mut self, args: &Value, now_ms: u64) -> Value {
        let Some(object) = args.as_object() else {
            return error("invalid_arguments");
        };
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "session" | "mode"))
        {
            return error("invalid_arguments");
        }
        let reference = match object.get("session") {
            None => None,
            Some(Value::String(value)) if !value.is_empty() => Some(value.as_str()),
            Some(_) => return error("invalid_arguments"),
        };
        let mode = match object.get("mode") {
            None => "summary",
            Some(Value::String(value)) if value == "summary" => "summary",
            Some(Value::String(value)) if value == "full" => "full",
            Some(_) => return error("invalid_arguments"),
        };
        let session = if let Some(reference) = reference {
            self.resolve(reference)
        } else if let Some((id, _)) = &self.selected {
            self.resolve(id)
        } else {
            None
        };
        let Some(session) = session else {
            return error("session_not_found_or_ambiguous");
        };
        let filtered_tail = if mode == "full" { 2 } else { 6 };
        let Some(mut text) = self
            .paseo
            .as_ref()
            .and_then(|paseo| paseo.read_log_text(&session.id, filtered_tail, true))
        else {
            return error("reply_unavailable");
        };
        if text.is_empty() {
            let Some(fallback) = self
                .paseo
                .as_ref()
                .and_then(|paseo| paseo.read_log_text(&session.id, self.log_tail_entries, false))
            else {
                return error("reply_unavailable");
            };
            text = fallback;
        }
        if text.is_empty() {
            return json!({"ok": true, "spoken_text": "No readable reply yet.", "respondable": false});
        }
        if self.active.is_some() {
            self.invalidate_active_response();
        }
        let summary_id = SummaryId::new(format!("summary-{}", Uuid::new_v4()))
            .expect("generated summary ID is valid");
        let thread_id = ThreadId::new(session.id.clone()).expect("validated session ID");
        let reply_digest = Sha256::digest([session.id.as_bytes(), &[0], text.as_bytes()].concat());
        let mut reply_id_text = String::from("reply-");
        for byte in reply_digest {
            write!(&mut reply_id_text, "{byte:02x}").expect("writing to String cannot fail");
        }
        let reply_id = ReplyId::new(reply_id_text).expect("digest ID is valid");
        let observed = self.safety.apply(Command::ObserveReply {
            summary_id: summary_id.clone(),
            source_thread_id: thread_id.clone(),
            source_reply_id: reply_id,
            observed_at_ms: now_ms,
        });
        if observed == Ok(Applied::DuplicateReply) {
            return json!({
                "ok": true, "session_id": session.id, "spoken_text": text,
                "respondable": false, "reason": "reply_already_observed"
            });
        }
        if observed.is_err()
            || self
                .safety
                .apply(Command::MarkSummaryReady {
                    summary_id: summary_id.clone(),
                })
                .is_err()
            || self.safety.apply(Command::ActivateNext).is_err()
        {
            return error("provenance_transition_rejected");
        }
        self.selected = Some((session.id.clone(), session.name.clone()));
        self.active = Some(ActiveContext {
            summary_id,
            title: session.name,
        });
        json!({
            "ok": true, "session_id": session.id, "spoken_text": text,
            "respondable": true, "mode": mode
        })
    }

    fn propose_response(&mut self, args: &Value, now_ms: u64) -> Value {
        let Some(text) = exact_string(args, "text") else {
            return error("invalid_arguments");
        };
        let Ok(response) = ResponseBody::new(text) else {
            return error("invalid_response_body");
        };
        let Some(active) = self.active.as_ref() else {
            return error("no_active_reply");
        };
        let proposal_id = ProposalId::new(format!("proposal-{}", Uuid::new_v4()))
            .expect("generated proposal ID is valid");
        if self
            .safety
            .apply(Command::ProposeResponse {
                proposal_id: proposal_id.clone(),
                summary_id: active.summary_id.clone(),
                response,
                interaction: self.interaction,
                now_ms,
            })
            .is_err()
        {
            return error("proposal_rejected");
        }
        self.pending = Some(proposal_id.clone());
        self.pending_run = None;
        json!({
            "ok": true, "proposed": true, "executed": false,
            "proposal_token": proposal_id.as_str(),
            "spoken_echo": format!("Send to {}: \"{}\". Confirm?", active.title, text)
        })
    }

    fn invalidate_active_response(&mut self) {
        let _ = self.safety.apply(Command::DeferActive);
        self.active = None;
        self.pending = None;
    }

    fn propose_run(&mut self, args: &Value, now_ms: u64) -> Value {
        let Some(object) = args.as_object() else {
            return error("invalid_arguments");
        };
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "prompt" | "provider" | "cwd" | "title"))
        {
            return error("invalid_arguments");
        }
        let Some(prompt) = object
            .get("prompt")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
        else {
            return error("invalid_arguments");
        };
        if ResponseBody::new(prompt).is_err() {
            return error("invalid_response_body");
        }
        let optional = |key: &str, maximum: usize| -> Result<Option<String>, ()> {
            match object.get(key) {
                None => Ok(None),
                Some(Value::String(value))
                    if !value.is_empty()
                        && value.len() <= maximum
                        && !value.chars().any(char::is_control) =>
                {
                    Ok(Some(value.clone()))
                }
                _ => Err(()),
            }
        };
        let (Ok(provider), Ok(cwd), Ok(title)) = (
            optional("provider", 256),
            optional("cwd", 4096),
            optional("title", 256),
        ) else {
            return error("invalid_arguments");
        };
        let Some(expires_at_ms) = now_ms.checked_add(self.proposal_ttl_ms) else {
            return error("time_overflow");
        };
        if let Some(proposal_id) = self.pending.take() {
            let _ = self.safety.apply(Command::CancelResponse { proposal_id });
        }
        let proposal_id = ProposalId::new(format!("proposal-{}", Uuid::new_v4()))
            .expect("generated proposal ID is valid");
        self.pending_run = Some(PendingRun {
            proposal_id: proposal_id.clone(),
            prompt: prompt.to_owned(),
            provider: provider.clone(),
            cwd: cwd.clone(),
            title: title.clone(),
            created_interaction: self.interaction,
            expires_at_ms,
        });
        let provider_echo = provider
            .as_deref()
            .map_or_else(String::new, |value| format!(" with provider {value}"));
        let cwd_echo = cwd
            .as_deref()
            .map_or_else(String::new, |value| format!(" in {value}"));
        let title_echo = title
            .as_deref()
            .map_or_else(String::new, |value| format!(" titled {value}"));
        json!({
            "ok": true, "proposed": true, "executed": false,
            "proposal_token": proposal_id.as_str(),
            "spoken_echo": format!(
                "Start a new agent{provider_echo}{cwd_echo}{title_echo}: \"{prompt}\". Confirm?"
            )
        })
    }

    fn confirm_response(&mut self, args: &Value, now_ms: u64) -> Value {
        let Some(token) = exact_string(args, "token") else {
            return error("invalid_arguments");
        };
        let Ok(proposal_id) = ProposalId::new(token) else {
            return error("invalid_proposal");
        };
        if self
            .pending_run
            .as_ref()
            .is_some_and(|run| run.proposal_id == proposal_id)
        {
            return self.confirm_run(now_ms);
        }
        if self.pending.as_ref() != Some(&proposal_id) {
            return error("not_pending");
        }
        let authorization = self.safety.apply(Command::ConfirmResponse {
            proposal_id: proposal_id.clone(),
            interaction: self.interaction,
            now_ms,
        });
        let Ok(Applied::DispatchAuthorized(authorization)) = authorization else {
            return error("confirmation_rejected");
        };
        let Some(active) = self.active.as_ref() else {
            return error("no_active_reply");
        };
        if !self.append_journal(
            proposal_id.as_str(),
            active.summary_id.as_str(),
            authorization.destination_thread_id().as_str(),
            &authorization.response().sha256_hex(),
            "dispatching",
            now_ms,
            None,
        ) {
            return error("journal_failed_before_dispatch");
        }
        let result = self.paseo.as_ref().map_or(WriteResult::Rejected, |paseo| {
            paseo.send_message(&authorization)
        });
        let outcome = match &result {
            WriteResult::Delivered { .. } => DeliveryOutcome::Delivered,
            WriteResult::Rejected => DeliveryOutcome::Rejected,
            WriteResult::OutcomeUnknown => DeliveryOutcome::OutcomeUnknown,
        };
        let _ = self.safety.apply(Command::RecordDelivery {
            proposal_id: proposal_id.clone(),
            outcome,
        });
        let (state, receiver_id) = match &result {
            WriteResult::Delivered {
                receiver_message_id,
            } => ("delivered", Some(receiver_message_id.as_str())),
            WriteResult::Rejected => ("rejected", None),
            WriteResult::OutcomeUnknown => ("outcome_unknown", None),
        };
        let journal_ok = self.append_journal(
            proposal_id.as_str(),
            active.summary_id.as_str(),
            authorization.destination_thread_id().as_str(),
            &authorization.response().sha256_hex(),
            state,
            now_ms,
            receiver_id,
        );
        self.pending = None;
        self.active = None;
        if !journal_ok {
            return json!({"ok": false, "executed": true, "delivery": "outcome_unknown"});
        }
        match result {
            WriteResult::Delivered {
                receiver_message_id,
            } => {
                json!({"ok": true, "executed": true, "delivery": "delivered", "receiver_message_id": receiver_message_id})
            }
            WriteResult::Rejected => error("delivery_rejected"),
            WriteResult::OutcomeUnknown => {
                json!({"ok": false, "executed": true, "delivery": "outcome_unknown"})
            }
        }
    }

    fn confirm_run(&mut self, now_ms: u64) -> Value {
        let Some(run) = self.pending_run.as_ref() else {
            return error("not_pending");
        };
        if self.interaction <= run.created_interaction {
            return error("confirmation_rejected");
        }
        if now_ms >= run.expires_at_ms {
            self.pending_run = None;
            return error("proposal_expired");
        }
        let run = self.pending_run.take().expect("pending run was checked");
        let response_sha256 = ResponseBody::new(run.prompt.clone())
            .expect("stored run prompt was validated")
            .sha256_hex();
        let summary_id = format!("run-{}", run.proposal_id.as_str());
        if !self.append_journal(
            run.proposal_id.as_str(),
            &summary_id,
            "new-run",
            &response_sha256,
            "dispatching",
            now_ms,
            None,
        ) {
            return error("journal_failed_before_dispatch");
        }
        let result = self.paseo.as_ref().map_or(WriteResult::Rejected, |paseo| {
            paseo.start_run(&RunAuthorization::new(
                run.prompt,
                run.provider,
                run.cwd,
                run.title,
            ))
        });
        let (state, receiver_id) = match &result {
            WriteResult::Delivered {
                receiver_message_id,
            } => ("delivered", Some(receiver_message_id.as_str())),
            WriteResult::Rejected => ("rejected", None),
            WriteResult::OutcomeUnknown => ("outcome_unknown", None),
        };
        if !self.append_journal(
            run.proposal_id.as_str(),
            &summary_id,
            "new-run",
            &response_sha256,
            state,
            now_ms,
            receiver_id,
        ) {
            return json!({"ok": false, "executed": true, "delivery": "outcome_unknown"});
        }
        write_result_value(result)
    }

    fn cancel_response(&mut self) -> Value {
        if self.pending_run.take().is_some() {
            return json!({"ok": true, "cancelled": true});
        }
        let Some(proposal_id) = self.pending.take() else {
            return json!({"ok": true, "cancelled": false});
        };
        let cancelled = self
            .safety
            .apply(Command::CancelResponse { proposal_id })
            .is_ok();
        json!({"ok": true, "cancelled": cancelled})
    }

    fn sessions(&self, include_archived: bool) -> Option<Vec<Session>> {
        self.paseo
            .as_ref()?
            .list_sessions(include_archived)
            .map(|rows| {
                rows.into_iter()
                    .filter_map(|raw| {
                        let id = raw.get("id")?.as_str()?.to_owned();
                        if ThreadId::new(id.clone()).is_err() {
                            return None;
                        }
                        Some(Session {
                            short_id: raw
                                .get("shortId")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            name: raw
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("(untitled)")
                                .to_owned(),
                            id,
                            raw,
                        })
                    })
                    .collect()
            })
    }

    #[allow(clippy::too_many_arguments)]
    fn append_journal(
        &self,
        operation_id: &str,
        summary_id: &str,
        destination_thread_id: &str,
        response_sha256: &str,
        state: &str,
        timestamp_ms: u64,
        receiver_message_id: Option<&str>,
    ) -> bool {
        let Some(journal) = &self.journal else {
            return true;
        };
        journal
            .lock()
            .ok()
            .and_then(|journal| {
                journal
                    .append(&JournalEntry {
                        operation_id,
                        summary_id,
                        destination_thread_id,
                        response_sha256,
                        state,
                        timestamp_ms,
                        receiver_message_id,
                    })
                    .ok()
            })
            .is_some()
    }

    fn resolve(&self, reference: &str) -> Option<Session> {
        let reference = reference.trim().to_lowercase();
        let sessions = self.sessions(false)?;
        let exact = sessions
            .iter()
            .filter(|session| {
                session.id.to_lowercase() == reference
                    || session.short_id.to_lowercase() == reference
            })
            .cloned()
            .collect::<Vec<_>>();
        let prefix = sessions
            .iter()
            .filter(|session| session.id.to_lowercase().starts_with(&reference))
            .cloned()
            .collect::<Vec<_>>();
        let title = sessions
            .iter()
            .filter(|session| session.name.to_lowercase().contains(&reference))
            .cloned()
            .collect::<Vec<_>>();
        [exact, prefix, title]
            .into_iter()
            .find(|matches| !matches.is_empty())
            .filter(|matches| matches.len() == 1)
            .and_then(|mut matches| matches.pop())
    }
}

fn exact_string<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    let object = args.as_object()?;
    (object.len() == 1)
        .then(|| object.get(key).and_then(Value::as_str))
        .flatten()
        .filter(|value| !value.is_empty())
}

fn error(code: &'static str) -> Value {
    json!({"ok": false, "error": code})
}

fn write_result_value(result: WriteResult) -> Value {
    match result {
        WriteResult::Delivered {
            receiver_message_id,
        } => {
            json!({"ok": true, "executed": true, "delivery": "delivered", "receiver_message_id": receiver_message_id})
        }
        WriteResult::Rejected => error("delivery_rejected"),
        WriteResult::OutcomeUnknown => {
            json!({"ok": false, "executed": true, "delivery": "outcome_unknown"})
        }
    }
}
