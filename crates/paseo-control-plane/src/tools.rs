//! Provenance-bound voice tool dispatcher.

use std::{
    fmt::Write as _,
    sync::{Arc, Mutex},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use paseo_safety_core::{
    Applied, Command, DeliveryOutcome, InteractionSequence, ProposalId, ReplyId, ResponseBody,
    SafetyCore, SafetyCoreCheckpoint, SafetyError, SummaryId, SummaryQueue, ThreadId,
};
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    config::PaseoHostProfile,
    journal::{
        Journal, JournalEntry, MAX_TIMELINE_ENTRIES, TimelineCursor, TimelineError, TimelinePage,
        TimelineQuery,
    },
    paseo::{PaseoAdapter, ProcessExecutor, RunAuthorization, RunResult, WriteResult},
};

const MAX_IDENTIFIER_LENGTH: usize = 128;
const TIMELINE_STATE_MAX_LENGTH: usize = 15;
const TIMELINE_CURSOR_VERSION: u8 = 1;
const TIMELINE_CURSOR_BYTES: usize = 57;
const TIMELINE_CURSOR_ENCODED_LENGTH: usize = 76;

struct StrictJsonValue(Value);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrictJsonVisitor;

        impl<'de> Visitor<'de> for StrictJsonVisitor {
            type Value = StrictJsonValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("JSON without duplicate object fields")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(Value::Bool(value)))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(Value::Number(value.into())))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(Value::Number(value.into())))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let number = serde_json::Number::from_f64(value)
                    .ok_or_else(|| E::custom("non-finite JSON number"))?;
                Ok(StrictJsonValue(Value::Number(number)))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(Value::String(value.to_owned())))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(Value::String(value)))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(Value::Null))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(Value::Null))
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                StrictJsonValue::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(StrictJsonValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(StrictJsonValue(Value::Array(values)))
            }

            fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = serde_json::Map::new();
                while let Some(key) = object.next_key::<String>()? {
                    if fields.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate field"));
                    }
                    let StrictJsonValue(value) = object.next_value()?;
                    fields.insert(key, value);
                }
                Ok(StrictJsonValue(Value::Object(fields)))
            }
        }

        deserializer.deserialize_any(StrictJsonVisitor)
    }
}

/// Parse one strict JSON object, rejecting duplicate fields at every depth.
pub(crate) fn parse_strict_json_object(input: &str) -> Option<Value> {
    let StrictJsonValue(value) = serde_json::from_str(input).ok()?;
    value.is_object().then_some(value)
}

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
            "type": "function", "name": "replay_summary",
            "description": "Replay the current broker-owned short summary from the beginning.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": false}
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
            "type": "function", "name": "list_operation_timeline",
            "description": "List recent content-free delivery metadata, newest first.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_TIMELINE_ENTRIES},
                    "state": {"type": "string", "maxLength": TIMELINE_STATE_MAX_LENGTH,
                        "enum": ["dispatching", "delivered", "rejected", "outcome_unknown", "invalidated"]
                    },
                    "summary_id": {"type": "string", "minLength": 1, "maxLength": MAX_IDENTIFIER_LENGTH},
                    "destination_thread_id": {"type": "string", "minLength": 1, "maxLength": MAX_IDENTIFIER_LENGTH},
                    "cursor": {
                        "type": "string",
                        "minLength": TIMELINE_CURSOR_ENCODED_LENGTH,
                        "maxLength": TIMELINE_CURSOR_ENCODED_LENGTH
                    }
                },
                "additionalProperties": false
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
            "type": "function", "name": "create_session",
            "description": "Begin or continue broker-gated session creation. The first interaction asks for a later task; a later interaction may propose using trusted host-profile defaults. This never creates it.",
            "parameters": {
                "type": "object",
                "properties": {"prompt": {"type": "string"}},
                "required": ["prompt"], "additionalProperties": false
            }
        },
        {
            "type": "function", "name": "cancel_action",
            "description": "Cancel the pending response.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": false}
        }
    ])
}

/// Broker-owned proposal state exposed to the browser only through a presentation handle.
#[derive(Clone, Debug)]
pub struct ProposalPresentation {
    handle: String,
    token: String,
    echo: String,
}

impl ProposalPresentation {
    /// Bind one safety-core proposal token to a fresh browser presentation handle.
    #[must_use]
    pub fn new(token: impl Into<String>, echo: impl Into<String>) -> Self {
        Self {
            handle: format!("presentation-{}", Uuid::new_v4()),
            token: token.into(),
            echo: echo.into(),
        }
    }

    /// Report whether a browser control supplies this exact current handle.
    #[must_use]
    pub fn matches(&self, control: &Value) -> bool {
        control.get("handle").and_then(Value::as_str) == Some(self.handle.as_str())
    }

    /// Borrow the internal safety-core proposal token.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }
}

/// Serialize current proposal state without exposing its safety-core token.
#[must_use]
pub fn proposal_frame(proposal: Option<&ProposalPresentation>) -> Value {
    proposal.map_or_else(
        || json!({"type":"proposal","echo":null}),
        |proposal| {
            json!({
                "type":"proposal",
                "echo":proposal.echo,
                "handle":proposal.handle
            })
        },
    )
}

#[derive(Clone, Debug)]
struct Session {
    id: String,
    short_id: String,
    name: String,
    provider: String,
    state: String,
    raw: Value,
}

#[derive(Clone, Debug)]
struct ActiveContext {
    summary_id: SummaryId,
    reply_id: ReplyId,
    title: String,
    thread_id: String,
    latest_summary: String,
    summary_degraded: bool,
}

#[derive(Clone, Debug)]
struct PendingRun {
    proposal_id: ProposalId,
    prompt: String,
    provider: String,
    cwd: String,
    created_interaction: InteractionSequence,
    expires_at_ms: u64,
}

/// One exact browser-displayed summary context supplied with a turn.
pub(crate) enum TurnContext<'a> {
    /// No summary was displayed.
    None,
    /// This exact immutable summary was displayed.
    Summary(&'a str),
}

/// Content-free connection routing state retained while the engine is executing elsewhere.
#[derive(Clone, Debug)]
pub(crate) struct ToolEngineView {
    active_summary_id: Option<String>,
    has_pending_action: bool,
    selected_host_id: String,
    host_ids: Vec<String>,
}

impl ToolEngineView {
    /// Borrow the immutable active summary identifier.
    #[must_use]
    pub(crate) fn active_summary_id(&self) -> Option<&str> {
        self.active_summary_id.as_deref()
    }

    /// Report whether a proposal currently awaits confirmation.
    #[must_use]
    pub(crate) const fn has_pending_action(&self) -> bool {
        self.has_pending_action
    }

    /// Borrow the selected host identifier.
    #[must_use]
    pub(crate) fn selected_host_id(&self) -> &str {
        &self.selected_host_id
    }

    /// Check whether a host identifier is one of the trusted profiles.
    #[must_use]
    pub(crate) fn has_host_id(&self, host_id: &str) -> bool {
        self.host_ids.iter().any(|known| known == host_id)
    }

    /// Reflect authoritative proposal cancellation while the engine is away.
    pub(crate) fn cancel_pending_action(&mut self) {
        self.has_pending_action = false;
    }

    /// Reflect authoritative host replacement while the engine is away.
    pub(crate) fn select_host(&mut self, host_id: &str) -> bool {
        if !self.has_host_id(host_id) {
            return false;
        }
        if self.selected_host_id != host_id {
            host_id.clone_into(&mut self.selected_host_id);
            self.active_summary_id = None;
            self.has_pending_action = false;
        }
        true
    }

    /// Validate an exact browser turn against this immutable routing snapshot.
    #[must_use]
    pub(crate) fn accept_turn_context<'a>(
        &self,
        control: &'a Value,
        fields: &[&str],
    ) -> Option<TurnContext<'a>> {
        let object = control.as_object()?;
        if object.len() != fields.len() || !object.keys().all(|key| fields.contains(&key.as_str()))
        {
            return None;
        }
        let supplied = match object.get("summary_id")? {
            Value::Null => TurnContext::None,
            Value::String(value) => TurnContext::Summary(value),
            _ => return None,
        };
        (supplied.as_deref() == self.active_summary_id()).then_some(supplied)
    }
}

impl TurnContext<'_> {
    /// Borrow the supplied summary identifier when one was displayed.
    #[must_use]
    pub(crate) const fn as_deref(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Summary(summary_id) => Some(summary_id),
        }
    }
}

#[derive(Clone)]
enum DispatchContext {
    Trusted,
    Model(Option<String>),
}

/// One trusted host profile paired with its credential-owning Paseo adapter.
pub struct PaseoHost<E> {
    profile: PaseoHostProfile,
    paseo: Option<PaseoAdapter<E>>,
}

impl<E> PaseoHost<E> {
    /// Pair validated profile metadata with an optional live adapter.
    #[must_use]
    pub fn new(profile: PaseoHostProfile, paseo: Option<PaseoAdapter<E>>) -> Self {
        Self { profile, paseo }
    }
}

/// Per-browser owner of selection, provenance, and confirmation state.
pub struct ToolEngine<E> {
    hosts: Vec<PaseoHost<E>>,
    selected_host: usize,
    safety: SafetyCore,
    proposal_ttl_ms: u64,
    interaction: InteractionSequence,
    selected: Option<(String, String)>,
    active: Option<ActiveContext>,
    pending: Option<ProposalId>,
    pending_run: Option<PendingRun>,
    journal: Option<Arc<Mutex<Journal>>>,
    log_tail_entries: usize,
    dashboard_agents: Vec<Value>,
    dispatch_context: DispatchContext,
    session_task_collection_started: Option<InteractionSequence>,
}

/// Ephemeral mutable state captured before one response dispatches model tools.
pub(crate) struct ToolEngineCheckpoint {
    selected_host: usize,
    safety: SafetyCoreCheckpoint,
    interaction: InteractionSequence,
    selected: Option<(String, String)>,
    active: Option<ActiveContext>,
    pending: Option<ProposalId>,
    pending_run: Option<PendingRun>,
    dashboard_agents: Vec<Value>,
    dispatch_context: DispatchContext,
    session_task_collection_started: Option<InteractionSequence>,
}

impl<E: ProcessExecutor> ToolEngine<E> {
    /// Construct a dispatcher. None keeps tools safely unavailable.
    #[must_use]
    pub fn new(paseo: Option<PaseoAdapter<E>>, proposal_ttl_ms: u64) -> Self {
        let profile = PaseoHostProfile {
            id: "local".to_owned(),
            label: "Local Paseo".to_owned(),
            target: None,
            default: true,
            default_cwd: "~/".to_owned(),
            default_provider: "opencode/gpt-5.6-sol-max".to_owned(),
        };
        Self::build(vec![PaseoHost::new(profile, paseo)], 0, proposal_ttl_ms)
    }

    /// Construct a dispatcher over explicit validated host profiles.
    ///
    /// # Errors
    ///
    /// Returns an error unless exactly one profile is the default.
    pub fn from_hosts(
        hosts: Vec<PaseoHost<E>>,
        proposal_ttl_ms: u64,
    ) -> Result<Self, &'static str> {
        let defaults = hosts
            .iter()
            .enumerate()
            .filter(|(_, host)| host.profile.default)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let [selected_host] = defaults.as_slice() else {
            return Err("exactly one default Paseo host is required");
        };
        Ok(Self::build(hosts, *selected_host, proposal_ttl_ms))
    }

    fn build(hosts: Vec<PaseoHost<E>>, selected_host: usize, proposal_ttl_ms: u64) -> Self {
        Self {
            hosts,
            selected_host,
            safety: SafetyCore::new(proposal_ttl_ms),
            proposal_ttl_ms,
            interaction: InteractionSequence::new(0),
            selected: None,
            active: None,
            pending: None,
            pending_run: None,
            journal: None,
            log_tail_entries: 40,
            dashboard_agents: Vec::new(),
            dispatch_context: DispatchContext::Trusted,
            session_task_collection_started: None,
        }
    }

    /// Return browser-safe host choices and connection-scoped selection state.
    #[must_use]
    pub fn host_state(&self) -> Value {
        json!({
            "ok": true,
            "selected_host_id": self.hosts[self.selected_host].profile.id,
            "hosts": self.hosts.iter().map(|host| json!({
                "id": host.profile.id,
                "label": host.profile.label,
                "default": host.profile.default,
                "default_cwd": host.profile.default_cwd,
                "default_provider": host.profile.default_provider,
                "available": host.paseo.as_ref().and_then(|paseo| paseo.list_sessions(false)).is_some()
            })).collect::<Vec<_>>()
        })
    }

    /// Borrow the selected profile ID for broker-internal change detection.
    #[must_use]
    pub(crate) fn selected_host_id(&self) -> &str {
        &self.hosts[self.selected_host].profile.id
    }

    /// Check a host identifier without running availability probes.
    #[must_use]
    pub(crate) fn has_host_id(&self, host_id: &str) -> bool {
        self.hosts.iter().any(|host| host.profile.id == host_id)
    }

    /// Return ephemeral browser-safe agent and provenance presentation data.
    #[must_use]
    pub fn dashboard_state(&mut self) -> Value {
        let sessions = self.sessions(false).unwrap_or_default();
        self.dashboard_agents = sessions
            .into_iter()
            .map(|session| {
                json!({
                    "thread_id": session.id,
                    "thread_name": session.name,
                    "provider": session.provider,
                    "state": session.state,
                    "queued_response_count": 0
                })
            })
            .collect();
        self.presentation_snapshot()
    }

    /// Return the last explicit dashboard presentation with current routing context and no I/O.
    #[must_use]
    pub fn presentation_snapshot(&self) -> Value {
        json!({
            "ok": true,
            "selected_host_id": self.hosts[self.selected_host].profile.id,
            "queue_count": 0,
            "bound_context": self.active.as_ref().map(|active| json!({
                "summary_id": active.summary_id.as_str(),
                "thread_id": active.thread_id,
                "thread_name": active.title,
                "latest_summary": active.latest_summary,
                "summary_degraded": active.summary_degraded
            })),
            "agents": self.dashboard_agents
        })
    }

    /// Borrow the immutable identifier for the currently active summary context.
    #[must_use]
    pub(crate) fn active_summary_id(&self) -> Option<&str> {
        self.active
            .as_ref()
            .map(|active| active.summary_id.as_str())
    }

    /// Validate an exact browser turn shape against the displayed immutable context.
    #[must_use]
    pub(crate) fn accept_turn_context<'a>(
        &self,
        control: &'a Value,
        fields: &[&str],
    ) -> Option<TurnContext<'a>> {
        self.connection_view().accept_turn_context(control, fields)
    }

    /// Capture content-free routing state for connection processing during an owned job.
    #[must_use]
    pub(crate) fn connection_view(&self) -> ToolEngineView {
        ToolEngineView {
            active_summary_id: self.active_summary_id().map(str::to_owned),
            has_pending_action: self.has_pending_action(),
            selected_host_id: self.selected_host_id().to_owned(),
            host_ids: self
                .hosts
                .iter()
                .map(|host| host.profile.id.clone())
                .collect(),
        }
    }

    /// Capture all mutable engine state without cloning adapters or credentials.
    pub(crate) fn checkpoint(&self) -> ToolEngineCheckpoint {
        ToolEngineCheckpoint {
            selected_host: self.selected_host,
            safety: self.safety.checkpoint(),
            interaction: self.interaction,
            selected: self.selected.clone(),
            active: self.active.clone(),
            pending: self.pending.clone(),
            pending_run: self.pending_run.clone(),
            dashboard_agents: self.dashboard_agents.clone(),
            dispatch_context: self.dispatch_context.clone(),
            session_task_collection_started: self.session_task_collection_started,
        }
    }

    /// Restore a response checkpoint while retaining immutable adapters and configuration.
    pub(crate) fn restore_checkpoint(&mut self, checkpoint: ToolEngineCheckpoint) {
        self.selected_host = checkpoint.selected_host;
        self.safety.restore(checkpoint.safety);
        self.interaction = checkpoint.interaction;
        self.selected = checkpoint.selected;
        self.active = checkpoint.active;
        self.pending = checkpoint.pending;
        self.pending_run = checkpoint.pending_run;
        self.dashboard_agents = checkpoint.dashboard_agents;
        self.dispatch_context = checkpoint.dispatch_context;
        self.session_task_collection_started = checkpoint.session_task_collection_started;
    }

    /// Snapshot the content-free summary queue for the shared process owner.
    #[must_use]
    pub fn queue_snapshot(&self) -> SummaryQueue {
        self.safety.queue_snapshot()
    }

    /// Seed the summary queue from the shared process owner at connection start.
    pub fn seed_queue(&mut self, queue: SummaryQueue) {
        self.safety.seed_queue(queue);
    }

    /// Replace the ephemeral summary only while its immutable context is still active.
    #[must_use]
    pub fn update_active_summary_if_current(
        &mut self,
        expected_summary_id: &str,
        summary: &str,
        degraded: bool,
    ) -> bool {
        let Some(active) = self
            .active
            .as_mut()
            .filter(|active| active.summary_id.as_str() == expected_summary_id)
        else {
            return false;
        };
        active.latest_summary = bounded_display(summary, 600, "");
        active.summary_degraded = degraded;
        true
    }

    /// Activate one read-only reply discovered by the opt-in completion poller.
    pub fn activate_polled_reply(
        &mut self,
        session_id: &str,
        session_name: &str,
        text: &str,
        now_ms: u64,
    ) -> Option<Value> {
        if text.is_empty() || ThreadId::new(session_id.to_owned()).is_err() {
            return None;
        }
        let result = self.activate_reply(
            session_id,
            &bounded_display(session_name, 128, "(untitled)"),
            text,
            "summary",
            now_ms,
        );
        (result.get("respondable").and_then(Value::as_bool) == Some(true)).then_some(result)
    }

    /// Select a host for this connection and invalidate every host-bound action.
    #[must_use]
    pub fn select_host(&mut self, host_id: &str) -> Value {
        let Some(index) = self
            .hosts
            .iter()
            .position(|host| host.profile.id == host_id)
        else {
            return error("host_not_found");
        };
        if index != self.selected_host {
            self.invalidate_active_response();
            self.pending_run = None;
            self.session_task_collection_started = None;
            self.selected = None;
            self.selected_host = index;
        }
        self.host_state()
    }

    /// Invalidate host-bound state after a deferred host-selection intent.
    pub(crate) fn invalidate_for_host_intent(&mut self) {
        self.invalidate_active_response();
        self.pending_run = None;
        self.session_task_collection_started = None;
        self.selected = None;
    }

    /// Invalidate all response and proposal state while preserving the selected Paseo host.
    pub(crate) fn invalidate_for_voice_switch(&mut self) {
        self.invalidate_active_response();
        self.pending_run = None;
        self.session_task_collection_started = None;
    }

    fn paseo(&self) -> Option<&PaseoAdapter<E>> {
        self.hosts.get(self.selected_host)?.paseo.as_ref()
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

    /// Report whether a write or session-creation proposal awaits confirmation.
    #[must_use]
    pub const fn has_pending_action(&self) -> bool {
        self.pending.is_some() || self.pending_run.is_some()
    }

    /// Borrow the current safety token for broker-internal presentation binding.
    #[must_use]
    pub fn pending_action_token(&self) -> Option<&str> {
        self.pending_run
            .as_ref()
            .map(|run| run.proposal_id.as_str())
            .or_else(|| self.pending.as_ref().map(ProposalId::as_str))
    }

    /// Dispatch one strict tool call. Errors are returned as bounded JSON.
    #[must_use]
    pub fn dispatch(&mut self, name: &str, arguments: &str, now_ms: u64) -> Value {
        if name == "create_session" && matches!(&self.dispatch_context, DispatchContext::Model(_)) {
            match self.session_task_collection_started {
                None => {
                    self.session_task_collection_started = Some(self.interaction);
                    return session_task_required();
                }
                Some(started) if self.interaction <= started => {
                    return session_task_required();
                }
                Some(_) => self.session_task_collection_started = None,
            }
        } else if name == "create_session" {
            self.session_task_collection_started = None;
        }
        if name == "send_message"
            && matches!(
                &self.dispatch_context,
                DispatchContext::Model(expected)
                    if expected.as_deref() != self.active_summary_id()
            )
        {
            return error("stale_response_context");
        }
        let Some(args) = parse_strict_json_object(arguments) else {
            return error("invalid_arguments");
        };
        match name {
            "list_sessions" => self.list_sessions(&args),
            "set_current_session" => self.set_current_session(&args),
            "read_latest_reply" => self.read_latest_reply(&args, now_ms),
            "replay_summary" => self.replay_summary(&args),
            "list_pending_permissions" => self.list_pending_permissions(),
            "get_operation_status" => self.operation_status(&args),
            "list_operation_timeline" => self.operation_timeline(&args),
            "send_message" => self.propose_response(&args, now_ms),
            "create_session" => self.propose_run(&args, now_ms),
            "confirm_action" => self.confirm_response(&args, now_ms),
            "cancel_action" => self.cancel_response(),
            _ => error("unknown_tool"),
        }
    }

    /// Dispatch a model-originated call under its broker-captured turn context.
    #[must_use]
    pub fn dispatch_model_call(
        &mut self,
        name: &str,
        arguments: &str,
        expected_summary_id: Option<&str>,
        now_ms: u64,
    ) -> Value {
        self.with_model_turn_context(expected_summary_id, |tools| {
            tools.dispatch(name, arguments, now_ms)
        })
    }

    /// Run deterministic model behavior under one broker-captured turn context.
    pub(crate) fn with_model_turn_context<R>(
        &mut self,
        expected_summary_id: Option<&str>,
        run: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let previous = std::mem::replace(
            &mut self.dispatch_context,
            DispatchContext::Model(expected_summary_id.map(str::to_owned)),
        );
        let result = run(self);
        self.dispatch_context = previous;
        result
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
            .paseo()
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

    fn replay_summary(&self, args: &Value) -> Value {
        if !args.as_object().is_some_and(serde_json::Map::is_empty) {
            return error("invalid_arguments");
        }
        let Some(active) = self.active.as_ref() else {
            return error("no_active_summary");
        };
        json!({
            "ok": true,
            "summary_id": active.summary_id.as_str(),
            "spoken_text": active.latest_summary
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
            "summary_id": status.summary_id,
            "destination_thread_id": status.destination_thread_id,
            "state": status.state,
            "timestamp_ms": status.timestamp_ms,
            "receiver_message_id": status.receiver_message_id
        })
    }

    fn operation_timeline(&self, args: &Value) -> Value {
        let Some(object) = args.as_object() else {
            return error("invalid_arguments");
        };
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "limit" | "state" | "summary_id" | "destination_thread_id" | "cursor"
            )
        }) {
            return error("invalid_arguments");
        }
        let limit = match object.get("limit") {
            None => 20,
            Some(Value::Number(value)) => {
                let Some(value) = value.as_u64() else {
                    return error("invalid_arguments");
                };
                let Ok(value) = usize::try_from(value) else {
                    return error("invalid_arguments");
                };
                if !(1..=MAX_TIMELINE_ENTRIES).contains(&value) {
                    return error("invalid_arguments");
                }
                value
            }
            Some(_) => return error("invalid_arguments"),
        };
        let state = match object.get("state") {
            None => None,
            Some(Value::String(value))
                if matches!(
                    value.as_str(),
                    "dispatching" | "delivered" | "rejected" | "outcome_unknown" | "invalidated"
                ) =>
            {
                Some(value.as_str())
            }
            Some(_) => return error("invalid_arguments"),
        };
        let summary_id = match object.get("summary_id") {
            None => None,
            Some(Value::String(value))
                if paseo_safety_core::Identifier::new(value.as_str()).is_ok() =>
            {
                Some(value.as_str())
            }
            Some(_) => return error("invalid_summary_id"),
        };
        let destination_thread_id = match object.get("destination_thread_id") {
            None => None,
            Some(Value::String(value))
                if paseo_safety_core::Identifier::new(value.as_str()).is_ok() =>
            {
                Some(value.as_str())
            }
            Some(_) => return error("invalid_destination_thread_id"),
        };
        let cursor = match object.get("cursor") {
            None => None,
            Some(Value::String(value)) => match decode_timeline_cursor(value) {
                Some(sequence) => Some(sequence),
                None => return error("invalid_cursor"),
            },
            Some(_) => return error("invalid_cursor"),
        };
        let Some(journal) = &self.journal else {
            return error("journal_unavailable");
        };
        let Ok(journal) = journal.lock() else {
            return error("journal_unavailable");
        };
        let page = match journal.query_timeline(&TimelineQuery {
            state,
            summary_id,
            destination_thread_id,
            cursor,
            limit,
        }) {
            Ok(page) => page,
            Err(TimelineError::InvalidCursor) => return error("invalid_cursor"),
            Err(TimelineError::CursorExpired) => return error("cursor_expired"),
            Err(TimelineError::Storage(_)) => return error("journal_unavailable"),
        };
        timeline_response(page)
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
            .paseo()
            .and_then(|paseo| paseo.read_log_text(&session.id, filtered_tail, true))
        else {
            return error("reply_unavailable");
        };
        if text.is_empty() {
            let Some(fallback) = self
                .paseo()
                .and_then(|paseo| paseo.read_log_text(&session.id, self.log_tail_entries, false))
            else {
                return error("reply_unavailable");
            };
            text = fallback;
        }
        if text.is_empty() {
            return json!({"ok": true, "spoken_text": "No readable reply yet.", "respondable": false});
        }
        self.activate_reply(&session.id, &session.name, &text, mode, now_ms)
    }

    fn activate_reply(
        &mut self,
        session_id: &str,
        session_name: &str,
        text: &str,
        mode: &str,
        now_ms: u64,
    ) -> Value {
        let summary_id = SummaryId::new(format!("summary-{}", Uuid::new_v4()))
            .expect("generated summary ID is valid");
        let thread_id = ThreadId::new(session_id.to_owned()).expect("validated session ID");
        let host_id = &self.hosts[self.selected_host].profile.id;
        let reply_id = synthetic_reply_id(host_id, session_id, text);
        let observed = self.safety.apply(Command::ObserveReply {
            summary_id: summary_id.clone(),
            source_thread_id: thread_id.clone(),
            source_reply_id: reply_id.clone(),
            observed_at_ms: now_ms,
        });
        if observed == Ok(Applied::DuplicateReply) {
            return self.duplicate_reply_result(&reply_id, session_id);
        }
        if observed.is_err() {
            return error("provenance_transition_rejected");
        }
        if self.active.is_some() {
            self.invalidate_active_response();
        }
        if self
            .safety
            .apply(Command::MarkSummaryReady {
                summary_id: summary_id.clone(),
            })
            .is_err()
            || self.safety.apply(Command::ActivateNext).is_err()
        {
            return error("provenance_transition_rejected");
        }
        self.selected = Some((session_id.to_owned(), session_name.to_owned()));
        self.active = Some(ActiveContext {
            summary_id,
            reply_id,
            title: session_name.to_owned(),
            thread_id: session_id.to_owned(),
            latest_summary: bounded_display(&crate::summarise::clean_for_speech(text), 600, ""),
            summary_degraded: false,
        });
        json!({
            "ok": true, "session_id": session_id, "spoken_text": text,
            "respondable": true, "mode": mode
        })
    }

    fn duplicate_reply_result(&mut self, reply_id: &ReplyId, session_id: &str) -> Value {
        if self
            .active
            .as_ref()
            .is_some_and(|active| &active.reply_id != reply_id)
        {
            self.invalidate_active_response();
        }
        json!({
            "ok": true, "session_id": session_id,
            "spoken_hint": "That reply was already read.",
            "respondable": false, "reason": "reply_already_observed"
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
        let Some(prompt) = exact_string(args, "prompt") else {
            return error("invalid_arguments");
        };
        if ResponseBody::new(prompt).is_err() {
            return error("invalid_response_body");
        }
        let host = &self.hosts[self.selected_host];
        let host_id = host.profile.id.clone();
        let host_label = host.profile.label.clone();
        let provider = host.profile.default_provider.clone();
        let cwd = host.profile.default_cwd.clone();
        let Some(paseo) = host.paseo.as_ref() else {
            return error("paseo_unavailable");
        };
        if !paseo.provider_model_available(&provider) {
            return error("provider_unavailable");
        }
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
            created_interaction: self.interaction,
            expires_at_ms,
        });
        json!({
            "ok": true, "proposed": true, "executed": false,
            "proposal_token": proposal_id.as_str(),
            "host_id": host_id,
            "spoken_echo": format!("Create a new session on {host_label} in {cwd} with {provider}: \"{prompt}\". Confirm?")
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
            self.session_task_collection_started = None;
            return self.confirm_run(now_ms);
        }
        if self.pending.as_ref() != Some(&proposal_id) {
            return error("not_pending");
        }
        self.session_task_collection_started = None;
        let authorization = self.safety.apply(Command::ConfirmResponse {
            proposal_id: proposal_id.clone(),
            interaction: self.interaction,
            now_ms,
        });
        let authorization = match authorization {
            Ok(Applied::DispatchAuthorized(authorization)) => authorization,
            Err(SafetyError::ProposalExpired) => {
                self.pending = None;
                return error("proposal_expired");
            }
            _ => return error("confirmation_rejected"),
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
            self.pending = None;
            self.active = None;
            return error("journal_failed_before_dispatch");
        }
        let result = self.paseo().map_or(WriteResult::Rejected, |paseo| {
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
        let result = self.paseo().map_or(RunResult::Rejected, |paseo| {
            paseo.start_run(&RunAuthorization::new(run.prompt, run.provider, run.cwd))
        });
        let (state, receiver_id) = match &result {
            RunResult::Created { agent_id } => ("delivered", Some(agent_id.as_str())),
            RunResult::Rejected => ("rejected", None),
            RunResult::OutcomeUnknown => ("outcome_unknown", None),
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
        match result {
            RunResult::Created { agent_id } => {
                self.invalidate_active_response();
                self.selected = Some((agent_id.clone(), agent_id.clone()));
                json!({
                    "ok":true,
                    "executed":true,
                    "delivery":"delivered",
                    "session_id":agent_id,
                    "spoken_hint":"Session created and selected."
                })
            }
            RunResult::Rejected => error("delivery_rejected"),
            RunResult::OutcomeUnknown => {
                let sessions_refreshed = self.sessions(false).is_some();
                json!({
                    "ok":false,
                    "executed":true,
                    "delivery":"outcome_unknown",
                    "sessions_refreshed":sessions_refreshed
                })
            }
        }
    }

    fn cancel_response(&mut self) -> Value {
        self.session_task_collection_started = None;
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
        self.paseo()?.list_sessions(include_archived).map(|rows| {
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
                        name: bounded_display(
                            raw.get("name").and_then(Value::as_str).unwrap_or_default(),
                            128,
                            "(untitled)",
                        ),
                        provider: bounded_display(&session_provider(&raw), 256, "unknown"),
                        state: bounded_display(
                            raw.get("status")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            64,
                            "unknown",
                        ),
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

fn synthetic_reply_id(host_id: &str, session_id: &str, reply_text: &str) -> ReplyId {
    let mut hasher = Sha256::new();
    hasher.update(host_id.as_bytes());
    hasher.update([0]);
    hasher.update(session_id.as_bytes());
    hasher.update([0]);
    hasher.update(reply_text.as_bytes());
    let mut reply_id = String::from("reply-");
    for byte in hasher.finalize() {
        write!(&mut reply_id, "{byte:02x}").expect("writing to String cannot fail");
    }
    ReplyId::new(reply_id).expect("digest ID is valid")
}

fn session_provider(raw: &Value) -> String {
    let provider = raw.get("provider").and_then(Value::as_str);
    let model = raw.get("model").and_then(Value::as_str);
    match (provider, model) {
        (Some(provider), Some(model)) if !provider.is_empty() && !model.is_empty() => {
            format!("{provider}/{model}")
        }
        (Some(provider), _) if !provider.is_empty() => provider.to_owned(),
        (_, Some(model)) if !model.is_empty() => model.to_owned(),
        _ => "unknown".to_owned(),
    }
}

fn bounded_display(value: &str, maximum: usize, fallback: &str) -> String {
    let bounded = value
        .chars()
        .filter(|character| !character.is_control())
        .take(maximum)
        .collect::<String>();
    if bounded.trim().is_empty() {
        fallback.to_owned()
    } else {
        bounded
    }
}

impl<E: ProcessExecutor + Clone> ToolEngine<E> {
    /// Build one connection-scoped engine from trusted profiles and a shared alpha credential.
    ///
    /// # Errors
    ///
    /// Returns an error unless exactly one profile is the default.
    pub fn configured(
        profiles: &[PaseoHostProfile],
        executor: &E,
        binary: &str,
        password: Option<&str>,
        proposal_ttl_ms: u64,
    ) -> Result<Self, &'static str> {
        let hosts = profiles
            .iter()
            .cloned()
            .map(|profile| {
                let paseo = password.map(|password| {
                    PaseoAdapter::new(
                        (*executor).clone(),
                        binary.to_owned(),
                        password.to_owned(),
                        profile.target.clone(),
                    )
                });
                PaseoHost::new(profile, paseo)
            })
            .collect();
        Self::from_hosts(hosts, proposal_ttl_ms)
    }
}

fn exact_string<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    let object = args.as_object()?;
    (object.len() == 1)
        .then(|| object.get(key).and_then(Value::as_str))
        .flatten()
        .filter(|value| !value.is_empty())
}

fn timeline_response(page: TimelinePage) -> Value {
    let next_cursor = page.next_cursor();
    let mut response = json!({
        "ok": true,
        "entries": page.entries.into_iter().map(|entry| json!({
            "operation_id": entry.operation_id,
            "summary_id": entry.summary_id,
            "destination_thread_id": entry.destination_thread_id,
            "state": entry.state,
            "timestamp_ms": entry.timestamp_ms,
            "receiver_message_id": entry.receiver_message_id
        })).collect::<Vec<_>>()
    });
    if let Some(cursor) = next_cursor {
        response
            .as_object_mut()
            .expect("timeline response object")
            .insert(
                "next_cursor".to_owned(),
                Value::String(encode_timeline_cursor(cursor)),
            );
    }
    response
}

fn encode_timeline_cursor(cursor: TimelineCursor) -> String {
    let mut payload = [0_u8; TIMELINE_CURSOR_BYTES];
    payload[0] = TIMELINE_CURSOR_VERSION;
    payload[1..33].copy_from_slice(&cursor.filter_fingerprint());
    payload[33..41].copy_from_slice(&cursor.snapshot_max_sequence().to_be_bytes());
    payload[41..49].copy_from_slice(&cursor.before_sequence().to_be_bytes());
    payload[49..57].copy_from_slice(&cursor.snapshot_min_sequence().to_be_bytes());
    URL_SAFE_NO_PAD.encode(payload)
}

fn decode_timeline_cursor(cursor: &str) -> Option<TimelineCursor> {
    if cursor.len() != TIMELINE_CURSOR_ENCODED_LENGTH {
        return None;
    }
    let mut payload = [0_u8; TIMELINE_CURSOR_BYTES];
    let decoded_length = URL_SAFE_NO_PAD.decode_slice(cursor, &mut payload).ok()?;
    if decoded_length != TIMELINE_CURSOR_BYTES || payload[0] != TIMELINE_CURSOR_VERSION {
        return None;
    }
    let filter_fingerprint = payload[1..33].try_into().ok()?;
    let snapshot_max_sequence = u64::from_be_bytes(payload[33..41].try_into().ok()?);
    let before_sequence = u64::from_be_bytes(payload[41..49].try_into().ok()?);
    let snapshot_min_sequence = u64::from_be_bytes(payload[49..57].try_into().ok()?);
    (snapshot_max_sequence > 0
        && before_sequence > 0
        && snapshot_min_sequence > 0
        && snapshot_min_sequence <= before_sequence
        && before_sequence <= snapshot_max_sequence
        && i64::try_from(snapshot_max_sequence).is_ok())
    .then(|| {
        TimelineCursor::from_parts(
            filter_fingerprint,
            snapshot_max_sequence,
            before_sequence,
            snapshot_min_sequence,
        )
    })
}

fn error(code: &'static str) -> Value {
    json!({"ok": false, "error": code})
}

fn session_task_required() -> Value {
    json!({
        "ok": false,
        "error": "session_task_required",
        "spoken_hint": "What should the new session work on?"
    })
}
