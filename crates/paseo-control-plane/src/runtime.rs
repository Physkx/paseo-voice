//! Single-process browser runtime.

use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt as _, StreamExt as _};
use paseo_safety_core::SummaryQueue;
use serde_json::{Value, json};
use tower_http::services::ServeDir;

use crate::{
    clock::Clock,
    commands::CommandState,
    config::{Config, EndpointLocation},
    dictation::DictationCleaner,
    journal::Journal,
    paseo::ProcessExecutor,
    realtime::{self, RealtimeConnector},
    secrets::Secrets,
    tools::{ProposalPresentation, ToolEngine, parse_strict_json_object, proposal_frame},
    voice_mode::VoiceMode,
};

const MAX_TERMINAL_DICTATION_IDS: usize = 64;

/// Build the shared HTTP transport for summarisation and dictation cleanup.
///
/// # Errors
///
/// Returns a bounded startup error if the client cannot be constructed.
pub fn build_model_http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "HTTP model client construction failed".to_owned())
}

/// Select live Realtime only when the configured endpoint has the credentials it requires.
#[must_use]
pub fn realtime_mode(config: &Config, openai_api_key: Option<&str>) -> &'static str {
    let available = config.realtime_endpoint().is_ok_and(|endpoint| {
        endpoint.location != EndpointLocation::OpenAiCloud || openai_api_key.is_some()
    });
    if !config.force_mock && available {
        "real"
    } else {
        "mock"
    }
}

#[derive(Clone)]
struct AppState {
    mode: &'static str,
    config: Config,
    openai_api_key: Option<String>,
    paseo_password: Option<String>,
    journal: Arc<Mutex<Journal>>,
    clock: Arc<dyn Clock>,
    http_client: reqwest::Client,
    realtime_connector: Arc<dyn RealtimeConnector>,
    dictation_cleaner: Arc<dyn DictationCleaner>,
    process_executor: Arc<dyn ProcessExecutor>,
    summary_queue: Arc<Mutex<SummaryQueue>>,
}

struct MockLiveRecording {
    id: u64,
    expected_summary_id: Option<String>,
}

struct MockDictationOperation {
    id: String,
    recording_id: u64,
    expected_summary_id: String,
}

/// Injected runtime boundaries constructed by the production composition root.
pub struct RuntimeDependencies {
    /// Monotonic time source.
    pub clock: Arc<dyn Clock>,
    /// HTTP transport used by model summarisation.
    pub http_client: reqwest::Client,
    /// Outbound `OpenAI` Realtime WebSocket connector.
    pub realtime_connector: Arc<dyn RealtimeConnector>,
    /// Text-only dictation cleanup service.
    pub dictation_cleaner: Arc<dyn DictationCleaner>,
    /// Direct process executor used by Paseo adapters.
    pub process_executor: Arc<dyn ProcessExecutor>,
}

/// Serve using injected clock, HTTP client, and listening socket boundaries.
///
/// # Errors
///
/// Returns an error when initialisation or serving fails.
pub async fn serve(
    config: Config,
    secrets: Secrets,
    dependencies: RuntimeDependencies,
    listener: tokio::net::TcpListener,
) -> Result<(), String> {
    let mode = realtime_mode(&config, secrets.openai_api_key.as_deref());
    if let Some(parent) = config.journal_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("journal directory failed: {error}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| format!("journal permissions failed: {error}"))?;
        }
    }
    let journal =
        Journal::open(&config.journal_path).map_err(|error| format!("journal failed: {error}"))?;
    journal
        .recover(0)
        .map_err(|error| format!("journal recovery failed: {error}"))?;
    let state = Arc::new(AppState {
        mode,
        config: config.clone(),
        openai_api_key: secrets.openai_api_key,
        paseo_password: secrets.paseo_password,
        journal: Arc::new(Mutex::new(journal)),
        clock: dependencies.clock,
        http_client: dependencies.http_client,
        realtime_connector: dependencies.realtime_connector,
        dictation_cleaner: dependencies.dictation_cleaner,
        process_executor: dependencies.process_executor,
        summary_queue: Arc::new(Mutex::new(SummaryQueue::new())),
    });
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/ws", get(websocket))
        .fallback_service(ServeDir::new(config.public_dir).append_index_html_on_directories(true))
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
        .map_err(|error| format!("server failed: {error}"))
}

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({ "ok": true, "mode": state.mode, "backend": "rust" }))
}

async fn websocket(
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !same_origin_or_non_browser(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    upgrade
        .on_upgrade(move |socket| browser_session(socket, state))
        .into_response()
}

fn same_origin_or_non_browser(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let (Ok(origin), Some(host)) = (origin.to_str(), headers.get(header::HOST)) else {
        return false;
    };
    let Ok(host) = host.to_str() else {
        return false;
    };
    origin == format!("http://{host}") || origin == format!("https://{host}")
}

async fn browser_session(mut socket: WebSocket, state: Arc<AppState>) {
    if !realtime::negotiate_browser_protocol(&mut socket).await {
        return;
    }
    if state.mode == "real" {
        Box::pin(realtime::run(
            socket,
            state.config.clone(),
            state.openai_api_key.clone(),
            state.paseo_password.clone(),
            realtime::RealtimeDependencies {
                journal: Arc::clone(&state.journal),
                clock: Arc::clone(&state.clock),
                http_client: state.http_client.clone(),
                connector: Arc::clone(&state.realtime_connector),
                dictation_cleaner: Arc::clone(&state.dictation_cleaner),
                process_executor: Arc::clone(&state.process_executor),
                summary_queue: Arc::clone(&state.summary_queue),
            },
        ))
        .await;
        return;
    }
    mock_session(socket, state).await;
}

#[allow(clippy::too_many_lines)]
async fn mock_session(socket: WebSocket, state: Arc<AppState>) {
    let Ok(mut tools) = ToolEngine::configured(
        &state.config.paseo_hosts,
        &state.process_executor,
        &state.config.paseo_bin,
        state.paseo_password.as_deref(),
        state.config.proposal_ttl_ms,
    ) else {
        return;
    };
    tools = tools
        .with_journal(Arc::clone(&state.journal))
        .with_log_tail_entries(state.config.log_tail_entries);
    if let Ok(queue) = state.summary_queue.lock() {
        tools.seed_queue(queue.clone());
    }
    let mut commands = CommandState::default();
    let mut browser_proposal: Option<ProposalPresentation> = None;
    let mut heard_audio = false;
    let mut live_response_recording: Option<MockLiveRecording> = None;
    let mut last_recording_id = 0_u64;
    let mut voice_mode = VoiceMode::default();
    let mut dictation_sequence = 0_u64;
    let mut active_dictation: Option<MockDictationOperation> = None;
    let mut terminal_dictation_ids = HashSet::<String>::new();
    let mut terminal_dictation_order = VecDeque::<String>::new();
    let mut last_text_turn_id = 0_u64;
    let (mut sender, mut receiver) = socket.split();
    let _ = send_json(&mut sender, json!({"type":"mode","mode":state.mode})).await;
    let _ = send_json(&mut sender, voice_mode.frame()).await;
    let _ = send_json(
        &mut sender,
        crate::dictation::capability_frame(&state.config, false),
    )
    .await;
    let _ = send_json(&mut sender, host_frame(&tools)).await;
    let _ = send_json(&mut sender, dashboard_frame(&mut tools)).await;
    while let Some(Ok(message)) = receiver.next().await {
        match message {
            Message::Text(text) => {
                let Some(control) = parse_strict_json_object(&text) else {
                    continue;
                };
                if control.get("type").and_then(Value::as_str) == Some("hello") {
                    if !realtime::exact_browser_hello(&control) {
                        let _ = send_json(&mut sender, realtime::browser_protocol_mismatch_frame())
                            .await;
                        let _ = sender.close().await;
                        break;
                    }
                    let _ = send_json(&mut sender, realtime::browser_protocol_ready_frame()).await;
                    let _ = sender
                        .send(Message::Text(
                            json!({ "type": "mode", "mode": state.mode })
                                .to_string()
                                .into(),
                        ))
                        .await;
                    let _ = send_json(&mut sender, voice_mode.frame()).await;
                    let _ = send_json(
                        &mut sender,
                        crate::dictation::capability_frame(&state.config, false),
                    )
                    .await;
                    let _ = send_json(&mut sender, host_frame(&tools)).await;
                    let _ = send_json(&mut sender, dashboard_frame(&mut tools)).await;
                } else {
                    match control.get("type").and_then(Value::as_str) {
                        Some("set_voice_mode") => {
                            if live_response_recording.is_some()
                                && control
                                    .get("mode")
                                    .and_then(Value::as_str)
                                    .and_then(VoiceMode::parse)
                                    .is_some_and(|requested| requested != voice_mode)
                            {
                                let _ = send_json(
                                    &mut sender,
                                    json!({
                                        "type":"error",
                                        "message":"Finish or abort active recording before changing mode."
                                    }),
                                )
                                .await;
                                continue;
                            }
                            if active_dictation.is_some() {
                                let _ = send_json(
                                    &mut sender,
                                    json!({
                                        "type":"error",
                                        "message":"Cancel active dictation before changing mode."
                                    }),
                                )
                                .await;
                                continue;
                            }
                            match voice_mode
                                .select_from_control(&control, tools.has_pending_action())
                            {
                                Ok(frame) => {
                                    let _ = send_json(&mut sender, frame).await;
                                }
                                Err(message) => {
                                    let _ = send_json(
                                        &mut sender,
                                        json!({"type":"error","message":message}),
                                    )
                                    .await;
                                }
                            }
                        }
                        Some("ptt_start") => {
                            let Some(recording_id) = recording_start_id(&control) else {
                                continue;
                            };
                            if recording_id <= last_recording_id {
                                continue;
                            }
                            last_recording_id = recording_id;
                            let Some(expected_summary_id) = tools.accept_turn_context(
                                &control,
                                &["type", "recording_id", "summary_id"],
                            ) else {
                                send_mock_recording_rejected(&mut sender, voice_mode, recording_id)
                                    .await;
                                continue;
                            };
                            if voice_mode == VoiceMode::Dictation
                                && expected_summary_id.as_deref().is_none()
                            {
                                send_mock_recording_rejected(&mut sender, voice_mode, recording_id)
                                    .await;
                                continue;
                            }
                            let expected_summary_id =
                                expected_summary_id.as_deref().map(str::to_owned);
                            if tools.has_pending_action() {
                                send_mock_recording_rejected(&mut sender, voice_mode, recording_id)
                                    .await;
                                continue;
                            }
                            match voice_mode {
                                VoiceMode::LiveResponse => {
                                    if live_response_recording.is_some() {
                                        send_mock_recording_rejected(
                                            &mut sender,
                                            voice_mode,
                                            recording_id,
                                        )
                                        .await;
                                        continue;
                                    }
                                    heard_audio = false;
                                    live_response_recording = Some(MockLiveRecording {
                                        id: recording_id,
                                        expected_summary_id,
                                    });
                                }
                                VoiceMode::Dictation => {
                                    if active_dictation.is_some() {
                                        send_mock_recording_rejected(
                                            &mut sender,
                                            voice_mode,
                                            recording_id,
                                        )
                                        .await;
                                        continue;
                                    }
                                    let Some(expected_summary_id) = expected_summary_id else {
                                        continue;
                                    };
                                    heard_audio = false;
                                    let Some(next_sequence) = dictation_sequence.checked_add(1)
                                    else {
                                        continue;
                                    };
                                    dictation_sequence = next_sequence;
                                    let operation_id =
                                        format!("mock-dictation-{dictation_sequence}");
                                    let operation = MockDictationOperation {
                                        id: operation_id,
                                        recording_id,
                                        expected_summary_id,
                                    };
                                    let _ = send_json(
                                        &mut sender,
                                        json!({
                                            "type":"dictation_operation",
                                            "operation_id":operation.id,
                                            "recording_id":operation.recording_id
                                        }),
                                    )
                                    .await;
                                    active_dictation = Some(operation);
                                }
                            }
                            let _ = send_json(&mut sender, json!({"type":"flush_audio"})).await;
                        }
                        Some("ptt_abort") => {
                            if voice_mode != VoiceMode::LiveResponse {
                                continue;
                            }
                            let Some(recording_id) = live_recording_id(&control) else {
                                continue;
                            };
                            if !exact_live_recording_terminal(&control)
                                || live_response_recording
                                    .as_ref()
                                    .is_none_or(|recording| recording.id != recording_id)
                            {
                                continue;
                            }
                            live_response_recording = None;
                            heard_audio = false;
                            let _ = send_json(&mut sender, json!({"type":"flush_audio"})).await;
                        }
                        Some("ptt_end") => match voice_mode {
                            VoiceMode::LiveResponse => {
                                let Some(recording_id) = live_recording_id(&control) else {
                                    continue;
                                };
                                if !exact_live_recording_terminal(&control)
                                    || live_response_recording
                                        .as_ref()
                                        .is_none_or(|recording| recording.id != recording_id)
                                {
                                    continue;
                                }
                                let Some(recording) = live_response_recording.take() else {
                                    continue;
                                };
                                let _ = send_json(&mut sender, json!({"type":"flush_audio"})).await;
                                if tools.active_summary_id()
                                    != recording.expected_summary_id.as_deref()
                                {
                                    heard_audio = false;
                                    let _ = send_json(
                                        &mut sender,
                                        json!({"type":"state","state":"ready"}),
                                    )
                                    .await;
                                    continue;
                                }
                                if std::mem::take(&mut heard_audio) {
                                    let _ = send_json(
                                        &mut sender,
                                        json!({"type":"state","state":"responding"}),
                                    )
                                    .await;
                                    tools.observe_user_turn();
                                    speak(
                                        &mut sender,
                                        "Mock mode has no speech recognition. Type a text turn instead.",
                                    )
                                    .await;
                                }
                                let _ =
                                    send_json(&mut sender, json!({"type":"state","state":"ready"}))
                                        .await;
                            }
                            VoiceMode::Dictation => {
                                let Some(operation_id) =
                                    dictation_terminal_operation_id(&control, "ptt_end")
                                else {
                                    continue;
                                };
                                if active_dictation
                                    .as_ref()
                                    .is_none_or(|operation| operation.id != operation_id)
                                {
                                    continue;
                                }
                                if let Some(operation) = active_dictation.take() {
                                    if tools.active_summary_id()
                                        != Some(operation.expected_summary_id.as_str())
                                    {
                                        heard_audio = false;
                                        let _ = send_json(
                                            &mut sender,
                                            json!({
                                                "type":"dictation_cancelled",
                                                "operation_id":operation.id
                                            }),
                                        )
                                        .await;
                                        let _ = send_json(
                                            &mut sender,
                                            json!({"type":"state","state":"ready"}),
                                        )
                                        .await;
                                        continue;
                                    }
                                    let terminal = if heard_audio {
                                        json!({
                                            "type":"dictation_result",
                                            "operation_id":operation.id,
                                            "status":"degraded",
                                            "text":"Mock dictation text."
                                        })
                                    } else {
                                        json!({
                                            "type":"dictation_empty",
                                            "operation_id":operation.id
                                        })
                                    };
                                    heard_audio = false;
                                    remember_terminal_dictation(
                                        &mut terminal_dictation_ids,
                                        &mut terminal_dictation_order,
                                        operation.id.clone(),
                                    );
                                    let _ = send_json(&mut sender, terminal).await;
                                    let _ = send_json(
                                        &mut sender,
                                        json!({"type":"state","state":"ready"}),
                                    )
                                    .await;
                                }
                            }
                        },
                        Some("cancel_dictation") => {
                            let Some(operation_id) =
                                dictation_terminal_operation_id(&control, "cancel_dictation")
                            else {
                                continue;
                            };
                            if terminal_dictation_ids.remove(operation_id) {
                                let _ = send_json(
                                    &mut sender,
                                    json!({
                                        "type":"dictation_cancelled",
                                        "operation_id":operation_id
                                    }),
                                )
                                .await;
                                let _ =
                                    send_json(&mut sender, json!({"type":"state","state":"ready"}))
                                        .await;
                                continue;
                            }
                            if active_dictation
                                .as_ref()
                                .is_none_or(|operation| operation.id != operation_id)
                            {
                                continue;
                            }
                            heard_audio = false;
                            if let Some(operation) = active_dictation.take() {
                                let _ = send_json(
                                    &mut sender,
                                    json!({
                                        "type":"dictation_cancelled",
                                        "operation_id":operation.id
                                    }),
                                )
                                .await;
                                let _ =
                                    send_json(&mut sender, json!({"type":"state","state":"ready"}))
                                        .await;
                            }
                        }
                        Some("text_turn") => {
                            let Some((turn_id, text)) = realtime::exact_text_turn(&control) else {
                                continue;
                            };
                            if turn_id <= last_text_turn_id {
                                continue;
                            }
                            last_text_turn_id = turn_id;
                            let Some(expected_summary_id) = tools.accept_turn_context(
                                &control,
                                &["type", "text", "summary_id", "turn_id"],
                            ) else {
                                let _ = send_json(
                                    &mut sender,
                                    realtime::text_turn_rejected_frame(turn_id),
                                )
                                .await;
                                continue;
                            };
                            let expected_summary_id =
                                expected_summary_id.as_deref().map(str::to_owned);
                            if active_dictation.is_some() {
                                let _ = send_json(
                                    &mut sender,
                                    realtime::text_turn_rejected_frame(turn_id),
                                )
                                .await;
                                continue;
                            }
                            if tools.has_pending_action() {
                                let _ = send_json(
                                    &mut sender,
                                    realtime::text_turn_rejected_frame(turn_id),
                                )
                                .await;
                                continue;
                            }
                            let _ =
                                send_json(&mut sender, realtime::text_turn_accepted_frame(turn_id))
                                    .await;
                            tools.observe_user_turn();
                            let _ = send_json(
                                &mut sender,
                                json!({"type":"state","state":"responding"}),
                            )
                            .await;
                            let now_ms = state.clock.now_ms();
                            let (reply, proposal, refresh_dashboard) =
                                tokio::task::block_in_place(|| {
                                    let result = tools.with_model_turn_context(
                                        expected_summary_id.as_deref(),
                                        |tools| commands.run(tools, text, now_ms),
                                    );
                                    let refresh_dashboard = result.refresh_dashboard();
                                    (result.speech, result.proposal, refresh_dashboard)
                                });
                            if live_response_recording.as_ref().is_some_and(|recording| {
                                tools.active_summary_id()
                                    != recording.expected_summary_id.as_deref()
                            }) {
                                live_response_recording = None;
                                heard_audio = false;
                                let _ = send_json(&mut sender, json!({"type":"flush_audio"})).await;
                            }
                            if let Some(echo) = proposal
                                && let Some(token) = tools.pending_action_token()
                            {
                                if live_response_recording.is_some() {
                                    live_response_recording = None;
                                    heard_audio = false;
                                }
                                browser_proposal = Some(ProposalPresentation::new(token, echo));
                                let _ = send_json(
                                    &mut sender,
                                    proposal_frame(browser_proposal.as_ref()),
                                )
                                .await;
                            }
                            let dashboard = if refresh_dashboard {
                                dashboard_frame(&mut tools)
                            } else {
                                with_frame_type(tools.presentation_snapshot(), "dashboard_state")
                            };
                            let _ = send_json(&mut sender, dashboard).await;
                            speak(&mut sender, &reply).await;
                            let _ = send_json(&mut sender, json!({"type":"state","state":"ready"}))
                                .await;
                        }
                        Some("select_host") => {
                            let Some(host_id) = control.get("host_id").and_then(Value::as_str)
                            else {
                                continue;
                            };
                            let host_changed = tools.selected_host_id() != host_id;
                            let known_host_change = host_changed && tools.has_host_id(host_id);
                            let cancelled_operation = if known_host_change {
                                commands.clear_pending();
                                browser_proposal = None;
                                live_response_recording = None;
                                heard_audio = false;
                                active_dictation.take()
                            } else {
                                None
                            };
                            let selected = tools.select_host(host_id);
                            if selected.get("ok").and_then(Value::as_bool) == Some(false) {
                                let _ = send_json(
                                    &mut sender,
                                    json!({"type":"error","message":"Unknown Paseo host profile."}),
                                )
                                .await;
                            } else if !host_changed {
                                let _ =
                                    send_json(&mut sender, with_frame_type(selected, "host_state"))
                                        .await;
                                let _ = send_json(&mut sender, dashboard_frame(&mut tools)).await;
                            } else {
                                if let Some(operation) = cancelled_operation {
                                    let _ = send_json(
                                        &mut sender,
                                        json!({
                                            "type":"dictation_cancelled",
                                            "operation_id":operation.id
                                        }),
                                    )
                                    .await;
                                    let _ = send_json(
                                        &mut sender,
                                        json!({"type":"state","state":"ready"}),
                                    )
                                    .await;
                                }
                                let _ =
                                    send_json(&mut sender, with_frame_type(selected, "host_state"))
                                        .await;
                                let _ = send_json(&mut sender, dashboard_frame(&mut tools)).await;
                                let _ =
                                    send_json(&mut sender, json!({"type":"proposal","echo":null}))
                                        .await;
                            }
                        }
                        Some("confirm_proposal") => {
                            let Some(proposal) = browser_proposal
                                .as_ref()
                                .filter(|proposal| proposal.matches(&control))
                            else {
                                let _ = send_json(
                                    &mut sender,
                                    proposal_frame(browser_proposal.as_ref()),
                                )
                                .await;
                                continue;
                            };
                            let token = proposal.token().to_owned();
                            tools.observe_user_turn();
                            let now_ms = state.clock.now_ms();
                            let result = tokio::task::block_in_place(|| {
                                tools.dispatch(
                                    "confirm_action",
                                    &json!({"token":token}).to_string(),
                                    now_ms,
                                )
                            });
                            if !tools.has_pending_action() {
                                browser_proposal = None;
                                commands.clear_pending();
                            }
                            let _ =
                                send_json(&mut sender, proposal_frame(browser_proposal.as_ref()))
                                    .await;
                            let _ = send_json(&mut sender, host_frame(&tools)).await;
                            let _ = send_json(&mut sender, dashboard_frame(&mut tools)).await;
                            speak(&mut sender, &mock_confirmation_speech(&result)).await;
                        }
                        Some("cancel_proposal") => {
                            if !browser_proposal
                                .as_ref()
                                .is_some_and(|proposal| proposal.matches(&control))
                            {
                                let _ = send_json(
                                    &mut sender,
                                    proposal_frame(browser_proposal.as_ref()),
                                )
                                .await;
                                continue;
                            }
                            let now_ms = state.clock.now_ms();
                            let _ = tokio::task::block_in_place(|| {
                                tools.dispatch("cancel_action", "{}", now_ms)
                            });
                            if !tools.has_pending_action() {
                                browser_proposal = None;
                                commands.clear_pending();
                            }
                            let _ =
                                send_json(&mut sender, proposal_frame(browser_proposal.as_ref()))
                                    .await;
                            let _ = send_json(&mut sender, dashboard_frame(&mut tools)).await;
                        }
                        _ => {}
                    }
                }
            }
            Message::Binary(audio)
                if match voice_mode {
                    VoiceMode::LiveResponse => live_response_recording.is_some(),
                    VoiceMode::Dictation => active_dictation.is_some(),
                } =>
            {
                if realtime::valid_browser_pcm_frame(&audio) {
                    heard_audio = true;
                    continue;
                }
                let rejected_recording_id = match voice_mode {
                    VoiceMode::LiveResponse => {
                        live_response_recording.take().map(|recording| recording.id)
                    }
                    VoiceMode::Dictation => active_dictation
                        .take()
                        .map(|operation| operation.recording_id),
                };
                heard_audio = false;
                if let Some(recording_id) = rejected_recording_id {
                    let _ = send_json(
                        &mut sender,
                        realtime::invalid_audio_rejection_frame(voice_mode, recording_id),
                    )
                    .await;
                    let _ = send_json(&mut sender, json!({"type":"flush_audio"})).await;
                }
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => {}
        }
    }
    // Persist the content-free summary queue for the next connection. The mock
    // runtime is synchronous with no response rollback pending here.
    if let Ok(mut queue) = state.summary_queue.lock() {
        *queue = tools.queue_snapshot();
    }
}

fn live_recording_id(control: &Value) -> Option<u64> {
    control
        .get("recording_id")?
        .as_u64()
        .filter(|recording_id| (1..=2_147_483_647).contains(recording_id))
}

fn recording_start_id(control: &Value) -> Option<u64> {
    let object = control.as_object()?;
    if object.len() != 3
        || object.get("type").and_then(Value::as_str) != Some("ptt_start")
        || !matches!(
            object.get("summary_id"),
            Some(Value::Null | Value::String(_))
        )
    {
        return None;
    }
    live_recording_id(control)
}

fn exact_live_recording_terminal(control: &Value) -> bool {
    control.as_object().is_some_and(|object| {
        object.len() == 2 && object.contains_key("type") && object.contains_key("recording_id")
    })
}

fn dictation_terminal_operation_id<'a>(control: &'a Value, expected_type: &str) -> Option<&'a str> {
    let object = control.as_object()?;
    (object.len() == 2 && object.get("type").and_then(Value::as_str) == Some(expected_type))
        .then(|| object.get("operation_id").and_then(Value::as_str))?
}

fn host_frame<E: crate::paseo::ProcessExecutor>(tools: &ToolEngine<E>) -> Value {
    with_frame_type(tools.host_state(), "host_state")
}

fn dashboard_frame<E: crate::paseo::ProcessExecutor>(tools: &mut ToolEngine<E>) -> Value {
    with_frame_type(tools.dashboard_state(), "dashboard_state")
}

fn with_frame_type(mut value: Value, frame_type: &str) -> Value {
    value["type"] = Value::String(frame_type.to_owned());
    value
}

fn remember_terminal_dictation(
    ids: &mut HashSet<String>,
    order: &mut VecDeque<String>,
    operation_id: String,
) {
    if !ids.insert(operation_id.clone()) {
        return;
    }
    order.push_back(operation_id);
    while order.len() > MAX_TERMINAL_DICTATION_IDS {
        if let Some(expired) = order.pop_front() {
            ids.remove(&expired);
        }
    }
}

async fn send_mock_recording_rejected<S>(sender: &mut S, mode: VoiceMode, recording_id: u64)
where
    S: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    let _ = send_json(
        sender,
        json!({
            "type":"recording_rejected",
            "mode":mode.as_str(),
            "recording_id":recording_id,
            "message":"Recording could not start. Try again."
        }),
    )
    .await;
}

fn mock_confirmation_speech(result: &Value) -> String {
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        result
            .get("spoken_hint")
            .and_then(Value::as_str)
            .unwrap_or("Done.")
            .to_owned()
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

async fn send_json<S>(sender: &mut S, value: Value) -> Result<(), axum::Error>
where
    S: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    sender.send(Message::Text(value.to_string().into())).await
}

async fn speak<S>(sender: &mut S, text: &str)
where
    S: futures_util::Sink<Message, Error = axum::Error> + Unpin,
{
    let _ = send_json(sender, json!({"type":"transcript_delta","text":text})).await;
    let _ = send_json(sender, json!({"type":"transcript_done","text":text})).await;
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}
