//! Single-process browser runtime.

use std::sync::{Arc, Mutex};

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
use serde_json::{Value, json};
use tower_http::services::ServeDir;

use crate::{
    clock::Clock,
    commands::CommandState,
    config::Config,
    journal::Journal,
    paseo::{PaseoAdapter, SystemProcessExecutor},
    realtime::{self, RealtimeConnector},
    secrets::Secrets,
    tools::ToolEngine,
};

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
}

/// Injected runtime boundaries constructed by the production composition root.
pub struct RuntimeDependencies {
    /// Monotonic time source.
    pub clock: Arc<dyn Clock>,
    /// HTTP transport used by local summarisation.
    pub http_client: reqwest::Client,
    /// Outbound `OpenAI` Realtime WebSocket connector.
    pub realtime_connector: Arc<dyn RealtimeConnector>,
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
    let mode = if secrets.openai_api_key.is_some() && !config.force_mock {
        "real"
    } else {
        "mock"
    };
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

async fn browser_session(socket: WebSocket, state: Arc<AppState>) {
    if let Some(api_key) = state
        .openai_api_key
        .clone()
        .filter(|_| !state.config.force_mock)
    {
        realtime::run(
            socket,
            state.config.clone(),
            api_key,
            state.paseo_password.clone(),
            realtime::RealtimeDependencies {
                journal: Arc::clone(&state.journal),
                clock: Arc::clone(&state.clock),
                http_client: state.http_client.clone(),
                connector: Arc::clone(&state.realtime_connector),
            },
        )
        .await;
        return;
    }
    mock_session(socket, state).await;
}

#[allow(clippy::too_many_lines)]
async fn mock_session(socket: WebSocket, state: Arc<AppState>) {
    let paseo = state.paseo_password.clone().map(|password| {
        PaseoAdapter::new(
            SystemProcessExecutor,
            state.config.paseo_bin.clone(),
            password,
            state.config.paseo_host.clone(),
        )
    });
    let mut tools = ToolEngine::new(paseo, state.config.proposal_ttl_ms)
        .with_journal(Arc::clone(&state.journal))
        .with_log_tail_entries(state.config.log_tail_entries);
    let mut commands = CommandState::default();
    let mut heard_audio = false;
    let (mut sender, mut receiver) = socket.split();
    if sender
        .send(Message::Text(
            json!({ "type": "mode", "mode": state.mode })
                .to_string()
                .into(),
        ))
        .await
        .is_err()
    {
        return;
    }
    while let Some(Ok(message)) = receiver.next().await {
        match message {
            Message::Text(text) => {
                let Ok(control) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                if control.get("type").and_then(Value::as_str) == Some("hello") {
                    let _ = sender
                        .send(Message::Text(
                            json!({ "type": "mode", "mode": state.mode })
                                .to_string()
                                .into(),
                        ))
                        .await;
                } else {
                    match control.get("type").and_then(Value::as_str) {
                        Some("ptt_start") => heard_audio = false,
                        Some("ptt_end") if heard_audio => {
                            tools.observe_user_turn();
                            speak(
                                &mut sender,
                                "Mock mode has no speech recognition. Type a text turn instead.",
                            )
                            .await;
                        }
                        Some("text_turn") => {
                            let Some(text) = control.get("text").and_then(Value::as_str) else {
                                continue;
                            };
                            tools.observe_user_turn();
                            let _ = send_json(
                                &mut sender,
                                json!({"type":"state","state":"responding"}),
                            )
                            .await;
                            let now_ms = state.clock.now_ms();
                            let (reply, proposal) = tokio::task::block_in_place(|| {
                                let result = commands.run(&mut tools, text, now_ms);
                                (result.speech, result.proposal)
                            });
                            if let Some(echo) = proposal {
                                let _ =
                                    send_json(&mut sender, json!({"type":"proposal","echo":echo}))
                                        .await;
                            }
                            speak(&mut sender, &reply).await;
                            let _ = send_json(&mut sender, json!({"type":"state","state":"ready"}))
                                .await;
                        }
                        Some("confirm_proposal") => {
                            tools.observe_user_turn();
                            let now_ms = state.clock.now_ms();
                            let (reply, _) = tokio::task::block_in_place(|| {
                                let result = commands.run(&mut tools, "yes", now_ms);
                                (result.speech, result.proposal)
                            });
                            let _ = send_json(&mut sender, json!({"type":"proposal","echo":null}))
                                .await;
                            speak(&mut sender, &reply).await;
                        }
                        Some("cancel_proposal") => {
                            let now_ms = state.clock.now_ms();
                            let _ = tokio::task::block_in_place(|| {
                                commands.run(&mut tools, "no", now_ms)
                            });
                            let _ = send_json(&mut sender, json!({"type":"proposal","echo":null}))
                                .await;
                        }
                        _ => {}
                    }
                }
            }
            Message::Binary(_) => heard_audio = true,
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) => {}
        }
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
