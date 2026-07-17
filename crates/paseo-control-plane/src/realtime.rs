//! Browser to `OpenAI` Realtime bridge with Rust-owned tool dispatch.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::extract::ws::{Message as BrowserMessage, WebSocket};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error as RealtimeError, Message as RealtimeMessage,
        client::IntoClientRequest as _,
        http::{HeaderValue, Request, Response, header::AUTHORIZATION},
    },
};

type RealtimeStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type ConnectResult = Result<(RealtimeStream, Response<Option<Vec<u8>>>), RealtimeError>;
type ConnectFuture = Pin<Box<dyn Future<Output = ConnectResult> + Send>>;

/// Injected outbound Realtime WebSocket boundary.
pub trait RealtimeConnector: Send + Sync {
    /// Connect an already validated and authenticated request.
    fn connect(&self, request: Request<()>) -> ConnectFuture;
}

/// Production Realtime connector.
pub struct SystemRealtimeConnector;

impl RealtimeConnector for SystemRealtimeConnector {
    fn connect(&self, request: Request<()>) -> ConnectFuture {
        Box::pin(connect_async(request))
    }
}

use crate::{
    clock::Clock,
    config::Config,
    journal::Journal,
    paseo::{PaseoAdapter, SystemProcessExecutor},
    tools::{ToolEngine, definitions},
};

/// Injected state and boundaries for one Realtime session.
pub struct RealtimeDependencies {
    /// Shared content-free delivery journal.
    pub journal: Arc<Mutex<Journal>>,
    /// Monotonic interaction clock.
    pub clock: Arc<dyn Clock>,
    /// HTTP client for local summarisation.
    pub http_client: reqwest::Client,
    /// Outbound Realtime connector.
    pub connector: Arc<dyn RealtimeConnector>,
}

const INSTRUCTIONS: &str = concat!(
    "You are Paseo Voice, a concise hands-free assistant for coding-agent sessions. ",
    "Tools are the only source of truth. Read a reply before proposing a response. ",
    "send_message only proposes a response bound to the reply most recently read. ",
    "Read its spoken_echo aloud, then wait for a new explicit user turn. ",
    "Never call confirm_action in the proposal turn. Silence or ambiguity is not consent. ",
    "Paseo permission approvals are never available by voice."
);

/// Run one live Realtime session until either socket closes.
#[allow(clippy::too_many_lines)]
pub async fn run(
    mut browser: WebSocket,
    config: Config,
    api_key: String,
    paseo_password: Option<String>,
    dependencies: RealtimeDependencies,
) {
    let authority_only = config
        .openai_base_url
        .split_once("://")
        .is_some_and(|(_, authority_and_path)| !authority_and_path.contains('/'));
    let base_url = if authority_only {
        format!("{}/", config.openai_base_url)
    } else {
        config.openai_base_url.clone()
    };
    let url = format!("{base_url}?model={}", percent_encode(&config.openai_model));
    let Ok(mut request) = url.into_client_request() else {
        send_error(&mut browser, "invalid Realtime URL").await;
        return;
    };
    let Ok(value) = HeaderValue::from_str(&format!("Bearer {api_key}")) else {
        send_error(&mut browser, "invalid Realtime credential").await;
        return;
    };
    request.headers_mut().insert(AUTHORIZATION, value);
    let Ok((mut realtime, _)) = dependencies.connector.connect(request).await else {
        send_error(&mut browser, "Realtime connection failed").await;
        return;
    };

    let paseo = paseo_password.map(|password| {
        PaseoAdapter::new(
            SystemProcessExecutor,
            config.paseo_bin.clone(),
            password,
            config.paseo_host.clone(),
        )
    });
    let mut tools = ToolEngine::new(paseo, config.proposal_ttl_ms)
        .with_journal(dependencies.journal)
        .with_log_tail_entries(config.log_tail_entries);
    let mut call_names = HashMap::<String, String>::new();
    let mut dispatched_calls = std::collections::HashSet::<String>::new();
    let mut active_response = false;
    let mut followup_queued = false;
    let mut browser_proposal_token: Option<String> = None;

    let update = json!({
        "type": "session.update",
        "session": {
            "type": "realtime",
            "instructions": INSTRUCTIONS,
            "tools": definitions(),
            "tool_choice": "auto",
            "output_modalities": ["audio"],
            "audio": {
                "input": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "turn_detection": null
                },
                "output": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "voice": config.openai_voice,
                    "speed": 1.0
                }
            }
        }
    });
    if send_realtime(&mut realtime, update).await.is_err() {
        return;
    }
    let _ = send_browser(&mut browser, json!({"type": "mode", "mode": "real"})).await;

    loop {
        tokio::select! {
            browser_message = browser.next() => {
                let Some(Ok(message)) = browser_message else { break };
                match message {
                    BrowserMessage::Binary(audio) if !audio.is_empty() => {
                        let _ = send_realtime(&mut realtime, json!({
                            "type": "input_audio_buffer.append",
                            "audio": BASE64.encode(audio)
                        })).await;
                    }
                    BrowserMessage::Text(text) => {
                        let Ok(control) = serde_json::from_str::<Value>(&text) else { continue };
                        match control.get("type").and_then(Value::as_str) {
                            Some("hello") => {
                                let _ = send_browser(&mut browser, json!({"type":"mode","mode":"real"})).await;
                            }
                            Some("ptt_start") => {
                                if active_response {
                                    let _ = send_realtime(&mut realtime, json!({"type":"response.cancel"})).await;
                                    active_response = false;
                                    let _ = send_browser(&mut browser, json!({"type":"flush_audio"})).await;
                                }
                                let _ = send_realtime(&mut realtime, json!({"type":"input_audio_buffer.clear"})).await;
                            }
                            Some("ptt_end") => {
                                tools.observe_user_turn();
                                let _ = send_realtime(&mut realtime, json!({"type":"input_audio_buffer.commit"})).await;
                                let _ = send_realtime(&mut realtime, json!({"type":"response.create"})).await;
                            }
                            Some("text_turn") => {
                                let Some(text) = control.get("text").and_then(Value::as_str).filter(|text| !text.is_empty()) else { continue };
                                tools.observe_user_turn();
                                let _ = send_realtime(&mut realtime, json!({
                                    "type":"conversation.item.create",
                                    "item":{"type":"message","role":"user","content":[{"type":"input_text","text":text}]}
                                })).await;
                                let _ = send_realtime(&mut realtime, json!({"type":"response.create"})).await;
                            }
                            Some("confirm_proposal") => {
                                if let Some(token) = browser_proposal_token.take() {
                                    tools.observe_user_turn();
                                    let now_ms = dependencies.clock.now_ms();
                                    let result = tokio::task::block_in_place(|| {
                                        tools.dispatch(
                                            "confirm_action",
                                            &json!({"token":token}).to_string(),
                                            now_ms,
                                        )
                                    });
                                    let message = if result.get("ok").and_then(Value::as_bool) == Some(true) {
                                        "Confirmed and delivered."
                                    } else if result.get("delivery").and_then(Value::as_str) == Some("outcome_unknown") {
                                        "Confirmed, but the delivery outcome is unknown. It was not retried."
                                    } else {
                                        "Confirmation was rejected. Nothing was reported delivered."
                                    };
                                    let _ = send_browser(&mut browser, json!({"type":"proposal","echo":null})).await;
                                    let _ = send_browser(&mut browser, json!({"type":"transcript_done","text":message})).await;
                                }
                            }
                            Some("cancel_proposal") => {
                                browser_proposal_token = None;
                                let now_ms = dependencies.clock.now_ms();
                                let _ = tokio::task::block_in_place(|| tools.dispatch("cancel_action", "{}", now_ms));
                                let _ = send_browser(&mut browser, json!({"type":"proposal","echo":null})).await;
                            }
                            _ => {}
                        }
                    }
                    BrowserMessage::Close(_) => break,
                    BrowserMessage::Ping(data) => {
                        let _ = browser.send(BrowserMessage::Pong(data)).await;
                    }
                    BrowserMessage::Binary(_) | BrowserMessage::Pong(_) => {}
                }
            }
            realtime_message = realtime.next() => {
                let Some(Ok(message)) = realtime_message else { break };
                let RealtimeMessage::Text(text) = message else { continue };
                let Ok(event) = serde_json::from_str::<Value>(&text) else { continue };
                let event_type = event.get("type").and_then(Value::as_str).unwrap_or_default();
                match event_type {
                    "session.created" | "session.updated" => {
                        let _ = send_browser(&mut browser, json!({"type":"state","state":"ready"})).await;
                    }
                    "response.created" => {
                        active_response = true;
                        let _ = send_browser(&mut browser, json!({"type":"state","state":"responding"})).await;
                    }
                    "response.output_audio.delta" | "response.audio.delta" => {
                        if let Some(delta) = event.get("delta").and_then(Value::as_str)
                            && let Ok(audio) = BASE64.decode(delta)
                        {
                            let _ = browser.send(BrowserMessage::Binary(audio.into())).await;
                        }
                    }
                    "response.output_audio_transcript.delta" => {
                        if let Some(text) = event.get("delta").and_then(Value::as_str) {
                            let _ = send_browser(&mut browser, json!({"type":"transcript_delta","text":text})).await;
                        }
                    }
                    "response.output_audio_transcript.done" => {
                        if let Some(text) = event.get("transcript").and_then(Value::as_str) {
                            let _ = send_browser(&mut browser, json!({"type":"transcript_done","text":text})).await;
                        }
                    }
                    "conversation.item.input_audio_transcription.completed" => {
                        if let Some(text) = event.get("transcript").and_then(Value::as_str) {
                            let _ = send_browser(&mut browser, json!({"type":"user_transcript","text":text})).await;
                        }
                    }
                    "response.output_item.added" | "response.output_item.done" => {
                        if let Some(item) = event.get("item")
                            && item.get("type").and_then(Value::as_str) == Some("function_call")
                            && let (Some(call_id), Some(name)) = (
                                item.get("call_id").and_then(Value::as_str),
                                item.get("name").and_then(Value::as_str),
                            )
                        {
                            call_names.insert(call_id.to_owned(), name.to_owned());
                        }
                    }
                    "response.function_call_arguments.done" => {
                        let Some(call_id) = event.get("call_id").and_then(Value::as_str) else { continue };
                        if !dispatched_calls.insert(call_id.to_owned()) {
                            continue;
                        }
                        let name = event
                            .get("name")
                            .and_then(Value::as_str)
                            .filter(|name| !name.is_empty())
                            .or_else(|| call_names.get(call_id).map(String::as_str))
                            .unwrap_or_default();
                        if name.is_empty() {
                            continue;
                        }
                        let arguments = event.get("arguments").and_then(Value::as_str).unwrap_or("{}");
                        let _ = send_browser(&mut browser, json!({"type":"tool","name":name,"phase":"call"})).await;
                        let now_ms = dependencies.clock.now_ms();
                        let mut result = tokio::task::block_in_place(|| {
                            tools.dispatch(name, arguments, now_ms)
                        });
                        if name == "read_latest_reply"
                            && result.get("mode").and_then(Value::as_str) != Some("full")
                            && let Some(text) = result
                                .get("spoken_text")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                            && text.len() > config.summarise_threshold_chars
                        {
                            let summary = crate::summarise::summarise(
                                &dependencies.http_client,
                                &config,
                                &text,
                                None,
                            )
                            .await;
                            if let Some(summary) = summary {
                                result["spoken_text"] = Value::String(summary);
                                result["degraded"] = Value::Bool(false);
                            } else {
                                result["spoken_text"] = Value::String(
                                    crate::summarise::clean_for_speech(&text)
                                        .chars()
                                        .rev()
                                        .take(2400)
                                        .collect::<String>()
                                        .chars()
                                        .rev()
                                        .collect(),
                                );
                                result["degraded"] = Value::Bool(true);
                            }
                        }
                        let ok = result.get("ok").and_then(Value::as_bool).unwrap_or(false);
                        if let Some(token) = result.get("proposal_token").and_then(Value::as_str) {
                            browser_proposal_token = Some(token.to_owned());
                        }
                        let _ = send_browser(&mut browser, json!({
                            "type":"tool","name":name,"phase":if ok {"ok"} else {"error"}
                        })).await;
                        let _ = send_browser(&mut browser, json!({
                            "type":"proposal","echo":result.get("spoken_echo").cloned().unwrap_or(Value::Null)
                        })).await;
                        let _ = send_realtime(&mut realtime, json!({
                            "type":"conversation.item.create",
                            "item":{
                                "type":"function_call_output",
                                "call_id":call_id,
                                "output":result.to_string()
                            }
                        })).await;
                        if active_response {
                            followup_queued = true;
                        } else {
                            let _ = send_realtime(&mut realtime, json!({"type":"response.create"})).await;
                        }
                    }
                    "response.done" => {
                        active_response = false;
                        if followup_queued {
                            followup_queued = false;
                            let _ = send_realtime(&mut realtime, json!({"type":"response.create"})).await;
                        } else {
                            let _ = send_browser(&mut browser, json!({"type":"state","state":"ready"})).await;
                        }
                    }
                    "error" => {
                        let message = event.pointer("/error/message").and_then(Value::as_str).unwrap_or("Realtime error");
                        if !message.to_lowercase().contains("cancel") {
                            send_error(&mut browser, message).await;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    let _ = realtime.close(None).await;
}

async fn send_realtime<S>(
    realtime: &mut S,
    value: Value,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: futures_util::Sink<RealtimeMessage, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    realtime
        .send(RealtimeMessage::Text(value.to_string().into()))
        .await
}

async fn send_browser(browser: &mut WebSocket, value: Value) -> Result<(), axum::Error> {
    browser
        .send(BrowserMessage::Text(value.to_string().into()))
        .await
}

async fn send_error(browser: &mut WebSocket, message: &str) {
    let _ = send_browser(browser, json!({"type":"error","message":message})).await;
}

fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![char::from(byte)]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}
