//! Single-process browser runtime.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use tower_http::services::ServeDir;

use crate::{config::Config, secrets::Secrets};

#[derive(Clone)]
struct AppState {
    mode: &'static str,
}

/// Run the local browser server until shutdown.
///
/// # Errors
///
/// Returns an error when binding or serving fails.
pub async fn run(config: Config, secrets: Secrets) -> Result<(), String> {
    let mode = if secrets.openai_api_key.is_some() && !config.force_mock {
        "real"
    } else {
        "mock"
    };
    let state = Arc::new(AppState { mode });
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/ws", get(websocket))
        .fallback_service(ServeDir::new(config.public_dir).append_index_html_on_directories(true))
        .with_state(state);
    let address: SocketAddr = format!("{}:{}", config.listen_host, config.listen_port)
        .parse()
        .map_err(|_| "invalid listen address")?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| format!("listen failed: {error}"))?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
        .map_err(|error| format!("server failed: {error}"))
}

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({ "ok": true, "mode": state.mode, "backend": "rust" }))
}

async fn websocket(
    upgrade: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| browser_session(socket, state))
}

async fn browser_session(socket: WebSocket, state: Arc<AppState>) {
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
                } else if state.mode == "mock"
                    && control.get("type").and_then(Value::as_str) == Some("text_turn")
                {
                    let _ = sender
                        .send(Message::Text(
                            json!({
                                "type": "error",
                                "message": "Rust mock mode is ready; live tool wiring is unavailable"
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => {}
        }
    }
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}
