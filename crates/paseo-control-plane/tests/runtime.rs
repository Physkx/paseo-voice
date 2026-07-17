use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};

use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest as _,
        http::{HeaderValue, header::ORIGIN},
    },
};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn rust_runtime_bridges_realtime_audio_transcripts_and_deduplicates_tool_calls() {
    let app_port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve app port")
        .local_addr()
        .expect("app address")
        .port();
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let update = socket
            .next()
            .await
            .expect("session update")
            .expect("update");
        let Message::Text(update) = update else {
            panic!("session update must be text");
        };
        let update: Value = serde_json::from_str(&update).expect("update JSON");
        assert_eq!(update["type"], "session.update");
        assert!(
            update["session"]["tools"]
                .as_array()
                .is_some_and(|tools| !tools.is_empty())
        );

        let mut saw_text_turn = false;
        for _ in 0..4 {
            let message = socket.next().await.expect("client event").expect("event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("client event JSON");
            if event["type"] == "conversation.item.create" {
                saw_text_turn = true;
            }
            if event["type"] == "response.create" {
                break;
            }
        }
        assert!(saw_text_turn);
        for event in [
            json!({"type":"session.updated"}),
            json!({"type":"response.created","response":{"id":"response-1"}}),
            json!({"type":"response.output_audio.delta","delta":"YWI="}),
            json!({"type":"response.output_audio_transcript.delta","delta":"Done"}),
            json!({"type":"response.output_audio_transcript.done","transcript":"Done"}),
            json!({"type":"response.done","response":{"id":"response-1"}}),
            json!({
                "type":"response.function_call_arguments.done",
                "call_id":"duplicate-call",
                "name":"cancel_action",
                "arguments":"{}"
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "call_id":"duplicate-call",
                "name":"cancel_action",
                "arguments":"{}"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send fake event");
        }
        let mut outputs = 0;
        for _ in 0..5 {
            let received = tokio::time::timeout(Duration::from_millis(200), socket.next()).await;
            let Ok(Some(Ok(Message::Text(message)))) = received else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("output JSON");
            if event.pointer("/item/type")
                == Some(&Value::String("function_call_output".to_owned()))
            {
                outputs += 1;
            }
        }
        assert_eq!(outputs, 1);
    });

    let directory = tempfile::tempdir().expect("temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        json!({
            "listenHost":"127.0.0.1",
            "listenPort":app_port,
            "openaiBaseUrl":format!("ws://127.0.0.1:{realtime_port}"),
            "secretProvider":"environment",
            "publicDir":workspace.join("public"),
            "journalPath":directory.path().join("state/journal.sqlite3")
        })
        .to_string(),
    )
    .expect("write config");
    let child = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
        .arg("serve")
        .current_dir(&workspace)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("OPENAI_API_KEY", "test-key")
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Rust runtime");
    let _guard = ChildGuard(child);
    let health_url = format!("http://127.0.0.1:{app_port}/healthz");
    for _ in 0..40 {
        if reqwest::get(&health_url).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (mut browser, _) = connect_async(format!("ws://127.0.0.1:{app_port}/ws"))
        .await
        .expect("browser websocket");
    browser
        .send(Message::Text(
            json!({"type":"text_turn","text":"cancel anything pending"})
                .to_string()
                .into(),
        ))
        .await
        .expect("text turn");

    let mut audio = None;
    let mut transcript = None;
    for _ in 0..12 {
        let Some(Ok(message)) = browser.next().await else {
            break;
        };
        match message {
            Message::Binary(bytes) => audio = Some(bytes.to_vec()),
            Message::Text(frame) => {
                let value: Value = serde_json::from_str(&frame).expect("browser JSON");
                if value["type"] == "transcript_done" {
                    transcript = value["text"].as_str().map(str::to_owned);
                }
            }
            _ => {}
        }
        if audio.is_some() && transcript.is_some() {
            break;
        }
    }
    assert_eq!(audio, Some(b"ab".to_vec()));
    assert_eq!(transcript.as_deref(), Some("Done"));
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn rust_runtime_serves_browser_health_and_mock_websocket() {
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve local port")
        .local_addr()
        .expect("local address")
        .port();
    let directory = tempfile::tempdir().expect("temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = directory.path().join("config.json");
    let config = json!({
        "listenHost": "127.0.0.1",
        "listenPort": port,
        "forceMock": true,
        "secretProvider": "environment",
        "publicDir": workspace.join("public"),
        "journalPath": directory.path().join("state/journal.sqlite3")
    });
    std::fs::write(&config_path, config.to_string()).expect("write config");
    let child = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
        .arg("serve")
        .current_dir(&workspace)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Rust runtime");
    let _guard = ChildGuard(child);

    let health_url = format!("http://127.0.0.1:{port}/healthz");
    let mut health = None;
    for _ in 0..40 {
        if let Ok(response) = reqwest::get(&health_url).await
            && let Ok(value) = response.json::<Value>().await
        {
            health = Some(value);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        health.expect("runtime became healthy"),
        json!({"ok":true,"mode":"mock","backend":"rust"})
    );
    let index = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .expect("browser index")
        .text()
        .await
        .expect("index text");
    assert!(index.contains("Paseo Voice"));

    let mut cross_origin = format!("ws://127.0.0.1:{port}/ws")
        .into_client_request()
        .expect("cross-origin request");
    cross_origin
        .headers_mut()
        .insert(ORIGIN, HeaderValue::from_static("https://attacker.example"));
    let error = connect_async(cross_origin)
        .await
        .expect_err("cross-origin browser websocket must be rejected");
    assert!(error.to_string().contains("403"));

    let (mut websocket, _) = connect_async(format!("ws://127.0.0.1:{port}/ws"))
        .await
        .expect("browser websocket");
    let first = websocket.next().await.expect("mode frame").expect("mode");
    let Message::Text(first) = first else {
        panic!("expected text mode frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&first).expect("mode JSON"),
        json!({"type":"mode","mode":"mock"})
    );
    websocket
        .send(Message::Text(
            json!({"type":"text_turn","text":"help"}).to_string().into(),
        ))
        .await
        .expect("send mock text turn");
    let mut transcript = None;
    for _ in 0..6 {
        let Some(Ok(Message::Text(frame))) = websocket.next().await else {
            break;
        };
        let value: Value = serde_json::from_str(&frame).expect("browser JSON");
        if value["type"] == "transcript_done" {
            transcript = value["text"].as_str().map(str::to_owned);
            break;
        }
    }
    assert!(transcript.expect("mock transcript").contains("Commands:"));
}
