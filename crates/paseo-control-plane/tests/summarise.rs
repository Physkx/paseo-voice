use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    Router,
    body::{Body, Bytes},
    http::{StatusCode, header},
    routing::{any, post},
};
use paseo_control_plane::{
    config::Config,
    dictation::{DictationCleaner, HttpDictationCleaner},
    runtime::build_model_http_client,
    summarise::{clean_for_speech, summarise_for_speech},
};

// Windows needs SystemRoot (and related OS variables) for WinSock init even
// after env_clear(); without them a spawned child's HTTP client cannot connect.
// Absent on other platforms, so this is empty there.
fn essential_os_env() -> Vec<(String, String)> {
    ["SystemRoot", "windir", "SystemDrive", "TEMP", "TMP"]
        .into_iter()
        .filter_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| (key.to_string(), value))
        })
        .collect()
}

async fn config_for_response(status: StatusCode, body: String) -> (Config, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind summariser endpoint");
    let address = listener.local_addr().expect("summariser endpoint address");
    let request_count = Arc::new(AtomicUsize::new(0));
    let handler_request_count = Arc::clone(&request_count);
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            handler_request_count.fetch_add(1, Ordering::Relaxed);
            let body = body.clone();
            async move { (status, [(header::CONTENT_TYPE, "application/json")], body) }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve summariser response");
    });
    (
        Config {
            spark_base_url: format!("http://{address}/v1"),
            summarise_threshold_chars: 3,
            ..Config::default()
        },
        request_count,
    )
}

async fn config_for_chunked_response(chunks: Vec<String>) -> (Config, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind chunked summariser endpoint");
    let address = listener
        .local_addr()
        .expect("chunked summariser endpoint address");
    let request_count = Arc::new(AtomicUsize::new(0));
    let handler_request_count = Arc::clone(&request_count);
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            handler_request_count.fetch_add(1, Ordering::Relaxed);
            let chunks = chunks.clone();
            async move {
                axum::http::Response::builder()
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from_stream(futures_util::stream::iter(
                        chunks
                            .into_iter()
                            .map(|chunk| Ok::<_, Infallible>(Bytes::from(chunk))),
                    )))
                    .expect("build chunked summariser response")
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve chunked summariser response");
    });
    (
        Config {
            spark_base_url: format!("http://{address}/v1"),
            summarise_threshold_chars: 3,
            ..Config::default()
        },
        request_count,
    )
}

async fn config_for_captured_request() -> (Config, Arc<Mutex<Option<serde_json::Value>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind capturing summariser endpoint");
    let address = listener
        .local_addr()
        .expect("capturing summariser endpoint address");
    let captured_request = Arc::new(Mutex::new(None));
    let handler_captured_request = Arc::clone(&captured_request);
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            let captured_request = Arc::clone(&handler_captured_request);
            async move {
                *captured_request.lock().expect("capture request lock") = Some(request);
                axum::Json(serde_json::json!({
                    "choices": [{"message": {"content": "Outcome summarised."}}]
                }))
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve capturing summariser response");
    });
    (
        Config {
            spark_base_url: format!("http://{address}/v1"),
            summarise_threshold_chars: 3,
            ..Config::default()
        },
        captured_request,
    )
}

#[tokio::test]
async fn oversized_successful_summary_is_unicode_safely_bounded_for_speech() {
    let oversized_summary = "界".repeat(2_401);
    let body = serde_json::json!({
        "choices": [{"message": {"content": oversized_summary}}]
    })
    .to_string();
    let (config, _) = config_for_response(StatusCode::OK, body).await;

    let result = summarise_for_speech(
        &reqwest::Client::new(),
        &config,
        "A long coding-agent reply",
        Some("Example session"),
    )
    .await;

    assert!(!result.degraded);
    assert_eq!(result.text.chars().count(), 2_400);
    assert_eq!(result.text, "界".repeat(2_400));
}

#[tokio::test]
async fn below_threshold_cleaned_reply_is_unicode_safely_bounded_for_speech() {
    let config = Config {
        summarise_threshold_chars: 20_000,
        ..Config::default()
    };
    let source = format!("# {}", "界".repeat(5_000));

    let result = summarise_for_speech(
        &reqwest::Client::new(),
        &config,
        &source,
        Some("Example session"),
    )
    .await;

    assert!(!result.degraded);
    assert_eq!(result.text.chars().count(), 2_400);
    assert_eq!(result.text, "界".repeat(2_400));
}

#[tokio::test]
async fn oversized_chunked_response_degrades_once_to_the_cleaned_bounded_fallback() {
    let body = serde_json::json!({
        "choices": [{"message": {"content": "summary ".repeat(8_750)}}]
    })
    .to_string();
    let chunks = body
        .as_bytes()
        .chunks(1_024)
        .map(|chunk| String::from_utf8(chunk.to_vec()).expect("ASCII response chunk"))
        .collect();
    let (config, request_count) = config_for_chunked_response(chunks).await;
    let source = format!("# outcome\n\n- {}final blocker", "detail ".repeat(500));

    let result = summarise_for_speech(
        &reqwest::Client::new(),
        &config,
        &source,
        Some("Example session"),
    )
    .await;

    assert!(result.degraded);
    assert_eq!(result.text.chars().count(), 2_400);
    assert!(result.text.ends_with("final blocker"));
    assert_eq!(request_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn oversized_summary_input_preserves_outcome_and_tail_within_a_fixed_bound() {
    let (config, captured_request) = config_for_captured_request().await;
    let source = format!(
        "OUTCOME: implementation complete.\n{}\nBLOCKER: review required.",
        "middle detail ".repeat(5_000)
    );

    let result = summarise_for_speech(
        &reqwest::Client::new(),
        &config,
        &source,
        Some("Example session"),
    )
    .await;

    assert!(!result.degraded);
    let request = captured_request
        .lock()
        .expect("captured request lock")
        .clone()
        .expect("captured summariser request");
    let user_content = request
        .pointer("/messages/1/content")
        .and_then(serde_json::Value::as_str)
        .expect("summariser user content")
        .strip_prefix("Session: Example session\n\n")
        .expect("session prefix");
    assert_eq!(user_content.chars().count(), 24_000);
    assert!(user_content.starts_with("OUTCOME: implementation complete."));
    assert!(user_content.contains("[... middle omitted ...]"));
    assert!(user_content.ends_with("BLOCKER: review required."));
}

#[tokio::test]
async fn non_success_response_degrades_once_to_the_cleaned_bounded_fallback() {
    let (config, request_count) =
        config_for_response(StatusCode::SERVICE_UNAVAILABLE, "unavailable".to_owned()).await;
    let source = format!("# outcome\n\n- {}final blocker", "detail ".repeat(500));

    let result = summarise_for_speech(
        &reqwest::Client::new(),
        &config,
        &source,
        Some("Example session"),
    )
    .await;

    assert!(result.degraded);
    assert_eq!(result.text.chars().count(), 2_400);
    assert!(result.text.ends_with("final blocker"));
    assert!(!result.text.contains('#'));
    assert_eq!(request_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn production_client_does_not_redirect_agent_output() {
    let destination_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("summariser redirect destination listener");
    let destination_address = destination_listener
        .local_addr()
        .expect("summariser redirect destination address");
    let destination_count = Arc::new(AtomicUsize::new(0));
    let destination_body = Arc::new(Mutex::new(None::<Vec<u8>>));
    let handler_destination_count = Arc::clone(&destination_count);
    let handler_destination_body = Arc::clone(&destination_body);
    let destination = Router::new().route(
        "/captured",
        post(move |body: Bytes| {
            handler_destination_count.fetch_add(1, Ordering::SeqCst);
            *handler_destination_body
                .lock()
                .expect("summariser redirect body lock") = Some(body.to_vec());
            async {
                axum::Json(serde_json::json!({
                    "choices": [{"message": {"content": "Redirected summary"}}]
                }))
            }
        }),
    );
    let destination_server = tokio::spawn(async move {
        axum::serve(destination_listener, destination)
            .await
            .expect("serve summariser redirect destination");
    });

    let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("summariser redirect listener");
    let redirect_address = redirect_listener
        .local_addr()
        .expect("summariser redirect address");
    let redirect_count = Arc::new(AtomicUsize::new(0));
    let handler_redirect_count = Arc::clone(&redirect_count);
    let location = format!("http://{destination_address}/captured");
    let redirect = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            handler_redirect_count.fetch_add(1, Ordering::SeqCst);
            let location = location.clone();
            async move {
                (
                    StatusCode::PERMANENT_REDIRECT,
                    [(header::LOCATION, location)],
                )
            }
        }),
    );
    let redirect_server = tokio::spawn(async move {
        axum::serve(redirect_listener, redirect)
            .await
            .expect("serve summariser redirect");
    });
    let config = Config {
        spark_base_url: format!("http://{redirect_address}/v1"),
        summarise_threshold_chars: 3,
        ..Config::default()
    };

    let result = summarise_for_speech(
        &build_model_http_client().expect("production HTTP client"),
        &config,
        "redirect-sensitive agent output",
        Some("Example session"),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(result.degraded);
    assert_eq!(result.text, "redirect-sensitive agent output");
    assert_eq!(redirect_count.load(Ordering::SeqCst), 1);
    assert_eq!(destination_count.load(Ordering::SeqCst), 0);
    assert!(
        destination_body
            .lock()
            .expect("summariser redirect body lock")
            .is_none(),
        "redirect destination received the agent output body"
    );
    redirect_server.abort();
    destination_server.abort();
}

#[tokio::test]
async fn model_http_client_proxy_subprocess() {
    if std::env::var("PASEO_VOICE_PROXY_TEST_CHILD").as_deref() != Ok("1") {
        return;
    }
    let endpoint =
        std::env::var("PASEO_VOICE_PROXY_TEST_ENDPOINT").expect("proxy test model endpoint");
    let config = Config {
        spark_base_url: endpoint,
        summarise_threshold_chars: 3,
        ..Config::default()
    };
    let client = build_model_http_client().expect("production HTTP client");
    let cleaner = HttpDictationCleaner::new(client.clone(), &config);

    let cleanup = cleaner
        .clean("ambient proxy transcript")
        .await
        .expect("direct cleanup result");
    assert!(!cleanup.degraded);
    assert_eq!(cleanup.text, "Cleaned directly.");

    let summary = summarise_for_speech(
        &client,
        &config,
        "ambient proxy agent output",
        Some("Example session"),
    )
    .await;
    assert!(!summary.degraded);
    assert_eq!(summary.text, "Summarised directly.");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn production_model_client_ignores_ambient_proxies() {
    let intended_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("intended model listener");
    let intended_address = intended_listener
        .local_addr()
        .expect("intended model address");
    let intended_requests = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let handler_intended_requests = Arc::clone(&intended_requests);
    let intended = Router::new().route(
        "/v1/chat/completions",
        post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            handler_intended_requests
                .lock()
                .expect("intended request lock")
                .push(request.clone());
            async move {
                let system_prompt = request
                    .pointer("/messages/0/content")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let content = if system_prompt.contains("Edit the English dictation") {
                    "Cleaned directly."
                } else {
                    "Summarised directly."
                };
                axum::Json(serde_json::json!({
                    "choices": [{"message": {"content": content}}]
                }))
            }
        }),
    );
    let intended_server = tokio::spawn(async move {
        axum::serve(intended_listener, intended)
            .await
            .expect("serve intended model endpoint");
    });

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake proxy listener");
    let proxy_address = proxy_listener.local_addr().expect("fake proxy address");
    let proxy_requests = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let handler_proxy_requests = Arc::clone(&proxy_requests);
    let proxy = Router::new().fallback(any(move |body: Bytes| {
        handler_proxy_requests
            .lock()
            .expect("fake proxy request lock")
            .push(body.to_vec());
        async { StatusCode::BAD_GATEWAY }
    }));
    let proxy_server = tokio::spawn(async move {
        axum::serve(proxy_listener, proxy)
            .await
            .expect("serve fake proxy");
    });
    let proxy_url = format!("http://{proxy_address}");
    let output =
        tokio::process::Command::new(std::env::current_exe().expect("current test binary"))
            .arg("--exact")
            .arg("model_http_client_proxy_subprocess")
            .arg("--nocapture")
            .env_clear()
            .envs(essential_os_env())
            .env("PASEO_VOICE_PROXY_TEST_CHILD", "1")
            .env(
                "PASEO_VOICE_PROXY_TEST_ENDPOINT",
                format!("http://{intended_address}/v1"),
            )
            .env("HTTP_PROXY", &proxy_url)
            .env("HTTPS_PROXY", &proxy_url)
            .env("ALL_PROXY", &proxy_url)
            .env("http_proxy", &proxy_url)
            .env("https_proxy", &proxy_url)
            .env("all_proxy", &proxy_url)
            .env("NO_PROXY", "")
            .env("no_proxy", "")
            .output()
            .await
            .expect("run isolated proxy test child");

    let intended_requests = intended_requests.lock().expect("intended request lock");
    assert_eq!(intended_requests.len(), 2);
    assert!(
        intended_requests
            .iter()
            .any(|request| request.to_string().contains("ambient proxy transcript"))
    );
    assert!(
        intended_requests
            .iter()
            .any(|request| request.to_string().contains("ambient proxy agent output"))
    );
    assert!(
        proxy_requests
            .lock()
            .expect("fake proxy request lock")
            .is_empty(),
        "ambient proxy received a model request or body"
    );
    assert!(
        output.status.success(),
        "proxy test child failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    proxy_server.abort();
    intended_server.abort();
}

#[test]
fn production_client_constructs_secure_remote_request_without_network() {
    let request = build_model_http_client()
        .expect("production HTTP client")
        .post("https://models.example/v1/chat/completions")
        .json(&serde_json::json!({"model":"configured-model"}))
        .build()
        .expect("secure remote model request");

    assert_eq!(request.url().scheme(), "https");
    assert_eq!(request.url().host_str(), Some("models.example"));
    assert_eq!(request.url().path(), "/v1/chat/completions");
}

#[tokio::test]
async fn malformed_json_degrades_once_to_the_cleaned_bounded_fallback() {
    let (config, request_count) =
        config_for_response(StatusCode::OK, "{not valid JSON".to_owned()).await;
    let source = format!("# outcome\n\n- {}final blocker", "detail ".repeat(500));

    let result = summarise_for_speech(
        &reqwest::Client::new(),
        &config,
        &source,
        Some("Example session"),
    )
    .await;

    assert!(result.degraded);
    assert_eq!(result.text.chars().count(), 2_400);
    assert!(result.text.ends_with("final blocker"));
    assert!(!result.text.contains('#'));
    assert_eq!(request_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn empty_content_degrades_once_to_the_cleaned_bounded_fallback() {
    let body = serde_json::json!({
        "choices": [{"message": {"content": ""}}]
    })
    .to_string();
    let (config, request_count) = config_for_response(StatusCode::OK, body).await;
    let source = format!("# outcome\n\n- {}final blocker", "detail ".repeat(500));

    let result = summarise_for_speech(
        &reqwest::Client::new(),
        &config,
        &source,
        Some("Example session"),
    )
    .await;

    assert!(result.degraded);
    assert_eq!(result.text.chars().count(), 2_400);
    assert!(result.text.ends_with("final blocker"));
    assert!(!result.text.contains('#'));
    assert_eq!(request_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn unavailable_summariser_returns_a_bounded_cleaned_degraded_fallback() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve failed endpoint");
    let address = listener.local_addr().expect("failed endpoint address");
    drop(listener);
    let config = Config {
        spark_base_url: format!("http://{address}/v1"),
        summarise_threshold_chars: 3,
        ..Config::default()
    };
    let source = format!("# outcome\n\n- {} final blocker", "detail ".repeat(500));

    let result =
        summarise_for_speech(&reqwest::Client::new(), &config, &source, Some("Agent A")).await;

    assert!(result.degraded);
    assert!(result.text.chars().count() <= 2_400);
    assert!(result.text.ends_with("final blocker"));
    assert!(!result.text.contains('#'));
    assert!(!result.text.contains("- "));
}

#[tokio::test]
async fn timed_out_summariser_degrades_once_without_retrying() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind timed-out summariser endpoint");
    let address = listener
        .local_addr()
        .expect("timed-out summariser endpoint address");
    let request_count = Arc::new(AtomicUsize::new(0));
    let server_request_count = Arc::clone(&request_count);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .expect("accept timed-out summariser request");
        let mut request = [0_u8; 8 * 1_024];
        let bytes = stream
            .read(&mut request)
            .await
            .expect("read timed-out summariser request");
        assert!(bytes > 0);
        server_request_count.fetch_add(1, Ordering::SeqCst);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 1024\r\n\r\n",
            )
            .await
            .expect("write timed-out summariser headers");
        std::future::pending::<()>().await;
    });
    let config = Config {
        spark_base_url: format!("http://{address}/v1"),
        summarise_threshold_chars: 3,
        ..Config::default()
    };
    let source = format!("# outcome\n\n- {}final blocker", "detail ".repeat(500));
    let client = reqwest::Client::builder()
        .read_timeout(std::time::Duration::from_millis(100))
        .build()
        .expect("timed-out summariser client");

    let result = summarise_for_speech(&client, &config, &source, Some("Example session")).await;

    assert!(result.degraded);
    assert_eq!(result.text.chars().count(), 2_400);
    assert!(result.text.ends_with("final blocker"));
    assert_eq!(request_count.load(Ordering::SeqCst), 1);
    server.abort();
}

#[test]
fn short_speech_text_is_cleaned_without_changing_its_outcome_order() {
    assert_eq!(
        clean_for_speech("# Done\n- Tests pass\n> No blocker"),
        "Done Tests pass No blocker"
    );
}
