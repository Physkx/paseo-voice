use std::{
    future::Future,
    net::TcpListener,
    pin::Pin,
    process::{Child, Command, Stdio},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures_util::{SinkExt as _, StreamExt as _};
use paseo_control_plane::{
    clock::SystemClock,
    config::{Config, PaseoHostProfile},
    dictation::{CleanupFuture, DictationCleaner, DictationCleanup},
    journal::Journal,
    paseo::{ProcessExecutor, ProcessOutput, SystemProcessExecutor},
    realtime::{RealtimeConnector, SystemRealtimeConnector},
    runtime::{RuntimeDependencies, realtime_mode, serve},
    secrets::Secrets,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_async, accept_hdr_async, connect_async,
    tungstenite::{
        Error as RealtimeError, Message,
        client::IntoClientRequest as _,
        handshake::server::{
            Callback, ErrorResponse, Request as ServerRequest, Response as ServerResponse,
        },
        http::{
            HeaderValue, Request, Response,
            header::{AUTHORIZATION, ORIGIN},
        },
    },
};

struct ChildGuard(Child);

#[test]
fn realtime_mode_requires_a_key_only_for_the_official_endpoint() {
    for (base_url, api_key, force_mock, expected) in [
        ("wss://api.openai.com/v1/realtime", None, false, "mock"),
        (
            "wss://api.openai.com/v1/realtime",
            Some("key"),
            false,
            "real",
        ),
        ("ws://127.0.0.1:8080/realtime", None, false, "real"),
        ("wss://realtime.example/v1/realtime", None, false, "real"),
        ("ws://127.0.0.1:8080/realtime", Some("key"), true, "mock"),
    ] {
        let config = Config {
            openai_base_url: base_url.to_owned(),
            force_mock,
            ..Config::default()
        };
        assert_eq!(realtime_mode(&config, api_key), expected, "{base_url}");
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct ServerGuard(tokio::task::JoinHandle<()>);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type TestConnectFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    (
                        WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
                        Response<Option<Vec<u8>>>,
                    ),
                    RealtimeError,
                >,
            > + Send,
    >,
>;

struct PendingRealtimeConnector {
    attempts: Arc<AtomicUsize>,
}

impl RealtimeConnector for PendingRealtimeConnector {
    fn connect(&self, _request: Request<()>) -> TestConnectFuture {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    }
}

#[derive(Debug)]
struct CapturedRealtimeRequest {
    uri: String,
    authorization: Option<Vec<u8>>,
    authorization_sensitive: bool,
    headers: Vec<(String, Vec<u8>)>,
}

struct CapturingRealtimeConnector {
    sender: Mutex<Option<tokio::sync::oneshot::Sender<CapturedRealtimeRequest>>>,
}

impl RealtimeConnector for CapturingRealtimeConnector {
    fn connect(&self, request: Request<()>) -> TestConnectFuture {
        let captured = CapturedRealtimeRequest {
            uri: request.uri().to_string(),
            authorization: request
                .headers()
                .get(AUTHORIZATION)
                .map(|value| value.as_bytes().to_vec()),
            authorization_sensitive: request
                .headers()
                .get(AUTHORIZATION)
                .is_some_and(HeaderValue::is_sensitive),
            headers: request
                .headers()
                .iter()
                .map(|(name, value)| (name.to_string(), value.as_bytes().to_vec()))
                .collect(),
        };
        self.sender
            .lock()
            .expect("capturing connector sender lock")
            .take()
            .expect("unused capturing connector")
            .send(captured)
            .expect("capture Realtime request");
        Box::pin(async { Err(RealtimeError::ConnectionClosed) })
    }
}

fn capturing_realtime_connector() -> (
    Arc<dyn RealtimeConnector>,
    tokio::sync::oneshot::Receiver<CapturedRealtimeRequest>,
) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    (
        Arc::new(CapturingRealtimeConnector {
            sender: Mutex::new(Some(sender)),
        }),
        receiver,
    )
}

struct AssertNoRealtimeAuthorization;

impl Callback for AssertNoRealtimeAuthorization {
    fn on_request(
        self,
        request: &ServerRequest,
        response: ServerResponse,
    ) -> Result<ServerResponse, ErrorResponse> {
        assert!(
            request.headers().get(AUTHORIZATION).is_none(),
            "loopback fake Realtime received OpenAI authorization"
        );
        Ok(response)
    }
}

struct PanickingDictationCleaner;

impl DictationCleaner for PanickingDictationCleaner {
    fn clean<'a>(&'a self, _transcript: &'a str) -> CleanupFuture<'a> {
        Box::pin(async { panic!("injected dictation cleanup panic") })
    }
}

struct LatchedDictationCleaner {
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl DictationCleaner for LatchedDictationCleaner {
    fn clean<'a>(&'a self, _transcript: &'a str) -> CleanupFuture<'a> {
        let started = self
            .started
            .lock()
            .expect("cleanup start lock")
            .take()
            .expect("single cleanup start");
        let release = self
            .release
            .lock()
            .expect("cleanup release lock")
            .take()
            .expect("single cleanup release");
        Box::pin(async move {
            let _ = started.send(());
            release.await.expect("release cleanup result");
            Some(DictationCleanup {
                text: "Cleaned dictation".to_owned(),
                degraded: false,
            })
        })
    }
}

struct ProcessLatch {
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: Mutex<bool>,
    release: Condvar,
}

impl ProcessLatch {
    fn wait(&self) {
        if let Some(started) = self
            .started
            .lock()
            .expect("process latch start lock")
            .take()
        {
            let _ = started.send(());
        }
        let mut released = self.released.lock().expect("process latch release lock");
        while !*released {
            released = self
                .release
                .wait(released)
                .expect("process latch release wait");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("process latch release lock") = true;
        self.release.notify_all();
    }
}

struct ProcessLatchGuard {
    latch: Arc<ProcessLatch>,
}

impl ProcessLatchGuard {
    fn release(&self) {
        self.latch.release();
    }
}

impl Drop for ProcessLatchGuard {
    fn drop(&mut self) {
        self.latch.release();
    }
}

fn process_latch() -> (
    Arc<ProcessLatch>,
    tokio::sync::oneshot::Receiver<()>,
    ProcessLatchGuard,
) {
    let (started, started_receiver) = tokio::sync::oneshot::channel();
    let latch = Arc::new(ProcessLatch {
        started: Mutex::new(Some(started)),
        released: Mutex::new(false),
        release: Condvar::new(),
    });
    (
        Arc::clone(&latch),
        started_receiver,
        ProcessLatchGuard { latch },
    )
}

struct ProcessStartGate {
    released: Mutex<bool>,
    release: Condvar,
}

impl ProcessStartGate {
    fn wait(&self) {
        let mut released = self.released.lock().expect("process start gate lock");
        while !*released {
            released = self
                .release
                .wait(released)
                .expect("process start gate wait");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("process start gate lock") = true;
        self.release.notify_all();
    }
}

struct ProcessStartGateGuard {
    gate: Arc<ProcessStartGate>,
}

impl ProcessStartGateGuard {
    fn release(&self) {
        self.gate.release();
    }
}

impl Drop for ProcessStartGateGuard {
    fn drop(&mut self) {
        self.gate.release();
    }
}

fn process_start_gate() -> (Arc<ProcessStartGate>, ProcessStartGateGuard) {
    let gate = Arc::new(ProcessStartGate {
        released: Mutex::new(false),
        release: Condvar::new(),
    });
    (Arc::clone(&gate), ProcessStartGateGuard { gate })
}

struct LatchingPermissionsExecutor {
    latch: Arc<ProcessLatch>,
    permission_calls: AtomicUsize,
}

impl ProcessExecutor for LatchingPermissionsExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &std::collections::HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        let stdout = match args.first().map(String::as_str) {
            Some("ls") => r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#,
            Some("logs") => "Bound reply for deferred intent tests.",
            Some("permit") => {
                self.permission_calls.fetch_add(1, Ordering::SeqCst);
                self.latch.wait();
                r#"[{"id":"permission-a"}]"#
            }
            _ => "[]",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

struct LatchingDashboardExecutor {
    latch: Arc<ProcessLatch>,
    dashboard_armed: AtomicBool,
}

impl ProcessExecutor for LatchingDashboardExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &std::collections::HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        let stdout = match args.first().map(String::as_str) {
            Some("ls") => {
                if self.dashboard_armed.swap(false, Ordering::SeqCst) {
                    self.latch.wait();
                }
                r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#
            }
            Some("logs") => "Bound reply for proposal mutation tests.",
            Some("permit") => {
                self.dashboard_armed.store(true, Ordering::SeqCst);
                r#"[{"id":"permission-a"}]"#
            }
            _ => "[]",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

struct LatchingInitialPresentationExecutor {
    latch: Arc<ProcessLatch>,
    list_calls: AtomicUsize,
}

impl ProcessExecutor for LatchingInitialPresentationExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &std::collections::HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        let stdout = match args.first().map(String::as_str) {
            Some("ls") => {
                if self.list_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.latch.wait();
                }
                r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#
            }
            _ => "[]",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

struct LatchingHostSelectionExecutor {
    latch: Arc<ProcessLatch>,
    second_host_armed: AtomicBool,
    list_calls: AtomicUsize,
}

impl ProcessExecutor for LatchingHostSelectionExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        env: &std::collections::HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        let stdout = match args.first().map(String::as_str) {
            Some("ls") => {
                self.list_calls.fetch_add(1, Ordering::SeqCst);
                if env.get("PASEO_HOST").map(String::as_str) == Some("second.example:6767")
                    && self.second_host_armed.swap(false, Ordering::SeqCst)
                {
                    self.latch.wait();
                }
                r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#
            }
            _ => "[]",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

struct LatchingSameHostExecutor {
    latch: Arc<ProcessLatch>,
    refresh_armed: AtomicBool,
    list_calls: AtomicUsize,
    send_calls: AtomicUsize,
}

impl ProcessExecutor for LatchingSameHostExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &std::collections::HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        let stdout = match args.first().map(String::as_str) {
            Some("ls") => {
                self.list_calls.fetch_add(1, Ordering::SeqCst);
                if self.refresh_armed.swap(false, Ordering::SeqCst) {
                    self.latch.wait();
                }
                r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#
            }
            Some("logs") => "Bound reply for same-host isolation tests.",
            Some("send") => {
                self.send_calls.fetch_add(1, Ordering::SeqCst);
                r#"{"messageId":"same-host-message"}"#
            }
            _ => "[]",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

struct CheckpointControlExecutor {
    send_calls: AtomicUsize,
    run_calls: AtomicUsize,
}

impl ProcessExecutor for CheckpointControlExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &std::collections::HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        let stdout = match args.first().map(String::as_str) {
            Some("ls") => r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#,
            Some("logs") => "Bound reply for response checkpoint tests.",
            Some("send") => {
                self.send_calls.fetch_add(1, Ordering::SeqCst);
                r#"{"messageId":"checkpoint-message"}"#
            }
            Some("provider") => r#"[{"id":"gpt-5.6-sol-max"}]"#,
            Some("run") => {
                self.run_calls.fetch_add(1, Ordering::SeqCst);
                "{}"
            }
            _ => "[]",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

struct LatchingConfirmedSendExecutor {
    latch: Arc<ProcessLatch>,
    start_gate: Option<Arc<ProcessStartGate>>,
    send_calls: AtomicUsize,
    permission_calls: AtomicUsize,
    send_returned: AtomicBool,
    post_send_probe: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl ProcessExecutor for LatchingConfirmedSendExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &std::collections::HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        let stdout = match args.first().map(String::as_str) {
            Some("ls") => {
                if self.send_returned.load(Ordering::SeqCst)
                    && let Some(sender) = self
                        .post_send_probe
                        .lock()
                        .expect("post-send probe lock")
                        .take()
                {
                    let _ = sender.send(());
                }
                r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#
            }
            Some("logs") => "Bound reply for confirmed send isolation tests.",
            Some("send") => {
                if let Some(gate) = self.start_gate.as_ref() {
                    gate.wait();
                }
                self.send_calls.fetch_add(1, Ordering::SeqCst);
                self.latch.wait();
                self.send_returned.store(true, Ordering::SeqCst);
                "{}"
            }
            Some("permit") => {
                self.permission_calls.fetch_add(1, Ordering::SeqCst);
                r#"[{"id":"permission-a"}]"#
            }
            _ => "[]",
        };
        ProcessOutput {
            exit_code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

struct PanickingPermissionsExecutor {
    permission_calls: AtomicUsize,
}

impl ProcessExecutor for PanickingPermissionsExecutor {
    fn execute(
        &self,
        _program: &str,
        args: &[String],
        _env: &std::collections::HashMap<String, String>,
        _timeout: Duration,
    ) -> ProcessOutput {
        if args.first().map(String::as_str) == Some("permit") {
            self.permission_calls.fetch_add(1, Ordering::SeqCst);
            panic!("injected model tool process panic");
        }
        ProcessOutput {
            exit_code: Some(0),
            stdout: r#"[{"id":"thread-a","shortId":"a","name":"Agent A","status":"idle"}]"#
                .to_owned(),
            stderr: String::new(),
            timed_out: false,
            spawn_failed: false,
        }
    }
}

type BrowserSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn browser_server_port(browser: &BrowserSocket) -> u16 {
    let MaybeTlsStream::Plain(stream) = browser.get_ref() else {
        panic!("test browser must use a plain loopback socket");
    };
    stream
        .peer_addr()
        .expect("test browser peer address")
        .port()
}

fn test_pcm(bytes: &[u8]) -> Vec<u8> {
    let mut audio = bytes.to_vec();
    if !audio.len().is_multiple_of(2) {
        audio.push(0);
    }
    audio
}

static NEXT_TEST_TEXT_TURN_ID: AtomicU64 = AtomicU64::new(1);

macro_rules! test_text_turn {
    ($text:expr, null) => {
        test_text_turn_control($text, &Value::Null)
    };
    ($text:expr, $summary_id:expr) => {
        test_text_turn_control($text, &json!($summary_id))
    };
}

fn test_text_turn_control(text: impl Into<String>, summary_id: &Value) -> Value {
    let turn_id = NEXT_TEST_TEXT_TURN_ID.fetch_add(1, Ordering::Relaxed);
    assert!(turn_id <= 2_147_483_647, "test text turn ID overflow");
    json!({
        "type":"text_turn",
        "text":text.into(),
        "summary_id":summary_id,
        "turn_id":turn_id
    })
}

fn test_text_turn_id(control: &Value) -> u64 {
    control["turn_id"].as_u64().expect("test text turn ID")
}

async fn next_browser_json_including_text_ack(browser: &mut BrowserSocket, context: &str) -> Value {
    let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
        .await
        .unwrap_or_else(|_| panic!("{context} timeout"))
        .unwrap_or_else(|| panic!("{context} frame"))
        .unwrap_or_else(|error| panic!("{context}: {error}"));
    let Message::Text(message) = message else {
        panic!("{context} must be text");
    };
    serde_json::from_str(&message).unwrap_or_else(|_| panic!("{context} JSON"))
}

async fn next_browser_json(browser: &mut BrowserSocket, context: &str) -> Value {
    loop {
        let frame = next_browser_json_including_text_ack(browser, context).await;
        if frame.get("type").and_then(Value::as_str) != Some("text_turn_accepted") {
            return frame;
        }
    }
}

async fn next_browser_json_without_process_start(
    browser: &mut BrowserSocket,
    process_started: &mut tokio::sync::oneshot::Receiver<()>,
    context: &str,
) -> Value {
    tokio::select! {
        result = process_started => {
            result.unwrap_or_else(|_| panic!("{context} process signal dropped"));
            panic!("{context} started a process probe");
        }
        frame = next_browser_json(browser, context) => frame,
    }
}

// Health probes use a client with an explicit per-request timeout so a stalled
// connect to a not-yet-ready port (which can hang for seconds on Windows) fails
// fast and the readiness loop keeps polling instead of exhausting its budget.
fn health_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .connect_timeout(Duration::from_millis(500))
        .build()
        .expect("health probe client")
}

// The spawned runtime clears its environment for isolation, but Windows still
// needs a few OS-level variables (notably SystemRoot, without which WinSock
// fails to initialize and the listener never binds - os error 10106). These
// keys are absent on other platforms, so this is empty there.
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

#[allow(dead_code)]
async fn next_bound_summary_id(browser: &mut BrowserSocket, context: &str) -> String {
    loop {
        let frame = next_browser_json(browser, context).await;
        if let Some(summary_id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            return summary_id.to_owned();
        }
    }
}

async fn next_browser_json_of_type(
    browser: &mut BrowserSocket,
    expected_type: &str,
    context: &str,
) -> Value {
    loop {
        let frame = next_browser_json(browser, context).await;
        if frame.get("type").and_then(Value::as_str) == Some(expected_type) {
            return frame;
        }
    }
}

async fn initialize_browser_protocol(browser: &mut BrowserSocket, context: &str) {
    browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .unwrap_or_else(|error| panic!("{context} hello: {error}"));
    for _ in 0..6 {
        let _ = next_browser_json(browser, context).await;
    }
}

async fn bind_browser_context(browser: &mut BrowserSocket) -> String {
    browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("request bound context");
    let mut summary_id = None;
    let mut ready = false;
    while summary_id.is_none() || !ready {
        let frame = next_browser_json(browser, "bound context frame").await;
        if let Some(id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_id = Some(id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    summary_id.expect("bound summary ID")
}

async fn start_and_commit_bound_dictation(
    browser: &mut BrowserSocket,
    summary_id: &str,
    recording_id: u64,
) -> String {
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select test dictation mode");
    let _ = next_browser_json_of_type(browser, "voice_mode", "test dictation mode").await;
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":recording_id,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start test dictation");
    let operation =
        next_browser_json_of_type(browser, "dictation_operation", "test dictation operation").await;
    let operation_id = operation["operation_id"]
        .as_str()
        .expect("test dictation operation ID")
        .to_owned();
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("commit test dictation");
    operation_id
}

async fn start_mock_dictation_browser() -> (tempfile::TempDir, ChildGuard, BrowserSocket) {
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve mock port")
        .local_addr()
        .expect("mock address")
        .port();
    let directory = tempfile::tempdir().expect("mock temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        json!({
            "listenHost":"127.0.0.1",
            "listenPort":port,
            "forceMock":true,
            "secretProvider":"environment",
            "paseoHosts":[
                {"id":"first","label":"First","target":null,"default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"},
                {"id":"second","label":"Second","target":"second.example:6767","default":false,"defaultCwd":"~/dev","defaultProvider":"opencode/model"}
            ],
            "publicDir":workspace.join("public"),
            "journalPath":directory.path().join("state/journal.sqlite3")
        })
        .to_string(),
    )
    .expect("write mock config");
    let child = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
        .arg("serve")
        .current_dir(&workspace)
        .env_clear()
        .envs(essential_os_env())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start mock runtime");
    let guard = ChildGuard(child);
    let health_url = format!("http://127.0.0.1:{port}/healthz");
    let mut healthy = false;
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            healthy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(healthy, "mock runtime became healthy");
    let (mut browser, _) = connect_async(format!("ws://127.0.0.1:{port}/ws"))
        .await
        .expect("mock browser websocket");
    initialize_browser_protocol(&mut browser, "initial mock frame").await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select mock dictation");
    assert_eq!(
        next_browser_json(&mut browser, "mock dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );
    (directory, guard, browser)
}

#[allow(clippy::too_many_lines)]
async fn bind_fake_realtime_context(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    suffix: &str,
) {
    let mut saw_response_create = false;
    let mut saw_bind_context = false;
    loop {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("bind request timeout")
            .expect("bind request")
            .expect("bind request result");
        let Message::Text(message) = message else {
            continue;
        };
        let event: Value = serde_json::from_str(&message).expect("bind request JSON");
        assert_ne!(event["type"], "input_audio_buffer.commit");
        saw_response_create |= event["type"] == "response.create";
        saw_bind_context |= event
            .pointer("/item/content/0/text")
            .and_then(Value::as_str)
            == Some("bind context");
        if saw_response_create && saw_bind_context {
            break;
        }
    }
    let response_id = format!("bind-response-{suffix}");
    socket
        .send(Message::Text(
            json!({"type":"response.created","response":{"id":response_id}})
                .to_string()
                .into(),
        ))
        .await
        .expect("send binding response created");
    let session = if matches!(
        suffix,
        "second-host-after-transcription" | "initial-thread-b"
    ) {
        "thread-b"
    } else {
        "thread-a"
    };
    for (output_index, (name, arguments)) in [
        (
            "set_current_session",
            json!({"session":session}).to_string(),
        ),
        ("read_latest_reply", "{}".to_owned()),
    ]
    .into_iter()
    .enumerate()
    {
        let call_id = format!("bind-{suffix}-{name}");
        let item_id = format!("bind-item-{suffix}-{name}");
        for event in function_call_events_at(
            &response_id,
            &item_id,
            &call_id,
            output_index as u64,
            name,
            &arguments,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send binding tool call");
        }
    }
    let mut outputs = 0;
    while outputs < 2 {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("binding output timeout")
            .expect("binding output")
            .expect("binding output result");
        let Message::Text(message) = message else {
            continue;
        };
        let event: Value = serde_json::from_str(&message).expect("binding output JSON");
        outputs += usize::from(
            event.pointer("/item/type").and_then(Value::as_str) == Some("function_call_output"),
        );
        assert_ne!(event["type"], "response.create");
        assert_ne!(event["type"], "input_audio_buffer.commit");
    }
    socket
        .send(Message::Text(
            json!({
                "type":"response.done",
                "response":{"id":response_id,"status":"completed"}
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("complete binding response");
    loop {
        if next_fake_realtime_json(socket, "binding followup request").await["type"]
            == "response.create"
        {
            break;
        }
    }
    let followup_id = format!("bind-followup-{suffix}");
    for event in [
        json!({"type":"response.created","response":{"id":followup_id}}),
        json!({
            "type":"response.done",
            "response":{"id":followup_id,"status":"completed"}
        }),
    ] {
        socket
            .send(Message::Text(event.to_string().into()))
            .await
            .expect("complete binding followup");
    }
}

async fn begin_existing_proposal_followup(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    suffix: &str,
    proposal_text: &str,
) {
    loop {
        if next_fake_realtime_json(socket, "proposal response request").await["type"]
            == "response.create"
        {
            break;
        }
    }
    let response_id = format!("proposal-response-{suffix}");
    let proposal_call_id = format!("proposal-call-{suffix}");
    socket
        .send(Message::Text(
            json!({"type":"response.created","response":{"id":response_id}})
                .to_string()
                .into(),
        ))
        .await
        .expect("create proposal response");
    for event in function_call_events(
        &response_id,
        &format!("proposal-item-{suffix}"),
        &proposal_call_id,
        "send_message",
        &json!({"text":proposal_text}).to_string(),
    ) {
        socket
            .send(Message::Text(event.to_string().into()))
            .await
            .expect("send proposal call");
    }
    let proposal = next_function_output(socket, &proposal_call_id, "proposal output").await;
    assert_eq!(proposal["proposed"], true);
    socket
        .send(Message::Text(
            json!({
                "type":"response.done",
                "response":{"id":response_id,"status":"completed"}
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("complete proposal response");
    loop {
        if next_fake_realtime_json(socket, "proposal followup request").await["type"]
            == "response.create"
        {
            break;
        }
    }
    let followup_id = format!("proposal-followup-{suffix}");
    let fast_call_id = format!("proposal-fast-call-{suffix}");
    socket
        .send(Message::Text(
            json!({"type":"response.created","response":{"id":followup_id}})
                .to_string()
                .into(),
        ))
        .await
        .expect("create proposal followup");
    for event in function_call_events(
        &followup_id,
        &format!("proposal-fast-item-{suffix}"),
        &fast_call_id,
        "list_sessions",
        "{}",
    ) {
        socket
            .send(Message::Text(event.to_string().into()))
            .await
            .expect("send fast followup call");
    }
    let output = next_function_output(socket, &fast_call_id, "fast followup output").await;
    assert_eq!(output["ok"], true);
}

async fn begin_send_proposal(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    suffix: &str,
    proposal_text: &str,
) {
    loop {
        if next_fake_realtime_json(socket, "send proposal response request").await["type"]
            == "response.create"
        {
            break;
        }
    }
    let response_id = format!("send-proposal-response-{suffix}");
    let call_id = format!("send-proposal-call-{suffix}");
    socket
        .send(Message::Text(
            json!({"type":"response.created","response":{"id":response_id}})
                .to_string()
                .into(),
        ))
        .await
        .expect("create send proposal response");
    for event in function_call_events(
        &response_id,
        &format!("send-proposal-item-{suffix}"),
        &call_id,
        "send_message",
        &json!({"text":proposal_text}).to_string(),
    ) {
        socket
            .send(Message::Text(event.to_string().into()))
            .await
            .expect("send proposal tool call");
    }
    let proposal = next_function_output(socket, &call_id, "send proposal output").await;
    assert_eq!(proposal["proposed"], true);
}

struct LiveRealtimeRuntime {
    _directory: tempfile::TempDir,
    _guard: ChildGuard,
    browser: BrowserSocket,
}

struct InjectedRealtimeRuntime {
    directory: tempfile::TempDir,
    _guard: ServerGuard,
    browser: BrowserSocket,
}

async fn start_injected_realtime_browser(
    connector: Arc<dyn RealtimeConnector>,
) -> InjectedRealtimeRuntime {
    start_injected_realtime_browser_at(
        connector,
        "wss://realtime.example.test/v1/realtime",
        Some("test-key"),
        "gpt-realtime-2.1",
    )
    .await
}

async fn start_injected_realtime_browser_at(
    connector: Arc<dyn RealtimeConnector>,
    openai_base_url: &str,
    api_key: Option<&str>,
    openai_model: &str,
) -> InjectedRealtimeRuntime {
    start_injected_realtime_browser_with_process(
        connector,
        openai_base_url,
        api_key,
        openai_model,
        None,
        Arc::new(SystemProcessExecutor),
    )
    .await
}

async fn start_injected_realtime_browser_with_process(
    connector: Arc<dyn RealtimeConnector>,
    openai_base_url: &str,
    api_key: Option<&str>,
    openai_model: &str,
    paseo_password: Option<&str>,
    process_executor: Arc<dyn ProcessExecutor>,
) -> InjectedRealtimeRuntime {
    start_injected_realtime_browser_with_hosts_and_process(
        connector,
        openai_base_url,
        api_key,
        openai_model,
        paseo_password,
        process_executor,
        Config::default().paseo_hosts,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_injected_realtime_browser_with_hosts_and_process(
    connector: Arc<dyn RealtimeConnector>,
    openai_base_url: &str,
    api_key: Option<&str>,
    openai_model: &str,
    paseo_password: Option<&str>,
    process_executor: Arc<dyn ProcessExecutor>,
    paseo_hosts: Vec<PaseoHostProfile>,
) -> InjectedRealtimeRuntime {
    start_injected_realtime_browser_with_hosts_process_and_cleaner(
        connector,
        openai_base_url,
        api_key,
        openai_model,
        paseo_password,
        process_executor,
        paseo_hosts,
        Arc::new(PanickingDictationCleaner),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_injected_realtime_browser_with_hosts_process_and_cleaner(
    connector: Arc<dyn RealtimeConnector>,
    openai_base_url: &str,
    api_key: Option<&str>,
    openai_model: &str,
    paseo_password: Option<&str>,
    process_executor: Arc<dyn ProcessExecutor>,
    paseo_hosts: Vec<PaseoHostProfile>,
    dictation_cleaner: Arc<dyn DictationCleaner>,
) -> InjectedRealtimeRuntime {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("injected runtime listener");
    let port = listener
        .local_addr()
        .expect("injected runtime address")
        .port();
    let directory = tempfile::tempdir().expect("injected runtime directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let api_key = api_key.map(str::to_owned);
    let paseo_password = paseo_password.map(str::to_owned);
    let config = Config {
        listen_host: "127.0.0.1".to_owned(),
        listen_port: port,
        openai_base_url: openai_base_url.to_owned(),
        openai_model: openai_model.to_owned(),
        public_dir: workspace.join("public"),
        journal_path: directory.path().join("state/journal.sqlite3"),
        paseo_hosts,
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        serve(
            config,
            Secrets {
                openai_api_key: api_key,
                paseo_password,
            },
            RuntimeDependencies {
                clock: Arc::new(SystemClock::start()),
                http_client: reqwest::Client::new(),
                realtime_connector: connector,
                dictation_cleaner,
                process_executor,
            },
            listener,
        )
        .await
        .expect("serve injected runtime");
    });
    let guard = ServerGuard(server);
    let health_url = format!("http://127.0.0.1:{port}/healthz");
    let mut healthy = false;
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            healthy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(healthy, "injected runtime became healthy");
    let (browser, _) = connect_async(format!("ws://127.0.0.1:{port}/ws"))
        .await
        .expect("injected runtime browser websocket");
    InjectedRealtimeRuntime {
        directory,
        _guard: guard,
        browser,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn slow_initial_presentation_does_not_block_browser_or_provider_transport() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingInitialPresentationExecutor {
        latch,
        list_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("slow initial presentation Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("slow initial presentation Realtime address")
        .port();
    let (exercise_sender, exercise_receiver) = tokio::sync::oneshot::channel();
    let (provider_sender, provider_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "slow initial session update").await["type"],
            "session.update"
        );
        exercise_receiver
            .await
            .expect("exercise slow initial transport");
        for event in [
            json!({"type":"session.created"}),
            json!({"type":"response.created","response":{"id":"initial-unsolicited"}}),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send event during slow initial presentation");
        }
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "slow initial provider transport").await;
            if event["type"] == "response.cancel" && event["response_id"] == "initial-unsolicited" {
                break;
            }
        }
        provider_sender
            .send(())
            .expect("signal provider transport during initial presentation");
        finish_receiver
            .await
            .expect("finish slow initial presentation test");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send initial browser hello");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "initial protocol ready").await,
        json!({"type":"protocol_ready","version":2})
    );
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("slow initial process start timeout")
        .expect("slow initial process start signal");
    exercise_sender
        .send(())
        .expect("exercise transport during initial presentation");
    let rejected_turn = test_text_turn!("blocked before routing presentation", null);
    let rejected_turn_id = test_text_turn_id(&rejected_turn);
    for control in [json!({"type":"hello","protocol_version":2}), rejected_turn] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send control during initial presentation");
    }
    let browser_transport = tokio::time::timeout(Duration::from_secs(2), async {
        let mut repeated_hello = false;
        let mut turn_rejected = false;
        while !repeated_hello || !turn_rejected {
            let frame = next_browser_json_including_text_ack(
                &mut runtime.browser,
                "browser transport during initial presentation",
            )
            .await;
            repeated_hello |= frame == json!({"type":"protocol_ready","version":2});
            if frame["type"] == "text_turn_rejected" {
                assert_eq!(frame["turn_id"], rejected_turn_id);
                turn_rejected = true;
            }
        }
    });
    let provider_transport = tokio::time::timeout(Duration::from_secs(2), provider_receiver);
    let (browser_result, provider_result) = tokio::join!(browser_transport, provider_transport);
    browser_result.expect("initial presentation blocked browser transport");
    provider_result
        .expect("initial presentation blocked provider transport")
        .expect("provider transport signal");
    assert_eq!(executor.list_calls.load(Ordering::SeqCst), 1);

    release_guard.release();
    let mut presentation_order = Vec::new();
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "initial presentation completion").await;
        if matches!(
            frame["type"].as_str(),
            Some("host_state" | "dashboard_state")
        ) {
            presentation_order.push(frame["type"].as_str().expect("frame type").to_owned());
        }
        if frame == json!({"type":"state","state":"ready"}) {
            break;
        }
    }
    assert_eq!(presentation_order, ["host_state", "dashboard_state"]);
    let calls_after_initial = executor.list_calls.load(Ordering::SeqCst);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat hello after initial presentation");
    let mut cached_order = Vec::new();
    while cached_order.len() < 2 {
        let frame = next_browser_json(&mut runtime.browser, "cached initial presentation").await;
        if matches!(
            frame["type"].as_str(),
            Some("host_state" | "dashboard_state")
        ) {
            cached_order.push(frame["type"].as_str().expect("frame type").to_owned());
        }
    }
    assert_eq!(cached_order, ["host_state", "dashboard_state"]);
    assert_eq!(
        executor.list_calls.load(Ordering::SeqCst),
        calls_after_initial
    );
    finish_sender
        .send(())
        .expect("finish slow initial presentation test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn slow_host_selection_preserves_transport_and_publishes_exact_order() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingHostSelectionExecutor {
        latch,
        second_host_armed: AtomicBool::new(false),
        list_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("slow host selection Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("slow host selection Realtime address")
        .port();
    let (exercise_sender, exercise_receiver) = tokio::sync::oneshot::channel();
    let (provider_sender, provider_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "slow host session update").await["type"],
            "session.update"
        );
        socket
            .send(Message::Text(
                json!({"type":"session.created"}).to_string().into(),
            ))
            .await
            .expect("ready slow host provider");
        exercise_receiver
            .await
            .expect("exercise slow host transport");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"host-unsolicited"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send event during slow host selection");
        loop {
            let event = next_fake_realtime_json(&mut socket, "slow host provider transport").await;
            if event["type"] == "response.cancel" && event["response_id"] == "host-unsolicited" {
                break;
            }
        }
        provider_sender
            .send(())
            .expect("signal provider transport during host selection");
        finish_receiver
            .await
            .expect("finish slow host selection test");
    });

    let hosts = vec![
        PaseoHostProfile {
            id: "first".to_owned(),
            label: "First".to_owned(),
            target: None,
            default: true,
            default_cwd: "~/".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
        PaseoHostProfile {
            id: "second".to_owned(),
            label: "Second".to_owned(),
            target: Some("second.example:6767".to_owned()),
            default: false,
            default_cwd: "~/dev".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
    ];
    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_hosts_and_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
        hosts,
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send slow host browser hello");
    loop {
        if next_browser_json(&mut runtime.browser, "slow host initialization").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    executor.second_host_armed.store(true, Ordering::SeqCst);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select slow second host");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("slow host process start timeout")
        .expect("slow host process start signal");
    exercise_sender
        .send(())
        .expect("exercise transport during host selection");
    let calls_while_blocked = executor.list_calls.load(Ordering::SeqCst);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat hello during host selection");
    let cached_hello = tokio::time::timeout(Duration::from_secs(2), async {
        let mut host = None;
        let mut dashboard = None;
        while host.is_none() || dashboard.is_none() {
            let frame =
                next_browser_json(&mut runtime.browser, "cached host selection hello").await;
            match frame["type"].as_str() {
                Some("host_state") => host = Some(frame),
                Some("dashboard_state") => dashboard = Some(frame),
                _ => {}
            }
        }
        (
            host.expect("cached host"),
            dashboard.expect("cached dashboard"),
        )
    })
    .await
    .expect("host selection blocked cached hello");
    assert_eq!(cached_hello.0["selected_host_id"], "first");
    assert_eq!(cached_hello.1["selected_host_id"], "first");
    tokio::time::timeout(Duration::from_secs(2), provider_receiver)
        .await
        .expect("host selection blocked provider transport")
        .expect("provider transport signal");
    assert_eq!(
        executor.list_calls.load(Ordering::SeqCst),
        calls_while_blocked,
        "repeated hello started another process probe"
    );

    release_guard.release();
    let mut ordered_frames = Vec::new();
    loop {
        let frame = next_browser_json(&mut runtime.browser, "slow host completion").await;
        match frame["type"].as_str() {
            Some("host_state") => {
                assert_eq!(frame["selected_host_id"], "second");
                ordered_frames.push("host_state");
            }
            Some("dashboard_state") => {
                assert_eq!(frame["selected_host_id"], "second");
                ordered_frames.push("dashboard_state");
            }
            Some("proposal") => {
                assert_eq!(frame["echo"], Value::Null);
                ordered_frames.push("proposal");
            }
            Some("state") if frame["state"] == "ready" => {
                ordered_frames.push("ready");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        ordered_frames,
        ["host_state", "dashboard_state", "proposal", "ready"]
    );
    finish_sender
        .send(())
        .expect("finish slow host selection test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn live_commit_ack_before_changed_host_worker_waits_for_transition_ready() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingHostSelectionExecutor {
        latch,
        second_host_armed: AtomicBool::new(false),
        list_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reverse host commit Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("reverse host commit Realtime address")
        .port();
    let (commit_sender, commit_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "reverse host commit session update").await["type"],
            "session.update"
        );
        socket
            .send(Message::Text(
                json!({"type":"session.created"}).to_string().into(),
            ))
            .await
            .expect("ready reverse host commit provider");
        loop {
            if next_fake_realtime_json(&mut socket, "reverse host audio commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        commit_sender
            .send(())
            .expect("signal reverse host audio commit");
        loop {
            if next_fake_realtime_json(&mut socket, "reverse host input clear").await["type"]
                == "input_audio_buffer.clear"
            {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"reverse-host-commit-item",
                "previous_item_id":null
            }),
            json!({
                "type":"error",
                "error":{"type":"server_error","message":"reverse host commit barrier"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send reverse host commit event");
        }
        finish_receiver
            .await
            .expect("finish reverse host commit test");
    });

    let hosts = vec![
        PaseoHostProfile {
            id: "first".to_owned(),
            label: "First".to_owned(),
            target: None,
            default: true,
            default_cwd: "~/".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
        PaseoHostProfile {
            id: "second".to_owned(),
            label: "Second".to_owned(),
            target: Some("second.example:6767".to_owned()),
            default: false,
            default_cwd: "~/dev".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
    ];
    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_hosts_and_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
        hosts,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "reverse host commit browser").await;
    loop {
        if next_browser_json(&mut runtime.browser, "reverse host commit initial ready").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start reverse host live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "reverse host live start").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"reverse host live audio").into()))
        .await
        .expect("send reverse host live audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("commit reverse host live audio");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "reverse host commit flush").await,
        json!({"type":"flush_audio"})
    );
    commit_receiver
        .await
        .expect("reverse host audio commit signal");
    executor.second_host_armed.store(true, Ordering::SeqCst);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change host with live commit pending");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("reverse host worker start timeout")
        .expect("reverse host worker start signal");

    loop {
        let frame = next_browser_json(&mut runtime.browser, "reverse host pre-completion").await;
        assert_ne!(
            frame,
            json!({"type":"state","state":"ready"}),
            "live commit acknowledgement emitted ready before the host worker"
        );
        if frame
            == json!({
                "type":"error",
                "message":"Realtime provider error."
            })
        {
            break;
        }
    }

    release_guard.release();
    let mut transition_order = Vec::new();
    loop {
        let frame = next_browser_json(&mut runtime.browser, "reverse host completion").await;
        match frame["type"].as_str() {
            Some("host_state") => {
                assert_eq!(frame["selected_host_id"], "second");
                transition_order.push("host_state");
            }
            Some("dashboard_state") => {
                assert_eq!(frame["selected_host_id"], "second");
                transition_order.push("dashboard_state");
            }
            Some("proposal") => {
                assert_eq!(frame["echo"], Value::Null);
                transition_order.push("proposal");
            }
            Some("state") if frame["state"] == "ready" => {
                transition_order.push("ready");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        transition_order,
        ["host_state", "dashboard_state", "proposal", "ready"]
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send reverse host ready barrier");
    for _ in 0..6 {
        assert_ne!(
            next_browser_json(&mut runtime.browser, "reverse host ready barrier").await,
            json!({"type":"state","state":"ready"}),
            "reverse host completion emitted duplicate ready states"
        );
    }
    finish_sender
        .send(())
        .expect("finish reverse host commit provider");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn slow_confirmation_preserves_transport_and_executes_once() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingConfirmedSendExecutor {
        latch,
        start_gate: None,
        send_calls: AtomicUsize::new(0),
        permission_calls: AtomicUsize::new(0),
        send_returned: AtomicBool::new(false),
        post_send_probe: Mutex::new(None),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("slow confirmation Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("slow confirmation Realtime address")
        .port();
    let (proposal_sender, proposal_receiver) = tokio::sync::oneshot::channel();
    let (exercise_sender, exercise_receiver) = tokio::sync::oneshot::channel();
    let (provider_sender, provider_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "slow confirmation session update").await["type"],
            "session.update"
        );
        socket
            .send(Message::Text(
                json!({"type":"session.created"}).to_string().into(),
            ))
            .await
            .expect("ready slow confirmation provider");
        bind_fake_realtime_context(&mut socket, "slow-confirmation").await;
        loop {
            if next_fake_realtime_json(&mut socket, "slow confirmation response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"slow-confirm-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create slow confirmation response");
        for event in function_call_events(
            "slow-confirm-response",
            "slow-confirm-item",
            "slow-confirm-call",
            "send_message",
            r#"{"text":"send exactly once"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send slow confirmation proposal call");
        }
        let proposal = next_function_output(
            &mut socket,
            "slow-confirm-call",
            "slow confirmation proposal output",
        )
        .await;
        assert_eq!(proposal["proposed"], true);
        proposal_sender
            .send(())
            .expect("signal slow confirmation proposal");
        exercise_receiver
            .await
            .expect("exercise slow confirmation transport");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"confirm-unsolicited"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send event during slow confirmation");
        loop {
            let event = next_fake_realtime_json(&mut socket, "slow confirmation provider").await;
            if event["type"] == "response.cancel" && event["response_id"] == "confirm-unsolicited" {
                break;
            }
        }
        provider_sender
            .send(())
            .expect("signal provider transport during confirmation");
        finish_receiver
            .await
            .expect("finish slow confirmation test");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "slow confirmation browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    loop {
        if next_browser_json(&mut runtime.browser, "settled slow confirmation context").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    let proposal_turn = test_text_turn!("propose slow confirmation", summary_id.as_str());
    let proposal_turn_id = test_text_turn_id(&proposal_turn);
    runtime
        .browser
        .send(Message::Text(proposal_turn.to_string().into()))
        .await
        .expect("request slow confirmation proposal");
    loop {
        let frame = next_browser_json_including_text_ack(
            &mut runtime.browser,
            "slow confirmation turn acceptance",
        )
        .await;
        assert_ne!(frame["type"], "text_turn_rejected");
        if frame["type"] == "text_turn_accepted" {
            assert_eq!(frame["turn_id"], proposal_turn_id);
            break;
        }
    }
    proposal_receiver
        .await
        .expect("slow confirmation proposal signal");
    let proposal = next_browser_json_of_type(
        &mut runtime.browser,
        "proposal",
        "slow confirmation proposal",
    )
    .await;
    let handle = proposal["handle"]
        .as_str()
        .expect("slow confirmation handle")
        .to_owned();
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("start slow confirmation");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("slow confirmation process start timeout")
        .expect("slow confirmation process start signal");
    exercise_sender
        .send(())
        .expect("exercise transport during confirmation");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("duplicate slow confirmation");
    tokio::time::timeout(Duration::from_secs(2), provider_receiver)
        .await
        .expect("confirmation blocked provider transport")
        .expect("provider transport signal");
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 1);

    release_guard.release();
    let mut completion_order = Vec::new();
    loop {
        let frame = next_browser_json(&mut runtime.browser, "slow confirmation completion").await;
        match frame["type"].as_str() {
            Some("host_state") => completion_order.push("host_state"),
            Some("dashboard_state") => completion_order.push("dashboard_state"),
            Some("proposal") => {
                assert_eq!(frame["echo"], Value::Null);
                completion_order.push("proposal");
            }
            Some("transcript_done") => {
                assert_eq!(
                    frame["text"],
                    "Confirmed, but the delivery outcome is unknown. It was not retried."
                );
                completion_order.push("transcript_done");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        completion_order,
        [
            "host_state",
            "dashboard_state",
            "proposal",
            "transcript_done"
        ]
    );
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 1);
    finish_sender
        .send(())
        .expect("finish slow confirmation test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn latched_confirmation_retires_origin_before_late_provider_output() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingConfirmedSendExecutor {
        latch,
        start_gate: None,
        send_calls: AtomicUsize::new(0),
        permission_calls: AtomicUsize::new(0),
        send_returned: AtomicBool::new(false),
        post_send_probe: Mutex::new(None),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("confirmation retirement Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("confirmation retirement Realtime address")
        .port();
    let (proposal_sender, proposal_receiver) = tokio::sync::oneshot::channel();
    let (retired_sender, retired_receiver) = tokio::sync::oneshot::channel();
    let (inspect_sender, inspect_receiver) = tokio::sync::oneshot::channel();
    let (checked_sender, checked_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "confirmation retirement session update").await["type"],
            "session.update"
        );
        socket
            .send(Message::Text(
                json!({"type":"session.created"}).to_string().into(),
            ))
            .await
            .expect("ready confirmation retirement provider");
        bind_fake_realtime_context(&mut socket, "confirmation-retirement").await;
        begin_send_proposal(
            &mut socket,
            "confirmation-retirement",
            "retire provider output",
        )
        .await;
        proposal_sender
            .send(())
            .expect("signal confirmation retirement proposal");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "confirmation origin retirement").await;
            if event["type"] == "response.cancel" {
                assert_eq!(
                    event["response_id"],
                    "send-proposal-response-confirmation-retirement"
                );
                break;
            }
        }
        retired_sender
            .send(())
            .expect("signal confirmation origin retirement");
        let response_id = "send-proposal-response-confirmation-retirement";
        let mut late_events = vec![
            json!({
                "type":"response.output_audio.delta",
                "response_id":response_id,
                "delta":BASE64.encode(test_pcm(b"late confirmed audio"))
            }),
            json!({
                "type":"response.output_audio_transcript.delta",
                "response_id":response_id,
                "delta":"late confirmed transcript"
            }),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":response_id,
                "transcript":"late confirmed transcript"
            }),
        ];
        late_events.extend(function_call_events_at(
            response_id,
            "late-confirmation-item",
            "late-confirmation-call",
            1,
            "list_pending_permissions",
            "{}",
        ));
        late_events.extend([
            json!({
                "type":"response.done",
                "response":{"id":response_id,"status":"completed"}
            }),
            json!({
                "type":"error",
                "error":{"type":"server_error","message":"confirmation retirement barrier"}
            }),
        ]);
        for event in late_events {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send late confirmation provider event");
        }

        inspect_receiver
            .await
            .expect("inspect confirmation retirement provider output");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.created",
                    "response":{"id":"confirmation-retirement-barrier"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send confirmation retirement provider barrier");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "confirmation retirement barrier").await;
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output"),
                "late confirmation tool published provider output"
            );
            assert_ne!(
                event["type"], "response.create",
                "confirmation dispatched a queued provider followup"
            );
            if event["type"] == "response.cancel"
                && event["response_id"] == "confirmation-retirement-barrier"
            {
                break;
            }
        }
        checked_sender
            .send(())
            .expect("signal confirmation retirement provider check");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "confirmation retirement browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    loop {
        if next_browser_json(
            &mut runtime.browser,
            "confirmation retirement initial ready",
        )
        .await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("propose confirmation retirement", summary_id.as_str())
                .to_string()
                .into(),
        ))
        .await
        .expect("request confirmation retirement proposal");
    proposal_receiver
        .await
        .expect("confirmation retirement proposal signal");
    let proposal = next_browser_json_of_type(
        &mut runtime.browser,
        "proposal",
        "confirmation retirement proposal",
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":proposal["handle"]})
                .to_string()
                .into(),
        ))
        .await
        .expect("start confirmation retirement");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("confirmation retirement process start timeout")
        .expect("confirmation retirement process start signal");
    tokio::time::timeout(Duration::from_secs(2), retired_receiver)
        .await
        .expect("latched confirmation did not retire its provider response")
        .expect("confirmation origin retirement signal");

    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "late confirmation browser frame").await;
        assert_ne!(frame["type"], "transcript_delta");
        assert_ne!(frame["type"], "transcript_done");
        assert_ne!(frame["type"], "tool");
        assert_ne!(frame["type"], "proposal");
        if frame
            == json!({
                "type":"error",
                "message":"Realtime provider error."
            })
        {
            break;
        }
        assert_eq!(frame, json!({"type":"flush_audio"}));
    }
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.permission_calls.load(Ordering::SeqCst), 0);

    release_guard.release();
    let mut completion_order = Vec::new();
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "confirmation retirement completion").await;
        match frame["type"].as_str() {
            Some("host_state") => completion_order.push("host_state"),
            Some("dashboard_state") => completion_order.push("dashboard_state"),
            Some("proposal") => {
                assert_eq!(frame["echo"], Value::Null);
                completion_order.push("proposal");
            }
            Some("transcript_done") => {
                assert_eq!(
                    frame["text"],
                    "Confirmed, but the delivery outcome is unknown. It was not retried."
                );
                completion_order.push("transcript_done");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        completion_order,
        [
            "host_state",
            "dashboard_state",
            "proposal",
            "transcript_done"
        ]
    );
    inspect_sender
        .send(())
        .expect("inspect confirmation retirement provider");
    tokio::time::timeout(Duration::from_secs(2), checked_receiver)
        .await
        .expect("confirmation retirement provider check timeout")
        .expect("confirmation retirement provider check signal");
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 1);
    assert_eq!(executor.permission_calls.load(Ordering::SeqCst), 0);
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn disconnected_confirmation_finishes_one_terminal_journal_classification() {
    let (latch, started, release_guard) = process_latch();
    let (start_gate, start_gate_guard) = process_start_gate();
    let (terminal_sender, terminal_receiver) = tokio::sync::oneshot::channel();
    let executor = Arc::new(LatchingConfirmedSendExecutor {
        latch,
        start_gate: Some(start_gate),
        send_calls: AtomicUsize::new(0),
        permission_calls: AtomicUsize::new(0),
        send_returned: AtomicBool::new(false),
        post_send_probe: Mutex::new(Some(terminal_sender)),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("disconnected confirmation Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("disconnected confirmation Realtime address")
        .port();
    let (proposal_sender, proposal_receiver) = tokio::sync::oneshot::channel();
    let (closed_sender, closed_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "disconnected confirmation session update").await
                ["type"],
            "session.update"
        );
        socket
            .send(Message::Text(
                json!({"type":"session.created"}).to_string().into(),
            ))
            .await
            .expect("ready disconnected confirmation provider");
        bind_fake_realtime_context(&mut socket, "disconnected-confirmation").await;
        begin_send_proposal(
            &mut socket,
            "disconnected-confirmation",
            "classify after disconnect",
        )
        .await;
        proposal_sender
            .send(())
            .expect("signal disconnected confirmation proposal");
        loop {
            match tokio::time::timeout(Duration::from_secs(2), socket.next()).await {
                Ok(Some(Ok(Message::Close(_)) | Err(_)) | None) => break,
                Ok(Some(Ok(_))) => {}
                Err(error) => panic!("confirmed transport did not disconnect: {error}"),
            }
        }
        closed_sender
            .send(())
            .expect("signal confirmed transport disconnect");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "disconnected confirmation browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    loop {
        if next_browser_json(
            &mut runtime.browser,
            "settled disconnected confirmation context",
        )
        .await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    let proposal_turn = test_text_turn!("propose disconnected confirmation", summary_id.as_str());
    runtime
        .browser
        .send(Message::Text(proposal_turn.to_string().into()))
        .await
        .expect("request disconnected confirmation proposal");
    proposal_receiver
        .await
        .expect("disconnected confirmation proposal signal");
    let proposal = next_browser_json_of_type(
        &mut runtime.browser,
        "proposal",
        "disconnected confirmation proposal",
    )
    .await;
    let handle = proposal["handle"]
        .as_str()
        .expect("disconnected confirmation handle")
        .to_owned();
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("start disconnected confirmation");
    runtime
        .browser
        .close(None)
        .await
        .expect("disconnect confirmed browser");
    tokio::time::timeout(Duration::from_secs(2), closed_receiver)
        .await
        .expect("confirmation blocked disconnect")
        .expect("provider disconnect signal");
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 0);

    start_gate_guard.release();
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("post-disconnect confirmation process start timeout")
        .expect("post-disconnect confirmation process start signal");
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 1);
    release_guard.release();
    tokio::time::timeout(Duration::from_secs(2), terminal_receiver)
        .await
        .expect("detached confirmation terminal classification timeout")
        .expect("detached confirmation terminal classification signal");
    let journal = Journal::open(&runtime.directory.path().join("state/journal.sqlite3"))
        .expect("open disconnected confirmation journal");
    let timeline = journal
        .timeline(None, 10)
        .expect("read disconnected confirmation journal");
    assert_eq!(timeline.len(), 1);
    assert_eq!(timeline[0].state, "outcome_unknown");
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 1);
    fake_realtime.await.expect("fake Realtime task");
}

#[allow(dead_code)]
struct DelayedSummariser {
    base_url: String,
    started: Option<tokio::sync::oneshot::Receiver<()>>,
    request_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

#[allow(dead_code)]
impl DelayedSummariser {
    async fn start(content: &str, delay: Duration) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("delayed summariser listener");
        let address = listener.local_addr().expect("delayed summariser address");
        let (started_sender, started) = tokio::sync::oneshot::channel();
        let started_sender = std::sync::Arc::new(std::sync::Mutex::new(Some(started_sender)));
        let handler_started_sender = std::sync::Arc::clone(&started_sender);
        let request_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handler_request_count = std::sync::Arc::clone(&request_count);
        let content = content.to_owned();
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                handler_request_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let started_sender = std::sync::Arc::clone(&handler_started_sender);
                let content = content.clone();
                async move {
                    if let Some(sender) = started_sender
                        .lock()
                        .expect("delayed summary start lock")
                        .take()
                    {
                        sender.send(()).expect("signal delayed summary start");
                    }
                    tokio::time::sleep(delay).await;
                    axum::Json(json!({"choices": [{"message": {"content": content}}]}))
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("delayed summariser server");
        });
        Self {
            base_url: format!("http://{address}/v1"),
            started: Some(started),
            request_count,
            server,
        }
    }

    async fn wait_started(&mut self) {
        tokio::time::timeout(
            Duration::from_secs(3),
            self.started.take().expect("unused summary start receiver"),
        )
        .await
        .expect("delayed summary start timeout")
        .expect("delayed summary start signal");
    }

    fn request_count(&self) -> usize {
        self.request_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for DelayedSummariser {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[allow(dead_code)]
struct LatchedSummariser {
    base_url: String,
    started: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
    request_count: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

#[allow(dead_code)]
impl LatchedSummariser {
    async fn start(content: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("latched summariser listener");
        let address = listener.local_addr().expect("latched summariser address");
        let (started_sender, started) = tokio::sync::oneshot::channel();
        let started_sender = Arc::new(Mutex::new(Some(started_sender)));
        let (release, release_receiver) = tokio::sync::oneshot::channel();
        let release_receiver = Arc::new(Mutex::new(Some(release_receiver)));
        let request_count = Arc::new(AtomicUsize::new(0));
        let handler_started = Arc::clone(&started_sender);
        let handler_release = Arc::clone(&release_receiver);
        let handler_request_count = Arc::clone(&request_count);
        let content = content.to_owned();
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                handler_request_count.fetch_add(1, Ordering::SeqCst);
                let started = handler_started
                    .lock()
                    .expect("latched summary start lock")
                    .take();
                let release = handler_release
                    .lock()
                    .expect("latched summary release lock")
                    .take();
                let content = content.clone();
                async move {
                    if let Some(started) = started {
                        let _ = started.send(());
                    }
                    if let Some(release) = release {
                        let _ = release.await;
                    }
                    axum::Json(json!({"choices": [{"message": {"content": content}}]}))
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("latched summariser server");
        });
        Self {
            base_url: format!("http://{address}/v1"),
            started: Some(started),
            release: Some(release),
            request_count,
            server,
        }
    }

    async fn wait_started(&mut self) {
        self.started
            .take()
            .expect("unused latched summary start receiver")
            .await
            .expect("latched summary start signal");
    }

    fn release(&mut self) {
        self.release
            .take()
            .expect("unused latched summary release")
            .send(())
            .expect("release latched summary");
    }

    fn request_count(&self) -> usize {
        self.request_count.load(Ordering::SeqCst)
    }
}

impl Drop for LatchedSummariser {
    fn drop(&mut self) {
        self.release.take();
        self.server.abort();
    }
}

#[cfg(unix)]
fn write_single_reply_paseo(
    directory: &tempfile::TempDir,
    file_name: &str,
    reply: &str,
) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;

    let path = directory.path().join(file_name);
    std::fs::write(
        &path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Session A\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf '%s\\n' '{}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            reply
        ),
    )
    .expect("write fake Paseo summary executable");
    let mut permissions = std::fs::metadata(&path)
        .expect("fake Paseo summary metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&path, permissions).expect("fake Paseo summary permissions");
    path
}

#[cfg(unix)]
fn write_polled_reply_paseo(
    directory: &tempfile::TempDir,
    file_name: &str,
    reply: &str,
) -> (std::path::PathBuf, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt as _;

    let path = directory.path().join(file_name);
    let completion_marker = directory.path().join("reply-complete");
    std::fs::write(
        &path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) if [ -f '{}' ]; then status=idle; else status=running; fi; ",
                "printf '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Session A\",\"status\":\"%s\"}}]\\n' \"$status\" ;;\n",
                "  logs) printf '%s\\n' '{}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            completion_marker.display(),
            reply
        ),
    )
    .expect("write fake polling Paseo executable");
    let mut permissions = std::fs::metadata(&path)
        .expect("fake polling Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&path, permissions).expect("fake polling Paseo permissions");
    (path, completion_marker)
}

async fn start_live_realtime_browser(realtime_port: u16) -> LiveRealtimeRuntime {
    start_live_realtime_browser_with_paseo(realtime_port, None).await
}

async fn start_live_realtime_browser_with_paseo(
    realtime_port: u16,
    paseo_bin: Option<&std::path::Path>,
) -> LiveRealtimeRuntime {
    start_live_realtime_browser_with_model(realtime_port, paseo_bin, None, None).await
}

#[allow(dead_code)]
async fn start_live_realtime_browser_with_summary(
    realtime_port: u16,
    paseo_bin: Option<&std::path::Path>,
    summariser_base_url: Option<&str>,
) -> LiveRealtimeRuntime {
    let threshold = summariser_base_url.map(|_| 3);
    start_live_realtime_browser_with_model(realtime_port, paseo_bin, summariser_base_url, threshold)
        .await
}

#[allow(dead_code)]
async fn start_live_realtime_browser_with_cleanup(
    realtime_port: u16,
    paseo_bin: &std::path::Path,
    cleanup_base_url: &str,
) -> LiveRealtimeRuntime {
    start_live_realtime_browser_with_model(
        realtime_port,
        Some(paseo_bin),
        Some(cleanup_base_url),
        None,
    )
    .await
}

#[cfg(unix)]
async fn start_live_realtime_browser_with_auto_reply(
    realtime_port: u16,
    paseo_bin: &std::path::Path,
) -> LiveRealtimeRuntime {
    start_live_realtime_browser_with_protocol(
        realtime_port,
        Some(paseo_bin),
        None,
        None,
        Some(250),
        true,
    )
    .await
}

async fn start_live_realtime_browser_with_model(
    realtime_port: u16,
    paseo_bin: Option<&std::path::Path>,
    model_base_url: Option<&str>,
    summarise_threshold_chars: Option<u64>,
) -> LiveRealtimeRuntime {
    start_live_realtime_browser_with_protocol(
        realtime_port,
        paseo_bin,
        model_base_url,
        summarise_threshold_chars,
        None,
        true,
    )
    .await
}

async fn start_live_realtime_browser_with_protocol(
    realtime_port: u16,
    paseo_bin: Option<&std::path::Path>,
    model_base_url: Option<&str>,
    summarise_threshold_chars: Option<u64>,
    auto_reply_poll_ms: Option<u64>,
    initialize_browser: bool,
) -> LiveRealtimeRuntime {
    let app_port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve app port")
        .local_addr()
        .expect("app address")
        .port();
    let directory = tempfile::tempdir().expect("temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = directory.path().join("config.json");
    let mut config = json!({
        "listenHost":"127.0.0.1",
        "listenPort":app_port,
        "openaiBaseUrl":format!("ws://127.0.0.1:{realtime_port}"),
        "secretProvider":"environment",
        "paseoHosts":[
            {"id":"local","label":"Local","target":null,"default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"},
            {"id":"second","label":"Second","target":"second.example:6767","default":false,"defaultCwd":"~/","defaultProvider":"opencode/model"}
        ],
        "publicDir":workspace.join("public"),
        "journalPath":directory.path().join("state/journal.sqlite3")
    });
    if let Some(paseo_bin) = paseo_bin {
        config["paseoBin"] = Value::String(paseo_bin.to_string_lossy().into_owned());
    }
    if let Some(model_base_url) = model_base_url {
        config["sparkBaseUrl"] = Value::String(model_base_url.to_owned());
    }
    if let Some(summarise_threshold_chars) = summarise_threshold_chars {
        config["summariseThresholdChars"] = Value::from(summarise_threshold_chars);
    }
    if let Some(auto_reply_poll_ms) = auto_reply_poll_ms {
        config["autoReplyPollMs"] = Value::from(auto_reply_poll_ms);
    }
    std::fs::write(&config_path, config.to_string()).expect("write config");
    let mut command = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"));
    command
        .arg("serve")
        .current_dir(&workspace)
        .env_clear()
        .envs(essential_os_env())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("OPENAI_API_KEY", "test-key")
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if paseo_bin.is_some() {
        command.env("PASEO_PASSWORD", "test-password");
    }
    let child = command.spawn().expect("start Rust runtime");
    let guard = ChildGuard(child);
    let health_url = format!("http://127.0.0.1:{app_port}/healthz");
    let mut healthy = false;
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            healthy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(healthy, "live runtime became healthy");
    let (mut browser, _) = connect_async(format!("ws://127.0.0.1:{app_port}/ws"))
        .await
        .expect("live browser websocket");
    if initialize_browser {
        initialize_browser_protocol(&mut browser, "initial live frame").await;
    }
    LiveRealtimeRuntime {
        _directory: directory,
        _guard: guard,
        browser,
    }
}

#[tokio::test]
#[cfg(unix)]
async fn completed_paseo_reply_is_automatically_summarised_by_realtime() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let (fake_paseo_path, completion_marker) = write_polled_reply_paseo(
        &paseo_directory,
        "fake-paseo-auto-reply",
        "The build passed. No blockers remain.",
    );
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "session update").await["type"],
            "session.update"
        );
        socket
            .send(Message::Text(
                json!({"type":"session.updated"}).to_string().into(),
            ))
            .await
            .expect("send session ready");

        let request = loop {
            let event = next_fake_realtime_json(&mut socket, "automatic summary request").await;
            if event["type"] == "response.create" {
                break event;
            }
        };
        assert_eq!(
            request.pointer("/response/conversation"),
            Some(&json!("none"))
        );
        assert_eq!(
            request.pointer("/response/metadata/kind"),
            Some(&json!("automatic_summary"))
        );
        assert_eq!(
            request.pointer("/response/input/0/content/0/text"),
            Some(&json!("The build passed. No blockers remain."))
        );
        assert_eq!(
            request.pointer("/response/output_modalities"),
            Some(&json!(["audio"]))
        );
        assert_eq!(request.pointer("/response/tools"), Some(&json!([])));
        assert_eq!(
            request.pointer("/response/tool_choice"),
            Some(&json!("none"))
        );

        for event in [
            json!({"type":"response.created","response":{"id":"automatic-summary-response"}}),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"automatic-summary-response",
                "transcript":"The build passed with no blockers."
            }),
            json!({
                "type":"response.done",
                "response":{"id":"automatic-summary-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete automatic summary response");
        }
    });

    let mut runtime =
        start_live_realtime_browser_with_auto_reply(realtime_port, &fake_paseo_path).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    std::fs::write(&completion_marker, b"complete").expect("mark reply complete");

    let mut bound = false;
    let mut spoken = false;
    for _ in 0..12 {
        let frame = tokio::time::timeout(
            Duration::from_secs(2),
            next_browser_json(&mut runtime.browser, "automatic reply browser frame"),
        )
        .await
        .expect("automatic reply frame timeout");
        if frame["type"] == "dashboard_state" && frame["bound_context"].is_object() {
            bound = true;
        }
        if frame["type"] == "transcript_done"
            && frame["text"] == "The build passed with no blockers."
        {
            spoken = true;
            break;
        }
    }
    assert!(bound, "automatic reply context was not published");
    assert!(spoken, "automatic reply was not spoken");
    fake_realtime.await.expect("fake Realtime task");
}

#[allow(dead_code)]
async fn start_mock_browser_with_paseo(paseo_bin: &std::path::Path) -> LiveRealtimeRuntime {
    let app_port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve mock app port")
        .local_addr()
        .expect("mock app address")
        .port();
    let directory = tempfile::tempdir().expect("mock temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        json!({
            "listenHost":"127.0.0.1",
            "listenPort":app_port,
            "forceMock":true,
            "paseoBin":paseo_bin,
            "secretProvider":"environment",
            "paseoHosts":[
                {"id":"local","label":"Local","target":null,"default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"},
                {"id":"second","label":"Second","target":"second.example:6767","default":false,"defaultCwd":"~/","defaultProvider":"opencode/model"}
            ],
            "publicDir":workspace.join("public"),
            "journalPath":directory.path().join("state/journal.sqlite3")
        })
        .to_string(),
    )
    .expect("write mock config");
    let child = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
        .arg("serve")
        .current_dir(&workspace)
        .env_clear()
        .envs(essential_os_env())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("PASEO_PASSWORD", "test-password")
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start mock runtime");
    let guard = ChildGuard(child);
    let health_url = format!("http://127.0.0.1:{app_port}/healthz");
    let mut healthy = false;
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            healthy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(healthy, "mock runtime became healthy");
    let (mut browser, _) = connect_async(format!("ws://127.0.0.1:{app_port}/ws"))
        .await
        .expect("mock browser websocket");
    initialize_browser_protocol(&mut browser, "initial mock frame").await;
    LiveRealtimeRuntime {
        _directory: directory,
        _guard: guard,
        browser,
    }
}

#[cfg(unix)]
async fn start_bound_mock_dictation_browser() -> (tempfile::TempDir, LiveRealtimeRuntime, String) {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-mock-dictation");
    std::fs::write(
        &fake_paseo_path,
        concat!(
            "#!/bin/sh\n",
            "case \"$1\" in\n",
            "  ls) printf '%s\\n' '[{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"}]' ;;\n",
            "  logs) printf '%s\\n' 'Reply A' ;;\n",
            "  *) printf '%s\\n' '[]' ;;\n",
            "esac\n",
        ),
    )
    .expect("write fake mock dictation Paseo");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake mock dictation Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions)
        .expect("fake mock dictation Paseo permissions");
    let mut runtime = start_mock_browser_with_paseo(&fake_paseo_path).await;
    let frames = run_bound_mock_text_turn(&mut runtime.browser, "read thread-a", None).await;
    let summary_id = frames
        .iter()
        .find_map(|frame| {
            frame
                .pointer("/bound_context/summary_id")
                .and_then(Value::as_str)
        })
        .expect("mock dictation summary ID")
        .to_owned();
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select mock dictation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "mock dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );
    (paseo_directory, runtime, summary_id)
}

#[allow(dead_code)]
async fn run_bound_mock_text_turn(
    browser: &mut BrowserSocket,
    text: &str,
    summary_id: Option<&str>,
) -> Vec<Value> {
    browser
        .send(Message::Text(
            test_text_turn!(text, summary_id).to_string().into(),
        ))
        .await
        .expect("send bound mock text turn");
    let mut frames = Vec::new();
    loop {
        let frame = next_browser_json(browser, "bound mock text turn frame").await;
        let ready = frame == json!({"type":"state","state":"ready"});
        frames.push(frame);
        if ready {
            return frames;
        }
    }
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn mock_browser_requires_a_later_task_turn_before_session_proposal() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-session-task-gate");
    let process_marker = paseo_directory.path().join("process-calls");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "printf '%s\\n' \"$*\" >> \"{}\"\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[]' ;;\n",
                "  provider) printf '%s\\n' '[{{\"id\":\"model\"}}]' ;;\n",
                "  run) printf '%s\\n' '{{\"agentId\":\"new-agent\"}}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            process_marker.display(),
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");
    let mut runtime = start_mock_browser_with_paseo(&fake_paseo_path).await;
    if process_marker.exists() {
        std::fs::remove_file(&process_marker).expect("clear startup process calls");
    }

    let intent_frames =
        run_bound_mock_text_turn(&mut runtime.browser, "new discard this model task", None).await;

    assert!(intent_frames.iter().any(|frame| {
        frame
            == &json!({
                "type":"transcript_done",
                "text":"What should the new session work on?"
            })
    }));
    assert!(
        intent_frames
            .iter()
            .all(|frame| { frame["type"] != "proposal" || !frame["handle"].is_string() })
    );
    assert!(
        !process_marker.exists(),
        "intent turn invoked the Paseo process boundary"
    );

    let cancellation_frames = run_bound_mock_text_turn(&mut runtime.browser, "cancel", None).await;
    assert!(
        cancellation_frames
            .iter()
            .any(|frame| { frame == &json!({"type":"transcript_done","text":"Done."}) })
    );
    assert!(
        cancellation_frames
            .iter()
            .all(|frame| { frame["type"] != "proposal" || !frame["handle"].is_string() })
    );
    assert!(
        !process_marker.exists(),
        "task collection cancellation invoked the Paseo process boundary"
    );

    let fresh_intent_frames =
        run_bound_mock_text_turn(&mut runtime.browser, "new discard another model task", None)
            .await;
    assert!(fresh_intent_frames.iter().any(|frame| {
        frame
            == &json!({
                "type":"transcript_done",
                "text":"What should the new session work on?"
            })
    }));
    assert!(
        fresh_intent_frames
            .iter()
            .all(|frame| { frame["type"] != "proposal" || !frame["handle"].is_string() })
    );
    assert!(
        !process_marker.exists(),
        "fresh intent invoked the Paseo process boundary"
    );

    let task_frames =
        run_bound_mock_text_turn(&mut runtime.browser, "fix only the later task", None).await;
    let proposal = task_frames
        .iter()
        .find(|frame| frame["type"] == "proposal" && frame["handle"].is_string())
        .expect("task turn proposal");
    assert_eq!(
        proposal["echo"],
        "Create a new session on Local in ~/ with opencode/model: \"fix only the later task\". Confirm?"
    );
    let calls = std::fs::read_to_string(&process_marker).expect("task turn process calls");
    assert_eq!(
        calls
            .lines()
            .filter(|line| line.starts_with("provider models "))
            .count(),
        1
    );
    assert!(!calls.lines().any(|line| line.starts_with("run ")));
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn realtime_requires_a_later_task_turn_before_session_proposal() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory
        .path()
        .join("fake-paseo-realtime-session-task-gate");
    let process_marker = paseo_directory.path().join("process-calls");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "printf '%s\\n' \"$*\" >> \"{}\"\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[]' ;;\n",
                "  provider) printf '%s\\n' '[{{\"id\":\"model\"}}]' ;;\n",
                "  run) printf '%s\\n' '{{\"agentId\":\"new-agent\"}}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            process_marker.display(),
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (intent_sender, intent_receiver) = tokio::sync::oneshot::channel();
    let (task_sender, task_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_hdr_async(stream, AssertNoRealtimeAuthorization)
            .await
            .expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "session task gate update").await["type"],
            "session.update"
        );

        for (response_id, item_id, call_id, prompt, output_sender, followup_id) in [
            (
                "session-intent-response",
                "session-intent-item",
                "session-intent-call",
                "model supplied without a task turn",
                intent_sender,
                "session-intent-followup",
            ),
            (
                "session-task-response",
                "session-task-item",
                "session-task-call",
                "fix only the trusted task turn",
                task_sender,
                "session-task-followup",
            ),
        ] {
            loop {
                if next_fake_realtime_json(&mut socket, "session task response request").await["type"]
                    == "response.create"
                {
                    break;
                }
            }
            socket
                .send(Message::Text(
                    json!({"type":"response.created","response":{"id":response_id}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("create session task response");
            for event in function_call_events(
                response_id,
                item_id,
                call_id,
                "create_session",
                &json!({"prompt":prompt}).to_string(),
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send session task tool call");
            }
            let output = next_function_output(&mut socket, call_id, "session task output").await;
            output_sender
                .send(output)
                .expect("report session task output");
            complete_tool_response(&mut socket, response_id, followup_id).await;
        }
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    if process_marker.exists() {
        std::fs::remove_file(&process_marker).expect("clear startup process calls");
    }
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("create a new session", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("send session creation intent");
    let intent_output = intent_receiver.await.expect("session intent output");
    assert_eq!(
        intent_output,
        json!({
            "ok":false,
            "error":"session_task_required",
            "spoken_hint":"What should the new session work on?"
        })
    );
    assert!(
        !process_marker.exists(),
        "session intent invoked the Paseo process boundary"
    );
    loop {
        let frame = next_browser_json(&mut runtime.browser, "session intent browser frame").await;
        assert!(
            frame["type"] != "proposal" || !frame["handle"].is_string(),
            "session intent created a proposal"
        );
        if frame == json!({"type":"state","state":"ready"}) {
            break;
        }
    }

    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("fix only the trusted task turn", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("send session task turn");
    let task_output = task_receiver.await.expect("session task output");
    assert_eq!(task_output["ok"], true);
    assert_eq!(task_output["proposed"], true);
    assert!(task_output.get("proposal_token").is_none());
    assert!(
        task_output["spoken_echo"]
            .as_str()
            .expect("session task readback")
            .contains("fix only the trusted task turn")
    );
    let proposal = loop {
        let frame = next_browser_json(&mut runtime.browser, "session task proposal").await;
        if frame["type"] == "proposal" && frame["handle"].is_string() {
            break frame;
        }
    };
    assert!(
        proposal["echo"]
            .as_str()
            .expect("browser session task readback")
            .contains("fix only the trusted task turn")
    );
    let calls = std::fs::read_to_string(&process_marker).expect("session task process calls");
    assert_eq!(
        calls
            .lines()
            .filter(|line| line.starts_with("provider models "))
            .count(),
        1
    );
    assert!(!calls.lines().any(|line| line.starts_with("run ")));
    fake_realtime.await.expect("fake Realtime task");
}

async fn next_fake_realtime_json(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    context: &str,
) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap_or_else(|_| panic!("{context} timeout"))
            .unwrap_or_else(|| panic!("{context} frame"))
            .unwrap_or_else(|error| panic!("{context}: {error}"));
        if let Message::Text(message) = message {
            return serde_json::from_str(&message).unwrap_or_else(|_| panic!("{context} JSON"));
        }
    }
}

async fn acknowledge_fake_audio_commit(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    item_id: &str,
) {
    socket
        .send(Message::Text(
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":item_id,
                "previous_item_id":null
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("acknowledge fake audio commit");
}

#[allow(dead_code)]
async fn acknowledge_fake_dictation_delete(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    item_id: &str,
    acknowledgement_event_id: &str,
) {
    let delete = loop {
        let event = next_fake_realtime_json(socket, "dictation item delete").await;
        assert_ne!(event["type"], "response.create");
        if event["type"] == "conversation.item.delete" {
            break event;
        }
    };
    assert_eq!(delete["item_id"], item_id);
    socket
        .send(Message::Text(
            json!({
                "type":"conversation.item.deleted",
                "item_id":item_id,
                "event_id":acknowledgement_event_id
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("acknowledge dictation item delete");
}

fn function_call_events(
    response_id: &str,
    item_id: &str,
    call_id: &str,
    name: &str,
    arguments: &str,
) -> [Value; 2] {
    function_call_events_at(response_id, item_id, call_id, 0, name, arguments)
}

fn function_call_events_at(
    response_id: &str,
    item_id: &str,
    call_id: &str,
    output_index: u64,
    name: &str,
    arguments: &str,
) -> [Value; 2] {
    [
        json!({
            "type":"response.output_item.done",
            "response_id":response_id,
            "output_index":output_index,
            "item":{
                "id":item_id,
                "type":"function_call",
                "call_id":call_id,
                "name":name
            }
        }),
        json!({
            "type":"response.function_call_arguments.done",
            "response_id":response_id,
            "item_id":item_id,
            "call_id":call_id,
            "name":name,
            "arguments":arguments
        }),
    ]
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn model_tool_process_does_not_block_transport_or_escape_after_barge_in() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("latched Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("latched Realtime address")
        .port();
    let (provider_event_sender, provider_event_receiver) = tokio::sync::oneshot::channel();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let (clean_sender, clean_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "latched tool session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "latched tool response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"latched-tool-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create latched tool response");
        for event in function_call_events(
            "latched-tool-response",
            "latched-tool-item",
            "latched-tool-call",
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send latched tool call");
        }
        provider_event_receiver
            .await
            .expect("release provider event after process start");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.output_audio_transcript.delta",
                    "response_id":"latched-tool-response",
                    "delta":"Transport stayed responsive."
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send transcript during latched tool call");

        let mut saw_cancel = false;
        let mut saw_clear = false;
        while !saw_cancel || !saw_clear {
            let event = next_fake_realtime_json(&mut socket, "latched tool cancellation").await;
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output")
            );
            assert_ne!(event["type"], "response.create");
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "latched-tool-response");
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"latched-tool-response","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete cancelled latched response");
        cancelled_sender
            .send(())
            .expect("signal latched response cancellation");

        let mut saw_new_turn = false;
        loop {
            let event = next_fake_realtime_json(&mut socket, "post-latch provider event").await;
            assert!(!matches!(
                event["type"].as_str(),
                Some("input_audio_buffer.append" | "input_audio_buffer.commit")
            ));
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("latched-tool-call"),
                "retired tool emitted a late function output"
            );
            if event["type"] == "response.create" {
                assert!(saw_new_turn, "retired tool requested a followup response");
                break;
            }
            saw_new_turn |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("new turn after retired tool");
        }
        for event in [
            json!({"type":"response.created","response":{"id":"post-latch-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"post-latch-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete post-latch response");
        }
        clean_sender
            .send(())
            .expect("signal clean retired tool state");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "latched tool browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("check pending permissions", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request latched permissions tool");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("latched tool start timeout")
        .expect("latched tool start signal");
    provider_event_sender
        .send(())
        .expect("release provider event after process start");

    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "transcript_delta",
            "provider event during latched tool",
        )
        .await,
        json!({"type":"transcript_delta","text":"Transport stayed responsive."})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge into latched tool response");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "flush_audio",
            "latched tool barge-in flush",
        )
        .await,
        json!({"type":"flush_audio"})
    );
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "recording_rejected",
            "latched tool recording rejection",
        )
        .await,
        json!({
            "type":"recording_rejected",
            "mode":"live_response",
            "recording_id":1,
            "message":"Recording could not start. Try again."
        })
    );
    tokio::time::timeout(Duration::from_secs(2), cancelled_receiver)
        .await
        .expect("latched response cancellation timeout")
        .expect("latched response cancellation signal");
    runtime
        .browser
        .send(Message::Binary(test_pcm(&[1, 2]).into()))
        .await
        .expect("send rejected recording audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("end rejected latched recording");

    release_guard.release();
    let _ = next_browser_json_of_type(
        &mut runtime.browser,
        "dashboard_state",
        "retired tool completion dashboard",
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("new turn after retired tool", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("send turn after retired tool");
    tokio::time::timeout(Duration::from_secs(2), clean_receiver)
        .await
        .expect("retired tool isolation timeout")
        .expect("retired tool isolation signal");
    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(executor.permission_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn dictation_start_while_tools_are_away_is_rejected_without_terminal_ownership() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("latched dictation Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("latched dictation Realtime address")
        .port();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "latched dictation session update").await;
        bind_fake_realtime_context(&mut socket, "latched-dictation").await;
        loop {
            if next_fake_realtime_json(&mut socket, "latched dictation response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"latched-dictation-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create latched dictation response");
        for event in function_call_events_at(
            "latched-dictation-response",
            "latched-dictation-item",
            "latched-dictation-call",
            0,
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send latched dictation call");
        }
        let mut saw_cancel = false;
        let mut saw_clear = false;
        while !saw_cancel || !saw_clear {
            let event =
                next_fake_realtime_json(&mut socket, "latched dictation cancellation").await;
            assert!(!matches!(
                event["type"].as_str(),
                Some("input_audio_buffer.append" | "input_audio_buffer.commit")
            ));
            saw_cancel |= event["type"] == "response.cancel";
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"latched-dictation-response","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete latched dictation response");
        cancelled_sender
            .send(())
            .expect("signal latched dictation cancellation");
        let deadline = tokio::time::Instant::now() + Duration::from_millis(350);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("latched dictation JSON");
            assert!(!matches!(
                event["type"].as_str(),
                Some("input_audio_buffer.append" | "input_audio_buffer.commit" | "response.create")
            ));
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("latched-dictation-call")
            );
        }
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "latched dictation browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind latched dictation context");
    let mut summary_id = None;
    let mut ready = false;
    while summary_id.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "latched dictation context").await;
        if let Some(id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_id = Some(id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_id = summary_id.expect("latched dictation summary ID");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select latched dictation mode");
    assert_eq!(
        next_browser_json_of_type(&mut runtime.browser, "voice_mode", "latched dictation mode",)
            .await,
        json!({"type":"voice_mode","mode":"dictation"})
    );
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("hold tools for dictation", summary_id)
                .to_string()
                .into(),
        ))
        .await
        .expect("request latched dictation response");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("latched dictation process start timeout")
        .expect("latched dictation process start signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start rejected latched dictation");
    let rejection = loop {
        let frame = next_browser_json(&mut runtime.browser, "latched dictation rejection").await;
        assert_ne!(frame["type"], "dictation_operation");
        if frame["type"] == "recording_rejected" {
            break frame;
        }
    };
    assert_eq!(
        rejection,
        json!({
            "type":"recording_rejected",
            "mode":"dictation",
            "recording_id":1,
            "message":"Recording could not start. Try again."
        })
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(&[1, 2]).into()))
        .await
        .expect("send rejected dictation audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":"dictation-1"})
                .to_string()
                .into(),
        ))
        .await
        .expect("end rejected dictation");
    tokio::time::timeout(Duration::from_secs(2), cancelled_receiver)
        .await
        .expect("latched dictation cancellation timeout")
        .expect("latched dictation cancellation signal");
    release_guard.release();
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn panicking_model_tool_job_closes_fail_closed_without_retry() {
    let executor = Arc::new(PanickingPermissionsExecutor {
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("panicking tool Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("panicking tool Realtime address")
        .port();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "panicking tool session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "panicking tool response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"panicking-tool-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create panicking tool response");
        for event in function_call_events_at(
            "panicking-tool-response",
            "panicking-tool-item",
            "panicking-tool-call",
            0,
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send panicking tool call");
        }
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Text(message)) => {
                    let event: Value =
                        serde_json::from_str(&message).expect("panicking tool provider JSON");
                    assert_ne!(
                        event.pointer("/item/type").and_then(Value::as_str),
                        Some("function_call_output")
                    );
                    assert_ne!(event["type"], "response.create");
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "panicking tool browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("panic the tool", null).to_string().into(),
        ))
        .await
        .expect("request panicking tool");
    assert_eq!(
        next_browser_json_of_type(&mut runtime.browser, "error", "panicking tool failure",).await,
        json!({
            "type":"error",
            "message":"The tool worker failed. Reconnecting safely."
        })
    );
    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(executor.permission_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn disconnect_while_model_tool_is_latched_closes_provider_without_retry() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("disconnect latch Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("disconnect latch Realtime address")
        .port();
    let (closed_sender, closed_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "disconnect latch session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "disconnect latch response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"disconnect-latch-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create disconnect latch response");
        for event in function_call_events_at(
            "disconnect-latch-response",
            "disconnect-latch-item",
            "disconnect-latch-call",
            0,
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send disconnect latch call");
        }
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Text(message)) => {
                    let event: Value =
                        serde_json::from_str(&message).expect("disconnect latch provider JSON");
                    assert_ne!(
                        event.pointer("/item/type").and_then(Value::as_str),
                        Some("function_call_output")
                    );
                    assert_ne!(event["type"], "response.create");
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        closed_sender
            .send(())
            .expect("signal provider close during latched tool");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "disconnect latch browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("disconnect while blocked", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request disconnect latch tool");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("disconnect latch process start timeout")
        .expect("disconnect latch process start signal");
    runtime
        .browser
        .close(None)
        .await
        .expect("close browser during latched tool");
    tokio::time::timeout(Duration::from_secs(2), closed_receiver)
        .await
        .expect("provider close while tool latched timeout")
        .expect("provider close while tool latched signal");
    release_guard.release();
    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(executor.permission_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn post_model_dashboard_probe_does_not_block_provider_transport() {
    let (dashboard_latch, dashboard_started, release_guard) = process_latch();
    let executor = Arc::new(LatchingDashboardExecutor {
        latch: dashboard_latch,
        dashboard_armed: AtomicBool::new(false),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("dashboard Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("dashboard Realtime address")
        .port();
    let (provider_event_sender, provider_event_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "dashboard session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "dashboard response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"dashboard-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create dashboard response");
        for event in function_call_events(
            "dashboard-response",
            "dashboard-item",
            "dashboard-call",
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send dashboard tool call");
        }
        provider_event_receiver
            .await
            .expect("release provider event during dashboard probe");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.output_audio_transcript.delta",
                    "response_id":"dashboard-response",
                    "delta":"Dashboard probe stayed responsive."
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send transcript during dashboard probe");
        let output =
            next_function_output(&mut socket, "dashboard-call", "dashboard tool output").await;
        assert_eq!(output["ok"], true);
        complete_tool_response(&mut socket, "dashboard-response", "dashboard-followup").await;
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "dashboard probe browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("check dashboard isolation", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request dashboard tool");
    tokio::time::timeout(Duration::from_secs(2), dashboard_started)
        .await
        .expect("dashboard process start timeout")
        .expect("dashboard process start signal");
    provider_event_sender
        .send(())
        .expect("release provider event during dashboard probe");
    let provider_frame = tokio::time::timeout(
        Duration::from_secs(2),
        next_browser_json_of_type(
            &mut runtime.browser,
            "transcript_delta",
            "provider event during dashboard probe",
        ),
    )
    .await
    .expect("provider event blocked by dashboard process");
    assert_eq!(
        provider_frame,
        json!({
            "type":"transcript_delta",
            "text":"Dashboard probe stayed responsive."
        })
    );
    release_guard.release();
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn model_calls_dispatch_in_output_order_when_arguments_complete_out_of_order() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ordered calls Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("ordered calls Realtime address")
        .port();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "ordered calls session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "ordered calls response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"ordered-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create ordered response");
        let [first_item, first_arguments] = function_call_events_at(
            "ordered-response",
            "ordered-item-0",
            "ordered-call-0",
            0,
            "cancel_action",
            "{}",
        );
        let [second_item, second_arguments] = function_call_events_at(
            "ordered-response",
            "ordered-item-1",
            "ordered-call-1",
            1,
            "cancel_action",
            "{}",
        );
        for event in [first_item, second_item, second_arguments] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send interleaved ordered call event");
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(250), socket.next())
                .await
                .is_err(),
            "later arguments dispatched before the earlier output item"
        );
        socket
            .send(Message::Text(first_arguments.to_string().into()))
            .await
            .expect("complete first ordered arguments");
        for expected_call_id in ["ordered-call-0", "ordered-call-1"] {
            loop {
                let event = next_fake_realtime_json(&mut socket, "ordered function output").await;
                if event.pointer("/item/type").and_then(Value::as_str)
                    == Some("function_call_output")
                {
                    assert_eq!(
                        event.pointer("/item/call_id").and_then(Value::as_str),
                        Some(expected_call_id)
                    );
                    break;
                }
            }
        }
        complete_tool_response(&mut socket, "ordered-response", "ordered-followup-response").await;
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("run ordered calls", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request ordered calls");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn ambiguous_duplicate_and_out_of_order_function_indices_fail_closed() {
    for (case, first_index, second_index) in [("duplicate", 0_u64, 0_u64), ("descending", 1, 0)] {
        let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ambiguous ordering Realtime listener");
        let realtime_port = realtime_listener
            .local_addr()
            .expect("ambiguous ordering Realtime address")
            .port();
        let case_name = case.to_owned();
        let fake_realtime = tokio::spawn(async move {
            let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
            let mut socket = accept_async(stream).await.expect("Realtime handshake");
            let _ = next_fake_realtime_json(&mut socket, "ambiguous ordering session update").await;
            loop {
                if next_fake_realtime_json(&mut socket, "ambiguous ordering response request").await
                    ["type"]
                    == "response.create"
                {
                    break;
                }
            }
            let response_id = format!("ambiguous-{case_name}-response");
            socket
                .send(Message::Text(
                    json!({"type":"response.created","response":{"id":response_id}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("create ambiguous ordering response");
            for (suffix, output_index) in [("first", first_index), ("second", second_index)] {
                let event = function_call_events_at(
                    &response_id,
                    &format!("ambiguous-{case_name}-{suffix}-item"),
                    &format!("ambiguous-{case_name}-{suffix}-call"),
                    output_index,
                    "cancel_action",
                    "{}",
                )[0]
                .clone();
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send ambiguous ordering item");
            }
            let _ = socket.next().await;
        });

        let mut runtime = start_live_realtime_browser(realtime_port).await;
        runtime
            .browser
            .send(Message::Text(
                test_text_turn!("ambiguous function ordering", null)
                    .to_string()
                    .into(),
            ))
            .await
            .expect("request ambiguous ordering response");
        assert_eq!(
            next_browser_json_of_type(
                &mut runtime.browser,
                "error",
                "ambiguous function ordering rejection",
            )
            .await,
            json!({
                "type":"error",
                "message":"Realtime reported ambiguous function-call ordering. Reconnecting safely."
            })
        );
        fake_realtime.await.expect("fake Realtime task");
    }
}

#[tokio::test]
async fn response_done_fails_closed_on_incomplete_function_ordering_gap() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("terminal ordering gap Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("terminal ordering gap Realtime address")
        .port();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "terminal ordering gap session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "terminal ordering gap response request").await
                ["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"terminal-gap-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create terminal ordering gap response");
        let [first_item, _] = function_call_events_at(
            "terminal-gap-response",
            "terminal-gap-item-0",
            "terminal-gap-call-0",
            0,
            "cancel_action",
            "{}",
        );
        let [second_item, second_arguments] = function_call_events_at(
            "terminal-gap-response",
            "terminal-gap-item-1",
            "terminal-gap-call-1",
            1,
            "cancel_action",
            "{}",
        );
        for event in [
            first_item,
            second_item,
            second_arguments,
            json!({
                "type":"response.done",
                "response":{"id":"terminal-gap-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send terminal ordering gap event");
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("terminal ordering gap provider close timeout");
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("finish with an ordering gap", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request terminal ordering gap response");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "error",
            "terminal ordering gap rejection",
        )
        .await,
        json!({
            "type":"error",
            "message":"Realtime reported ambiguous function-call ordering. Reconnecting safely."
        })
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn response_done_freezes_late_media_and_tool_calls_while_accepted_work_drains() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("frozen response Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("frozen response Realtime address")
        .port();
    let (terminal_sender, terminal_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "frozen response session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "frozen response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"frozen-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create frozen response");
        for event in function_call_events_at(
            "frozen-response",
            "accepted-item",
            "accepted-call",
            0,
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send accepted frozen call");
        }
        terminal_receiver
            .await
            .expect("release frozen terminal events");
        for event in [
            json!({
                "type":"response.done",
                "response":{"id":"frozen-response","status":"completed"}
            }),
            json!({
                "type":"response.output_audio.delta",
                "response_id":"frozen-response",
                "delta":"AQI="
            }),
            json!({
                "type":"response.output_audio_transcript.delta",
                "response_id":"frozen-response",
                "delta":"late frozen transcript"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send frozen response terminal event");
        }
        for event in function_call_events_at(
            "frozen-response",
            "late-item",
            "late-call",
            1,
            "send_message",
            r#"{"text":"must not escape"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send late frozen call");
        }

        let mut accepted_output = false;
        loop {
            let event = next_fake_realtime_json(&mut socket, "frozen response drain").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("late-call"),
                "late post-terminal call escaped"
            );
            accepted_output |=
                event.pointer("/item/call_id").and_then(Value::as_str) == Some("accepted-call");
            if event["type"] == "response.create" {
                assert!(accepted_output, "terminal finalized before accepted output");
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"frozen-followup"}}),
            json!({
                "type":"response.done",
                "response":{"id":"frozen-followup","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete frozen followup");
        }
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "frozen response browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("freeze this response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request frozen response");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("frozen process start timeout")
        .expect("frozen process start signal");
    terminal_sender
        .send(())
        .expect("release frozen terminal events");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(350);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(message))) = tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        match message {
            Message::Binary(_) => panic!("post-terminal audio escaped"),
            Message::Text(message) => {
                let frame: Value = serde_json::from_str(&message).expect("frozen browser frame");
                assert_ne!(frame["type"], "transcript_delta");
                assert_ne!(frame["type"], "proposal");
            }
            _ => {}
        }
    }
    release_guard.release();
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failed_response_done_suppresses_running_tool_publication() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed terminal Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("failed terminal Realtime address")
        .port();
    let (terminal_sender, terminal_receiver) = tokio::sync::oneshot::channel();
    let (terminal_observed_sender, terminal_observed_receiver) = tokio::sync::oneshot::channel();
    let (restored_sender, restored_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "failed terminal session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "failed terminal response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"failed-terminal-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create failed terminal response");
        for event in function_call_events_at(
            "failed-terminal-response",
            "failed-terminal-item",
            "failed-terminal-call",
            0,
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send failed terminal tool call");
        }
        terminal_receiver
            .await
            .expect("release failed terminal response");
        for event in [
            json!({
                "type":"response.done",
                "response":{"id":"failed-terminal-response","status":"failed"}
            }),
            json!({"type":"session.updated"}),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send failed terminal event");
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"failed-terminal-observed"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send failed terminal observation barrier");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "failed terminal observation barrier").await;
            if event["type"] == "response.cancel"
                && event["response_id"] == "failed-terminal-observed"
            {
                break;
            }
        }
        terminal_observed_sender
            .send(())
            .expect("signal failed terminal observation");
        restored_receiver
            .await
            .expect("wait for failed terminal engine restoration");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"failed-terminal-barrier"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send failed terminal barrier");
        loop {
            let event = next_fake_realtime_json(&mut socket, "failed terminal barrier").await;
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output"),
                "failed response published its running tool result"
            );
            assert_ne!(event["type"], "response.create");
            if event["type"] == "response.cancel"
                && event["response_id"] == "failed-terminal-barrier"
            {
                break;
            }
        }
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "failed terminal browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("fail while the tool runs", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request failed terminal response");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("failed terminal process start timeout")
        .expect("failed terminal process start signal");
    terminal_sender
        .send(())
        .expect("release failed terminal response");
    terminal_observed_receiver
        .await
        .expect("failed terminal observation");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("probe failed terminal pre-restoration state");
    loop {
        let frame = next_browser_json(
            &mut runtime.browser,
            "failed terminal pre-restoration state",
        )
        .await;
        assert_ne!(
            frame,
            json!({"type":"state","state":"ready"}),
            "session update emitted ready before terminal finalization"
        );
        if frame["type"] == "dictation_capabilities" {
            break;
        }
    }
    release_guard.release();
    let ready = next_browser_json_of_type(
        &mut runtime.browser,
        "state",
        "failed terminal engine restoration",
    )
    .await;
    assert_eq!(ready, json!({"type":"state","state":"ready"}));
    restored_sender
        .send(())
        .expect("signal failed terminal engine restoration");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failed_response_done_invalidates_running_tool_routing_mutation() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingDashboardExecutor {
        latch,
        dashboard_armed: AtomicBool::new(false),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed routing Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("failed routing Realtime address")
        .port();
    let (terminal_sender, terminal_receiver) = tokio::sync::oneshot::channel();
    let (terminal_observed_sender, terminal_observed_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "failed routing session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "failed routing response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"failed-routing-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create failed routing response");
        for event in function_call_events_at(
            "failed-routing-response",
            "failed-routing-item",
            "failed-routing-call",
            0,
            "read_latest_reply",
            r#"{"session":"thread-a"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send failed routing tool call");
        }
        terminal_receiver
            .await
            .expect("release failed routing terminal");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"failed-routing-response","status":"failed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send failed routing terminal");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"failed-routing-barrier"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send failed routing terminal barrier");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "failed routing terminal barrier").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("failed-routing-call"),
                "failed routing tool published provider output"
            );
            if event["type"] == "response.cancel"
                && event["response_id"] == "failed-routing-barrier"
            {
                break;
            }
        }
        terminal_observed_sender
            .send(())
            .expect("signal failed routing terminal observation");
        let mut saw_fresh_text = false;
        let mut saw_fresh_response = false;
        while !saw_fresh_text || !saw_fresh_response {
            let event = next_fake_realtime_json(&mut socket, "fresh null-context turn").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("failed-routing-call"),
                "failed routing tool published late provider output"
            );
            saw_fresh_text |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("fresh after failed routing");
            if event["type"] == "response.create" {
                assert!(saw_fresh_text, "failed routing response created a followup");
                saw_fresh_response = true;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"fresh-failed-routing-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"fresh-failed-routing-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete fresh null-context response");
        }
        finish_receiver.await.expect("finish failed routing test");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "failed routing browser").await;
    executor.dashboard_armed.store(true, Ordering::SeqCst);
    let failed_turn = test_text_turn!("mutate routing before failure", null);
    let failed_turn_id = test_text_turn_id(&failed_turn);
    runtime
        .browser
        .send(Message::Text(failed_turn.to_string().into()))
        .await
        .expect("request failed routing response");
    assert_eq!(
        next_browser_json_including_text_ack(&mut runtime.browser, "failed routing acceptance")
            .await,
        json!({"type":"text_turn_accepted","turn_id":failed_turn_id})
    );
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("failed routing process start timeout")
        .expect("failed routing process start signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send failed routing pre-terminal barrier");
    let mut protocol_ready = false;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "failed routing pre-terminal barrier").await;
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
        protocol_ready |= frame["type"] == "protocol_ready";
        if frame["type"] == "host_state" {
            assert_eq!(frame["selected_host_id"], "local");
        }
        if frame["type"] == "dashboard_state" {
            assert_eq!(frame["selected_host_id"], "local");
        }
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }
    terminal_sender
        .send(())
        .expect("release failed routing terminal");
    terminal_observed_receiver
        .await
        .expect("failed routing terminal observation");
    release_guard.release();
    let mut dashboard_reconciled = false;
    let mut proposal_cleared = false;
    loop {
        let frame = next_browser_json(&mut runtime.browser, "failed routing restoration").await;
        assert_ne!(
            frame["type"], "tool",
            "failed tool content reached the browser"
        );
        if frame["type"] == "dashboard_state" {
            assert_eq!(frame["bound_context"], Value::Null);
            dashboard_reconciled = true;
            continue;
        }
        if frame["type"] == "proposal" {
            assert_eq!(frame, json!({"type":"proposal","echo":null}));
            proposal_cleared = true;
            continue;
        }
        if frame == json!({"type":"state","state":"ready"}) {
            assert!(
                dashboard_reconciled,
                "ready preceded dashboard reconciliation"
            );
            assert!(proposal_cleared, "ready preceded proposal reconciliation");
            break;
        }
    }
    let fresh_turn = test_text_turn!("fresh after failed routing", null);
    let fresh_turn_id = test_text_turn_id(&fresh_turn);
    runtime
        .browser
        .send(Message::Text(fresh_turn.to_string().into()))
        .await
        .expect("send fresh null-context turn");
    loop {
        let frame =
            next_browser_json_including_text_ack(&mut runtime.browser, "fresh null-context turn")
                .await;
        if frame["type"] == "text_turn_accepted" {
            assert_eq!(frame["turn_id"], fresh_turn_id);
            break;
        }
        assert_ne!(frame["type"], "text_turn_rejected");
    }
    loop {
        if next_browser_json(&mut runtime.browser, "fresh null-context response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    finish_sender.send(()).expect("finish failed routing test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failed_terminal_rolls_back_fast_read_dedup_and_context() {
    let (latch, _started, release_guard) = process_latch();
    release_guard.release();
    let executor = Arc::new(LatchingDashboardExecutor {
        latch,
        dashboard_armed: AtomicBool::new(false),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fast failed read Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("fast failed read Realtime address")
        .port();
    let (first_output_sender, first_output_receiver) = tokio::sync::oneshot::channel();
    let (release_terminal_sender, release_terminal_receiver) = tokio::sync::oneshot::channel();
    let (terminal_observed_sender, terminal_observed_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "fast failed read session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "fast failed read response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"fast-failed-read-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create fast failed read response");
        for event in function_call_events_at(
            "fast-failed-read-response",
            "fast-failed-read-item",
            "fast-failed-read-call",
            0,
            "read_latest_reply",
            r#"{"session":"thread-a"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send fast failed read call");
        }
        let first = next_function_output(
            &mut socket,
            "fast-failed-read-call",
            "fast failed read output",
        )
        .await;
        assert_eq!(first["respondable"], true);
        first_output_sender
            .send(())
            .expect("signal fast failed read output");
        release_terminal_receiver
            .await
            .expect("release fast failed read terminal");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"fast-failed-read-response","status":"failed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("fail fast read response");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"fast-failed-read-barrier"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send fast failed read barrier");
        loop {
            let event = next_fake_realtime_json(&mut socket, "fast failed read barrier").await;
            if event["type"] == "response.cancel"
                && event["response_id"] == "fast-failed-read-barrier"
            {
                break;
            }
        }
        terminal_observed_sender
            .send(())
            .expect("signal fast failed read terminal");
        let mut saw_fresh_text = false;
        let mut saw_fresh_response = false;
        while !saw_fresh_text || !saw_fresh_response {
            let event = next_fake_realtime_json(&mut socket, "fast failed read fresh turn").await;
            saw_fresh_text |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("reread after failed fast tool");
            if event["type"] == "response.create" {
                assert!(
                    saw_fresh_text,
                    "failed read created an unrequested followup"
                );
                saw_fresh_response = true;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"fast-reread-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create fast reread response");
        for event in function_call_events_at(
            "fast-reread-response",
            "fast-reread-item",
            "fast-reread-call",
            0,
            "read_latest_reply",
            r#"{"session":"thread-a"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send fast reread call");
        }
        let reread =
            next_function_output(&mut socket, "fast-reread-call", "fast reread output").await;
        assert_eq!(reread["respondable"], true);
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"fast-reread-response","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete fast reread response");
        loop {
            if next_fake_realtime_json(&mut socket, "fast reread followup").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"fast-reread-followup"}}),
            json!({
                "type":"response.done",
                "response":{"id":"fast-reread-followup","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete fast reread followup");
        }
        let mut saw_committed_text = false;
        let mut saw_committed_response = false;
        while !saw_committed_text || !saw_committed_response {
            let event = next_fake_realtime_json(&mut socket, "committed fast reread turn").await;
            saw_committed_text |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("read after completed checkpoint");
            if event["type"] == "response.create" {
                assert!(
                    saw_committed_text,
                    "completed read created an unrequested followup"
                );
                saw_committed_response = true;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"committed-dedup-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create committed dedup response");
        for event in function_call_events_at(
            "committed-dedup-response",
            "committed-dedup-item",
            "committed-dedup-call",
            0,
            "read_latest_reply",
            r#"{"session":"thread-a"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send committed dedup call");
        }
        let committed = next_function_output(
            &mut socket,
            "committed-dedup-call",
            "committed dedup output",
        )
        .await;
        assert_eq!(committed["respondable"], false);
        assert_eq!(committed["reason"], "reply_already_observed");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"committed-dedup-response","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete committed dedup response");
        loop {
            if next_fake_realtime_json(&mut socket, "committed dedup followup").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"committed-dedup-followup"}}),
            json!({
                "type":"response.done",
                "response":{"id":"committed-dedup-followup","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete committed dedup followup");
        }
        finish_receiver.await.expect("finish fast failed read test");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "fast failed read browser").await;
    let first_turn = test_text_turn!("read before a failed terminal", null);
    let first_turn_id = test_text_turn_id(&first_turn);
    runtime
        .browser
        .send(Message::Text(first_turn.to_string().into()))
        .await
        .expect("request fast failed read");
    assert_eq!(
        next_browser_json_including_text_ack(&mut runtime.browser, "fast failed read acceptance")
            .await,
        json!({"type":"text_turn_accepted","turn_id":first_turn_id})
    );
    first_output_receiver
        .await
        .expect("fast failed read output signal");
    let mut bound_context_published = false;
    let mut tool_published = false;
    while !bound_context_published || !tool_published {
        let frame = next_browser_json(&mut runtime.browser, "fast failed read publication").await;
        bound_context_published |=
            frame["type"] == "dashboard_state" && frame["bound_context"].is_object();
        tool_published |= frame["type"] == "tool"
            && frame["name"] == "read_latest_reply"
            && frame["phase"] == "ok";
    }
    release_terminal_sender
        .send(())
        .expect("release fast failed read terminal");
    terminal_observed_receiver
        .await
        .expect("fast failed read terminal observation");
    let mut dashboard_reconciled = false;
    let mut proposal_cleared = false;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "fast failed read reconciliation").await;
        if frame["type"] == "dashboard_state" {
            assert_eq!(frame["bound_context"], Value::Null);
            dashboard_reconciled = true;
            continue;
        }
        if frame["type"] == "proposal" {
            assert_eq!(frame, json!({"type":"proposal","echo":null}));
            proposal_cleared = true;
            continue;
        }
        if frame == json!({"type":"state","state":"ready"}) {
            assert!(
                dashboard_reconciled,
                "ready preceded fast read dashboard rollback"
            );
            assert!(
                proposal_cleared,
                "ready preceded fast read proposal rollback"
            );
            break;
        }
    }
    let reread_turn = test_text_turn!("reread after failed fast tool", null);
    let reread_turn_id = test_text_turn_id(&reread_turn);
    runtime
        .browser
        .send(Message::Text(reread_turn.to_string().into()))
        .await
        .expect("send null-context reread turn");
    loop {
        let frame = next_browser_json_including_text_ack(
            &mut runtime.browser,
            "null-context reread acceptance",
        )
        .await;
        if frame["type"] == "text_turn_accepted" {
            assert_eq!(frame["turn_id"], reread_turn_id);
            break;
        }
        assert_ne!(frame["type"], "text_turn_rejected");
    }
    let mut reread_summary_id = None;
    let mut final_ready = false;
    while reread_summary_id.is_none() || !final_ready {
        let frame = next_browser_json(&mut runtime.browser, "completed fast reread").await;
        if frame["type"] == "dashboard_state" {
            reread_summary_id = frame
                .pointer("/bound_context/summary_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        final_ready |= frame == json!({"type":"state","state":"ready"});
    }
    let committed_turn = test_text_turn!(
        "read after completed checkpoint",
        reread_summary_id
            .as_deref()
            .expect("completed reread summary")
    );
    let committed_turn_id = test_text_turn_id(&committed_turn);
    runtime
        .browser
        .send(Message::Text(committed_turn.to_string().into()))
        .await
        .expect("send committed dedup turn");
    loop {
        let frame = next_browser_json_including_text_ack(
            &mut runtime.browser,
            "committed dedup acceptance",
        )
        .await;
        if frame["type"] == "text_turn_accepted" {
            assert_eq!(frame["turn_id"], committed_turn_id);
            break;
        }
    }
    loop {
        if next_browser_json(&mut runtime.browser, "committed dedup response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    finish_sender
        .send(())
        .expect("finish fast failed read test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn failed_followup_restores_existing_proposal_presentation_and_confirmation() {
    let executor = Arc::new(CheckpointControlExecutor {
        send_calls: AtomicUsize::new(0),
        run_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("proposal restore Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("proposal restore Realtime address")
        .port();
    let (fast_sender, fast_receiver) = tokio::sync::oneshot::channel();
    let (fail_sender, fail_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "proposal restore session update").await["type"],
            "session.update"
        );
        bind_fake_realtime_context(&mut socket, "proposal-restore").await;
        begin_existing_proposal_followup(
            &mut socket,
            "presentation-restore",
            "restore this proposal",
        )
        .await;
        fast_sender.send(()).expect("signal fast proposal followup");
        fail_receiver
            .await
            .expect("release failed proposal followup");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{
                        "id":"proposal-followup-presentation-restore",
                        "status":"failed"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("fail proposal followup");
        finish_receiver
            .await
            .expect("finish proposal presentation restore test");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "proposal restore browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("create persistent proposal", summary_id.as_str())
                .to_string()
                .into(),
        ))
        .await
        .expect("request persistent proposal");
    let proposal = next_browser_json_of_type(
        &mut runtime.browser,
        "proposal",
        "persistent proposal presentation",
    )
    .await;
    let handle = proposal["handle"]
        .as_str()
        .expect("persistent proposal handle")
        .to_owned();
    assert!(
        proposal["echo"]
            .as_str()
            .is_some_and(|echo| { echo.contains("restore this proposal") })
    );
    fast_receiver
        .await
        .expect("fast proposal followup completed");
    fail_sender
        .send(())
        .expect("release failed proposal followup");
    let restored = next_browser_json_of_type(
        &mut runtime.browser,
        "proposal",
        "restored proposal presentation",
    )
    .await;
    assert_eq!(restored, proposal);
    loop {
        if next_browser_json(&mut runtime.browser, "proposal restore ready").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("confirm restored proposal");
    loop {
        let frame = next_browser_json(&mut runtime.browser, "restored proposal confirmation").await;
        if frame["type"] == "transcript_done" {
            assert_eq!(frame["text"], "Confirmed and delivered.");
            break;
        }
    }
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 1);
    finish_sender
        .send(())
        .expect("finish proposal presentation restore test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn browser_cancellation_remains_authoritative_after_checkpoint_rollback() {
    let (latch, mut process_started, release_guard) = process_latch();
    let executor = Arc::new(LatchingSameHostExecutor {
        latch,
        refresh_armed: AtomicBool::new(false),
        list_calls: AtomicUsize::new(0),
        send_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("checkpoint cancellation Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("checkpoint cancellation Realtime address")
        .port();
    let (fast_sender, fast_receiver) = tokio::sync::oneshot::channel();
    let (fail_sender, fail_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "checkpoint cancellation session update").await["type"],
            "session.update"
        );
        bind_fake_realtime_context(&mut socket, "checkpoint-cancellation").await;
        begin_existing_proposal_followup(
            &mut socket,
            "checkpoint-cancellation",
            "cancel this proposal",
        )
        .await;
        fast_sender
            .send(())
            .expect("signal checkpoint cancellation fast tool");
        fail_receiver
            .await
            .expect("release checkpoint cancellation terminal");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{
                        "id":"proposal-followup-checkpoint-cancellation",
                        "status":"failed"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("fail checkpoint cancellation response");
        let mut saw_fresh_text = false;
        let mut saw_fresh_response = false;
        while !saw_fresh_text || !saw_fresh_response {
            let event = next_fake_realtime_json(&mut socket, "fresh post-cancellation turn").await;
            saw_fresh_text |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("fresh after checkpoint cancellation");
            saw_fresh_response |= event["type"] == "response.create";
        }
        for event in [
            json!({"type":"response.created","response":{"id":"fresh-post-cancellation"}}),
            json!({
                "type":"response.done",
                "response":{"id":"fresh-post-cancellation","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete fresh post-cancellation response");
        }
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "checkpoint cancellation browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("create cancellable proposal", summary_id.as_str())
                .to_string()
                .into(),
        ))
        .await
        .expect("request cancellable proposal");
    let proposal = next_browser_json_of_type(
        &mut runtime.browser,
        "proposal",
        "cancellable proposal presentation",
    )
    .await;
    let handle = proposal["handle"]
        .as_str()
        .expect("cancellable proposal handle")
        .to_owned();
    fast_receiver
        .await
        .expect("checkpoint cancellation fast tool completed");
    let baseline_list_calls = executor.list_calls.load(Ordering::SeqCst);
    executor.refresh_armed.store(true, Ordering::SeqCst);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"local"})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat selected host with checkpointed proposal");
    let mut presentation_frames = std::collections::HashSet::new();
    while presentation_frames.len() < 2 {
        let frame = next_browser_json_without_process_start(
            &mut runtime.browser,
            &mut process_started,
            "same-host checkpoint selection",
        )
        .await;
        if let Some(frame_type @ ("host_state" | "dashboard_state")) = frame["type"].as_str() {
            assert!(
                presentation_frames.insert(frame_type.to_owned()),
                "duplicate same-host checkpoint frame: {frame}"
            );
        } else {
            assert_ne!(
                frame["type"], "proposal",
                "same-host checkpoint selection mutated the visible proposal"
            );
        }
    }
    assert_eq!(
        executor.list_calls.load(Ordering::SeqCst),
        baseline_list_calls,
        "same-host checkpoint selection invoked a process probe"
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"cancel_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel checkpointed proposal");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "proposal",
            "checkpoint cancellation clear",
        )
        .await,
        json!({"type":"proposal","echo":null})
    );
    fail_sender
        .send(())
        .expect("release checkpoint cancellation terminal");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "proposal",
            "post-rollback cancellation presentation",
        )
        .await,
        json!({"type":"proposal","echo":null})
    );
    loop {
        if next_browser_json(&mut runtime.browser, "checkpoint cancellation ready").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    let fresh_turn = test_text_turn!("fresh after checkpoint cancellation", summary_id.as_str());
    let fresh_turn_id = test_text_turn_id(&fresh_turn);
    runtime
        .browser
        .send(Message::Text(fresh_turn.to_string().into()))
        .await
        .expect("send fresh turn after checkpoint cancellation");
    loop {
        let frame = next_browser_json_including_text_ack(
            &mut runtime.browser,
            "fresh checkpoint cancellation turn",
        )
        .await;
        if frame["type"] == "text_turn_accepted" {
            assert_eq!(frame["turn_id"], fresh_turn_id);
            break;
        }
        assert_ne!(frame["type"], "text_turn_rejected");
    }
    loop {
        if next_browser_json(
            &mut runtime.browser,
            "fresh checkpoint cancellation response",
        )
        .await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 0);
    release_guard.release();
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn browser_confirmation_commits_checkpoint_before_outcome_unknown_run() {
    let executor = Arc::new(CheckpointControlExecutor {
        send_calls: AtomicUsize::new(0),
        run_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("confirmed run checkpoint Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("confirmed run checkpoint Realtime address")
        .port();
    let (followup_sender, followup_receiver) = tokio::sync::oneshot::channel();
    let (fail_sender, fail_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "confirmed run checkpoint session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "run intent response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"run-intent-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create run intent response");
        for event in function_call_events(
            "run-intent-response",
            "run-intent-item",
            "run-intent-call",
            "create_session",
            r#"{"prompt":"discard this prompt"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send run intent call");
        }
        let intent =
            next_function_output(&mut socket, "run-intent-call", "run intent output").await;
        assert_eq!(intent["error"], "session_task_required");
        complete_tool_response(&mut socket, "run-intent-response", "run-intent-followup").await;
        loop {
            if next_fake_realtime_json(&mut socket, "run proposal response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"run-proposal-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create run proposal response");
        for event in function_call_events(
            "run-proposal-response",
            "run-proposal-item",
            "run-proposal-call",
            "create_session",
            r#"{"prompt":"checkpointed run"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send run proposal call");
        }
        let proposal =
            next_function_output(&mut socket, "run-proposal-call", "run proposal output").await;
        assert_eq!(proposal["proposed"], true);
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"run-proposal-response","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete run proposal response");
        loop {
            if next_fake_realtime_json(&mut socket, "run proposal followup request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"run-confirm-followup"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create run confirmation followup");
        for event in function_call_events(
            "run-confirm-followup",
            "run-fast-item",
            "run-fast-call",
            "list_sessions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send fast run followup call");
        }
        let output = next_function_output(&mut socket, "run-fast-call", "fast run output").await;
        assert_eq!(output["ok"], true);
        followup_sender
            .send(())
            .expect("signal checkpointed run followup");
        fail_receiver
            .await
            .expect("release failed run confirmation followup");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"run-confirm-followup","status":"failed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("fail run confirmation followup");
        let mut saw_fresh_text = false;
        let mut saw_fresh_response = false;
        while !saw_fresh_text || !saw_fresh_response {
            let event = next_fake_realtime_json(&mut socket, "fresh post-run turn").await;
            saw_fresh_text |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("fresh after outcome unknown run");
            saw_fresh_response |= event["type"] == "response.create";
        }
        for event in [
            json!({"type":"response.created","response":{"id":"fresh-post-run"}}),
            json!({
                "type":"response.done",
                "response":{"id":"fresh-post-run","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete fresh post-run response");
        }
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "confirmed run checkpoint browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("create session", null).to_string().into(),
        ))
        .await
        .expect("request run task collection");
    loop {
        if next_browser_json(&mut runtime.browser, "run task collection").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("checkpointed run", null).to_string().into(),
        ))
        .await
        .expect("request run proposal");
    let proposal = next_browser_json_of_type(
        &mut runtime.browser,
        "proposal",
        "checkpointed run proposal",
    )
    .await;
    let handle = proposal["handle"]
        .as_str()
        .expect("checkpointed run handle")
        .to_owned();
    followup_receiver
        .await
        .expect("checkpointed run followup completed");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("confirm checkpointed run");
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "outcome unknown run confirmation").await;
        if frame["type"] == "transcript_done" {
            assert_eq!(
                frame["text"],
                "Confirmed, but the delivery outcome is unknown. It was not retried."
            );
            break;
        }
    }
    assert_eq!(executor.run_calls.load(Ordering::SeqCst), 1);
    fail_sender
        .send(())
        .expect("release failed run confirmation followup");
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "post-confirmation failed terminal").await;
        assert!(
            frame["type"] != "proposal" || frame["echo"].is_null(),
            "failed terminal restored an already executed run proposal"
        );
        if frame == json!({"type":"state","state":"ready"}) {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("retry confirmed run handle");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "proposal",
            "retired run proposal handle",
        )
        .await,
        json!({"type":"proposal","echo":null})
    );
    assert_eq!(executor.run_calls.load(Ordering::SeqCst), 1);
    let fresh_turn = test_text_turn!("fresh after outcome unknown run", null);
    let fresh_turn_id = test_text_turn_id(&fresh_turn);
    runtime
        .browser
        .send(Message::Text(fresh_turn.to_string().into()))
        .await
        .expect("send fresh turn after outcome unknown run");
    loop {
        let frame = next_browser_json_including_text_ack(
            &mut runtime.browser,
            "fresh outcome unknown run turn",
        )
        .await;
        if frame["type"] == "text_turn_accepted" {
            assert_eq!(frame["turn_id"], fresh_turn_id);
            break;
        }
        assert_ne!(frame["type"], "text_turn_rejected");
    }
    loop {
        if next_browser_json(&mut runtime.browser, "fresh outcome unknown run response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    fake_realtime.await.expect("fake Realtime task");
    let journal = Journal::open(&runtime.directory.path().join("state/journal.sqlite3"))
        .expect("open confirmed run journal");
    let timeline = journal
        .timeline(None, 10)
        .expect("read confirmed run journal");
    assert_eq!(timeline.len(), 1);
    assert_eq!(timeline[0].state, "outcome_unknown");
    assert_eq!(timeline[0].destination_thread_id, "new-run");
    assert_eq!(executor.run_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_model_tool_arguments_close_before_dispatch() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("oversized arguments Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("oversized arguments Realtime address")
        .port();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "oversized arguments session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "oversized arguments response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"oversized-arguments-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create oversized arguments response");
        let arguments = "x".repeat(64 * 1_024 + 1);
        for event in function_call_events_at(
            "oversized-arguments-response",
            "oversized-arguments-item",
            "oversized-arguments-call",
            0,
            "cancel_action",
            &arguments,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send oversized arguments call");
        }
        let _ = socket.next().await;
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("oversized arguments", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request oversized arguments response");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "error",
            "oversized arguments rejection",
        )
        .await,
        json!({
            "type":"error",
            "message":"Realtime function-call arguments exceeded the safe limit. Reconnect required."
        })
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn aggregate_model_tool_arguments_close_before_exceeding_retained_limit() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("aggregate arguments Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("aggregate arguments Realtime address")
        .port();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "aggregate arguments session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "aggregate arguments response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"aggregate-arguments-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create aggregate arguments response");
        let arguments = format!(r#"{{"padding":"{}"}}"#, "x".repeat(60 * 1_024));
        for index in 0..5_u64 {
            let name = if index == 0 {
                "list_pending_permissions"
            } else {
                "cancel_action"
            };
            for event in function_call_events_at(
                "aggregate-arguments-response",
                &format!("aggregate-item-{index}"),
                &format!("aggregate-call-{index}"),
                index,
                name,
                &arguments,
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send aggregate arguments call");
            }
        }
        let _ = socket.next().await;
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "aggregate arguments browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("aggregate arguments", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request aggregate arguments response");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("aggregate process start timeout")
        .expect("aggregate process start signal");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "error",
            "aggregate arguments rejection",
        )
        .await,
        json!({
            "type":"error",
            "message":"Realtime function-call argument capacity exhausted. Reconnect required."
        })
    );
    release_guard.release();
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn proposal_cancellation_while_tools_are_away_is_authoritative() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("deferred cancellation Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("deferred cancellation Realtime address")
        .port();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let (fresh_complete_sender, fresh_complete_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "deferred cancellation session update").await;
        bind_fake_realtime_context(&mut socket, "deferred-cancellation").await;
        loop {
            if next_fake_realtime_json(&mut socket, "deferred cancellation response request").await
                ["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"deferred-cancellation-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create deferred cancellation response");
        for event in function_call_events_at(
            "deferred-cancellation-response",
            "proposal-item",
            "proposal-call",
            0,
            "send_message",
            r#"{"text":"proposed response"}"#,
        )
        .into_iter()
        .chain(function_call_events_at(
            "deferred-cancellation-response",
            "latched-after-proposal-item",
            "latched-after-proposal-call",
            1,
            "list_pending_permissions",
            "{}",
        )) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send deferred cancellation tool call");
        }
        let proposal_output = next_function_output(
            &mut socket,
            "proposal-call",
            "deferred cancellation proposal output",
        )
        .await;
        assert_eq!(proposal_output["proposed"], true);
        let mut saw_cancel = false;
        let mut saw_clear = false;
        while !saw_cancel || !saw_clear {
            let event =
                next_fake_realtime_json(&mut socket, "deferred proposal cancellation").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("latched-after-proposal-call")
            );
            saw_cancel |= event["type"] == "response.cancel";
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"deferred-cancellation-response","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete deferred cancellation response");
        cancelled_sender
            .send(())
            .expect("signal authoritative proposal cancellation");
        let mut saw_fresh_text = false;
        let mut saw_fresh_response = false;
        while !saw_fresh_text || !saw_fresh_response {
            let event =
                next_fake_realtime_json(&mut socket, "fresh deferred cancellation turn").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("latched-after-proposal-call"),
                "returned stale work emitted provider output"
            );
            saw_fresh_text |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("fresh after cancellation");
            saw_fresh_response |= event["type"] == "response.create";
        }
        for event in [
            json!({"type":"response.created","response":{"id":"fresh-cancellation-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"fresh-cancellation-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete fresh deferred cancellation response");
        }
        fresh_complete_receiver
            .await
            .expect("finish fresh deferred cancellation response");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "deferred cancellation browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind deferred cancellation context");
    let mut summary_id = None;
    let mut ready = false;
    while summary_id.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "deferred cancellation context").await;
        if let Some(id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_id = Some(id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_id = summary_id.expect("deferred cancellation summary ID");
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("propose then keep working", summary_id.as_str())
                .to_string()
                .into(),
        ))
        .await
        .expect("request deferred cancellation tools");
    let proposal_handle = loop {
        let frame = next_browser_json(&mut runtime.browser, "deferred cancellation proposal").await;
        if frame["type"] == "proposal"
            && frame["echo"].is_string()
            && let Some(handle) = frame["handle"].as_str()
        {
            break handle.to_owned();
        }
    };
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("deferred cancellation process start timeout")
        .expect("deferred cancellation process start signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"cancel_proposal","handle":proposal_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel proposal while tools are away");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "proposal",
            "authoritative deferred proposal cancellation",
        )
        .await,
        json!({"type":"proposal","echo":null})
    );
    tokio::time::timeout(Duration::from_secs(2), cancelled_receiver)
        .await
        .expect("authoritative cancellation timeout")
        .expect("authoritative cancellation signal");
    release_guard.release();
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("probe deferred cancellation restoration");
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "deferred cancellation restoration").await;
        if frame["type"] == "host_state" {
            break;
        }
        if frame["type"] == "dictation_capabilities" {
            runtime
                .browser
                .send(Message::Text(
                    json!({"type":"hello","protocol_version":2})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("retry deferred cancellation restoration probe");
        }
    }
    let fresh_turn = test_text_turn!("fresh after cancellation", summary_id.as_str());
    let fresh_turn_id = test_text_turn_id(&fresh_turn);
    runtime
        .browser
        .send(Message::Text(fresh_turn.to_string().into()))
        .await
        .expect("send fresh turn after deferred cancellation");
    loop {
        let frame =
            next_browser_json_including_text_ack(&mut runtime.browser, "fresh cancellation turn")
                .await;
        if frame["type"] == "text_turn_accepted" {
            assert_eq!(frame["turn_id"], fresh_turn_id);
            break;
        }
    }
    loop {
        if next_browser_json(&mut runtime.browser, "fresh cancellation response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    fresh_complete_sender
        .send(())
        .expect("finish fresh deferred cancellation test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failed_terminal_clears_proposal_mutated_by_running_tool() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingDashboardExecutor {
        latch,
        dashboard_armed: AtomicBool::new(false),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("proposal mutation Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("proposal mutation Realtime address")
        .port();
    let (cancel_sender, cancel_receiver) = tokio::sync::oneshot::channel();
    let (terminal_sender, terminal_receiver) = tokio::sync::oneshot::channel();
    let (terminal_observed_sender, terminal_observed_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "proposal mutation session update").await;
        bind_fake_realtime_context(&mut socket, "proposal-mutation").await;
        loop {
            if next_fake_realtime_json(&mut socket, "proposal mutation response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"proposal-mutation-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create proposal mutation response");
        for event in function_call_events_at(
            "proposal-mutation-response",
            "proposal-mutation-item",
            "proposal-mutation-call",
            0,
            "send_message",
            r#"{"text":"visible proposal"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send proposal mutation proposal call");
        }
        let proposal = next_function_output(
            &mut socket,
            "proposal-mutation-call",
            "proposal mutation proposal output",
        )
        .await;
        assert_eq!(proposal["proposed"], true);
        cancel_receiver
            .await
            .expect("release proposal mutation cancellation");
        for event in function_call_events_at(
            "proposal-mutation-response",
            "proposal-cancel-item",
            "proposal-cancel-call",
            1,
            "cancel_action",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send proposal mutation cancellation call");
        }
        terminal_receiver
            .await
            .expect("release proposal mutation failed terminal");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"proposal-mutation-response","status":"failed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send proposal mutation failed terminal");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"proposal-mutation-barrier"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send proposal mutation terminal barrier");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "proposal mutation terminal barrier").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("proposal-cancel-call"),
                "failed response published its proposal mutation"
            );
            if event["type"] == "response.cancel"
                && event["response_id"] == "proposal-mutation-barrier"
            {
                break;
            }
        }
        terminal_observed_sender
            .send(())
            .expect("signal proposal mutation failed terminal");
        let mut saw_fresh_text = false;
        let mut saw_fresh_response = false;
        while !saw_fresh_text || !saw_fresh_response {
            let event = next_fake_realtime_json(&mut socket, "fresh proposal mutation turn").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("proposal-cancel-call"),
                "failed response published its proposal mutation"
            );
            saw_fresh_text |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("fresh after proposal mutation");
            saw_fresh_response |= event["type"] == "response.create";
        }
        for event in [
            json!({"type":"response.created","response":{"id":"fresh-proposal-mutation-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"fresh-proposal-mutation-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete fresh proposal mutation response");
        }
        finish_receiver
            .await
            .expect("finish proposal mutation test");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "proposal mutation browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("mutate visible proposal", summary_id.as_str())
                .to_string()
                .into(),
        ))
        .await
        .expect("request proposal mutation response");
    let stale_proposal_handle = loop {
        let frame = next_browser_json(&mut runtime.browser, "visible proposal mutation card").await;
        if let Some(handle) = frame["handle"].as_str() {
            break handle.to_owned();
        }
    };
    executor.dashboard_armed.store(true, Ordering::SeqCst);
    cancel_sender
        .send(())
        .expect("release proposal mutation cancellation");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("proposal mutation process start timeout")
        .expect("proposal mutation process start signal");
    terminal_sender
        .send(())
        .expect("release proposal mutation failed terminal");
    terminal_observed_receiver
        .await
        .expect("proposal mutation failed terminal observation");
    release_guard.release();
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "proposal",
            "failed terminal proposal reconciliation",
        )
        .await,
        json!({"type":"proposal","echo":null})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":stale_proposal_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("reject failed response proposal handle");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "proposal",
            "failed response stale proposal confirmation",
        )
        .await,
        json!({"type":"proposal","echo":null})
    );
    let fresh_turn = test_text_turn!("fresh after proposal mutation", summary_id.as_str());
    let fresh_turn_id = test_text_turn_id(&fresh_turn);
    runtime
        .browser
        .send(Message::Text(fresh_turn.to_string().into()))
        .await
        .expect("send fresh proposal mutation turn");
    loop {
        let frame = next_browser_json_including_text_ack(
            &mut runtime.browser,
            "fresh proposal mutation turn",
        )
        .await;
        if frame["type"] == "text_turn_accepted" {
            assert_eq!(frame["turn_id"], fresh_turn_id);
            break;
        }
    }
    loop {
        if next_browser_json(&mut runtime.browser, "fresh proposal mutation response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    finish_sender
        .send(())
        .expect("finish proposal mutation test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn known_host_replacement_while_tools_are_away_is_authoritative() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("deferred host Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("deferred host Realtime address")
        .port();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "deferred host session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "deferred host response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"deferred-host-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create deferred host response");
        for event in function_call_events_at(
            "deferred-host-response",
            "deferred-host-item",
            "deferred-host-call",
            0,
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send deferred host call");
        }
        let mut saw_cancel = false;
        let mut saw_clear = false;
        while !saw_cancel || !saw_clear {
            let event = next_fake_realtime_json(&mut socket, "deferred host cancellation").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("deferred-host-call")
            );
            saw_cancel |= event["type"] == "response.cancel";
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"deferred-host-response","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete deferred host response");
        cancelled_sender
            .send(())
            .expect("signal deferred host cancellation");
        let deadline = tokio::time::Instant::now() + Duration::from_millis(400);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("deferred host JSON");
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("deferred-host-call"),
                "returned host-replaced work emitted provider output"
            );
            assert_ne!(event["type"], "response.create");
        }
    });

    let hosts = vec![
        PaseoHostProfile {
            id: "first".to_owned(),
            label: "First".to_owned(),
            target: None,
            default: true,
            default_cwd: "~/".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
        PaseoHostProfile {
            id: "second".to_owned(),
            label: "Second".to_owned(),
            target: Some("second.example:6767".to_owned()),
            default: false,
            default_cwd: "~/dev".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
    ];
    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_hosts_and_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
        hosts,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "deferred host browser").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("hold the engine", null).to_string().into(),
        ))
        .await
        .expect("request deferred host response");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("deferred host process start timeout")
        .expect("deferred host process start signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("replace host while tools are away");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "proposal",
            "deferred host proposal clear",
        )
        .await,
        json!({"type":"proposal","echo":null})
    );
    tokio::time::timeout(Duration::from_secs(2), cancelled_receiver)
        .await
        .expect("deferred host cancellation timeout")
        .expect("deferred host cancellation signal");
    release_guard.release();
    loop {
        let frame = next_browser_json(&mut runtime.browser, "deferred host frame").await;
        if frame["type"] == "host_state" {
            assert_eq!(frame["selected_host_id"], "second");
            break;
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn coalesced_host_replacement_invalidates_hidden_engine_state() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("coalesced host Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("coalesced host Realtime address")
        .port();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "coalesced host session update").await;
        bind_fake_realtime_context(&mut socket, "coalesced-host").await;
        loop {
            if next_fake_realtime_json(&mut socket, "coalesced host response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"coalesced-host-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create coalesced host response");
        for event in function_call_events_at(
            "coalesced-host-response",
            "coalesced-host-item",
            "coalesced-host-call",
            0,
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send coalesced host tool call");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "coalesced host cancellation").await;
            if event["type"] == "response.cancel" {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"coalesced-host-response","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete coalesced host response");
        finish_receiver.await.expect("finish coalesced host test");
    });

    let hosts = vec![
        PaseoHostProfile {
            id: "first".to_owned(),
            label: "First".to_owned(),
            target: None,
            default: true,
            default_cwd: "~/".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
        PaseoHostProfile {
            id: "second".to_owned(),
            label: "Second".to_owned(),
            target: Some("second.example:6767".to_owned()),
            default: false,
            default_cwd: "~/dev".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
    ];
    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_hosts_and_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
        hosts,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "coalesced host browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("hold hidden host state", summary_id)
                .to_string()
                .into(),
        ))
        .await
        .expect("request coalesced host response");
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("coalesced host process start timeout")
        .expect("coalesced host process start signal");
    for host_id in ["second", "first"] {
        runtime
            .browser
            .send(Message::Text(
                json!({"type":"select_host","host_id":host_id})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("coalesce host replacement while tools are away");
        assert_eq!(
            next_browser_json_of_type(
                &mut runtime.browser,
                "proposal",
                "coalesced host proposal clear",
            )
            .await,
            json!({"type":"proposal","echo":null})
        );
    }
    release_guard.release();
    loop {
        let frame = next_browser_json(&mut runtime.browser, "coalesced host restoration").await;
        if frame["type"] == "dashboard_state" && frame["selected_host_id"] == "first" {
            assert_eq!(frame["bound_context"], Value::Null);
            break;
        }
    }
    finish_sender.send(()).expect("finish coalesced host test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn function_call_capacity_drains_summary_without_creating_followup() {
    let mut summariser = LatchedSummariser::start("Capacity summary.").await;
    let paseo_directory = tempfile::tempdir().expect("capacity summary Paseo directory");
    let long_reply = "Capacity summary source. ".repeat(80);
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-capacity-summary", &long_reply);
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("capacity summary Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("capacity summary Realtime address")
        .port();
    let (capacity_sender, capacity_receiver) = tokio::sync::oneshot::channel();
    let (outputs_drained_sender, outputs_drained_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "capacity-summary-response",
            "capacity-summary-call",
            "thread-a",
        )
        .await;
        capacity_receiver
            .await
            .expect("release capacity calls after summary start");
        for index in 1..=64_u64 {
            for event in function_call_events_at(
                "capacity-summary-response",
                &format!("capacity-summary-item-{index}"),
                &format!("capacity-summary-extra-call-{index}"),
                index,
                "cancel_action",
                "{}",
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send capacity summary call");
            }
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"capacity-summary-response","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete capacity summary response");
        let mut outputs = 0;
        let mut outputs_drained_sender = Some(outputs_drained_sender);
        loop {
            let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
                .await
                .expect("capacity summary provider close timeout");
            match message {
                Some(Ok(Message::Text(message))) => {
                    let event: Value =
                        serde_json::from_str(&message).expect("capacity summary provider JSON");
                    assert_ne!(
                        event["type"], "response.create",
                        "capacity exhaustion created a followup response"
                    );
                    outputs += usize::from(
                        event.pointer("/item/type").and_then(Value::as_str)
                            == Some("function_call_output"),
                    );
                    if outputs == 64
                        && let Some(sender) = outputs_drained_sender.take()
                    {
                        sender
                            .send(())
                            .expect("signal capacity summary outputs drained");
                    }
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            }
        }
        assert_eq!(outputs, 64);
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read then exhaust calls", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request capacity summary response");
    summariser.wait_started().await;
    capacity_sender
        .send(())
        .expect("release capacity calls after summary start");
    outputs_drained_receiver
        .await
        .expect("capacity summary outputs drained");
    summariser.release();
    assert_eq!(
        next_browser_json_of_type(&mut runtime.browser, "error", "capacity summary exhaustion",)
            .await,
        json!({
            "type":"error",
            "message":"Realtime function-call capacity exhausted. Reconnect required."
        })
    );
    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[allow(dead_code)]
async fn begin_fake_summary_call(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    response_id: &str,
    call_id: &str,
    session_id: &str,
) {
    assert_eq!(
        next_fake_realtime_json(socket, "summary session update").await["type"],
        "session.update"
    );
    loop {
        if next_fake_realtime_json(socket, "summary response request").await["type"]
            == "response.create"
        {
            break;
        }
    }
    socket
        .send(Message::Text(
            json!({"type":"response.created","response":{"id":response_id}})
                .to_string()
                .into(),
        ))
        .await
        .expect("send summary response created");
    for event in function_call_events(
        response_id,
        &format!("item-{call_id}"),
        call_id,
        "read_latest_reply",
        &json!({"session":session_id}).to_string(),
    ) {
        socket
            .send(Message::Text(event.to_string().into()))
            .await
            .expect("send summary tool call");
    }
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn slow_summary_allows_barge_in_and_retires_only_its_provider_response() {
    let mut summariser =
        DelayedSummariser::start("Summary A completed.", Duration::from_millis(1_500)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Long coding-agent outcome with implementation detail. ".repeat(40);
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-slow-summary", &long_reply);

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-summary-a",
            "call-summary-a",
            "thread-a",
        )
        .await;

        let mut saw_cancel = false;
        let mut saw_clear = false;
        while !saw_cancel || !saw_clear {
            let event = next_fake_realtime_json(&mut socket, "summary barge-in").await;
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output"),
                "canceled response received a function output"
            );
            assert_ne!(event["type"], "response.create");
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-summary-a");
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-summary-a","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete canceled summary response");
        cancelled_sender
            .send(())
            .expect("signal summary response cancellation");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("post-cancel provider JSON");
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output")
            );
            assert_ne!(event["type"], "response.create");
        }
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read reply A", null).to_string().into(),
        ))
        .await
        .expect("request reply A");
    summariser.wait_started().await;
    let summary_id = next_bound_summary_id(&mut runtime.browser, "reply A context").await;

    let responsiveness_deadline = tokio::time::Instant::now() + Duration::from_millis(700);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge into slow summary");
    let mut flushed = false;
    while !flushed {
        let remaining =
            responsiveness_deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "slow summary blocked browser barge-in"
        );
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("barge-in browser timeout")
            .expect("barge-in browser frame")
            .expect("barge-in browser result");
        if let Message::Text(message) = message {
            let frame: Value = serde_json::from_str(&message).expect("barge-in browser JSON");
            flushed |= frame["type"] == "flush_audio";
        }
    }
    let remaining = responsiveness_deadline.saturating_duration_since(tokio::time::Instant::now());
    tokio::time::timeout(remaining, cancelled_receiver)
        .await
        .expect("exact response cancellation timeout")
        .expect("exact response cancellation signal");

    let dashboard_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let context = loop {
        let remaining = dashboard_deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "summary A dashboard update timeout");
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("summary A browser timeout")
            .expect("summary A browser frame")
            .expect("summary A browser result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("summary A browser JSON");
        assert_ne!(frame.pointer("/phase").and_then(Value::as_str), Some("ok"));
        if frame["type"] == "dashboard_state"
            && frame["bound_context"]["latest_summary"] == "Summary A completed."
        {
            break frame["bound_context"].clone();
        }
    };
    assert_eq!(context["thread_id"], "thread-a");
    assert!(
        context["summary_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(context["summary_degraded"], false);

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
async fn replay_after_another_tool_requires_a_dedicated_turn_without_summary_leakage() {
    let mut summariser =
        DelayedSummariser::start("Completed summary A.", Duration::from_millis(600)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Long unresolved reply A. ".repeat(80);
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-unresolved-replay",
        &long_reply,
    );
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (replay_sender, replay_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-unresolved-replay",
            "call-unresolved-read",
            "thread-a",
        )
        .await;
        replay_receiver.await.expect("release unresolved replay");
        for event in function_call_events_at(
            "response-unresolved-replay",
            "item-unresolved-replay",
            "call-unresolved-replay",
            1,
            "replay_summary",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send unresolved replay call");
        }

        let first_output = loop {
            let event = next_fake_realtime_json(&mut socket, "unresolved replay output").await;
            if event.pointer("/item/type").and_then(Value::as_str) == Some("function_call_output") {
                break event;
            }
        };
        assert_eq!(
            first_output
                .pointer("/item/call_id")
                .and_then(Value::as_str),
            Some("call-unresolved-replay"),
            "replay degraded and overtook the unresolved read"
        );
        let replay: Value = serde_json::from_str(
            first_output
                .pointer("/item/output")
                .and_then(Value::as_str)
                .expect("unresolved replay output body"),
        )
        .expect("unresolved replay output JSON");
        assert_eq!(
            replay,
            json!({"ok":false,"error":"replay_requires_dedicated_turn"})
        );
        assert_eq!(replay.as_object().expect("dedicated-turn error").len(), 2);
        assert!(replay.get("summary_id").is_none());
        assert!(replay.get("spoken_text").is_none());

        let read = next_function_output(
            &mut socket,
            "call-unresolved-read",
            "completed unresolved read output",
        )
        .await;
        assert_eq!(read["spoken_text"], "Completed summary A.");
        assert_eq!(read["degraded"], false);
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read and replay immediately", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request unresolved replay response");
    summariser.wait_started().await;
    replay_sender.send(()).expect("request unresolved replay");

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn replay_of_a_retired_summary_job_uses_preliminary_text_while_work_drains() {
    let mut summariser =
        DelayedSummariser::start("Completed summary A.", Duration::from_millis(900)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Preliminary broker summary A with detail. ".repeat(80);
    let preliminary = paseo_control_plane::summarise::clean_for_speech(&long_reply)
        .chars()
        .take(600)
        .collect::<String>();
    let provider_preliminary = preliminary.clone();
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-retired-replay", &long_reply);
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (replay_sender, replay_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-retired-read",
            "call-retired-read",
            "thread-a",
        )
        .await;

        let mut saw_cancel = false;
        let mut saw_clear = false;
        let mut saw_commit = false;
        let mut saw_replay_request = false;
        while !saw_cancel || !saw_clear || !saw_commit || !saw_replay_request {
            let event = next_fake_realtime_json(&mut socket, "retired summary barge-in").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("call-retired-read"),
                "retired read output escaped before replay"
            );
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-retired-read");
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
            saw_commit |= event["type"] == "input_audio_buffer.commit";
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "retired-replay-audio").await;
            }
            saw_replay_request |= event["type"] == "response.create";
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.created",
                    "response":{"id":"response-retired-replay"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("create retired replay response");
        for event in function_call_events(
            "response-retired-replay",
            "item-retired-replay",
            "call-retired-replay",
            "replay_summary",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send retired replay call");
        }
        let replay =
            next_function_output(&mut socket, "call-retired-replay", "retired replay output").await;
        assert_eq!(replay["spoken_text"], provider_preliminary);
        assert!(replay.get("degraded").is_none());
        replay_sender
            .send(
                replay["summary_id"]
                    .as_str()
                    .expect("summary ID")
                    .to_owned(),
            )
            .expect("report retired replay");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-retired-replay","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete retired replay tool response");
        let replay_speech = loop {
            let event = next_fake_realtime_json(&mut socket, "retired replay speech request").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("call-retired-read"),
                "retired read output escaped while the job drained"
            );
            if event["type"] == "response.create" {
                break event;
            }
        };
        assert_eq!(replay_speech["response"]["tool_choice"], "none");
        for event in [
            json!({
                "type":"response.created",
                "response":{"id":"response-retired-replay-speech"}
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-retired-replay-speech","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete retired replay speech");
        }
        finish_receiver.await.expect("finish retired replay test");
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read retired summary A", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request retired summary read");
    summariser.wait_started().await;
    let summary_id = next_bound_summary_id(&mut runtime.browser, "retired summary context").await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge into retired summary read");
    loop {
        if next_browser_json(&mut runtime.browser, "retired replay flush").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("request retired summary replay");
    let summary_id = tokio::time::timeout(Duration::from_secs(3), replay_receiver)
        .await
        .expect("retired replay timeout")
        .expect("retired replay result");

    let completion_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let context = loop {
        let remaining = completion_deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "retired summary completion timeout");
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("retired summary browser timeout")
            .expect("retired summary browser frame")
            .expect("retired summary browser result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("retired summary completion JSON");
        if frame["type"] == "dashboard_state"
            && frame["bound_context"]["latest_summary"] == "Completed summary A."
        {
            break frame["bound_context"].clone();
        }
    };
    assert_eq!(context["summary_id"], summary_id);
    assert_eq!(context["summary_degraded"], false);
    assert_ne!(context["latest_summary"], preliminary);
    assert_eq!(summariser.request_count(), 1);
    finish_sender.send(()).expect("finish retired replay");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn replay_after_barge_in_locks_origin_and_speech_responses() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-replay-summary");
    let process_marker = paseo_directory.path().join("process-calls");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "printf '%s\\n' \"$*\" >> '{}'\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"}},{{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Agent B\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf '%s\\n' 'Broker summary A.' ;;\n",
                "  send) printf '%s\\n' '{{\"messageId\":\"message-replay-a\"}}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            process_marker.display(),
        ),
    )
    .expect("write fake Paseo replay executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo replay metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo replay permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (summary_started_sender, summary_started_receiver) = tokio::sync::oneshot::channel();
    let (replays_sender, replays_receiver) = tokio::sync::oneshot::channel();
    let (continue_sender, continue_receiver) = tokio::sync::oneshot::channel();
    let (locked_sender, locked_receiver) = tokio::sync::oneshot::channel();
    let (speech_sender, speech_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let provider_process_marker = process_marker.clone();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let session_update = next_fake_realtime_json(&mut socket, "replay session update").await;
        let replay_definition = session_update["session"]["tools"]
            .as_array()
            .expect("Realtime tools")
            .iter()
            .find(|tool| tool["name"] == "replay_summary")
            .expect("Realtime replay tool");
        assert_eq!(
            replay_definition["parameters"],
            json!({"type":"object","properties":{},"additionalProperties":false})
        );
        assert!(
            session_update["session"]["instructions"]
                .as_str()
                .is_some_and(|instructions| instructions.contains("replay_summary"))
        );
        loop {
            if next_fake_realtime_json(&mut socket, "read A response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-read-a"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create read A response");
        for event in function_call_events(
            "response-read-a",
            "item-read-a",
            "call-read-a",
            "read_latest_reply",
            &json!({"session":"thread-a"}).to_string(),
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send read A tool call");
        }
        let read = next_function_output(&mut socket, "call-read-a", "read A output").await;
        assert_eq!(read["spoken_text"], "Broker summary A.");
        assert_eq!(read["respondable"], true);
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-read-a","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete read A response");
        loop {
            if next_fake_realtime_json(&mut socket, "summary A response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"response-summary-a"}}),
            json!({
                "type":"response.output_audio_transcript.delta",
                "response_id":"response-summary-a",
                "delta":"Broker summary A."
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("start summary A response");
        }
        summary_started_sender
            .send(())
            .expect("signal summary A response");

        let mut saw_cancel = false;
        let mut saw_clear = false;
        let mut saw_commit = false;
        let mut saw_replay_request = false;
        while !saw_cancel || !saw_clear || !saw_commit || !saw_replay_request {
            let event = next_fake_realtime_json(&mut socket, "summary A barge-in").await;
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-summary-a");
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
            saw_commit |= event["type"] == "input_audio_buffer.commit";
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "replay-audio").await;
            }
            saw_replay_request |= event["type"] == "response.create";
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-replay-a"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create replay response");
        let calls_before = std::fs::read_to_string(&provider_process_marker)
            .unwrap_or_default()
            .lines()
            .count();
        for event in [
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-summary-a",
                "delta":"AQIDBA=="
            }),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-summary-a",
                "transcript":"Late old summary A."
            }),
            function_call_events(
                "response-summary-a",
                "item-late-old-a",
                "call-late-old-a",
                "replay_summary",
                "{}",
            )[0]
            .clone(),
            function_call_events(
                "response-summary-a",
                "item-late-old-a",
                "call-late-old-a",
                "replay_summary",
                "{}",
            )[1]
            .clone(),
            json!({
                "type":"response.done",
                "response":{"id":"response-summary-a","status":"cancelled"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send late summary A event");
        }
        for event in function_call_events(
            "response-replay-a",
            "item-replay-a",
            "call-replay-a",
            "replay_summary",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send replay summary call");
        }
        let replay =
            next_function_output(&mut socket, "call-replay-a", "replay summary output").await;
        let calls_after = std::fs::read_to_string(&provider_process_marker)
            .unwrap_or_default()
            .lines()
            .count();
        replays_sender
            .send((replay, calls_before, calls_after))
            .expect("report replay outputs");
        continue_receiver
            .await
            .expect("continue after replay checks");

        let calls_before_locked = std::fs::read_to_string(&provider_process_marker)
            .unwrap_or_default()
            .lines()
            .count();
        let mut locked_outputs = Vec::new();
        for (offset, (item_id, call_id, name, arguments)) in [
            (
                "item-origin-malicious-send",
                "call-origin-malicious-send",
                "send_message",
                json!({"text":"must not become a proposal"}).to_string(),
            ),
            (
                "item-origin-malicious-select",
                "call-origin-malicious-select",
                "set_current_session",
                json!({"session":"thread-b"}).to_string(),
            ),
            (
                "item-origin-malicious-cancel",
                "call-origin-malicious-cancel",
                "cancel_action",
                "{}".to_owned(),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            for event in function_call_events_at(
                "response-replay-a",
                item_id,
                call_id,
                offset as u64 + 1,
                name,
                &arguments,
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send replay-locked origin tool call");
            }
            locked_outputs.push(
                next_function_output(&mut socket, call_id, "replay-locked origin output").await,
            );
        }
        let calls_after_locked = std::fs::read_to_string(&provider_process_marker)
            .unwrap_or_default()
            .lines()
            .count();
        locked_sender
            .send((locked_outputs, calls_before_locked, calls_after_locked))
            .expect("report replay-locked origin results");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-replay-a","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete replay tool response");
        let replay_speech_request = loop {
            let event = next_fake_realtime_json(&mut socket, "replay speech request").await;
            if event["type"] == "response.create" {
                break event;
            }
        };
        assert_eq!(replay_speech_request["response"]["tools"], json!([]));
        assert_eq!(replay_speech_request["response"]["tool_choice"], "none");
        let replay_instruction = replay_speech_request["response"]["instructions"]
            .as_str()
            .expect("replay speech instruction");
        assert!(replay_instruction.contains("replay_summary"));
        assert!(replay_instruction.contains("Speak only"));
        assert!(replay_instruction.chars().count() <= 256);
        socket
            .send(Message::Text(
                json!({
                    "type":"response.created",
                    "response":{"id":"response-replay-speech-a"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("create replay speech response");
        let calls_before_speech = std::fs::read_to_string(&provider_process_marker)
            .unwrap_or_default()
            .lines()
            .count();
        for (item_id, call_id, name, arguments) in [
            (
                "item-malicious-select",
                "call-malicious-select",
                "set_current_session",
                json!({"session":"thread-b"}).to_string(),
            ),
            (
                "item-malicious-send",
                "call-malicious-send",
                "send_message",
                json!({"text":"replace the pending proposal"}).to_string(),
            ),
            (
                "item-malicious-cancel",
                "call-malicious-cancel",
                "cancel_action",
                "{}".to_owned(),
            ),
        ] {
            for event in function_call_events(
                "response-replay-speech-a",
                item_id,
                call_id,
                name,
                &arguments,
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send malicious replay speech tool call");
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("replay speech outbound JSON");
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output"),
                "replay speech executed a provider tool"
            );
            assert_ne!(event["type"], "response.create");
        }
        let calls_after_speech = std::fs::read_to_string(&provider_process_marker)
            .unwrap_or_default()
            .lines()
            .count();
        for event in [
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-replay-speech-a",
                "transcript":"Broker summary A."
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-replay-speech-a","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete replay speech response");
        }
        speech_sender
            .send((calls_before_speech, calls_after_speech))
            .expect("report replay speech isolation");
        finish_receiver.await.expect("finish replay connection");
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read summary A", null).to_string().into(),
        ))
        .await
        .expect("request summary A");
    tokio::time::timeout(Duration::from_secs(3), summary_started_receiver)
        .await
        .expect("summary A response timeout")
        .expect("summary A response signal");
    let mut summary_id = None;
    loop {
        let frame = next_browser_json(&mut runtime.browser, "summary A playback start").await;
        if let Some(id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_id = Some(id.to_owned());
        }
        if frame == json!({"type":"transcript_delta","text":"Broker summary A."})
            && summary_id.is_some()
        {
            break;
        }
    }
    let summary_id = summary_id.expect("summary A ID");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge into summary A");
    loop {
        if next_browser_json(&mut runtime.browser, "summary replay flush").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("request summary A replay");
    let (replay, calls_before, calls_after) =
        tokio::time::timeout(Duration::from_secs(5), replays_receiver)
            .await
            .expect("replay outputs timeout")
            .expect("replay outputs");
    assert_eq!(replay["spoken_text"], "Broker summary A.");
    assert!(
        replay["summary_id"]
            .as_str()
            .is_some_and(|summary_id| !summary_id.is_empty())
    );
    assert_eq!(replay.as_object().expect("replay result").len(), 3);
    assert_eq!(calls_after, calls_before, "replay invoked a Paseo process");
    continue_sender
        .send(())
        .expect("continue to replay-locked calls");
    let (locked_outputs, calls_before_locked, calls_after_locked) =
        tokio::time::timeout(Duration::from_secs(3), locked_receiver)
            .await
            .expect("replay-locked outputs timeout")
            .expect("replay-locked outputs");
    assert_eq!(locked_outputs.len(), 3);
    for output in locked_outputs {
        assert_eq!(output, json!({"ok":false,"error":"response_replay_locked"}));
        assert!(output.to_string().len() <= 128);
    }
    assert_eq!(
        calls_after_locked, calls_before_locked,
        "replay-locked origin invoked a Paseo process"
    );
    let (calls_before_speech, calls_after_speech) =
        tokio::time::timeout(Duration::from_secs(3), speech_receiver)
            .await
            .expect("replay speech isolation timeout")
            .expect("replay speech isolation result");
    assert_eq!(
        calls_after_speech, calls_before_speech,
        "replay speech invoked a Paseo process"
    );
    let mut replay_spoken = false;
    let mut replay_ready = false;
    while !replay_spoken || !replay_ready {
        let frame = next_browser_json(&mut runtime.browser, "replay speech browser frame").await;
        assert_ne!(
            frame["type"], "proposal",
            "replay-locked response created or changed a proposal"
        );
        if frame["type"] == "tool" {
            assert!(!matches!(
                frame["name"].as_str(),
                Some("set_current_session" | "send_message" | "cancel_action")
            ));
        }
        replay_spoken |= frame
            == json!({
                "type":"transcript_done",
                "text":"Broker summary A."
            });
        replay_ready |= frame == json!({"type":"state","state":"ready"});
    }
    let process_calls = std::fs::read_to_string(&process_marker).expect("replay process calls");
    let sends = process_calls
        .lines()
        .filter(|call| call.starts_with("send "))
        .collect::<Vec<_>>();
    assert!(
        sends.is_empty(),
        "replay-locked response dispatched a write"
    );
    finish_sender.send(()).expect("finish replay test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn replacement_context_drains_one_summary_request_across_three_reads() {
    use std::{
        os::unix::fs::PermissionsExt as _,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    let summary_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("context summariser listener");
    let summary_address = summary_listener
        .local_addr()
        .expect("context summariser address");
    let request_count = Arc::new(AtomicUsize::new(0));
    let handler_request_count = Arc::clone(&request_count);
    let (first_started_sender, first_started_receiver) = tokio::sync::oneshot::channel();
    let first_started_sender = Arc::new(Mutex::new(Some(first_started_sender)));
    let handler_first_started_sender = Arc::clone(&first_started_sender);
    let summary_app = axum::Router::new().route(
        "/v1/chat/completions",
        axum::routing::post(move || {
            handler_request_count.fetch_add(1, Ordering::SeqCst);
            let first_started_sender = Arc::clone(&handler_first_started_sender);
            async move {
                if let Some(sender) = first_started_sender
                    .lock()
                    .expect("first summary start lock")
                    .take()
                {
                    sender.send(()).expect("signal first summary start");
                }
                tokio::time::sleep(Duration::from_millis(1_500)).await;
                axum::Json(json!({
                    "choices": [{"message": {"content": "Stale summary A."}}]
                }))
            }
        }),
    );
    let summary_server = tokio::spawn(async move {
        axum::serve(summary_listener, summary_app)
            .await
            .expect("context summariser server");
    });

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-summary-context");
    let reply_a = "Reply A long implementation detail. ".repeat(40);
    let reply_b = "Reply B distinct long implementation detail. ".repeat(100);
    let reply_c = "Reply C final actionable implementation detail. ".repeat(100);
    let expected_b = reply_b
        .trim()
        .chars()
        .rev()
        .take(2_400)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    let provider_expected_b = expected_b.clone();
    let expected_c = reply_c
        .trim()
        .chars()
        .rev()
        .take(2_400)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    let expected_c_dashboard = expected_c.chars().take(600).collect::<String>();
    let provider_expected_c = expected_c.clone();
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Session A\",\"status\":\"idle\"}},{{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Session B\",\"status\":\"idle\"}},{{\"id\":\"thread-c\",\"shortId\":\"c\",\"name\":\"Session C\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) if [ \"$2\" = 'thread-a' ]; then printf '%s\\n' '{}'; elif [ \"$2\" = 'thread-b' ]; then printf '%s\\n' '{}'; else printf '%s\\n' '{}'; fi ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            reply_a, reply_b, reply_c
        ),
    )
    .expect("write fake Paseo context executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo context metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions)
        .expect("fake Paseo context permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (replace_sender, replace_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-context",
            "call-context-a",
            "thread-a",
        )
        .await;
        replace_receiver.await.expect("replace context signal");
        for event in function_call_events_at(
            "response-context",
            "item-context-b",
            "call-context-b",
            1,
            "read_latest_reply",
            &json!({"session":"thread-b"}).to_string(),
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send context B read");
        }
        for event in function_call_events_at(
            "response-context",
            "item-context-c",
            "call-context-c",
            2,
            "read_latest_reply",
            &json!({"session":"thread-c"}).to_string(),
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send context C read");
        }

        let first = next_fake_realtime_json(&mut socket, "retired context A output").await;
        assert_eq!(
            first.pointer("/item/call_id").and_then(Value::as_str),
            Some("call-context-a"),
            "replacement output overtook the retired context"
        );
        let first_output: Value = serde_json::from_str(
            first
                .pointer("/item/output")
                .and_then(Value::as_str)
                .expect("retired context A output body"),
        )
        .expect("retired context A output JSON");
        assert_eq!(first_output["spoken_text"], reply_a.trim());
        assert_eq!(first_output["degraded"], true);

        let output = next_function_output(
            &mut socket,
            "call-context-b",
            "replacement context function output",
        )
        .await;
        assert_eq!(output["spoken_text"], provider_expected_b);
        assert!(
            output["spoken_text"]
                .as_str()
                .is_some_and(|summary| summary.chars().count() <= 2_400)
        );
        assert_eq!(output["degraded"], true);
        assert_eq!(output["respondable"], true);
        assert_eq!(output["session_id"], "thread-b");
        let output = next_function_output(
            &mut socket,
            "call-context-c",
            "final replacement context function output",
        )
        .await;
        assert_eq!(output["spoken_text"], provider_expected_c);
        assert!(
            output["spoken_text"]
                .as_str()
                .is_some_and(|summary| summary.chars().count() <= 2_400)
        );
        assert_eq!(output["degraded"], true);
        assert_eq!(output["respondable"], true);
        assert_eq!(output["session_id"], "thread-c");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-context","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete context response");
        loop {
            if next_fake_realtime_json(&mut socket, "context followup request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"response-context-followup"}}),
            json!({
                "type":"response.done",
                "response":{"id":"response-context-followup","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete context followup");
        }
        tokio::time::sleep(Duration::from_millis(1_600)).await;
        let unexpected = tokio::time::timeout(Duration::from_millis(200), socket.next()).await;
        if let Ok(Some(Ok(Message::Text(message)))) = unexpected {
            let event: Value = serde_json::from_str(&message).expect("late context provider JSON");
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output"),
                "a replacement call resolved more than once"
            );
            assert_ne!(event["type"], "response.create");
        }
    });

    let summary_base_url = format!("http://{summary_address}/v1");
    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summary_base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("replace summary context", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request context response");
    tokio::time::timeout(Duration::from_secs(3), first_started_receiver)
        .await
        .expect("first summary start timeout")
        .expect("first summary start signal");
    replace_sender
        .send(())
        .expect("request context replacement");

    let dashboard_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let context = loop {
        let remaining = dashboard_deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "current context dashboard timeout");
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("current context browser timeout")
            .expect("current context browser frame")
            .expect("current context browser result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("current context browser JSON");
        if frame["type"] == "dashboard_state"
            && frame["bound_context"]["thread_id"] == "thread-c"
            && frame["bound_context"]["summary_degraded"] == true
        {
            break frame["bound_context"].clone();
        }
    };
    assert_eq!(context["latest_summary"], expected_c_dashboard);
    assert_eq!(context["summary_degraded"], true);

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(request_count.load(Ordering::SeqCst), 1);
    let overwrite_deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    while tokio::time::Instant::now() < overwrite_deadline {
        let remaining = overwrite_deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("late context browser JSON");
        if frame["type"] == "dashboard_state" {
            assert_ne!(frame["bound_context"]["thread_id"], "thread-a");
            assert_ne!(frame["bound_context"]["thread_id"], "thread-b");
            assert_ne!(frame["bound_context"]["latest_summary"], "Stale summary A.");
        }
    }
    summary_server.abort();
}

#[tokio::test]
#[cfg(unix)]
async fn host_change_cancels_active_response_before_slow_host_probe() {
    use std::os::unix::fs::PermissionsExt as _;

    let mut summariser =
        DelayedSummariser::start("Too-late host summary.", Duration::from_secs(5)).await;
    let paseo_directory = tempfile::tempdir().expect("slow host Paseo directory");
    let marker = paseo_directory.path().join("slow-host-probe");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-slow-host-probe");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "if [ -e '{}' ]; then sleep 2; fi\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Session A\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf '%s\\n' '{}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            marker.display(),
            "Host probe ordering reply. ".repeat(40)
        ),
    )
    .expect("write slow host Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("slow host Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("slow host Paseo permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-slow-host-probe",
            "call-slow-host-probe",
            "thread-a",
        )
        .await;
        loop {
            let event = next_fake_realtime_json(&mut socket, "pre-probe host cancellation").await;
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-slow-host-probe");
                cancelled_sender
                    .send(())
                    .expect("signal pre-probe host cancellation");
                break;
            }
        }
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read before changing host", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request host-bound summary");
    summariser.wait_started().await;
    std::fs::write(&marker, "slow").expect("enable slow host probe");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("request slow host change");
    tokio::time::timeout(Duration::from_millis(700), cancelled_receiver)
        .await
        .expect("provider cancellation waited for slow host probe")
        .expect("provider cancellation signal");

    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn host_change_retires_slow_summary_and_clears_its_context_promptly() {
    let mut summariser =
        DelayedSummariser::start("Stale host summary.", Duration::from_millis(1_500)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Host-bound long coding-agent reply. ".repeat(40);
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-summary-host-change",
        &long_reply,
    );

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-host-summary",
            "call-host-summary",
            "thread-a",
        )
        .await;

        let mut saw_cancel = false;
        let mut saw_clear = false;
        while !saw_cancel || !saw_clear {
            let event = next_fake_realtime_json(&mut socket, "summary host change").await;
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output")
            );
            assert_ne!(event["type"], "response.create");
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-host-summary");
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-host-summary","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete host-change cancellation");
        cancelled_sender
            .send(())
            .expect("signal host-change cancellation");
        let deadline = tokio::time::Instant::now() + Duration::from_millis(1_800);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("late host provider JSON");
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output")
            );
            assert_ne!(event["type"], "response.create");
        }
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read host-bound reply", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request host-bound reply");
    summariser.wait_started().await;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(700);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change host during summary");
    let mut flushed = false;
    let mut selected_second = false;
    let mut context_cleared = false;
    while !flushed || !selected_second || !context_cleared {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "slow summary blocked host change");
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("host-change browser timeout")
            .expect("host-change browser frame")
            .expect("host-change browser result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("host-change browser JSON");
        flushed |= frame["type"] == "flush_audio";
        selected_second |= frame["type"] == "host_state" && frame["selected_host_id"] == "second";
        context_cleared |= frame["type"] == "dashboard_state" && frame["bound_context"].is_null();
        assert_ne!(frame.pointer("/phase").and_then(Value::as_str), Some("ok"));
    }
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    tokio::time::timeout(remaining, cancelled_receiver)
        .await
        .expect("host-change response cancellation timeout")
        .expect("host-change response cancellation signal");

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn disconnect_during_slow_summary_closes_provider_promptly() {
    let mut summariser =
        DelayedSummariser::start("Disconnected summary.", Duration::from_millis(1_500)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Disconnect-bound long coding-agent reply. ".repeat(40);
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-summary-disconnect",
        &long_reply,
    );

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (provider_closed_sender, provider_closed_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-disconnect",
            "call-disconnect",
            "thread-a",
        )
        .await;

        loop {
            match socket.next().await {
                Some(Ok(Message::Text(message))) => {
                    let event: Value =
                        serde_json::from_str(&message).expect("disconnect provider JSON");
                    assert_ne!(
                        event.pointer("/item/type").and_then(Value::as_str),
                        Some("function_call_output")
                    );
                    assert_ne!(event["type"], "response.create");
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("disconnect provider socket: {error}"),
            }
        }
        provider_closed_sender
            .send(())
            .expect("signal provider disconnect");
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("disconnect during summary", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request disconnect response");
    summariser.wait_started().await;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(700);
    tokio::time::timeout(
        deadline.saturating_duration_since(tokio::time::Instant::now()),
        runtime.browser.close(None),
    )
    .await
    .expect("browser disconnect timeout")
    .expect("browser disconnect");
    tokio::time::timeout(
        deadline.saturating_duration_since(tokio::time::Instant::now()),
        provider_closed_receiver,
    )
    .await
    .expect("provider disconnect timeout")
    .expect("provider disconnect signal");

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn provider_events_remain_responsive_during_slow_summary() {
    let mut summariser =
        DelayedSummariser::start("Provider-safe summary.", Duration::from_millis(1_500)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Provider-event long coding-agent reply. ".repeat(40);
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-provider-event", &long_reply);

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (emit_sender, emit_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-provider-event",
            "call-provider-event",
            "thread-a",
        )
        .await;
        emit_receiver.await.expect("emit provider event signal");
        for event in [
            json!({
                "type":"response.output_audio_transcript.delta",
                "response_id":"response-provider-event",
                "delta":"Provider event handled."
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-provider-event","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send provider event during summary");
        }

        let output = next_function_output(
            &mut socket,
            "call-provider-event",
            "provider-event summary output",
        )
        .await;
        assert_eq!(output["spoken_text"], "Provider-safe summary.");
        assert_eq!(output["degraded"], false);
        loop {
            if next_fake_realtime_json(&mut socket, "provider-event followup").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"response-provider-followup"}}),
            json!({
                "type":"response.done",
                "response":{"id":"response-provider-followup","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete provider-event followup");
        }
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("handle provider events", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request provider-event response");
    summariser.wait_started().await;

    emit_sender.send(()).expect("release provider events");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(700);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "slow summary blocked provider event");
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("provider-event browser timeout")
            .expect("provider-event browser frame")
            .expect("provider-event browser result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("provider-event browser JSON");
        if frame
            == json!({
                "type":"transcript_delta",
                "text":"Provider event handled."
            })
        {
            break;
        }
    }

    let dashboard_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let remaining = dashboard_deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "provider-event dashboard timeout");
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("provider-event dashboard browser timeout")
            .expect("provider-event dashboard frame")
            .expect("provider-event dashboard result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("provider-event dashboard JSON");
        if frame["type"] == "dashboard_state"
            && frame["bound_context"]["latest_summary"] == "Provider-safe summary."
        {
            break;
        }
    }

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
async fn unaccepted_response_created_events_do_not_retire_active_summary() {
    let mut summariser = DelayedSummariser::start(
        "Accepted summary remains current.",
        Duration::from_millis(700),
    )
    .await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Response creation correlation detail. ".repeat(40);
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-response-created-correlation",
        &long_reply,
    );
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (emit_sender, emit_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-created-current",
            "call-response-created-current",
            "thread-a",
        )
        .await;
        emit_receiver
            .await
            .expect("release unaccepted response.created events");
        for event in [
            json!({"type":"response.created","response":{}}),
            json!({
                "type":"response.created",
                "response":{"id":"x".repeat(129)}
            }),
            json!({
                "type":"response.created",
                "response":{"id":"response-created-current"}
            }),
            json!({
                "type":"response.created",
                "response":{"id":"response-created-unsolicited"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send unaccepted response.created event");
        }

        let output = next_function_output(
            &mut socket,
            "call-response-created-current",
            "current summary after unaccepted response.created",
        )
        .await;
        assert_eq!(output["spoken_text"], "Accepted summary remains current.");
        assert_eq!(output["degraded"], false);
        complete_tool_response(
            &mut socket,
            "response-created-current",
            "response-created-current-followup",
        )
        .await;
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("keep the accepted summary current", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request response-created correlation summary");
    summariser.wait_started().await;
    emit_sender
        .send(())
        .expect("release response.created adversaries");

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn later_completed_response_generation_retires_slow_summary_output() {
    let mut summariser =
        DelayedSummariser::start("Late summary R1.", Duration::from_millis(1_200)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Generation-bound long coding-agent reply. ".repeat(40);
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-summary-generation",
        &long_reply,
    );
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (r1_done_sender, r1_done_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-generation-r1",
            "call-generation-r1",
            "thread-a",
        )
        .await;
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-generation-r1","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete response R1 before its summary");
        tokio::time::sleep(Duration::from_millis(150)).await;
        r1_done_sender
            .send(())
            .expect("signal completed response R1");

        loop {
            let event = next_fake_realtime_json(&mut socket, "response R2 request").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("call-generation-r1"),
                "R1 output overtook the R2 request"
            );
            if event["type"] == "response.create" {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"response-generation-r2"}}),
            json!({
                "type":"response.done",
                "response":{"id":"response-generation-r2","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete response R2");
        }

        let deadline = tokio::time::Instant::now() + Duration::from_millis(1_400);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("late R1 provider JSON");
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("call-generation-r1"),
                "R1 emitted after R2 completed"
            );
            assert_ne!(
                event["type"], "response.create",
                "R1 triggered a late followup"
            );
        }
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read response R1", null).to_string().into(),
        ))
        .await
        .expect("request response R1");
    summariser.wait_started().await;
    tokio::time::timeout(Duration::from_secs(2), r1_done_receiver)
        .await
        .expect("response R1 completion timeout")
        .expect("response R1 completion signal");
    let summary_id = next_bound_summary_id(&mut runtime.browser, "response R1 context").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("complete response R2", summary_id)
                .to_string()
                .into(),
        ))
        .await
        .expect("request response R2");

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn typed_turn_barges_active_slow_summary_response() {
    let mut summariser =
        DelayedSummariser::start("Retired typed summary.", Duration::from_millis(1_200)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Typed-barge long coding-agent reply. ".repeat(40);
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-typed-barge", &long_reply);
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-typed-r1",
            "call-typed-r1",
            "thread-a",
        )
        .await;

        let mut saw_cancel = false;
        let mut saw_user_item = false;
        loop {
            let event = next_fake_realtime_json(&mut socket, "typed barge request").await;
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("call-typed-r1")
            );
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-typed-r1");
                saw_cancel = true;
                socket
                    .send(Message::Text(
                        json!({
                            "type":"response.done",
                            "response":{"id":"response-typed-r1","status":"cancelled"}
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("complete typed-cancelled R1");
            }
            saw_user_item |= event.pointer("/item/role").and_then(Value::as_str) == Some("user");
            if event["type"] == "response.create" {
                assert!(saw_cancel, "typed R2 was requested before canceling R1");
                assert!(saw_user_item, "typed R2 omitted its user item");
                break;
            }
        }
        cancelled_sender
            .send(())
            .expect("signal typed response cancellation");
        for event in [
            json!({"type":"response.created","response":{"id":"response-typed-r2"}}),
            json!({
                "type":"response.done",
                "response":{"id":"response-typed-r2","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete typed response R2");
        }

        let deadline = tokio::time::Instant::now() + Duration::from_millis(1_400);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("late typed provider JSON");
            assert_ne!(
                event.pointer("/item/call_id").and_then(Value::as_str),
                Some("call-typed-r1")
            );
            assert_ne!(event["type"], "response.create");
        }
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read typed R1", null).to_string().into(),
        ))
        .await
        .expect("request typed response R1");
    summariser.wait_started().await;
    let summary_id = next_bound_summary_id(&mut runtime.browser, "typed R1 context").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("replace with typed R2", summary_id)
                .to_string()
                .into(),
        ))
        .await
        .expect("barge with typed response R2");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(700);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "typed barge-in did not flush promptly"
        );
        let frame = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("typed barge browser timeout")
            .expect("typed barge browser frame")
            .expect("typed barge browser result");
        let Message::Text(frame) = frame else {
            continue;
        };
        if serde_json::from_str::<Value>(&frame).expect("typed barge browser JSON")["type"]
            == "flush_audio"
        {
            break;
        }
    }
    tokio::time::timeout(
        deadline.saturating_duration_since(tokio::time::Instant::now()),
        cancelled_receiver,
    )
    .await
    .expect("typed response cancellation timeout")
    .expect("typed response cancellation signal");

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn second_tool_call_receives_output_only_after_unresolved_summary_is_degraded() {
    let mut summariser =
        DelayedSummariser::start("Too-late summary.", Duration::from_millis(1_500)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = format!("Outcome first. {}Final blocker.", "detail ".repeat(500));
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-two-calls", &long_reply);
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (second_call_sender, second_call_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        begin_fake_summary_call(
            &mut socket,
            "response-two-calls",
            "call-summary-first",
            "thread-a",
        )
        .await;
        second_call_receiver
            .await
            .expect("release second tool call");
        for event in function_call_events_at(
            "response-two-calls",
            "item-list-second",
            "call-list-second",
            1,
            "list_sessions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send second tool call");
        }

        let first = next_fake_realtime_json(&mut socket, "first ordered tool output").await;
        assert_eq!(
            first.pointer("/item/type").and_then(Value::as_str),
            Some("function_call_output")
        );
        assert_eq!(
            first.pointer("/item/call_id").and_then(Value::as_str),
            Some("call-summary-first"),
            "second tool output overtook unresolved summary output"
        );
        let first_output: Value = serde_json::from_str(
            first
                .pointer("/item/output")
                .and_then(Value::as_str)
                .expect("degraded first output"),
        )
        .expect("degraded first output JSON");
        assert_eq!(first_output["degraded"], true);
        let spoken = first_output["spoken_text"]
            .as_str()
            .expect("degraded first spoken text");
        assert!(spoken.chars().count() <= 2_400);
        assert!(spoken.ends_with("Final blocker."));

        let second = next_function_output(
            &mut socket,
            "call-list-second",
            "second ordered tool output",
        )
        .await;
        assert_eq!(second["ok"], true);
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-two-calls","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete two-call response");
        loop {
            if next_fake_realtime_json(&mut socket, "two-call followup").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"response-two-calls-followup"}}),
            json!({
                "type":"response.done",
                "response":{"id":"response-two-calls-followup","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete two-call followup");
        }
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("run two provider calls", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request two-call response");
    summariser.wait_started().await;
    second_call_sender
        .send(())
        .expect("release second provider tool call");

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
async fn read_reply_publishes_bound_dashboard_before_summary_completes() {
    let mut summariser =
        DelayedSummariser::start("Delayed dashboard summary.", Duration::from_millis(1_500)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let long_reply = "Immediate bound dashboard context. ".repeat(60);
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-immediate-dashboard",
        &long_reply,
    );
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
        begin_fake_summary_call(
            &mut socket,
            "response-immediate-dashboard",
            "call-immediate-dashboard",
            "thread-a",
        )
        .await;
        let output = next_function_output(
            &mut socket,
            "call-immediate-dashboard",
            "delayed dashboard summary output",
        )
        .await;
        assert_eq!(output["spoken_text"], "Delayed dashboard summary.");
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("show the current reply", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request immediate dashboard response");
    summariser.wait_started().await;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(700);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "bound dashboard waited for summarisation"
        );
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("immediate dashboard timeout")
            .expect("immediate dashboard frame")
            .expect("immediate dashboard result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("immediate dashboard JSON");
        if frame["type"] == "dashboard_state" && frame["bound_context"].is_object() {
            assert_eq!(frame["bound_context"]["thread_id"], "thread-a");
            assert_eq!(frame["bound_context"]["summary_degraded"], false);
            assert!(
                frame["bound_context"]["latest_summary"]
                    .as_str()
                    .is_some_and(|summary| !summary.is_empty())
            );
            break;
        }
    }

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn summary_dashboard_snapshots_do_not_run_slow_paseo_list_probes() {
    use std::os::unix::fs::PermissionsExt as _;

    let summariser =
        DelayedSummariser::start("Probe-free summary.", Duration::from_millis(100)).await;
    let paseo_directory = tempfile::tempdir().expect("slow dashboard Paseo directory");
    let probe_armed = paseo_directory.path().join("dashboard-probe-armed");
    let probe_started = paseo_directory.path().join("dashboard-probe-started");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-slow-dashboard");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) if [ -e '{}' ]; then touch '{}'; sleep 3; fi; printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Session A\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) touch '{}'; printf '%s\\n' '{}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            probe_armed.display(),
            probe_started.display(),
            probe_armed.display(),
            "Dashboard probe isolation reply. ".repeat(50)
        ),
    )
    .expect("write slow dashboard Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("slow dashboard Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions)
        .expect("slow dashboard Paseo permissions");

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
        begin_fake_summary_call(
            &mut socket,
            "response-dashboard-snapshot",
            "call-dashboard-snapshot",
            "thread-a",
        )
        .await;
        socket
            .send(Message::Text(
                json!({
                    "type":"response.output_audio_transcript.delta",
                    "response_id":"response-dashboard-snapshot",
                    "delta":"Provider remained responsive."
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send provider event during dashboard snapshot");
        let output = next_function_output(
            &mut socket,
            "call-dashboard-snapshot",
            "summary output without dashboard probe",
        )
        .await;
        assert_eq!(output["spoken_text"], "Probe-free summary.");
        assert_eq!(output["degraded"], false);
        complete_tool_response(
            &mut socket,
            "response-dashboard-snapshot",
            "response-dashboard-snapshot-followup",
        )
        .await;
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read without dashboard probes", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request probe-free summary");

    let deadline = tokio::time::Instant::now() + Duration::from_millis(900);
    let mut saw_bound_context = false;
    let mut saw_provider_event = false;
    let mut saw_completed_summary = false;
    while !saw_bound_context || !saw_provider_event || !saw_completed_summary {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "summary lifecycle entered a slow Paseo dashboard probe"
        );
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("probe-free summary browser timeout")
            .expect("probe-free summary browser frame")
            .expect("probe-free summary browser result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("probe-free browser JSON");
        if frame["type"] == "dashboard_state" && frame["bound_context"]["thread_id"] == "thread-a" {
            saw_bound_context = true;
            saw_completed_summary |=
                frame["bound_context"]["latest_summary"] == "Probe-free summary.";
        }
        saw_provider_event |= frame
            == json!({
                "type":"transcript_delta",
                "text":"Provider remained responsive."
            });
    }

    fake_realtime.await.expect("fake Realtime task");
    assert!(probe_armed.exists());
    assert!(
        !probe_started.exists(),
        "summary lifecycle invoked a Paseo list probe"
    );
    assert_eq!(summariser.request_count(), 1);
}

#[tokio::test]
#[cfg(unix)]
async fn low_threshold_read_without_active_summary_emits_original_result() {
    let summariser =
        DelayedSummariser::start("Must not be requested.", Duration::from_millis(10)).await;
    let paseo_directory = tempfile::tempdir().expect("empty reply Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(&paseo_directory, "fake-paseo-empty-reply", "");
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
        begin_fake_summary_call(
            &mut socket,
            "response-empty-reply",
            "call-empty-reply",
            "thread-a",
        )
        .await;
        let output = next_function_output(
            &mut socket,
            "call-empty-reply",
            "bounded empty reply output",
        )
        .await;
        assert_eq!(output["ok"], true);
        assert_eq!(output["spoken_text"], "No readable reply yet.");
        assert_eq!(output["respondable"], false);
        assert!(output.get("error").is_none());
        complete_tool_response(
            &mut socket,
            "response-empty-reply",
            "response-empty-reply-followup",
        )
        .await;
    });

    let mut runtime = start_live_realtime_browser_with_summary(
        realtime_port,
        Some(&fake_paseo_path),
        Some(&summariser.base_url),
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read the empty reply", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request empty reply");

    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(summariser.request_count(), 0);
}

async fn next_function_output(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    call_id: &str,
    context: &str,
) -> Value {
    loop {
        let event = next_fake_realtime_json(socket, context).await;
        if event.pointer("/item/type").and_then(Value::as_str) != Some("function_call_output")
            || event.pointer("/item/call_id").and_then(Value::as_str) != Some(call_id)
        {
            continue;
        }
        let output = event
            .pointer("/item/output")
            .and_then(Value::as_str)
            .expect("function output body");
        return serde_json::from_str(output).expect("function output JSON");
    }
}

async fn complete_tool_response(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    response_id: &str,
    followup_id: &str,
) {
    socket
        .send(Message::Text(
            json!({
                "type":"response.done",
                "response":{"id":response_id,"status":"completed"}
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("complete tool response");
    loop {
        if next_fake_realtime_json(socket, "tool followup request").await["type"]
            == "response.create"
        {
            break;
        }
    }
    for event in [
        json!({"type":"response.created","response":{"id":followup_id}}),
        json!({
            "type":"response.done",
            "response":{"id":followup_id,"status":"completed"}
        }),
    ] {
        socket
            .send(Message::Text(event.to_string().into()))
            .await
            .expect("complete tool followup");
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn oversized_response_id_does_not_consume_pending_request() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let oversized_id = "r".repeat(129);
    let provider_id = oversized_id.clone();
    let (invalid_sender, invalid_receiver) = tokio::sync::oneshot::channel();
    let (valid_sender, valid_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "invalid response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":provider_id}}),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":provider_id,
                "transcript":"Invalid response output"
            }),
            json!({
                "type":"response.done",
                "response":{"id":provider_id,"status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send invalid response event");
        }
        invalid_sender.send(()).expect("signal invalid response");
        valid_receiver.await.expect("release valid response");
        for event in [
            json!({"type":"response.created","response":{"id":"response-valid"}}),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-valid",
                "transcript":"Valid response output"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-valid","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send valid response event");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("request invalid response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request invalid response");
    tokio::time::timeout(Duration::from_secs(2), invalid_receiver)
        .await
        .expect("invalid response timeout")
        .expect("invalid response signal");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("invalid response browser JSON");
        assert_ne!(frame["text"], "Invalid response output");
    }
    valid_sender.send(()).expect("release valid response");
    let mut transcript = None;
    while transcript.is_none() {
        let frame = next_browser_json(&mut runtime.browser, "valid response frame").await;
        if frame["type"] == "transcript_done" {
            transcript = frame["text"].as_str().map(str::to_owned);
        }
    }
    assert_eq!(transcript.as_deref(), Some("Valid response output"));
    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(oversized_id.len(), 129);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn oversized_call_item_and_tool_name_are_not_retained() {
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
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "validation response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-validation"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send validation response created");
        let oversized_call_id = "c".repeat(129);
        let oversized_item_id = "i".repeat(129);
        let oversized_name = "n".repeat(65);
        for (call_id, item_id, name) in [
            (oversized_call_id.as_str(), "item-call", "cancel_action"),
            ("call-item", oversized_item_id.as_str(), "cancel_action"),
            ("call-name", "item-name", oversized_name.as_str()),
            ("call-valid", "item-valid", "cancel_action"),
        ] {
            for event in [
                json!({
                    "type":"response.output_item.done",
                    "response_id":"response-validation",
                    "output_index":0,
                    "item":{
                        "id":item_id,
                        "type":"function_call",
                        "call_id":call_id,
                        "name":name
                    }
                }),
                json!({
                    "type":"response.function_call_arguments.done",
                    "response_id":"response-validation",
                    "item_id":item_id,
                    "call_id":call_id,
                    "name":name,
                    "arguments":"{}"
                }),
            ] {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send identifier validation event");
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut outputs = 0;
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("validation output JSON");
            outputs += usize::from(
                event.pointer("/item/type").and_then(Value::as_str) == Some("function_call_output"),
            );
        }
        assert_eq!(outputs, 1);
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("validate provider identifiers", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request validation response");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
async fn invalid_dictation_commit_ack_closes_before_later_valid_item() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-dictation-item", "Reply A");
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
        bind_fake_realtime_context(&mut socket, "dictation-item").await;
        loop {
            if next_fake_realtime_json(&mut socket, "dictation commit request").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        for item_id in ["i".repeat(129), "later-valid-item".to_owned()] {
            socket
                .send(Message::Text(
                    json!({
                        "type":"input_audio_buffer.committed",
                        "item_id":item_id,
                        "previous_item_id":null
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send invalid then valid commit acknowledgement");
        }
        loop {
            let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("invalid commit did not close provider socket");
            match message {
                Some(Ok(Message::Text(message))) => {
                    let event: Value =
                        serde_json::from_str(&message).expect("post-invalid commit JSON");
                    assert_ne!(event["type"], "conversation.item.delete");
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => break true,
                Some(Ok(_)) => {}
            }
        }
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    let operation_id = start_and_commit_bound_dictation(&mut runtime.browser, &summary_id, 1).await;
    loop {
        let message = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
            .await
            .expect("invalid commit browser close timeout");
        let Some(message) = message else { break };
        match message.expect("invalid commit browser result") {
            Message::Text(message) => {
                let frame: Value =
                    serde_json::from_str(&message).expect("invalid commit browser JSON");
                assert_ne!(frame["type"], "dictation_result");
                assert_ne!(frame["type"], "dictation_empty");
                assert_ne!(frame["type"], "dictation_failed");
                assert_ne!(frame["type"], "dictation_cancelled");
                assert_ne!(frame, json!({"type":"state","state":"ready"}));
                assert_ne!(frame["operation_id"], operation_id);
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider remained connected after invalid commit acknowledgement"
    );
}

#[tokio::test]
async fn duplicate_active_response_created_is_ignored() {
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
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "duplicate response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"response-duplicate"}}),
            json!({"type":"response.created","response":{"id":"response-duplicate"}}),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-duplicate",
                "transcript":"Duplicate ignored"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-duplicate","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send duplicate response event");
        }
        let unexpected = tokio::time::timeout(Duration::from_millis(200), socket.next()).await;
        if let Ok(Some(Ok(Message::Text(message)))) = unexpected {
            let event: Value = serde_json::from_str(&message).expect("duplicate output JSON");
            assert_ne!(event["type"], "response.cancel");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("duplicate response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request duplicate response");
    let mut transcript = None;
    let mut ready = false;
    while transcript.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "duplicate response frame").await;
        if frame["type"] == "transcript_done" {
            transcript = frame["text"].as_str().map(str::to_owned);
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    assert_eq!(transcript.as_deref(), Some("Duplicate ignored"));
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn barge_in_cancels_exact_active_response_and_discards_queued_followup() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (followup_sender, followup_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "initial response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-active"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send active response");
        for event in function_call_events(
            "response-active",
            "queued-followup-item",
            "queued-followup-call",
            "cancel_action",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send queued followup tool call");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "queued followup output").await;
            assert_ne!(event["type"], "response.create");
            if event.pointer("/item/type").and_then(Value::as_str) == Some("function_call_output") {
                break;
            }
        }
        followup_sender.send(()).expect("signal queued followup");

        let mut saw_clear = false;
        let mut saw_cancel = false;
        while !saw_clear || !saw_cancel {
            let event = next_fake_realtime_json(&mut socket, "barge-in request").await;
            if event["type"] == "response.cancel" {
                assert_eq!(
                    event,
                    json!({
                        "type":"response.cancel",
                        "response_id":"response-active"
                    })
                );
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-active","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send cancelled response completion");
        let quiet = tokio::time::timeout(Duration::from_millis(200), socket.next()).await;
        if let Ok(Some(Ok(Message::Text(message)))) = quiet {
            let event: Value = serde_json::from_str(&message).expect("post-cancel event JSON");
            assert_ne!(event["type"], "response.create");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("start response", null).to_string().into(),
        ))
        .await
        .expect("start response");
    tokio::time::timeout(Duration::from_secs(2), followup_receiver)
        .await
        .expect("queued followup timeout")
        .expect("queued followup signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge in");
    loop {
        let frame = next_browser_json(&mut runtime.browser, "barge-in browser frame").await;
        if frame["type"] == "flush_audio" {
            break;
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn canceled_response_cannot_emit_late_output_or_tool_calls() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (created_sender, created_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-canceled"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send response created");
        created_sender.send(()).expect("signal response created");
        let mut saw_cancel = false;
        let mut saw_clear = false;
        while !saw_cancel || !saw_clear {
            let event = next_fake_realtime_json(&mut socket, "cancellation request").await;
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-canceled");
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        for event in [
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-canceled",
                "delta":"bGF0ZSBhdWRpbw=="
            }),
            json!({
                "type":"response.output_audio_transcript.delta",
                "response_id":"response-canceled",
                "delta":"late transcript"
            }),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-canceled",
                "transcript":"late transcript"
            }),
            json!({
                "type":"response.output_item.added",
                "response_id":"response-canceled",
                "item":{
                    "id":"late-item",
                    "type":"function_call",
                    "call_id":"late-call",
                    "name":"cancel_action"
                }
            }),
            json!({
                "type":"response.output_item.done",
                "response_id":"response-canceled",
                "item":{
                    "id":"late-item",
                    "type":"function_call",
                    "call_id":"late-call",
                    "name":"cancel_action"
                }
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "response_id":"response-canceled",
                "item_id":"late-item",
                "call_id":"late-call",
                "name":"cancel_action",
                "arguments":"{}"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-canceled","status":"cancelled"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send canceled response replay");
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("late output JSON");
            assert_ne!(event["type"], "response.create");
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output")
            );
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("cancel this response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("start response");
    tokio::time::timeout(Duration::from_secs(2), created_receiver)
        .await
        .expect("response created timeout")
        .expect("response created signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge in");
    loop {
        if next_browser_json(&mut runtime.browser, "cancellation browser frame").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(message))) = tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        match message {
            Message::Binary(_) => panic!("canceled response emitted audio"),
            Message::Text(message) => {
                let frame: Value = serde_json::from_str(&message).expect("late browser frame JSON");
                assert!(!matches!(
                    frame["type"].as_str(),
                    Some(
                        "transcript_delta"
                            | "transcript_done"
                            | "tool"
                            | "proposal"
                            | "dashboard_state"
                    )
                ));
                assert_ne!(frame, json!({"type":"state","state":"ready"}));
            }
            _ => {}
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn canceled_done_cannot_advance_a_newer_response_or_its_followup() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (created_sender, created_receiver) = tokio::sync::oneshot::channel();
    let (cancel_sender, cancel_receiver) = tokio::sync::oneshot::channel();
    let (old_done_sender, old_done_receiver) = tokio::sync::oneshot::channel();
    let (continue_sender, continue_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "old response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-old"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send old response created");
        created_sender
            .send(())
            .expect("signal old response created");
        let mut saw_cancel = false;
        let mut saw_clear = false;
        while !saw_cancel || !saw_clear {
            let event = next_fake_realtime_json(&mut socket, "old response cancellation").await;
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-old");
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
        }
        cancel_sender.send(()).expect("signal old response cancel");
        loop {
            let event = next_fake_realtime_json(&mut socket, "new response request").await;
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "new-response-audio").await;
            } else if event["type"] == "response.create" {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-new"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send new response created");
        for event in function_call_events(
            "response-new",
            "new-followup-item",
            "new-followup-call",
            "cancel_action",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("queue new response followup");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "new response tool output").await;
            assert_ne!(event["type"], "response.create");
            if event.pointer("/item/type").and_then(Value::as_str) == Some("function_call_output") {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-old","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("replay old canceled done");
        old_done_sender.send(()).expect("signal old canceled done");
        continue_receiver
            .await
            .expect("release current canceled done");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-new","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("cancel current response");
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("canceled done output JSON");
            assert_ne!(event["type"], "response.create");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("start old response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("start old response");
    tokio::time::timeout(Duration::from_secs(2), created_receiver)
        .await
        .expect("old response created timeout")
        .expect("old response created signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge into old response");
    loop {
        if next_browser_json(&mut runtime.browser, "old cancellation frame").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(2), cancel_receiver)
        .await
        .expect("old response cancel timeout")
        .expect("old response cancel signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("request new response");
    tokio::time::timeout(Duration::from_secs(2), old_done_receiver)
        .await
        .expect("old canceled done timeout")
        .expect("old canceled done signal");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("old done browser frame JSON");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
    }
    continue_sender
        .send(())
        .expect("release current canceled done");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn barge_in_before_response_created_cancels_the_late_response() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (requested_sender, requested_receiver) = tokio::sync::oneshot::channel();
    let (replay_sender, replay_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "pending response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        requested_sender
            .send(())
            .expect("signal pending response request");
        loop {
            if next_fake_realtime_json(&mut socket, "pending barge-in clear").await["type"]
                == "input_audio_buffer.clear"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-late"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("release late response created");
        loop {
            let event = next_fake_realtime_json(&mut socket, "late response cancellation").await;
            if event["type"] == "response.cancel" {
                assert_eq!(
                    event,
                    json!({
                        "type":"response.cancel",
                        "response_id":"response-late"
                    })
                );
                break;
            }
        }
        for event in [
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-late",
                "delta":"bGF0ZQ=="
            }),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-late",
                "transcript":"late response"
            }),
            json!({
                "type":"response.output_item.done",
                "response_id":"response-late",
                "item":{
                    "id":"late-created-item",
                    "type":"function_call",
                    "call_id":"late-created-call",
                    "name":"cancel_action"
                }
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "response_id":"response-late",
                "item_id":"late-created-item",
                "call_id":"late-created-call",
                "name":"cancel_action",
                "arguments":"{}"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-late","status":"cancelled"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send late-created response replay");
        }
        replay_sender.send(()).expect("signal late-created replay");
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("late-created output JSON");
            assert_ne!(event["type"], "response.create");
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output")
            );
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("create a delayed response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request delayed response");
    tokio::time::timeout(Duration::from_secs(2), requested_receiver)
        .await
        .expect("pending response request timeout")
        .expect("pending response request signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge into pending response");
    tokio::time::timeout(Duration::from_secs(3), replay_receiver)
        .await
        .expect("late-created replay timeout")
        .expect("late-created replay signal");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(message))) = tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        match message {
            Message::Binary(_) => panic!("late-created response emitted audio"),
            Message::Text(message) => {
                let frame: Value =
                    serde_json::from_str(&message).expect("late-created browser frame JSON");
                assert!(!matches!(
                    frame["type"].as_str(),
                    Some("transcript_delta" | "transcript_done" | "tool" | "proposal")
                ));
                assert_ne!(frame, json!({"type":"state","state":"responding"}));
                assert_ne!(frame, json!({"type":"state","state":"ready"}));
            }
            _ => {}
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn retired_created_replay_cannot_claim_a_new_pending_response() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (requested_sender, requested_receiver) = tokio::sync::oneshot::channel();
    let (cancelled_sender, cancelled_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "first response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        requested_sender.send(()).expect("signal first request");
        loop {
            if next_fake_realtime_json(&mut socket, "first response clear").await["type"]
                == "input_audio_buffer.clear"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-replayed"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send first late response created");
        loop {
            let event = next_fake_realtime_json(&mut socket, "first late cancellation").await;
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-replayed");
                break;
            }
        }
        cancelled_sender
            .send(())
            .expect("signal first late cancellation");
        loop {
            let event = next_fake_realtime_json(&mut socket, "new response request").await;
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "retired-response-audio").await;
            } else if event["type"] == "response.create" {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-replayed"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("replay retired response created");
        for event in [
            json!({"type":"response.created","response":{"id":"response-new"}}),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-new",
                "transcript":"New response"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-new","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send new response event");
        }
        let unexpected = tokio::time::timeout(Duration::from_millis(200), socket.next()).await;
        if let Ok(Some(Ok(Message::Text(message)))) = unexpected {
            let event: Value = serde_json::from_str(&message).expect("retired replay output JSON");
            assert_ne!(event["type"], "response.cancel");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("request first response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request first response");
    tokio::time::timeout(Duration::from_secs(2), requested_receiver)
        .await
        .expect("first request timeout")
        .expect("first request signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge into first request");
    tokio::time::timeout(Duration::from_secs(2), cancelled_receiver)
        .await
        .expect("first cancellation timeout")
        .expect("first cancellation signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("request new response");
    let mut transcript = None;
    let mut ready = false;
    while transcript.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "new response browser frame").await;
        if frame["type"] == "transcript_done" {
            transcript = frame["text"].as_str().map(str::to_owned);
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    assert_eq!(transcript.as_deref(), Some("New response"));
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn response_id_cap_fails_closed_without_eviction() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (created_sender, mut created_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (cancelled_sender, mut cancelled_receiver) = tokio::sync::mpsc::unbounded_channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        for index in 0..64 {
            loop {
                if next_fake_realtime_json(&mut socket, "churn response request").await["type"]
                    == "response.create"
                {
                    break;
                }
            }
            let response_id = format!("response-tombstone-{index}");
            socket
                .send(Message::Text(
                    json!({"type":"response.created","response":{"id":response_id}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("send churn response created");
            created_sender
                .send(index)
                .expect("signal churn response created");
            let mut saw_cancel = false;
            let mut saw_clear = false;
            while !saw_cancel || !saw_clear {
                let event = next_fake_realtime_json(&mut socket, "churn cancellation").await;
                if event["type"] == "response.cancel" {
                    assert_eq!(event["response_id"], response_id);
                    saw_cancel = true;
                }
                saw_clear |= event["type"] == "input_audio_buffer.clear";
            }
            cancelled_sender
                .send(index)
                .expect("signal churn cancellation");
        }
        assert_eq!(
            next_fake_realtime_json(&mut socket, "final churn recording abort").await["type"],
            "input_audio_buffer.clear"
        );
        let closed = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("response cap provider close timeout");
        assert!(matches!(
            closed,
            Some(Ok(Message::Close(_)) | Err(_)) | None
        ));

        let (stream, _) = realtime_listener
            .accept()
            .await
            .expect("fresh Realtime accept");
        let mut socket = accept_async(stream)
            .await
            .expect("fresh Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "fresh session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "fresh response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"fresh-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"fresh-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete fresh response");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    for index in 0..64 {
        let recording_id = u64::try_from(index + 1).expect("bounded recording ID");
        runtime
            .browser
            .send(Message::Text(
                test_text_turn!(format!("churn response {index}"), null)
                    .to_string()
                    .into(),
            ))
            .await
            .expect("request churn response");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), created_receiver.recv())
                .await
                .expect("churn created timeout")
                .expect("churn created signal"),
            index
        );
        runtime
            .browser
            .send(Message::Text(
                json!({"type":"ptt_start","recording_id":recording_id,"summary_id":null})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("cancel churn response");
        loop {
            if next_browser_json(&mut runtime.browser, "churn browser frame").await["type"]
                == "flush_audio"
            {
                break;
            }
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), cancelled_receiver.recv())
                .await
                .expect("churn cancellation timeout")
                .expect("churn cancellation signal"),
            index
        );
        runtime
            .browser
            .send(Message::Text(
                json!({"type":"ptt_abort","recording_id":recording_id})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("finish churn recording");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "churn terminal flush").await,
            json!({"type":"flush_audio"})
        );
    }
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("exceed response ID cap", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request overflow response");
    assert_eq!(
        next_browser_json_including_text_ack(&mut runtime.browser, "response cap exhaustion").await,
        json!({
            "type":"error",
            "message":"Realtime response capacity exhausted. Reconnect required."
        })
    );
    let app_port = browser_server_port(&runtime.browser);
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("response cap browser close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));

    let (mut reconnected, _) = connect_async(format!("ws://127.0.0.1:{app_port}/ws"))
        .await
        .expect("fresh browser reconnect");
    initialize_browser_protocol(&mut reconnected, "fresh reconnect frame").await;
    let fresh_turn = test_text_turn!("fresh response after reconnect", null);
    let fresh_turn_id = test_text_turn_id(&fresh_turn);
    reconnected
        .send(Message::Text(fresh_turn.to_string().into()))
        .await
        .expect("send fresh reconnect turn");
    assert_eq!(
        next_browser_json_including_text_ack(&mut reconnected, "fresh reconnect acceptance").await,
        json!({"type":"text_turn_accepted","turn_id":fresh_turn_id})
    );
    loop {
        if next_browser_json(&mut reconnected, "fresh reconnect response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn unsolicited_response_id_overflow_closes_both_sockets() {
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "response overflow session update").await["type"],
            "session.update"
        );
        for index in 0..64 {
            let response_id = format!("unsolicited-response-{index}");
            socket
                .send(Message::Text(
                    json!({"type":"response.created","response":{"id":response_id}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("send unsolicited response ID");
            let cancelled =
                next_fake_realtime_json(&mut socket, "unsolicited response cancel").await;
            assert_eq!(cancelled["type"], "response.cancel");
            assert_eq!(cancelled["response_id"], response_id);
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.created",
                    "response":{"id":"unsolicited-response-overflow"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send overflowing response ID");
        let closed = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("response overflow provider close timeout");
        assert!(matches!(
            closed,
            Some(Ok(Message::Close(_)) | Err(_)) | None
        ));
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    assert_eq!(
        next_browser_json(
            &mut runtime.browser,
            "provider response capacity exhaustion"
        )
        .await,
        json!({
            "type":"error",
            "message":"Realtime response capacity exhausted. Reconnect required."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("response overflow browser close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn call_id_is_single_use_across_responses() {
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
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "first call response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-call-first"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send first call response created");
        for event in function_call_events(
            "response-call-first",
            "item-call-first",
            "call-single-use",
            "cancel_action",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send first function call event");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "first function output").await;
            if event.pointer("/item/type").and_then(Value::as_str) == Some("function_call_output") {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-call-first","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete first call response");
        loop {
            if next_fake_realtime_json(&mut socket, "second call response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-call-second"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send second call response created");
        for event in function_call_events(
            "response-call-second",
            "item-call-second",
            "call-single-use",
            "cancel_action",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("replay call ID in second response");
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("reused call output JSON");
            assert_ne!(
                event.pointer("/item/type").and_then(Value::as_str),
                Some("function_call_output")
            );
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("use one call ID once", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request call ID response");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn function_arguments_require_the_exact_prior_output_item() {
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
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "function join response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-function-join"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send function join response created");
        for event in [
            function_call_events(
                "response-function-join",
                "item-original",
                "call-item-mismatch",
                "cancel_action",
                "{}",
            )[0]
            .clone(),
            json!({
                "type":"response.function_call_arguments.done",
                "response_id":"response-function-join",
                "item_id":"item-other",
                "call_id":"call-item-mismatch",
                "name":"confirm_action",
                "arguments":"{}"
            }),
            function_call_events_at(
                "response-function-join",
                "item-name",
                "call-name",
                1,
                "cancel_action",
                "{}",
            )[0]
            .clone(),
            json!({
                "type":"response.function_call_arguments.done",
                "response_id":"response-function-join",
                "item_id":"item-name",
                "call_id":"call-name",
                "name":"confirm_action",
                "arguments":"{}"
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "response_id":"response-function-join",
                "item_id":"item-unbound",
                "call_id":"call-unbound",
                "name":"cancel_action",
                "arguments":"{}"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send function join event");
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(400);
        let mut output_call_ids = Vec::new();
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("function join output JSON");
            if event.pointer("/item/type").and_then(Value::as_str) == Some("function_call_output") {
                output_call_ids.push(
                    event
                        .pointer("/item/call_id")
                        .and_then(Value::as_str)
                        .expect("function output call ID")
                        .to_owned(),
                );
            }
        }
        assert!(output_call_ids.is_empty());
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("join function events", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request function join response");
    fake_realtime.await.expect("fake Realtime task");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    let mut called_tools = Vec::new();
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("function join browser JSON");
        if frame["type"] == "tool" && frame["phase"] == "call" {
            called_tools.push(frame["name"].as_str().expect("called tool name").to_owned());
        }
    }
    assert!(called_tools.is_empty());
}

#[tokio::test]
async fn correlated_response_error_closes_before_a_late_success_can_be_accepted() {
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
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        let response_create = loop {
            let event = next_fake_realtime_json(&mut socket, "correlated response create").await;
            if event["type"] == "response.create" {
                break event;
            }
        };
        let event_id = response_create["event_id"]
            .as_str()
            .expect("response.create event ID");
        socket
            .send(Message::Text(
                json!({
                    "type":"error",
                    "error":{
                        "type":"invalid_request_error",
                        "message":"response.create failed",
                        "event_id":event_id
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send correlated response error");
        for event in [
            json!({"type":"response.created","response":{"id":"late-old-response"}}),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"late-old-response",
                "transcript":"must not be accepted"
            }),
        ] {
            if socket
                .send(Message::Text(event.to_string().into()))
                .await
                .is_err()
            {
                return true;
            }
        }
        tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .is_ok()
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("request a response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request correlated response");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "correlated response error").await,
        json!({
            "type":"error",
            "message":"Realtime rejected the response request. Reconnecting safely."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("correlated response error close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider socket remained open after a correlated response error"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn response_created_without_id_preserves_its_pending_request() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (missing_sender, missing_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "missing-ID response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send response.created without ID");
        missing_sender.send(()).expect("signal missing response ID");
        for event in [
            json!({"type":"response.created","response":{"id":"response-after-missing"}}),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-after-missing",
                "transcript":"Valid after missing ID"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-after-missing","status":"completed"}
            }),
            json!({"type":"response.created","response":{"id":"response-unsolicited"}}),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send post-missing response event");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "unsolicited cancellation").await;
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-unsolicited");
                break;
            }
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("request missing-ID response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request missing-ID response");
    tokio::time::timeout(Duration::from_secs(2), missing_receiver)
        .await
        .expect("missing response ID timeout")
        .expect("missing response ID signal");
    let mut transcript = None;
    while transcript.is_none() {
        let frame = next_browser_json(&mut runtime.browser, "post-missing browser frame").await;
        if frame["type"] == "transcript_done" {
            transcript = frame["text"].as_str().map(str::to_owned);
        }
    }
    assert_eq!(transcript.as_deref(), Some("Valid after missing ID"));
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn only_completed_terminal_status_can_trigger_a_followup() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (terminal_sender, mut terminal_receiver) = tokio::sync::mpsc::unbounded_channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        for (index, status) in [
            Some("failed"),
            Some("incomplete"),
            None,
            Some("in_progress"),
            Some("future_status"),
            Some("cancelled"),
        ]
        .into_iter()
        .enumerate()
        {
            loop {
                if next_fake_realtime_json(&mut socket, "terminal response request").await["type"]
                    == "response.create"
                {
                    break;
                }
            }
            let response_id = format!("response-terminal-{index}");
            socket
                .send(Message::Text(
                    json!({"type":"response.created","response":{"id":response_id}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("send terminal response created");
            for event in function_call_events(
                &response_id,
                &format!("item-terminal-{index}"),
                &format!("call-terminal-{index}"),
                "cancel_action",
                "{}",
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send terminal function call");
            }
            loop {
                let event = next_fake_realtime_json(&mut socket, "terminal function output").await;
                if event.pointer("/item/type").and_then(Value::as_str)
                    == Some("function_call_output")
                {
                    break;
                }
            }
            let mut response = json!({"id":response_id});
            if let Some(status) = status {
                response["status"] = Value::String(status.to_owned());
            }
            socket
                .send(Message::Text(
                    json!({"type":"response.done","response":response})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("send non-completed terminal");
            let unexpected = tokio::time::timeout(Duration::from_millis(120), socket.next()).await;
            if let Ok(Some(Ok(Message::Text(message)))) = unexpected {
                let event: Value = serde_json::from_str(&message).expect("terminal output JSON");
                assert_ne!(event["type"], "response.create");
            }
            terminal_sender
                .send(index)
                .expect("signal non-completed terminal");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    for index in 0..6 {
        runtime
            .browser
            .send(Message::Text(
                test_text_turn!(format!("terminal status {index}"), null)
                    .to_string()
                    .into(),
            ))
            .await
            .expect("request terminal response");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), terminal_receiver.recv())
                .await
                .expect("terminal signal timeout")
                .expect("terminal signal"),
            index
        );
        loop {
            if next_browser_json(&mut runtime.browser, "terminal browser frame").await
                == json!({"type":"state","state":"ready"})
            {
                break;
            }
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn explicit_cancellation_allows_barge_in_while_provider_confirmation_is_blocked() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-race");
    let log_marker = paseo_directory.path().join("logs-started");
    let write_marker = paseo_directory.path().join("write-dispatched");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf 'started\\n' >> \"{}\"; sleep 0.6; printf 'Reply to answer\\n' ;;\n",
                "  send) printf 'written\\n' > \"{}\"; printf '%s\\n' '{{\"id\":\"message-race\"}}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            log_marker.display(),
            write_marker.display(),
        ),
    )
    .expect("write fake Paseo race executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo race metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo race permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let (proposal_sender, proposal_receiver) = tokio::sync::oneshot::channel();
    let (race_sender, race_receiver) = tokio::sync::oneshot::channel();
    let provider_log_marker = log_marker.clone();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "proposal-race").await;
        bound_sender.send(()).expect("signal proposal context");
        loop {
            if next_fake_realtime_json(&mut socket, "proposal response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-proposal"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send proposal response created");
        for event in function_call_events(
            "response-proposal",
            "item-propose",
            "call-propose",
            "send_message",
            &json!({"text":"write once"}).to_string(),
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send proposal call");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "proposal output").await;
            if event.pointer("/item/call_id").and_then(Value::as_str) != Some("call-propose") {
                continue;
            }
            let output = event
                .pointer("/item/output")
                .and_then(Value::as_str)
                .expect("proposal output body");
            let output: Value = serde_json::from_str(output).expect("proposal output JSON");
            assert!(output.get("proposal_token").is_none());
            break;
        }
        proposal_sender.send(()).expect("signal proposal ready");
        let mut saw_cancel = false;
        let mut saw_clear = false;
        let mut saw_commit = false;
        let mut saw_create = false;
        while !saw_cancel || !saw_clear || !saw_commit || !saw_create {
            let event = next_fake_realtime_json(&mut socket, "later confirmation turn").await;
            saw_cancel |= event["type"] == "response.cancel";
            saw_clear |= event["type"] == "input_audio_buffer.clear";
            saw_commit |= event["type"] == "input_audio_buffer.commit";
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "confirmation-race-audio").await;
            }
            saw_create |= event["type"] == "response.create";
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-confirm-race"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send confirmation race response");
        socket
            .send(Message::Text(
                json!({"type":"test.noop"}).to_string().into(),
            ))
            .await
            .expect("shift unbiased provider selection");
        socket
            .send(Message::Text(
                json!({"type":"test.noop"}).to_string().into(),
            ))
            .await
            .expect("shift unbiased provider selection again");
        for event in function_call_events_at(
            "response-confirm-race",
            "item-race-read",
            "call-race-read",
            0,
            "read_latest_reply",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send slow race read");
        }
        let confirm_item = function_call_events_at(
            "response-confirm-race",
            "item-confirm-race",
            "call-confirm-race",
            1,
            "confirm_action",
            "{}",
        )[0]
        .clone();
        socket
            .send(Message::Text(confirm_item.to_string().into()))
            .await
            .expect("send confirmation output item");
        let mut second_read_started = false;
        for _ in 0..100 {
            let starts = std::fs::read_to_string(&provider_log_marker)
                .unwrap_or_default()
                .lines()
                .count();
            if starts >= 2 {
                second_read_started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(second_read_started, "slow race read started");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.function_call_arguments.done",
                    "response_id":"response-confirm-race",
                    "item_id":"item-confirm-race",
                    "call_id":"call-confirm-race",
                    "name":"confirm_action",
                    "arguments":json!({"token":"provider-supplied-token"}).to_string()
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("queue write-capable confirmation");
        race_sender.send(()).expect("signal ready race");
        let mut race_cancelled = false;
        let mut race_cleared = false;
        while !race_cancelled || !race_cleared {
            let event = next_fake_realtime_json(&mut socket, "race cancellation").await;
            race_cancelled |= event["type"] == "response.cancel";
            race_cleared |= event["type"] == "input_audio_buffer.clear";
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind proposal context");
    bound_receiver.await.expect("proposal context bound");
    let mut summary_id = None;
    let mut ready = false;
    while summary_id.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "proposal context frame").await;
        if let Some(id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_id = Some(id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_id = summary_id.expect("proposal summary ID");
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("prepare a response", summary_id)
                .to_string()
                .into(),
        ))
        .await
        .expect("request proposal response");
    tokio::time::timeout(Duration::from_secs(5), proposal_receiver)
        .await
        .expect("proposal timeout")
        .expect("proposal signal");
    let proposal_handle = loop {
        let frame = next_browser_json(&mut runtime.browser, "proposal presentation frame").await;
        if frame["type"] == "proposal"
            && frame["echo"].is_string()
            && let Some(handle) = frame["handle"].as_str()
        {
            break handle.to_owned();
        }
    };
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"cancel_proposal","handle":proposal_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel proposal explicitly");
    loop {
        if next_browser_json(&mut runtime.browser, "explicit proposal cancellation").await
            == json!({"type":"proposal","echo":null})
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel proposal response");
    loop {
        if next_browser_json(&mut runtime.browser, "proposal cancellation frame").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("create later confirmation turn");
    tokio::time::timeout(Duration::from_secs(5), race_receiver)
        .await
        .expect("ready race timeout")
        .expect("ready race signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("barge into confirmation race");
    tokio::time::sleep(Duration::from_millis(700)).await;
    loop {
        if next_browser_json(&mut runtime.browser, "race browser frame").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    fake_realtime.await.expect("fake Realtime task");
    assert!(
        !write_marker.exists(),
        "injected provider confirmation dispatched a write"
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn stale_real_handle_and_provider_confirmation_cannot_execute_replacement() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory
        .path()
        .join("fake-paseo-confirmation-origin");
    let write_marker = paseo_directory.path().join("writes");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf '%s\\n' 'Reply to answer' ;;\n",
                "  send) printf '%s\\n' \"$*\" >> \"{}\"; printf '%s\\n' '{{\"messageId\":\"message-browser-confirmed\"}}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            write_marker.display(),
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let (proposal_sender, proposal_receiver) = tokio::sync::oneshot::channel();
    let (rejection_sender, rejection_receiver) = tokio::sync::oneshot::channel();
    let (inspect_sender, inspect_receiver) = tokio::sync::oneshot::channel();
    let (stale_sender, stale_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let update = next_fake_realtime_json(&mut socket, "session update").await;
        assert!(
            update["session"]["tools"]
                .as_array()
                .expect("session tools")
                .iter()
                .all(|tool| tool["name"] != "confirm_action")
        );
        bind_fake_realtime_context(&mut socket, "confirmation-origin").await;
        bound_sender.send(()).expect("signal confirmation context");

        loop {
            if next_fake_realtime_json(&mut socket, "proposal response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-proposal-origin"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send proposal response created");
        let mut proposal_recording_clears = 0;
        for (output_index, (suffix, text)) in [
            ("first", "first response"),
            ("second", "browser-only response"),
        ]
        .into_iter()
        .enumerate()
        {
            let item_id = format!("item-origin-propose-{suffix}");
            let call_id = format!("call-origin-propose-{suffix}");
            for event in function_call_events_at(
                "response-proposal-origin",
                &item_id,
                &call_id,
                output_index as u64,
                "send_message",
                &json!({"text":text}).to_string(),
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send proposal call");
            }
            let proposal = loop {
                let event = next_fake_realtime_json(&mut socket, "proposal function output").await;
                proposal_recording_clears +=
                    usize::from(event["type"] == "input_audio_buffer.clear");
                if event.pointer("/item/type").and_then(Value::as_str)
                    != Some("function_call_output")
                    || event.pointer("/item/call_id").and_then(Value::as_str)
                        != Some(call_id.as_str())
                {
                    continue;
                }
                let output = event
                    .pointer("/item/output")
                    .and_then(Value::as_str)
                    .expect("proposal function output body");
                break serde_json::from_str::<Value>(output)
                    .expect("proposal function output JSON");
            };
            assert_eq!(proposal["ok"], true);
            assert!(
                proposal.get("proposal_token").is_none(),
                "provider received the safety token"
            );
        }
        assert_eq!(proposal_recording_clears, 1);
        for (offset, (suffix, name, arguments)) in [
            ("read-only", "list_sessions", "{}"),
            ("malformed", "send_message", "{}"),
        ]
        .into_iter()
        .enumerate()
        {
            let item_id = format!("item-origin-{suffix}");
            let call_id = format!("call-origin-{suffix}");
            for event in function_call_events_at(
                "response-proposal-origin",
                &item_id,
                &call_id,
                offset as u64 + 2,
                name,
                arguments,
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send non-terminal proposal tool call");
            }
            let _ =
                next_function_output(&mut socket, &call_id, "non-terminal proposal tool output")
                    .await;
        }
        for event in function_call_events_at(
            "response-proposal-origin",
            "item-provider-confirmation",
            "call-provider-confirmation",
            4,
            "confirm_action",
            &json!({"token":"provider-supplied-token"}).to_string(),
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("inject provider confirmation");
        }
        let rejection = next_function_output(
            &mut socket,
            "call-provider-confirmation",
            "provider confirmation output",
        )
        .await;
        complete_tool_response(
            &mut socket,
            "response-proposal-origin",
            "response-provider-confirmation-followup",
        )
        .await;
        proposal_sender.send(()).expect("signal proposals ready");
        rejection_sender
            .send(rejection)
            .expect("signal provider rejection");
        inspect_receiver
            .await
            .expect("inspect stale proposal release");
        let mut stale_events = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("stale proposal event JSON");
            stale_events.push(
                event["type"]
                    .as_str()
                    .expect("stale proposal event type")
                    .to_owned(),
            );
        }
        stale_sender
            .send(stale_events)
            .expect("report stale proposal events");
        finish_receiver
            .await
            .expect("wait for browser confirmation");
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind confirmation context");
    bound_receiver.await.expect("confirmation context bound");
    let mut summary_id = None;
    let mut ready = false;
    while summary_id.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "confirmation context frame").await;
        if let Some(id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_id = Some(id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_id = summary_id.expect("confirmation summary ID");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start live recording before proposal");
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"proposal-bound audio").into()))
        .await
        .expect("send audio before proposal");
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("prepare a response", summary_id)
                .to_string()
                .into(),
        ))
        .await
        .expect("request proposal");
    tokio::time::timeout(Duration::from_secs(5), proposal_receiver)
        .await
        .expect("proposal timeout")
        .expect("proposal signal");
    let rejection = tokio::time::timeout(Duration::from_secs(5), rejection_receiver)
        .await
        .expect("provider rejection timeout")
        .expect("provider rejection signal");
    assert_eq!(rejection["error"], "confirmation_requires_browser");
    let mut first_handle = None;
    let mut second_handle = None;
    let mut ready = false;
    while first_handle.is_none() || second_handle.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "proposal browser frame").await;
        assert!(
            !(second_handle.is_some() && frame["type"] == "proposal" && frame["echo"].is_null()),
            "unchanged replacement proposal was hidden by a non-terminal tool"
        );
        if frame["type"] == "proposal" && frame["echo"].is_string() {
            let handle = frame["handle"]
                .as_str()
                .expect("browser proposal handle")
                .to_owned();
            let echo = frame["echo"].as_str().expect("proposal echo");
            if echo.contains("first response") {
                first_handle = Some(handle);
            } else if echo.contains("browser-only response") {
                second_handle = Some(handle);
            }
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let first_handle = first_handle.expect("first browser proposal handle");
    let second_handle = second_handle.expect("second browser proposal handle");
    assert_ne!(first_handle, second_handle);
    assert!(
        !write_marker.exists(),
        "provider confirmation dispatched a write"
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"missing"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select unknown real host");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "unknown real host error").await,
        json!({"type":"error","message":"Unknown Paseo host profile."})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"local"})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat selected real host with pending proposal");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "pending duplicate real host state").await["type"],
        "host_state"
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "pending duplicate real dashboard").await["type"],
        "dashboard_state"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "duplicate real host cleared the pending presentation"
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"stale proposal audio").into()))
        .await
        .expect("send real audio after proposal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("release real recording after proposal");
    inspect_sender.send(()).expect("release stale inspection");
    let stale_events = tokio::time::timeout(Duration::from_secs(2), stale_receiver)
        .await
        .expect("stale proposal event timeout")
        .expect("stale proposal event signal");
    assert!(
        stale_events.is_empty(),
        "stale proposal events: {stale_events:?}"
    );

    for stale_control in [
        json!({"type":"cancel_proposal"}),
        json!({"type":"cancel_proposal","handle":first_handle}),
    ] {
        runtime
            .browser
            .send(Message::Text(stale_control.to_string().into()))
            .await
            .expect("send stale real cancellation");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "stale real cancellation catch-up").await,
            json!({
                "type":"proposal",
                "echo":"Send to Agent A: \"browser-only response\". Confirm?",
                "handle":second_handle
            })
        );
    }

    for control in [test_text_turn!(
        "queued while approval is visible",
        summary_id
    )] {
        let turn_id = test_text_turn_id(&control);
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send blocked pending interaction");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "pending interaction rejection").await,
            json!({
                "type":"text_turn_rejected",
                "turn_id":turn_id,
                "message":"Typed turn was rejected. The draft was not changed."
            })
        );
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("send blocked pending recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "pending recording rejection").await,
        json!({
            "type":"recording_rejected",
            "mode":"live_response",
            "recording_id":2,
            "message":"Recording could not start. Try again."
        })
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":first_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("send stale browser confirmation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "replacement proposal catch-up").await,
        json!({
            "type":"proposal",
            "echo":"Send to Agent A: \"browser-only response\". Confirm?",
            "handle":second_handle
        })
    );
    assert!(
        !write_marker.exists(),
        "stale handle dispatched replacement"
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":second_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("confirm replacement through browser");
    let mut confirmation_order = Vec::new();
    loop {
        let frame = next_browser_json(&mut runtime.browser, "browser confirmation frame").await;
        match frame["type"].as_str() {
            Some("host_state") => confirmation_order.push("host_state"),
            Some("dashboard_state") => confirmation_order.push("dashboard_state"),
            Some("proposal") => {
                assert_eq!(frame["echo"], Value::Null);
                confirmation_order.push("proposal");
            }
            Some("transcript_done") => {
                assert_eq!(frame["text"], "Confirmed and delivered.");
                confirmation_order.push("transcript_done");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        confirmation_order,
        [
            "host_state",
            "dashboard_state",
            "proposal",
            "transcript_done"
        ]
    );
    let expected_write = "send thread-a --prompt browser-only response --no-wait --json\n";
    assert_eq!(
        std::fs::read_to_string(&write_marker).expect("browser write marker"),
        expected_write
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":second_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("replay browser confirmation");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        std::fs::read_to_string(&write_marker).expect("single browser write marker"),
        expected_write
    );
    finish_sender
        .send(())
        .expect("release fake Realtime connection");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ptt_start_flushes_completed_response_audio_backlog_after_provider_clear() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (clear_sender, clear_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "completed backlog session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "completed backlog response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"completed-backlog"}}),
            json!({
                "type":"response.output_audio.delta",
                "response_id":"completed-backlog",
                "delta":"AQIDBA=="
            }),
            json!({
                "type":"response.done",
                "response":{"id":"completed-backlog","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send completed response backlog");
        }
        assert_eq!(
            next_fake_realtime_json(&mut socket, "completed backlog PTT start").await["type"],
            "input_audio_buffer.clear"
        );
        clear_sender.send(()).expect("signal provider input clear");
        finish_receiver
            .await
            .expect("release completed backlog connection");
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("queue completed audio", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request completed audio backlog");

    let mut playback_backlog = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), runtime.browser.next())
            .await
            .expect("completed backlog browser timeout")
            .expect("completed backlog browser closed")
            .expect("completed backlog browser frame");
        match frame {
            Message::Binary(audio) => playback_backlog.extend_from_slice(&audio),
            Message::Text(message) => {
                let event: Value =
                    serde_json::from_str(&message).expect("completed backlog browser JSON");
                if event == json!({"type":"state","state":"ready"}) {
                    break;
                }
            }
            _ => {}
        }
    }
    assert_eq!(playback_backlog, [1, 2, 3, 4]);

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start PTT over completed audio backlog");
    tokio::time::timeout(Duration::from_secs(2), clear_receiver)
        .await
        .expect("provider input clear timeout")
        .expect("provider input clear signal");
    let flush = tokio::time::timeout(Duration::from_millis(300), runtime.browser.next()).await;
    finish_sender
        .send(())
        .expect("release completed backlog connection");
    fake_realtime.await.expect("fake Realtime task");
    let flush = flush
        .expect("accepted PTT start did not flush completed audio backlog")
        .expect("browser closed before completed backlog flush")
        .expect("completed backlog flush frame");
    let Message::Text(flush) = flush else {
        panic!("completed backlog flush was not text: {flush:?}");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&flush).expect("completed backlog flush JSON"),
        json!({"type":"flush_audio"})
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn real_live_response_abort_discards_only_an_accepted_recording() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let (inspect_idle_sender, inspect_idle_receiver) = tokio::sync::oneshot::channel();
    let (idle_sender, idle_receiver) = tokio::sync::oneshot::channel();
    let (inspect_abort_sender, inspect_abort_receiver) = tokio::sync::oneshot::channel();
    let (abort_sender, abort_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "abort session update").await["type"],
            "session.update"
        );
        ready_sender.send(()).expect("signal abort Realtime ready");

        inspect_idle_receiver.await.expect("inspect idle abort");
        let mut idle_events = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            idle_events.push(
                serde_json::from_str::<Value>(&message).expect("idle abort provider JSON")["type"]
                    .as_str()
                    .expect("idle abort event type")
                    .to_owned(),
            );
        }
        idle_sender
            .send(idle_events)
            .expect("report idle abort events");

        inspect_abort_receiver
            .await
            .expect("inspect accepted abort");
        let mut aborted_events = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            aborted_events.push(
                serde_json::from_str::<Value>(&message).expect("accepted abort provider JSON")
                    ["type"]
                    .as_str()
                    .expect("accepted abort event type")
                    .to_owned(),
            );
        }
        abort_sender
            .send(aborted_events)
            .expect("report accepted abort events");

        let mut fresh_events = Vec::new();
        while !fresh_events.iter().any(|event| event == "response.create") {
            let event = next_fake_realtime_json(&mut socket, "fresh live turn").await;
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "fresh-after-abort").await;
            }
            fresh_events.push(
                event["type"]
                    .as_str()
                    .expect("fresh live event type")
                    .to_owned(),
            );
        }
        assert_eq!(
            fresh_events,
            [
                "input_audio_buffer.clear",
                "input_audio_buffer.append",
                "input_audio_buffer.commit",
                "response.create",
            ]
        );
        for event in [
            json!({"type":"response.created","response":{"id":"response-after-abort"}}),
            json!({
                "type":"response.done",
                "response":{"id":"response-after-abort","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete fresh response after abort");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    tokio::time::timeout(Duration::from_secs(2), ready_receiver)
        .await
        .expect("abort Realtime ready timeout")
        .expect("abort Realtime ready signal");

    for control in [
        json!({"type":"ptt_abort","recording_id":1}),
        json!({"type":"ptt_end","recording_id":1}),
    ] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send idle live abort control");
    }
    inspect_idle_sender
        .send(())
        .expect("release idle abort inspection");
    assert!(
        tokio::time::timeout(Duration::from_secs(2), idle_receiver)
            .await
            .expect("idle abort event timeout")
            .expect("idle abort event signal")
            .is_empty()
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start abortable live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "abortable recording start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"discarded live audio").into()))
        .await
        .expect("send discarded live audio");
    for control in [
        json!({"type":"ptt_abort","recording_id":1}),
        json!({"type":"ptt_abort","recording_id":1}),
        json!({"type":"ptt_end","recording_id":1}),
    ] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send accepted live abort control");
    }
    inspect_abort_sender
        .send(())
        .expect("release accepted abort inspection");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), abort_receiver)
            .await
            .expect("accepted abort event timeout")
            .expect("accepted abort event signal"),
        [
            "input_audio_buffer.clear",
            "input_audio_buffer.append",
            "input_audio_buffer.clear",
        ]
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "accepted abort flush").await,
        json!({"type":"flush_audio"})
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "aborted live recording emitted more than its terminal flush"
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start fresh live recording after abort");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "fresh recording start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"fresh live audio").into()))
        .await
        .expect("send fresh live audio after abort");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish fresh live recording after abort");
    loop {
        if next_browser_json(&mut runtime.browser, "fresh post-abort response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn real_live_recording_ids_reject_stale_terminals_and_flush_accepted_ones() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let (first_terminal_sender, first_terminal_receiver) = tokio::sync::oneshot::channel();
    let (second_start_sender, second_start_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "recording ID session update").await["type"],
            "session.update"
        );
        ready_sender.send(()).expect("signal recording ID ready");
        for expected in [
            "input_audio_buffer.clear",
            "input_audio_buffer.append",
            "input_audio_buffer.clear",
        ] {
            assert_eq!(
                next_fake_realtime_json(&mut socket, "first correlated recording").await["type"],
                expected
            );
        }
        first_terminal_sender
            .send(())
            .expect("signal first correlated terminal");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "second correlated start").await["type"],
            "input_audio_buffer.clear"
        );
        second_start_sender
            .send(())
            .expect("signal second correlated start");
        for expected in [
            "input_audio_buffer.append",
            "input_audio_buffer.commit",
            "response.create",
        ] {
            let event = next_fake_realtime_json(&mut socket, "second correlated recording").await;
            assert_eq!(
                event["type"], expected,
                "stale terminal mutated the newer recording"
            );
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "recording-id-audio").await;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"recording-id-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"recording-id-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete recording ID response");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    ready_receiver.await.expect("recording ID ready signal");
    for control in [
        json!({"type":"ptt_start","recording_id":0,"summary_id":null}),
        json!({"type":"ptt_start","recording_id":2_147_483_648_u64,"summary_id":null}),
        json!({"type":"ptt_start","recording_id":"1","summary_id":null}),
        json!({"type":"ptt_start","recording_id":1}),
        json!({"type":"ptt_start","recording_id":1,"summary_id":null,"extra":true}),
        json!({"type":"ptt_abort","recording_id":0}),
        json!({"type":"ptt_end","recording_id":2_147_483_648_u64}),
    ] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send invalid recording control");
    }
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"invalid recording audio").into()))
        .await
        .expect("send invalid recording audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start recording A");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "recording A start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"recording A audio").into()))
        .await
        .expect("send recording A audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("abort recording A");
    first_terminal_receiver
        .await
        .expect("first correlated terminal signal");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "recording A flush").await,
        json!({"type":"flush_audio"})
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("reject reused recording A ID");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "a completed live recording ID was accepted again"
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start recording B");
    second_start_receiver
        .await
        .expect("second correlated start signal");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "recording B start flush").await,
        json!({"type":"flush_audio"})
    );
    for control in [
        json!({"type":"ptt_start","recording_id":3,"summary_id":null}),
        json!({"type":"ptt_abort","recording_id":1}),
        json!({"type":"ptt_end","recording_id":1}),
        json!({"type":"ptt_abort"}),
        json!({"type":"ptt_end"}),
        json!({"type":"ptt_abort","recording_id":2,"extra":true}),
        json!({"type":"ptt_end","recording_id":2,"extra":true}),
    ] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send stale recording terminal");
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("reject mode change during recording B");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "concurrent recording rejection").await,
        json!({
            "type":"recording_rejected",
            "mode":"live_response",
            "recording_id":3,
            "message":"Recording could not start. Try again."
        })
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "active recording mode rejection").await,
        json!({
            "type":"error",
            "message":"Finish or abort active recording before changing mode."
        })
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "stale recording terminal emitted a browser frame"
    );

    runtime
        .browser
        .send(Message::Binary(test_pcm(b"recording B audio").into()))
        .await
        .expect("send recording B audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish recording B");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "recording B flush").await,
        json!({"type":"flush_audio"})
    );
    loop {
        if next_browser_json(&mut runtime.browser, "recording B response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn same_host_selection_during_live_audio_preserves_commit_acknowledgement() {
    let (latch, mut process_started, release_guard) = process_latch();
    let executor = Arc::new(LatchingSameHostExecutor {
        latch,
        refresh_armed: AtomicBool::new(false),
        list_calls: AtomicUsize::new(0),
        send_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("same-host live Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("same-host live Realtime address")
        .port();
    let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "same-host live session update").await["type"],
            "session.update"
        );
        socket
            .send(Message::Text(
                json!({"type":"session.created"}).to_string().into(),
            ))
            .await
            .expect("ready same-host live provider");
        let mut appended = false;
        loop {
            let event = next_fake_realtime_json(&mut socket, "same-host live commit").await;
            appended |= event["type"] == "input_audio_buffer.append";
            if event["type"] == "input_audio_buffer.commit" {
                break;
            }
        }
        assert!(appended, "same-host live recording lost its audio");
        acknowledge_fake_audio_commit(&mut socket, "same-host-live-item").await;
        loop {
            if next_fake_realtime_json(&mut socket, "same-host live response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        response_sender
            .send(())
            .expect("signal same-host live response");
        for event in [
            json!({"type":"response.created","response":{"id":"same-host-live-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"same-host-live-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete same-host live response");
        }
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "same-host live browser").await;
    loop {
        if next_browser_json(&mut runtime.browser, "same-host live initial ready").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    let baseline_list_calls = executor.list_calls.load(Ordering::SeqCst);
    executor.refresh_armed.store(true, Ordering::SeqCst);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start same-host live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "same-host live start").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"same-host live audio").into()))
        .await
        .expect("send same-host live audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"local"})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat selected host during live audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("commit same-host live audio");

    let mut same_host_frames = std::collections::HashSet::new();
    while same_host_frames.len() < 3 {
        let frame = next_browser_json_without_process_start(
            &mut runtime.browser,
            &mut process_started,
            "same-host live selection",
        )
        .await;
        let frame_type = frame["type"].as_str().expect("same-host live frame type");
        assert!(
            ["host_state", "dashboard_state", "flush_audio"].contains(&frame_type),
            "unexpected same-host live frame: {frame}"
        );
        assert!(
            same_host_frames.insert(frame_type.to_owned()),
            "duplicate same-host live frame: {frame}"
        );
    }
    tokio::time::timeout(Duration::from_secs(2), response_receiver)
        .await
        .expect("same-host live acknowledgement was discarded")
        .expect("same-host live response signal");
    assert_eq!(
        executor.list_calls.load(Ordering::SeqCst),
        baseline_list_calls,
        "same-host live selection invoked a process probe"
    );
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 0);
    loop {
        if next_browser_json(&mut runtime.browser, "same-host live completion").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    release_guard.release();
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn same_host_selection_during_dictation_preserves_commit_acknowledgement() {
    let (latch, mut process_started, release_guard) = process_latch();
    let executor = Arc::new(LatchingSameHostExecutor {
        latch,
        refresh_armed: AtomicBool::new(false),
        list_calls: AtomicUsize::new(0),
        send_calls: AtomicUsize::new(0),
    });
    let (cleanup_started_sender, cleanup_started_receiver) = tokio::sync::oneshot::channel();
    let (cleanup_release_sender, cleanup_release_receiver) = tokio::sync::oneshot::channel();
    let cleaner = Arc::new(LatchedDictationCleaner {
        started: Mutex::new(Some(cleanup_started_sender)),
        release: Mutex::new(Some(cleanup_release_receiver)),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("same-host dictation Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("same-host dictation Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let (commit_sender, commit_receiver) = tokio::sync::oneshot::channel();
    let (ack_sender, ack_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "same-host dictation session update").await["type"],
            "session.update"
        );
        socket
            .send(Message::Text(
                json!({"type":"session.created"}).to_string().into(),
            ))
            .await
            .expect("ready same-host dictation provider");
        bind_fake_realtime_context(&mut socket, "same-host-dictation").await;
        bound_sender
            .send(())
            .expect("signal same-host dictation context");
        let mut appended = false;
        loop {
            let event = next_fake_realtime_json(&mut socket, "same-host dictation commit").await;
            appended |= event["type"] == "input_audio_buffer.append";
            if event["type"] == "input_audio_buffer.commit" {
                break;
            }
        }
        assert!(appended, "same-host dictation lost its audio");
        commit_sender
            .send(())
            .expect("signal same-host dictation commit");
        ack_receiver
            .await
            .expect("release same-host dictation acknowledgement");
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"same-host-dictation-item",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"same-host-dictation-item",
                "transcript":"same host dictation"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete same-host dictation transcription");
        }
        let delete = loop {
            let event = next_fake_realtime_json(&mut socket, "same-host dictation delete").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "same-host-dictation-item");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"same-host-dictation-item",
                    "event_id":"same-host-dictation-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge same-host dictation deletion");
        finish_receiver
            .await
            .expect("finish same-host dictation test");
    });

    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_hosts_process_and_cleaner(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
        Config::default().paseo_hosts,
        cleaner,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "same-host dictation browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    bound_receiver
        .await
        .expect("same-host dictation context signal");
    loop {
        if next_browser_json(&mut runtime.browser, "same-host dictation context ready").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select same-host dictation mode");
    let _ = next_browser_json_of_type(
        &mut runtime.browser,
        "voice_mode",
        "same-host dictation mode",
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start same-host dictation");
    let operation = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_operation",
        "same-host dictation operation",
    )
    .await;
    let operation_id = operation["operation_id"]
        .as_str()
        .expect("same-host dictation operation ID")
        .to_owned();
    assert_eq!(
        next_browser_json(&mut runtime.browser, "same-host dictation start").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(
            test_pcm(b"same-host dictation audio").into(),
        ))
        .await
        .expect("send same-host dictation audio");
    let baseline_list_calls = executor.list_calls.load(Ordering::SeqCst);
    executor.refresh_armed.store(true, Ordering::SeqCst);
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"local"})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat selected host during dictation");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("commit same-host dictation");

    let mut same_host_frames = std::collections::HashSet::new();
    while same_host_frames.len() < 3 {
        let frame = next_browser_json_without_process_start(
            &mut runtime.browser,
            &mut process_started,
            "same-host dictation selection",
        )
        .await;
        let frame_type = frame["type"]
            .as_str()
            .expect("same-host dictation frame type");
        assert!(
            ["host_state", "dashboard_state", "state"].contains(&frame_type),
            "unexpected same-host dictation frame: {frame}"
        );
        if frame_type == "state" {
            assert_eq!(frame["state"], "transcribing");
        }
        assert!(
            same_host_frames.insert(frame_type.to_owned()),
            "duplicate same-host dictation frame: {frame}"
        );
    }
    commit_receiver
        .await
        .expect("same-host dictation commit signal");
    ack_sender
        .send(())
        .expect("release same-host dictation acknowledgement");
    tokio::time::timeout(Duration::from_secs(2), cleanup_started_receiver)
        .await
        .expect("same-host dictation acknowledgement was discarded")
        .expect("same-host dictation cleanup signal");
    cleanup_release_sender
        .send(())
        .expect("release same-host dictation cleanup");
    let result = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_result",
        "same-host dictation result",
    )
    .await;
    assert_eq!(result["operation_id"], operation_id);
    assert_eq!(result["text"], "Cleaned dictation");
    assert_eq!(
        executor.list_calls.load(Ordering::SeqCst),
        baseline_list_calls,
        "same-host dictation selection invoked a process probe"
    );
    assert_eq!(executor.send_calls.load(Ordering::SeqCst), 0);
    loop {
        if next_browser_json(&mut runtime.browser, "same-host dictation completion").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    finish_sender
        .send(())
        .expect("finish same-host dictation provider");
    release_guard.release();
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn real_live_audio_requires_an_accepted_start_and_survives_duplicate_host_selection() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let (inspect_sender, inspect_receiver) = tokio::sync::oneshot::channel();
    let (stale_sender, stale_receiver) = tokio::sync::oneshot::channel();
    let (accepted_sender, accepted_receiver) = tokio::sync::oneshot::channel();
    let (inspect_change_sender, inspect_change_receiver) = tokio::sync::oneshot::channel();
    let (changed_sender, changed_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "session update").await["type"],
            "session.update"
        );
        ready_sender.send(()).expect("signal Realtime ready");
        inspect_receiver.await.expect("inspect stale live input");
        let mut stale_events = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("stale live event JSON");
            stale_events.push(
                event["type"]
                    .as_str()
                    .expect("stale live event type")
                    .to_owned(),
            );
        }
        stale_sender
            .send(stale_events)
            .expect("report stale live events");

        let mut clears = 0;
        let mut appends = 0;
        let mut commits = 0;
        let mut response_creates = 0;
        while response_creates == 0 {
            let event = next_fake_realtime_json(&mut socket, "accepted live event").await;
            clears += usize::from(event["type"] == "input_audio_buffer.clear");
            appends += usize::from(event["type"] == "input_audio_buffer.append");
            commits += usize::from(event["type"] == "input_audio_buffer.commit");
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "accepted-live-audio").await;
            }
            response_creates += usize::from(event["type"] == "response.create");
        }
        accepted_sender
            .send((clears, appends, commits, response_creates))
            .expect("report accepted live events");
        for event in [
            json!({"type":"response.created","response":{"id":"accepted-live-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"accepted-live-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete accepted live response");
        }
        inspect_change_receiver
            .await
            .expect("inspect changed-host live input");
        let mut changed_counts = (0, 0, 0, 0);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("changed-host event JSON");
            changed_counts.0 += usize::from(event["type"] == "input_audio_buffer.clear");
            changed_counts.1 += usize::from(event["type"] == "input_audio_buffer.append");
            changed_counts.2 += usize::from(event["type"] == "input_audio_buffer.commit");
            changed_counts.3 += usize::from(event["type"] == "response.create");
        }
        changed_sender
            .send(changed_counts)
            .expect("report changed-host live events");
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    tokio::time::timeout(Duration::from_secs(2), ready_receiver)
        .await
        .expect("Realtime ready timeout")
        .expect("Realtime ready signal");
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"stale live audio").into()))
        .await
        .expect("send live audio without start");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("send live end without start");
    inspect_sender.send(()).expect("release stale inspection");
    let stale_events = tokio::time::timeout(Duration::from_secs(2), stale_receiver)
        .await
        .expect("stale event timeout")
        .expect("stale event signal");
    assert!(
        stale_events.is_empty(),
        "stale live events: {stale_events:?}"
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start accepted live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "accepted live start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"missing"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select unknown real host");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "unknown real host error").await,
        json!({"type":"error","message":"Unknown Paseo host profile."})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"local"})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat selected real host");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "duplicate real host state").await["type"],
        "host_state"
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "duplicate real dashboard").await["type"],
        "dashboard_state"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "duplicate real host mutated proposal presentation"
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"accepted live audio").into()))
        .await
        .expect("send accepted live audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish accepted live recording");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), accepted_receiver)
            .await
            .expect("accepted event timeout")
            .expect("accepted event signal"),
        (1, 1, 1, 1)
    );
    loop {
        if next_browser_json(&mut runtime.browser, "accepted live response terminal").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start live recording before host change");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "changed-host live start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"changed-host live audio").into()))
        .await
        .expect("send live audio before host change");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change real host during live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "changed real host state").await["type"],
        "host_state"
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "changed real host dashboard").await["type"],
        "dashboard_state"
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "changed real host proposal").await,
        json!({"type":"proposal","echo":null})
    );
    runtime
        .browser
        .send(Message::Binary(
            test_pcm(b"stale changed-host audio").into(),
        ))
        .await
        .expect("send stale audio after real host change");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("release live recording after host change");
    inspect_change_sender
        .send(())
        .expect("release changed-host inspection");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), changed_receiver)
            .await
            .expect("changed-host event timeout")
            .expect("changed-host event signal"),
        (2, 1, 0, 0)
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn pending_response_request_accounting_is_bounded() {
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
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut response_creates = 0;
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("flood event JSON");
            response_creates += usize::from(event["type"] == "response.create");
        }
        assert_eq!(response_creates, 1);
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    for index in 0..80 {
        runtime
            .browser
            .send(Message::Text(
                test_text_turn!(format!("pending response {index}"), null)
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send pending response turn");
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn response_call_id_accounting_is_bounded() {
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
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-calls"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send response created");
        for index in 0..65 {
            for event in function_call_events_at(
                "response-calls",
                &format!("bounded-item-{index}"),
                &format!("bounded-call-{index}"),
                index,
                "cancel_action",
                "{}",
            ) {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("send bounded call");
            }
        }
        let mut outputs = 0;
        loop {
            let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("call cap provider close timeout");
            match message {
                Some(Ok(Message::Text(message))) => {
                    let event: Value =
                        serde_json::from_str(&message).expect("bounded call output JSON");
                    outputs += usize::from(
                        event.pointer("/item/type").and_then(Value::as_str)
                            == Some("function_call_output"),
                    );
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            }
        }
        assert_eq!(outputs, 64);
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("exercise bounded calls", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request bounded calls response");
    let error = loop {
        let frame = next_browser_json(&mut runtime.browser, "call capacity exhaustion").await;
        if frame["type"] == "error" {
            break frame;
        }
    };
    assert_eq!(
        error,
        json!({
            "type":"error",
            "message":"Realtime function-call capacity exhausted. Reconnect required."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("call cap browser close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn realtime_audio_deltas_are_bounded_pcm16_and_preserve_later_valid_audio() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let oversized_encoded = "A".repeat(200_000);
    let oversized_decoded = BASE64.encode(vec![0_u8; 96_002]);
    let odd_pcm16 = BASE64.encode([0_u8, 1, 2]);
    let valid_audio = vec![0_u8, 128, 255, 127, 42, 0];
    let provider_valid_audio = valid_audio.clone();
    let valid_delta = BASE64.encode(&valid_audio);
    let quarantined_delta = BASE64.encode(b"quarantined audio");
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "audio validation response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-audio-validation"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send audio validation response created");
        for event in [
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-quarantined",
                "delta":quarantined_delta
            }),
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-audio-validation",
                "delta":"not base64!"
            }),
            json!({
                "type":"response.audio.delta",
                "response_id":"response-audio-validation",
                "delta":""
            }),
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-audio-validation",
                "delta":odd_pcm16
            }),
            json!({
                "type":"response.audio.delta",
                "response_id":"response-audio-validation",
                "delta":oversized_encoded
            }),
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-audio-validation",
                "delta":oversized_decoded
            }),
            json!({
                "type":"response.audio.delta",
                "response_id":"response-audio-validation",
                "delta":valid_delta
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-audio-validation","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send audio validation event");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        provider_valid_audio
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("validate provider audio", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request audio validation response");
    let mut browser_audio = Vec::new();
    let mut ready = false;
    while !ready {
        let message = tokio::time::timeout(Duration::from_secs(3), runtime.browser.next())
            .await
            .expect("audio validation browser timeout")
            .expect("audio validation browser frame")
            .expect("audio validation browser result");
        match message {
            Message::Binary(audio) => browser_audio.push(audio.to_vec()),
            Message::Text(message) => {
                let frame: Value =
                    serde_json::from_str(&message).expect("audio validation browser JSON");
                ready = frame == json!({"type":"state","state":"ready"});
            }
            _ => {}
        }
    }
    let provider_valid_audio = fake_realtime.await.expect("fake Realtime task");
    assert_eq!(browser_audio, vec![provider_valid_audio]);
}

#[tokio::test]
async fn invalid_live_pcm_frames_abort_only_the_active_recording() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (cleared_sender, mut cleared_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (inspect_sender, inspect_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "invalid live session update").await["type"],
            "session.update"
        );
        for index in 0..3 {
            for context in ["invalid live start clear", "invalid live abort clear"] {
                let event = next_fake_realtime_json(&mut socket, context).await;
                assert_eq!(event["type"], "input_audio_buffer.clear");
            }
            cleared_sender
                .send(index)
                .expect("signal invalid live clear");
        }
        inspect_receiver
            .await
            .expect("inspect invalid live provider state");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), socket.next())
                .await
                .is_err(),
            "invalid live audio produced provider work"
        );
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    let invalid_frames = [vec![], vec![0_u8; 3], vec![0_u8; 96_002]];
    for (index, audio) in invalid_frames.into_iter().enumerate() {
        let recording_id = u64::try_from(index + 1).expect("bounded invalid recording ID");
        runtime
            .browser
            .send(Message::Text(
                json!({"type":"ptt_start","recording_id":recording_id,"summary_id":null})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("start invalid live recording");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "invalid live start flush").await,
            json!({"type":"flush_audio"})
        );
        runtime
            .browser
            .send(Message::Binary(audio.into()))
            .await
            .expect("send invalid live PCM");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "invalid live rejection").await,
            json!({
                "type":"recording_rejected",
                "mode":"live_response",
                "recording_id":recording_id,
                "message":"Invalid PCM16 audio frame. Recording was aborted."
            })
        );
        assert_eq!(
            next_browser_json(&mut runtime.browser, "invalid live abort flush").await,
            json!({"type":"flush_audio"})
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), cleared_receiver.recv())
                .await
                .expect("invalid live clear timeout")
                .expect("invalid live clear signal"),
            index
        );
        runtime
            .browser
            .send(Message::Text(
                json!({"type":"ptt_end","recording_id":recording_id})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send stale invalid live terminal");
    }
    inspect_sender
        .send(())
        .expect("inspect invalid live provider state");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn invalid_dictation_pcm_frames_abort_only_the_active_recording() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-invalid-pcm", "Reply A");
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (cleared_sender, mut cleared_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (inspect_sender, inspect_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "invalid-pcm").await;
        for index in 0..3 {
            for context in [
                "invalid dictation start clear",
                "invalid dictation abort clear",
            ] {
                let event = next_fake_realtime_json(&mut socket, context).await;
                assert_eq!(event["type"], "input_audio_buffer.clear");
            }
            cleared_sender
                .send(index)
                .expect("signal invalid dictation clear");
        }
        inspect_receiver
            .await
            .expect("inspect invalid dictation provider state");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), socket.next())
                .await
                .is_err(),
            "invalid dictation audio produced provider work"
        );
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select invalid PCM dictation mode");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "invalid PCM dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );

    let invalid_frames = [vec![], vec![0_u8; 3], vec![0_u8; 96_002]];
    for (index, audio) in invalid_frames.into_iter().enumerate() {
        let recording_id = u64::try_from(index + 1).expect("bounded invalid recording ID");
        runtime
            .browser
            .send(Message::Text(
                json!({
                    "type":"ptt_start",
                    "recording_id":recording_id,
                    "summary_id":summary_id
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("start invalid dictation recording");
        let operation = next_browser_json(
            &mut runtime.browser,
            "invalid dictation operation allocation",
        )
        .await;
        assert_eq!(operation["type"], "dictation_operation");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "invalid dictation start flush").await,
            json!({"type":"flush_audio"})
        );
        runtime
            .browser
            .send(Message::Binary(audio.into()))
            .await
            .expect("send invalid dictation PCM");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "invalid dictation rejection").await,
            json!({
                "type":"recording_rejected",
                "mode":"dictation",
                "recording_id":recording_id,
                "message":"Invalid PCM16 audio frame. Recording was aborted."
            })
        );
        assert_eq!(
            next_browser_json(&mut runtime.browser, "invalid dictation abort flush").await,
            json!({"type":"flush_audio"})
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), cleared_receiver.recv())
                .await
                .expect("invalid dictation clear timeout")
                .expect("invalid dictation clear signal"),
            index
        );
        runtime
            .browser
            .send(Message::Text(
                json!({"type":"ptt_end","operation_id":operation["operation_id"]})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send stale invalid dictation terminal");
    }
    inspect_sender
        .send(())
        .expect("inspect invalid dictation provider state");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn maximum_valid_browser_pcm_frame_forwards_exactly() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let expected_audio = (0_u8..=255).cycle().take(96_000).collect::<Vec<_>>();
    let provider_expected_audio = expected_audio.clone();
    let (forwarded_sender, forwarded_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "maximum PCM session update").await["type"],
            "session.update"
        );
        assert_eq!(
            next_fake_realtime_json(&mut socket, "maximum PCM start clear").await["type"],
            "input_audio_buffer.clear"
        );
        let append = next_fake_realtime_json(&mut socket, "maximum PCM append").await;
        assert_eq!(append["type"], "input_audio_buffer.append");
        let forwarded = BASE64
            .decode(append["audio"].as_str().expect("maximum PCM base64"))
            .expect("decode maximum PCM");
        assert_eq!(forwarded, provider_expected_audio);
        forwarded_sender
            .send(())
            .expect("signal maximum PCM forwarding");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "maximum PCM abort clear").await["type"],
            "input_audio_buffer.clear"
        );
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start maximum PCM recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "maximum PCM start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(expected_audio.into()))
        .await
        .expect("send maximum PCM frame");
    forwarded_receiver
        .await
        .expect("maximum PCM forwarding signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("abort maximum PCM recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "maximum PCM abort flush").await,
        json!({"type":"flush_audio"})
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn current_correlated_response_and_followup_still_work() {
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
        let _ = next_fake_realtime_json(&mut socket, "session update").await;
        loop {
            if next_fake_realtime_json(&mut socket, "current response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-current"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send current response created");
        for event in [
            json!({"type":"response.output_audio.delta","delta":"d3Jvbmc="}),
            json!({
                "type":"response.output_audio_transcript.done",
                "transcript":"wrong transcript"
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "call_id":"current-call",
                "name":"cancel_action",
                "arguments":"{}"
            }),
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-current",
                "delta":"b2s="
            }),
            json!({
                "type":"response.output_audio_transcript.delta",
                "response_id":"response-current",
                "delta":"Current"
            }),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-current",
                "transcript":"Current response"
            }),
            json!({
                "type":"response.output_item.done",
                "response_id":"response-current",
                "output_index":0,
                "item":{
                    "id":"current-item",
                    "type":"function_call",
                    "call_id":"current-call",
                    "name":"cancel_action"
                }
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "response_id":"response-current",
                "item_id":"current-item",
                "call_id":"current-call",
                "name":"cancel_action",
                "arguments":"{}"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send current response event");
        }
        let mut outputs = 0;
        while outputs == 0 {
            let event = next_fake_realtime_json(&mut socket, "current tool output").await;
            outputs += usize::from(
                event.pointer("/item/type").and_then(Value::as_str) == Some("function_call_output"),
            );
        }
        assert_eq!(outputs, 1);
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-current","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete current response");
        loop {
            if next_fake_realtime_json(&mut socket, "followup response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-followup"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send followup response created");
        for event in [
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-followup",
                "transcript":"Followup response"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"response-followup","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send followup response event");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("run current response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request current response");
    let mut audio = Vec::new();
    let mut transcripts = Vec::new();
    let mut ready = false;
    while !ready {
        let message = tokio::time::timeout(Duration::from_secs(2), runtime.browser.next())
            .await
            .expect("current browser frame timeout")
            .expect("current browser frame")
            .expect("current browser result");
        match message {
            Message::Binary(bytes) => audio.push(bytes.to_vec()),
            Message::Text(message) => {
                let frame: Value = serde_json::from_str(&message).expect("current browser JSON");
                if frame["type"] == "transcript_done" {
                    transcripts.push(
                        frame["text"]
                            .as_str()
                            .expect("current transcript text")
                            .to_owned(),
                    );
                }
                ready = frame == json!({"type":"state","state":"ready"});
            }
            _ => {}
        }
    }
    assert_eq!(audio, vec![b"ok".to_vec()]);
    assert_eq!(
        transcripts,
        vec![
            "Current response".to_owned(),
            "Followup response".to_owned()
        ]
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn rust_runtime_bridges_realtime_audio_and_drops_canceled_tool_calls() {
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
        let mut socket = accept_hdr_async(stream, AssertNoRealtimeAuthorization)
            .await
            .expect("Realtime handshake");
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
        assert!(
            update["session"]["tools"]
                .as_array()
                .expect("session tools")
                .iter()
                .all(|tool| tool["name"] != "confirm_action")
        );
        let instructions = update["session"]["instructions"]
            .as_str()
            .expect("session instructions");
        assert!(!instructions.contains("confirm_action"));
        assert!(instructions.contains("browser confirmation control"));

        let mut saw_audio = false;
        let mut saw_commit = false;
        let mut response_creates = 0;
        for _ in 0..8 {
            let message = socket.next().await.expect("client event").expect("event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("client event JSON");
            if event["type"] == "input_audio_buffer.append" {
                saw_audio = true;
            }
            if event["type"] == "response.create" {
                response_creates += 1;
                if saw_commit {
                    let duplicate =
                        tokio::time::timeout(Duration::from_millis(100), socket.next()).await;
                    if let Ok(Some(Ok(Message::Text(message)))) = duplicate {
                        let event: Value =
                            serde_json::from_str(&message).expect("duplicate event JSON");
                        response_creates += usize::from(event["type"] == "response.create");
                    }
                    break;
                }
            }
            if event["type"] == "input_audio_buffer.commit" {
                saw_commit = true;
                acknowledge_fake_audio_commit(&mut socket, "bridge-live-audio").await;
            }
        }
        assert!(saw_audio);
        assert!(saw_commit);
        assert_eq!(response_creates, 1);
        for event in [
            json!({"type":"session.updated"}),
            json!({"type":"response.created","response":{"id":"response-1"}}),
            json!({
                "type":"response.output_audio.delta",
                "response_id":"response-1",
                "delta":"YWI="
            }),
            json!({
                "type":"response.output_audio_transcript.delta",
                "response_id":"response-1",
                "delta":"Done"
            }),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"response-1",
                "transcript":"Done"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send fake event");
        }
        let mut saw_cancel = false;
        let mut saw_clear = false;
        for _ in 0..3 {
            let message = tokio::time::timeout(Duration::from_millis(300), socket.next())
                .await
                .expect("barge-in event timeout")
                .expect("barge-in event")
                .expect("barge-in event result");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("barge-in JSON");
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "response-1");
                saw_cancel = true;
            }
            saw_clear |= event["type"] == "input_audio_buffer.clear";
            if saw_cancel && saw_clear {
                break;
            }
        }
        assert!(saw_cancel);
        assert!(saw_clear);
        for event in [
            json!({
                "type":"response.done",
                "response":{"id":"response-1","status":"cancelled"}
            }),
            json!({
                "type":"response.output_item.done",
                "response_id":"response-1",
                "item":{
                    "id":"duplicate-item",
                    "type":"function_call",
                    "call_id":"duplicate-call",
                    "name":"cancel_action"
                }
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "response_id":"response-1",
                "item_id":"duplicate-item",
                "call_id":"duplicate-call",
                "name":"cancel_action",
                "arguments":"{}"
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "response_id":"response-1",
                "item_id":"duplicate-item",
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
            assert_ne!(event["type"], "response.create");
        }
        assert_eq!(outputs, 0);
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
            "paseoHosts":[
                {"id":"wsl","label":"Paseo WSL Host","target":"paseo-wsl.example:6767","default":true,"defaultCwd":"~/","defaultProvider":"opencode/gpt-5.6-sol-max"},
                {"id":"spark","label":"Paseo Spark Host","target":"paseo-spark.example:6767","default":false,"defaultCwd":"~/dev","defaultProvider":"codex/gpt-5.4"}
            ],
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
        .envs(essential_os_env())
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
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (mut browser, _) = connect_async(format!("ws://127.0.0.1:{app_port}/ws"))
        .await
        .expect("browser websocket");
    browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send browser hello");
    assert_eq!(
        next_browser_json(&mut browser, "protocol ready frame").await,
        json!({"type":"protocol_ready","version":2})
    );
    let mode = browser.next().await.expect("mode frame").expect("mode");
    let Message::Text(mode) = mode else {
        panic!("expected mode text frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&mode).expect("mode JSON"),
        json!({"type":"mode","mode":"real"})
    );
    let voice_mode = browser
        .next()
        .await
        .expect("voice mode frame")
        .expect("voice mode");
    let Message::Text(voice_mode) = voice_mode else {
        panic!("expected voice mode text frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&voice_mode).expect("voice mode JSON"),
        json!({"type":"voice_mode","mode":"live_response"})
    );
    let capabilities = browser
        .next()
        .await
        .expect("capability frame")
        .expect("capabilities");
    let Message::Text(capabilities) = capabilities else {
        panic!("expected capability text frame");
    };
    let capabilities = serde_json::from_str::<Value>(&capabilities).expect("capability JSON");
    assert_eq!(capabilities["type"], "dictation_capabilities");
    assert_eq!(
        capabilities["speech_to_text"]["processing_location"],
        "Broker-configured local endpoint"
    );
    assert_eq!(
        capabilities["cleanup"]["processing_location"],
        "Broker-configured local endpoint"
    );
    let encoded_capabilities = capabilities.to_string();
    assert!(!encoded_capabilities.contains("127.0.0.1"));
    assert!(!encoded_capabilities.contains("test-key"));
    let hosts = tokio::time::timeout(Duration::from_millis(200), browser.next())
        .await
        .expect("host state timeout")
        .expect("host frame")
        .expect("hosts");
    let Message::Text(hosts) = hosts else {
        panic!("expected host text frame");
    };
    let hosts: Value = serde_json::from_str(&hosts).expect("host JSON");
    assert_eq!(hosts["type"], "host_state");
    assert_eq!(hosts["selected_host_id"], "wsl");
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start push-to-talk");
    loop {
        let frame = browser
            .next()
            .await
            .expect("start flush frame")
            .expect("start flush");
        let Message::Text(frame) = frame else {
            continue;
        };
        if serde_json::from_str::<Value>(&frame).expect("start flush JSON")
            == json!({"type":"flush_audio"})
        {
            break;
        }
    }
    browser
        .send(Message::Binary(test_pcm(b"mic").into()))
        .await
        .expect("microphone audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("end push-to-talk");
    loop {
        let terminal = browser
            .next()
            .await
            .expect("terminal flush frame")
            .expect("terminal flush");
        let Message::Text(terminal) = terminal else {
            continue;
        };
        if serde_json::from_str::<Value>(&terminal).expect("terminal flush JSON")
            == json!({"type":"flush_audio"})
        {
            break;
        }
    }

    let mut audio = None;
    let mut transcript = None;
    let mut playback_flushed = false;
    let mut barge_in_sent = false;
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
                if value["type"] == "flush_audio" {
                    playback_flushed = true;
                }
            }
            _ => {}
        }
        if audio.is_some() && transcript.is_some() && !barge_in_sent {
            browser
                .send(Message::Text(
                    json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("barge in");
            barge_in_sent = true;
        }
        if playback_flushed {
            break;
        }
    }
    assert_eq!(audio, Some(b"ab".to_vec()));
    assert_eq!(transcript.as_deref(), Some("Done"));
    assert!(playback_flushed);
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn dictation_mode_commits_audio_without_creating_an_assistant_response() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-dictation-mode", "Reply A");
    let app_port = TcpListener::bind("localhost:0")
        .expect("reserve app port")
        .local_addr()
        .expect("app address")
        .port();
    let realtime_listener = tokio::net::TcpListener::bind("localhost:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (delete_seen_sender, delete_seen_receiver) = tokio::sync::oneshot::channel();
    let (release_delete_sender, release_delete_receiver) = tokio::sync::oneshot::channel();
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
        assert_eq!(
            update.pointer("/session/audio/input/transcription/model"),
            Some(&Value::String("gpt-4o-mini-transcribe".to_owned()))
        );
        assert_eq!(
            update.pointer("/session/audio/input/transcription/language"),
            Some(&Value::String("en".to_owned()))
        );
        bind_fake_realtime_context(&mut socket, "dictation-mode").await;

        let mut saw_audio = false;
        let mut saw_commit = false;
        for _ in 0..4 {
            let message = socket
                .next()
                .await
                .expect("dictation event")
                .expect("event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("dictation event JSON");
            saw_audio |= event["type"] == "input_audio_buffer.append";
            saw_commit |= event["type"] == "input_audio_buffer.commit";
            if saw_audio && saw_commit {
                break;
            }
        }
        assert!(saw_audio);
        assert!(saw_commit);
        let unexpected = tokio::time::timeout(Duration::from_millis(100), socket.next()).await;
        if let Ok(Some(Ok(Message::Text(message)))) = unexpected {
            let event: Value = serde_json::from_str(&message).expect("unexpected event JSON");
            assert_ne!(event["type"], "response.create");
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"input_audio_buffer.committed",
                    "item_id":"dictation-item",
                    "previous_item_id":null
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send committed item");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"dictation-item",
                    "transcript":"Draft the response"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send dictation transcript");
        let delete = loop {
            let event = next_fake_realtime_json(&mut socket, "dictation item delete").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "dictation-item");
        assert_eq!(delete.as_object().expect("delete object").len(), 3);
        let delete_event_id = delete["event_id"]
            .as_str()
            .expect("dictation delete event ID")
            .to_owned();
        assert!(!delete_event_id.contains("Draft the response"));
        delete_seen_sender
            .send(())
            .expect("signal dictation delete");
        release_delete_receiver
            .await
            .expect("release dictation delete acknowledgement");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"dictation-item",
                    "event_id":"provider-delete-ack-1"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge dictation item delete");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"dictation-item",
                    "event_id":"provider-delete-ack-1"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("repeat dictation item deletion acknowledgement");
        tokio::time::sleep(Duration::from_millis(100)).await;
        for event in [
            json!({
                "type":"conversation.item.deleted",
                "item_id":"dictation-item",
                "event_id":"provider-delete-ack-1"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"dictation-item",
                "transcript":"Late duplicate dictation"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send late dictation tombstone event");
        }
        let mut saw_live_audio = false;
        loop {
            let event = next_fake_realtime_json(&mut socket, "post-dictation live turn").await;
            assert_ne!(event["type"], "conversation.item.delete");
            saw_live_audio |= event["type"] == "input_audio_buffer.append";
            if event["type"] == "input_audio_buffer.commit" {
                break;
            }
        }
        assert!(saw_live_audio);
        acknowledge_fake_audio_commit(&mut socket, "post-dictation-live-item").await;
        loop {
            let event = next_fake_realtime_json(&mut socket, "post-dictation live response").await;
            assert_ne!(event["type"], "conversation.item.delete");
            if event["type"] == "response.create" {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"post-dictation-live-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"post-dictation-live-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete post-dictation live response");
        }
        if let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(Duration::from_millis(150), socket.next()).await
        {
            let event: Value = serde_json::from_str(&message).expect("post-live provider JSON");
            assert_ne!(event["type"], "conversation.item.delete");
        }
    });

    let directory = tempfile::tempdir().expect("temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        json!({
            "listenHost":"localhost",
            "listenPort":app_port,
            "openaiBaseUrl":format!("ws://localhost:{realtime_port}"),
            "paseoBin":fake_paseo_path,
            "secretProvider":"environment",
            "paseoHosts":[
                {"id":"local","label":"Local","target":null,"default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"}
            ],
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
        .envs(essential_os_env())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("OPENAI_API_KEY", "test-key")
        .env("PASEO_PASSWORD", "test-password")
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Rust runtime");
    let _guard = ChildGuard(child);
    let health_url = format!("http://localhost:{app_port}/healthz");
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (mut browser, _) = connect_async(format!("ws://localhost:{app_port}/ws"))
        .await
        .expect("browser websocket");
    initialize_browser_protocol(&mut browser, "initial frame").await;
    let summary_id = bind_browser_context(&mut browser).await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation");
    let selected = browser.next().await.expect("mode frame").expect("mode");
    let Message::Text(selected) = selected else {
        panic!("expected selected mode frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&selected).expect("mode JSON"),
        json!({"type":"voice_mode","mode":"dictation"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start dictation");
    let operation = next_browser_json(&mut browser, "dictation operation").await;
    assert_eq!(operation["type"], "dictation_operation");
    let operation_id = operation["operation_id"]
        .as_str()
        .expect("dictation operation ID")
        .to_owned();
    assert_eq!(
        next_browser_json(&mut browser, "dictation start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"mic").into()))
        .await
        .expect("send microphone audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("ignore live abort control during dictation");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish dictation");

    tokio::time::timeout(Duration::from_millis(500), delete_seen_receiver)
        .await
        .expect("dictation item delete timeout")
        .expect("dictation item delete signal");
    let terminal_deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while tokio::time::Instant::now() < terminal_deadline {
        let remaining = terminal_deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("pre-delete browser JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_failed");
        assert_ne!(frame["type"], "dictation_cancelled");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
    }
    release_delete_sender
        .send(())
        .expect("release dictation delete");

    let mut result = None;
    for _ in 0..4 {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("dictation result timeout")
            .expect("dictation result frame")
            .expect("dictation result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("dictation result JSON");
        assert_ne!(frame["type"], "proposal");
        assert_ne!(frame["type"], "tool");
        if frame["type"] == "dictation_result" {
            result = Some(frame);
            break;
        }
    }
    let result = result.expect("dictation result was emitted");
    assert_eq!(result["status"], "degraded");
    assert_eq!(result["text"], "Draft the response");
    assert_eq!(result["operation_id"], operation_id);
    assert_eq!(
        next_browser_json(&mut browser, "post-dictation ready").await,
        json!({"type":"state","state":"ready"})
    );
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select live response after dictation deletion");
    assert_eq!(
        next_browser_json(&mut browser, "post-dictation live mode").await,
        json!({"type":"voice_mode","mode":"live_response"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start live turn after dictation deletion");
    assert_eq!(
        next_browser_json(&mut browser, "post-dictation live flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"live after deletion").into()))
        .await
        .expect("send post-dictation live audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish live turn after dictation deletion");
    assert_eq!(
        next_browser_json(&mut browser, "post-dictation live terminal flush").await,
        json!({"type":"flush_audio"})
    );
    loop {
        if next_browser_json(&mut browser, "post-dictation live completion").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
async fn wrong_deleted_item_and_its_replay_fail_closed_before_terminal() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-wrong-dictation-delete",
        "Reply A",
    );
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
        bind_fake_realtime_context(&mut socket, "wrong-dictation-delete").await;
        loop {
            if next_fake_realtime_json(&mut socket, "wrong-delete commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"pending-delete-item-a",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"pending-delete-item-a",
                "transcript":"Pending deletion A"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance wrong-delete dictation");
        }
        let delete = loop {
            let event = next_fake_realtime_json(&mut socket, "pending delete A").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "pending-delete-item-a");
        let wrong = json!({
            "type":"conversation.item.deleted",
            "item_id":"wrong-delete-item-b",
            "event_id":"provider-wrong-delete-b"
        });
        for _ in 0..2 {
            socket
                .send(Message::Text(wrong.to_string().into()))
                .await
                .expect("send wrong deletion replay");
        }
        let closed = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("wrong deletion did not close provider socket");
        matches!(closed, Some(Ok(Message::Close(_))) | None)
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    let operation_id = start_and_commit_bound_dictation(&mut runtime.browser, &summary_id, 1).await;
    loop {
        let message = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
            .await
            .expect("wrong deletion browser close timeout");
        let Some(message) = message else { break };
        match message.expect("wrong deletion browser result") {
            Message::Text(message) => {
                let frame: Value =
                    serde_json::from_str(&message).expect("wrong deletion browser JSON");
                assert_ne!(frame["type"], "dictation_result");
                assert_ne!(frame["type"], "dictation_empty");
                assert_ne!(frame["type"], "dictation_failed");
                assert_ne!(frame["type"], "dictation_cancelled");
                assert_ne!(frame, json!({"type":"state","state":"ready"}));
                assert_ne!(frame["operation_id"], operation_id);
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider remained connected after wrong deletion item"
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn dictation_transcription_failure_waits_for_exact_item_deletion() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-dictation-transcription-failure",
        "Reply A",
    );
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (delete_seen_sender, delete_seen_receiver) = tokio::sync::oneshot::channel();
    let (release_delete_sender, release_delete_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "dictation-transcription-failure").await;
        loop {
            if next_fake_realtime_json(&mut socket, "failed dictation commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"failed-dictation-item",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.failed",
                "item_id":"failed-dictation-item"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send failed dictation event");
        }
        let delete = loop {
            let event = next_fake_realtime_json(&mut socket, "failed dictation delete").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "failed-dictation-item");
        delete_seen_sender
            .send(())
            .expect("signal failed dictation delete");
        release_delete_receiver
            .await
            .expect("release failed dictation delete");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"failed-dictation-item",
                    "event_id":"provider-failed-dictation-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge failed dictation delete");
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select failed dictation mode");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "failed dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start failed dictation");
    let operation = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_operation",
        "failed operation",
    )
    .await;
    let operation_id = operation["operation_id"]
        .as_str()
        .expect("failed operation ID")
        .to_owned();
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish failed dictation");

    tokio::time::timeout(Duration::from_millis(500), delete_seen_receiver)
        .await
        .expect("failed dictation delete timeout")
        .expect("failed dictation delete signal");
    let pre_delete_deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while tokio::time::Instant::now() < pre_delete_deadline {
        let remaining = pre_delete_deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("pre-delete failure JSON");
        assert_ne!(frame["type"], "dictation_failed");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
    }
    release_delete_sender
        .send(())
        .expect("release failed dictation delete acknowledgement");
    let failed = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_failed",
        "failed dictation terminal",
    )
    .await;
    assert_eq!(failed["operation_id"], operation_id);
    assert_eq!(
        failed["message"],
        "Speech transcription failed. The draft was not changed."
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "failed dictation ready").await,
        json!({"type":"state","state":"ready"})
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn uncorrelated_error_during_dictation_delete_fails_closed() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-dictation-delete-error",
        "Reply A",
    );
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
        bind_fake_realtime_context(&mut socket, "dictation-delete-error").await;
        loop {
            if next_fake_realtime_json(&mut socket, "delete-error commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"delete-error-item",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"delete-error-item",
                "transcript":"Delete error draft"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance delete-error dictation");
        }
        let delete = loop {
            let event = next_fake_realtime_json(&mut socket, "delete-error request").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "delete-error-item");
        socket
            .send(Message::Text(
                json!({
                    "type":"error",
                    "error":{
                        "type":"server_error",
                        "message":"delete status is unknown"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send uncorrelated delete error");
        let closed = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("delete error did not close provider socket");
        matches!(closed, Some(Ok(Message::Close(_))) | None)
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select delete-error dictation mode");
    let _ = next_browser_json_of_type(
        &mut runtime.browser,
        "voice_mode",
        "delete-error dictation mode",
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start delete-error dictation");
    let operation = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_operation",
        "delete-error operation",
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation["operation_id"]})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish delete-error dictation");

    let error = loop {
        let frame = next_browser_json(&mut runtime.browser, "delete-error browser frame").await;
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_failed");
        assert_ne!(frame["type"], "dictation_cancelled");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
        if frame["type"] == "error" {
            break frame;
        }
    };
    assert_eq!(
        error,
        json!({
            "type":"error",
            "message":"Realtime rejected dictation item deletion. Reconnecting safely."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("delete-error browser close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider remained connected after ambiguous dictation deletion"
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn missing_dictation_delete_acknowledgement_times_out_fail_closed() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-dictation-delete-timeout",
        "Reply A",
    );
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
        bind_fake_realtime_context(&mut socket, "dictation-delete-timeout").await;
        loop {
            if next_fake_realtime_json(&mut socket, "delete-timeout commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"delete-timeout-item",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"delete-timeout-item",
                "transcript":"Delete timeout draft"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance delete-timeout dictation");
        }
        let delete = loop {
            let event = next_fake_realtime_json(&mut socket, "delete-timeout request").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "delete-timeout-item");
        let closed = tokio::time::timeout(Duration::from_secs(6), socket.next())
            .await
            .expect("delete acknowledgement timeout did not close provider socket");
        matches!(closed, Some(Ok(Message::Close(_))) | None)
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select delete-timeout dictation mode");
    let _ = next_browser_json_of_type(
        &mut runtime.browser,
        "voice_mode",
        "delete-timeout dictation mode",
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start delete-timeout dictation");
    let operation = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_operation",
        "delete-timeout operation",
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation["operation_id"]})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish delete-timeout dictation");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    let error = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "dictation delete timeout browser error"
        );
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("dictation delete timeout browser frame timeout")
            .expect("dictation delete timeout browser frame")
            .expect("dictation delete timeout browser result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("delete-timeout browser JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_failed");
        assert_ne!(frame["type"], "dictation_cancelled");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
        if frame["type"] == "error" {
            break frame;
        }
    };
    assert_eq!(
        error,
        json!({
            "type":"error",
            "message":"Realtime did not acknowledge dictation item deletion. Reconnecting safely."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("delete-timeout browser close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider remained connected after missing deletion acknowledgement"
    );
}

#[tokio::test]
#[cfg(unix)]
async fn dictation_transcription_timeout_deletes_item_before_failure() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-dictation-transcription-timeout",
        "Reply A",
    );
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
        bind_fake_realtime_context(&mut socket, "dictation-transcription-timeout").await;
        loop {
            if next_fake_realtime_json(&mut socket, "transcription-timeout commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        acknowledge_fake_audio_commit(&mut socket, "transcription-timeout-item").await;
        let delete = tokio::time::timeout(Duration::from_secs(31), async {
            loop {
                let message = socket
                    .next()
                    .await
                    .expect("transcription-timeout delete frame")
                    .expect("transcription-timeout delete result");
                let Message::Text(message) = message else {
                    continue;
                };
                let event: Value =
                    serde_json::from_str(&message).expect("transcription-timeout delete JSON");
                if event["type"] == "conversation.item.delete" {
                    break event;
                }
            }
        })
        .await
        .expect("transcription timeout did not request deletion");
        assert_eq!(delete["item_id"], "transcription-timeout-item");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"transcription-timeout-item",
                    "event_id":"provider-transcription-timeout-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge transcription-timeout deletion");
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    let operation_id = start_and_commit_bound_dictation(&mut runtime.browser, &summary_id, 1).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(31);
    let failed = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "transcription-timeout failure deadline"
        );
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("transcription-timeout browser frame timeout")
            .expect("transcription-timeout browser frame")
            .expect("transcription-timeout browser result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value =
            serde_json::from_str(&message).expect("transcription-timeout browser JSON");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
        if frame["type"] == "dictation_failed" {
            break frame;
        }
    };
    assert_eq!(failed["operation_id"], operation_id);
    assert_eq!(
        failed["message"],
        "Speech transcription timed out. The draft was not changed."
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "transcription-timeout ready").await,
        json!({"type":"state","state":"ready"})
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
async fn provider_disconnect_during_dictation_delete_emits_no_terminal() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-dictation-delete-disconnect",
        "Reply A",
    );
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
        bind_fake_realtime_context(&mut socket, "dictation-delete-disconnect").await;
        loop {
            if next_fake_realtime_json(&mut socket, "disconnect dictation commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"delete-disconnect-item",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"delete-disconnect-item",
                "transcript":"Disconnect before deletion"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance disconnect dictation");
        }
        let delete = loop {
            let event = next_fake_realtime_json(&mut socket, "disconnect dictation delete").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "delete-disconnect-item");
        socket.close(None).await.expect("close fake Realtime");
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    let operation_id = start_and_commit_bound_dictation(&mut runtime.browser, &summary_id, 1).await;
    loop {
        let message = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
            .await
            .expect("delete disconnect browser close timeout");
        let Some(message) = message else { break };
        match message.expect("delete disconnect browser result") {
            Message::Text(message) => {
                let frame: Value =
                    serde_json::from_str(&message).expect("delete disconnect browser JSON");
                assert_ne!(frame["type"], "dictation_result");
                assert_ne!(frame["type"], "dictation_empty");
                assert_ne!(frame["type"], "dictation_failed");
                assert_ne!(frame["type"], "dictation_cancelled");
                assert_ne!(frame, json!({"type":"state","state":"ready"}));
                assert_ne!(frame["operation_id"], operation_id);
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn cancelled_dictation_ignores_late_transcription() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-cancelled-dictation",
        "Reply A",
    );
    let app_port = TcpListener::bind("localhost:0")
        .expect("reserve app port")
        .local_addr()
        .expect("app address")
        .port();
    let realtime_listener = tokio::net::TcpListener::bind("localhost:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (release_commit_sender, release_commit_receiver) = tokio::sync::oneshot::channel();
    let (cancel_delete_seen_sender, cancel_delete_seen_receiver) = tokio::sync::oneshot::channel();
    let (release_cancel_delete_sender, release_cancel_delete_receiver) =
        tokio::sync::oneshot::channel();
    let (replay_sender, replay_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "cancelled-dictation").await;
        loop {
            let message = socket.next().await.expect("control event").expect("event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("event JSON");
            if event["type"] == "input_audio_buffer.commit" {
                break;
            }
        }
        release_commit_receiver
            .await
            .expect("release cancelled commit acknowledgment");
        socket
            .send(Message::Text(
                json!({
                    "type":"input_audio_buffer.committed",
                    "item_id":"late-item",
                    "previous_item_id":null
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send late committed item");
        let delete = loop {
            let event =
                next_fake_realtime_json(&mut socket, "cancelled committed item delete").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "late-item");
        cancel_delete_seen_sender
            .send(())
            .expect("signal cancelled committed item delete");
        release_cancel_delete_receiver
            .await
            .expect("release cancelled committed item delete");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"late-item",
                    "event_id":"provider-cancelled-commit-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge cancelled committed item delete");
        tokio::time::sleep(Duration::from_millis(100)).await;
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"late-item",
                    "transcript":"This must not be inserted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send late transcript");
        replay_receiver
            .await
            .expect("release cancelled item replay");
        for event in [
            json!({
                "type":"conversation.item.input_audio_transcription.delta",
                "item_id":"late-item",
                "delta":"This must not be previewed"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"late-item",
                "transcript":"This must not become a live transcript"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.failed",
                "item_id":"late-item"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"late-item",
                "transcript":"This duplicate must also be ignored"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send cancelled item replay");
        }
        let mut second_clears = 0;
        loop {
            let message = socket
                .next()
                .await
                .expect("second dictation event")
                .expect("second event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("second event JSON");
            second_clears += usize::from(event["type"] == "input_audio_buffer.clear");
            if event["type"] == "input_audio_buffer.commit" {
                break;
            }
        }
        assert_eq!(second_clears, 1);
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"second-item",
                "previous_item_id":"late-item"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.delta",
                "item_id":"second-item",
                "delta":"Partial second dictation"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send second dictation event");
        }
        let mut saw_second_cancel_clear = false;
        let second_delete = loop {
            let event = next_fake_realtime_json(&mut socket, "second dictation delete").await;
            saw_second_cancel_clear |= event["type"] == "input_audio_buffer.clear";
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert!(saw_second_cancel_clear);
        assert_eq!(second_delete["item_id"], "second-item");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"second-item",
                    "event_id":"provider-second-item-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge second dictation delete");
        let mut third_clears = 0;
        loop {
            let message = socket
                .next()
                .await
                .expect("third dictation event")
                .expect("third event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("third event JSON");
            third_clears += usize::from(event["type"] == "input_audio_buffer.clear");
            if event["type"] == "input_audio_buffer.commit" {
                break;
            }
        }
        assert_eq!(third_clears, 1);
        for event in [
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"second-item",
                "transcript":"Cancelled second dictation"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.failed",
                "item_id":"second-item"
            }),
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"third-item",
                "previous_item_id":"second-item"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"third-item",
                "transcript":"Third dictation"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send third dictation result");
        }
        let third_delete = loop {
            let event = next_fake_realtime_json(&mut socket, "third dictation delete").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(third_delete["item_id"], "third-item");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"third-item",
                    "event_id":"provider-third-item-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge third dictation delete");
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let directory = tempfile::tempdir().expect("temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        json!({
            "listenHost":"localhost",
            "listenPort":app_port,
            "openaiBaseUrl":format!("ws://localhost:{realtime_port}"),
            "paseoBin":fake_paseo_path,
            "secretProvider":"environment",
            "paseoHosts":[
                {"id":"local","label":"Local","target":null,"default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"}
            ],
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
        .envs(essential_os_env())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("OPENAI_API_KEY", "test-key")
        .env("PASEO_PASSWORD", "test-password")
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Rust runtime");
    let _guard = ChildGuard(child);
    let health_url = format!("http://localhost:{app_port}/healthz");
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (mut browser, _) = connect_async(format!("ws://localhost:{app_port}/ws"))
        .await
        .expect("browser websocket");
    initialize_browser_protocol(&mut browser, "initial frame").await;
    let summary_id = bind_browser_context(&mut browser).await;
    for control in [
        json!({"type":"set_voice_mode","mode":"dictation"}),
        json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id}),
    ] {
        browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send dictation control");
    }
    let _ = browser.next().await.expect("mode frame").expect("mode");
    let operation = browser
        .next()
        .await
        .expect("dictation operation frame")
        .expect("dictation operation");
    let Message::Text(operation) = operation else {
        panic!("expected dictation operation text frame");
    };
    let operation: Value = serde_json::from_str(&operation).expect("dictation operation JSON");
    assert_eq!(operation["type"], "dictation_operation");
    let recording_operation_id = operation["operation_id"].clone();
    browser
        .send(Message::Binary(test_pcm(b"mic").into()))
        .await
        .expect("send microphone audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("ignore live abort before dictation cancellation");
    browser
        .send(Message::Text(
            json!({
                "type":"cancel_dictation",
                "operation_id":recording_operation_id
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("cancel recording");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let mut cancellation_count = 0;
    let mut ready_count = 0;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("recording cancel frame JSON");
        if frame["type"] == "dictation_cancelled" {
            cancellation_count += 1;
            assert_eq!(frame["operation_id"], recording_operation_id);
        }
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
    }
    assert_eq!(cancellation_count, 1);
    assert_eq!(ready_count, 1);

    for _ in 0..2 {
        browser
            .send(Message::Text(
                json!({"type":"cancel_dictation"}).to_string().into(),
            ))
            .await
            .expect("repeat idle cancel");
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("idle cancel frame JSON");
        assert_ne!(frame["type"], "dictation_cancelled");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
        assert_ne!(frame, json!({"type":"state","state":"cancelling"}));
    }

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start pending commit dictation");
    let pending_operation = browser
        .next()
        .await
        .expect("pending operation frame")
        .expect("pending operation");
    let Message::Text(pending_operation) = pending_operation else {
        panic!("expected pending operation text frame");
    };
    let pending_operation: Value =
        serde_json::from_str(&pending_operation).expect("pending operation JSON");
    assert_eq!(pending_operation["type"], "dictation_operation");
    let pending_operation_id = pending_operation["operation_id"].clone();
    browser
        .send(Message::Binary(test_pcm(b"pending").into()))
        .await
        .expect("send pending microphone audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":pending_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish pending dictation");
    browser
        .send(Message::Text(
            json!({
                "type":"cancel_dictation",
                "operation_id":pending_operation_id
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("cancel dictation");

    loop {
        let message = tokio::time::timeout(Duration::from_millis(300), browser.next())
            .await
            .expect("cancelling state timeout")
            .expect("cancelling state frame")
            .expect("cancelling state");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("cancelling state JSON");
        assert_ne!(frame["type"], "dictation_cancelled");
        if frame == json!({"type":"state","state":"cancelling"}) {
            break;
        }
    }
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":3,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("attempt rapid restart");
    let rejected = tokio::time::timeout(Duration::from_millis(300), browser.next())
        .await
        .expect("rapid restart rejection timeout")
        .expect("rapid restart rejection frame")
        .expect("rapid restart rejection");
    let Message::Text(rejected) = rejected else {
        panic!("expected rapid restart rejection text frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&rejected).expect("rapid restart rejection JSON"),
        json!({
            "type":"recording_rejected",
            "mode":"dictation",
            "recording_id":3,
            "message":"Recording could not start. Try again."
        })
    );
    release_commit_sender
        .send(())
        .expect("release cancelled commit acknowledgment");

    tokio::time::timeout(Duration::from_millis(500), cancel_delete_seen_receiver)
        .await
        .expect("cancelled committed item delete timeout")
        .expect("cancelled committed item delete signal");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "cancellation completed before committed item deletion"
    );
    release_cancel_delete_sender
        .send(())
        .expect("release cancelled committed item delete acknowledgement");

    assert_eq!(
        next_browser_json(&mut browser, "cancelled commit terminal").await,
        json!({
            "type":"dictation_cancelled",
            "operation_id":pending_operation_id
        })
    );
    assert_eq!(
        next_browser_json(&mut browser, "cancelled commit ready state").await,
        json!({"type":"state","state":"ready"})
    );
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("switch to live response after cancellation");
    let live_mode = tokio::time::timeout(Duration::from_millis(300), browser.next())
        .await
        .expect("live mode timeout")
        .expect("live mode frame")
        .expect("live mode");
    let Message::Text(live_mode) = live_mode else {
        panic!("expected live mode text frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&live_mode).expect("live mode JSON"),
        json!({"type":"voice_mode","mode":"live_response"})
    );
    replay_sender
        .send(())
        .expect("release cancelled item replay");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("cancelled replay frame JSON");
        assert_ne!(frame["type"], "user_transcript");
        assert_ne!(frame["type"], "dictation_preview");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_failed");
    }
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("restore dictation mode");
    let dictation_mode = tokio::time::timeout(Duration::from_millis(300), browser.next())
        .await
        .expect("dictation mode timeout")
        .expect("dictation mode frame")
        .expect("dictation mode");
    let Message::Text(dictation_mode) = dictation_mode else {
        panic!("expected dictation mode text frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&dictation_mode).expect("dictation mode JSON"),
        json!({"type":"voice_mode","mode":"dictation"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":4,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start item-correlated dictation");
    let item_operation = browser
        .next()
        .await
        .expect("item operation frame")
        .expect("item operation");
    let Message::Text(item_operation) = item_operation else {
        panic!("expected item operation text frame");
    };
    let item_operation: Value = serde_json::from_str(&item_operation).expect("item operation JSON");
    assert_eq!(item_operation["type"], "dictation_operation");
    let item_operation_id = item_operation["operation_id"].clone();
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":item_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish item-correlated recording");
    loop {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("item preview timeout")
            .expect("item preview frame")
            .expect("item preview");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("item preview JSON");
        if frame["type"] == "dictation_preview" {
            assert_eq!(frame["operation_id"], item_operation_id);
            break;
        }
    }
    browser
        .send(Message::Text(
            json!({"type":"cancel_dictation","operation_id":item_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel item-correlated dictation");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let mut cancellation_count = 0;
    let mut ready_count = 0;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("item cancel frame JSON");
        if frame["type"] == "dictation_cancelled" {
            cancellation_count += 1;
            assert_eq!(frame["operation_id"], item_operation_id);
        }
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
        assert_ne!(frame["type"], "dictation_result");
    }
    assert_eq!(cancellation_count, 1);
    assert_eq!(ready_count, 1);
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":5,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start third dictation");
    let third_operation = next_browser_json(&mut browser, "third dictation operation").await;
    let third_operation_id = third_operation["operation_id"]
        .as_str()
        .expect("third dictation operation ID")
        .to_owned();
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":third_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish third dictation");
    let mut third_result = None;
    for _ in 0..5 {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("third result timeout")
            .expect("third result frame")
            .expect("third result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("third result JSON");
        if frame["type"] == "dictation_result" {
            third_result = Some(frame);
            break;
        }
    }
    assert_eq!(
        third_result.expect("third dictation result")["text"],
        "Third dictation"
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn cancelled_cleanup_emits_one_correlated_terminal_and_ignores_late_result() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-cancelled-cleanup", "Reply A");
    let app_port = TcpListener::bind("localhost:0")
        .expect("reserve app port")
        .local_addr()
        .expect("app address")
        .port();
    let realtime_listener = tokio::net::TcpListener::bind("localhost:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let cleanup_listener = tokio::net::TcpListener::bind("localhost:0")
        .await
        .expect("fake cleanup listener");
    let cleanup_port = cleanup_listener
        .local_addr()
        .expect("cleanup address")
        .port();
    let (cleanup_started_sender, cleanup_started_receiver) = tokio::sync::oneshot::channel();
    let (cancel_seen_sender, cancel_seen_receiver) = tokio::sync::oneshot::channel();
    let (release_delete_sender, release_delete_receiver) = tokio::sync::oneshot::channel();
    let (replay_sender, replay_receiver) = tokio::sync::oneshot::channel();
    let fake_cleanup = tokio::spawn(async move {
        let (mut stream, _) = cleanup_listener.accept().await.expect("cleanup accept");
        let mut request = vec![0_u8; 16_384];
        let length = stream
            .read(&mut request)
            .await
            .expect("read cleanup request");
        let request = String::from_utf8_lossy(&request[..length]);
        assert!(request.contains("Cleanup to cancel"));
        cleanup_started_sender
            .send(())
            .expect("signal cleanup start");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let body = r#"{"choices":[{"message":{"content":"Late cleanup result"}}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    });
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "cancelled-cleanup").await;
        let mut clears = 0;
        let mut commits = 0;
        while commits == 0 {
            let message = socket.next().await.expect("cleanup event").expect("event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("cleanup event JSON");
            clears += usize::from(event["type"] == "input_audio_buffer.clear");
            commits += usize::from(event["type"] == "input_audio_buffer.commit");
            assert_ne!(event["type"], "response.create");
        }
        assert_eq!(clears, 1);
        assert_eq!(commits, 1);
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"cleanup-cancel-item",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"cleanup-cancel-item",
                "transcript":"Cleanup to cancel"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send cleanup event");
        }
        let mut delete = None;
        loop {
            let message = socket
                .next()
                .await
                .expect("cleanup cancel event")
                .expect("cancel event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("cleanup cancel JSON");
            assert_ne!(event["type"], "input_audio_buffer.commit");
            if event["type"] == "conversation.item.delete" {
                assert!(
                    delete.is_none(),
                    "dictation item was deleted more than once"
                );
                assert_eq!(event["item_id"], "cleanup-cancel-item");
                delete = Some(event);
                continue;
            }
            if event["type"] == "input_audio_buffer.clear" {
                break;
            }
        }
        let delete = delete.expect("cleanup cancellation item delete");
        assert!(
            delete["event_id"]
                .as_str()
                .is_some_and(|event_id| { event_id.starts_with("dictation-delete-") })
        );
        cancel_seen_sender
            .send(())
            .expect("signal cleanup cancellation clear");
        release_delete_receiver
            .await
            .expect("release cleanup cancellation delete");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"cleanup-cancel-item",
                    "event_id":"provider-cleanup-cancel-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge cleanup cancellation delete");
        replay_receiver.await.expect("release cleanup item replay");
        for event in [
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"cleanup-cancel-item",
                "transcript":"Duplicate cleanup transcription"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.failed",
                "item_id":"cleanup-cancel-item"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send duplicate cleanup event");
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
    });

    let directory = tempfile::tempdir().expect("temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        json!({
            "listenHost":"localhost",
            "listenPort":app_port,
            "openaiBaseUrl":format!("ws://localhost:{realtime_port}"),
            "sparkBaseUrl":format!("http://localhost:{cleanup_port}/v1"),
            "paseoBin":fake_paseo_path,
            "secretProvider":"environment",
            "paseoHosts":[
                {"id":"local","label":"Local","target":null,"default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"}
            ],
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
        .envs(essential_os_env())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("OPENAI_API_KEY", "test-key")
        .env("PASEO_PASSWORD", "test-password")
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Rust runtime");
    let _guard = ChildGuard(child);
    let health_url = format!("http://localhost:{app_port}/healthz");
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (mut browser, _) = connect_async(format!("ws://localhost:{app_port}/ws"))
        .await
        .expect("browser websocket");
    initialize_browser_protocol(&mut browser, "initial frame").await;
    let summary_id = bind_browser_context(&mut browser).await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation");
    let _ = browser.next().await.expect("mode frame").expect("mode");
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start cleanup dictation");
    let operation = browser
        .next()
        .await
        .expect("cleanup operation frame")
        .expect("cleanup operation");
    let Message::Text(operation) = operation else {
        panic!("expected cleanup operation text frame");
    };
    let operation: Value = serde_json::from_str(&operation).expect("cleanup operation JSON");
    assert_eq!(operation["type"], "dictation_operation");
    let operation_id = operation["operation_id"].clone();
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish cleanup recording");
    loop {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("cleaning state timeout")
            .expect("cleaning state frame")
            .expect("cleaning state");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("cleaning state JSON");
        if frame == json!({"type":"state","state":"cleaning"}) {
            break;
        }
    }
    tokio::time::timeout(Duration::from_millis(500), cleanup_started_receiver)
        .await
        .expect("cleanup start timeout")
        .expect("cleanup start signal");
    browser
        .send(Message::Text(
            json!({"type":"cancel_dictation","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel cleanup");
    tokio::time::timeout(Duration::from_millis(500), cancel_seen_receiver)
        .await
        .expect("cleanup cancellation clear timeout")
        .expect("cleanup cancellation clear signal");
    let pre_delete_deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while tokio::time::Instant::now() < pre_delete_deadline {
        let remaining = pre_delete_deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("pre-delete cancel frame JSON");
        assert_ne!(frame["type"], "dictation_cancelled");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
    }
    release_delete_sender
        .send(())
        .expect("release cleanup cancellation delete acknowledgement");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(600);
    let mut cancellation_count = 0;
    let mut ready_count = 0;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("cleanup cancel frame JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        if frame["type"] == "dictation_cancelled" {
            cancellation_count += 1;
            assert_eq!(frame["operation_id"], operation_id);
        }
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
    }
    assert_eq!(cancellation_count, 1);
    assert_eq!(ready_count, 1);
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("switch mode after cleanup cancellation");
    let live_mode = tokio::time::timeout(Duration::from_millis(300), browser.next())
        .await
        .expect("cleanup live mode timeout")
        .expect("cleanup live mode frame")
        .expect("cleanup live mode");
    let Message::Text(live_mode) = live_mode else {
        panic!("expected cleanup live mode text frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&live_mode).expect("cleanup live mode JSON"),
        json!({"type":"voice_mode","mode":"live_response"})
    );
    replay_sender.send(()).expect("release cleanup item replay");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("cleanup replay frame JSON");
        assert_ne!(frame["type"], "user_transcript");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_failed");
    }
    fake_realtime.await.expect("fake Realtime task");
    fake_cleanup.await.expect("fake cleanup task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn host_change_cancels_recording_and_ignores_the_stale_release() {
    let app_port = TcpListener::bind("localhost:0")
        .expect("reserve app port")
        .local_addr()
        .expect("app address")
        .port();
    let realtime_listener = tokio::net::TcpListener::bind("localhost:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let cleanup_listener = tokio::net::TcpListener::bind("localhost:0")
        .await
        .expect("fake cleanup listener");
    let cleanup_port = cleanup_listener
        .local_addr()
        .expect("cleanup address")
        .port();
    let (cleanup_started_sender, cleanup_started_receiver) = tokio::sync::oneshot::channel();
    let (cleanup_finished_sender, cleanup_finished_receiver) = tokio::sync::oneshot::channel();
    let fake_cleanup = tokio::spawn(async move {
        let mut cleanup_started_sender = Some(cleanup_started_sender);
        let mut cleanup_finished_sender = Some(cleanup_finished_sender);
        for (index, cleaned) in ["Fresh dictation", "Late cleanup content"]
            .into_iter()
            .enumerate()
        {
            let (mut stream, _) = cleanup_listener.accept().await.expect("cleanup accept");
            let mut request = vec![0_u8; 16_384];
            let length = stream
                .read(&mut request)
                .await
                .expect("read cleanup request");
            let request = String::from_utf8_lossy(&request[..length]);
            assert!(request.contains(if index == 0 {
                "Fresh dictation"
            } else {
                "Cleanup in flight"
            }));
            if index == 1 {
                cleanup_started_sender
                    .take()
                    .expect("cleanup start sender")
                    .send(())
                    .expect("signal delayed cleanup");
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            let body = json!({"choices":[{"message":{"content":cleaned}}]}).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            if index == 0 {
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write first cleanup response");
            } else {
                let _ = stream.write_all(response.as_bytes()).await;
                cleanup_finished_sender
                    .take()
                    .expect("cleanup finish sender")
                    .send(())
                    .expect("signal completed cleanup");
            }
        }
    });
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "initial-thread-b").await;

        let mut clears = 0;
        let mut appends = 0;
        while clears < 2 || appends < 1 {
            let message = socket
                .next()
                .await
                .expect("recording event")
                .expect("event");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("recording event JSON");
            clears += usize::from(event["type"] == "input_audio_buffer.clear");
            appends += usize::from(event["type"] == "input_audio_buffer.append");
            assert_ne!(event["type"], "input_audio_buffer.commit");
        }
        bind_fake_realtime_context(&mut socket, "second-host").await;

        let mut fresh_clears = 0;
        let mut fresh_appends = 0;
        let mut commits = 0;
        while commits == 0 {
            let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("fresh operation event timeout")
                .expect("fresh operation event")
                .expect("fresh operation event result");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("fresh operation event JSON");
            fresh_clears += usize::from(event["type"] == "input_audio_buffer.clear");
            fresh_appends += usize::from(event["type"] == "input_audio_buffer.append");
            commits += usize::from(event["type"] == "input_audio_buffer.commit");
            assert_ne!(event["type"], "response.create");
        }
        assert_eq!(fresh_clears, 1);
        assert_eq!(fresh_appends, 1);
        assert_eq!(commits, 1);
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"fresh-recording-item",
                "previous_item_id":"stale-recording-item"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"fresh-recording-item",
                "transcript":"Fresh dictation"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send fresh dictation event");
        }
        acknowledge_fake_dictation_delete(
            &mut socket,
            "fresh-recording-item",
            "provider-fresh-recording-deleted",
        )
        .await;

        let mut pending_commit_clears = 0;
        let mut pending_commit_appends = 0;
        let mut pending_commit_commits = 0;
        while pending_commit_commits == 0 {
            let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("pending commit event timeout")
                .expect("pending commit event")
                .expect("pending commit event result");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("pending commit event JSON");
            pending_commit_clears += usize::from(event["type"] == "input_audio_buffer.clear");
            pending_commit_appends += usize::from(event["type"] == "input_audio_buffer.append");
            pending_commit_commits += usize::from(event["type"] == "input_audio_buffer.commit");
            assert_ne!(event["type"], "response.create");
        }
        assert_eq!(pending_commit_clears, 1);
        assert_eq!(pending_commit_appends, 1);
        assert_eq!(pending_commit_commits, 1);
        loop {
            let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("pending commit host-change clear timeout")
                .expect("pending commit host-change clear")
                .expect("pending commit host-change clear result");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value =
                serde_json::from_str(&message).expect("pending commit host-change clear JSON");
            assert_ne!(event["type"], "input_audio_buffer.commit");
            if event["type"] == "input_audio_buffer.clear" {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"pending-commit-item",
                "previous_item_id":"fresh-recording-item"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"pending-commit-item",
                "transcript":"Stale pending commit content"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send stale pending commit event");
        }
        acknowledge_fake_dictation_delete(
            &mut socket,
            "pending-commit-item",
            "provider-pending-commit-deleted",
        )
        .await;
        bind_fake_realtime_context(&mut socket, "first-host-after-pending-commit").await;

        let mut awaiting_clears = 0;
        let mut awaiting_appends = 0;
        let mut awaiting_commits = 0;
        while awaiting_commits == 0 {
            let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("awaiting transcription event timeout")
                .expect("awaiting transcription event")
                .expect("awaiting transcription event result");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value =
                serde_json::from_str(&message).expect("awaiting transcription event JSON");
            awaiting_clears += usize::from(event["type"] == "input_audio_buffer.clear");
            awaiting_appends += usize::from(event["type"] == "input_audio_buffer.append");
            awaiting_commits += usize::from(event["type"] == "input_audio_buffer.commit");
            assert_ne!(event["type"], "response.create");
        }
        assert_eq!(awaiting_clears, 1);
        assert_eq!(awaiting_appends, 1);
        assert_eq!(awaiting_commits, 1);
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"awaiting-transcription-item",
                "previous_item_id":"pending-commit-item"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.delta",
                "item_id":"awaiting-transcription-item",
                "delta":"Partial"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send awaiting transcription event");
        }

        loop {
            let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("host-change clear timeout")
                .expect("host-change clear")
                .expect("host-change clear result");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("host-change clear JSON");
            assert_ne!(event["type"], "input_audio_buffer.commit");
            if event["type"] == "input_audio_buffer.clear" {
                break;
            }
        }
        acknowledge_fake_dictation_delete(
            &mut socket,
            "awaiting-transcription-item",
            "provider-awaiting-transcription-deleted",
        )
        .await;
        for event in [
            json!({
                "type":"conversation.item.input_audio_transcription.delta",
                "item_id":"awaiting-transcription-item",
                "delta":" stale"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"awaiting-transcription-item",
                "transcript":"Stale transcription content"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send stale awaiting transcription event");
        }
        bind_fake_realtime_context(&mut socket, "second-host-after-transcription").await;
        let mut cleanup_clears = 0;
        let mut cleanup_appends = 0;
        let mut cleanup_commits = 0;
        while cleanup_commits == 0 {
            let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
                .await
                .expect("cleanup operation event timeout")
                .expect("cleanup operation event")
                .expect("cleanup operation event result");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value =
                serde_json::from_str(&message).expect("cleanup operation event JSON");
            cleanup_clears += usize::from(event["type"] == "input_audio_buffer.clear");
            cleanup_appends += usize::from(event["type"] == "input_audio_buffer.append");
            cleanup_commits += usize::from(event["type"] == "input_audio_buffer.commit");
            assert_ne!(event["type"], "response.create");
        }
        assert_eq!(cleanup_clears, 1);
        assert_eq!(cleanup_appends, 1);
        assert_eq!(cleanup_commits, 1);
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"cleanup-item",
                "previous_item_id":"awaiting-transcription-item"
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"cleanup-item",
                "transcript":"Cleanup in flight"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send cleanup operation event");
        }
        let mut cleanup_delete = None;
        loop {
            let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("cleanup host-change clear timeout")
                .expect("cleanup host-change clear")
                .expect("cleanup host-change clear result");
            let Message::Text(message) = message else {
                continue;
            };
            let event: Value = serde_json::from_str(&message).expect("cleanup clear JSON");
            assert_ne!(event["type"], "input_audio_buffer.commit");
            if event["type"] == "conversation.item.delete" {
                assert_eq!(event["item_id"], "cleanup-item");
                cleanup_delete = Some(event);
                continue;
            }
            if event["type"] == "input_audio_buffer.clear" {
                break;
            }
        }
        assert!(cleanup_delete.is_some(), "cleanup item delete request");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"cleanup-item",
                    "event_id":"provider-cleanup-item-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge cleanup item delete");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"cleanup-item",
                    "transcript":"Duplicate stale cleanup content"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send duplicate cleanup completion");
        tokio::time::sleep(Duration::from_millis(700)).await;
    });

    let directory = tempfile::tempdir().expect("temporary directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fake_paseo_path = directory.path().join("fake-paseo");
    std::fs::write(
        &fake_paseo_path,
        concat!(
            "#!/bin/sh\n",
            "case \"$1\" in\n",
            "  ls) printf '%s\\n' '[{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"},{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Agent B\",\"status\":\"idle\"}]' ;;\n",
            "  logs) printf 'Fresh reply from %s\\n' \"$2\" ;;\n",
            "  *) printf '%s\\n' '[]' ;;\n",
            "esac\n",
        ),
    )
    .expect("write fake Paseo executable");
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mut permissions = std::fs::metadata(&fake_paseo_path)
            .expect("fake Paseo metadata")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");
    }
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        json!({
            "listenHost":"localhost",
            "listenPort":app_port,
            "openaiBaseUrl":format!("ws://localhost:{realtime_port}"),
            "sparkBaseUrl":format!("http://localhost:{cleanup_port}/v1"),
            "paseoBin":fake_paseo_path,
            "secretProvider":"environment",
            "paseoHosts":[
                {"id":"first","label":"First","target":null,"default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"},
                {"id":"second","label":"Second","target":"second.example:6767","default":false,"defaultCwd":"~/dev","defaultProvider":"opencode/model"}
            ],
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
        .envs(essential_os_env())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", directory.path())
        .env("OPENAI_API_KEY", "test-key")
        .env("PASEO_PASSWORD", "test-password")
        .env("PASEO_VOICE_CONFIG", &config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Rust runtime");
    let _guard = ChildGuard(child);
    let health_url = format!("http://localhost:{app_port}/healthz");
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (mut browser, _) = connect_async(format!("ws://localhost:{app_port}/ws"))
        .await
        .expect("browser websocket");
    initialize_browser_protocol(&mut browser, "initial frame").await;
    let initial_summary_id = bind_browser_context(&mut browser).await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation");
    let _ = browser.next().await.expect("mode frame").expect("mode");
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":initial_summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start recording");
    let operation = tokio::time::timeout(Duration::from_millis(500), browser.next())
        .await
        .expect("recording operation timeout")
        .expect("recording operation frame")
        .expect("recording operation");
    let Message::Text(operation) = operation else {
        panic!("expected recording operation text frame");
    };
    let operation: Value = serde_json::from_str(&operation).expect("recording operation JSON");
    assert_eq!(operation["type"], "dictation_operation");
    let operation_id = operation["operation_id"]
        .as_str()
        .expect("recording operation ID")
        .to_owned();
    browser
        .send(Message::Binary(test_pcm(b"mic").into()))
        .await
        .expect("send recording audio");
    browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change host");

    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut cancellation_count = 0;
    let mut ready_count = 0;
    let mut selected_host = None;
    let mut bound_context = None;
    let mut cancellation_seen = false;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("host change frame JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_preview");
        if frame["type"] == "dictation_cancelled" {
            cancellation_count += 1;
            cancellation_seen = true;
            assert_eq!(
                frame,
                json!({
                    "type":"dictation_cancelled",
                    "operation_id":operation_id
                })
            );
        }
        if frame == json!({"type":"state","state":"ready"}) {
            ready_count += 1;
        }
        if frame["type"] == "host_state" {
            assert!(
                cancellation_seen,
                "cancellation must precede the new host presentation"
            );
            selected_host = frame["selected_host_id"].as_str().map(str::to_owned);
        }
        if frame["type"] == "dashboard_state" {
            assert!(
                cancellation_seen,
                "cancellation must precede the new dashboard"
            );
        }
        if frame["type"] == "dashboard_state" {
            bound_context = Some(frame["bound_context"].clone());
        }
    }
    assert_eq!(cancellation_count, 1);
    assert_eq!(ready_count, 1);
    assert_eq!(selected_host.as_deref(), Some("second"));
    assert_eq!(bound_context, Some(Value::Null));

    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("send stale release");
    browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("request fresh bound context");
    let mut fresh_bound_context = None;
    let mut binding_ready = false;
    while fresh_bound_context.is_none() || !binding_ready {
        let message = tokio::time::timeout(Duration::from_secs(2), browser.next())
            .await
            .expect("fresh bound context timeout")
            .expect("fresh bound context frame")
            .expect("fresh bound context");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("fresh bound context JSON");
        assert_ne!(frame["type"], "dictation_result");
        if frame["type"] == "dashboard_state" && frame["bound_context"].is_object() {
            assert_eq!(frame["selected_host_id"], "second");
            fresh_bound_context = Some(frame["bound_context"].clone());
        }
        binding_ready |= frame == json!({"type":"state","state":"ready"});
    }
    assert!(
        fresh_bound_context
            .as_ref()
            .and_then(|context| context["summary_id"].as_str())
            .is_some_and(|summary_id| !summary_id.is_empty())
    );
    let fresh_summary_id = fresh_bound_context
        .as_ref()
        .and_then(|context| context["summary_id"].as_str())
        .expect("fresh summary ID")
        .to_owned();
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":fresh_summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start fresh operation");
    let fresh_operation = tokio::time::timeout(Duration::from_millis(500), browser.next())
        .await
        .expect("fresh operation timeout")
        .expect("fresh operation frame")
        .expect("fresh operation");
    let Message::Text(fresh_operation) = fresh_operation else {
        panic!("expected fresh operation text frame");
    };
    let fresh_operation: Value =
        serde_json::from_str(&fresh_operation).expect("fresh operation JSON");
    assert_eq!(fresh_operation["type"], "dictation_operation");
    assert_ne!(fresh_operation["operation_id"], operation_id);
    let fresh_operation_id = fresh_operation["operation_id"].clone();
    browser
        .send(Message::Binary(test_pcm(b"new").into()))
        .await
        .expect("send fresh audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":fresh_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish fresh operation");
    let mut result = None;
    for _ in 0..4 {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("fresh result timeout")
            .expect("fresh result frame")
            .expect("fresh result");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("fresh result JSON");
        if frame["type"] == "dictation_result" {
            result = Some(frame);
            break;
        }
    }
    let result = result.expect("fresh dictation result");
    assert_eq!(result["operation_id"], fresh_operation_id);
    assert_eq!(result["text"], "Fresh dictation");
    loop {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("fresh ready timeout")
            .expect("fresh ready frame")
            .expect("fresh ready");
        let Message::Text(message) = message else {
            continue;
        };
        if serde_json::from_str::<Value>(&message).expect("fresh ready JSON")
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":3,"summary_id":fresh_summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start pending commit operation");
    let pending_commit_operation = tokio::time::timeout(Duration::from_millis(500), browser.next())
        .await
        .expect("pending commit operation timeout")
        .expect("pending commit operation frame")
        .expect("pending commit operation");
    let Message::Text(pending_commit_operation) = pending_commit_operation else {
        panic!("expected pending commit operation text frame");
    };
    let pending_commit_operation: Value =
        serde_json::from_str(&pending_commit_operation).expect("pending commit operation JSON");
    assert_eq!(pending_commit_operation["type"], "dictation_operation");
    let pending_commit_operation_id = pending_commit_operation["operation_id"].clone();
    browser
        .send(Message::Binary(test_pcm(b"committed").into()))
        .await
        .expect("send pending commit audio");
    browser
        .send(Message::Text(
            json!({
                "type":"ptt_end",
                "operation_id":pending_commit_operation_id
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("finish pending commit recording");
    loop {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("pending commit state timeout")
            .expect("pending commit state frame")
            .expect("pending commit state");
        let Message::Text(message) = message else {
            continue;
        };
        if serde_json::from_str::<Value>(&message).expect("pending commit state JSON")
            == json!({"type":"state","state":"transcribing"})
        {
            break;
        }
    }
    browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"first"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change host before provider commit correlation");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut cancellation_count = 0;
    let mut ready_count = 0;
    let mut selected_host = None;
    let mut cancellation_started = false;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("pending host change frame JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_preview");
        if frame["type"] == "dictation_cancelled" {
            cancellation_count += 1;
            assert_eq!(
                frame,
                json!({
                    "type":"dictation_cancelled",
                    "operation_id":pending_commit_operation_id
                })
            );
        }
        if frame == json!({"type":"state","state":"ready"}) {
            ready_count += 1;
        }
        if frame == json!({"type":"state","state":"cancelling"}) {
            cancellation_started = true;
        }
        if frame["type"] == "host_state" {
            assert!(
                cancellation_started,
                "pending cancellation must precede the new host presentation"
            );
            selected_host = frame["selected_host_id"].as_str().map(str::to_owned);
        }
        if frame["type"] == "dashboard_state" {
            assert!(
                cancellation_started,
                "pending cancellation must precede the new dashboard"
            );
        }
    }
    assert_eq!(cancellation_count, 1);
    assert_eq!(ready_count, 1);
    assert_eq!(selected_host.as_deref(), Some("first"));
    browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("request post-commit bound context");
    let mut pending_commit_bound_context = None;
    let mut binding_ready = false;
    while pending_commit_bound_context.is_none() || !binding_ready {
        let message = tokio::time::timeout(Duration::from_secs(2), browser.next())
            .await
            .expect("post-commit bound context timeout")
            .expect("post-commit bound context frame")
            .expect("post-commit bound context");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("post-commit bound context JSON");
        assert_ne!(frame["type"], "dictation_result");
        if frame["type"] == "dashboard_state" && frame["bound_context"].is_object() {
            assert_eq!(frame["selected_host_id"], "first");
            pending_commit_bound_context = Some(frame["bound_context"].clone());
        }
        binding_ready |= frame == json!({"type":"state","state":"ready"});
    }
    let pending_commit_summary_id = pending_commit_bound_context
        .as_ref()
        .and_then(|context| context["summary_id"].as_str())
        .expect("post-commit summary ID")
        .to_owned();

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":4,"summary_id":pending_commit_summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start awaiting transcription operation");
    let awaiting_operation = tokio::time::timeout(Duration::from_millis(500), browser.next())
        .await
        .expect("awaiting transcription operation timeout")
        .expect("awaiting transcription operation frame")
        .expect("awaiting transcription operation");
    let Message::Text(awaiting_operation) = awaiting_operation else {
        panic!("expected awaiting transcription operation text frame");
    };
    let awaiting_operation: Value =
        serde_json::from_str(&awaiting_operation).expect("awaiting transcription operation JSON");
    assert_eq!(awaiting_operation["type"], "dictation_operation");
    let awaiting_operation_id = awaiting_operation["operation_id"].clone();
    browser
        .send(Message::Binary(test_pcm(b"pending").into()))
        .await
        .expect("send awaiting transcription audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":awaiting_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish awaiting transcription recording");
    loop {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("partial transcription timeout")
            .expect("partial transcription frame")
            .expect("partial transcription");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("partial transcription JSON");
        if frame["type"] == "dictation_preview" {
            assert_eq!(frame["operation_id"], awaiting_operation_id);
            break;
        }
    }
    browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change host while awaiting transcription");

    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut cancellation_count = 0;
    let mut ready_count = 0;
    let mut selected_host = None;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("awaiting host change frame JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_preview");
        if frame["type"] == "dictation_cancelled" {
            cancellation_count += 1;
            assert_eq!(
                frame,
                json!({
                    "type":"dictation_cancelled",
                    "operation_id":awaiting_operation_id
                })
            );
        }
        if frame == json!({"type":"state","state":"ready"}) {
            ready_count += 1;
        }
        if frame["type"] == "host_state" {
            selected_host = frame["selected_host_id"].as_str().map(str::to_owned);
        }
    }
    assert_eq!(cancellation_count, 1);
    assert_eq!(ready_count, 1);
    assert_eq!(selected_host.as_deref(), Some("second"));
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":awaiting_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("send stale awaiting transcription release");
    let late_result = tokio::time::timeout(Duration::from_millis(150), browser.next()).await;
    if let Ok(Some(Ok(Message::Text(message)))) = late_result {
        let frame: Value = serde_json::from_str(&message).expect("late browser frame JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_preview");
    }
    browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("request cleanup bound context");
    let mut cleanup_bound_context = None;
    let mut binding_ready = false;
    while cleanup_bound_context.is_none() || !binding_ready {
        let message = tokio::time::timeout(Duration::from_secs(2), browser.next())
            .await
            .expect("cleanup bound context timeout")
            .expect("cleanup bound context frame")
            .expect("cleanup bound context");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("cleanup bound context JSON");
        assert_ne!(frame["type"], "dictation_result");
        if frame["type"] == "dashboard_state" && frame["bound_context"].is_object() {
            assert_eq!(frame["selected_host_id"], "second");
            cleanup_bound_context = Some(frame["bound_context"].clone());
        }
        binding_ready |= frame == json!({"type":"state","state":"ready"});
    }
    assert!(
        cleanup_bound_context
            .as_ref()
            .and_then(|context| context["summary_id"].as_str())
            .is_some_and(|summary_id| !summary_id.is_empty())
    );
    let cleanup_summary_id = cleanup_bound_context
        .as_ref()
        .and_then(|context| context["summary_id"].as_str())
        .expect("cleanup summary ID")
        .to_owned();

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":5,"summary_id":cleanup_summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start cleanup operation");
    let cleanup_operation = tokio::time::timeout(Duration::from_millis(500), browser.next())
        .await
        .expect("cleanup operation timeout")
        .expect("cleanup operation frame")
        .expect("cleanup operation");
    let Message::Text(cleanup_operation) = cleanup_operation else {
        panic!("expected cleanup operation text frame");
    };
    let cleanup_operation: Value =
        serde_json::from_str(&cleanup_operation).expect("cleanup operation JSON");
    assert_eq!(cleanup_operation["type"], "dictation_operation");
    let cleanup_operation_id = cleanup_operation["operation_id"].clone();
    browser
        .send(Message::Binary(test_pcm(b"cleanup").into()))
        .await
        .expect("send cleanup operation audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":cleanup_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish cleanup operation recording");
    loop {
        let message = tokio::time::timeout(Duration::from_millis(500), browser.next())
            .await
            .expect("cleaning state timeout")
            .expect("cleaning state frame")
            .expect("cleaning state");
        let Message::Text(message) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&message).expect("cleaning state JSON");
        assert_ne!(frame["type"], "dictation_result");
        if frame == json!({"type":"state","state":"cleaning"}) {
            break;
        }
    }
    tokio::time::timeout(Duration::from_millis(500), cleanup_started_receiver)
        .await
        .expect("delayed cleanup start timeout")
        .expect("delayed cleanup start signal");
    tokio::time::timeout(Duration::from_millis(500), cleanup_finished_receiver)
        .await
        .expect("completed cleanup timeout")
        .expect("completed cleanup signal");
    browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"first"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change host during cleanup");

    let deadline = tokio::time::Instant::now() + Duration::from_millis(650);
    let mut cancellation_count = 0;
    let mut ready_count = 0;
    let mut selected_host = None;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("cleanup host change frame JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_preview");
        if frame["type"] == "dictation_cancelled" {
            cancellation_count += 1;
            assert_eq!(
                frame,
                json!({
                    "type":"dictation_cancelled",
                    "operation_id":cleanup_operation_id
                })
            );
        }
        if frame == json!({"type":"state","state":"ready"}) {
            ready_count += 1;
        }
        if frame["type"] == "host_state" {
            selected_host = frame["selected_host_id"].as_str().map(str::to_owned);
        }
    }
    assert_eq!(cancellation_count, 1);
    assert_eq!(ready_count, 1);
    assert_eq!(selected_host.as_deref(), Some("first"));
    fake_realtime.await.expect("fake Realtime task");
    fake_cleanup.await.expect("fake cleanup task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn mock_browser_requires_the_current_presentation_handle_for_writes() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-mock-confirmation");
    let write_marker = paseo_directory.path().join("mock-writes");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf '%s\\n' 'Reply to answer' ;;\n",
                "  send) printf '%s\\n' \"$*\" >> \"{}\"; printf '%s\\n' '{{\"messageId\":\"message-mock-confirmed\"}}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            write_marker.display(),
        ),
    )
    .expect("write fake mock Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake mock Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake mock Paseo permissions");
    let mut runtime = start_mock_browser_with_paseo(&fake_paseo_path).await;

    runtime
        .browser
        .send(Message::Binary(test_pcm(b"stale mock audio").into()))
        .await
        .expect("send mock audio without an accepted start");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("send mock end without an accepted start");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "mock audio without an accepted start produced output"
    );

    let _ = run_bound_mock_text_turn(&mut runtime.browser, "use thread-a", None).await;
    let read_frames = run_bound_mock_text_turn(&mut runtime.browser, "read", None).await;
    let summary_id = read_frames
        .iter()
        .find_map(|frame| {
            frame
                .pointer("/bound_context/summary_id")
                .and_then(Value::as_str)
        })
        .expect("mock summary ID")
        .to_owned();
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start accepted mock live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "accepted mock live start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"accepted mock audio").into()))
        .await
        .expect("send accepted mock audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"missing"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select unknown mock host");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "unknown mock host error").await,
        json!({"type":"error","message":"Unknown Paseo host profile."})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"local"})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat selected mock host");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "duplicate mock host state").await["type"],
        "host_state"
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "duplicate mock dashboard").await["type"],
        "dashboard_state"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "duplicate mock host mutated presentation or recording state"
    );
    let first_frames = run_bound_mock_text_turn(
        &mut runtime.browser,
        "send first response",
        Some(&summary_id),
    )
    .await;
    let first = first_frames
        .iter()
        .find(|frame| frame["type"] == "proposal" && frame["echo"].is_string())
        .expect("first mock proposal");
    let first_handle = first["handle"]
        .as_str()
        .expect("first mock presentation handle")
        .to_owned();
    assert_eq!(first.as_object().expect("first proposal object").len(), 3);

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"local"})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat selected mock host with pending proposal");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "pending duplicate mock host state").await["type"],
        "host_state"
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "pending duplicate mock dashboard").await["type"],
        "dashboard_state"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "duplicate mock host cleared the pending presentation"
    );
    runtime
        .browser
        .send(Message::Binary(
            test_pcm(b"stale mock proposal audio").into(),
        ))
        .await
        .expect("send mock audio after proposal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("release mock recording after proposal");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "stale mock release created a response after proposal"
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"cancel_proposal"}).to_string().into(),
        ))
        .await
        .expect("send missing-handle cancellation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "missing cancellation catch-up").await,
        json!({
            "type":"proposal",
            "echo":"Send to Agent A: \"first response\". Confirm?",
            "handle":first_handle
        })
    );
    assert!(!write_marker.exists());

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"cancel_proposal","handle":first_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel first mock proposal");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "first cancellation terminal").await,
        json!({"type":"proposal","echo":null})
    );

    let second_frames = run_bound_mock_text_turn(
        &mut runtime.browser,
        "send browser-only response",
        Some(&summary_id),
    )
    .await;
    let second = second_frames
        .iter()
        .find(|frame| frame["type"] == "proposal" && frame["echo"].is_string())
        .expect("second mock proposal");
    let second_handle = second["handle"]
        .as_str()
        .expect("second mock presentation handle")
        .to_owned();
    assert_ne!(first_handle, second_handle);
    assert_eq!(second.as_object().expect("second proposal object").len(), 3);

    for control in [
        test_text_turn!("yes", summary_id),
        test_text_turn!("help", summary_id),
    ] {
        let turn_id = test_text_turn_id(&control);
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send blocked mock interaction");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "blocked mock interaction").await,
            json!({
                "type":"text_turn_rejected",
                "turn_id":turn_id,
                "message":"Typed turn was rejected. The draft was not changed."
            })
        );
        assert!(
            !write_marker.exists(),
            "blocked mock input dispatched a write"
        );
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("send blocked mock recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "blocked mock recording").await,
        json!({
            "type":"recording_rejected",
            "mode":"live_response",
            "recording_id":2,
            "message":"Recording could not start. Try again."
        })
    );

    for stale_control in [
        json!({"type":"confirm_proposal"}),
        json!({"type":"confirm_proposal","handle":first_handle}),
    ] {
        runtime
            .browser
            .send(Message::Text(stale_control.to_string().into()))
            .await
            .expect("send stale mock confirmation");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "stale mock confirmation catch-up").await,
            json!({
                "type":"proposal",
                "echo":"Send to Agent A: \"browser-only response\". Confirm?",
                "handle":second_handle
            })
        );
        assert!(
            !write_marker.exists(),
            "stale mock handle dispatched a write"
        );
    }

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":second_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("confirm current mock proposal");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "mock confirmation proposal terminal").await,
        json!({"type":"proposal","echo":null})
    );
    let mut delivered = false;
    while !delivered {
        let frame = next_browser_json(&mut runtime.browser, "mock confirmation terminal").await;
        if frame["type"] == "transcript_done" {
            assert_eq!(frame["text"], "Done.");
            delivered = true;
        }
    }
    let expected_write = "send thread-a --prompt browser-only response --no-wait --json\n";
    assert_eq!(
        std::fs::read_to_string(&write_marker).expect("mock browser write marker"),
        expected_write
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":second_handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("replay current mock confirmation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "mock confirmation replay catch-up").await,
        json!({"type":"proposal","echo":null})
    );
    assert_eq!(
        std::fs::read_to_string(&write_marker).expect("single mock browser write marker"),
        expected_write
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mock recording before host change");
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"host-bound mock audio").into()))
        .await
        .expect("send host-bound mock audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change mock host during live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "changed mock host state").await["type"],
        "host_state"
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "changed mock host dashboard").await["type"],
        "dashboard_state"
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "changed mock host proposal").await,
        json!({"type":"proposal","echo":null})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("release mock recording after host change");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "stale mock release survived an actual host change"
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn mock_dictation_allocates_at_start_and_rejects_concurrent_start() {
    let (_paseo_directory, runtime, summary_id) = start_bound_mock_dictation_browser().await;
    let LiveRealtimeRuntime {
        _directory,
        _guard,
        mut browser,
    } = runtime;
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start first mock dictation");
    let first_operation = next_browser_json(&mut browser, "first mock operation").await;
    assert_eq!(first_operation["type"], "dictation_operation");
    let first_operation_id = first_operation["operation_id"]
        .as_str()
        .expect("first mock operation ID")
        .to_owned();
    assert_eq!(
        next_browser_json(&mut browser, "first mock dictation start flush").await,
        json!({"type":"flush_audio"})
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("attempt concurrent mock dictation");
    assert_eq!(
        next_browser_json(&mut browser, "concurrent mock rejection").await,
        json!({
            "type":"recording_rejected",
            "mode":"dictation",
            "recording_id":2,
            "message":"Recording could not start. Try again."
        })
    );
    browser
        .send(Message::Binary(test_pcm(b"mock audio").into()))
        .await
        .expect("send first mock audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":first_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish first mock dictation");

    let mut result_count = 0;
    let mut ready_count = 0;
    while result_count == 0 || ready_count == 0 {
        let frame = next_browser_json(&mut browser, "first mock terminal").await;
        assert_ne!(frame["type"], "transcript_done");
        assert_ne!(frame["type"], "tool");
        assert_ne!(frame["type"], "proposal");
        if frame["type"] == "dictation_result" {
            result_count += 1;
            assert_eq!(frame["operation_id"], first_operation_id);
            assert_eq!(frame["text"], "Mock dictation text.");
        }
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
    }
    assert_eq!(result_count, 1);
    assert_eq!(ready_count, 1);

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":3,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start second mock dictation");
    let second_operation = next_browser_json(&mut browser, "second mock operation").await;
    assert_eq!(second_operation["type"], "dictation_operation");
    assert_ne!(second_operation["operation_id"], first_operation_id);
    let second_operation_id = second_operation["operation_id"].clone();
    assert_eq!(
        next_browser_json(&mut browser, "second mock dictation start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"more mock audio").into()))
        .await
        .expect("send second mock audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":second_operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish second mock dictation");
    let mut second_result = None;
    let mut ready = false;
    while second_result.is_none() || !ready {
        let frame = next_browser_json(&mut browser, "second mock terminal").await;
        assert_ne!(frame["type"], "transcript_done");
        assert_ne!(frame["type"], "tool");
        assert_ne!(frame["type"], "proposal");
        if frame["type"] == "dictation_result" {
            second_result = Some(frame);
        } else if frame == json!({"type":"state","state":"ready"}) {
            ready = true;
        }
    }
    assert_eq!(
        second_result.expect("second mock result")["operation_id"],
        second_operation_id
    );
}

#[tokio::test]
#[cfg(unix)]
async fn mock_mode_change_waits_for_active_dictation_terminal() {
    let (_paseo_directory, runtime, summary_id) = start_bound_mock_dictation_browser().await;
    let LiveRealtimeRuntime {
        _directory,
        _guard,
        mut browser,
    } = runtime;
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mode-bound mock dictation");
    let operation = next_browser_json(&mut browser, "mode-bound mock operation").await;
    assert_eq!(operation["type"], "dictation_operation");
    let operation_id = operation["operation_id"].clone();
    assert_eq!(
        next_browser_json(&mut browser, "mode-bound mock start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("attempt active mock mode change");
    assert_eq!(
        next_browser_json(&mut browser, "active mock mode rejection").await,
        json!({
            "type":"error",
            "message":"Cancel active dictation before changing mode."
        })
    );

    browser
        .send(Message::Binary(test_pcm(b"preserved mock audio").into()))
        .await
        .expect("send preserved mock audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish preserved mock operation");
    let result = next_browser_json(&mut browser, "preserved mock result").await;
    assert_eq!(result["type"], "dictation_result");
    assert_eq!(result["operation_id"], operation_id);
    assert_eq!(
        next_browser_json(&mut browser, "preserved mock ready").await,
        json!({"type":"state","state":"ready"})
    );

    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("change mock mode after terminal");
    assert_eq!(
        next_browser_json(&mut browser, "post-terminal mock mode").await,
        json!({"type":"voice_mode","mode":"live_response"})
    );
}

#[tokio::test]
async fn mock_accepted_start_flushes_before_audio() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select mock live response");
    assert_eq!(
        next_browser_json(&mut browser, "mock start-flush mode").await,
        json!({"type":"voice_mode","mode":"live_response"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mock flush recording");
    assert_eq!(
        next_browser_json(&mut browser, "mock accepted start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("abort mock flush recording");
    assert_eq!(
        next_browser_json(&mut browser, "mock accepted abort flush").await,
        json!({"type":"flush_audio"})
    );
}

#[tokio::test]
async fn invalid_mock_live_pcm_frames_abort_only_the_active_recording() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select invalid mock live mode");
    assert_eq!(
        next_browser_json(&mut browser, "invalid mock live mode").await,
        json!({"type":"voice_mode","mode":"live_response"})
    );

    for (index, audio) in [vec![], vec![0_u8; 3], vec![0_u8; 96_002]]
        .into_iter()
        .enumerate()
    {
        let recording_id = u64::try_from(index + 1).expect("bounded mock recording ID");
        browser
            .send(Message::Text(
                json!({"type":"ptt_start","recording_id":recording_id,"summary_id":null})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("start invalid mock live recording");
        assert_eq!(
            next_browser_json(&mut browser, "invalid mock live start flush").await,
            json!({"type":"flush_audio"})
        );
        browser
            .send(Message::Binary(audio.into()))
            .await
            .expect("send invalid mock live PCM");
        assert_eq!(
            next_browser_json(&mut browser, "invalid mock live rejection").await,
            json!({
                "type":"recording_rejected",
                "mode":"live_response",
                "recording_id":recording_id,
                "message":"Invalid PCM16 audio frame. Recording was aborted."
            })
        );
        assert_eq!(
            next_browser_json(&mut browser, "invalid mock live abort flush").await,
            json!({"type":"flush_audio"})
        );
        browser
            .send(Message::Text(
                json!({"type":"ptt_end","recording_id":recording_id})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send stale invalid mock live terminal");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "invalid mock live audio produced a response"
    );
}

#[tokio::test]
#[cfg(unix)]
async fn invalid_mock_dictation_pcm_frames_abort_only_the_active_recording() {
    let (_paseo_directory, runtime, summary_id) = start_bound_mock_dictation_browser().await;
    let LiveRealtimeRuntime {
        _directory,
        _guard,
        mut browser,
    } = runtime;
    for (index, audio) in [vec![], vec![0_u8; 3], vec![0_u8; 96_002]]
        .into_iter()
        .enumerate()
    {
        let recording_id = u64::try_from(index + 1).expect("bounded mock recording ID");
        browser
            .send(Message::Text(
                json!({
                    "type":"ptt_start",
                    "recording_id":recording_id,
                    "summary_id":summary_id
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("start invalid mock dictation");
        let operation = next_browser_json(&mut browser, "invalid mock dictation operation").await;
        assert_eq!(operation["type"], "dictation_operation");
        assert_eq!(
            next_browser_json(&mut browser, "invalid mock dictation start flush").await,
            json!({"type":"flush_audio"})
        );
        browser
            .send(Message::Binary(audio.into()))
            .await
            .expect("send invalid mock dictation PCM");
        assert_eq!(
            next_browser_json(&mut browser, "invalid mock dictation rejection").await,
            json!({
                "type":"recording_rejected",
                "mode":"dictation",
                "recording_id":recording_id,
                "message":"Invalid PCM16 audio frame. Recording was aborted."
            })
        );
        assert_eq!(
            next_browser_json(&mut browser, "invalid mock dictation abort flush").await,
            json!({"type":"flush_audio"})
        );
        browser
            .send(Message::Text(
                json!({"type":"ptt_end","operation_id":operation["operation_id"]})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send stale invalid mock dictation terminal");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "invalid mock dictation audio produced a result"
    );
}

#[tokio::test]
async fn mock_live_response_push_to_talk_emits_terminal_lifecycle_states() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("restore mock live response");
    assert_eq!(
        next_browser_json(&mut browser, "restored mock live mode").await,
        json!({"type":"voice_mode","mode":"live_response"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mock live response");
    assert_eq!(
        next_browser_json(&mut browser, "mock live start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"live mock audio").into()))
        .await
        .expect("send mock live audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish mock live response");
    let expected = "Mock mode has no speech recognition. Type a text turn instead.";
    assert_eq!(
        next_browser_json(&mut browser, "mock live terminal flush").await,
        json!({"type":"flush_audio"})
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock live responding state").await,
        json!({"type":"state","state":"responding"})
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock live transcript delta").await,
        json!({"type":"transcript_delta","text":expected})
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock live transcript done").await,
        json!({"type":"transcript_done","text":expected})
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock live ready state").await,
        json!({"type":"state","state":"ready"})
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start silent mock live response");
    assert_eq!(
        next_browser_json(&mut browser, "silent mock live start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish silent mock live response");
    assert_eq!(
        next_browser_json(&mut browser, "silent mock live terminal flush").await,
        json!({"type":"flush_audio"})
    );
    assert_eq!(
        next_browser_json(&mut browser, "silent mock live ready state").await,
        json!({"type":"state","state":"ready"})
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn mock_live_response_abort_discards_audio_and_allows_fresh_turn() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("restore mock live response for abort");
    assert_eq!(
        next_browser_json(&mut browser, "mock abort live mode").await,
        json!({"type":"voice_mode","mode":"live_response"})
    );

    for control in [
        json!({"type":"ptt_abort","recording_id":1}),
        json!({"type":"ptt_end","recording_id":1}),
    ] {
        browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send idle mock abort control");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "idle mock abort emitted a frame"
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start abortable mock live recording");
    assert_eq!(
        next_browser_json(&mut browser, "abortable mock live start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(
            test_pcm(b"discarded mock live audio").into(),
        ))
        .await
        .expect("send discarded mock live audio");
    for control in [
        json!({"type":"ptt_abort","recording_id":1}),
        json!({"type":"ptt_abort","recording_id":1}),
        json!({"type":"ptt_end","recording_id":1}),
    ] {
        browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send accepted mock abort control");
    }
    assert_eq!(
        next_browser_json(&mut browser, "accepted mock abort flush").await,
        json!({"type":"flush_audio"})
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "aborted mock live recording emitted more than its terminal flush"
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start fresh mock live recording");
    assert_eq!(
        next_browser_json(&mut browser, "fresh mock live start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"fresh mock live audio").into()))
        .await
        .expect("send fresh mock live audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish fresh mock live recording");
    let expected = "Mock mode has no speech recognition. Type a text turn instead.";
    assert_eq!(
        next_browser_json(&mut browser, "fresh mock live terminal flush").await,
        json!({"type":"flush_audio"})
    );
    assert_eq!(
        next_browser_json(&mut browser, "fresh mock live responding").await,
        json!({"type":"state","state":"responding"})
    );
    assert_eq!(
        next_browser_json(&mut browser, "fresh mock live transcript delta").await,
        json!({"type":"transcript_delta","text":expected})
    );
    assert_eq!(
        next_browser_json(&mut browser, "fresh mock live transcript done").await,
        json!({"type":"transcript_done","text":expected})
    );
    assert_eq!(
        next_browser_json(&mut browser, "fresh mock live ready").await,
        json!({"type":"state","state":"ready"})
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn mock_live_recording_ids_reject_stale_terminals_and_flush_accepted_ones() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("restore mock live response for recording IDs");
    assert_eq!(
        next_browser_json(&mut browser, "mock recording ID mode").await,
        json!({"type":"voice_mode","mode":"live_response"})
    );
    for control in [
        json!({"type":"ptt_start","recording_id":0,"summary_id":null}),
        json!({"type":"ptt_start","recording_id":2_147_483_648_u64,"summary_id":null}),
        json!({"type":"ptt_start","recording_id":"1","summary_id":null}),
        json!({"type":"ptt_start","recording_id":1}),
        json!({"type":"ptt_start","recording_id":1,"summary_id":null,"extra":true}),
        json!({"type":"ptt_abort","recording_id":0}),
        json!({"type":"ptt_end","recording_id":2_147_483_648_u64}),
    ] {
        browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send invalid mock recording control");
    }
    browser
        .send(Message::Binary(
            test_pcm(b"invalid mock recording audio").into(),
        ))
        .await
        .expect("send invalid mock recording audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mock recording A");
    assert_eq!(
        next_browser_json(&mut browser, "mock recording A start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"mock recording A audio").into()))
        .await
        .expect("send mock recording A audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("abort mock recording A");
    assert_eq!(
        next_browser_json(&mut browser, "mock recording A flush").await,
        json!({"type":"flush_audio"})
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("reject reused mock recording A ID");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "a completed mock live recording ID was accepted again"
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mock recording B");
    assert_eq!(
        next_browser_json(&mut browser, "mock recording B start flush").await,
        json!({"type":"flush_audio"})
    );
    for control in [
        json!({"type":"ptt_start","recording_id":3,"summary_id":null}),
        json!({"type":"ptt_abort","recording_id":1}),
        json!({"type":"ptt_end","recording_id":1}),
        json!({"type":"ptt_abort"}),
        json!({"type":"ptt_end"}),
        json!({"type":"ptt_abort","recording_id":2,"extra":true}),
        json!({"type":"ptt_end","recording_id":2,"extra":true}),
    ] {
        browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send stale mock recording terminal");
    }
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("reject mock mode change during recording B");
    assert_eq!(
        next_browser_json(&mut browser, "concurrent mock recording rejection").await,
        json!({
            "type":"recording_rejected",
            "mode":"live_response",
            "recording_id":3,
            "message":"Recording could not start. Try again."
        })
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock active recording mode rejection").await,
        json!({
            "type":"error",
            "message":"Finish or abort active recording before changing mode."
        })
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "stale mock recording terminal emitted a frame"
    );
    browser
        .send(Message::Binary(test_pcm(b"mock recording B audio").into()))
        .await
        .expect("send mock recording B audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish mock recording B");
    let expected = "Mock mode has no speech recognition. Type a text turn instead.";
    assert_eq!(
        next_browser_json(&mut browser, "mock recording B flush").await,
        json!({"type":"flush_audio"})
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock recording B responding").await,
        json!({"type":"state","state":"responding"})
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock recording B transcript delta").await,
        json!({"type":"transcript_delta","text":expected})
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock recording B transcript done").await,
        json!({"type":"transcript_done","text":expected})
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock recording B ready").await,
        json!({"type":"state","state":"ready"})
    );
}

#[tokio::test]
#[cfg(unix)]
async fn mock_ptt_abort_preserves_correlated_dictation() {
    let (_paseo_directory, runtime, summary_id) = start_bound_mock_dictation_browser().await;
    let LiveRealtimeRuntime {
        _directory,
        _guard,
        mut browser,
    } = runtime;
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mock dictation with ignored abort");
    let completed_operation =
        next_browser_json(&mut browser, "post-abort mock dictation operation").await;
    assert_eq!(
        next_browser_json(&mut browser, "post-abort mock dictation start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(
            test_pcm(b"preserved dictation audio").into(),
        ))
        .await
        .expect("send preserved mock dictation audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("ignore abort during mock dictation");
    browser
        .send(Message::Text(
            json!({
                "type":"ptt_end",
                "operation_id":completed_operation["operation_id"]
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("complete mock dictation after ignored abort");
    let result = next_browser_json(&mut browser, "post-abort mock dictation result").await;
    assert_eq!(result["type"], "dictation_result");
    assert_eq!(result["operation_id"], completed_operation["operation_id"]);
    assert_eq!(
        next_browser_json(&mut browser, "post-abort mock dictation ready").await,
        json!({"type":"state","state":"ready"})
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start cancellable mock dictation after abort");
    let cancelled_operation =
        next_browser_json(&mut browser, "cancellable post-abort mock operation").await;
    assert_eq!(
        next_browser_json(&mut browser, "cancellable post-abort mock start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("ignore abort before correlated mock cancellation");
    browser
        .send(Message::Text(
            json!({
                "type":"cancel_dictation",
                "operation_id":cancelled_operation["operation_id"]
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("cancel mock dictation through correlated path");
    assert_eq!(
        next_browser_json(&mut browser, "correlated post-abort mock cancellation").await,
        json!({
            "type":"dictation_cancelled",
            "operation_id":cancelled_operation["operation_id"]
        })
    );
    assert_eq!(
        next_browser_json(&mut browser, "correlated post-abort mock ready").await,
        json!({"type":"state","state":"ready"})
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn mock_dictation_empty_and_cancel_terminals_are_correlated() {
    let (_paseo_directory, runtime, summary_id) = start_bound_mock_dictation_browser().await;
    let LiveRealtimeRuntime {
        _directory,
        _guard,
        mut browser,
    } = runtime;
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start empty mock dictation");
    let operation = next_browser_json(&mut browser, "empty mock operation").await;
    assert_eq!(operation["type"], "dictation_operation");
    let operation_id = operation["operation_id"].clone();
    assert_eq!(
        next_browser_json(&mut browser, "empty mock dictation start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish empty mock dictation");

    let mut empty_count = 0;
    let mut ready_count = 0;
    while empty_count == 0 || ready_count == 0 {
        let frame = next_browser_json(&mut browser, "empty mock terminal").await;
        assert_ne!(frame["type"], "transcript_done");
        assert_ne!(frame["type"], "tool");
        assert_ne!(frame["type"], "proposal");
        if frame["type"] == "dictation_empty" {
            empty_count += 1;
            assert_eq!(frame["operation_id"], operation_id);
        }
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
    }
    assert_eq!(empty_count, 1);
    assert_eq!(ready_count, 1);

    for _ in 0..2 {
        browser
            .send(Message::Text(
                json!({"type":"cancel_dictation"}).to_string().into(),
            ))
            .await
            .expect("repeat idle mock cancel");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "idle mock cancel must emit no frame"
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start cancellable mock dictation");
    let cancelled_operation = next_browser_json(&mut browser, "cancellable mock operation").await;
    assert_eq!(cancelled_operation["type"], "dictation_operation");
    let cancelled_operation_id = cancelled_operation["operation_id"].clone();
    assert_eq!(
        next_browser_json(&mut browser, "cancellable mock dictation start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"discarded mock audio").into()))
        .await
        .expect("send discarded mock audio");
    browser
        .send(Message::Text(
            json!({
                "type":"cancel_dictation",
                "operation_id":cancelled_operation_id
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("cancel active mock dictation");
    assert_eq!(
        next_browser_json(&mut browser, "mock cancellation").await,
        json!({
            "type":"dictation_cancelled",
            "operation_id":cancelled_operation_id
        })
    );
    assert_eq!(
        next_browser_json(&mut browser, "mock cancellation ready").await,
        json!({"type":"state","state":"ready"})
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":3,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mock dictation after cancellation");
    let next_operation = next_browser_json(&mut browser, "post-cancel mock operation").await;
    assert_eq!(next_operation["type"], "dictation_operation");
    assert_ne!(next_operation["operation_id"], cancelled_operation_id);
    assert_eq!(
        next_browser_json(&mut browser, "post-cancel mock start flush").await,
        json!({"type":"flush_audio"})
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn mock_host_selection_cancels_active_dictation_and_allows_restart() {
    let (_paseo_directory, runtime, summary_id) = start_bound_mock_dictation_browser().await;
    let LiveRealtimeRuntime {
        _directory,
        _guard,
        mut browser,
    } = runtime;
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start host-bound mock dictation");
    let operation = next_browser_json(&mut browser, "host-bound mock operation").await;
    assert_eq!(operation["type"], "dictation_operation");
    let operation_id = operation["operation_id"].clone();
    assert_eq!(
        next_browser_json(&mut browser, "host-bound mock start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"host-bound audio").into()))
        .await
        .expect("send host-bound mock audio");
    browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select host during mock dictation");

    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut cancellation_count = 0;
    let mut ready_count = 0;
    let mut selected_host = None;
    let mut cancellation_seen = false;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("mock host frame JSON");
        assert_ne!(frame["type"], "transcript_done");
        assert_ne!(frame["type"], "tool");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        if frame["type"] == "proposal" {
            assert_eq!(frame["echo"], Value::Null);
        }
        if frame["type"] == "dictation_cancelled" {
            cancellation_count += 1;
            cancellation_seen = true;
            assert_eq!(frame["operation_id"], operation_id);
        }
        if frame == json!({"type":"state","state":"ready"}) {
            assert!(cancellation_seen, "mock cancellation must precede ready");
            ready_count += 1;
        }
        if frame["type"] == "host_state" {
            assert!(
                cancellation_seen,
                "mock cancellation must precede the new host presentation"
            );
            selected_host = frame["selected_host_id"].as_str().map(str::to_owned);
        }
        if frame["type"] == "dashboard_state" {
            assert!(
                cancellation_seen,
                "mock cancellation must precede the new dashboard"
            );
        }
    }
    assert_eq!(cancellation_count, 1);
    assert_eq!(ready_count, 1);
    assert_eq!(selected_host.as_deref(), Some("second"));

    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("send stale mock release after host change");
    let frames = run_bound_mock_text_turn(&mut browser, "read thread-a", None).await;
    let next_summary_id = frames
        .iter()
        .find_map(|frame| {
            frame
                .pointer("/bound_context/summary_id")
                .and_then(Value::as_str)
        })
        .expect("post-host mock summary ID")
        .to_owned();
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":next_summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("restart mock dictation after host change");
    let next_operation = next_browser_json(&mut browser, "post-host mock operation").await;
    assert_eq!(next_operation["type"], "dictation_operation");
    assert_ne!(next_operation["operation_id"], operation_id);
    assert_eq!(
        next_browser_json(&mut browser, "post-host mock start flush").await,
        json!({"type":"flush_audio"})
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
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
        "paseoHosts": [
            {"id":"wsl","label":"Paseo WSL Host","target":"paseo-wsl.example:6767","default":true,"defaultCwd":"~/","defaultProvider":"opencode/gpt-5.6-sol-max"},
            {"id":"spark","label":"Paseo Spark Host","target":"paseo-spark.example:6767","default":false,"defaultCwd":"~/dev","defaultProvider":"codex/gpt-5.4"}
        ],
        "publicDir": workspace.join("public"),
        "journalPath": directory.path().join("state/journal.sqlite3")
    });
    std::fs::write(&config_path, config.to_string()).expect("write config");
    let child = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
        .arg("serve")
        .current_dir(&workspace)
        .env_clear()
        .envs(essential_os_env())
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
    let client = health_client();
    for _ in 0..150 {
        if let Ok(response) = client.get(&health_url).send().await
            && let Ok(value) = response.json::<Value>().await
        {
            health = Some(value);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    assert!(index.contains("id=\"voice-mode-toggle\""));
    assert!(index.contains("id=\"dictation-preview\""));
    assert!(index.contains("id=\"recording-mode\""));
    assert!(index.contains("id=\"cancel-dictation\""));
    assert!(index.contains("id=\"microphone-select\""));
    assert!(index.contains("id=\"hold-shortcut\""));
    assert!(index.contains("id=\"sound-cues\""));
    let stylesheet = reqwest::get(format!("http://localhost:{port}/style.css"))
        .await
        .expect("browser stylesheet")
        .text()
        .await
        .expect("stylesheet text");
    assert!(stylesheet.contains("prefers-reduced-motion: reduce"));

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
    websocket
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send browser hello");
    assert_eq!(
        next_browser_json(&mut websocket, "protocol ready frame").await,
        json!({"type":"protocol_ready","version":2})
    );
    let first = websocket.next().await.expect("mode frame").expect("mode");
    let Message::Text(first) = first else {
        panic!("expected text mode frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&first).expect("mode JSON"),
        json!({"type":"mode","mode":"mock"})
    );
    let voice_mode = websocket
        .next()
        .await
        .expect("voice mode frame")
        .expect("voice mode");
    let Message::Text(voice_mode) = voice_mode else {
        panic!("expected text voice mode frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&voice_mode).expect("voice mode JSON"),
        json!({"type":"voice_mode","mode":"live_response"})
    );
    let capabilities = websocket
        .next()
        .await
        .expect("capability frame")
        .expect("capabilities");
    let Message::Text(capabilities) = capabilities else {
        panic!("expected text capability frame");
    };
    let capabilities: Value = serde_json::from_str(&capabilities).expect("capability JSON");
    assert_eq!(capabilities["type"], "dictation_capabilities");
    assert!(capabilities.get("endpoint").is_none());
    assert!(capabilities.get("credential").is_none());
    let hosts = websocket.next().await.expect("host frame").expect("hosts");
    let Message::Text(hosts) = hosts else {
        panic!("expected text host frame");
    };
    let hosts: Value = serde_json::from_str(&hosts).expect("host JSON");
    assert_eq!(hosts["type"], "host_state");
    assert_eq!(hosts["selected_host_id"], "wsl");
    assert!(hosts.get("current_session").is_none());
    assert!(hosts["hosts"][0].get("target").is_none());
    let dashboard = websocket
        .next()
        .await
        .expect("dashboard frame")
        .expect("dashboard");
    let Message::Text(dashboard) = dashboard else {
        panic!("expected dashboard text frame");
    };
    let dashboard: Value = serde_json::from_str(&dashboard).expect("dashboard JSON");
    assert_eq!(dashboard["type"], "dashboard_state");
    assert_eq!(dashboard["selected_host_id"], "wsl");
    assert!(dashboard["agents"].is_array());
    assert!(dashboard.get("target").is_none());
    websocket
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation mode");
    let selected = websocket
        .next()
        .await
        .expect("selected voice mode frame")
        .expect("selected voice mode");
    let Message::Text(selected) = selected else {
        panic!("expected selected voice mode frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&selected).expect("selected voice mode JSON"),
        json!({"type":"voice_mode","mode":"dictation"})
    );
    websocket
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"invalid"})
                .to_string()
                .into(),
        ))
        .await
        .expect("send invalid voice mode");
    let invalid = websocket
        .next()
        .await
        .expect("invalid voice mode frame")
        .expect("invalid voice mode");
    let Message::Text(invalid) = invalid else {
        panic!("expected invalid voice mode error");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&invalid).expect("invalid voice mode JSON"),
        json!({"type":"error","message":"Invalid voice mode request."})
    );
    websocket
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("repeat dictation mode");
    let repeated = websocket
        .next()
        .await
        .expect("repeated voice mode frame")
        .expect("repeated voice mode");
    let Message::Text(repeated) = repeated else {
        panic!("expected repeated voice mode frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&repeated).expect("repeated voice mode JSON"),
        json!({"type":"voice_mode","mode":"dictation"})
    );
    websocket
        .send(Message::Text(
            json!({"type":"select_host","host_id":"spark"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select host");
    let changed = websocket
        .next()
        .await
        .expect("changed host")
        .expect("changed");
    let Message::Text(changed) = changed else {
        panic!("expected changed host frame");
    };
    let changed: Value = serde_json::from_str(&changed).expect("changed JSON");
    assert_eq!(changed["type"], "host_state");
    assert_eq!(changed["selected_host_id"], "spark");
    websocket
        .send(Message::Text(
            test_text_turn!("help", null).to_string().into(),
        ))
        .await
        .expect("send mock text turn");
    let mut transcript = None;
    for _ in 0..7 {
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
    websocket.close(None).await.expect("close first connection");

    let (mut reconnected, _) = connect_async(format!("ws://127.0.0.1:{port}/ws"))
        .await
        .expect("reconnect browser websocket");
    reconnected
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send reconnect browser hello");
    assert_eq!(
        next_browser_json(&mut reconnected, "reconnect protocol ready").await,
        json!({"type":"protocol_ready","version":2})
    );
    let _mode = reconnected
        .next()
        .await
        .expect("reconnect mode")
        .expect("mode");
    let voice_mode = reconnected
        .next()
        .await
        .expect("reconnect voice mode")
        .expect("voice mode");
    let Message::Text(voice_mode) = voice_mode else {
        panic!("expected reconnect voice mode frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&voice_mode).expect("reconnect voice mode JSON"),
        json!({"type":"voice_mode","mode":"live_response"})
    );
    let capabilities = reconnected
        .next()
        .await
        .expect("reconnect capability frame")
        .expect("capabilities");
    let Message::Text(capabilities) = capabilities else {
        panic!("expected reconnect capability frame");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&capabilities).expect("capability JSON")["type"],
        "dictation_capabilities"
    );
    let hosts = reconnected
        .next()
        .await
        .expect("reconnect host frame")
        .expect("hosts");
    let Message::Text(hosts) = hosts else {
        panic!("expected reconnect host frame");
    };
    let hosts: Value = serde_json::from_str(&hosts).expect("reconnect host JSON");
    assert_eq!(hosts["selected_host_id"], "wsl");
    let dashboard = reconnected
        .next()
        .await
        .expect("reconnect dashboard frame")
        .expect("dashboard");
    let Message::Text(dashboard) = dashboard else {
        panic!("expected reconnect dashboard frame");
    };
    let dashboard: Value = serde_json::from_str(&dashboard).expect("reconnect dashboard JSON");
    assert_eq!(dashboard["bound_context"], Value::Null);
    assert_eq!(dashboard["queue_count"], 0);
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn response_started_without_context_cannot_send_after_reading_one() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-null-response-context",
        "Reply A",
    );
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "session update").await["type"],
            "session.update"
        );
        let mut saw_turn = false;
        let mut saw_response_request = false;
        while !saw_turn || !saw_response_request {
            let event = next_fake_realtime_json(&mut socket, "null-context turn").await;
            saw_turn |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("read A then send");
            saw_response_request |= event["type"] == "response.create";
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-null-context"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create null-context response");
        for event in function_call_events(
            "response-null-context",
            "item-read-a",
            "call-read-a",
            "read_latest_reply",
            r#"{"session":"thread-a"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send read call");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "read output").await;
            if event.pointer("/item/call_id").and_then(Value::as_str) == Some("call-read-a") {
                break;
            }
        }
        for event in function_call_events_at(
            "response-null-context",
            "item-send-a",
            "call-send-a",
            1,
            "send_message",
            r#"{"text":"response for A"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send response call");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "send output").await;
            if event.pointer("/item/call_id").and_then(Value::as_str) == Some("call-send-a") {
                let output = event
                    .pointer("/item/output")
                    .and_then(Value::as_str)
                    .expect("output");
                return serde_json::from_str::<Value>(output).expect("output JSON");
            }
        }
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("read A then send", null).to_string().into(),
        ))
        .await
        .expect("send null-context turn");

    let output = fake_realtime.await.expect("fake Realtime task");
    assert_eq!(output, json!({"ok":false,"error":"stale_response_context"}));
    while let Ok(Some(Ok(Message::Text(message)))) =
        tokio::time::timeout(Duration::from_millis(50), runtime.browser.next()).await
    {
        let frame: Value = serde_json::from_str(&message).expect("browser frame JSON");
        assert!(
            frame["type"] != "proposal" || frame["echo"].is_null(),
            "stale response created a browser proposal"
        );
    }
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn stale_typed_summary_is_rejected_before_provider_or_process_input() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-stale-typed-turn");
    let process_marker = paseo_directory.path().join("process-calls");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "printf '%s\\n' \"$*\" >> \"{}\"\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Session A\",\"status\":\"idle\"}},{{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Session B\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf 'Reply from %s\\n' \"$2\" ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            process_marker.display(),
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (first_bound_sender, first_bound_receiver) = tokio::sync::oneshot::channel();
    let (second_bound_sender, second_bound_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "first-host-after-pending-commit").await;
        first_bound_sender.send(()).expect("signal context A");
        bind_fake_realtime_context(&mut socket, "second-host-after-transcription").await;
        second_bound_sender.send(()).expect("signal context B");
        tokio::time::timeout(Duration::from_millis(250), socket.next())
            .await
            .is_ok()
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind context A");
    first_bound_receiver.await.expect("context A bound");
    let mut summary_a = None;
    let mut ready = false;
    while summary_a.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "context A frame").await;
        if let Some(summary_id) = frame
            .get("bound_context")
            .and_then(|context| context.get("summary_id"))
            .and_then(Value::as_str)
        {
            summary_a = Some(summary_id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_a = summary_a.expect("summary A");
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", summary_a)
                .to_string()
                .into(),
        ))
        .await
        .expect("bind context B");
    second_bound_receiver.await.expect("context B bound");
    let mut summary_b = None;
    let mut ready = false;
    while summary_b.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "context B frame").await;
        if let Some(summary_id) = frame
            .get("bound_context")
            .and_then(|context| context.get("summary_id"))
            .and_then(Value::as_str)
            .filter(|summary_id| *summary_id != summary_a)
        {
            summary_b = Some(summary_id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_b = summary_b.expect("summary B");
    assert_ne!(summary_a, summary_b);
    let calls_before =
        std::fs::read_to_string(&process_marker).expect("process calls before stale");

    let stale_turn = test_text_turn!("stale A", summary_a);
    let stale_turn_id = test_text_turn_id(&stale_turn);
    for control in [
        stale_turn,
        json!({"type":"text_turn","text":"missing context"}),
        json!({
            "type":"text_turn",
            "text":"extra context field",
            "summary_id":summary_b,
            "extra":true
        }),
    ] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send rejected typed context");
    }

    assert!(!fake_realtime.await.expect("fake Realtime task"));
    assert_eq!(
        std::fs::read_to_string(&process_marker).expect("process calls after stale"),
        calls_before
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "stale turn rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":stale_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    if let Ok(Some(Ok(Message::Text(message)))) =
        tokio::time::timeout(Duration::from_millis(50), runtime.browser.next()).await
    {
        panic!("malformed turn produced a browser frame: {message}");
    }
}

#[tokio::test]
async fn dictation_start_rejects_null_stale_missing_and_extra_summary_context() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "session update").await["type"],
            "session.update"
        );
        ready_sender.send(()).expect("signal provider ready");
        tokio::time::timeout(Duration::from_millis(300), socket.next())
            .await
            .is_ok()
    });
    let mut runtime = start_live_realtime_browser(realtime_port).await;
    ready_receiver.await.expect("provider ready");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );

    for control in [
        json!({"type":"ptt_start","recording_id":1,"summary_id":null}),
        json!({"type":"ptt_start","recording_id":2,"summary_id":"summary-stale"}),
        json!({"type":"ptt_start"}),
        json!({"type":"ptt_start","recording_id":3,"summary_id":null,"extra":true}),
    ] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send rejected dictation start");
    }

    assert!(!fake_realtime.await.expect("fake Realtime task"));
    for recording_id in 1..=2 {
        assert_eq!(
            next_browser_json(&mut runtime.browser, "rejected dictation start").await,
            json!({
                "type":"recording_rejected",
                "mode":"dictation",
                "recording_id":recording_id,
                "message":"Recording could not start. Try again."
            })
        );
    }
    if let Ok(Some(Ok(Message::Text(message)))) =
        tokio::time::timeout(Duration::from_millis(50), runtime.browser.next()).await
    {
        panic!("rejected dictation start produced a browser frame: {message}");
    }
}

#[tokio::test]
#[cfg(unix)]
async fn mock_turn_context_rejects_stale_write_and_delivers_current_write() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-mock-turn-context");
    let write_marker = paseo_directory.path().join("writes");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"}},{{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Agent B\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf 'Reply from %s\\n' \"$2\" ;;\n",
                "  send) printf '%s\\n' \"$*\" >> \"{}\"; printf '%s\\n' '{{\"messageId\":\"mock-message\"}}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            write_marker.display(),
        ),
    )
    .expect("write fake mock Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake mock Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");
    let mut runtime = start_mock_browser_with_paseo(&fake_paseo_path).await;

    let frames_a = run_bound_mock_text_turn(&mut runtime.browser, "read thread-a", None).await;
    let summary_a = frames_a
        .iter()
        .find_map(|frame| {
            frame
                .pointer("/bound_context/summary_id")
                .and_then(Value::as_str)
        })
        .expect("summary A")
        .to_owned();
    let frames_b =
        run_bound_mock_text_turn(&mut runtime.browser, "read thread-b", Some(&summary_a)).await;
    let summary_b = frames_b
        .iter()
        .find_map(|frame| {
            frame
                .pointer("/bound_context/summary_id")
                .and_then(Value::as_str)
        })
        .filter(|summary_id| *summary_id != summary_a)
        .expect("summary B")
        .to_owned();

    let stale_turn = test_text_turn!("send stale response", summary_a);
    let stale_turn_id = test_text_turn_id(&stale_turn);
    runtime
        .browser
        .send(Message::Text(stale_turn.to_string().into()))
        .await
        .expect("send stale mock turn");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "stale mock turn rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":stale_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    assert!(!write_marker.exists());

    let current_frames = run_bound_mock_text_turn(
        &mut runtime.browser,
        "send current response",
        Some(&summary_b),
    )
    .await;
    let handle = current_frames
        .iter()
        .find(|frame| frame["type"] == "proposal" && frame["echo"].is_string())
        .and_then(|frame| frame["handle"].as_str())
        .expect("current mock proposal handle")
        .to_owned();
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("confirm current mock proposal");
    loop {
        let frame = next_browser_json(&mut runtime.browser, "mock confirmation frame").await;
        if frame["type"] == "transcript_done" {
            break;
        }
    }
    assert_eq!(
        std::fs::read_to_string(&write_marker).expect("mock write marker"),
        "send thread-b --prompt current response --no-wait --json\n"
    );
}

#[tokio::test]
async fn mock_dictation_start_requires_an_exact_current_summary() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;

    for control in [
        json!({"type":"ptt_start","recording_id":1,"summary_id":null}),
        json!({"type":"ptt_start","recording_id":2,"summary_id":"summary-stale"}),
        json!({"type":"ptt_start"}),
        json!({"type":"ptt_start","recording_id":3,"summary_id":null,"extra":true}),
    ] {
        browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send rejected mock dictation start");
    }

    for recording_id in 1..=2 {
        assert_eq!(
            next_browser_json(&mut browser, "rejected mock dictation start").await,
            json!({
                "type":"recording_rejected",
                "mode":"dictation",
                "recording_id":recording_id,
                "message":"Recording could not start. Try again."
            })
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), browser.next())
            .await
            .is_err(),
        "rejected mock dictation start allocated an operation"
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn current_typed_summary_proposes_and_browser_confirmation_delivers_to_it() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-current-summary");
    let write_marker = paseo_directory.path().join("writes");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"}}]' ;;\n",
                "  logs) printf '%s\\n' 'Reply A' ;;\n",
                "  send) printf '%s\\n' \"$*\" >> \"{}\"; printf '%s\\n' '{{\"messageId\":\"message-a\"}}' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            write_marker.display(),
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let (proposal_sender, proposal_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "current-summary-a").await;
        bound_sender.send(()).expect("signal context A");
        let mut saw_turn = false;
        let mut saw_request = false;
        while !saw_turn || !saw_request {
            let event = next_fake_realtime_json(&mut socket, "current A turn").await;
            saw_turn |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("send current A");
            saw_request |= event["type"] == "response.create";
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-current-a"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create current A response");
        for event in function_call_events(
            "response-current-a",
            "item-send-current-a",
            "call-send-current-a",
            "send_message",
            r#"{"text":"response for A"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send current A call");
        }
        let output = loop {
            let event = next_fake_realtime_json(&mut socket, "current A output").await;
            if event.pointer("/item/call_id").and_then(Value::as_str) == Some("call-send-current-a")
            {
                let output = event
                    .pointer("/item/output")
                    .and_then(Value::as_str)
                    .expect("current A output");
                break serde_json::from_str::<Value>(output).expect("current A output JSON");
            }
        };
        assert_eq!(output["ok"], true);
        assert!(output.get("proposal_token").is_none());
        proposal_sender.send(()).expect("signal current A proposal");
        finish_receiver
            .await
            .expect("wait for browser confirmation");
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind context A");
    bound_receiver.await.expect("context A bound");
    let mut summary_a = None;
    let mut ready = false;
    while summary_a.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "context A frame").await;
        if let Some(summary_id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_a = Some(summary_id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_a = summary_a.expect("summary A");
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("send current A", summary_a)
                .to_string()
                .into(),
        ))
        .await
        .expect("send current A turn");
    proposal_receiver.await.expect("current A proposal ready");
    let handle = loop {
        let frame = next_browser_json(&mut runtime.browser, "current A proposal frame").await;
        if frame["type"] == "proposal"
            && frame["echo"].is_string()
            && let Some(handle) = frame["handle"].as_str()
        {
            break handle.to_owned();
        }
    };
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":handle})
                .to_string()
                .into(),
        ))
        .await
        .expect("confirm current A proposal");
    loop {
        let frame = next_browser_json(&mut runtime.browser, "current A confirmation").await;
        if frame["type"] == "transcript_done" {
            assert_eq!(frame["text"], "Confirmed and delivered.");
            break;
        }
    }
    finish_sender.send(()).expect("finish fake Realtime");
    fake_realtime.await.expect("fake Realtime task");
    assert_eq!(
        std::fs::read_to_string(&write_marker).expect("current A write marker"),
        "send thread-a --prompt response for A --no-wait --json\n"
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn live_turn_from_a_cannot_send_after_followup_activates_b() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-live-turn-context");
    std::fs::write(
        &fake_paseo_path,
        concat!(
            "#!/bin/sh\n",
            "case \"$1\" in\n",
            "  ls) printf '%s\\n' '[{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"},{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Agent B\",\"status\":\"idle\"}]' ;;\n",
            "  logs) printf 'Reply from %s\\n' \"$2\" ;;\n",
            "  *) printf '%s\\n' '[]' ;;\n",
            "esac\n",
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "live-summary-a").await;
        bound_sender.send(()).expect("signal context A");
        let mut saw_commit = false;
        let mut saw_response_request = false;
        while !saw_commit || !saw_response_request {
            let event = next_fake_realtime_json(&mut socket, "live A request").await;
            saw_commit |= event["type"] == "input_audio_buffer.commit";
            if event["type"] == "input_audio_buffer.commit" {
                acknowledge_fake_audio_commit(&mut socket, "live-context-audio").await;
            }
            saw_response_request |= event["type"] == "response.create";
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-live-a"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create live A response");
        for event in function_call_events(
            "response-live-a",
            "item-read-b",
            "call-read-b",
            "read_latest_reply",
            r#"{"session":"thread-b"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send read B call");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "read B output").await;
            if event.pointer("/item/call_id").and_then(Value::as_str) == Some("call-read-b") {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"response-live-a","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete live A response");
        loop {
            if next_fake_realtime_json(&mut socket, "live A followup request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-live-a-followup"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create live A followup");
        for event in function_call_events(
            "response-live-a-followup",
            "item-delayed-send",
            "call-delayed-send",
            "send_message",
            r#"{"text":"must remain bound to A"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send delayed response call");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "delayed send output").await;
            if event.pointer("/item/call_id").and_then(Value::as_str) == Some("call-delayed-send") {
                let output = event
                    .pointer("/item/output")
                    .and_then(Value::as_str)
                    .expect("delayed output");
                return serde_json::from_str::<Value>(output).expect("delayed output JSON");
            }
        }
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind context A");
    bound_receiver.await.expect("context A bound");
    let mut summary_a = None;
    let mut ready = false;
    while summary_a.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "context A frame").await;
        if let Some(summary_id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_a = Some(summary_id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_a = summary_a.expect("summary A");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_a})
                .to_string()
                .into(),
        ))
        .await
        .expect("start live A recording");
    loop {
        if next_browser_json(&mut runtime.browser, "live A start frame").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"live A audio").into()))
        .await
        .expect("send live A audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish live A recording");

    assert_eq!(
        fake_realtime.await.expect("fake Realtime task"),
        json!({"ok":false,"error":"stale_response_context"})
    );
    while let Ok(Some(Ok(Message::Text(message)))) =
        tokio::time::timeout(Duration::from_millis(50), runtime.browser.next()).await
    {
        let frame: Value = serde_json::from_str(&message).expect("browser frame JSON");
        assert!(
            frame["type"] != "proposal" || frame["echo"].is_null(),
            "delayed stale response created a browser proposal"
        );
    }
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn context_change_aborts_a_live_recording_before_its_release() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-recording-context");
    std::fs::write(
        &fake_paseo_path,
        concat!(
            "#!/bin/sh\n",
            "case \"$1\" in\n",
            "  ls) printf '%s\\n' '[{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"},{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Agent B\",\"status\":\"idle\"}]' ;;\n",
            "  logs) printf 'Reply from %s\\n' \"$2\" ;;\n",
            "  *) printf '%s\\n' '[]' ;;\n",
            "esac\n",
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let (changed_sender, changed_receiver) = tokio::sync::oneshot::channel();
    let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "recording-summary-a").await;
        bound_sender.send(()).expect("signal context A");
        loop {
            if next_fake_realtime_json(&mut socket, "recording A start").await["type"]
                == "input_audio_buffer.clear"
            {
                break;
            }
        }
        let mut saw_turn = false;
        let mut saw_request = false;
        while !saw_turn || !saw_request {
            let event = next_fake_realtime_json(&mut socket, "context B turn").await;
            saw_turn |= event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("activate B while recording");
            saw_request |= event["type"] == "response.create";
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-recording-b"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create context B response");
        for event in function_call_events(
            "response-recording-b",
            "item-recording-read-b",
            "call-recording-read-b",
            "read_latest_reply",
            r#"{"session":"thread-b"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send recording read B call");
        }
        let mut saw_abort_clear = false;
        loop {
            let event = next_fake_realtime_json(&mut socket, "recording B output").await;
            saw_abort_clear |= event["type"] == "input_audio_buffer.clear";
            if event.pointer("/item/call_id").and_then(Value::as_str)
                == Some("call-recording-read-b")
            {
                break;
            }
        }
        changed_sender
            .send(saw_abort_clear)
            .expect("report recording context change");
        release_receiver.await.expect("wait for stale release");
        tokio::time::timeout(Duration::from_millis(200), socket.next())
            .await
            .is_ok()
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind context A");
    bound_receiver.await.expect("context A bound");
    let mut summary_a = None;
    let mut ready = false;
    while summary_a.is_none() || !ready {
        let frame = next_browser_json(&mut runtime.browser, "context A frame").await;
        if let Some(summary_id) = frame
            .pointer("/bound_context/summary_id")
            .and_then(Value::as_str)
        {
            summary_a = Some(summary_id.to_owned());
        }
        ready |= frame == json!({"type":"state","state":"ready"});
    }
    let summary_a = summary_a.expect("summary A");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_a})
                .to_string()
                .into(),
        ))
        .await
        .expect("start recording A");
    loop {
        if next_browser_json(&mut runtime.browser, "recording A flush").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"recording A audio").into()))
        .await
        .expect("send recording A audio");
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("activate B while recording", summary_a)
                .to_string()
                .into(),
        ))
        .await
        .expect("activate B while recording");
    assert!(changed_receiver.await.expect("recording context changed"));
    loop {
        if next_browser_json(&mut runtime.browser, "recording context abort").await["type"]
            == "flush_audio"
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("release stale recording A");
    release_sender.send(()).expect("signal stale release");
    assert!(!fake_realtime.await.expect("fake Realtime task"));
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn reversed_response_creation_cannot_swap_request_origin() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-response-order", "Reply A");
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let (first_request_sender, first_request_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "response-order-a").await;
        bound_sender.send(()).expect("signal context A");
        loop {
            if next_fake_realtime_json(&mut socket, "first ordered request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        first_request_sender
            .send(())
            .expect("signal first response request");

        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let mut second_was_sent_early = false;
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("queued request JSON");
            second_was_sent_early |= event["type"] == "response.create";
        }

        if second_was_sent_early {
            for response_id in ["response-b-reversed", "response-a-late"] {
                socket
                    .send(Message::Text(
                        json!({"type":"response.created","response":{"id":response_id}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .expect("send reversed response creation");
            }
        } else {
            socket
                .send(Message::Text(
                    json!({"type":"response.created","response":{"id":"response-a-late"}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("send late first response creation");
            let mut saw_cancel = false;
            let mut saw_second_request = false;
            while !saw_cancel || !saw_second_request {
                let event = next_fake_realtime_json(&mut socket, "quarantine then dequeue").await;
                saw_cancel |=
                    event["type"] == "response.cancel" && event["response_id"] == "response-a-late";
                saw_second_request |= event["type"] == "response.create";
            }
            socket
                .send(Message::Text(
                    json!({"type":"response.created","response":{"id":"response-b-current"}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("send current response creation");
        }
        second_was_sent_early
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind context A");
    bound_receiver.await.expect("context A bound");
    let summary_a = next_bound_summary_id(&mut runtime.browser, "context A summary").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("request A", summary_a).to_string().into(),
        ))
        .await
        .expect("request response A");
    first_request_receiver
        .await
        .expect("first response request");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("clear context A");
    let mut host_selected = false;
    let mut dashboard_selected = false;
    loop {
        let frame = next_browser_json(&mut runtime.browser, "ordered host selection").await;
        if frame["type"] == "host_state" {
            assert_eq!(frame["selected_host_id"], "second");
            host_selected = true;
        }
        if frame["type"] == "dashboard_state" {
            assert!(host_selected, "dashboard preceded ordered host state");
            assert_eq!(frame["selected_host_id"], "second");
            dashboard_selected = true;
        }
        if frame == json!({"type":"proposal","echo":null}) {
            assert!(
                dashboard_selected,
                "proposal clear preceded ordered dashboard"
            );
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("request B", null).to_string().into(),
        ))
        .await
        .expect("request response B");

    assert!(
        !fake_realtime.await.expect("fake Realtime task"),
        "a later response.create was sent before the cancelled request was correlated"
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn text_turn_during_delayed_cleanup_is_rejected_without_replacing_dictation() {
    use std::os::unix::fs::PermissionsExt as _;

    let mut delayed_model =
        DelayedSummariser::start("Cleaned A.", Duration::from_millis(300)).await;
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-cleanup-context");
    std::fs::write(
        &fake_paseo_path,
        concat!(
            "#!/bin/sh\n",
            "case \"$1\" in\n",
            "  ls) printf '%s\\n' '[{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"},{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Agent B\",\"status\":\"idle\"}]' ;;\n",
            "  logs) printf 'Reply from %s\\n' \"$2\" ;;\n",
            "  *) printf '%s\\n' '[]' ;;\n",
            "esac\n",
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");

    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let (delete_ack_sender, delete_ack_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "cleanup-summary-a").await;
        bound_sender.send(()).expect("signal context A");
        loop {
            if next_fake_realtime_json(&mut socket, "dictation A commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"dictation-cleanup-a",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"dictation-cleanup-a",
                "transcript":"unclean A"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance dictation A");
        }
        let delete = next_fake_realtime_json(&mut socket, "dictation A delete").await;
        assert_eq!(delete["type"], "conversation.item.delete");
        assert_eq!(delete["item_id"], "dictation-cleanup-a");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"dictation-cleanup-a",
                    "event_id":"provider-cleanup-a-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge dictation A delete");
        delete_ack_sender
            .send(())
            .expect("signal dictation A delete acknowledgement");
        assert!(
            tokio::time::timeout(Duration::from_millis(150), socket.next())
                .await
                .is_err(),
            "text during cleanup reached the provider"
        );
        finish_receiver.await.expect("finish delayed cleanup test");
    });

    let mut runtime = start_live_realtime_browser_with_cleanup(
        realtime_port,
        &fake_paseo_path,
        &delayed_model.base_url,
    )
    .await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind context A");
    bound_receiver.await.expect("context A bound");
    let summary_a = next_bound_summary_id(&mut runtime.browser, "context A summary").await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation");
    loop {
        if next_browser_json(&mut runtime.browser, "dictation mode").await
            == json!({"type":"voice_mode","mode":"dictation"})
        {
            break;
        }
    }
    loop {
        if next_browser_json(&mut runtime.browser, "dictation ready").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_a})
                .to_string()
                .into(),
        ))
        .await
        .expect("start dictation A");
    let operation_id = loop {
        let frame = next_browser_json(&mut runtime.browser, "dictation operation").await;
        if frame["type"] == "dictation_operation" {
            break frame["operation_id"]
                .as_str()
                .expect("dictation operation ID")
                .to_owned();
        }
    };
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("commit dictation A");
    delayed_model.wait_started().await;
    tokio::time::timeout(Duration::from_millis(500), delete_ack_receiver)
        .await
        .expect("dictation A delete acknowledgement timeout")
        .expect("dictation A delete acknowledgement signal");
    let pre_cleanup_deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    while tokio::time::Instant::now() < pre_cleanup_deadline {
        let remaining = pre_cleanup_deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("pre-cleanup browser JSON");
        assert_ne!(frame["type"], "dictation_result");
        assert_ne!(frame["type"], "dictation_empty");
        assert_ne!(frame["type"], "dictation_failed");
        assert_ne!(frame["type"], "dictation_cancelled");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
    }
    let rejected_text_turn = test_text_turn!("activate B during cleanup", summary_a);
    let rejected_turn_id = test_text_turn_id(&rejected_text_turn);
    runtime
        .browser
        .send(Message::Text(rejected_text_turn.to_string().into()))
        .await
        .expect("reject text during cleanup");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "text_turn_rejected",
            "cleanup text rejection",
        )
        .await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":rejected_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    let result = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_result",
        "preserved cleanup result",
    )
    .await;
    assert_eq!(result["operation_id"], operation_id);
    assert_eq!(result["text"], "Cleaned A.");
    finish_sender
        .send(())
        .expect("finish delayed cleanup provider");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
async fn delayed_dictation_a_terminals_cannot_target_dictation_b() {
    let (_paseo_directory, runtime, summary_id) = start_bound_mock_dictation_browser().await;
    let LiveRealtimeRuntime {
        _directory,
        _guard,
        mut browser,
    } = runtime;
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start dictation A");
    let operation_a = next_browser_json(&mut browser, "dictation A operation").await;
    let operation_a = operation_a["operation_id"]
        .as_str()
        .expect("dictation A operation ID")
        .to_owned();
    assert_eq!(
        next_browser_json(&mut browser, "dictation A start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Text(
            json!({"type":"cancel_dictation","operation_id":operation_a})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel dictation A");
    assert_eq!(
        next_browser_json(&mut browser, "dictation A cancellation").await,
        json!({"type":"dictation_cancelled","operation_id":operation_a})
    );
    assert_eq!(
        next_browser_json(&mut browser, "dictation A ready").await,
        json!({"type":"state","state":"ready"})
    );

    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start dictation B");
    let operation_b = next_browser_json(&mut browser, "dictation B operation").await;
    let operation_b = operation_b["operation_id"]
        .as_str()
        .expect("dictation B operation ID")
        .to_owned();
    assert_ne!(operation_a, operation_b);
    assert_eq!(
        next_browser_json(&mut browser, "dictation B start flush").await,
        json!({"type":"flush_audio"})
    );
    for stale in [
        json!({"type":"ptt_end"}),
        json!({"type":"cancel_dictation"}),
        json!({"type":"ptt_end","operation_id":operation_a}),
        json!({"type":"cancel_dictation","operation_id":operation_a}),
        json!({"type":"ptt_end","operation_id":operation_b,"extra":true}),
        json!({"type":"cancel_dictation","operation_id":operation_b,"extra":true}),
    ] {
        browser
            .send(Message::Text(stale.to_string().into()))
            .await
            .expect("send delayed dictation A terminal");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(150), browser.next())
            .await
            .is_err(),
        "a delayed dictation A terminal changed dictation B"
    );

    browser
        .send(Message::Binary(test_pcm(b"dictation B audio").into()))
        .await
        .expect("send dictation B audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_b})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish dictation B");
    assert_eq!(
        next_browser_json(&mut browser, "dictation B result").await,
        json!({
            "type":"dictation_result",
            "operation_id":operation_b,
            "status":"degraded",
            "text":"Mock dictation text."
        })
    );
}

#[tokio::test]
async fn duplicate_browser_control_fields_fail_before_provider_input() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let (observed_sender, observed_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel::<()>();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "duplicate control session update").await["type"],
            "session.update"
        );
        ready_sender.send(()).expect("signal provider ready");
        let observed = tokio::time::timeout(Duration::from_millis(300), socket.next())
            .await
            .is_ok();
        observed_sender
            .send(observed)
            .expect("report duplicate control provider input");
        finish_receiver.await.expect("release fake Realtime");
        observed
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    ready_receiver.await.expect("provider ready");
    for raw in [
        r#"{"type":"text_turn","text":"first","text":"second","summary_id":null,"turn_id":1}"#,
        r#"{"type":"text_turn","text":"turn","summary_id":"stale","summary_id":null,"turn_id":1}"#,
    ] {
        runtime
            .browser
            .send(Message::Text(raw.to_owned().into()))
            .await
            .expect("send duplicate browser control");
    }

    assert!(
        !observed_receiver.await.expect("provider input report"),
        "duplicate browser control fields reached the provider"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), runtime.browser.next())
            .await
            .is_err(),
        "duplicate browser control fields produced a browser frame"
    );
    finish_sender.send(()).expect("finish fake Realtime");
    assert!(!fake_realtime.await.expect("fake Realtime task"));
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn duplicate_send_message_text_fails_before_proposal() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-duplicate-tool", "Reply A");
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "duplicate-tool-a").await;
        bound_sender.send(()).expect("signal context A");
        loop {
            if next_fake_realtime_json(&mut socket, "duplicate tool response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"response-duplicate-tool"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create duplicate tool response");
        for event in function_call_events(
            "response-duplicate-tool",
            "item-duplicate-send",
            "call-duplicate-send",
            "send_message",
            r#"{"text":"first","text":"second"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send duplicate tool arguments");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "duplicate tool output").await;
            if event.pointer("/item/call_id").and_then(Value::as_str) == Some("call-duplicate-send")
            {
                let output = event
                    .pointer("/item/output")
                    .and_then(Value::as_str)
                    .expect("duplicate tool output");
                return serde_json::from_str::<Value>(output).expect("duplicate tool output JSON");
            }
        }
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind context A");
    bound_receiver.await.expect("context A bound");
    let summary_a = next_bound_summary_id(&mut runtime.browser, "context A summary").await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("attempt duplicate send", summary_a)
                .to_string()
                .into(),
        ))
        .await
        .expect("request duplicate tool response");

    assert_eq!(
        fake_realtime.await.expect("fake Realtime task"),
        json!({"ok":false,"error":"invalid_arguments"})
    );
    while let Ok(Some(Ok(Message::Text(message)))) =
        tokio::time::timeout(Duration::from_millis(50), runtime.browser.next()).await
    {
        let frame: Value = serde_json::from_str(&message).expect("browser frame JSON");
        assert!(
            frame["type"] != "proposal" || frame["echo"].is_null(),
            "duplicate tool arguments created a proposal"
        );
    }
}

#[tokio::test]
#[cfg(unix)]
async fn mock_text_turn_is_rejected_while_dictation_is_recording() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory
        .path()
        .join("fake-paseo-mock-dictation-context");
    std::fs::write(
        &fake_paseo_path,
        concat!(
            "#!/bin/sh\n",
            "case \"$1\" in\n",
            "  ls) printf '%s\\n' '[{\"id\":\"thread-a\",\"shortId\":\"a\",\"name\":\"Agent A\",\"status\":\"idle\"},{\"id\":\"thread-b\",\"shortId\":\"b\",\"name\":\"Agent B\",\"status\":\"idle\"}]' ;;\n",
            "  logs) printf 'Reply from %s\\n' \"$2\" ;;\n",
            "  *) printf '%s\\n' '[]' ;;\n",
            "esac\n",
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");
    let mut runtime = start_mock_browser_with_paseo(&fake_paseo_path).await;
    let frames = run_bound_mock_text_turn(&mut runtime.browser, "read thread-a", None).await;
    let summary_a = frames
        .iter()
        .find_map(|frame| {
            frame
                .pointer("/bound_context/summary_id")
                .and_then(Value::as_str)
        })
        .expect("summary A")
        .to_owned();
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_a})
                .to_string()
                .into(),
        ))
        .await
        .expect("start dictation A");
    let operation = next_browser_json(&mut runtime.browser, "dictation A operation").await;
    let operation_id = operation["operation_id"]
        .as_str()
        .expect("dictation A operation ID")
        .to_owned();
    assert_eq!(
        next_browser_json(&mut runtime.browser, "dictation A mock start flush").await,
        json!({"type":"flush_audio"})
    );
    let rejected_text_turn = test_text_turn!("read thread-b", summary_a);
    let rejected_turn_id = test_text_turn_id(&rejected_text_turn);
    runtime
        .browser
        .send(Message::Text(rejected_text_turn.to_string().into()))
        .await
        .expect("reject text during mock dictation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "mock dictation text rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":rejected_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish preserved mock dictation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "preserved mock dictation result").await,
        json!({
            "type":"dictation_empty",
            "operation_id":operation_id
        })
    );
}

#[tokio::test]
#[cfg(unix)]
async fn stale_dictation_start_returns_a_generic_correlated_rejection() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-rejected-start", "Reply A");
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (bound_sender, bound_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel::<()>();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "rejected-start-a").await;
        bound_sender.send(()).expect("signal context A");
        finish_receiver.await.expect("release fake Realtime");
        tokio::time::timeout(Duration::from_millis(100), socket.next())
            .await
            .is_ok()
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("bind context", null).to_string().into(),
        ))
        .await
        .expect("bind context A");
    bound_receiver.await.expect("context A bound");
    let _ = next_bound_summary_id(&mut runtime.browser, "context A summary").await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation");
    loop {
        if next_browser_json(&mut runtime.browser, "dictation mode").await
            == json!({"type":"voice_mode","mode":"dictation"})
        {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({
                "type":"ptt_start",
                "recording_id":1,
                "summary_id":"stale-summary"
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send stale dictation start");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "recording_rejected",
            "rejected dictation start",
        )
        .await,
        json!({
            "type":"recording_rejected",
            "mode":"dictation",
            "recording_id":1,
            "message":"Recording could not start. Try again."
        })
    );
    finish_sender.send(()).expect("finish fake Realtime");
    assert!(!fake_realtime.await.expect("fake Realtime task"));
}

#[tokio::test]
async fn live_response_waits_for_audio_commit_ack_before_response_create() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (commit_sender, commit_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "commit-gated session update").await["type"],
            "session.update"
        );
        for expected in [
            "input_audio_buffer.clear",
            "input_audio_buffer.append",
            "input_audio_buffer.commit",
        ] {
            assert_eq!(
                next_fake_realtime_json(&mut socket, "commit-gated live turn").await["type"],
                expected
            );
        }
        commit_sender.send(()).expect("signal live commit");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), socket.next())
                .await
                .is_err(),
            "response.create was sent before the audio commit acknowledgement"
        );
        socket
            .send(Message::Text(
                json!({"type":"input_audio_buffer.committed","item_id":"live-item"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("acknowledge live commit");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "post-commit response").await["type"],
            "response.create"
        );
        for event in [
            json!({"type":"response.created","response":{"id":"commit-gated-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"commit-gated-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete commit-gated response");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start commit-gated recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "commit-gated start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"commit-gated audio").into()))
        .await
        .expect("send commit-gated audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish commit-gated recording");
    commit_receiver.await.expect("live commit signal");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "commit-gated terminal flush").await,
        json!({"type":"flush_audio"})
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_empty_transcript_is_deleted_before_ready() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (delete_seen_sender, delete_seen_receiver) = tokio::sync::oneshot::channel();
    let (release_delete_sender, release_delete_receiver) = tokio::sync::oneshot::channel();
    let (controls_checked_sender, controls_checked_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "empty live session update").await["type"],
            "session.update"
        );
        for expected in [
            "input_audio_buffer.clear",
            "input_audio_buffer.append",
            "input_audio_buffer.commit",
        ] {
            assert_eq!(
                next_fake_realtime_json(&mut socket, "empty live turn").await["type"],
                expected
            );
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"empty-live-item",
                "previous_item_id":null
            }),
            json!({"type":"response.created","response":{"id":"empty-live-response"}}),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"empty-live-item",
                "transcript":""
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance empty live turn");
            if event["type"] == "input_audio_buffer.committed" {
                assert_eq!(
                    next_fake_realtime_json(&mut socket, "empty live response request").await["type"],
                    "response.create"
                );
            }
        }
        let delete = next_fake_realtime_json(&mut socket, "empty live item deletion").await;
        assert_eq!(delete["type"], "conversation.item.delete");
        assert_eq!(delete["item_id"], "empty-live-item");
        delete_seen_sender
            .send(())
            .expect("signal empty live item deletion");
        controls_checked_receiver
            .await
            .expect("check controls during empty live deletion");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"empty-live-response","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete empty live response");
        release_delete_receiver
            .await
            .expect("release empty live item deletion");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"empty-live-item",
                    "event_id":"provider-empty-live-item-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge empty live item deletion");
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start empty live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "empty live start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"empty live audio").into()))
        .await
        .expect("send empty live audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish empty live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "empty live terminal flush").await,
        json!({"type":"flush_audio"})
    );
    tokio::time::timeout(Duration::from_millis(500), delete_seen_receiver)
        .await
        .expect("empty live deletion timeout")
        .expect("empty live deletion signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("reject recording during empty live deletion");
    assert_eq!(
        next_browser_json_of_type(
            &mut runtime.browser,
            "recording_rejected",
            "deletion-gated live recording",
        )
        .await,
        json!({
            "type":"recording_rejected",
            "mode":"live_response",
            "recording_id":2,
            "message":"Recording could not start. Try again."
        })
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("reject host change during empty live deletion");
    assert_eq!(
        next_browser_json_of_type(&mut runtime.browser, "error", "deletion-gated host change",)
            .await,
        json!({
            "type":"error",
            "message":"Wait for active dictation item deletion before changing host."
        })
    );
    let blocked_turn = test_text_turn!("must wait for deletion", null);
    let blocked_turn_id = test_text_turn_id(&blocked_turn);
    runtime
        .browser
        .send(Message::Text(blocked_turn.to_string().into()))
        .await
        .expect("reject text during empty live deletion");
    assert_eq!(
        next_browser_json_including_text_ack(&mut runtime.browser, "deletion-gated text turn")
            .await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":blocked_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    controls_checked_sender
        .send(())
        .expect("finish empty live deletion control checks");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("pre-delete empty live JSON");
        assert_ne!(frame["type"], "user_transcript");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
    }
    release_delete_sender
        .send(())
        .expect("release empty live deletion acknowledgement");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "post-delete empty live ready").await,
        json!({"type":"state","state":"ready"})
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_empty_deletion_before_response_done_emits_one_ready() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reverse empty live Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("reverse empty live Realtime address")
        .port();
    let (deletion_acked_sender, deletion_acked_receiver) = tokio::sync::oneshot::channel();
    let (release_done_sender, release_done_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "reverse empty live session update").await["type"],
            "session.update"
        );
        for expected in [
            "input_audio_buffer.clear",
            "input_audio_buffer.append",
            "input_audio_buffer.commit",
        ] {
            assert_eq!(
                next_fake_realtime_json(&mut socket, "reverse empty live turn").await["type"],
                expected
            );
        }
        socket
            .send(Message::Text(
                json!({"type":"input_audio_buffer.committed","item_id":"reverse-empty-item"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("acknowledge reverse empty live commit");
        loop {
            if next_fake_realtime_json(&mut socket, "reverse empty live response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"reverse-empty-response"}}),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"reverse-empty-item",
                "transcript":""
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance reverse empty live turn");
        }
        let delete = next_fake_realtime_json(&mut socket, "reverse empty live deletion").await;
        assert_eq!(delete["type"], "conversation.item.delete");
        assert_eq!(delete["item_id"], "reverse-empty-item");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"reverse-empty-item",
                    "event_id":"provider-reverse-empty-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge reverse empty live deletion");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"reverse-empty-barrier"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send reverse empty deletion barrier");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "reverse empty deletion barrier").await;
            if event["type"] == "response.cancel" && event["response_id"] == "reverse-empty-barrier"
            {
                break;
            }
        }
        deletion_acked_sender
            .send(())
            .expect("signal reverse empty deletion acknowledgement");
        release_done_receiver
            .await
            .expect("release reverse empty response terminal");
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"reverse-empty-response","status":"completed"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete reverse empty response");
        finish_receiver
            .await
            .expect("finish reverse empty live test");
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start reverse empty live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "reverse empty live start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(
            test_pcm(b"reverse empty live audio").into(),
        ))
        .await
        .expect("send reverse empty live audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish reverse empty live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "reverse empty live terminal flush").await,
        json!({"type":"flush_audio"})
    );
    deletion_acked_receiver
        .await
        .expect("reverse empty deletion acknowledgement");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send reverse empty pre-terminal barrier");
    let mut protocol_ready = false;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "reverse empty pre-terminal barrier").await;
        assert_ne!(
            frame,
            json!({"type":"state","state":"ready"}),
            "deletion acknowledgement emitted ready before response completion"
        );
        protocol_ready |= frame["type"] == "protocol_ready";
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }
    release_done_sender
        .send(())
        .expect("release reverse empty response terminal");
    let mut ready_count = 0;
    loop {
        let frame = next_browser_json(&mut runtime.browser, "reverse empty final ready").await;
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
        if ready_count == 1 {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send reverse empty final ready barrier");
    protocol_ready = false;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "reverse empty final ready barrier").await;
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
        protocol_ready |= frame["type"] == "protocol_ready";
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }
    assert_eq!(
        ready_count, 1,
        "reverse completion emitted duplicate ready states"
    );
    finish_sender
        .send(())
        .expect("finish reverse empty live test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_empty_deletion_ack_before_tool_return_preserves_deferred_host() {
    let (latch, started, release_guard) = process_latch();
    let executor = Arc::new(LatchingPermissionsExecutor {
        latch,
        permission_calls: AtomicUsize::new(0),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("empty live deferred host Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("empty live deferred host Realtime address")
        .port();
    let (tool_started_sender, tool_started_receiver) = tokio::sync::oneshot::channel();
    let (delete_seen_sender, delete_seen_receiver) = tokio::sync::oneshot::channel();
    let (deletion_acked_sender, deletion_acked_receiver) = tokio::sync::oneshot::channel();
    let (release_delete_sender, release_delete_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "empty live deferred host session update").await["type"],
            "session.update"
        );
        for expected in [
            "input_audio_buffer.clear",
            "input_audio_buffer.append",
            "input_audio_buffer.commit",
        ] {
            assert_eq!(
                next_fake_realtime_json(&mut socket, "empty live deferred host turn").await["type"],
                expected
            );
        }
        socket
            .send(Message::Text(
                json!({"type":"input_audio_buffer.committed","item_id":"empty-host-item"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("acknowledge empty live deferred host commit");
        loop {
            if next_fake_realtime_json(&mut socket, "empty live deferred host response request")
                .await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"empty-host-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create empty live deferred host response");
        for event in function_call_events_at(
            "empty-host-response",
            "empty-host-tool-item",
            "empty-host-call",
            0,
            "list_pending_permissions",
            "{}",
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send empty live deferred host tool call");
        }
        tool_started_receiver
            .await
            .expect("wait for empty live deferred host tool");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"empty-host-item",
                    "transcript":""
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete empty live deferred host transcription");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "empty live deferred host deletion").await;
            if event["type"] == "conversation.item.delete" {
                assert_eq!(event["item_id"], "empty-host-item");
                break;
            }
        }
        delete_seen_sender
            .send(())
            .expect("signal empty live deferred host deletion");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "empty live deferred host cancellation").await;
            if event["type"] == "response.cancel" {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"response.done",
                    "response":{"id":"empty-host-response","status":"cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete empty live deferred host response");
        release_delete_receiver
            .await
            .expect("release empty live deferred host deletion");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"empty-host-item",
                    "event_id":"provider-empty-host-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge empty live deferred host deletion");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"empty-host-barrier"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send empty live deferred host deletion barrier");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "empty live deferred host deletion barrier")
                    .await;
            if event["type"] == "response.cancel" && event["response_id"] == "empty-host-barrier" {
                break;
            }
        }
        deletion_acked_sender
            .send(())
            .expect("signal empty live deferred host deletion acknowledgement");
        finish_receiver
            .await
            .expect("finish empty live deferred host test");
    });

    let hosts = vec![
        PaseoHostProfile {
            id: "first".to_owned(),
            label: "First".to_owned(),
            target: None,
            default: true,
            default_cwd: "~/".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
        PaseoHostProfile {
            id: "second".to_owned(),
            label: "Second".to_owned(),
            target: Some("second.example:6767".to_owned()),
            default: false,
            default_cwd: "~/dev".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
    ];
    let process_executor: Arc<dyn ProcessExecutor> = executor;
    let mut runtime = start_injected_realtime_browser_with_hosts_and_process(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
        hosts,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "empty live deferred host browser").await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start empty live deferred host recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "empty live deferred host start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(
            test_pcm(b"empty deferred host audio").into(),
        ))
        .await
        .expect("send empty live deferred host audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish empty live deferred host recording");
    let _ = next_browser_json(
        &mut runtime.browser,
        "empty live deferred host terminal flush",
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .expect("empty live deferred host process start timeout")
        .expect("empty live deferred host process start signal");
    tool_started_sender
        .send(())
        .expect("signal empty live deferred host tool");
    tokio::time::timeout(Duration::from_secs(2), delete_seen_receiver)
        .await
        .expect("empty live deferred host deletion timeout")
        .expect("empty live deferred host deletion signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("defer host during empty live deletion");
    let _ = next_browser_json_of_type(
        &mut runtime.browser,
        "proposal",
        "empty live deferred host proposal clear",
    )
    .await;
    release_delete_sender
        .send(())
        .expect("release empty live deferred host deletion");
    deletion_acked_receiver
        .await
        .expect("empty live deferred host deletion acknowledgement");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("probe empty live deferred host deletion acknowledgement");
    let mut protocol_ready = false;
    loop {
        let frame = next_browser_json(
            &mut runtime.browser,
            "empty live deferred host acknowledgement probe",
        )
        .await;
        assert_ne!(
            frame,
            json!({"type":"state","state":"ready"}),
            "ready escaped while the model tool still owned the engine"
        );
        protocol_ready |= frame["type"] == "protocol_ready";
        if frame["type"] == "host_state" {
            assert_eq!(frame["selected_host_id"], "first");
        }
        if frame["type"] == "dashboard_state" {
            assert_eq!(frame["selected_host_id"], "first");
        }
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }
    release_guard.release();
    let mut host_published = false;
    let mut dashboard_published = false;
    let mut proposal_cleared = false;
    let mut ready_count = 0;
    loop {
        let frame = next_browser_json(&mut runtime.browser, "empty live deferred host drain").await;
        if frame["type"] == "host_state" {
            assert_eq!(frame["selected_host_id"], "second");
            host_published = true;
            continue;
        }
        if frame["type"] == "dashboard_state" {
            assert!(host_published, "dashboard preceded deferred host state");
            assert_eq!(frame["selected_host_id"], "second");
            dashboard_published = true;
            continue;
        }
        if frame == json!({"type":"proposal","echo":null}) {
            assert!(
                dashboard_published,
                "proposal clear preceded deferred dashboard"
            );
            proposal_cleared = true;
            continue;
        }
        if frame == json!({"type":"state","state":"ready"}) {
            assert!(host_published, "ready preceded deferred host state");
            assert!(dashboard_published, "ready preceded deferred dashboard");
            assert!(proposal_cleared, "ready preceded deferred proposal clear");
            ready_count += 1;
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send empty live final ready barrier");
    let mut protocol_ready = false;
    loop {
        let frame = next_browser_json(&mut runtime.browser, "empty live final ready barrier").await;
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
        protocol_ready |= frame["type"] == "protocol_ready";
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }
    assert_eq!(
        ready_count, 1,
        "deferred host emitted duplicate ready states"
    );
    finish_sender
        .send(())
        .expect("finish empty live deferred host test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn normal_cleanup_deferred_host_publishes_transition_before_one_ready() {
    let (dashboard_latch, dashboard_started, release_dashboard) = process_latch();
    let executor = Arc::new(LatchingDashboardExecutor {
        latch: dashboard_latch,
        dashboard_armed: AtomicBool::new(false),
    });
    let (cleanup_started_sender, cleanup_started_receiver) = tokio::sync::oneshot::channel();
    let (release_cleanup_sender, release_cleanup_receiver) = tokio::sync::oneshot::channel();
    let cleaner = Arc::new(LatchedDictationCleaner {
        started: Mutex::new(Some(cleanup_started_sender)),
        release: Mutex::new(Some(release_cleanup_receiver)),
    });
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("normal cleanup deferred host Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("normal cleanup deferred host Realtime address")
        .port();
    let (delete_seen_sender, delete_seen_receiver) = tokio::sync::oneshot::channel();
    let (host_change_seen_sender, host_change_seen_receiver) = tokio::sync::oneshot::channel();
    let (release_delete_sender, release_delete_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "normal cleanup deferred host session update")
                .await["type"],
            "session.update"
        );
        bind_fake_realtime_context(&mut socket, "normal-cleanup-deferred-host").await;
        let mut saw_clear = false;
        let mut saw_commit = false;
        while !saw_clear || !saw_commit {
            let event =
                next_fake_realtime_json(&mut socket, "normal cleanup deferred host recording")
                    .await;
            assert_ne!(event["type"], "response.create");
            saw_clear |= event["type"] == "input_audio_buffer.clear";
            saw_commit |= event["type"] == "input_audio_buffer.commit";
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"normal-cleanup-host-item",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"normal-cleanup-host-item",
                "transcript":"Normal cleanup before a host change"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance normal cleanup deferred host dictation");
        }
        let delete = loop {
            let event =
                next_fake_realtime_json(&mut socket, "normal cleanup deferred host deletion").await;
            if event["type"] == "conversation.item.delete" {
                break event;
            }
        };
        assert_eq!(delete["item_id"], "normal-cleanup-host-item");
        delete_seen_sender
            .send(())
            .expect("signal normal cleanup deletion");
        loop {
            let event =
                next_fake_realtime_json(&mut socket, "normal cleanup deferred host clear").await;
            if event["type"] == "input_audio_buffer.clear" {
                break;
            }
        }
        host_change_seen_sender
            .send(())
            .expect("signal normal cleanup host change");
        release_delete_receiver
            .await
            .expect("release normal cleanup deletion");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.deleted",
                    "item_id":"normal-cleanup-host-item",
                    "event_id":"provider-normal-cleanup-host-deleted"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("acknowledge normal cleanup deletion");
        finish_receiver
            .await
            .expect("finish normal cleanup deferred host test");
    });

    let hosts = vec![
        PaseoHostProfile {
            id: "first".to_owned(),
            label: "First".to_owned(),
            target: None,
            default: true,
            default_cwd: "~/".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
        PaseoHostProfile {
            id: "second".to_owned(),
            label: "Second".to_owned(),
            target: Some("second.example:6767".to_owned()),
            default: false,
            default_cwd: "~/dev".to_owned(),
            default_provider: "opencode/model".to_owned(),
        },
    ];
    let process_executor: Arc<dyn ProcessExecutor> = executor.clone();
    let mut runtime = start_injected_realtime_browser_with_hosts_process_and_cleaner(
        Arc::new(SystemRealtimeConnector),
        &format!("ws://127.0.0.1:{realtime_port}"),
        None,
        "gpt-realtime-test",
        Some("test-password"),
        process_executor,
        hosts,
        cleaner,
    )
    .await;
    initialize_browser_protocol(&mut runtime.browser, "normal cleanup deferred host browser").await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    let _operation_id =
        start_and_commit_bound_dictation(&mut runtime.browser, &summary_id, 1).await;
    tokio::time::timeout(Duration::from_secs(2), cleanup_started_receiver)
        .await
        .expect("normal cleanup start timeout")
        .expect("normal cleanup start signal");
    tokio::time::timeout(Duration::from_secs(2), delete_seen_receiver)
        .await
        .expect("normal cleanup deletion timeout")
        .expect("normal cleanup deletion signal");
    release_cleanup_sender
        .send(())
        .expect("release normal cleanup result");

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send normal cleanup completion barrier");
    let mut protocol_ready = false;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "normal cleanup completion barrier").await;
        assert_ne!(
            frame,
            json!({"type":"state","state":"ready"}),
            "ready escaped while normal cleanup deletion was unresolved"
        );
        protocol_ready |= frame["type"] == "protocol_ready";
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"select_host","host_id":"second"})
                .to_string()
                .into(),
        ))
        .await
        .expect("defer host until normal cleanup deletion");
    host_change_seen_receiver
        .await
        .expect("normal cleanup host change signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send pending normal cleanup host barrier");
    protocol_ready = false;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "pending normal cleanup host barrier").await;
        assert_ne!(
            frame,
            json!({"type":"state","state":"ready"}),
            "ready escaped while the normal cleanup host intent was pending"
        );
        protocol_ready |= frame["type"] == "protocol_ready";
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }

    executor.dashboard_armed.store(true, Ordering::SeqCst);
    release_delete_sender
        .send(())
        .expect("release normal cleanup deletion acknowledgement");
    tokio::time::timeout(Duration::from_secs(2), dashboard_started)
        .await
        .expect("normal cleanup host process start timeout")
        .expect("normal cleanup host process start signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send active normal cleanup host barrier");
    protocol_ready = false;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "active normal cleanup host barrier").await;
        assert_ne!(
            frame,
            json!({"type":"state","state":"ready"}),
            "ready escaped while the host-selection job owned the engine"
        );
        protocol_ready |= frame["type"] == "protocol_ready";
        if frame["type"] == "host_state" {
            assert_eq!(frame["selected_host_id"], "first");
        }
        if frame["type"] == "dashboard_state" {
            assert_eq!(frame["selected_host_id"], "first");
        }
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }

    release_dashboard.release();
    let mut host_published = false;
    let mut dashboard_published = false;
    let mut proposal_cleared = false;
    let mut ready_count = 0;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "normal cleanup deferred host drain").await;
        if frame["type"] == "host_state" {
            assert_eq!(frame["selected_host_id"], "second");
            host_published = true;
            continue;
        }
        if frame["type"] == "dashboard_state" {
            assert!(
                host_published,
                "dashboard preceded normal cleanup host state"
            );
            assert_eq!(frame["selected_host_id"], "second");
            dashboard_published = true;
            continue;
        }
        if frame == json!({"type":"proposal","echo":null}) {
            assert!(
                dashboard_published,
                "proposal clear preceded normal cleanup dashboard"
            );
            proposal_cleared = true;
            continue;
        }
        if frame == json!({"type":"state","state":"ready"}) {
            assert!(host_published, "ready preceded normal cleanup host state");
            assert!(
                dashboard_published,
                "ready preceded normal cleanup dashboard"
            );
            assert!(
                proposal_cleared,
                "ready preceded normal cleanup proposal clear"
            );
            ready_count += 1;
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send normal cleanup final ready barrier");
    protocol_ready = false;
    loop {
        let frame =
            next_browser_json(&mut runtime.browser, "normal cleanup final ready barrier").await;
        ready_count += usize::from(frame == json!({"type":"state","state":"ready"}));
        protocol_ready |= frame["type"] == "protocol_ready";
        if protocol_ready && frame["type"] == "dashboard_state" {
            break;
        }
    }
    assert_eq!(
        ready_count, 1,
        "normal cleanup emitted duplicate ready states"
    );
    finish_sender
        .send(())
        .expect("finish normal cleanup deferred host test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn unacknowledged_response_create_closes_the_realtime_session() {
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "response-timeout session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "unacknowledged response create").await["type"]
                == "response.create"
            {
                break;
            }
        }
        let closed = tokio::time::timeout(Duration::from_secs(7), socket.next())
            .await
            .expect("response timeout did not close provider socket");
        assert!(closed.is_none_or(|message| message.is_ok()));
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    let timeout_turn = test_text_turn!("timeout response create", null);
    let timeout_turn_id = test_text_turn_id(&timeout_turn);
    runtime
        .browser
        .send(Message::Text(timeout_turn.to_string().into()))
        .await
        .expect("request unacknowledged response");
    assert_eq!(
        next_browser_json_including_text_ack(&mut runtime.browser, "timeout turn acceptance").await,
        json!({"type":"text_turn_accepted","turn_id":timeout_turn_id})
    );
    let message = tokio::time::timeout(Duration::from_secs(6), runtime.browser.next())
        .await
        .expect("response-create acknowledgement timeout")
        .expect("response timeout browser frame")
        .expect("response timeout browser result");
    let Message::Text(message) = message else {
        panic!("response timeout must be text");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&message).expect("response timeout JSON"),
        json!({
            "type":"error",
            "message":"Realtime did not acknowledge the response request. Reconnecting safely."
        })
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn cancelled_unsent_response_requests_do_not_drop_the_newest_turn() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "compaction session update").await["type"],
            "session.update"
        );
        let first_create = loop {
            let event = next_fake_realtime_json(&mut socket, "first compacted create").await;
            if event["type"] == "response.create" {
                break event;
            }
        };
        assert_eq!(first_create["event_id"], "response-create-1");
        first_sender.send(()).expect("signal first response create");
        loop {
            let event = next_fake_realtime_json(&mut socket, "newest queued text").await;
            if event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("newest-70")
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"superseded-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("resolve superseded response request");
        let newest_create = loop {
            let event = next_fake_realtime_json(&mut socket, "newest compacted create").await;
            if event["type"] == "response.create" {
                break event;
            }
        };
        assert_eq!(newest_create["event_id"], "response-create-71");
        for event in [
            json!({"type":"response.created","response":{"id":"newest-response"}}),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"newest-response",
                "transcript":"Newest turn survived"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"newest-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete newest response");
        }
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("superseded", null).to_string().into(),
        ))
        .await
        .expect("request superseded response");
    first_receiver.await.expect("first response create signal");
    for index in 1..=70 {
        runtime
            .browser
            .send(Message::Text(
                test_text_turn!(format!("newest-{index}"), null)
                    .to_string()
                    .into(),
            ))
            .await
            .expect("replace queued response");
    }
    let transcript = loop {
        let frame = next_browser_json(&mut runtime.browser, "compacted response frame").await;
        if frame["type"] == "transcript_done" {
            break frame["text"].as_str().map(str::to_owned);
        }
    };
    assert_eq!(transcript.as_deref(), Some("Newest turn survived"));
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn delayed_audio_commit_acks_cannot_cross_live_and_dictation_recordings() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-delayed-commits", "Reply A");
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (live_commit_sender, live_commit_receiver) = tokio::sync::oneshot::channel();
    let (release_live_sender, release_live_receiver) = tokio::sync::oneshot::channel();
    let (dictation_commit_sender, dictation_commit_receiver) = tokio::sync::oneshot::channel();
    let (release_dictation_sender, release_dictation_receiver) = tokio::sync::oneshot::channel();
    let (dictation_ack_sender, dictation_ack_receiver) = tokio::sync::oneshot::channel();
    let (release_transcript_sender, release_transcript_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "delayed-commit-context").await;
        loop {
            let event = next_fake_realtime_json(&mut socket, "delayed live commit").await;
            if event["type"] == "input_audio_buffer.commit" {
                break;
            }
        }
        live_commit_sender
            .send(())
            .expect("signal delayed live commit");
        release_live_receiver
            .await
            .expect("release live commit ack");
        assert!(
            tokio::time::timeout(Duration::from_millis(150), socket.next())
                .await
                .is_err(),
            "blocked live input reached the provider"
        );
        acknowledge_fake_audio_commit(&mut socket, "delayed-live-item").await;
        loop {
            if next_fake_realtime_json(&mut socket, "post-live-commit response").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"delayed-live-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"delayed-live-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete delayed live response");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "delayed dictation commit").await;
            if event["type"] == "input_audio_buffer.commit" {
                break;
            }
        }
        dictation_commit_sender
            .send(())
            .expect("signal delayed dictation commit");
        release_dictation_receiver
            .await
            .expect("release dictation commit ack");
        assert!(
            tokio::time::timeout(Duration::from_millis(150), socket.next())
                .await
                .is_err(),
            "blocked dictation input reached the provider"
        );
        acknowledge_fake_audio_commit(&mut socket, "delayed-dictation-item").await;
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.input_audio_transcription.delta",
                    "item_id":"delayed-dictation-item",
                    "delta":"Delayed"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send delayed transcription preview");
        dictation_ack_sender
            .send(())
            .expect("signal dictation commit ack");
        release_transcript_receiver
            .await
            .expect("release delayed transcription");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"delayed-dictation-item",
                    "transcript":"Delayed dictation"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("complete delayed transcription");
        acknowledge_fake_dictation_delete(
            &mut socket,
            "delayed-dictation-item",
            "provider-delayed-dictation-deleted",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start delayed live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "delayed live start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"delayed live audio").into()))
        .await
        .expect("send delayed live audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish delayed live recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "delayed live terminal flush").await,
        json!({"type":"flush_audio"})
    );
    live_commit_receiver
        .await
        .expect("delayed live commit signal");
    let live_commit_text_turn = test_text_turn!("blocked during live commit", summary_id);
    let live_commit_turn_id = test_text_turn_id(&live_commit_text_turn);
    for control in [
        json!({"type":"set_voice_mode","mode":"dictation"}),
        json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id}),
        live_commit_text_turn,
    ] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send input during delayed live commit");
    }
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"blocked live bytes").into()))
        .await
        .expect("send binary during delayed live commit");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "live commit mode rejection").await,
        json!({
            "type":"error",
            "message":"Finish or abort active recording before changing mode."
        })
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "live commit start rejection").await,
        json!({
            "type":"recording_rejected",
            "mode":"live_response",
            "recording_id":2,
            "message":"Recording could not start. Try again."
        })
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "live commit text rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":live_commit_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    release_live_sender.send(()).expect("release live commit");
    loop {
        if next_browser_json(&mut runtime.browser, "delayed live response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select dictation after live ack");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "post-live dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":3,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start delayed dictation");
    let operation = next_browser_json(&mut runtime.browser, "delayed dictation operation").await;
    assert_eq!(operation["type"], "dictation_operation");
    assert_eq!(operation["recording_id"], 3);
    let operation_id = operation["operation_id"].clone();
    assert_eq!(
        next_browser_json(&mut runtime.browser, "delayed dictation start flush").await,
        json!({"type":"flush_audio"})
    );
    let recording_text_turn = test_text_turn!("blocked while recording", summary_id);
    let recording_turn_id = test_text_turn_id(&recording_text_turn);
    runtime
        .browser
        .send(Message::Text(recording_text_turn.to_string().into()))
        .await
        .expect("send text while dictation records");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "recording text rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":recording_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"delayed dictation audio").into()))
        .await
        .expect("send delayed dictation audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish delayed dictation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "delayed dictation state").await,
        json!({"type":"state","state":"transcribing"})
    );
    dictation_commit_receiver
        .await
        .expect("delayed dictation commit signal");
    let dictation_commit_text_turn = test_text_turn!("blocked during dictation commit", summary_id);
    let dictation_commit_turn_id = test_text_turn_id(&dictation_commit_text_turn);
    for control in [
        json!({"type":"ptt_start","recording_id":4,"summary_id":summary_id}),
        dictation_commit_text_turn,
    ] {
        runtime
            .browser
            .send(Message::Text(control.to_string().into()))
            .await
            .expect("send input during delayed dictation commit");
    }
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"blocked dictation bytes").into()))
        .await
        .expect("send binary during delayed dictation commit");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "dictation commit start rejection").await,
        json!({
            "type":"recording_rejected",
            "mode":"dictation",
            "recording_id":4,
            "message":"Recording could not start. Try again."
        })
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "dictation commit text rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":dictation_commit_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    release_dictation_sender
        .send(())
        .expect("release dictation commit");
    dictation_ack_receiver
        .await
        .expect("dictation commit ack signal");
    let preview = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_preview",
        "post-ack dictation preview",
    )
    .await;
    assert_eq!(preview["operation_id"], operation_id);
    let transcription_text_turn = test_text_turn!("blocked during transcription", summary_id);
    let transcription_turn_id = test_text_turn_id(&transcription_text_turn);
    runtime
        .browser
        .send(Message::Text(transcription_text_turn.to_string().into()))
        .await
        .expect("send text during transcription");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "transcription text rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":transcription_turn_id,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    release_transcript_sender
        .send(())
        .expect("release delayed transcript");
    let result = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_result",
        "delayed dictation result",
    )
    .await;
    assert_eq!(result["operation_id"], operation_id);
    assert_eq!(result["text"], "Delayed dictation");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn dictation_commit_timeout_closes_without_terminal_or_ready() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-commit-timeout", "Reply A");
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
        bind_fake_realtime_context(&mut socket, "commit-timeout-context").await;
        loop {
            if next_fake_realtime_json(&mut socket, "timed-out dictation commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        let closed = tokio::time::timeout(Duration::from_secs(6), socket.next())
            .await
            .expect("dictation commit timeout did not close provider socket");
        matches!(closed, Some(Ok(Message::Close(_))) | None)
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select timeout dictation mode");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "timeout dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start timeout dictation");
    let operation = next_browser_json(&mut runtime.browser, "timeout dictation operation").await;
    assert_eq!(operation["recording_id"], 1);
    let operation_id = operation["operation_id"].clone();
    assert_eq!(
        next_browser_json(&mut runtime.browser, "timeout dictation flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"timeout dictation audio").into()))
        .await
        .expect("send timeout dictation audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish timeout dictation");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "timeout transcribing state").await,
        json!({"type":"state","state":"transcribing"})
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "dictation commit timeout browser close"
        );
        let message = tokio::time::timeout(remaining, runtime.browser.next())
            .await
            .expect("dictation commit timeout browser frame");
        let Some(message) = message else { break };
        match message.expect("dictation commit timeout browser result") {
            Message::Text(message) => {
                let frame: Value =
                    serde_json::from_str(&message).expect("dictation commit timeout JSON");
                assert_ne!(frame["type"], "dictation_result");
                assert_ne!(frame["type"], "dictation_empty");
                assert_ne!(frame["type"], "dictation_failed");
                assert_ne!(frame["type"], "dictation_cancelled");
                assert_ne!(frame, json!({"type":"state","state":"ready"}));
                assert_ne!(frame["operation_id"], operation_id);
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider remained connected after dictation commit timeout"
    );
}

#[tokio::test]
#[cfg(unix)]
async fn mock_recording_ids_are_monotonic_across_voice_modes() {
    let (_paseo_directory, runtime, summary_id) = start_bound_mock_dictation_browser().await;
    let LiveRealtimeRuntime {
        _directory,
        _guard,
        mut browser,
    } = runtime;
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start cross-mode mock dictation");
    let operation = next_browser_json(&mut browser, "cross-mode mock operation").await;
    assert_eq!(operation["recording_id"], 1);
    assert_eq!(
        next_browser_json(&mut browser, "cross-mode mock dictation start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Text(
            json!({"type":"cancel_dictation","operation_id":operation["operation_id"]})
                .to_string()
                .into(),
        ))
        .await
        .expect("cancel cross-mode mock dictation");
    assert_eq!(
        next_browser_json(&mut browser, "cross-mode mock cancellation").await["type"],
        "dictation_cancelled"
    );
    assert_eq!(
        next_browser_json(&mut browser, "cross-mode mock ready").await,
        json!({"type":"state","state":"ready"})
    );
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"live_response"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select cross-mode mock live response");
    assert_eq!(
        next_browser_json(&mut browser, "cross-mode mock live mode").await,
        json!({"type":"voice_mode","mode":"live_response"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("replay dictation recording ID in live mode");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), browser.next())
            .await
            .is_err(),
        "dictation recording ID was reusable in live mode"
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start next cross-mode mock recording");
    assert_eq!(
        next_browser_json(&mut browser, "cross-mode mock live start flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_abort","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("abort cross-mode mock live recording");
    assert_eq!(
        next_browser_json(&mut browser, "cross-mode mock abort flush").await,
        json!({"type":"flush_audio"})
    );
}

#[tokio::test]
async fn correlated_audio_commit_error_closes_before_a_late_ack_can_be_accepted() {
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "commit-error session update").await["type"],
            "session.update"
        );
        let commit = loop {
            let event = next_fake_realtime_json(&mut socket, "correlated audio commit").await;
            if event["type"] == "input_audio_buffer.commit" {
                break event;
            }
        };
        socket
            .send(Message::Text(
                json!({
                    "type":"error",
                    "error":{
                        "type":"invalid_request_error",
                        "message":"commit rejected",
                        "event_id":commit["event_id"]
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("reject correlated audio commit");
        acknowledge_fake_audio_commit(&mut socket, "late-commit-after-error").await;
        tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .is_ok()
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start rejected audio commit recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "commit-error start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"rejected commit audio").into()))
        .await
        .expect("send rejected commit audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish rejected commit recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "commit-error terminal flush").await,
        json!({"type":"flush_audio"})
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "correlated commit error").await,
        json!({
            "type":"error",
            "message":"Realtime rejected recorded audio. Reconnecting safely."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("correlated commit error close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider socket remained open after a correlated commit error"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn replayed_audio_commit_ack_during_a_later_recording_closes_as_ambiguous() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (first_ready_sender, first_ready_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "ACK replay session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "first audio commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        acknowledge_fake_audio_commit(&mut socket, "committed-item-a").await;
        loop {
            if next_fake_realtime_json(&mut socket, "first response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        first_ready_sender
            .send(())
            .expect("signal first commit acknowledged");

        loop {
            if next_fake_realtime_json(&mut socket, "second audio commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        acknowledge_fake_audio_commit(&mut socket, "committed-item-a").await;
        let closed = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("replayed commit ambiguity close timeout");
        matches!(closed, Some(Ok(Message::Close(_))) | None)
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start first ACK replay recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "first ACK replay flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"first ACK replay audio").into()))
        .await
        .expect("send first ACK replay audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish first ACK replay recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "first ACK replay terminal flush").await,
        json!({"type":"flush_audio"})
    );
    first_ready_receiver
        .await
        .expect("first ACK replay response request");

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start second ACK replay recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "second ACK replay flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"second ACK replay audio").into()))
        .await
        .expect("send second ACK replay audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish second ACK replay recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "second ACK replay terminal flush").await,
        json!({"type":"flush_audio"})
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "replayed commit ambiguity").await,
        json!({
            "type":"error",
            "message":"Realtime reported conflicting recorded audio state. Reconnecting safely."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("replayed commit browser close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider remained connected after a replayed commit ACK"
    );
}

#[tokio::test]
async fn audio_commit_ack_followed_by_a_correlated_error_closes_as_ambiguous() {
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "ambiguous commit session update").await["type"],
            "session.update"
        );
        let commit = loop {
            let event = next_fake_realtime_json(&mut socket, "ambiguous audio commit").await;
            if event["type"] == "input_audio_buffer.commit" {
                break event;
            }
        };
        let commit_event_id = commit["event_id"]
            .as_str()
            .expect("audio commit event ID")
            .to_owned();
        acknowledge_fake_audio_commit(&mut socket, "ambiguous-commit-item").await;
        loop {
            if next_fake_realtime_json(&mut socket, "post-commit response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({
                    "type":"error",
                    "error":{
                        "type":"invalid_request_error",
                        "message":"late conflicting audio state",
                        "event_id":commit_event_id
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send late correlated commit error");
        tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .is_ok()
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start ambiguous commit recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "ambiguous commit start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"ambiguous commit audio").into()))
        .await
        .expect("send ambiguous commit audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish ambiguous commit recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "ambiguous commit terminal flush").await,
        json!({"type":"flush_audio"})
    );
    assert_eq!(
        next_browser_json(&mut runtime.browser, "ambiguous commit error").await,
        json!({
            "type":"error",
            "message":"Realtime reported conflicting recorded audio state. Reconnecting safely."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("ambiguous commit browser close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider remained connected after conflicting commit state"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn duplicate_provider_fields_fail_before_response_or_tool_state() {
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "strict provider session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "strict provider response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                r#"{"type":"session.updated","type":"response.created","response":{"id":"poison-response"}}"#
                    .into(),
            ))
            .await
            .expect("send duplicate provider type");
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"strict-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send valid response.created");
        socket
            .send(Message::Text(
                r#"{"type":"response.output_audio_transcript.done","response_id":"other-response","response_id":"strict-response","transcript":"duplicate response ID leaked"}"#
                    .into(),
            ))
            .await
            .expect("send duplicate provider response ID");
        for event in [
            json!({
                "type":"response.output_item.added",
                "response_id":"strict-response",
                "output_index":0,
                "item":{"id":"strict-item-1","type":"function_call","call_id":"strict-call-1","name":"cancel_action"}
            }),
            json!({
                "type":"response.output_item.added",
                "response_id":"strict-response",
                "output_index":1,
                "item":{"id":"strict-item-2","type":"function_call","call_id":"strict-call-2","name":"cancel_action"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send strict provider function item");
        }
        socket
            .send(Message::Text(
                r#"{"type":"response.function_call_arguments.done","response_id":"strict-response","item_id":"other-item","item_id":"strict-item-1","call_id":"strict-call-1","name":"cancel_action","arguments":"{}"}"#
                    .into(),
            ))
            .await
            .expect("send duplicate provider item ID");
        socket
            .send(Message::Text(
                r#"{"type":"response.function_call_arguments.done","response_id":"strict-response","item_id":"strict-item-2","call_id":"other-call","call_id":"strict-call-2","name":"cancel_action","arguments":"{}"}"#
                    .into(),
            ))
            .await
            .expect("send duplicate provider call ID");
        for event in [
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"strict-response",
                "transcript":"Strict provider event accepted"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"strict-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send valid strict provider terminal");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("exercise strict provider parsing", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request strict provider response");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut transcripts = Vec::new();
    let mut tool_calls = Vec::new();
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("strict provider browser JSON");
        if frame["type"] == "transcript_done" {
            transcripts.push(frame["text"].as_str().expect("transcript text").to_owned());
        }
        if frame["type"] == "tool" && frame["phase"] == "call" {
            tool_calls.push(frame["name"].as_str().expect("tool name").to_owned());
        }
        if frame == json!({"type":"state","state":"ready"}) {
            break;
        }
    }
    assert_eq!(transcripts, ["Strict provider event accepted"]);
    assert!(
        tool_calls.is_empty(),
        "duplicate provider fields called tools"
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn unowned_live_transcription_completion_is_ignored() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (send_sender, send_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "unowned transcript session update").await["type"],
            "session.update"
        );
        send_receiver.await.expect("send unowned transcript");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"unowned-live-item",
                    "transcript":"must not reach the browser"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send unowned live transcript");
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let mut runtime = start_live_realtime_browser(realtime_port).await;
    send_sender.send(()).expect("trigger unowned transcript");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "unowned live transcript reached the browser"
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn owned_live_transcription_completion_is_emitted_once() {
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "owned transcript session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "owned transcript commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        acknowledge_fake_audio_commit(&mut socket, "owned-live-item").await;
        for _ in 0..2 {
            socket
                .send(Message::Text(
                    json!({
                        "type":"conversation.item.input_audio_transcription.completed",
                        "item_id":"owned-live-item",
                        "transcript":"Owned live transcript"
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send owned live transcript");
        }
        loop {
            if next_fake_realtime_json(&mut socket, "owned transcript response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        for event in [
            json!({"type":"response.created","response":{"id":"owned-transcript-response"}}),
            json!({
                "type":"response.done",
                "response":{"id":"owned-transcript-response","status":"completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("complete owned transcript response");
        }
    });
    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start owned transcript recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "owned transcript start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"owned transcript audio").into()))
        .await
        .expect("send owned transcript audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish owned transcript recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "owned transcript terminal flush").await,
        json!({"type":"flush_audio"})
    );
    let mut transcripts = Vec::new();
    loop {
        let frame = next_browser_json(&mut runtime.browser, "owned transcript frame").await;
        if frame["type"] == "user_transcript" {
            transcripts.push(
                frame["text"]
                    .as_str()
                    .expect("owned transcript text")
                    .to_owned(),
            );
        }
        if frame == json!({"type":"state","state":"ready"}) {
            break;
        }
    }
    assert_eq!(transcripts, ["Owned live transcript"]);
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn voice_mode_change_retires_owned_live_transcription() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (owned_sender, owned_receiver) = tokio::sync::oneshot::channel();
    let (complete_sender, complete_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "mode transcript session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "mode transcript commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        acknowledge_fake_audio_commit(&mut socket, "mode-change-live-item").await;
        loop {
            if next_fake_realtime_json(&mut socket, "mode transcript response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        owned_sender.send(()).expect("signal owned live item");
        complete_receiver
            .await
            .expect("complete mode-change transcript");
        socket
            .send(Message::Text(
                json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"mode-change-live-item",
                    "transcript":"retired by mode change"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send retired mode-change transcript");
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":null})
                .to_string()
                .into(),
        ))
        .await
        .expect("start mode-change transcript recording");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "mode-change transcript start flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(
            test_pcm(b"mode-change transcript audio").into(),
        ))
        .await
        .expect("send mode-change transcript audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","recording_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish mode-change transcript recording");
    assert_eq!(
        next_browser_json(
            &mut runtime.browser,
            "mode-change transcript terminal flush"
        )
        .await,
        json!({"type":"flush_audio"})
    );
    owned_receiver.await.expect("owned mode-change live item");
    for mode in ["dictation", "live_response"] {
        runtime
            .browser
            .send(Message::Text(
                json!({"type":"set_voice_mode","mode":mode})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("change voice mode around live transcript");
        assert_eq!(
            next_browser_json(&mut runtime.browser, "mode-change voice frame").await,
            json!({"type":"voice_mode","mode":mode})
        );
    }
    complete_sender
        .send(())
        .expect("trigger retired mode-change transcript");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), runtime.browser.next())
            .await
            .is_err(),
        "mode-retired live transcript reached the browser"
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn browser_hello_timeout_does_not_allocate_realtime_connector() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut runtime = start_injected_realtime_browser(Arc::new(PendingRealtimeConnector {
        attempts: Arc::clone(&attempts),
    }))
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 0);
    let message = tokio::time::timeout(Duration::from_secs(6), runtime.browser.next())
        .await
        .expect("browser hello deadline")
        .expect("browser hello deadline frame")
        .expect("browser hello deadline result");
    let Message::Text(message) = message else {
        panic!("browser hello deadline must be text");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&message).expect("browser hello deadline JSON"),
        json!({
            "type":"error",
            "message":"Browser protocol negotiation timed out. Reconnect and try again."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("browser hello timeout close deadline");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert_eq!(attempts.load(Ordering::SeqCst), 0);
}

async fn capture_realtime_request(
    base_url: &str,
    api_key: Option<&str>,
    model: &str,
) -> CapturedRealtimeRequest {
    let (connector, receiver) = capturing_realtime_connector();
    let mut runtime = start_injected_realtime_browser_at(connector, base_url, api_key, model).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send request-capture hello");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "request-capture protocol ready").await,
        json!({"type":"protocol_ready","version":2})
    );
    tokio::time::timeout(Duration::from_secs(1), receiver)
        .await
        .expect("Realtime request capture timeout")
        .expect("Realtime request captured")
}

#[tokio::test]
async fn official_openai_realtime_request_gets_exact_bearer_and_safe_model_query() {
    let model = "model/with spaces?variant=one&other=two";
    for (base_url, expected_path) in [
        ("wss://api.openai.com/v1/realtime", "/v1/realtime"),
        ("wss://api.openai.com/v1/realtime/", "/v1/realtime/"),
    ] {
        let captured = capture_realtime_request(base_url, Some("official-test-key"), model).await;

        assert_eq!(
            captured.authorization.as_deref(),
            Some(b"Bearer official-test-key".as_slice())
        );
        assert!(captured.authorization_sensitive);
        let url = reqwest::Url::parse(&captured.uri).expect("captured Realtime URL");
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.host_str(), Some("api.openai.com"));
        assert_eq!(url.path(), expected_path);
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            [("model".into(), model.into())]
        );
    }
}

#[tokio::test]
async fn custom_realtime_requests_never_receive_openai_credentials() {
    for base_url in [
        "ws://localhost:8080/realtime",
        "wss://realtime.example/v1/realtime",
        "wss://api.openai.com.example/v1/realtime",
        "wss://api.openai.com:443/v1/realtime",
        "wss://api.openai.com:444/v1/realtime",
        "wss://api.openai.com/v1/realtime/custom",
    ] {
        let captured =
            capture_realtime_request(base_url, Some("custom-endpoint-secret"), "model").await;
        assert_eq!(captured.authorization, None, "{base_url}");
        assert!(!captured.authorization_sensitive, "{base_url}");
        assert!(
            captured
                .headers
                .iter()
                .all(|(name, value)| name != "api-key"
                    && !String::from_utf8_lossy(value).contains("custom-endpoint-secret")),
            "credential reached custom endpoint {base_url}"
        );
    }
}

#[tokio::test]
async fn keyless_custom_realtime_endpoints_stay_in_real_mode() {
    for base_url in [
        "ws://127.0.0.1:8080/realtime",
        "wss://realtime.example/v1/realtime",
    ] {
        let captured = capture_realtime_request(base_url, None, "model").await;
        assert_eq!(captured.authorization, None, "{base_url}");
        assert!(!captured.authorization_sensitive, "{base_url}");
    }
}

#[tokio::test]
async fn system_realtime_connector_does_not_follow_redirects_with_credentials() {
    let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("redirecting Realtime listener");
    let redirect_address = redirect_listener
        .local_addr()
        .expect("redirecting Realtime address");
    let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("redirect target listener");
    let target_address = target_listener
        .local_addr()
        .expect("redirect target address");
    let redirect_server = tokio::spawn(async move {
        let (mut stream, _) = redirect_listener.accept().await.expect("redirect request");
        let mut request = vec![0_u8; 8 * 1_024];
        let length = stream
            .read(&mut request)
            .await
            .expect("read redirect request");
        let request = String::from_utf8_lossy(&request[..length]).to_ascii_lowercase();
        assert!(request.contains("authorization: bearer redirect-test-key\r\n"));
        stream
            .write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: ws://{target_address}/redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .expect("write Realtime redirect");
    });
    let mut request = format!("ws://{redirect_address}/v1/realtime")
        .into_client_request()
        .expect("redirect test request");
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer redirect-test-key"),
    );

    tokio::time::timeout(
        Duration::from_secs(1),
        SystemRealtimeConnector.connect(request),
    )
    .await
    .expect("Realtime redirect connector deadline")
    .expect_err("Realtime redirect must not be followed");

    redirect_server.await.expect("redirect server");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), target_listener.accept())
            .await
            .is_err(),
        "redirect target received a credential-bearing follow-up"
    );
}

#[tokio::test]
async fn binary_browser_hello_is_protocol_mismatch_without_connector() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut runtime = start_injected_realtime_browser(Arc::new(PendingRealtimeConnector {
        attempts: Arc::clone(&attempts),
    }))
    .await;
    runtime
        .browser
        .send(Message::Binary(test_pcm(b"not a protocol hello").into()))
        .await
        .expect("send binary browser hello");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "binary protocol mismatch").await,
        json!({"type":"protocol_mismatch","required_version":2})
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("binary protocol mismatch close deadline");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert_eq!(attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn realtime_connector_timeout_is_generic_and_closes_browser() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut runtime = start_injected_realtime_browser(Arc::new(PendingRealtimeConnector {
        attempts: Arc::clone(&attempts),
    }))
    .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send connector-timeout hello");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "connector-timeout protocol ready").await,
        json!({"type":"protocol_ready","version":2})
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let message = tokio::time::timeout(Duration::from_secs(6), runtime.browser.next())
        .await
        .expect("Realtime connector deadline")
        .expect("Realtime connector error frame")
        .expect("Realtime connector error result");
    let Message::Text(message) = message else {
        panic!("Realtime connector error must be text");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&message).expect("Realtime connector error JSON"),
        json!({
            "type":"error",
            "message":"Realtime connection unavailable. Reconnect and try again."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("Realtime connector close deadline");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
}

#[tokio::test]
async fn browser_controls_require_an_exact_v2_hello() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (ready_sender, mut ready_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "hello session update").await["type"],
            "session.update"
        );
        ready_sender.send(()).expect("signal provider ready");
        finish_receiver.await.expect("finish hello test");
    });

    let mut runtime =
        start_live_realtime_browser_with_protocol(realtime_port, None, None, None, None, false)
            .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut ready_receiver)
            .await
            .is_err(),
        "provider connected before protocol hello"
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send exact protocol hello");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "protocol ready acknowledgement").await,
        json!({"type":"protocol_ready","version":2})
    );
    ready_receiver.await.expect("provider ready signal");
    let mut frame_types = Vec::new();
    for _ in 0..5 {
        frame_types.push(
            next_browser_json(&mut runtime.browser, "post-hello browser state").await["type"]
                .as_str()
                .expect("post-hello frame type")
                .to_owned(),
        );
    }
    assert_eq!(
        frame_types,
        [
            "mode",
            "voice_mode",
            "dictation_capabilities",
            "host_state",
            "dashboard_state"
        ]
    );
    finish_sender.send(()).expect("finish exact hello test");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn incompatible_browser_protocol_closes_the_realtime_session() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let mut runtime =
        start_live_realtime_browser_with_protocol(realtime_port, None, None, None, None, false)
            .await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("send incompatible protocol hello");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "protocol mismatch error").await,
        json!({"type":"protocol_mismatch","required_version":2})
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("protocol mismatch close timeout");
    assert!(
        matches!(closed, Some(Ok(Message::Close(_))) | None),
        "incompatible browser protocol remained connected"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), realtime_listener.accept())
            .await
            .is_err(),
        "incompatible browser protocol allocated a provider connection"
    );
}

#[tokio::test]
async fn incompatible_browser_protocol_closes_the_mock_session() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("send incompatible mock protocol hello");
    assert_eq!(
        next_browser_json(&mut browser, "mock protocol mismatch error").await,
        json!({"type":"protocol_mismatch","required_version":2})
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), browser.next())
        .await
        .expect("mock protocol mismatch close timeout");
    assert!(
        matches!(closed, Some(Ok(Message::Close(_))) | None),
        "incompatible mock browser protocol remained connected"
    );
}

#[tokio::test]
async fn mock_text_turn_acceptance_echoes_the_exact_turn_id() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({
                "type":"text_turn",
                "text":"help",
                "summary_id":null,
                "turn_id":1
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send correlated mock text turn");
    assert_eq!(
        next_browser_json_including_text_ack(&mut browser, "mock text turn acceptance").await,
        json!({"type":"text_turn_accepted","turn_id":1})
    );
}

#[tokio::test]
async fn realtime_text_turn_acceptance_echoes_the_exact_turn_id() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (received_sender, received_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "text turn session update").await["type"],
            "session.update"
        );
        loop {
            let event = next_fake_realtime_json(&mut socket, "correlated text turn").await;
            if event
                .pointer("/item/content/0/text")
                .and_then(Value::as_str)
                == Some("show status")
            {
                received_sender.send(()).expect("signal received text turn");
                break;
            }
        }
    });
    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({
                "type":"text_turn",
                "text":"show status",
                "summary_id":null,
                "turn_id":7
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send correlated Realtime text turn");
    assert_eq!(
        next_browser_json_including_text_ack(
            &mut runtime.browser,
            "Realtime text turn acceptance",
        )
        .await,
        json!({"type":"text_turn_accepted","turn_id":7})
    );
    received_receiver
        .await
        .expect("provider received text turn");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn structurally_valid_mock_text_turn_rejection_is_correlated() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({
                "type":"text_turn",
                "text":"stale reply",
                "summary_id":"stale-summary",
                "turn_id":1
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send stale mock text turn");
    assert_eq!(
        next_browser_json(&mut browser, "mock text turn rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":1,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
}

#[tokio::test]
async fn structurally_valid_realtime_text_turn_rejection_is_correlated() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (inspect_sender, inspect_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "rejected turn session update").await["type"],
            "session.update"
        );
        inspect_receiver.await.expect("inspect rejected text turn");
        tokio::time::timeout(Duration::from_millis(200), socket.next())
            .await
            .is_err()
    });
    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            json!({
                "type":"text_turn",
                "text":"stale reply",
                "summary_id":"stale-summary",
                "turn_id":1
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send stale Realtime text turn");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "Realtime text turn rejection").await,
        json!({
            "type":"text_turn_rejected",
            "turn_id":1,
            "message":"Typed turn was rejected. The draft was not changed."
        })
    );
    inspect_sender.send(()).expect("inspect rejected text turn");
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "rejected text turn reached the provider"
    );
}

#[tokio::test]
async fn mock_text_turn_ids_are_exact_monotonic_and_bounded() {
    let (_directory, _guard, mut browser) = start_mock_dictation_browser().await;
    browser
        .send(Message::Text(
            json!({"type":"text_turn","text":"help","summary_id":null,"turn_id":1})
                .to_string()
                .into(),
        ))
        .await
        .expect("send initial mock text turn");
    assert_eq!(
        next_browser_json_including_text_ack(&mut browser, "initial mock text acknowledgement")
            .await,
        json!({"type":"text_turn_accepted","turn_id":1})
    );
    loop {
        if next_browser_json(&mut browser, "initial mock text response").await
            == json!({"type":"state","state":"ready"})
        {
            break;
        }
    }

    for invalid in [
        json!({"type":"text_turn","text":"replay","summary_id":null,"turn_id":1}),
        json!({"type":"text_turn","text":"zero","summary_id":null,"turn_id":0}),
        json!({"type":"text_turn","text":"overflow","summary_id":null,"turn_id":2_147_483_648_u64}),
        json!({"type":"text_turn","text":"missing ID","summary_id":null}),
        json!({"type":"text_turn","text":"extra field","summary_id":null,"turn_id":2,"extra":true}),
    ] {
        browser
            .send(Message::Text(invalid.to_string().into()))
            .await
            .expect("send invalid mock text turn");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), browser.next())
                .await
                .is_err(),
            "invalid mock text turn produced a browser frame"
        );
    }

    browser
        .send(Message::Text(
            json!({"type":"text_turn","text":"status","summary_id":null,"turn_id":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send next monotonic mock text turn");
    assert_eq!(
        next_browser_json_including_text_ack(&mut browser, "next mock text acknowledgement").await,
        json!({"type":"text_turn_accepted","turn_id":2})
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn successful_browser_confirmation_retires_its_originating_response() {
    use std::os::unix::fs::PermissionsExt as _;

    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = paseo_directory.path().join("fake-paseo-confirmed-run");
    let process_marker = paseo_directory.path().join("process-calls");
    std::fs::write(
        &fake_paseo_path,
        format!(
            concat!(
                "#!/bin/sh\n",
                "printf '%s\\n' \"$*\" >> \"{}\"\n",
                "case \"$1\" in\n",
                "  ls) printf '%s\\n' '[{{\"id\":\"old-agent\",\"shortId\":\"old\",\"name\":\"Old Agent\",\"status\":\"idle\"}}]' ;;\n",
                "  provider) printf '%s\\n' '[{{\"id\":\"model\"}}]' ;;\n",
                "  run) printf '%s\\n' '{{\"agentId\":\"new-agent\"}}' ;;\n",
                "  logs) printf '%s\\n' 'Stale old-agent reply' ;;\n",
                "  *) printf '%s\\n' '[]' ;;\n",
                "esac\n"
            ),
            process_marker.display(),
        ),
    )
    .expect("write fake Paseo executable");
    let mut permissions = std::fs::metadata(&fake_paseo_path)
        .expect("fake Paseo metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_paseo_path, permissions).expect("fake Paseo permissions");

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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "confirmed run session update").await["type"],
            "session.update"
        );
        loop {
            if next_fake_realtime_json(&mut socket, "session intent response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"session-intent-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create session intent response");
        for event in function_call_events(
            "session-intent-response",
            "session-intent-item",
            "session-intent-call",
            "create_session",
            r#"{"prompt":"discarded intent prompt"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send session intent call");
        }
        let intent_output = next_function_output(
            &mut socket,
            "session-intent-call",
            "session intent tool output",
        )
        .await;
        assert_eq!(intent_output["error"], "session_task_required");
        complete_tool_response(
            &mut socket,
            "session-intent-response",
            "session-intent-followup",
        )
        .await;
        loop {
            if next_fake_realtime_json(&mut socket, "confirmed run response request").await["type"]
                == "response.create"
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"confirmed-run-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create confirmed run response");
        for event in function_call_events(
            "confirmed-run-response",
            "create-run-item",
            "create-run-call",
            "create_session",
            r#"{"prompt":"new task"}"#,
        ) {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("propose confirmed run");
        }
        loop {
            let event = next_fake_realtime_json(&mut socket, "create run tool output").await;
            if event.pointer("/item/call_id").and_then(Value::as_str) == Some("create-run-call") {
                break;
            }
        }

        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut saw_cancel = false;
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("confirmation event JSON");
            if event["type"] == "response.cancel" {
                assert_eq!(event["response_id"], "confirmed-run-response");
                saw_cancel = true;
                break;
            }
        }

        for event in function_call_events(
            "confirmed-run-response",
            "stale-select-item",
            "stale-select-call",
            "set_current_session",
            r#"{"session":"old-agent"}"#,
        )
        .into_iter()
        .chain(function_call_events(
            "confirmed-run-response",
            "stale-read-item",
            "stale-read-call",
            "read_latest_reply",
            r#"{"session":"old-agent"}"#,
        ))
        .chain([json!({
            "type":"response.done",
            "response":{"id":"confirmed-run-response","status":"completed"}
        })]) {
            if socket
                .send(Message::Text(event.to_string().into()))
                .await
                .is_err()
            {
                return saw_cancel;
            }
        }

        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        let mut stale_output_or_followup = false;
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(Some(Ok(Message::Text(message)))) =
                tokio::time::timeout(remaining, socket.next()).await
            else {
                break;
            };
            let event: Value = serde_json::from_str(&message).expect("stale provider output JSON");
            stale_output_or_followup |= event["type"] == "response.create"
                || event.pointer("/item/type").and_then(Value::as_str)
                    == Some("function_call_output");
        }
        saw_cancel && !stale_output_or_followup
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("create a new session", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("request new session task");
    loop {
        let frame = next_browser_json(&mut runtime.browser, "new session task request").await;
        assert!(
            frame["type"] != "proposal" || !frame["handle"].is_string(),
            "intent turn created a proposal"
        );
        if frame == json!({"type":"state","state":"ready"}) {
            break;
        }
    }
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("new task", null).to_string().into(),
        ))
        .await
        .expect("provide new session task");
    let proposal = loop {
        let frame = next_browser_json(&mut runtime.browser, "new session proposal").await;
        if frame["type"] == "proposal" && frame["handle"].is_string() {
            break frame;
        }
    };
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"confirm_proposal","handle":proposal["handle"]})
                .to_string()
                .into(),
        ))
        .await
        .expect("confirm new session proposal");
    let mut confirmed = false;
    let mut bound_context = Value::Bool(true);
    while !confirmed || !bound_context.is_null() {
        let frame = next_browser_json(&mut runtime.browser, "new session confirmation").await;
        if frame["type"] == "dashboard_state" {
            bound_context = frame["bound_context"].clone();
        }
        confirmed |=
            frame == json!({"type":"transcript_done","text":"Session created and selected."});
    }
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "confirmed context did not retire stale provider work"
    );
    let process_calls =
        std::fs::read_to_string(&process_marker).expect("confirmed run process calls");
    assert!(
        process_calls
            .lines()
            .any(|line| line.starts_with("run new task "))
    );
    assert!(!process_calls.contains("discarded intent prompt"));
    assert!(
        !process_calls.lines().any(|line| line.starts_with("logs ")),
        "stale provider read changed the newly selected session context"
    );
}

#[tokio::test]
#[cfg(unix)]
#[allow(clippy::similar_names, clippy::too_many_lines)]
async fn late_cancel_of_terminal_dictation_does_not_touch_the_newer_operation() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path = write_single_reply_paseo(
        &paseo_directory,
        "fake-paseo-terminal-dictation-cancel",
        "Reply A",
    );
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (b_delete_seen_sender, b_delete_seen_receiver) = tokio::sync::oneshot::channel();
    let (release_b_delete_sender, release_b_delete_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        bind_fake_realtime_context(&mut socket, "terminal-dictation-cancel").await;
        let mut b_delete_seen_sender = Some(b_delete_seen_sender);
        let mut release_b_delete_receiver = Some(release_b_delete_receiver);
        for (index, (item_id, transcript)) in [
            ("terminal-dictation-a", "Terminal dictation A"),
            ("terminal-dictation-b", "Terminal dictation B"),
        ]
        .into_iter()
        .enumerate()
        {
            loop {
                if next_fake_realtime_json(&mut socket, "terminal dictation commit").await["type"]
                    == "input_audio_buffer.commit"
                {
                    break;
                }
            }
            for event in [
                json!({
                    "type":"input_audio_buffer.committed",
                    "item_id":item_id,
                    "previous_item_id":null
                }),
                json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":item_id,
                    "transcript":transcript
                }),
            ] {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .expect("complete terminal dictation");
            }
            if index == 0 {
                acknowledge_fake_dictation_delete(
                    &mut socket,
                    item_id,
                    &format!("provider-{item_id}-deleted"),
                )
                .await;
            } else {
                let delete = loop {
                    let event = next_fake_realtime_json(&mut socket, "dictation B delete").await;
                    if event["type"] == "conversation.item.delete" {
                        break event;
                    }
                };
                assert_eq!(delete["item_id"], item_id);
                b_delete_seen_sender
                    .take()
                    .expect("dictation B deletion sender")
                    .send(())
                    .expect("signal dictation B deletion");
                release_b_delete_receiver
                    .take()
                    .expect("dictation B deletion receiver")
                    .await
                    .expect("release dictation B deletion");
                socket
                    .send(Message::Text(
                        json!({
                            "type":"conversation.item.deleted",
                            "item_id":item_id,
                            "event_id":"provider-terminal-dictation-b-deleted"
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("acknowledge dictation B deletion");
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let mut runtime =
        start_live_realtime_browser_with_paseo(realtime_port, Some(&fake_paseo_path)).await;
    let summary_id = bind_browser_context(&mut runtime.browser).await;
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select terminal dictation mode");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "terminal dictation mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start terminal dictation A");
    let operation_a =
        next_browser_json(&mut runtime.browser, "terminal dictation A operation").await;
    let operation_a_id = operation_a["operation_id"]
        .as_str()
        .expect("terminal dictation A operation ID")
        .to_owned();
    assert_eq!(
        next_browser_json(&mut runtime.browser, "terminal dictation A flush").await,
        json!({"type":"flush_audio"})
    );
    runtime
        .browser
        .send(Message::Binary(
            test_pcm(b"terminal dictation A audio").into(),
        ))
        .await
        .expect("send terminal dictation A audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_a_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish terminal dictation A");
    let result_a = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_result",
        "terminal dictation A result",
    )
    .await;
    assert_eq!(result_a["operation_id"], operation_a_id);
    assert_eq!(
        next_browser_json(&mut runtime.browser, "terminal dictation A ready").await,
        json!({"type":"state","state":"ready"})
    );

    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":2,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start terminal dictation B");
    let operation_b =
        next_browser_json(&mut runtime.browser, "terminal dictation B operation").await;
    let operation_b_id = operation_b["operation_id"]
        .as_str()
        .expect("terminal dictation B operation ID")
        .to_owned();
    assert_ne!(operation_a_id, operation_b_id);
    assert_eq!(
        next_browser_json(&mut runtime.browser, "terminal dictation B flush").await,
        json!({"type":"flush_audio"})
    );

    runtime
        .browser
        .send(Message::Binary(
            test_pcm(b"terminal dictation B audio").into(),
        ))
        .await
        .expect("send terminal dictation B audio");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_b_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish terminal dictation B");
    tokio::time::timeout(Duration::from_millis(500), b_delete_seen_receiver)
        .await
        .expect("dictation B deletion timeout")
        .expect("dictation B deletion signal");
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"cancel_dictation","operation_id":operation_a_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("send late terminal dictation A cancellation");
    let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Some(Ok(Message::Text(message)))) =
            tokio::time::timeout(remaining, runtime.browser.next()).await
        else {
            break;
        };
        let frame: Value = serde_json::from_str(&message).expect("late A cancel browser JSON");
        assert_ne!(frame["type"], "dictation_cancelled");
        assert_ne!(frame, json!({"type":"state","state":"ready"}));
        assert_ne!(frame["type"], "dictation_result");
    }
    release_b_delete_sender
        .send(())
        .expect("release dictation B deletion acknowledgement");
    let result_b = next_browser_json_of_type(
        &mut runtime.browser,
        "dictation_result",
        "terminal dictation B result",
    )
    .await;
    assert_eq!(result_b["operation_id"], operation_b_id);
    assert_eq!(result_b["text"], "Terminal dictation B");
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn accepted_response_error_closes_before_late_output_or_tools() {
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
        assert_eq!(
            next_fake_realtime_json(&mut socket, "accepted error session update").await["type"],
            "session.update"
        );
        let response_create = loop {
            let event = next_fake_realtime_json(&mut socket, "accepted response request").await;
            if event["type"] == "response.create" {
                break event;
            }
        };
        let response_event_id = response_create["event_id"]
            .as_str()
            .expect("accepted response event ID")
            .to_owned();
        socket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"accepted-error-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("create accepted error response");
        socket
            .send(Message::Text(
                json!({
                    "type":"error",
                    "error":{
                        "type":"invalid_request_error",
                        "message":"accepted response became ambiguous",
                        "event_id":response_event_id
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send accepted response error");
        for event in function_call_events(
            "accepted-error-response",
            "late-error-item",
            "late-error-call",
            "cancel_action",
            "{}",
        )
        .into_iter()
        .chain([json!({
            "type":"response.output_audio_transcript.done",
            "response_id":"accepted-error-response",
            "transcript":"must not reach browser"
        })]) {
            if socket
                .send(Message::Text(event.to_string().into()))
                .await
                .is_err()
            {
                return true;
            }
        }
        tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .is_ok()
    });

    let mut runtime = start_live_realtime_browser(realtime_port).await;
    runtime
        .browser
        .send(Message::Text(
            test_text_turn!("start accepted response", null)
                .to_string()
                .into(),
        ))
        .await
        .expect("start accepted response");
    let error = loop {
        let frame = next_browser_json(&mut runtime.browser, "accepted response ambiguity").await;
        assert_ne!(frame["type"], "tool");
        assert_ne!(frame["type"], "transcript_done");
        if frame["type"] == "error" {
            break frame;
        }
    };
    assert_eq!(
        error,
        json!({
            "type":"error",
            "message":"Realtime reported conflicting response state. Reconnecting safely."
        })
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), runtime.browser.next())
        .await
        .expect("accepted response ambiguity close timeout");
    assert!(matches!(closed, Some(Ok(Message::Close(_))) | None));
    assert!(
        fake_realtime.await.expect("fake Realtime task"),
        "provider remained connected after accepted response ambiguity"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(unix)]
#[allow(clippy::too_many_lines)]
async fn panicking_dictation_cleanup_emits_correlated_failure_and_ready() {
    let paseo_directory = tempfile::tempdir().expect("fake Paseo directory");
    let fake_paseo_path =
        write_single_reply_paseo(&paseo_directory, "fake-paseo-panicking-cleanup", "Reply A");
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
        bind_fake_realtime_context(&mut socket, "panicking-cleanup").await;
        loop {
            if next_fake_realtime_json(&mut socket, "panicking cleanup commit").await["type"]
                == "input_audio_buffer.commit"
            {
                break;
            }
        }
        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "item_id":"panicking-cleanup-item",
                "previous_item_id":null
            }),
            json!({
                "type":"conversation.item.input_audio_transcription.completed",
                "item_id":"panicking-cleanup-item",
                "transcript":"cleanup panic input"
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("advance panicking cleanup");
        }
        acknowledge_fake_dictation_delete(
            &mut socket,
            "panicking-cleanup-item",
            "provider-panicking-cleanup-deleted",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    });

    let app_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("injected runtime listener");
    let app_port = app_listener
        .local_addr()
        .expect("injected runtime address")
        .port();
    let runtime_directory = tempfile::tempdir().expect("injected runtime directory");
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config = Config {
        listen_host: "127.0.0.1".to_owned(),
        listen_port: app_port,
        openai_base_url: format!("ws://127.0.0.1:{realtime_port}"),
        paseo_bin: fake_paseo_path.to_string_lossy().into_owned(),
        public_dir: workspace.join("public"),
        journal_path: runtime_directory.path().join("state/journal.sqlite3"),
        ..Config::default()
    };
    let server = tokio::spawn(async move {
        serve(
            config,
            Secrets {
                openai_api_key: Some("test-key".to_owned()),
                paseo_password: Some("test-password".to_owned()),
            },
            RuntimeDependencies {
                clock: std::sync::Arc::new(SystemClock::start()),
                http_client: reqwest::Client::new(),
                realtime_connector: std::sync::Arc::new(SystemRealtimeConnector),
                dictation_cleaner: std::sync::Arc::new(PanickingDictationCleaner),
                process_executor: std::sync::Arc::new(SystemProcessExecutor),
            },
            app_listener,
        )
        .await
        .expect("serve injected runtime");
    });
    let _server_guard = ServerGuard(server);
    let health_url = format!("http://127.0.0.1:{app_port}/healthz");
    let mut healthy = false;
    let client = health_client();
    for _ in 0..150 {
        if client.get(&health_url).send().await.is_ok() {
            healthy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(healthy, "injected runtime became healthy");
    let (mut browser, _) = connect_async(format!("ws://127.0.0.1:{app_port}/ws"))
        .await
        .expect("injected runtime browser websocket");
    initialize_browser_protocol(&mut browser, "injected runtime initial frame").await;
    let summary_id = bind_browser_context(&mut browser).await;
    browser
        .send(Message::Text(
            json!({"type":"set_voice_mode","mode":"dictation"})
                .to_string()
                .into(),
        ))
        .await
        .expect("select panicking cleanup dictation");
    assert_eq!(
        next_browser_json(&mut browser, "panicking cleanup mode").await,
        json!({"type":"voice_mode","mode":"dictation"})
    );
    browser
        .send(Message::Text(
            json!({"type":"ptt_start","recording_id":1,"summary_id":summary_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("start panicking cleanup dictation");
    let operation = next_browser_json(&mut browser, "panicking cleanup operation").await;
    let operation_id = operation["operation_id"]
        .as_str()
        .expect("panicking cleanup operation ID")
        .to_owned();
    assert_eq!(
        next_browser_json(&mut browser, "panicking cleanup flush").await,
        json!({"type":"flush_audio"})
    );
    browser
        .send(Message::Binary(test_pcm(b"panicking cleanup audio").into()))
        .await
        .expect("send panicking cleanup audio");
    browser
        .send(Message::Text(
            json!({"type":"ptt_end","operation_id":operation_id})
                .to_string()
                .into(),
        ))
        .await
        .expect("finish panicking cleanup dictation");
    let failed = next_browser_json_of_type(
        &mut browser,
        "dictation_failed",
        "panicking cleanup failure",
    )
    .await;
    assert_eq!(
        failed,
        json!({
            "type":"dictation_failed",
            "operation_id":operation_id,
            "message":"Speech cleanup failed. The draft was not changed."
        })
    );
    assert_eq!(
        next_browser_json(&mut browser, "panicking cleanup ready").await,
        json!({"type":"state","state":"ready"})
    );
    fake_realtime.await.expect("fake Realtime task");
}

#[tokio::test]
async fn generic_provider_error_cannot_impersonate_protocol_mismatch() {
    let realtime_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Realtime listener");
    let realtime_port = realtime_listener
        .local_addr()
        .expect("Realtime address")
        .port();
    let (send_sender, send_receiver) = tokio::sync::oneshot::channel();
    let (finish_sender, finish_receiver) = tokio::sync::oneshot::channel();
    let fake_realtime = tokio::spawn(async move {
        let (stream, _) = realtime_listener.accept().await.expect("Realtime accept");
        let mut socket = accept_async(stream).await.expect("Realtime handshake");
        assert_eq!(
            next_fake_realtime_json(&mut socket, "generic provider session update").await["type"],
            "session.update"
        );
        send_receiver.await.expect("send generic provider error");
        socket
            .send(Message::Text(
                json!({
                    "type":"error",
                    "error":{
                        "type":"server_error",
                        "message":"Protocol version mismatch. Reload required."
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send generic provider error");
        finish_receiver.await.expect("finish generic provider test");
    });
    let mut runtime = start_live_realtime_browser(realtime_port).await;
    send_sender
        .send(())
        .expect("trigger generic provider error");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "generic provider error").await,
        json!({"type":"error","message":"Realtime provider error."})
    );
    runtime
        .browser
        .send(Message::Text(
            json!({"type":"hello","protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("renegotiate after generic provider error");
    assert_eq!(
        next_browser_json(&mut runtime.browser, "post-error protocol acknowledgement").await,
        json!({"type":"protocol_ready","version":2})
    );
    finish_sender
        .send(())
        .expect("finish generic provider test");
    fake_realtime.await.expect("fake Realtime task");
}
