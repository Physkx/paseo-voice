use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::Duration,
};

use paseo_control_plane::{
    config::{self, ApiCredentialConfig},
    dictation::capability_frame,
    paseo::{ProcessExecutor, ProcessOutput},
    secrets::{SecretConfig, SecretProvider, load_secrets, parse_bws_token},
};
use serde_json::{Value, json};

type Call = (String, Vec<String>, HashMap<String, String>, Duration);

struct FakeExecutor {
    outputs: Mutex<VecDeque<ProcessOutput>>,
    calls: Mutex<Vec<Call>>,
}

impl FakeExecutor {
    fn with_outputs(outputs: impl IntoIterator<Item = ProcessOutput>) -> Self {
        Self {
            outputs: Mutex::new(outputs.into_iter().collect()),
            calls: Mutex::default(),
        }
    }
}

impl ProcessExecutor for FakeExecutor {
    fn execute(
        &self,
        program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        timeout: Duration,
    ) -> ProcessOutput {
        self.calls.lock().expect("calls lock").push((
            program.to_owned(),
            args.to_vec(),
            env.clone(),
            timeout,
        ));
        self.outputs
            .lock()
            .expect("outputs lock")
            .pop_front()
            .expect("queued output")
    }
}

fn success(stdout: &str) -> ProcessOutput {
    ProcessOutput {
        exit_code: Some(0),
        stdout: stdout.to_owned(),
        stderr: String::new(),
        timed_out: false,
        spawn_failed: false,
    }
}

fn environment_config(credentials: Vec<ApiCredentialConfig>) -> SecretConfig {
    SecretConfig {
        provider: SecretProvider::Environment,
        bws_binary: "bws".to_owned(),
        bws_env_file: "unused".into(),
        api_credentials: credentials,
        bws_paseo_id: None,
        onepassword_binary: "op".to_owned(),
        onepassword_paseo_ref: None,
    }
}

fn credential(id: &str, variable: &str) -> ApiCredentialConfig {
    ApiCredentialConfig {
        id: id.to_owned(),
        bws_secret_id: Some(format!("{id}-bws")),
        one_password_secret_ref: Some(format!("op://vault/{id}/credential")),
        environment_variable: Some(variable.to_owned()),
    }
}

fn load_json(value: &Value) -> Result<config::Config, String> {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("config.json");
    std::fs::write(&path, value.to_string()).expect("write config");
    config::load(&HashMap::from([(
        "PASEO_VOICE_CONFIG".to_owned(),
        path.to_string_lossy().into_owned(),
    )]))
}

fn voice(id: &str, provider: &str, url: &str, default: bool, credential: Option<&str>) -> Value {
    json!({
        "id":id,
        "label":format!("{id} voice"),
        "providerType":provider,
        "baseUrl":url,
        "model":"voice-model",
        "voice":"eve",
        "transcriptionModel":if provider == "xai" { "grok-transcribe" } else { "transcribe" },
        "credentialRef":credential,
        "default":default
    })
}

fn cleanup(id: &str, url: &str, default: bool, allow_http: bool) -> Value {
    json!({
        "id":id,
        "label":format!("{id} cleanup"),
        "baseUrl":url,
        "model":"cleanup-model",
        "credentialRef":null,
        "default":default,
        "allowInsecurePrivateHttp":allow_http
    })
}

#[test]
fn legacy_json_and_environment_fields_are_rejected() {
    for field in [
        "openaiModel",
        "openaiVoice",
        "openaiBaseUrl",
        "sparkBaseUrl",
        "sparkModel",
        "allowInsecureTailscaleSpark",
        "bwsSecretIdOpenai",
        "bwsSecretIdSpark",
        "onePasswordSecretRefOpenai",
        "onePasswordSecretRefSpark",
    ] {
        assert_eq!(
            load_json(&json!({field:"legacy"})).expect_err("legacy JSON rejected"),
            "invalid configuration JSON"
        );
    }
    for name in [
        "PASEO_VOICE_OPENAI_MODEL",
        "PASEO_VOICE_OPENAI_BASE_URL",
        "PASEO_VOICE_SPARK_BASE_URL",
        "PASEO_VOICE_SPARK_API_KEY",
        "PASEO_VOICE_BWS_SECRET_ID_OPENAI",
    ] {
        assert_eq!(
            config::load(&HashMap::from([(name.to_owned(), "legacy".to_owned())]))
                .expect_err("legacy environment field rejected"),
            "legacy voice or model environment fields are not supported"
        );
    }
}

#[test]
fn profile_collections_require_unique_ids_and_exactly_one_default() {
    assert_eq!(
        load_json(&json!({"voiceProfiles":[]})).expect_err("empty voices rejected"),
        "at least one voice profile is required"
    );
    assert_eq!(
        load_json(&json!({"cleanupProfiles":[]})).expect_err("empty cleanup rejected"),
        "at least one cleanup profile is required"
    );
    let duplicate_voice = voice(
        "same",
        "openai-compatible",
        "ws://127.0.0.1:9000/realtime",
        true,
        None,
    );
    assert_eq!(
        load_json(&json!({"voiceProfiles":[duplicate_voice.clone(), duplicate_voice]}))
            .expect_err("duplicate voice rejected"),
        "voice profile IDs must be unique"
    );
    assert_eq!(
        load_json(&json!({
            "cleanupProfiles":[
                cleanup("one", "http://127.0.0.1:1234/v1", true, false),
                cleanup("two", "http://127.0.0.1:1235/v1", true, false)
            ]
        }))
        .expect_err("multiple cleanup defaults rejected"),
        "exactly one cleanup profile must be the default"
    );
}

#[test]
fn profile_and_credential_collections_are_bounded() {
    let voices = (0..65)
        .map(|index| {
            voice(
                &format!("voice-{index}"),
                "openai-compatible",
                &format!("ws://127.0.0.1:{}/realtime", 9000 + index),
                index == 0,
                None,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        load_json(&json!({"voiceProfiles":voices})).expect_err("oversized profiles rejected"),
        "voice, cleanup, and credential collections are limited to 64 entries"
    );
}

#[test]
fn credential_ids_are_unique_and_all_references_resolve() {
    assert_eq!(
        load_json(&json!({
            "voiceProfiles":[voice("local", "openai-compatible", "ws://127.0.0.1:9000/realtime", true, Some("missing"))]
        }))
        .expect_err("missing reference rejected"),
        "profile credentialRef does not resolve"
    );
    assert_eq!(
        load_json(&json!({
            "apiCredentials":[
                {"id":"same","environmentVariable":"ONE"},
                {"id":"same","environmentVariable":"TWO"}
            ]
        }))
        .expect_err("duplicate credential rejected"),
        "API credential IDs must be unique and valid"
    );
}

#[test]
fn official_provider_endpoints_are_exactly_pinned() {
    for (provider, endpoint) in [
        ("openai", "wss://realtime.example/v1"),
        ("openai", "wss://api.openai.com/v1/realtime/"),
        ("xai", "wss://realtime.example/v1"),
        ("xai", "wss://api.x.ai/v1/realtime/"),
    ] {
        let error = load_json(&json!({
            "voiceProfiles":[voice("official", provider, endpoint, true, Some("openai"))]
        }))
        .expect_err("unpinned endpoint rejected");
        assert!(error.contains("exact official Realtime endpoint"));
    }
}

#[test]
fn endpoint_parsing_rejects_credentials_queries_fragments_and_unsafe_plaintext() {
    for endpoint in [
        "wss://user:secret@realtime.example/v1",
        "wss://realtime.example/v1?secret=value",
        "wss://realtime.example/v1#fragment",
        "ws://realtime.example/v1",
        "http://models.example/v1",
    ] {
        let value = if endpoint.starts_with("ws") {
            json!({"voiceProfiles":[voice("custom", "openai-compatible", endpoint, true, None)]})
        } else {
            json!({"cleanupProfiles":[cleanup("remote", endpoint, true, false)]})
        };
        assert!(
            load_json(&value).is_err(),
            "unsafe endpoint accepted: {endpoint}"
        );
    }
}

#[test]
fn processing_location_is_derived_from_exact_hosts() {
    let config = load_json(&json!({
        "voiceProfiles":[voice("local", "openai-compatible", "ws://127.0.0.1:9000/realtime", true, None)],
        "cleanupProfiles":[cleanup("tailnet", "http://100.64.0.10:1234/v1", true, true)],
        "summarisation":{"baseUrl":"https://models.example/v1","model":"summary","credentialRef":null},
        "approvedPrivateEndpointHosts":["private-model.example"]
    }))
    .expect("valid location config");
    let frame = capability_frame(&config, "local", "tailnet", true);
    assert_eq!(frame["speech_to_text"]["processing_location"], "local");
    assert_eq!(frame["cleanup"]["processing_location"], "private_remote");

    let explicit = load_json(&json!({
        "cleanupProfiles":[cleanup("private", "http://private-model.example:1234/v1", true, true)],
        "approvedPrivateEndpointHosts":["private-model.example"]
    }))
    .expect("explicit private host");
    let frame = capability_frame(&explicit, "openai", "private", true);
    assert_eq!(frame["cleanup"]["processing_location"], "private_remote");
}

#[test]
fn environment_credentials_are_named_and_preserve_exact_values() {
    let executor = FakeExecutor::with_outputs([]);
    let config = environment_config(vec![
        credential("openai", "VOICE_OPENAI_KEY"),
        credential("xai", "VOICE_XAI_KEY"),
    ]);
    let secrets = load_secrets(
        &config,
        &executor,
        &HashMap::from([
            ("VOICE_OPENAI_KEY".to_owned(), "  exact-key\n".to_owned()),
            ("VOICE_XAI_KEY".to_owned(), "xai-key".to_owned()),
            (
                "OPENAI_API_KEY".to_owned(),
                "ambient-must-not-load".to_owned(),
            ),
        ]),
    );
    assert_eq!(
        secrets.api_credentials.get("openai").map(String::as_str),
        Some("  exact-key\n")
    );
    assert_eq!(
        secrets.api_credentials.get("xai").map(String::as_str),
        Some("xai-key")
    );
    assert_eq!(secrets.api_credentials.len(), 2);
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn grok_oauth_is_loaded_separately_from_named_api_credentials() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let auth_path = directory.path().join("auth.json");
    std::fs::write(
        &auth_path,
        r#"{"https://auth.x.ai::client":{"key":"oidc-access-token","auth_mode":"oidc"}}"#,
    )
    .expect("write Grok auth store");
    let secrets = load_secrets(
        &environment_config(Vec::new()),
        &FakeExecutor::with_outputs([]),
        &HashMap::from([(
            "GROK_AUTH_FILE".to_owned(),
            auth_path.to_string_lossy().into_owned(),
        )]),
    );
    assert_eq!(
        secrets.grok_oauth_token.as_deref(),
        Some("oidc-access-token")
    );
    assert!(secrets.api_credentials.is_empty());
}

#[test]
fn bitwarden_resolves_each_named_id_without_forwarding_unrelated_credentials() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let token_file = directory.path().join("bws.env");
    std::fs::write(&token_file, "BWS_ACCESS_TOKEN=test-access-token\n").expect("write token");
    let executor = FakeExecutor::with_outputs([
        success(r#"{"value":"voice-secret"}"#),
        success(r#"{"value":"paseo-secret"}"#),
    ]);
    let config = SecretConfig {
        provider: SecretProvider::Bitwarden,
        bws_binary: "bws".to_owned(),
        bws_env_file: token_file,
        api_credentials: vec![credential("voice", "VOICE_KEY")],
        bws_paseo_id: Some("paseo-bws".to_owned()),
        onepassword_binary: "op".to_owned(),
        onepassword_paseo_ref: None,
    };
    let secrets = load_secrets(
        &config,
        &executor,
        &HashMap::from([
            ("PATH".to_owned(), "/usr/bin".to_owned()),
            ("VOICE_KEY".to_owned(), "must-not-forward".to_owned()),
            ("UNRELATED_SECRET".to_owned(), "must-not-forward".to_owned()),
        ]),
    );
    assert_eq!(
        secrets.api_credentials.get("voice").map(String::as_str),
        Some("voice-secret")
    );
    assert_eq!(secrets.paseo_password.as_deref(), Some("paseo-secret"));
    let calls = executor.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].1, ["secret", "get", "voice-bws"]);
    assert!(!calls[0].2.contains_key("VOICE_KEY"));
    assert!(!calls[0].2.contains_key("UNRELATED_SECRET"));
    assert!(calls[0].2.contains_key("BWS_ACCESS_TOKEN"));
}

#[test]
fn onepassword_resolves_named_refs_and_forwards_only_platform_and_op_environment() {
    let executor =
        FakeExecutor::with_outputs([success(r#""voice-secret""#), success(r#""paseo-secret""#)]);
    let config = SecretConfig {
        provider: SecretProvider::OnePassword,
        bws_binary: "bws".to_owned(),
        bws_env_file: "unused".into(),
        api_credentials: vec![credential("voice", "VOICE_KEY")],
        bws_paseo_id: None,
        onepassword_binary: "op".to_owned(),
        onepassword_paseo_ref: Some("op://vault/paseo/password".to_owned()),
    };
    let secrets = load_secrets(
        &config,
        &executor,
        &HashMap::from([
            ("PATH".to_owned(), "/bin".to_owned()),
            (
                "OP_SERVICE_ACCOUNT_TOKEN".to_owned(),
                "op-session".to_owned(),
            ),
            ("VOICE_KEY".to_owned(), "must-not-forward".to_owned()),
        ]),
    );
    assert_eq!(
        secrets.api_credentials.get("voice").map(String::as_str),
        Some("voice-secret")
    );
    assert_eq!(secrets.paseo_password.as_deref(), Some("paseo-secret"));
    let calls = executor.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].1,
        ["read", "--format", "json", "op://vault/voice/credential"]
    );
    assert!(calls[0].2.contains_key("OP_SERVICE_ACCOUNT_TOKEN"));
    assert!(!calls[0].2.contains_key("VOICE_KEY"));
}

#[test]
fn bitwarden_token_parser_never_sources_shell_text() {
    assert_eq!(
        parse_bws_token("# note\nexport BWS_ACCESS_TOKEN='token-value' # local"),
        Some("token-value".to_owned())
    );
    assert_eq!(parse_bws_token("OTHER=value"), None);
}
