use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::Duration,
};

use paseo_control_plane::{
    config,
    paseo::{ProcessExecutor, ProcessOutput},
    secrets::{SecretConfig, SecretProvider, load_secrets, parse_bws_token},
};

type Call = (String, Vec<String>, HashMap<String, String>, Duration);

struct FakeExecutor {
    outputs: Mutex<VecDeque<ProcessOutput>>,
    calls: Mutex<Vec<Call>>,
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

#[test]
fn config_file_is_strict_and_environment_has_precedence() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("config.json");
    std::fs::write(
        &path,
        r#"{"listenPort":8000,"secretProvider":"onepassword","proposalTtlMs":500}"#,
    )
    .expect("write config");
    let environment = HashMap::from([
        (
            "PASEO_VOICE_CONFIG".to_owned(),
            path.to_string_lossy().into_owned(),
        ),
        ("PASEO_VOICE_LISTEN_PORT".to_owned(), "9000".to_owned()),
        (
            "PASEO_VOICE_SECRET_PROVIDER".to_owned(),
            "environment".to_owned(),
        ),
        (
            "PASEO_VOICE_AUTO_REPLY_POLL_MS".to_owned(),
            "1000".to_owned(),
        ),
    ]);
    let loaded = config::load(&environment).expect("load configuration");
    assert_eq!(loaded.listen_port, 9000);
    assert_eq!(loaded.secret_provider, "environment");
    assert_eq!(loaded.auto_reply_poll_ms, 1000);
    assert_eq!(loaded.proposal_ttl_ms, 500);

    std::fs::write(&path, r#"{"unknownDangerousSwitch":true}"#).expect("write invalid config");
    assert_eq!(
        config::load(&environment).expect_err("unknown field rejected"),
        "invalid configuration JSON"
    );

    std::fs::write(&path, r#"{"proposalTtlMs":0}"#).expect("write invalid config");
    assert_eq!(
        config::load(&HashMap::from([(
            "PASEO_VOICE_CONFIG".to_owned(),
            path.to_string_lossy().into_owned(),
        )]))
        .expect_err("zero TTL rejected"),
        "proposal TTL must be positive"
    );
    assert_eq!(
        config::load(&HashMap::from([(
            "PASEO_VOICE_MOCK".to_owned(),
            "sometimes".to_owned(),
        )]))
        .expect_err("ambiguous boolean rejected"),
        "PASEO_VOICE_MOCK is invalid"
    );

    std::fs::write(&path, r#"{"autoReplyPollMs":249}"#).expect("write invalid config");
    assert_eq!(
        config::load(&HashMap::from([(
            "PASEO_VOICE_CONFIG".to_owned(),
            path.to_string_lossy().into_owned(),
        )]))
        .expect_err("unsafe polling interval rejected"),
        "auto reply poll interval must be zero or between 250 and 60000 ms"
    );
}

#[test]
fn plaintext_remote_realtime_endpoint_is_rejected_at_config_load() {
    let environment = HashMap::from([(
        "PASEO_VOICE_OPENAI_BASE_URL".to_owned(),
        "ws://realtime.example/v1/realtime".to_owned(),
    )]);

    assert_eq!(
        config::load(&environment).expect_err("plaintext remote Realtime rejected"),
        "remote OpenAI Realtime base URL must use wss"
    );
}

#[test]
#[cfg(unix)]
fn plaintext_remote_endpoint_fails_before_secret_resolution_or_runtime_start() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().expect("temporary directory");
    let marker = directory.path().join("secret-provider-called");
    let fake_bws = directory.path().join("fake-bws");
    std::fs::write(
        &fake_bws,
        format!("#!/bin/sh\nprintf x > \"{}\"\n", marker.display()),
    )
    .expect("write fake secret provider");
    let mut permissions = std::fs::metadata(&fake_bws)
        .expect("fake secret provider metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&fake_bws, permissions).expect("fake secret provider permissions");
    let token_file = directory.path().join("bws.env");
    std::fs::write(&token_file, "BWS_ACCESS_TOKEN=placeholder\n")
        .expect("write fake secret provider environment");
    let config_path = directory.path().join("config.json");
    for (field, value, expected_error) in [
        (
            "openaiBaseUrl",
            "ws://realtime.example/v1/realtime",
            "configuration failed: remote OpenAI Realtime base URL must use wss\n",
        ),
        (
            "sparkBaseUrl",
            "http://models.example/v1",
            "configuration failed: remote summariser base URL must use https\n",
        ),
    ] {
        let mut config = serde_json::json!({
            "secretProvider":"bitwarden",
            "bwsBin":&fake_bws,
            "bwsEnvFile":&token_file,
            "bwsSecretIdOpenai":"placeholder-id"
        });
        config[field] = serde_json::Value::String(value.to_owned());
        std::fs::write(&config_path, config.to_string()).expect("write invalid endpoint config");

        let output = std::process::Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
            .arg("serve")
            .env_clear()
            .env("HOME", directory.path())
            .env("PASEO_VOICE_CONFIG", &config_path)
            .output()
            .expect("run control plane with invalid endpoint");

        assert_eq!(output.status.code(), Some(1));
        assert_eq!(
            String::from_utf8(output.stderr).expect("configuration error UTF-8"),
            expected_error
        );
        assert!(!marker.exists(), "secret resolution ran before validation");
    }
}

#[test]
fn plaintext_remote_summariser_endpoint_is_rejected_at_config_load() {
    let environment = HashMap::from([(
        "PASEO_VOICE_SPARK_BASE_URL".to_owned(),
        "http://models.example/v1".to_owned(),
    )]);

    assert_eq!(
        config::load(&environment).expect_err("plaintext remote summariser rejected"),
        "remote summariser base URL must use https"
    );
}

#[test]
fn plaintext_tailscale_summariser_requires_explicit_opt_in() {
    let endpoint = format!("http://{}.{}.{}.{}:1234/v1", 100, 100, 20, 30);
    let rejected = HashMap::from([("PASEO_VOICE_SPARK_BASE_URL".to_owned(), endpoint.clone())]);
    assert_eq!(
        config::load(&rejected).expect_err("plaintext Tailscale endpoint rejected by default"),
        "remote summariser base URL must use https"
    );

    let accepted = HashMap::from([
        ("PASEO_VOICE_SPARK_BASE_URL".to_owned(), endpoint.clone()),
        (
            "PASEO_VOICE_ALLOW_INSECURE_TAILSCALE_SPARK".to_owned(),
            "true".to_owned(),
        ),
    ]);
    let loaded = config::load(&accepted).expect("explicit Tailscale endpoint opt-in");
    assert_eq!(loaded.spark_base_url, endpoint);
    assert!(loaded.allow_insecure_tailscale_spark);

    let non_tailscale = HashMap::from([
        (
            "PASEO_VOICE_SPARK_BASE_URL".to_owned(),
            "http://models.example:1234/v1".to_owned(),
        ),
        (
            "PASEO_VOICE_ALLOW_INSECURE_TAILSCALE_SPARK".to_owned(),
            "true".to_owned(),
        ),
    ]);
    assert_eq!(
        config::load(&non_tailscale).expect_err("opt-in is limited to Tailscale IPv4"),
        "remote summariser base URL must use https"
    );
}

#[test]
fn embedded_endpoint_credentials_are_rejected_at_config_load() {
    for (name, value, error) in [
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "wss://user:password@realtime.example/v1/realtime",
            "OpenAI Realtime base URL must not contain credentials",
        ),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "wss://user@realtime.example/v1/realtime",
            "OpenAI Realtime base URL must not contain credentials",
        ),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "wss://@realtime.example/v1/realtime",
            "OpenAI Realtime base URL must not contain credentials",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "https://user:password@models.example/v1",
            "summariser base URL must not contain credentials",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "http://user@localhost:1234/v1",
            "summariser base URL must not contain credentials",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "https://@models.example/v1",
            "summariser base URL must not contain credentials",
        ),
    ] {
        let environment = HashMap::from([(name.to_owned(), value.to_owned())]);
        assert_eq!(
            config::load(&environment).expect_err("embedded endpoint credentials rejected"),
            error
        );
    }
}

#[test]
fn endpoint_queries_and_fragments_are_rejected_at_config_load() {
    for (name, value, error) in [
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "wss://realtime.example/v1/realtime?existing=value",
            "OpenAI Realtime base URL must not contain a query or fragment",
        ),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "wss://realtime.example/v1/realtime#fragment",
            "OpenAI Realtime base URL must not contain a query or fragment",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "https://models.example/v1?existing=value",
            "summariser base URL must not contain a query or fragment",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "https://models.example/v1#fragment",
            "summariser base URL must not contain a query or fragment",
        ),
    ] {
        let environment = HashMap::from([(name.to_owned(), value.to_owned())]);
        assert_eq!(
            config::load(&environment).expect_err("URL suffix rejected"),
            error
        );
    }
}

#[test]
fn malformed_hostless_and_wrong_scheme_endpoints_are_rejected() {
    for (name, value, error) in [
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "not a URL",
            "invalid OpenAI Realtime base URL",
        ),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            " wss://api.openai.com/v1/realtime",
            "invalid OpenAI Realtime base URL",
        ),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "wss:///v1/realtime",
            "invalid OpenAI Realtime base URL",
        ),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "https://realtime.example/v1/realtime",
            "invalid OpenAI Realtime base URL",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "not a URL",
            "invalid summariser base URL",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "https://models.example/v1\n",
            "invalid summariser base URL",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "https:///v1",
            "invalid summariser base URL",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "wss://models.example/v1",
            "invalid summariser base URL",
        ),
    ] {
        let environment = HashMap::from([(name.to_owned(), value.to_owned())]);
        assert_eq!(
            config::load(&environment).expect_err("invalid endpoint rejected"),
            error,
            "{name}={value}"
        );
    }
}

#[test]
fn exact_loopback_and_secure_remote_endpoint_variants_are_accepted() {
    for (name, value) in [
        ("PASEO_VOICE_OPENAI_BASE_URL", "ws://localhost:8080"),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "ws://127.255.255.254:8080/realtime/",
        ),
        ("PASEO_VOICE_OPENAI_BASE_URL", "ws://[::1]:8080/realtime"),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "wss://api.openai.com/v1/realtime/",
        ),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "wss://realtime.example/v1/realtime",
        ),
        ("PASEO_VOICE_SPARK_BASE_URL", "http://localhost:1234/v1"),
        ("PASEO_VOICE_SPARK_BASE_URL", "http://127.0.0.2:1234/v1/"),
        ("PASEO_VOICE_SPARK_BASE_URL", "http://[::1]:1234/v1"),
        ("PASEO_VOICE_SPARK_BASE_URL", "https://models.example/v1"),
    ] {
        let environment = HashMap::from([(name.to_owned(), value.to_owned())]);
        config::load(&environment).unwrap_or_else(|error| panic!("{name}={value}: {error}"));
    }
}

#[test]
fn plaintext_near_loopback_hosts_are_rejected() {
    for (name, value, error) in [
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "ws://localhost.example/v1/realtime",
            "remote OpenAI Realtime base URL must use wss",
        ),
        (
            "PASEO_VOICE_OPENAI_BASE_URL",
            "ws://[::ffff:127.0.0.1]/v1/realtime",
            "remote OpenAI Realtime base URL must use wss",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "http://localhost.example/v1",
            "remote summariser base URL must use https",
        ),
        (
            "PASEO_VOICE_SPARK_BASE_URL",
            "http://[::ffff:127.0.0.1]/v1",
            "remote summariser base URL must use https",
        ),
    ] {
        let environment = HashMap::from([(name.to_owned(), value.to_owned())]);
        assert_eq!(
            config::load(&environment).expect_err("near-loopback endpoint rejected"),
            error,
            "{name}={value}"
        );
    }
}

#[test]
fn paseo_host_profiles_are_strict_and_have_one_unambiguous_default() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("config.json");
    let environment = HashMap::from([(
        "PASEO_VOICE_CONFIG".to_owned(),
        path.to_string_lossy().into_owned(),
    )]);
    let profile = |id: &str, default: bool| {
        format!(
            r#"{{"id":"{id}","label":"{id} host","target":"{id}.example:6767","default":{default},"defaultCwd":"~/","defaultProvider":"opencode/gpt-5.6-sol-max"}}"#
        )
    };
    std::fs::write(
        &path,
        format!(
            r#"{{"paseoHosts":[{},{}]}}"#,
            profile("wsl", true),
            profile("spark", false)
        ),
    )
    .expect("write config");
    let loaded = config::load(&environment).expect("valid profiles");
    assert_eq!(loaded.paseo_hosts.len(), 2);
    let default = loaded
        .paseo_hosts
        .iter()
        .find(|profile| profile.default)
        .expect("default profile");
    assert_eq!(default.id, "wsl");
    assert_eq!(default.default_cwd, "~/");

    std::fs::write(
        &path,
        format!(
            r#"{{"paseoHosts":[{},{}]}}"#,
            profile("wsl", true),
            profile("spark", true)
        ),
    )
    .expect("write ambiguous config");
    assert_eq!(
        config::load(&environment).expect_err("multiple defaults rejected"),
        "exactly one Paseo host profile must be the default"
    );

    std::fs::write(
        &path,
        format!(
            r#"{{"paseoHosts":[{},{}]}}"#,
            profile("same", true),
            profile("same", false)
        ),
    )
    .expect("write duplicate config");
    assert_eq!(
        config::load(&environment).expect_err("duplicate IDs rejected"),
        "Paseo host profile IDs must be unique"
    );
}

#[test]
fn unsafe_paseo_host_profile_fields_fail_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("config.json");
    let environment = HashMap::from([(
        "PASEO_VOICE_CONFIG".to_owned(),
        path.to_string_lossy().into_owned(),
    )]);
    for invalid in [
        r#"{"id":"bad id","label":"Host","target":"host.example:6767","default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"}"#,
        r#"{"id":"host","label":"Host","target":"tcp://host.example:6767?password=secret","default":true,"defaultCwd":"~/","defaultProvider":"opencode/model"}"#,
        r#"{"id":"host","label":"Host","target":"host.example:6767","default":true,"defaultCwd":"~/","defaultProvider":"opencode"}"#,
    ] {
        std::fs::write(&path, format!(r#"{{"paseoHosts":[{invalid}]}}"#))
            .expect("write invalid profile");
        assert_eq!(
            config::load(&environment).expect_err("invalid profile rejected"),
            "invalid Paseo host profile"
        );
    }
}

#[test]
fn environment_secret_provider_preserves_exact_values_and_empty_is_missing() {
    let executor = FakeExecutor {
        outputs: Mutex::default(),
        calls: Mutex::default(),
    };
    let config = SecretConfig {
        provider: SecretProvider::Environment,
        bws_binary: "bws".to_owned(),
        bws_env_file: "unused".into(),
        bws_openai_id: None,
        bws_paseo_id: None,
        bws_spark_id: None,
        onepassword_binary: "op".to_owned(),
        onepassword_openai_ref: None,
        onepassword_paseo_ref: None,
        onepassword_spark_ref: None,
    };
    let environment = HashMap::from([
        ("OPENAI_API_KEY".to_owned(), "  exact-key\n".to_owned()),
        (
            "PASEO_VOICE_SPARK_API_KEY".to_owned(),
            "test-model-key".to_owned(),
        ),
        ("PASEO_PASSWORD".to_owned(), String::new()),
    ]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.openai_api_key.as_deref(), Some("  exact-key\n"));
    assert_eq!(secrets.spark_api_key.as_deref(), Some("test-model-key"));
    assert!(secrets.paseo_password.is_none());
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn environment_provider_accepts_xai_api_key_alias_for_model_endpoint() {
    let executor = FakeExecutor {
        outputs: Mutex::default(),
        calls: Mutex::default(),
    };
    let config = SecretConfig {
        provider: SecretProvider::Environment,
        bws_binary: "bws".to_owned(),
        bws_env_file: "unused".into(),
        bws_openai_id: None,
        bws_paseo_id: None,
        bws_spark_id: None,
        onepassword_binary: "op".to_owned(),
        onepassword_openai_ref: None,
        onepassword_paseo_ref: None,
        onepassword_spark_ref: None,
    };
    let environment = HashMap::from([("XAI_API_KEY".to_owned(), "xai-sub-key".to_owned())]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.spark_api_key.as_deref(), Some("xai-sub-key"));

    let environment = HashMap::from([
        (
            "PASEO_VOICE_SPARK_API_KEY".to_owned(),
            "dedicated-model-key".to_owned(),
        ),
        ("XAI_API_KEY".to_owned(), "xai-sub-key".to_owned()),
    ]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(
        secrets.spark_api_key.as_deref(),
        Some("dedicated-model-key"),
        "dedicated spark key wins over XAI_API_KEY"
    );
}

#[test]
fn environment_provider_reads_grok_subscription_auth_store() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let auth_path = directory.path().join("auth.json");
    std::fs::write(
        &auth_path,
        r#"{"https://auth.x.ai::client":{"key":"oidc-access-token","auth_mode":"oidc"}}"#,
    )
    .expect("write grok auth store");
    let executor = FakeExecutor {
        outputs: Mutex::default(),
        calls: Mutex::default(),
    };
    let config = SecretConfig {
        provider: SecretProvider::Environment,
        bws_binary: "bws".to_owned(),
        bws_env_file: "unused".into(),
        bws_openai_id: None,
        bws_paseo_id: None,
        bws_spark_id: None,
        onepassword_binary: "op".to_owned(),
        onepassword_openai_ref: None,
        onepassword_paseo_ref: None,
        onepassword_spark_ref: None,
    };
    let environment = HashMap::from([(
        "GROK_AUTH_FILE".to_owned(),
        auth_path.to_string_lossy().into_owned(),
    )]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.spark_api_key.as_deref(), Some("oidc-access-token"));
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn official_xai_summariser_endpoint_is_accepted() {
    for value in ["https://api.x.ai/v1", "https://api.x.ai/v1/"] {
        let environment = HashMap::from([
            ("PASEO_VOICE_SPARK_BASE_URL".to_owned(), value.to_owned()),
            (
                "PASEO_VOICE_SPARK_MODEL".to_owned(),
                "grok-4-1-fast-non-reasoning".to_owned(),
            ),
        ]);
        let loaded = config::load(&environment).unwrap_or_else(|error| {
            panic!("xAI endpoint {value} should load: {error}");
        });
        assert_eq!(loaded.spark_base_url, value);
        assert_eq!(loaded.spark_model, "grok-4-1-fast-non-reasoning");
    }
}

#[test]
fn official_xai_realtime_endpoint_is_accepted() {
    for value in ["wss://api.x.ai/v1/realtime", "wss://api.x.ai/v1/realtime/"] {
        let environment = HashMap::from([
            ("PASEO_VOICE_OPENAI_BASE_URL".to_owned(), value.to_owned()),
            (
                "PASEO_VOICE_OPENAI_MODEL".to_owned(),
                "grok-voice-latest".to_owned(),
            ),
            ("PASEO_VOICE_OPENAI_VOICE".to_owned(), "eve".to_owned()),
        ]);
        let loaded = config::load(&environment).unwrap_or_else(|error| {
            panic!("xAI Realtime endpoint {value} should load: {error}");
        });
        assert_eq!(loaded.openai_base_url, value);
        assert_eq!(loaded.openai_model, "grok-voice-latest");
        assert_eq!(loaded.openai_voice, "eve");
    }
}

#[test]
fn bitwarden_passes_essential_os_variables_and_token_to_the_bws_child() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let token_file = directory.path().join("bws.env");
    std::fs::write(
        &token_file,
        "BWS_ACCESS_TOKEN=0.aaaaaaaa.bbbbbbbb:cccccccc\n",
    )
    .expect("write token file");
    let executor = FakeExecutor {
        outputs: Mutex::new(VecDeque::from([
            success(r#"{"value":"sk-openai"}"#),
            success(r#"{"value":"paseo-password"}"#),
        ])),
        calls: Mutex::default(),
    };
    let config = SecretConfig {
        provider: SecretProvider::Bitwarden,
        bws_binary: "bws".to_owned(),
        bws_env_file: token_file,
        bws_openai_id: Some("openai-secret-id".to_owned()),
        bws_paseo_id: Some("paseo-secret-id".to_owned()),
        bws_spark_id: None,
        onepassword_binary: "op".to_owned(),
        onepassword_openai_ref: None,
        onepassword_paseo_ref: None,
        onepassword_spark_ref: None,
    };
    let environment = HashMap::from([
        ("SystemRoot".to_owned(), r"C:\Windows".to_owned()),
        ("PATH".to_owned(), "/usr/bin".to_owned()),
        ("UNRELATED_SECRET".to_owned(), "must-not-forward".to_owned()),
    ]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.openai_api_key.as_deref(), Some("sk-openai"));
    assert!(secrets.spark_api_key.is_none());
    assert_eq!(secrets.paseo_password.as_deref(), Some("paseo-password"));

    let calls = executor.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 2);
    let (program, args, env, _timeout) = &calls[0];
    assert_eq!(program, "bws");
    assert_eq!(args, &["secret", "get", "openai-secret-id"]);
    // Windows sockets fail to initialize without SystemRoot (os error 10106),
    // so it must reach the bws child even under a cleared environment.
    assert_eq!(
        env.get("SystemRoot").map(String::as_str),
        Some(r"C:\Windows")
    );
    assert_eq!(env.get("PATH").map(String::as_str), Some("/usr/bin"));
    assert!(env.contains_key("BWS_ACCESS_TOKEN"));
    // Unrelated process variables are never forwarded to the child.
    assert!(!env.contains_key("UNRELATED_SECRET"));
}

#[test]
fn bitwarden_provider_accepts_environment_fallback_for_local_model_key() {
    let executor = FakeExecutor {
        outputs: Mutex::default(),
        calls: Mutex::default(),
    };
    let config = SecretConfig {
        provider: SecretProvider::Bitwarden,
        bws_binary: "bws".to_owned(),
        bws_env_file: "missing-token-file".into(),
        bws_openai_id: None,
        bws_paseo_id: None,
        bws_spark_id: None,
        onepassword_binary: "op".to_owned(),
        onepassword_openai_ref: None,
        onepassword_paseo_ref: None,
        onepassword_spark_ref: None,
    };
    let environment = HashMap::from([(
        "PASEO_VOICE_SPARK_API_KEY".to_owned(),
        "test-model-key".to_owned(),
    )]);

    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.spark_api_key.as_deref(), Some("test-model-key"));
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn onepassword_reads_sequentially_and_never_logs_or_transforms_values() {
    let executor = FakeExecutor {
        outputs: Mutex::new(VecDeque::from([
            success(r#""  openai\n""#),
            success(r#""  xai-model\n""#),
            success(r#""  paseo\t""#),
        ])),
        calls: Mutex::default(),
    };
    let config = SecretConfig {
        provider: SecretProvider::OnePassword,
        bws_binary: "bws".to_owned(),
        bws_env_file: "unused".into(),
        bws_openai_id: None,
        bws_paseo_id: None,
        bws_spark_id: None,
        onepassword_binary: "custom-op".to_owned(),
        onepassword_openai_ref: Some("op://vault/openai/key".to_owned()),
        onepassword_paseo_ref: Some("op://vault/paseo/password".to_owned()),
        onepassword_spark_ref: Some("op://vault/xai/credential".to_owned()),
    };
    let environment = HashMap::from([
        ("PATH".to_owned(), "/bin".to_owned()),
        (
            "PASEO_VOICE_SPARK_API_KEY".to_owned(),
            "unused-fallback".to_owned(),
        ),
        ("XAI_API_KEY".to_owned(), "also-unused".to_owned()),
    ]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.openai_api_key.as_deref(), Some("  openai\n"));
    assert_eq!(secrets.spark_api_key.as_deref(), Some("  xai-model\n"));
    assert_eq!(secrets.paseo_password.as_deref(), Some("  paseo\t"));
    let calls = executor.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 3);
    assert!(calls.iter().all(|call| {
        !call.2.contains_key("PASEO_VOICE_SPARK_API_KEY") && !call.2.contains_key("XAI_API_KEY")
    }));
    assert_eq!(
        calls[0].1,
        ["read", "--format", "json", "op://vault/openai/key"]
    );
    assert_eq!(
        calls[1].1,
        ["read", "--format", "json", "op://vault/xai/credential"]
    );
    assert_eq!(
        calls[2].1,
        ["read", "--format", "json", "op://vault/paseo/password"]
    );
    assert_eq!(calls[0].3, Duration::from_secs(20));
}

#[test]
fn onepassword_falls_back_to_environment_model_key_when_spark_ref_missing() {
    let executor = FakeExecutor {
        outputs: Mutex::new(VecDeque::from([
            success(r#""openai""#),
            success(r#""paseo""#),
        ])),
        calls: Mutex::default(),
    };
    let config = SecretConfig {
        provider: SecretProvider::OnePassword,
        bws_binary: "bws".to_owned(),
        bws_env_file: "unused".into(),
        bws_openai_id: None,
        bws_paseo_id: None,
        bws_spark_id: None,
        onepassword_binary: "op".to_owned(),
        onepassword_openai_ref: Some("op://vault/openai/key".to_owned()),
        onepassword_paseo_ref: Some("op://vault/paseo/password".to_owned()),
        onepassword_spark_ref: None,
    };
    let environment = HashMap::from([("XAI_API_KEY".to_owned(), "xai-env".to_owned())]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.openai_api_key.as_deref(), Some("openai"));
    assert_eq!(secrets.spark_api_key.as_deref(), Some("xai-env"));
    assert_eq!(secrets.paseo_password.as_deref(), Some("paseo"));
    assert_eq!(executor.calls.lock().expect("calls lock").len(), 2);
}

#[test]
fn bitwarden_token_parser_never_sources_shell_text() {
    assert_eq!(
        parse_bws_token("# note\nexport BWS_ACCESS_TOKEN='token-value' # local"),
        Some("token-value".to_owned())
    );
    assert_eq!(parse_bws_token("OTHER=value"), None);
}
