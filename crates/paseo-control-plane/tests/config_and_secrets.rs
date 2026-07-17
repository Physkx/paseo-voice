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
    ]);
    let loaded = config::load(&environment).expect("load configuration");
    assert_eq!(loaded.listen_port, 9000);
    assert_eq!(loaded.secret_provider, "environment");
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
        onepassword_binary: "op".to_owned(),
        onepassword_openai_ref: None,
        onepassword_paseo_ref: None,
    };
    let environment = HashMap::from([
        ("OPENAI_API_KEY".to_owned(), "  exact-key\n".to_owned()),
        ("PASEO_PASSWORD".to_owned(), String::new()),
    ]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.openai_api_key.as_deref(), Some("  exact-key\n"));
    assert!(secrets.paseo_password.is_none());
    assert!(executor.calls.lock().expect("calls lock").is_empty());
}

#[test]
fn onepassword_reads_sequentially_and_never_logs_or_transforms_values() {
    let executor = FakeExecutor {
        outputs: Mutex::new(VecDeque::from([
            success(r#""  openai\n""#),
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
        onepassword_binary: "custom-op".to_owned(),
        onepassword_openai_ref: Some("op://vault/openai/key".to_owned()),
        onepassword_paseo_ref: Some("op://vault/paseo/password".to_owned()),
    };
    let environment = HashMap::from([("PATH".to_owned(), "/bin".to_owned())]);
    let secrets = load_secrets(&config, &executor, &environment);
    assert_eq!(secrets.openai_api_key.as_deref(), Some("  openai\n"));
    assert_eq!(secrets.paseo_password.as_deref(), Some("  paseo\t"));
    let calls = executor.calls.lock().expect("calls lock");
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].1,
        ["read", "--format", "json", "op://vault/openai/key"]
    );
    assert_eq!(calls[0].3, Duration::from_secs(20));
}

#[test]
fn bitwarden_token_parser_never_sources_shell_text() {
    assert_eq!(
        parse_bws_token("# note\nexport BWS_ACCESS_TOKEN='token-value' # local"),
        Some("token-value".to_owned())
    );
    assert_eq!(parse_bws_token("OTHER=value"), None);
}
