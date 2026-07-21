//! Strict Rust runtime configuration.

use std::{
    collections::{HashMap, HashSet},
    fs,
    net::IpAddr,
    path::PathBuf,
};

use serde::Deserialize;

use crate::secrets::{SecretConfig, SecretProvider};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EndpointLocation {
    OpenAiCloud,
    BrokerConfiguredLocal,
    BrokerConfiguredRemote,
}

impl EndpointLocation {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::OpenAiCloud => "OpenAI cloud",
            Self::BrokerConfiguredLocal => "Broker-configured local endpoint",
            Self::BrokerConfiguredRemote => "Broker-configured remote endpoint",
        }
    }
}

pub(crate) struct ValidatedEndpoint {
    pub(crate) url: reqwest::Url,
    pub(crate) location: EndpointLocation,
}

/// One trusted Paseo daemon choice and its new-session defaults.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PaseoHostProfile {
    /// Stable browser-safe identifier.
    pub id: String,
    /// Browser-safe display label.
    pub label: String,
    /// Optional remote daemon target. None uses Paseo's local connection.
    pub target: Option<String>,
    /// Whether this profile is selected for each new browser connection.
    pub default: bool,
    /// Working directory passed unchanged to the selected daemon.
    pub default_cwd: String,
    /// Provider/model passed to Paseo for new sessions.
    pub default_provider: String,
}

fn default_paseo_hosts() -> Vec<PaseoHostProfile> {
    vec![PaseoHostProfile {
        id: "local".to_owned(),
        label: "Local Paseo".to_owned(),
        target: None,
        default: true,
        default_cwd: "~/".to_owned(),
        default_provider: "opencode/gpt-5.6-sol-max".to_owned(),
    }]
}

/// Validated runtime configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Config {
    /// Local listen address.
    pub listen_host: String,
    /// Local listen port.
    pub listen_port: u16,
    /// `OpenAI` Realtime model.
    pub openai_model: String,
    /// `OpenAI` voice.
    pub openai_voice: String,
    /// Realtime WebSocket base URL.
    pub openai_base_url: String,
    /// Compatible summariser base URL.
    pub spark_base_url: String,
    /// Summariser model.
    pub spark_model: String,
    /// Characters before model summarisation.
    pub summarise_threshold_chars: usize,
    /// Recent Paseo log entries inspected for a reply.
    pub log_tail_entries: usize,
    /// Opt-in alpha interval for automatic reply polling; zero disables it.
    pub auto_reply_poll_ms: u64,
    /// Proposal lifetime.
    pub proposal_ttl_ms: u64,
    /// Paseo executable.
    pub paseo_bin: String,
    /// Trusted selectable Paseo daemon profiles.
    pub paseo_hosts: Vec<PaseoHostProfile>,
    /// Optional remote Paseo host.
    pub paseo_host: Option<String>,
    /// Force text-only mock mode.
    pub force_mock: bool,
    /// Bounded runtime log verbosity name.
    pub log_level: String,
    /// Static browser directory.
    pub public_dir: PathBuf,
    /// Metadata journal path.
    pub journal_path: PathBuf,
    /// Secret provider name.
    pub secret_provider: String,
    /// Bitwarden executable.
    pub bws_bin: String,
    /// Bitwarden environment file.
    pub bws_env_file: PathBuf,
    /// Bitwarden `OpenAI` secret ID.
    pub bws_secret_id_openai: Option<String>,
    /// Bitwarden Paseo secret ID.
    pub bws_secret_id_paseo: Option<String>,
    /// 1Password executable.
    pub one_password_bin: String,
    /// 1Password `OpenAI` reference.
    pub one_password_secret_ref_openai: Option<String>,
    /// 1Password Paseo reference.
    pub one_password_secret_ref_paseo: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        let home = std::env::var_os("HOME").map_or_else(|| PathBuf::from("~"), PathBuf::from);
        Self {
            listen_host: "127.0.0.1".to_owned(),
            listen_port: 8790,
            openai_model: "gpt-realtime-2.1".to_owned(),
            openai_voice: "marin".to_owned(),
            openai_base_url: "wss://api.openai.com/v1/realtime".to_owned(),
            spark_base_url: "http://127.0.0.1:1234/v1".to_owned(),
            spark_model: "qwen3.5-9b-instruct-nvfp4".to_owned(),
            summarise_threshold_chars: 700,
            log_tail_entries: 40,
            auto_reply_poll_ms: 0,
            proposal_ttl_ms: 120_000,
            paseo_bin: "paseo".to_owned(),
            paseo_hosts: default_paseo_hosts(),
            paseo_host: None,
            force_mock: false,
            log_level: "info".to_owned(),
            public_dir: PathBuf::from("public"),
            journal_path: home.join(".local/state/paseo-voice/journal.sqlite3"),
            secret_provider: "bitwarden".to_owned(),
            bws_bin: "bws".to_owned(),
            bws_env_file: home.join(".config/bws.env"),
            bws_secret_id_openai: None,
            bws_secret_id_paseo: None,
            one_password_bin: "op".to_owned(),
            one_password_secret_ref_openai: None,
            one_password_secret_ref_paseo: None,
        }
    }
}

/// Load JSON configuration and apply environment overrides.
///
/// # Errors
///
/// Returns a bounded message for malformed configuration.
#[allow(clippy::implicit_hasher)]
#[allow(clippy::too_many_lines)]
pub fn load(environment: &HashMap<String, String>) -> Result<Config, String> {
    if environment.contains_key("PASEO_VOICE_DEV") {
        return Err("PASEO_VOICE_DEV was removed; select the environment provider".to_owned());
    }
    let default_path = environment.get("HOME").map_or_else(
        || PathBuf::from("~/.config/paseo-voice/config.json"),
        |home| PathBuf::from(home).join(".config/paseo-voice/config.json"),
    );
    let path = environment
        .get("PASEO_VOICE_CONFIG")
        .map_or(default_path, PathBuf::from);
    let mut config = match fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).map_err(|_| "invalid configuration JSON")?,
        Err(_) => Config::default(),
    };
    macro_rules! string_override {
        ($env:literal, $field:ident) => {
            if let Some(value) = environment.get($env).filter(|value| !value.is_empty()) {
                config.$field.clone_from(value);
            }
        };
    }
    string_override!("PASEO_VOICE_LISTEN_HOST", listen_host);
    string_override!("PASEO_VOICE_OPENAI_MODEL", openai_model);
    string_override!("PASEO_VOICE_OPENAI_VOICE", openai_voice);
    string_override!("PASEO_VOICE_OPENAI_BASE_URL", openai_base_url);
    string_override!("PASEO_VOICE_SPARK_BASE_URL", spark_base_url);
    string_override!("PASEO_VOICE_SPARK_MODEL", spark_model);
    string_override!("PASEO_VOICE_PASEO_BIN", paseo_bin);
    string_override!("PASEO_VOICE_SECRET_PROVIDER", secret_provider);
    string_override!("PASEO_VOICE_LOG_LEVEL", log_level);
    string_override!("PASEO_VOICE_BWS_BIN", bws_bin);
    string_override!("PASEO_VOICE_ONEPASSWORD_BIN", one_password_bin);
    if let Some(value) = environment.get("PASEO_VOICE_PASEO_HOST") {
        config.paseo_host = (!value.is_empty()).then(|| value.clone());
    }
    if let Some(value) = environment.get("PASEO_VOICE_BWS_ENV_FILE") {
        config.bws_env_file = PathBuf::from(value);
    }
    optional_override(
        environment,
        "PASEO_VOICE_BWS_SECRET_ID_OPENAI",
        &mut config.bws_secret_id_openai,
    );
    optional_override(
        environment,
        "PASEO_VOICE_BWS_SECRET_ID_PASEO",
        &mut config.bws_secret_id_paseo,
    );
    optional_override(
        environment,
        "PASEO_VOICE_ONEPASSWORD_SECRET_REF_OPENAI",
        &mut config.one_password_secret_ref_openai,
    );
    optional_override(
        environment,
        "PASEO_VOICE_ONEPASSWORD_SECRET_REF_PASEO",
        &mut config.one_password_secret_ref_paseo,
    );
    config.listen_port =
        number_override(environment, "PASEO_VOICE_LISTEN_PORT", config.listen_port)?;
    config.proposal_ttl_ms = number_override(
        environment,
        "PASEO_VOICE_PROPOSAL_TTL_MS",
        config.proposal_ttl_ms,
    )?;
    config.summarise_threshold_chars = number_override(
        environment,
        "PASEO_VOICE_SUMMARISE_THRESHOLD",
        config.summarise_threshold_chars,
    )?;
    config.log_tail_entries =
        number_override(environment, "PASEO_VOICE_LOG_TAIL", config.log_tail_entries)?;
    config.auto_reply_poll_ms = number_override(
        environment,
        "PASEO_VOICE_AUTO_REPLY_POLL_MS",
        config.auto_reply_poll_ms,
    )?;
    if let Some(value) = environment.get("PASEO_VOICE_MOCK") {
        config.force_mock = match value.to_lowercase().as_str() {
            "1" | "true" => true,
            "0" | "false" => false,
            _ => return Err("PASEO_VOICE_MOCK is invalid".to_owned()),
        };
    }
    if config.proposal_ttl_ms == 0 {
        return Err("proposal TTL must be positive".to_owned());
    }
    if config.summarise_threshold_chars == 0 {
        return Err("summarise threshold must be positive".to_owned());
    }
    if config.log_tail_entries == 0 || config.log_tail_entries > 200 {
        return Err("log tail must be between 1 and 200".to_owned());
    }
    if config.auto_reply_poll_ms != 0 && !(250..=60_000).contains(&config.auto_reply_poll_ms) {
        return Err("auto reply poll interval must be zero or between 250 and 60000 ms".to_owned());
    }
    if !matches!(
        config.log_level.as_str(),
        "debug" | "info" | "warn" | "error"
    ) {
        return Err("invalid log level".to_owned());
    }
    config.realtime_endpoint()?;
    config.summariser_endpoint()?;
    validate_paseo_hosts(&mut config)?;
    Ok(config)
}

fn validate_realtime_url(value: &str) -> Result<ValidatedEndpoint, String> {
    if value.chars().any(char::is_whitespace) || !has_nonempty_authority(value) {
        return Err("invalid OpenAI Realtime base URL".to_owned());
    }
    let url = reqwest::Url::parse(value).map_err(|_| "invalid OpenAI Realtime base URL")?;
    if authority_contains_userinfo(value) || !url.username().is_empty() || url.password().is_some()
    {
        return Err("OpenAI Realtime base URL must not contain credentials".to_owned());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("OpenAI Realtime base URL must not contain a query or fragment".to_owned());
    }
    let host = url
        .host_str()
        .ok_or_else(|| "invalid OpenAI Realtime base URL".to_owned())?;
    let loopback = is_loopback_host(host);
    match url.scheme() {
        "wss" => {}
        "ws" if loopback => {}
        "ws" => return Err("remote OpenAI Realtime base URL must use wss".to_owned()),
        _ => return Err("invalid OpenAI Realtime base URL".to_owned()),
    }
    let official_openai = matches!(
        value,
        "wss://api.openai.com/v1/realtime" | "wss://api.openai.com/v1/realtime/"
    );
    let location = if official_openai {
        EndpointLocation::OpenAiCloud
    } else if loopback {
        EndpointLocation::BrokerConfiguredLocal
    } else {
        EndpointLocation::BrokerConfiguredRemote
    };
    Ok(ValidatedEndpoint { url, location })
}

fn validate_summariser_url(value: &str) -> Result<ValidatedEndpoint, String> {
    if value.chars().any(char::is_whitespace) || !has_nonempty_authority(value) {
        return Err("invalid summariser base URL".to_owned());
    }
    let url = reqwest::Url::parse(value).map_err(|_| "invalid summariser base URL")?;
    if authority_contains_userinfo(value) || !url.username().is_empty() || url.password().is_some()
    {
        return Err("summariser base URL must not contain credentials".to_owned());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("summariser base URL must not contain a query or fragment".to_owned());
    }
    let host = url
        .host_str()
        .ok_or_else(|| "invalid summariser base URL".to_owned())?;
    let loopback = is_loopback_host(host);
    match url.scheme() {
        "https" => {}
        "http" if loopback => {}
        "http" => return Err("remote summariser base URL must use https".to_owned()),
        _ => return Err("invalid summariser base URL".to_owned()),
    }
    Ok(ValidatedEndpoint {
        url,
        location: if loopback {
            EndpointLocation::BrokerConfiguredLocal
        } else {
            EndpointLocation::BrokerConfiguredRemote
        },
    })
}

fn has_nonempty_authority(value: &str) -> bool {
    value.split_once("://").is_some_and(|(_, remainder)| {
        remainder
            .split(['/', '?', '#'])
            .next()
            .is_some_and(|authority| !authority.is_empty())
    })
}

fn authority_contains_userinfo(value: &str) -> bool {
    value.split_once("://").is_some_and(|(_, remainder)| {
        remainder
            .split(['/', '?', '#'])
            .next()
            .is_some_and(|authority| authority.contains('@'))
    })
}

fn is_loopback_host(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn validate_paseo_hosts(config: &mut Config) -> Result<(), String> {
    let mut defaults = config
        .paseo_hosts
        .iter_mut()
        .filter(|profile| profile.default);
    let Some(default_profile) = defaults.next() else {
        return Err("exactly one Paseo host profile must be the default".to_owned());
    };
    if defaults.next().is_some() {
        return Err("exactly one Paseo host profile must be the default".to_owned());
    }
    if let Some(target) = config.paseo_host.clone() {
        default_profile.target = Some(target);
    }
    let mut profile_ids = HashSet::new();
    if config
        .paseo_hosts
        .iter()
        .any(|profile| !profile_ids.insert(profile.id.as_str()))
    {
        return Err("Paseo host profile IDs must be unique".to_owned());
    }
    if config
        .paseo_hosts
        .iter()
        .any(|profile| !valid_profile(profile))
    {
        return Err("invalid Paseo host profile".to_owned());
    }
    Ok(())
}

fn valid_profile(profile: &PaseoHostProfile) -> bool {
    let valid_id = !profile.id.is_empty()
        && profile.id.len() <= 64
        && profile.id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        });
    let bounded_text = |value: &str, maximum: usize| {
        !value.is_empty()
            && value.len() <= maximum
            && value.trim() == value
            && !value.chars().any(char::is_control)
    };
    let valid_target = profile.target.as_deref().is_none_or(|target| {
        bounded_text(target, 2048) && !target.to_ascii_lowercase().contains("password=")
    });
    let valid_provider = profile
        .default_provider
        .split_once('/')
        .is_some_and(|(provider, model)| !provider.is_empty() && !model.is_empty());
    valid_id
        && bounded_text(&profile.label, 128)
        && valid_target
        && bounded_text(&profile.default_cwd, 4096)
        && bounded_text(&profile.default_provider, 256)
        && valid_provider
}

fn optional_override(
    environment: &HashMap<String, String>,
    name: &str,
    target: &mut Option<String>,
) {
    if let Some(value) = environment.get(name) {
        *target = (!value.is_empty()).then(|| value.clone());
    }
}

fn number_override<T: std::str::FromStr>(
    environment: &HashMap<String, String>,
    name: &str,
    current: T,
) -> Result<T, String> {
    environment.get(name).map_or(Ok(current), |value| {
        value.parse().map_err(|_| format!("{name} is invalid"))
    })
}

impl Config {
    pub(crate) fn realtime_endpoint(&self) -> Result<ValidatedEndpoint, String> {
        validate_realtime_url(&self.openai_base_url)
    }

    pub(crate) fn summariser_endpoint(&self) -> Result<ValidatedEndpoint, String> {
        validate_summariser_url(&self.spark_base_url)
    }

    /// Convert provider fields for the secret resolver.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported provider.
    pub fn secret_config(&self) -> Result<SecretConfig, String> {
        let provider = match self.secret_provider.as_str() {
            "bitwarden" => SecretProvider::Bitwarden,
            "onepassword" => SecretProvider::OnePassword,
            "environment" => SecretProvider::Environment,
            _ => return Err("unsupported secret provider".to_owned()),
        };
        Ok(SecretConfig {
            provider,
            bws_binary: self.bws_bin.clone(),
            bws_env_file: self.bws_env_file.clone(),
            bws_openai_id: self.bws_secret_id_openai.clone(),
            bws_paseo_id: self.bws_secret_id_paseo.clone(),
            onepassword_binary: self.one_password_bin.clone(),
            onepassword_openai_ref: self.one_password_secret_ref_openai.clone(),
            onepassword_paseo_ref: self.one_password_secret_ref_paseo.clone(),
        })
    }
}
