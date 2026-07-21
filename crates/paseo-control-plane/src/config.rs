//! Strict Rust runtime configuration.

use std::{
    collections::{HashMap, HashSet},
    fs,
    net::IpAddr,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::secrets::{SecretConfig, SecretProvider};

const OPENAI_REALTIME_ENDPOINT: &str = "wss://api.openai.com/v1/realtime";
const XAI_REALTIME_ENDPOINT: &str = "wss://api.x.ai/v1/realtime";
const MAX_PROFILE_ENTRIES: usize = 64;
const MAX_PRIVATE_ENDPOINT_HOSTS: usize = 64;

/// Supported Realtime provider protocols.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum VoiceProviderType {
    /// Official `OpenAI` Realtime.
    Openai,
    /// Official xAI Voice Agent API.
    Xai,
    /// Conservatively handled OpenAI-compatible Realtime endpoint.
    OpenaiCompatible,
}

impl VoiceProviderType {
    /// Browser-safe provider identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Xai => "xai",
            Self::OpenaiCompatible => "openai-compatible",
        }
    }
}

/// Location derived from a validated endpoint, never from profile display data.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingLocation {
    /// Exact loopback host.
    Local,
    /// Tailscale IPv4 or exact broker allowlisted host.
    PrivateRemote,
    /// Other public HTTPS or WSS endpoint.
    Cloud,
}

impl ProcessingLocation {
    /// Browser-safe location identifier.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::PrivateRemote => "private_remote",
            Self::Cloud => "cloud",
        }
    }
}

/// Validated model endpoint.
pub(crate) struct ValidatedEndpoint {
    pub(crate) url: reqwest::Url,
    pub(crate) location: ProcessingLocation,
}

/// One named Realtime voice route.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct VoiceProfile {
    pub id: String,
    pub label: String,
    pub provider_type: VoiceProviderType,
    pub base_url: String,
    pub model: String,
    pub voice: String,
    pub transcription_model: String,
    pub credential_ref: Option<String>,
    pub default: bool,
}

/// One named OpenAI-compatible dictation cleanup route.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CleanupProfile {
    pub id: String,
    pub label: String,
    pub base_url: String,
    pub model: String,
    pub credential_ref: Option<String>,
    pub default: bool,
    #[serde(default)]
    pub allow_insecure_private_http: bool,
}

/// Fixed OpenAI-compatible reply summarisation route.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SummarisationConfig {
    pub base_url: String,
    pub model: String,
    pub credential_ref: Option<String>,
    #[serde(default)]
    pub allow_insecure_private_http: bool,
}

/// One named broker-only API credential definition.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ApiCredentialConfig {
    pub id: String,
    pub bws_secret_id: Option<String>,
    pub one_password_secret_ref: Option<String>,
    pub environment_variable: Option<String>,
}

/// One trusted Paseo daemon choice and its new-session defaults.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PaseoHostProfile {
    pub id: String,
    pub label: String,
    pub target: Option<String>,
    pub default: bool,
    pub default_cwd: String,
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

fn default_voice_profiles() -> Vec<VoiceProfile> {
    vec![VoiceProfile {
        id: "openai".to_owned(),
        label: "OpenAI".to_owned(),
        provider_type: VoiceProviderType::Openai,
        base_url: OPENAI_REALTIME_ENDPOINT.to_owned(),
        model: "gpt-realtime-2.1".to_owned(),
        voice: "marin".to_owned(),
        transcription_model: "gpt-4o-mini-transcribe".to_owned(),
        credential_ref: Some("openai".to_owned()),
        default: true,
    }]
}

fn default_cleanup_profiles() -> Vec<CleanupProfile> {
    vec![CleanupProfile {
        id: "local".to_owned(),
        label: "Local cleanup".to_owned(),
        base_url: "http://127.0.0.1:1234/v1".to_owned(),
        model: "qwen3.5-9b-instruct-nvfp4".to_owned(),
        credential_ref: None,
        default: true,
        allow_insecure_private_http: false,
    }]
}

fn default_summarisation() -> SummarisationConfig {
    SummarisationConfig {
        base_url: "http://127.0.0.1:1234/v1".to_owned(),
        model: "qwen3.5-9b-instruct-nvfp4".to_owned(),
        credential_ref: None,
        allow_insecure_private_http: false,
    }
}

fn default_api_credentials() -> Vec<ApiCredentialConfig> {
    vec![ApiCredentialConfig {
        id: "openai".to_owned(),
        bws_secret_id: None,
        one_password_secret_ref: None,
        environment_variable: Some("PASEO_VOICE_CREDENTIAL_OPENAI".to_owned()),
    }]
}

/// Validated runtime configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Config {
    pub listen_host: String,
    pub listen_port: u16,
    pub voice_profiles: Vec<VoiceProfile>,
    pub cleanup_profiles: Vec<CleanupProfile>,
    pub summarisation: SummarisationConfig,
    pub api_credentials: Vec<ApiCredentialConfig>,
    pub approved_private_endpoint_hosts: Vec<String>,
    pub summarise_threshold_chars: usize,
    pub log_tail_entries: usize,
    pub auto_reply_poll_ms: u64,
    pub proposal_ttl_ms: u64,
    pub paseo_bin: String,
    pub paseo_hosts: Vec<PaseoHostProfile>,
    pub paseo_host: Option<String>,
    pub force_mock: bool,
    pub log_level: String,
    pub public_dir: PathBuf,
    pub journal_path: PathBuf,
    pub secret_provider: String,
    pub bws_bin: String,
    pub bws_env_file: PathBuf,
    pub bws_secret_id_paseo: Option<String>,
    pub one_password_bin: String,
    pub one_password_secret_ref_paseo: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        let home = std::env::var_os("HOME").map_or_else(|| PathBuf::from("~"), PathBuf::from);
        Self {
            listen_host: "127.0.0.1".to_owned(),
            listen_port: 8790,
            voice_profiles: default_voice_profiles(),
            cleanup_profiles: default_cleanup_profiles(),
            summarisation: default_summarisation(),
            api_credentials: default_api_credentials(),
            approved_private_endpoint_hosts: Vec::new(),
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
            bws_secret_id_paseo: None,
            one_password_bin: "op".to_owned(),
            one_password_secret_ref_paseo: None,
        }
    }
}

/// Load JSON configuration and apply unrelated runtime environment overrides.
///
/// Profile and model endpoint overrides were intentionally removed. Operators must use the
/// strongly typed JSON collections so routing and credential relationships are reviewable.
///
/// # Errors
///
/// Returns a bounded message for malformed, legacy, or unsafe configuration.
#[allow(clippy::implicit_hasher)]
pub fn load(environment: &HashMap<String, String>) -> Result<Config, String> {
    if environment.contains_key("PASEO_VOICE_DEV") {
        return Err("PASEO_VOICE_DEV was removed; select the environment provider".to_owned());
    }
    reject_legacy_environment(environment)?;
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
        "PASEO_VOICE_BWS_SECRET_ID_PASEO",
        &mut config.bws_secret_id_paseo,
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
        config.force_mock = boolean_override(value, "PASEO_VOICE_MOCK")?;
    }
    config.validate()?;
    Ok(config)
}

fn reject_legacy_environment(environment: &HashMap<String, String>) -> Result<(), String> {
    const LEGACY: &[&str] = &[
        "PASEO_VOICE_OPENAI_MODEL",
        "PASEO_VOICE_OPENAI_VOICE",
        "PASEO_VOICE_OPENAI_BASE_URL",
        "PASEO_VOICE_SPARK_BASE_URL",
        "PASEO_VOICE_SPARK_MODEL",
        "PASEO_VOICE_SPARK_API_KEY",
        "PASEO_VOICE_ALLOW_INSECURE_TAILSCALE_SPARK",
        "PASEO_VOICE_BWS_SECRET_ID_OPENAI",
        "PASEO_VOICE_BWS_SECRET_ID_SPARK",
        "PASEO_VOICE_ONEPASSWORD_SECRET_REF_OPENAI",
        "PASEO_VOICE_ONEPASSWORD_SECRET_REF_SPARK",
    ];
    if LEGACY.iter().any(|name| environment.contains_key(*name)) {
        return Err("legacy voice or model environment fields are not supported".to_owned());
    }
    Ok(())
}

impl Config {
    /// Validate all profile, endpoint, credential, and unrelated runtime invariants.
    ///
    /// # Errors
    ///
    /// Returns a bounded message for the first invalid invariant.
    pub fn validate(&mut self) -> Result<(), String> {
        if self.proposal_ttl_ms == 0 {
            return Err("proposal TTL must be positive".to_owned());
        }
        if self.summarise_threshold_chars == 0 {
            return Err("summarise threshold must be positive".to_owned());
        }
        if self.log_tail_entries == 0 || self.log_tail_entries > 200 {
            return Err("log tail must be between 1 and 200".to_owned());
        }
        if self.auto_reply_poll_ms != 0 && !(250..=60_000).contains(&self.auto_reply_poll_ms) {
            return Err(
                "auto reply poll interval must be zero or between 250 and 60000 ms".to_owned(),
            );
        }
        if !matches!(self.log_level.as_str(), "debug" | "info" | "warn" | "error") {
            return Err("invalid log level".to_owned());
        }
        if self.voice_profiles.len() > MAX_PROFILE_ENTRIES
            || self.cleanup_profiles.len() > MAX_PROFILE_ENTRIES
            || self.api_credentials.len() > MAX_PROFILE_ENTRIES
        {
            return Err(
                "voice, cleanup, and credential collections are limited to 64 entries".to_owned(),
            );
        }
        if self.approved_private_endpoint_hosts.len() > MAX_PRIVATE_ENDPOINT_HOSTS {
            return Err("private endpoint allowlist is limited to 64 entries".to_owned());
        }
        validate_private_hosts(&self.approved_private_endpoint_hosts)?;
        validate_credentials(self)?;
        validate_profiles(self)?;
        validate_paseo_hosts(self)?;
        self.secret_config()?;
        Ok(())
    }

    pub(crate) fn voice_endpoint(
        &self,
        profile: &VoiceProfile,
    ) -> Result<ValidatedEndpoint, String> {
        validate_voice_url(profile, &self.approved_private_endpoint_hosts)
    }

    pub(crate) fn cleanup_endpoint(
        &self,
        profile: &CleanupProfile,
    ) -> Result<ValidatedEndpoint, String> {
        validate_model_url(
            &profile.base_url,
            profile.allow_insecure_private_http,
            &self.approved_private_endpoint_hosts,
            "cleanup",
        )
    }

    pub(crate) fn summariser_endpoint(&self) -> Result<ValidatedEndpoint, String> {
        validate_model_url(
            &self.summarisation.base_url,
            self.summarisation.allow_insecure_private_http,
            &self.approved_private_endpoint_hosts,
            "summarisation",
        )
    }

    #[must_use]
    /// Return the unique validated default voice profile.
    ///
    /// # Panics
    ///
    /// Panics only if called before configuration validation.
    pub fn default_voice_profile(&self) -> &VoiceProfile {
        self.voice_profiles
            .iter()
            .find(|profile| profile.default)
            .expect("validated default voice profile")
    }

    #[must_use]
    /// Return the unique validated default cleanup profile.
    ///
    /// # Panics
    ///
    /// Panics only if called before configuration validation.
    pub fn default_cleanup_profile(&self) -> &CleanupProfile {
        self.cleanup_profiles
            .iter()
            .find(|profile| profile.default)
            .expect("validated default cleanup profile")
    }

    /// Convert provider fields for the secret resolver.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported secret provider.
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
            api_credentials: self.api_credentials.clone(),
            bws_paseo_id: self.bws_secret_id_paseo.clone(),
            onepassword_binary: self.one_password_bin.clone(),
            onepassword_paseo_ref: self.one_password_secret_ref_paseo.clone(),
        })
    }
}

fn validate_profiles(config: &Config) -> Result<(), String> {
    validate_named_collection(
        config
            .voice_profiles
            .iter()
            .map(|profile| (&profile.id, &profile.label)),
        "voice profile",
    )?;
    validate_named_collection(
        config
            .cleanup_profiles
            .iter()
            .map(|profile| (&profile.id, &profile.label)),
        "cleanup profile",
    )?;
    exactly_one_default(
        config
            .voice_profiles
            .iter()
            .filter(|profile| profile.default)
            .count(),
        "voice profile",
    )?;
    exactly_one_default(
        config
            .cleanup_profiles
            .iter()
            .filter(|profile| profile.default)
            .count(),
        "cleanup profile",
    )?;
    for profile in &config.voice_profiles {
        validate_model_fields(
            &profile.model,
            Some(&profile.voice),
            Some(&profile.transcription_model),
        )?;
        config.voice_endpoint(profile)?;
        validate_credential_ref(config, profile.credential_ref.as_deref())?;
        if matches!(
            profile.provider_type,
            VoiceProviderType::Openai | VoiceProviderType::Xai
        ) && profile.credential_ref.is_none()
        {
            return Err("official voice profiles require a credentialRef".to_owned());
        }
    }
    for profile in &config.cleanup_profiles {
        validate_model_fields(&profile.model, None, None)?;
        config.cleanup_endpoint(profile)?;
        validate_credential_ref(config, profile.credential_ref.as_deref())?;
    }
    validate_model_fields(&config.summarisation.model, None, None)?;
    config.summariser_endpoint()?;
    validate_credential_ref(config, config.summarisation.credential_ref.as_deref())?;
    Ok(())
}

fn validate_named_collection<'a>(
    values: impl Iterator<Item = (&'a String, &'a String)>,
    kind: &str,
) -> Result<(), String> {
    let values = values.collect::<Vec<_>>();
    if values.is_empty() {
        return Err(format!("at least one {kind} is required"));
    }
    let mut ids = HashSet::new();
    for (id, label) in values {
        if !valid_id(id) || !bounded_text(label, 128) {
            return Err(format!("invalid {kind}"));
        }
        if !ids.insert(id) {
            return Err(format!("{kind} IDs must be unique"));
        }
    }
    Ok(())
}

fn exactly_one_default(count: usize, kind: &str) -> Result<(), String> {
    if count != 1 {
        return Err(format!("exactly one {kind} must be the default"));
    }
    Ok(())
}

fn validate_credentials(config: &Config) -> Result<(), String> {
    let mut ids = HashSet::new();
    for credential in &config.api_credentials {
        if !valid_id(&credential.id) || !ids.insert(&credential.id) {
            return Err("API credential IDs must be unique and valid".to_owned());
        }
        for value in [
            &credential.bws_secret_id,
            &credential.one_password_secret_ref,
            &credential.environment_variable,
        ]
        .into_iter()
        .flatten()
        {
            if !bounded_text(value, 512) {
                return Err("invalid API credential reference".to_owned());
            }
        }
        if credential
            .environment_variable
            .as_deref()
            .is_some_and(|name| !valid_environment_name(name))
        {
            return Err("invalid API credential environment variable".to_owned());
        }
    }
    Ok(())
}

fn validate_credential_ref(config: &Config, reference: Option<&str>) -> Result<(), String> {
    if reference.is_some_and(|reference| {
        !config
            .api_credentials
            .iter()
            .any(|credential| credential.id == reference)
    }) {
        return Err("profile credentialRef does not resolve".to_owned());
    }
    Ok(())
}

fn validate_private_hosts(hosts: &[String]) -> Result<(), String> {
    let mut unique = HashSet::new();
    for host in hosts {
        if !valid_endpoint_host(host)
            || is_loopback_host(host)
            || !unique.insert(host.to_ascii_lowercase())
        {
            return Err("approved private endpoint hosts must be unique exact hosts".to_owned());
        }
    }
    Ok(())
}

fn validate_voice_url(
    profile: &VoiceProfile,
    private_hosts: &[String],
) -> Result<ValidatedEndpoint, String> {
    if matches!(profile.provider_type, VoiceProviderType::Openai)
        && profile.base_url != OPENAI_REALTIME_ENDPOINT
    {
        return Err(
            "official OpenAI voice profiles must use the exact official Realtime endpoint"
                .to_owned(),
        );
    }
    if matches!(profile.provider_type, VoiceProviderType::Xai)
        && profile.base_url != XAI_REALTIME_ENDPOINT
    {
        return Err(
            "official xAI voice profiles must use the exact official Realtime endpoint".to_owned(),
        );
    }
    let url = parse_endpoint(&profile.base_url, "voice")?;
    let host = url
        .host_str()
        .ok_or_else(|| "invalid voice base URL".to_owned())?;
    let location = classify_host(host, private_hosts);
    match url.scheme() {
        "wss" => {}
        "ws" if location == ProcessingLocation::Local
            && profile.provider_type == VoiceProviderType::OpenaiCompatible => {}
        "ws" => return Err("remote voice base URL must use wss".to_owned()),
        _ => return Err("invalid voice base URL scheme".to_owned()),
    }
    Ok(ValidatedEndpoint { url, location })
}

fn validate_model_url(
    value: &str,
    allow_insecure_private: bool,
    private_hosts: &[String],
    kind: &str,
) -> Result<ValidatedEndpoint, String> {
    let url = parse_endpoint(value, kind)?;
    let host = url
        .host_str()
        .ok_or_else(|| format!("invalid {kind} base URL"))?;
    let location = classify_host(host, private_hosts);
    match url.scheme() {
        "https" => {}
        "http" if location == ProcessingLocation::Local => {}
        "http" if location == ProcessingLocation::PrivateRemote && allow_insecure_private => {}
        "http" => {
            return Err(format!(
                "remote {kind} base URL must use https unless explicit private HTTP is enabled"
            ));
        }
        _ => return Err(format!("invalid {kind} base URL scheme")),
    }
    Ok(ValidatedEndpoint { url, location })
}

fn parse_endpoint(value: &str, kind: &str) -> Result<reqwest::Url, String> {
    if value.chars().any(char::is_whitespace) || !has_nonempty_authority(value) {
        return Err(format!("invalid {kind} base URL"));
    }
    let url = reqwest::Url::parse(value).map_err(|_| format!("invalid {kind} base URL"))?;
    if authority_contains_userinfo(value) || !url.username().is_empty() || url.password().is_some()
    {
        return Err(format!("{kind} base URL must not contain credentials"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(format!(
            "{kind} base URL must not contain a query or fragment"
        ));
    }
    Ok(url)
}

fn classify_host(host: &str, private_hosts: &[String]) -> ProcessingLocation {
    if is_loopback_host(host) {
        ProcessingLocation::Local
    } else if is_tailscale_ipv4(host)
        || private_hosts
            .iter()
            .any(|approved| approved.eq_ignore_ascii_case(host))
    {
        ProcessingLocation::PrivateRemote
    } else {
        ProcessingLocation::Cloud
    }
}

fn validate_model_fields(
    model: &str,
    voice: Option<&str>,
    transcription: Option<&str>,
) -> Result<(), String> {
    if !bounded_text(model, 256)
        || voice.is_some_and(|value| !bounded_text(value, 128))
        || transcription.is_some_and(|value| !bounded_text(value, 256))
    {
        return Err("invalid model or voice identifier".to_owned());
    }
    Ok(())
}

fn has_nonempty_authority(value: &str) -> bool {
    value.split_once("://").is_some_and(|(_, rest)| {
        rest.split(['/', '?', '#'])
            .next()
            .is_some_and(|authority| !authority.is_empty())
    })
}

fn authority_contains_userinfo(value: &str) -> bool {
    value.split_once("://").is_some_and(|(_, rest)| {
        rest.split(['/', '?', '#'])
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

fn is_tailscale_ipv4(host: &str) -> bool {
    host.parse::<std::net::Ipv4Addr>().is_ok_and(|address| {
        let [first, second, ..] = address.octets();
        first == 100 && (64..=127).contains(&second)
    })
}

fn valid_endpoint_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && host.trim() == host
        && !host.contains(['/', '?', '#', '@'])
        && !host.chars().any(char::is_control)
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn valid_environment_name(value: &str) -> bool {
    value.len() <= 128
        && value.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_alphabetic() || byte == b'_'
            } else {
                byte.is_ascii_alphanumeric() || byte == b'_'
            }
        })
}

fn bounded_text(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn validate_paseo_hosts(config: &mut Config) -> Result<(), String> {
    exactly_one_default(
        config
            .paseo_hosts
            .iter()
            .filter(|profile| profile.default)
            .count(),
        "Paseo host profile",
    )?;
    let default_index = config
        .paseo_hosts
        .iter()
        .position(|profile| profile.default)
        .ok_or_else(|| "exactly one Paseo host profile must be the default".to_owned())?;
    if let Some(target) = config.paseo_host.clone() {
        config.paseo_hosts[default_index].target = Some(target);
    }
    validate_named_collection(
        config
            .paseo_hosts
            .iter()
            .map(|profile| (&profile.id, &profile.label)),
        "Paseo host profile",
    )?;
    if config
        .paseo_hosts
        .iter()
        .any(|profile| !valid_paseo_profile(profile))
    {
        return Err("invalid Paseo host profile".to_owned());
    }
    Ok(())
}

fn valid_paseo_profile(profile: &PaseoHostProfile) -> bool {
    let valid_target = profile.target.as_deref().is_none_or(|target| {
        bounded_text(target, 2048) && !target.to_ascii_lowercase().contains("password=")
    });
    let valid_provider = profile
        .default_provider
        .split_once('/')
        .is_some_and(|(provider, model)| !provider.is_empty() && !model.is_empty());
    valid_target
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

fn boolean_override(value: &str, name: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => Err(format!("{name} is invalid")),
    }
}
