//! Strict Rust runtime configuration.

use std::{collections::HashMap, fs, path::PathBuf};

use serde::Deserialize;

use crate::secrets::{SecretConfig, SecretProvider};

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
    /// Local compatible summariser base URL.
    pub spark_base_url: String,
    /// Summariser model.
    pub spark_model: String,
    /// Characters before local summarisation.
    pub summarise_threshold_chars: usize,
    /// Proposal lifetime.
    pub proposal_ttl_ms: u64,
    /// Paseo executable.
    pub paseo_bin: String,
    /// Optional remote Paseo host.
    pub paseo_host: Option<String>,
    /// Force text-only mock mode.
    pub force_mock: bool,
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
            proposal_ttl_ms: 120_000,
            paseo_bin: "paseo".to_owned(),
            paseo_host: None,
            force_mock: false,
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
pub fn load(environment: &HashMap<String, String>) -> Result<Config, String> {
    if environment.contains_key("PASEO_VOICE_DEV") {
        return Err("PASEO_VOICE_DEV was removed; select the environment provider".to_owned());
    }
    let default_path = Config::default()
        .journal_path
        .parent()
        .and_then(|path| path.parent())
        .map_or_else(
            || PathBuf::from("config.json"),
            |path| path.join("paseo-voice/config.json"),
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
    config.listen_port =
        number_override(environment, "PASEO_VOICE_LISTEN_PORT", config.listen_port)?;
    config.proposal_ttl_ms = number_override(
        environment,
        "PASEO_VOICE_PROPOSAL_TTL_MS",
        config.proposal_ttl_ms,
    )?;
    if let Some(value) = environment.get("PASEO_VOICE_MOCK") {
        config.force_mock = matches!(value.as_str(), "1" | "true");
    }
    Ok(config)
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
