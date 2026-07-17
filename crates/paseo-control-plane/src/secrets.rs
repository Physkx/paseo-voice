//! In-memory secret resolution with direct child execution.

use std::{collections::HashMap, fs, path::PathBuf, time::Duration};

use serde_json::Value;

use crate::paseo::ProcessExecutor;

/// Supported startup secret providers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretProvider {
    /// Bitwarden Secrets Manager CLI.
    Bitwarden,
    /// 1Password CLI.
    OnePassword,
    /// Process environment, primarily for development.
    Environment,
}

/// Non-secret configuration for secret resolution.
pub struct SecretConfig {
    /// Selected provider.
    pub provider: SecretProvider,
    /// Bitwarden executable.
    pub bws_binary: String,
    /// File containing only the Bitwarden access-token assignment.
    pub bws_env_file: PathBuf,
    /// `OpenAI` Bitwarden secret ID.
    pub bws_openai_id: Option<String>,
    /// Paseo Bitwarden secret ID.
    pub bws_paseo_id: Option<String>,
    /// 1Password executable.
    pub onepassword_binary: String,
    /// `OpenAI` 1Password reference.
    pub onepassword_openai_ref: Option<String>,
    /// Paseo 1Password reference.
    pub onepassword_paseo_ref: Option<String>,
}

/// Secrets retained only in process memory.
pub struct Secrets {
    /// Realtime API credential, or none for mock mode.
    pub openai_api_key: Option<String>,
    /// Paseo credential, or none for read/write unavailable mode.
    pub paseo_password: Option<String>,
}

/// Resolve configured secrets once, independently and without logging values.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn load_secrets<E: ProcessExecutor>(
    config: &SecretConfig,
    executor: &E,
    environment: &HashMap<String, String>,
) -> Secrets {
    match config.provider {
        SecretProvider::Environment => Secrets {
            openai_api_key: nonempty(environment.get("OPENAI_API_KEY")),
            paseo_password: nonempty(environment.get("PASEO_PASSWORD")),
        },
        SecretProvider::Bitwarden => {
            let token = fs::read_to_string(&config.bws_env_file)
                .ok()
                .and_then(|text| parse_bws_token(&text));
            let Some(token) = token else {
                return Secrets {
                    openai_api_key: None,
                    paseo_password: None,
                };
            };
            Secrets {
                openai_api_key: config
                    .bws_openai_id
                    .as_deref()
                    .and_then(|id| read_bws(executor, &config.bws_binary, id, &token, environment)),
                paseo_password: config
                    .bws_paseo_id
                    .as_deref()
                    .and_then(|id| read_bws(executor, &config.bws_binary, id, &token, environment)),
            }
        }
        SecretProvider::OnePassword => Secrets {
            openai_api_key: config
                .onepassword_openai_ref
                .as_deref()
                .and_then(|reference| {
                    read_onepassword(executor, &config.onepassword_binary, reference, environment)
                }),
            paseo_password: config
                .onepassword_paseo_ref
                .as_deref()
                .and_then(|reference| {
                    read_onepassword(executor, &config.onepassword_binary, reference, environment)
                }),
        },
    }
}

fn nonempty(value: Option<&String>) -> Option<String> {
    value.filter(|value| !value.is_empty()).cloned()
}

/// Parse a Bitwarden token assignment without sourcing a shell file.
#[must_use]
pub fn parse_bws_token(text: &str) -> Option<String> {
    text.lines().find_map(|line| {
        let line = line.trim();
        let line = line.strip_prefix("export ").unwrap_or(line);
        let value = line.strip_prefix("BWS_ACCESS_TOKEN=")?.trim();
        let value = value.split_once(" #").map_or(value, |(before, _)| before);
        Some(
            value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| {
                    value
                        .strip_prefix('\'')
                        .and_then(|value| value.strip_suffix('\''))
                })
                .unwrap_or(value)
                .to_owned(),
        )
    })
}

fn read_bws<E: ProcessExecutor>(
    executor: &E,
    binary: &str,
    id: &str,
    token: &str,
    environment: &HashMap<String, String>,
) -> Option<String> {
    let mut env = minimal_base_environment(environment);
    env.insert("BWS_ACCESS_TOKEN".to_owned(), token.to_owned());
    let output = executor.execute(
        binary,
        &["secret".to_owned(), "get".to_owned(), id.to_owned()],
        &env,
        Duration::from_secs(20),
    );
    if output.exit_code != Some(0) || output.timed_out || output.spawn_failed {
        return None;
    }
    serde_json::from_str::<Value>(&output.stdout)
        .ok()?
        .get("value")?
        .as_str()
        .map(str::to_owned)
}

fn read_onepassword<E: ProcessExecutor>(
    executor: &E,
    binary: &str,
    reference: &str,
    environment: &HashMap<String, String>,
) -> Option<String> {
    let output = executor.execute(
        binary,
        &[
            "read".to_owned(),
            "--format".to_owned(),
            "json".to_owned(),
            reference.to_owned(),
        ],
        environment,
        Duration::from_secs(20),
    );
    if output.exit_code != Some(0) || output.timed_out || output.spawn_failed {
        return None;
    }
    serde_json::from_str::<String>(&output.stdout).ok()
}

fn minimal_base_environment(environment: &HashMap<String, String>) -> HashMap<String, String> {
    ["PATH", "HOME", "WSL_INTEROP", "WSLENV"]
        .into_iter()
        .filter_map(|name| {
            environment
                .get(name)
                .map(|value| (name.to_owned(), value.clone()))
        })
        .collect()
}
