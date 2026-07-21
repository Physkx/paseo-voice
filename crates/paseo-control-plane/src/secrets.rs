//! In-memory secret resolution with direct child execution.

use std::{collections::HashMap, fs, path::PathBuf, time::Duration};

use serde_json::Value;

use crate::{config::ApiCredentialConfig, paseo::ProcessExecutor};

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
    /// Named API credential sources.
    pub api_credentials: Vec<ApiCredentialConfig>,
    /// Paseo Bitwarden secret ID.
    pub bws_paseo_id: Option<String>,
    /// 1Password executable.
    pub onepassword_binary: String,
    /// Paseo 1Password reference.
    pub onepassword_paseo_ref: Option<String>,
}

/// Secrets retained only in process memory.
pub struct Secrets {
    /// Broker-only API credentials keyed by configured credential ID.
    pub api_credentials: HashMap<String, String>,
    /// Provider-owned Grok OAuth token, restricted to exact xAI cleanup routes.
    pub grok_oauth_token: Option<String>,
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
            api_credentials: config
                .api_credentials
                .iter()
                .filter_map(|credential| {
                    let value = credential
                        .environment_variable
                        .as_ref()
                        .and_then(|name| nonempty(environment.get(name)))?;
                    Some((credential.id.clone(), value))
                })
                .collect(),
            grok_oauth_token: read_grok_subscription_token(environment),
            paseo_password: nonempty(environment.get("PASEO_PASSWORD")),
        },
        SecretProvider::Bitwarden => {
            let token = fs::read_to_string(&config.bws_env_file)
                .ok()
                .and_then(|text| parse_bws_token(&text));
            let Some(token) = token else {
                return Secrets {
                    api_credentials: HashMap::new(),
                    grok_oauth_token: read_grok_subscription_token(environment),
                    paseo_password: None,
                };
            };
            Secrets {
                api_credentials: config
                    .api_credentials
                    .iter()
                    .filter_map(|credential| {
                        let value = credential.bws_secret_id.as_deref().and_then(|id| {
                            read_bws(executor, &config.bws_binary, id, &token, environment)
                        })?;
                        Some((credential.id.clone(), value))
                    })
                    .collect(),
                grok_oauth_token: read_grok_subscription_token(environment),
                paseo_password: config
                    .bws_paseo_id
                    .as_deref()
                    .and_then(|id| read_bws(executor, &config.bws_binary, id, &token, environment)),
            }
        }
        SecretProvider::OnePassword => {
            let op_env = onepassword_environment(environment);
            Secrets {
                api_credentials: config
                    .api_credentials
                    .iter()
                    .filter_map(|credential| {
                        let value = credential.one_password_secret_ref.as_deref().and_then(
                            |reference| {
                                read_onepassword(
                                    executor,
                                    &config.onepassword_binary,
                                    reference,
                                    &op_env,
                                )
                            },
                        )?;
                        Some((credential.id.clone(), value))
                    })
                    .collect(),
                grok_oauth_token: read_grok_subscription_token(environment),
                paseo_password: config
                    .onepassword_paseo_ref
                    .as_deref()
                    .and_then(|reference| {
                        read_onepassword(executor, &config.onepassword_binary, reference, &op_env)
                    }),
            }
        }
    }
}

/// Load the broker-side Grok CLI OAuth session without exposing it to child processes.
fn read_grok_subscription_token(environment: &HashMap<String, String>) -> Option<String> {
    let path = environment.get("GROK_AUTH_FILE").map_or_else(
        || {
            environment.get("HOME").map_or_else(
                || PathBuf::from("~/.grok/auth.json"),
                |home| PathBuf::from(home).join(".grok/auth.json"),
            )
        },
        PathBuf::from,
    );
    let text = fs::read_to_string(path).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    value.as_object()?.values().find_map(|entry| {
        entry
            .get("key")
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
    })
}

fn nonempty(value: Option<&String>) -> Option<String> {
    value.filter(|value| !value.is_empty()).cloned()
}

fn onepassword_environment(environment: &HashMap<String, String>) -> HashMap<String, String> {
    let mut child = passthrough_env(environment);
    child.extend(
        environment
            .iter()
            .filter(|(name, _)| name.starts_with("OP_"))
            .map(|(name, value)| (name.clone(), value.clone())),
    );
    child
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
    let mut env = passthrough_env(environment);
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

/// Forward only platform-essential, non-secret variables to a directly-spawned
/// child under a cleared environment.
///
/// `SystemRoot` in particular is required for Windows sockets to initialize; a
/// child spawned without it fails with `WSAEPROVIDERFAILEDINIT` (os error 10106)
/// before it can reach any network endpoint. The Windows names are simply absent
/// on other platforms, so this stays correct everywhere.
///
/// Names are matched case-insensitively and forwarded under their original key,
/// because Windows exposes variables with canonical casing (for example `Path`)
/// that would be missed by an exact-case lookup.
pub(crate) fn passthrough_env(source: &HashMap<String, String>) -> HashMap<String, String> {
    const NAMES: &[&str] = &[
        "PATH",
        "HOME",
        "WSL_INTEROP",
        "WSLENV",
        "SystemRoot",
        "windir",
        "SystemDrive",
        "TEMP",
        "TMP",
    ];
    source
        .iter()
        .filter(|(key, _)| NAMES.iter().any(|name| key.eq_ignore_ascii_case(name)))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_forwards_platform_essentials_and_drops_everything_else() {
        let source = HashMap::from([
            ("PATH".to_owned(), "/usr/bin".to_owned()),
            ("HOME".to_owned(), "/home/mark".to_owned()),
            ("SystemRoot".to_owned(), r"C:\Windows".to_owned()),
            ("windir".to_owned(), r"C:\Windows".to_owned()),
            ("SystemDrive".to_owned(), "C:".to_owned()),
            ("TEMP".to_owned(), r"C:\Temp".to_owned()),
            ("TMP".to_owned(), r"C:\Temp".to_owned()),
            ("OPENAI_API_KEY".to_owned(), "secret".to_owned()),
        ]);
        let env = passthrough_env(&source);
        for name in [
            "PATH",
            "HOME",
            "SystemRoot",
            "windir",
            "SystemDrive",
            "TEMP",
            "TMP",
        ] {
            assert_eq!(
                env.get(name),
                source.get(name),
                "passthrough dropped {name}"
            );
        }
        // A secret that happens to be in the process environment is never forwarded.
        assert!(!env.contains_key("OPENAI_API_KEY"));
    }

    #[test]
    fn passthrough_matches_names_case_insensitively_for_windows() {
        // Native Windows exposes the variable as "Path" (canonical casing) through
        // std::env::vars(), so an exact-case lookup would drop it from the child.
        let source = HashMap::from([
            ("Path".to_owned(), r"C:\Windows\System32".to_owned()),
            ("SystemRoot".to_owned(), r"C:\Windows".to_owned()),
        ]);
        let env = passthrough_env(&source);
        assert_eq!(
            env.get("Path").map(String::as_str),
            Some(r"C:\Windows\System32")
        );
        assert_eq!(
            env.get("SystemRoot").map(String::as_str),
            Some(r"C:\Windows")
        );
    }
}
