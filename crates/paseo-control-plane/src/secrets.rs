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
    /// Model (summariser / dictation cleanup) Bitwarden secret ID.
    pub bws_spark_id: Option<String>,
    /// 1Password executable.
    pub onepassword_binary: String,
    /// `OpenAI` 1Password reference.
    pub onepassword_openai_ref: Option<String>,
    /// Paseo 1Password reference.
    pub onepassword_paseo_ref: Option<String>,
    /// Model (summariser / dictation cleanup) 1Password reference.
    pub onepassword_spark_ref: Option<String>,
}

/// Secrets retained only in process memory.
pub struct Secrets {
    /// Realtime API credential, or none for mock mode.
    pub openai_api_key: Option<String>,
    /// Summarisation and dictation-cleanup API credential, or none.
    pub spark_api_key: Option<String>,
    /// Paseo credential, or none for read/write unavailable mode.
    pub paseo_password: Option<String>,
    /// Non-secret origin of the model / xAI bearer for operator logs.
    pub model_credential_source: Option<&'static str>,
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
        SecretProvider::Environment => {
            let (spark_api_key, model_credential_source) =
                environment_model_credential(environment);
            Secrets {
                openai_api_key: nonempty(environment.get("OPENAI_API_KEY")),
                spark_api_key,
                paseo_password: nonempty(environment.get("PASEO_PASSWORD")),
                model_credential_source,
            }
        }
        SecretProvider::Bitwarden => {
            let token = fs::read_to_string(&config.bws_env_file)
                .ok()
                .and_then(|text| parse_bws_token(&text));
            let Some(token) = token else {
                let (spark_api_key, model_credential_source) =
                    environment_model_credential(environment);
                return Secrets {
                    openai_api_key: None,
                    spark_api_key,
                    paseo_password: None,
                    model_credential_source,
                };
            };
            let managed_spark = config
                .bws_spark_id
                .as_deref()
                .and_then(|id| read_bws(executor, &config.bws_binary, id, &token, environment));
            let (spark_api_key, model_credential_source) = managed_spark.map_or_else(
                || environment_model_credential(environment),
                |key| (Some(key), Some("bitwarden")),
            );
            Secrets {
                openai_api_key: config
                    .bws_openai_id
                    .as_deref()
                    .and_then(|id| read_bws(executor, &config.bws_binary, id, &token, environment)),
                spark_api_key,
                paseo_password: config
                    .bws_paseo_id
                    .as_deref()
                    .and_then(|id| read_bws(executor, &config.bws_binary, id, &token, environment)),
                model_credential_source,
            }
        }
        SecretProvider::OnePassword => {
            let op_env = onepassword_environment(environment);
            // Keep read order stable: OpenAI, model/spark, then Paseo.
            let openai_api_key = config
                .onepassword_openai_ref
                .as_deref()
                .and_then(|reference| {
                    read_onepassword(executor, &config.onepassword_binary, reference, &op_env)
                });
            let managed_spark = config
                .onepassword_spark_ref
                .as_deref()
                .and_then(|reference| {
                    read_onepassword(executor, &config.onepassword_binary, reference, &op_env)
                });
            let (spark_api_key, model_credential_source) = managed_spark.map_or_else(
                || environment_model_credential(environment),
                |key| (Some(key), Some("onepassword")),
            );
            let paseo_password = config
                .onepassword_paseo_ref
                .as_deref()
                .and_then(|reference| {
                    read_onepassword(executor, &config.onepassword_binary, reference, &op_env)
                });
            Secrets {
                openai_api_key,
                spark_api_key,
                paseo_password,
                model_credential_source,
            }
        }
    }
}

fn nonempty(value: Option<&String>) -> Option<String> {
    value.filter(|value| !value.is_empty()).cloned()
}

/// Explicit spark key, then Grok subscription OAuth, then pay-per-token `XAI_API_KEY`.
fn environment_model_credential(
    environment: &HashMap<String, String>,
) -> (Option<String>, Option<&'static str>) {
    if let Some(key) = nonempty(environment.get("PASEO_VOICE_SPARK_API_KEY")) {
        return (Some(key), Some("env_spark"));
    }
    if let Some(key) = read_grok_subscription_token(environment) {
        return (Some(key), Some("grok_subscription_oauth"));
    }
    if let Some(key) = nonempty(environment.get("XAI_API_KEY")) {
        return (Some(key), Some("env_xai_api_key"));
    }
    (None, None)
}

const GROK_TOKEN_ENDPOINT: &str = "https://auth.x.ai/oauth2/token";
/// Refresh five minutes early so startup does not hand a nearly-expired bearer to long sessions.
const GROK_REFRESH_SKEW_SECS: u64 = 300;

/// Load the Grok / xAI OIDC access token from the native Grok CLI store.
///
/// The store path is `~/.grok/auth.json` (or `$GROK_AUTH_FILE`). This is a provider-owned
/// `SuperGrok` / grok.com subscription OAuth session (not a console API key). Values are never
/// logged. Near-expiry tokens are refreshed via the OIDC refresh grant and written back to the
/// store so the Grok CLI and this broker share one subscription session.
fn read_grok_subscription_token(environment: &HashMap<String, String>) -> Option<String> {
    let path = grok_auth_path(environment);
    let text = fs::read_to_string(&path).ok()?;
    let mut root: Value = serde_json::from_str(&text).ok()?;
    let object = root.as_object_mut()?;
    let entry_key = object.keys().next()?.clone();
    let entry = object.get_mut(&entry_key)?.as_object_mut()?;
    let access = entry
        .get("key")
        .and_then(Value::as_str)
        .filter(|key| !key.is_empty())
        .map(str::to_owned)?;
    let refresh = entry
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned);
    let client_id = entry
        .get("oidc_client_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned);

    let needs_refresh =
        refresh.is_some() && client_id.is_some() && oauth_access_needs_refresh(&access);

    if !needs_refresh {
        return Some(access);
    }

    let Some(refresh_token) = refresh else {
        return Some(access);
    };
    let Some(client_id) = client_id else {
        return Some(access);
    };
    let Some(refreshed) = refresh_grok_access_token(&client_id, &refresh_token) else {
        // Keep the existing subscription bearer if refresh is temporarily unavailable.
        return Some(access);
    };
    entry.insert(
        "key".to_owned(),
        Value::String(refreshed.access_token.clone()),
    );
    if let Some(next_refresh) = refreshed.refresh_token {
        entry.insert("refresh_token".to_owned(), Value::String(next_refresh));
    }
    if let Some(expires_at) = refreshed.expires_at {
        entry.insert("expires_at".to_owned(), Value::String(expires_at));
    }
    // Best-effort durable write so Grok CLI and later broker starts share the new grant.
    let _ = fs::write(&path, serde_json::to_vec_pretty(&root).ok()?);
    Some(refreshed.access_token)
}

fn grok_auth_path(environment: &HashMap<String, String>) -> PathBuf {
    environment.get("GROK_AUTH_FILE").map_or_else(
        || {
            environment.get("HOME").map_or_else(
                || PathBuf::from("~/.grok/auth.json"),
                |home| PathBuf::from(home).join(".grok/auth.json"),
            )
        },
        PathBuf::from,
    )
}

struct RefreshedGrokToken {
    access_token: String,
    refresh_token: Option<String>,
    expires_at: Option<String>,
}

fn oauth_access_needs_refresh(access_token: &str) -> bool {
    let Some(exp) = jwt_exp_unix(access_token) else {
        // Non-JWT subscription tokens: refresh whenever a refresh grant is available.
        return true;
    };
    exp <= now_unix_secs().saturating_add(GROK_REFRESH_SKEW_SECS)
}

fn jwt_exp_unix(token: &str) -> Option<u64> {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let value: Value = serde_json::from_slice(&decoded).ok()?;
    value.get("exp")?.as_u64()
}

fn now_unix_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn expires_at_label(expires_in: u64) -> String {
    // Operator-facing store field only; JWT `exp` is authoritative for refresh decisions.
    format!("unix:{}", now_unix_secs().saturating_add(expires_in))
}

fn refresh_grok_access_token(client_id: &str, refresh_token: &str) -> Option<RefreshedGrokToken> {
    // Secrets must not appear on process argv. Use an isolated Tokio runtime thread so this
    // stays callable from the sync secret loader without nesting inside the main runtime.
    let client_id = client_id.to_owned();
    let refresh_token = refresh_token.to_owned();
    let join = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;
        runtime.block_on(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .ok()?;
            // `reqwest` is built without default form helpers; send the OAuth body directly.
            let body = format!(
                "grant_type=refresh_token&refresh_token={}&client_id={}",
                urlencoding_encode(&refresh_token),
                urlencoding_encode(&client_id)
            );
            let response = client
                .post(GROK_TOKEN_ENDPOINT)
                .header(reqwest::header::ACCEPT, "application/json")
                .header(
                    reqwest::header::CONTENT_TYPE,
                    "application/x-www-form-urlencoded",
                )
                .body(body)
                .send()
                .await
                .ok()?;
            if !response.status().is_success() {
                return None;
            }
            let body: Value = response.json().await.ok()?;
            let access_token = body.get("access_token")?.as_str()?.to_owned();
            if access_token.is_empty() {
                return None;
            }
            let refresh_token = body
                .get("refresh_token")
                .and_then(Value::as_str)
                .filter(|token| !token.is_empty())
                .map(str::to_owned);
            let expires_at = body
                .get("expires_in")
                .and_then(Value::as_u64)
                .map(expires_at_label);
            Some(RefreshedGrokToken {
                access_token,
                refresh_token,
                expires_at,
            })
        })
    });
    join.join().ok()?
}

fn urlencoding_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => {
                use std::fmt::Write as _;
                let _ = write!(encoded, "%{byte:02X}");
            }
        }
    }
    encoded
}

fn onepassword_environment(environment: &HashMap<String, String>) -> HashMap<String, String> {
    let mut environment = environment.clone();
    // Keep model credentials out of the op child process.
    environment.remove("PASEO_VOICE_SPARK_API_KEY");
    environment.remove("XAI_API_KEY");
    environment
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
