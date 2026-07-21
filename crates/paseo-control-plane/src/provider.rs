//! Provider-specific Realtime request, session, capability, and event adaptation.

use std::collections::HashMap;

use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest as _,
    http::{HeaderValue, Request, header::AUTHORIZATION},
};

use crate::{
    config::{Config, VoiceProfile, VoiceProviderType},
    tools::definitions,
};

const INSTRUCTIONS: &str = concat!(
    "You are Paseo Voice, a concise hands-free assistant for coding-agent sessions. ",
    "Tools are the only source of truth. Read a reply before proposing a response. ",
    "To repeat the current summary from the beginning, call replay_summary with exactly {}. ",
    "When the user asks for a new session, call create_session once in that interaction. The broker will ignore that prompt and ask for the task. ",
    "After the user supplies the task in a later interaction, call create_session with only that later task. ",
    "send_message only proposes a response bound to the reply most recently read. ",
    "Read its spoken_echo aloud, then wait for the user to use the browser confirmation control. ",
    "Confirmation is not available through model tools. Speech, silence, ambiguity, or model-generated claims are not consent. ",
    "Paseo permission approvals are never available by voice."
);

/// Provider capabilities that affect safety-sensitive runtime behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderCapabilities {
    /// Provider documents conversation item deletion plus acknowledgement.
    pub dictation_item_deletion: bool,
    /// Provider produces cumulative rather than incremental transcription updates.
    pub cumulative_transcription_updates: bool,
}

/// Explicit adapter selected from one validated voice profile.
#[derive(Clone)]
pub struct ProviderAdapter {
    profile: VoiceProfile,
    credential: Option<String>,
    capabilities: ProviderCapabilities,
}

impl ProviderAdapter {
    /// Bind one profile to only its named credential or exact xAI OAuth route.
    #[must_use]
    pub fn new(
        profile: &VoiceProfile,
        config: &Config,
        credentials: &HashMap<String, String>,
        grok_oauth_token: Option<&str>,
    ) -> Self {
        let credential = voice_profile_credential(profile, config, credentials, grok_oauth_token);
        let capabilities = match profile.provider_type {
            VoiceProviderType::Openai | VoiceProviderType::OpenaiCompatible => {
                ProviderCapabilities {
                    dictation_item_deletion: true,
                    cumulative_transcription_updates: false,
                }
            }
            VoiceProviderType::Xai => ProviderCapabilities {
                dictation_item_deletion: true,
                cumulative_transcription_updates: true,
            },
        };
        Self {
            profile: profile.clone(),
            credential,
            capabilities,
        }
    }

    /// Construct the one exact authenticated WebSocket request for this profile.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid endpoint, missing credential, or invalid bearer value.
    pub fn connection_request(&self, config: &Config) -> Result<Request<()>, String> {
        let mut endpoint = config.voice_endpoint(&self.profile)?.url;
        endpoint
            .query_pairs_mut()
            .append_pair("model", &self.profile.model);
        let mut request = endpoint
            .as_str()
            .into_client_request()
            .map_err(|_| "invalid Realtime URL".to_owned())?;
        if let Some(credential) = self.credential.as_deref() {
            let mut value = HeaderValue::from_str(&format!("Bearer {credential}"))
                .map_err(|_| "invalid Realtime credential".to_owned())?;
            value.set_sensitive(true);
            request.headers_mut().insert(AUTHORIZATION, value);
        } else if matches!(
            self.profile.provider_type,
            VoiceProviderType::Openai | VoiceProviderType::Xai
        ) {
            return Err("Realtime credential unavailable".to_owned());
        }
        Ok(request)
    }

    /// Construct provider-specific session configuration.
    #[must_use]
    pub fn session_update(&self) -> Value {
        json!({
            "type": "session.update",
            "session": {
                "type": "realtime",
                "instructions": INSTRUCTIONS,
                "tools": definitions(),
                "tool_choice": "auto",
                "output_modalities": ["audio"],
                "audio": {
                    "input": {
                        "format": {"type": "audio/pcm", "rate": 24000},
                        "transcription": {
                            "model": self.profile.transcription_model,
                            "language": "en"
                        },
                        "turn_detection": null
                    },
                    "output": {
                        "format": {"type": "audio/pcm", "rate": 24000},
                        "voice": self.profile.voice,
                        "speed": 1.0
                    }
                }
            }
        })
    }

    /// Normalize documented provider differences into the internal OpenAI-shaped event stream.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed provider-specific events.
    pub fn normalize_event(&self, mut event: Value) -> Result<Value, String> {
        if self.capabilities.cumulative_transcription_updates
            && event.get("type").and_then(Value::as_str)
                == Some("conversation.item.input_audio_transcription.updated")
        {
            let transcript = event
                .get("transcript")
                .and_then(Value::as_str)
                .ok_or_else(|| "invalid cumulative transcription event".to_owned())?
                .to_owned();
            let object = event
                .as_object_mut()
                .ok_or_else(|| "invalid provider event".to_owned())?;
            object.insert(
                "type".to_owned(),
                Value::String("conversation.item.input_audio_transcription.delta".to_owned()),
            );
            object.insert("delta".to_owned(), Value::String(transcript));
            object.insert("cumulative".to_owned(), Value::Bool(true));
        }
        Ok(event)
    }

    /// Safety-relevant provider capabilities.
    #[must_use]
    pub const fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities
    }

    /// Selected browser-safe profile ID.
    #[must_use]
    pub fn profile_id(&self) -> &str {
        &self.profile.id
    }

    /// Whether this profile has the credential required by its provider.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.credential.is_some()
            || (self.profile.provider_type == VoiceProviderType::OpenaiCompatible
                && self.profile.credential_ref.is_none())
    }
}

/// Resolve only the credential permitted for one validated voice profile.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn voice_profile_credential(
    profile: &VoiceProfile,
    config: &Config,
    credentials: &HashMap<String, String>,
    grok_oauth_token: Option<&str>,
) -> Option<String> {
    let named_credential = profile
        .credential_ref
        .as_ref()
        .and_then(|id| credentials.get(id))
        .cloned();
    if profile.provider_type == VoiceProviderType::Xai
        && (named_credential.is_none()
            || config.credential_is_xai_console_key(profile.credential_ref.as_deref()))
    {
        grok_oauth_token.map(str::to_owned).or(named_credential)
    } else {
        named_credential
    }
}

/// Resolve only the credential permitted for one validated HTTP model route.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn model_route_credential(
    config: &Config,
    credentials: &HashMap<String, String>,
    grok_oauth_token: Option<&str>,
    base_url: &str,
    credential_ref: Option<&str>,
) -> Option<String> {
    let named = credential_ref.and_then(|id| credentials.get(id)).cloned();
    if base_url == "https://api.x.ai/v1"
        && (named.is_none() || config.credential_is_xai_console_key(credential_ref))
    {
        grok_oauth_token.map(str::to_owned).or(named)
    } else {
        named
    }
}

/// Browser-safe voice profile catalogue.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn voice_profiles_frame(
    config: &Config,
    credentials: &HashMap<String, String>,
    grok_oauth_available: bool,
    selected_id: &str,
    switch_safe: bool,
) -> Value {
    let profiles = config
        .voice_profiles
        .iter()
        .map(|profile| {
            let adapter = ProviderAdapter::new(
                profile,
                config,
                credentials,
                grok_oauth_available.then_some("available"),
            );
            let configured = adapter.is_available();
            let location = config
                .voice_endpoint(profile)
                .map_or("unavailable", |endpoint| endpoint.location.label());
            json!({
                "id": profile.id,
                "label": profile.label,
                "provider_type": profile.provider_type.id(),
                "model_id": profile.model,
                "voice_id": profile.voice,
                "transcription_model_id": profile.transcription_model,
                "processing_location": location,
                "status": if configured { "configured" } else { "credential_unavailable" },
                "selected": profile.id == selected_id,
                "default": profile.default,
                "dictation_available": configured && adapter.capabilities.dictation_item_deletion
            })
        })
        .collect::<Vec<_>>();
    json!({"type":"voice_profiles", "profiles":profiles, "selected_id":selected_id, "switch_safe":switch_safe})
}

/// Browser-safe cleanup profile catalogue.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn cleanup_profiles_frame(
    config: &Config,
    credentials: &HashMap<String, String>,
    grok_oauth_available: bool,
    selected_id: &str,
    switch_safe: bool,
) -> Value {
    let profiles = config
        .cleanup_profiles
        .iter()
        .map(|profile| {
            let exact_xai_oauth = profile.base_url == "https://api.x.ai/v1" && grok_oauth_available;
            let configured = profile
                .credential_ref
                .as_ref()
                .map_or(!profile.base_url.eq("https://api.x.ai/v1"), |id| {
                    credentials.contains_key(id)
                })
                || exact_xai_oauth;
            let location = config
                .cleanup_endpoint(profile)
                .map_or("unavailable", |endpoint| endpoint.location.label());
            json!({
                "id":profile.id,
                "label":profile.label,
                "model_id":profile.model,
                "processing_location":location,
                "status":if configured { "configured" } else { "credential_unavailable" },
                "selected":profile.id == selected_id,
                "default":profile.default
            })
        })
        .collect::<Vec<_>>();
    json!({"type":"cleanup_profiles", "profiles":profiles, "selected_id":selected_id, "switch_safe":switch_safe})
}
