use std::collections::HashMap;

use paseo_control_plane::{
    config::{CleanupProfile, Config, VoiceProfile, VoiceProviderType},
    provider::{ProviderAdapter, cleanup_profiles_frame, voice_profiles_frame},
};
use serde_json::json;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

fn profile(provider_type: VoiceProviderType, base_url: &str) -> VoiceProfile {
    VoiceProfile {
        id: "voice".to_owned(),
        label: "Test voice".to_owned(),
        provider_type,
        base_url: base_url.to_owned(),
        model: if provider_type == VoiceProviderType::Xai {
            "grok-voice-latest"
        } else {
            "realtime-model"
        }
        .to_owned(),
        voice: "eve".to_owned(),
        transcription_model: if provider_type == VoiceProviderType::Xai {
            "grok-transcribe"
        } else {
            "transcribe"
        }
        .to_owned(),
        credential_ref: (provider_type != VoiceProviderType::OpenaiCompatible)
            .then(|| "voice-key".to_owned()),
        default: true,
    }
}

#[test]
fn xai_request_uses_exact_endpoint_model_query_and_explicit_bearer() {
    let config = Config {
        voice_profiles: vec![profile(
            VoiceProviderType::Xai,
            "wss://api.x.ai/v1/realtime",
        )],
        ..Config::default()
    };
    let adapter = ProviderAdapter::new(
        &config.voice_profiles[0],
        &HashMap::from([("voice-key".to_owned(), "xai-secret".to_owned())]),
    );
    let request = adapter.connection_request(&config).expect("xAI request");
    assert_eq!(
        request.uri().to_string(),
        "wss://api.x.ai/v1/realtime?model=grok-voice-latest"
    );
    assert_eq!(
        request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer xai-secret")
    );
    let session = adapter.session_update();
    assert_eq!(
        session["session"]["audio"]["input"]["transcription"]["model"],
        "grok-transcribe"
    );
    assert_eq!(session["session"]["audio"]["output"]["voice"], "eve");
    assert_eq!(session["session"]["tool_choice"], "auto");
    assert!(
        session["session"]["tools"]
            .as_array()
            .is_some_and(|tools| !tools.is_empty())
    );
}

#[test]
fn xai_cumulative_transcription_is_normalized_as_replacement_text() {
    let config = Config::default();
    let xai = profile(VoiceProviderType::Xai, "wss://api.x.ai/v1/realtime");
    let adapter = ProviderAdapter::new(
        &xai,
        &HashMap::from([("voice-key".to_owned(), "secret".to_owned())]),
    );
    let normalized = adapter
        .normalize_event(json!({
            "type":"conversation.item.input_audio_transcription.updated",
            "event_id":"event-1",
            "item_id":"item-1",
            "transcript":"corrected cumulative text"
        }))
        .expect("normalized event");
    assert_eq!(
        normalized["type"],
        "conversation.item.input_audio_transcription.delta"
    );
    assert_eq!(normalized["delta"], "corrected cumulative text");
    assert_eq!(normalized["cumulative"], true);
    assert_eq!(
        config.default_voice_profile().provider_type,
        VoiceProviderType::Openai
    );
}

#[test]
fn compatible_keyless_loopback_gets_no_ambient_credential_and_keeps_strict_deletion() {
    let config = Config {
        voice_profiles: vec![profile(
            VoiceProviderType::OpenaiCompatible,
            "ws://127.0.0.1:9000/realtime",
        )],
        ..Config::default()
    };
    let credentials = HashMap::from([
        ("openai".to_owned(), "openai-secret".to_owned()),
        ("xai".to_owned(), "xai-secret".to_owned()),
    ]);
    let adapter = ProviderAdapter::new(&config.voice_profiles[0], &credentials);
    let request = adapter
        .connection_request(&config)
        .expect("compatible request");
    assert!(!request.headers().contains_key(AUTHORIZATION));
    assert!(adapter.capabilities().dictation_item_deletion);
    let frame = voice_profiles_frame(&config, &credentials, "voice", true);
    assert_eq!(frame["profiles"][0]["dictation_available"], true);
    let encoded = frame.to_string();
    assert!(!encoded.contains("127.0.0.1"));
    assert!(!encoded.contains("openai-secret"));
    assert!(!encoded.contains("credential_ref"));
}

#[test]
fn exact_xai_cleanup_reports_oauth_availability_without_exposing_routing_data() {
    let config = Config {
        cleanup_profiles: vec![CleanupProfile {
            id: "xai-cleanup".to_owned(),
            label: "xAI cleanup".to_owned(),
            base_url: "https://api.x.ai/v1".to_owned(),
            model: "grok-cleanup".to_owned(),
            credential_ref: None,
            default: true,
            allow_insecure_private_http: false,
        }],
        ..Config::default()
    };
    let unavailable = cleanup_profiles_frame(&config, &HashMap::new(), false, "xai-cleanup", true);
    assert_eq!(
        unavailable["profiles"][0]["status"],
        "credential_unavailable"
    );
    let configured = cleanup_profiles_frame(&config, &HashMap::new(), true, "xai-cleanup", true);
    assert_eq!(configured["profiles"][0]["status"], "configured");
    let encoded = configured.to_string();
    assert!(!encoded.contains("api.x.ai"));
    assert!(!encoded.contains("credential_ref"));
}
