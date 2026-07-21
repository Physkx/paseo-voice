use paseo_control_plane::voice_mode::VoiceMode;
use serde_json::json;

#[test]
fn pending_actions_and_malformed_requests_cannot_change_voice_mode() {
    let mut mode = VoiceMode::default();
    let request = json!({"type":"set_voice_mode","mode":"dictation"});

    assert_eq!(
        mode.select_from_control(&request, true),
        Err("Finish or cancel the pending action before changing voice mode.")
    );
    assert_eq!(mode, VoiceMode::LiveResponse);
    assert_eq!(
        mode.select_from_control(
            &json!({"type":"set_voice_mode","mode":"dictation","extra":true}),
            false
        ),
        Err("Invalid voice mode request.")
    );
    assert_eq!(mode, VoiceMode::LiveResponse);
}
