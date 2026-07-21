//! Connection-scoped voice interaction mode.

use serde_json::{Value, json};

/// Selects whether a microphone turn creates an assistant response or a dictation draft.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VoiceMode {
    /// Preserve the conversational Realtime tool loop.
    #[default]
    LiveResponse,
    /// Transcribe speech without creating an assistant response.
    Dictation,
}

impl VoiceMode {
    /// Parse the only accepted browser wire values.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "live_response" => Some(Self::LiveResponse),
            "dictation" => Some(Self::Dictation),
            _ => None,
        }
    }

    /// Return the stable browser wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LiveResponse => "live_response",
            Self::Dictation => "dictation",
        }
    }

    /// Build the browser presentation frame for this mode.
    #[must_use]
    pub fn frame(self) -> Value {
        json!({"type":"voice_mode","mode":self.as_str()})
    }

    /// Apply one strict browser mode request without disturbing a pending write.
    ///
    /// # Errors
    ///
    /// Returns a stable presentation-safe message for malformed requests or pending actions.
    pub fn select_from_control(
        &mut self,
        control: &Value,
        pending_action: bool,
    ) -> Result<Value, &'static str> {
        let Some(object) = control.as_object() else {
            return Err("Invalid voice mode request.");
        };
        if object.len() != 2 || object.get("type").and_then(Value::as_str) != Some("set_voice_mode")
        {
            return Err("Invalid voice mode request.");
        }
        let Some(requested) = object
            .get("mode")
            .and_then(Value::as_str)
            .and_then(Self::parse)
        else {
            return Err("Invalid voice mode request.");
        };
        if pending_action {
            return Err("Finish or cancel the pending action before changing voice mode.");
        }
        *self = requested;
        Ok(self.frame())
    }
}
