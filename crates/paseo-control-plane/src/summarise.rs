//! Best-effort OpenAI-compatible summarisation.

use serde_json::{Value, json};

use crate::config::Config;

const MAX_SPEECH_OUTPUT_CHARS: usize = 2_400;
// A 180-token summary should be far smaller; this leaves room for compatible response metadata.
const MAX_SUMMARISER_RESPONSE_BYTES: usize = 64 * 1_024;
// Preserve leading outcomes and trailing blockers while keeping model context predictable.
const MAX_SUMMARISER_INPUT_CHARS: usize = 24_000;
const OMITTED_SUMMARISER_INPUT: &str = "\n\n[... middle omitted ...]\n\n";

/// Speakable summary plus an explicit indication that summarisation failed.
pub struct SpeechSummary {
    /// Cleaned, bounded text safe to hand to the speech layer.
    pub text: String,
    /// True when the configured summariser was unavailable or returned invalid output.
    pub degraded: bool,
}

/// Summarise a reply or return a bounded cleaned tail when the endpoint is unavailable.
pub async fn summarise_for_speech(
    client: &reqwest::Client,
    config: &Config,
    text: &str,
    title: Option<&str>,
) -> SpeechSummary {
    if let Some(summary) = summarise(client, config, text, title).await {
        return SpeechSummary {
            text: summary,
            degraded: false,
        };
    }
    SpeechSummary {
        text: degraded_speech_tail(text),
        degraded: true,
    }
}

pub(crate) fn degraded_speech_tail(text: &str) -> String {
    clean_for_speech(text)
        .chars()
        .rev()
        .take(MAX_SPEECH_OUTPUT_CHARS)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

/// Summarise long agent output through the configured endpoint.
#[must_use]
pub async fn summarise(
    client: &reqwest::Client,
    config: &Config,
    text: &str,
    title: Option<&str>,
) -> Option<String> {
    if text.len() <= config.summarise_threshold_chars {
        return Some(
            clean_for_speech(text)
                .chars()
                .take(MAX_SPEECH_OUTPUT_CHARS)
                .collect(),
        );
    }
    let endpoint = format!(
        "{}/chat/completions",
        config.spark_base_url.trim_end_matches('/')
    );
    let mut response = client
        .post(endpoint)
        .timeout(std::time::Duration::from_secs(20))
        .json(&json!({
            "model": config.spark_model,
            "messages": [
                {
                    "role": "system",
                    "content": "Summarise this coding-agent reply for speech in at most three short sentences. Lead with the outcome, then blockers or questions. No markdown."
                },
                {
                    "role": "user",
                    "content": format!(
                        "Session: {}\n\n{}",
                        title.unwrap_or("unknown"),
                        bounded_summariser_input(text)
                    )
                }
            ],
            "temperature": 0.2,
            "max_tokens": 180
        }))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if chunk.len() > MAX_SUMMARISER_RESPONSE_BYTES - body.len() {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&body).ok()?;
    let summary = value.pointer("/choices/0/message/content")?.as_str()?;
    let cleaned = clean_for_speech(summary)
        .chars()
        .take(MAX_SPEECH_OUTPUT_CHARS)
        .collect::<String>();
    (!cleaned.is_empty()).then_some(cleaned)
}

fn bounded_summariser_input(text: &str) -> String {
    if text.chars().count() <= MAX_SUMMARISER_INPUT_CHARS {
        return text.to_owned();
    }
    let retained_chars = MAX_SUMMARISER_INPUT_CHARS - OMITTED_SUMMARISER_INPUT.chars().count();
    let leading_chars = retained_chars / 2;
    let trailing_chars = retained_chars - leading_chars;
    let leading = text.chars().take(leading_chars).collect::<String>();
    let trailing = text
        .chars()
        .rev()
        .take(trailing_chars)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{leading}{OMITTED_SUMMARISER_INPUT}{trailing}")
}

/// Remove common markdown noise while preserving ordinary words.
#[must_use]
pub fn clean_for_speech(text: &str) -> String {
    text.lines()
        .map(|line| line.trim().trim_start_matches(['#', '-', '*', '>']).trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}
