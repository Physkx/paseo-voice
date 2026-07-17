//! Best-effort OpenAI-compatible local summarisation.

use serde_json::{Value, json};

use crate::config::Config;

/// Summarise long agent output through the configured local endpoint.
#[must_use]
pub async fn summarise(
    client: &reqwest::Client,
    config: &Config,
    text: &str,
    title: Option<&str>,
) -> Option<String> {
    if text.len() <= config.summarise_threshold_chars {
        return Some(clean_for_speech(text));
    }
    let endpoint = format!(
        "{}/chat/completions",
        config.spark_base_url.trim_end_matches('/')
    );
    let response = client
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
                    "content": format!("Session: {}\n\n{}", title.unwrap_or("unknown"), text)
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
    let value: Value = response.json().await.ok()?;
    let summary = value.pointer("/choices/0/message/content")?.as_str()?;
    let cleaned = clean_for_speech(summary);
    (!cleaned.is_empty()).then_some(cleaned)
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
