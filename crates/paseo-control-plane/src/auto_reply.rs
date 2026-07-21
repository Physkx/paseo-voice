//! Opt-in polling fallback for automatic reply announcements.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::{
    config::{Config, PaseoHostProfile},
    paseo::{PaseoAdapter, ProcessExecutor},
};

#[derive(Debug)]
pub(crate) struct PolledReply {
    pub(crate) host_id: String,
    pub(crate) session_id: String,
    pub(crate) session_name: String,
    pub(crate) text: String,
}

#[derive(Default)]
struct CompletionTracker {
    states: HashMap<String, String>,
}

impl CompletionTracker {
    fn observe(&mut self, rows: &[Value]) -> Vec<(String, String)> {
        let current_ids = rows
            .iter()
            .filter_map(|row| row.get("id").and_then(Value::as_str))
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        self.states
            .retain(|session_id, _| current_ids.contains(session_id));

        let mut completed = Vec::new();
        for row in rows {
            let (Some(session_id), Some(state)) = (
                row.get("id").and_then(Value::as_str),
                row.get("status").and_then(Value::as_str),
            ) else {
                continue;
            };
            let previous = self.states.insert(session_id.to_owned(), state.to_owned());
            if state.eq_ignore_ascii_case("idle")
                && previous.is_some_and(|value| !value.eq_ignore_ascii_case("idle"))
            {
                let name = row
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("(untitled)")
                    .to_owned();
                completed.push((session_id.to_owned(), name));
            }
        }
        completed
    }
}

pub(crate) fn start(
    config: &Config,
    password: Option<&str>,
    executor: Arc<dyn ProcessExecutor>,
    mut selected_host: watch::Receiver<String>,
    replies: mpsc::Sender<PolledReply>,
) -> Option<tokio::task::JoinHandle<()>> {
    let interval = (config.auto_reply_poll_ms > 0)
        .then(|| Duration::from_millis(config.auto_reply_poll_ms))?;
    let password = password?.to_owned();
    let profiles = config.paseo_hosts.clone();
    let binary = config.paseo_bin.clone();
    let log_tail_entries = config.log_tail_entries;

    Some(tokio::spawn(async move {
        let mut tracker = CompletionTracker::default();
        let mut tracked_host = selected_host.borrow().clone();
        loop {
            tokio::select! {
                changed = selected_host.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    tracked_host.clone_from(&selected_host.borrow());
                    tracker = CompletionTracker::default();
                    continue;
                }
                () = tokio::time::sleep(interval) => {}
            }

            let Some(profile) = profiles.iter().find(|profile| profile.id == tracked_host) else {
                continue;
            };
            let adapter = Arc::new(adapter_for(
                profile,
                Arc::clone(&executor),
                &binary,
                &password,
            ));
            let host_id = profile.id.clone();
            let owned_tracker = std::mem::take(&mut tracker);
            let polled = tokio::task::spawn_blocking(move || {
                poll_once(adapter.as_ref(), owned_tracker, &host_id, log_tail_entries)
            })
            .await;
            let Ok((next_tracker, completed)) = polled else {
                return;
            };
            tracker = next_tracker;
            for reply in completed {
                if replies.send(reply).await.is_err() {
                    return;
                }
            }
        }
    }))
}

fn adapter_for(
    profile: &PaseoHostProfile,
    executor: Arc<dyn ProcessExecutor>,
    binary: &str,
    password: &str,
) -> PaseoAdapter<Arc<dyn ProcessExecutor>> {
    PaseoAdapter::new(
        executor,
        binary.to_owned(),
        password.to_owned(),
        profile.target.clone(),
    )
}

fn poll_once(
    adapter: &PaseoAdapter<Arc<dyn ProcessExecutor>>,
    mut tracker: CompletionTracker,
    host_id: &str,
    log_tail_entries: usize,
) -> (CompletionTracker, Vec<PolledReply>) {
    let Some(rows) = adapter.list_sessions(false) else {
        return (tracker, Vec::new());
    };
    let candidates = tracker.observe(&rows);
    let replies = candidates
        .into_iter()
        .filter_map(|(session_id, session_name)| {
            let text = adapter
                .read_log_text(&session_id, 6, true)
                .filter(|text| !text.is_empty())
                .or_else(|| adapter.read_log_text(&session_id, log_tail_entries, false))?;
            (!text.is_empty()).then(|| PolledReply {
                host_id: host_id.to_owned(),
                session_id,
                session_name,
                text,
            })
        })
        .collect();
    (tracker, replies)
}

#[cfg(test)]
mod tests {
    use super::CompletionTracker;
    use serde_json::json;

    #[test]
    fn announces_only_a_transition_to_idle() {
        let mut tracker = CompletionTracker::default();
        assert!(
            tracker
                .observe(&[json!({"id":"a","status":"idle"})])
                .is_empty()
        );
        assert!(
            tracker
                .observe(&[json!({"id":"a","status":"running"})])
                .is_empty()
        );
        assert_eq!(
            tracker.observe(&[json!({"id":"a","name":"Agent A","status":"idle"})]),
            [("a".to_owned(), "Agent A".to_owned())]
        );
        assert!(
            tracker
                .observe(&[json!({"id":"a","status":"idle"})])
                .is_empty()
        );
    }
}
