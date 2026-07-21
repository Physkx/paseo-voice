//! Paseo Voice control-plane adapters and protocol.

#![forbid(unsafe_code)]

pub mod clock;
pub mod commands;
pub mod config;
pub mod console;
pub mod dictation;
pub mod journal;
pub mod paseo;
pub mod protocol;
pub mod provider;
pub mod realtime;
pub mod runtime;
pub mod secrets;
pub mod summarise;
pub mod tools;
pub mod voice_mode;

/// Return the stable version line printed by the scaffold executable.
#[must_use]
pub fn version_line() -> String {
    format!("paseo-control-plane {}", env!("CARGO_PKG_VERSION"))
}
mod auto_reply;
