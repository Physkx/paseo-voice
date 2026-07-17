//! Paseo Voice control-plane adapters and protocol.

#![forbid(unsafe_code)]

pub mod protocol;

/// Return the stable version line printed by the scaffold executable.
#[must_use]
pub fn version_line() -> String {
    format!("paseo-control-plane {}", env!("CARGO_PKG_VERSION"))
}
