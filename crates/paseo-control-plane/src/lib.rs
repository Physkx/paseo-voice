//! Inert control-plane scaffold.
//!
//! Phase 1 exposes version metadata only. It does not open sockets, read files,
//! resolve secrets, or invoke Paseo.

#![forbid(unsafe_code)]

/// Return the stable version line printed by the scaffold executable.
#[must_use]
pub fn version_line() -> String {
    format!("paseo-control-plane {}", env!("CARGO_PKG_VERSION"))
}
