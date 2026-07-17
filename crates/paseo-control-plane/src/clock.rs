//! Injected monotonic clock boundary.

use std::time::Instant;

/// Monotonic milliseconds used for interaction ordering and proposal expiry.
pub trait Clock: Send + Sync {
    /// Return elapsed monotonic milliseconds.
    fn now_ms(&self) -> u64;
}

/// Production monotonic clock created at the composition root.
pub struct SystemClock {
    started: Instant,
}

impl SystemClock {
    /// Start a new process-relative monotonic clock.
    #[must_use]
    pub fn start() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}
