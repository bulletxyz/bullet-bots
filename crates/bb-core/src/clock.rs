//! Wall-clock and monotonic time abstraction.
//!
//! The [`Clock`] trait is the single time source for actors. Inject a
//! [`TestClock`] via [`HarnessBuilder::with_clock`] to write deterministic
//! tests without sleeping:
//!
//! ```ignore
//! let clock = TestClock::new(1_000_000);
//! let harness = HarnessBuilder::new()
//!     .with_clock(Arc::new(clock.clone()))
//!     .wire_actor(...)
//!     .build()?;
//!
//! // In the test body, advance time and inject events:
//! clock.advance(Duration::from_secs(61));
//! ```
//!
//! Strategies read time via `cx.clock()`:
//! ```ignore
//! let elapsed_ms = cx.clock().unix_ms() - self.entry_time_ms;
//! ```

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Abstracted time source. Implement this trait to control time in tests.
pub trait Clock: Send + Sync + 'static {
    /// Monotonic instant suitable for measuring durations within a session.
    /// Not comparable across process restarts.
    fn now(&self) -> Instant;

    /// Current time as Unix milliseconds. Use this for event timestamps or
    /// any comparison that must survive a restart.
    fn unix_ms(&self) -> u64;
}

/// Production clock — delegates to the OS.
#[derive(Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// Deterministic clock for tests. Start at a fixed Unix timestamp and
/// advance explicitly.
///
/// `TestClock` is cheaply cloned — clones share the same internal state, so
/// the harness and the test body see the same logical time.
#[derive(Clone, Default)]
pub struct TestClock {
    inner: Arc<Mutex<TestClockInner>>,
}

struct TestClockInner {
    unix_ms: u64,
    start: Instant,
    offset: Duration,
}

impl Default for TestClockInner {
    fn default() -> Self {
        Self { unix_ms: 0, start: Instant::now(), offset: Duration::ZERO }
    }
}

impl TestClock {
    /// Create a `TestClock` with the given initial Unix timestamp (ms).
    pub fn new(initial_unix_ms: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestClockInner {
                unix_ms: initial_unix_ms,
                start: Instant::now(),
                offset: Duration::ZERO,
            })),
        }
    }

    /// Advance logical time by `duration`. Both `now()` and `unix_ms()` advance.
    pub fn advance(&self, duration: Duration) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.unix_ms += u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        inner.offset += duration;
    }

    /// Jump to a specific Unix timestamp (ms).
    pub fn set_unix_ms(&self, ms: u64) {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).unix_ms = ms;
    }
}

impl Clock for TestClock {
    fn now(&self) -> Instant {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.start + inner.offset
    }

    fn unix_ms(&self) -> u64 {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).unix_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_advance() {
        let clock = TestClock::new(1_000_000);
        assert_eq!(clock.unix_ms(), 1_000_000);
        clock.advance(Duration::from_secs(5));
        assert_eq!(clock.unix_ms(), 1_005_000);
    }

    #[test]
    fn test_clock_clones_share_state() {
        let clock = TestClock::new(0);
        let clone = clock.clone();
        clock.advance(Duration::from_millis(500));
        assert_eq!(clone.unix_ms(), 500);
    }

    #[test]
    fn system_clock_is_nonzero() {
        let clock = SystemClock;
        assert!(clock.unix_ms() > 0);
    }
}
